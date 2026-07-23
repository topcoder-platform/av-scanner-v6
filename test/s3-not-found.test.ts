import assert from "node:assert/strict";
import { Readable } from "node:stream";
import test from "node:test";

import {
  GetObjectCommand,
  HeadObjectCommand,
  NoSuchBucket,
  NoSuchKey,
  NotFound,
  S3Client,
  S3ServiceException,
} from "@aws-sdk/client-s3";
import axios from "axios";
import { createLogger } from "winston";

import { M2MTokenProvider } from "../src/auth.js";
import { loadConfig } from "../src/config.js";
import { S3ObjectNotFoundError } from "../src/errors.js";
import {
  KafkaMessageHandler,
  type ConsumableKafkaMessage,
} from "../src/kafka.js";
import { ScanResultHandler } from "../src/result-handler.js";
import { S3ObjectStore, isS3ObjectNotFound } from "../src/s3.js";
import { ScanProcessor } from "../src/scan-processor.js";
import {
  CallbackOptions,
  type AntivirusScanner,
  type ObjectStore,
  type ResultHandler,
  type S3Location,
  type ScanEvent,
  type ScanPayload,
  type ScanResultPayload,
} from "../src/types.js";

const logger = createLogger({ silent: true });
const source: S3Location = {
  bucket: "source-bucket",
  key: "submission.zip",
  region: "us-east-1",
};

/** Creates deterministic retry configuration without opening broker sockets. */
function retryConfig(attempts = 3, baseDelayMs = 10) {
  return loadConfig({
    AWS_REGION: "us-east-1",
    KAFKA_URL: "localhost:9092",
    S3_NOT_FOUND_MAX_ATTEMPTS: String(attempts),
    S3_NOT_FOUND_RETRY_BASE_DELAY_MS: String(baseDelayMs),
  });
}

/** Creates a complete event whose source would normally be moved after scan. */
function movedEvent(
  callbackOption: ScanPayload["callbackOption"] = CallbackOptions.NoCallback,
): ScanEvent {
  return {
    "mime-type": "application/json",
    originator: "test-suite",
    payload: {
      ...(callbackOption === CallbackOptions.Kafka
        ? { callbackKafkaTopic: "submission.scan.complete" }
        : {}),
      callbackOption,
      cleanDestinationBucket: "clean-bucket",
      fileName: "submission.zip",
      moveFile: true,
      quarantineDestinationBucket: "quarantine-bucket",
      submissionId: "submission-id",
      url: "s3://source-bucket/submission.zip",
    },
    timestamp: "2026-07-24T00:00:00.000Z",
    topic: "avscan.action.scan",
  };
}

/** Scanner double that consumes a body and reports it clean. */
const cleanScanner: AntivirusScanner = {
  async ping() {},
  async scan(stream) {
    for await (const chunk of stream) {
      void chunk;
    }
    return false;
  },
};

/** Converts an internal terminal payload for focused ResultHandler doubles. */
function externalResult(payload: ScanPayload): ScanResultPayload {
  if (
    typeof payload.isInfected !== "boolean" ||
    typeof payload.status !== "string"
  ) {
    throw new Error("Expected a terminal result");
  }
  return {
    ...payload,
    fileName: payload.fileName,
    isInfected: payload.isInfected,
    status: payload.status,
    url: payload.url,
  };
}

void test("classifies only modeled S3 missing-object 404 errors", () => {
  const notFound = new NotFound({
    $metadata: { httpStatusCode: 404 },
    message: "UnknownError",
  });
  const noSuchKey = new NoSuchKey({
    $metadata: { httpStatusCode: 404 },
    message: "missing",
  });
  const noSuchBucket = new NoSuchBucket({
    $metadata: { httpStatusCode: 404 },
    message: "missing bucket",
  });
  const accessDenied = new S3ServiceException({
    $fault: "client",
    $metadata: { httpStatusCode: 403 },
    message: "denied",
    name: "AccessDenied",
  });
  const wrongStatus = new NotFound({
    $metadata: { httpStatusCode: 500 },
    message: "unexpected",
  });

  assert.equal(isS3ObjectNotFound(notFound), true);
  assert.equal(isS3ObjectNotFound(noSuchKey), true);
  assert.equal(isS3ObjectNotFound(noSuchBucket), false);
  assert.equal(isS3ObjectNotFound(accessDenied), false);
  assert.equal(isS3ObjectNotFound(wrongStatus), false);
  assert.equal(isS3ObjectNotFound(new Error("NotFound")), false);
});

void test("S3ObjectStore translates HeadObject and GetObject missing responses", async () => {
  const headCause = new NotFound({
    $metadata: { httpStatusCode: 404 },
    message: "UnknownError",
  });
  const headStore = new S3ObjectStore({
    send(command: unknown) {
      assert.ok(command instanceof HeadObjectCommand);
      return Promise.reject(headCause);
    },
  } as unknown as S3Client);

  await assert.rejects(headStore.getMetadata(source), (error: unknown) => {
    assert.ok(error instanceof S3ObjectNotFoundError);
    assert.equal(error.operation, "HeadObject");
    assert.deepEqual(error.location, source);
    assert.equal(error.cause, headCause);
    return true;
  });

  const getCause = new NoSuchKey({
    $metadata: { httpStatusCode: 404 },
    message: "missing",
  });
  const getStore = new S3ObjectStore({
    send(command: unknown) {
      assert.ok(command instanceof GetObjectCommand);
      return Promise.reject(getCause);
    },
  } as unknown as S3Client);

  await assert.rejects(getStore.openReadStream(source), (error: unknown) => {
    assert.ok(error instanceof S3ObjectNotFoundError);
    assert.equal(error.operation, "GetObject");
    assert.deepEqual(error.location, source);
    assert.equal(error.cause, getCause);
    return true;
  });
});

void test("ScanProcessor retries missing metadata and recovers normally", async () => {
  const config = retryConfig(3, 10);
  let metadataCalls = 0;
  const delays: number[] = [];
  const store: ObjectStore = {
    copy() {
      return Promise.resolve();
    },
    delete() {
      return Promise.resolve();
    },
    getMetadata() {
      metadataCalls += 1;
      return metadataCalls < 3
        ? Promise.reject(
            new S3ObjectNotFoundError("HeadObject", source, {
              cause: new Error("missing"),
            }),
          )
        : Promise.resolve({ contentLength: 4 });
    },
    openReadStream() {
      return Promise.resolve(Readable.from([Buffer.from("file")]));
    },
  };
  const handled: ScanPayload[] = [];
  const results: ResultHandler = {
    handle(payload) {
      handled.push(payload);
      return Promise.resolve({ payload: externalResult(payload) });
    },
  };
  const processor = new ScanProcessor(
    config,
    store,
    cleanScanner,
    results,
    logger,
    (delayMs) => {
      delays.push(delayMs);
      return Promise.resolve();
    },
  );

  const result = await processor.process(movedEvent());

  assert.equal(metadataCalls, 3);
  assert.deepEqual(delays, [10, 20]);
  assert.equal(handled.length, 1);
  assert.equal(result.payload.status, "scanned");
  assert.equal(result.payload.isInfected, false);
});

void test("ScanProcessor restarts at HeadObject after a GetObject race", async () => {
  const config = retryConfig(2, 25);
  let metadataCalls = 0;
  let getCalls = 0;
  const delays: number[] = [];
  const store: ObjectStore = {
    copy() {
      return Promise.resolve();
    },
    delete() {
      return Promise.resolve();
    },
    getMetadata() {
      metadataCalls += 1;
      return Promise.resolve({ contentLength: 4 });
    },
    openReadStream() {
      getCalls += 1;
      return getCalls === 1
        ? Promise.reject(new S3ObjectNotFoundError("GetObject", source))
        : Promise.resolve(Readable.from([Buffer.from("file")]));
    },
  };
  const results: ResultHandler = {
    handle(payload) {
      return Promise.resolve({ payload: externalResult(payload) });
    },
  };
  const processor = new ScanProcessor(
    config,
    store,
    cleanScanner,
    results,
    logger,
    (delayMs) => {
      delays.push(delayMs);
      return Promise.resolve();
    },
  );

  const result = await processor.process(movedEvent());

  assert.equal(metadataCalls, 2);
  assert.equal(getCalls, 2);
  assert.deepEqual(delays, [25]);
  assert.equal(result.payload.status, "scanned");
});

void test("exhausted missing source emits fail-closed result without movement", async () => {
  const config = retryConfig(3, 10);
  let metadataCalls = 0;
  let copyCalls = 0;
  let deleteCalls = 0;
  const delays: number[] = [];
  const store: ObjectStore = {
    copy() {
      copyCalls += 1;
      return Promise.resolve();
    },
    delete() {
      deleteCalls += 1;
      return Promise.resolve();
    },
    getMetadata() {
      metadataCalls += 1;
      return Promise.reject(new S3ObjectNotFoundError("HeadObject", source));
    },
    openReadStream() {
      return Promise.reject(new Error("GetObject must not run"));
    },
  };
  const http = axios.create();
  const results = new ScanResultHandler(
    config,
    store,
    new M2MTokenProvider(config.auth, http),
    http,
  );
  const processor = new ScanProcessor(
    config,
    store,
    cleanScanner,
    results,
    logger,
    (delayMs) => {
      delays.push(delayMs);
      return Promise.resolve();
    },
  );

  const processed = await processor.process(movedEvent());

  assert.equal(metadataCalls, 3);
  assert.deepEqual(delays, [10, 20]);
  assert.equal(copyCalls, 0);
  assert.equal(deleteCalls, 0);
  assert.equal(processed.afterCommit, undefined);
  assert.equal(processed.payload.status, "scan-failed");
  assert.equal(processed.payload.isInfected, true);
  assert.equal(processed.payload.scanError, "s3-object-not-found");
  assert.equal(processed.payload.url, "s3://source-bucket/submission.zip");
  assert.equal(processed.payload.submissionId, "submission-id");
  assert.equal("moveFile" in processed.payload, false);
});

void test("terminal missing-source result lets Kafka commit the poison record", async () => {
  const config = retryConfig(1, 10);
  const store: ObjectStore = {
    copy() {
      return Promise.reject(new Error("copy must not run"));
    },
    delete() {
      return Promise.reject(new Error("delete must not run"));
    },
    getMetadata() {
      return Promise.reject(new S3ObjectNotFoundError("HeadObject", source));
    },
    openReadStream() {
      return Promise.reject(new Error("GetObject must not run"));
    },
  };
  const results: ResultHandler = {
    handle(payload) {
      assert.equal(payload.moveFile, false);
      assert.equal(payload.scanError, "s3-object-not-found");
      return Promise.resolve({ payload: externalResult(payload) });
    },
  };
  const processor = new ScanProcessor(
    config,
    store,
    cleanScanner,
    results,
    logger,
  );
  const handler = new KafkaMessageHandler(config, processor, logger);
  let commits = 0;
  const event = movedEvent();
  const message: ConsumableKafkaMessage = {
    commit() {
      commits += 1;
      return Promise.resolve();
    },
    metadata: {
      consumer: {
        coordinatorId: 1,
        generationId: 1,
        groupId: "file-scanner-processor",
        memberId: "member-a",
      },
    },
    offset: 1251n,
    partition: 8,
    topic: event.topic,
    value: Buffer.from(JSON.stringify(event)),
  };

  await handler.handle(message);

  assert.equal(commits, 1);
});

void test("missing-source callback failure leaves the Kafka offset uncommitted", async () => {
  const config = retryConfig(1, 10);
  const store: ObjectStore = {
    copy() {
      return Promise.reject(new Error("copy must not run"));
    },
    delete() {
      return Promise.reject(new Error("delete must not run"));
    },
    getMetadata() {
      return Promise.reject(new S3ObjectNotFoundError("HeadObject", source));
    },
    openReadStream() {
      return Promise.reject(new Error("GetObject must not run"));
    },
  };
  const results: ResultHandler = {
    handle(payload) {
      assert.equal(payload.moveFile, false);
      return Promise.reject(new Error("callback unavailable"));
    },
  };
  const processor = new ScanProcessor(
    config,
    store,
    cleanScanner,
    results,
    logger,
  );
  const handler = new KafkaMessageHandler(config, processor, logger);
  let commits = 0;
  const event = movedEvent(CallbackOptions.Kafka);
  const message: ConsumableKafkaMessage = {
    commit() {
      commits += 1;
      return Promise.resolve();
    },
    metadata: {
      consumer: {
        coordinatorId: 1,
        generationId: 1,
        groupId: "file-scanner-processor",
        memberId: "member-a",
      },
    },
    offset: 1194n,
    partition: 1,
    topic: event.topic,
    value: Buffer.from(JSON.stringify(event)),
  };

  await assert.rejects(handler.handle(message), /callback unavailable/);

  assert.equal(commits, 0);
});

void test("non-missing S3 failures remain uncommitted operational errors", async () => {
  const config = retryConfig(3, 10);
  const operationalFailure = new Error("S3 unavailable");
  let calls = 0;
  let waits = 0;
  const store: ObjectStore = {
    copy() {
      return Promise.resolve();
    },
    delete() {
      return Promise.resolve();
    },
    getMetadata() {
      calls += 1;
      return Promise.reject(operationalFailure);
    },
    openReadStream() {
      return Promise.reject(new Error("GetObject must not run"));
    },
  };
  const results: ResultHandler = {
    handle() {
      return Promise.reject(new Error("result handler must not run"));
    },
  };
  const processor = new ScanProcessor(
    config,
    store,
    cleanScanner,
    results,
    logger,
    () => {
      waits += 1;
      return Promise.resolve();
    },
  );

  await assert.rejects(processor.process(movedEvent()), operationalFailure);

  assert.equal(calls, 1);
  assert.equal(waits, 0);
});
