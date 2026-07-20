import assert from "node:assert/strict";
import { Readable } from "node:stream";
import test from "node:test";

import axios, {
  AxiosHeaders,
  type AxiosAdapter,
  type InternalAxiosRequestConfig,
} from "axios";
import { createLogger } from "winston";

import { jwtCacheLifetimeSeconds, M2MTokenProvider } from "../src/auth.js";
import { loadConfig, type AppConfig } from "../src/config.js";
import { ClamAvError, ClamAvSizeLimitError } from "../src/errors.js";
import {
  KafkaMessageHandler,
  processMessageStream,
  type ConsumableKafkaMessage,
  type ConsumableKafkaMessageHandler,
  type ScanEventProcessor,
} from "../src/kafka.js";
import { ScanResultHandler } from "../src/result-handler.js";
import { ScanProcessor } from "../src/scan-processor.js";
import {
  CallbackOptions,
  WebhookAuthMethods,
  WebhookMethods,
  type AntivirusScanner,
  type ObjectStore,
  type ProcessedScan,
  type ResultHandler,
  type S3Location,
  type ScanEvent,
  type ScanPayload,
  type ScanResultPayload,
} from "../src/types.js";

const logger = createLogger({ silent: true });

/**
 * Creates deterministic scanner configuration for processing tests.
 *
 * @param overrides - Environment values that should replace the test defaults.
 * @returns Validated application configuration.
 */
function testConfig(overrides: NodeJS.ProcessEnv = {}): AppConfig {
  return loadConfig({
    AWS_REGION: "us-east-1",
    KAFKA_URL: "localhost:9092",
    ...overrides,
  });
}

/**
 * Creates the canonical no-callback event used by ScanProcessor tests.
 *
 * @param payloadOverrides - Payload fields to replace for one scenario.
 * @returns A complete validated-shape scan event.
 */
function scanEvent(payloadOverrides: Partial<ScanPayload> = {}): ScanEvent {
  return {
    "mime-type": "application/json",
    originator: "test-suite",
    payload: {
      callbackOption: CallbackOptions.NoCallback,
      fileName: "submission.zip",
      moveFile: false,
      submissionId: "submission-id",
      url: "https://s3.amazonaws.com/source-bucket/submission.zip",
      ...payloadOverrides,
    },
    timestamp: "2026-07-19T00:00:00.000Z",
    topic: "avscan.action.scan",
  };
}

/**
 * Converts an internal terminal payload to the callback result test doubles use.
 *
 * @param payload - Payload supplied to ResultHandler.
 * @returns Required external result fields plus preserved business fields.
 * @throws When the processor supplies an incomplete terminal result.
 */
function externalResult(payload: ScanPayload): ScanResultPayload {
  if (
    typeof payload.status !== "string" ||
    typeof payload.isInfected !== "boolean"
  ) {
    throw new Error("Expected a terminal scan payload");
  }
  return {
    ...payload,
    fileName: payload.fileName,
    isInfected: payload.isInfected,
    status: payload.status,
    url: payload.url,
  };
}

/**
 * Creates a completed scan value for Kafka processor test doubles.
 *
 * @param overrides - Callback payload fields to replace for one test.
 * @returns Completed result without post-commit cleanup.
 */
function processedResult(
  overrides: Partial<ScanResultPayload> = {},
): ProcessedScan {
  return {
    payload: {
      fileName: "submission.zip",
      isInfected: false,
      status: "scanned",
      url: "s3://source-bucket/submission.zip",
      ...overrides,
    },
  };
}

/**
 * Creates an unsigned JWT-shaped token for cache-expiry tests.
 *
 * @param expiresAtSeconds - Numeric exp claim in Unix seconds.
 * @returns Three-segment token readable by the non-authorizing cache parser.
 */
function testJwt(expiresAtSeconds: number): string {
  const header = Buffer.from(JSON.stringify({ alg: "none" })).toString(
    "base64url",
  );
  const payload = Buffer.from(
    JSON.stringify({ exp: expiresAtSeconds }),
  ).toString("base64url");
  return `${header}.${payload}.signature`;
}

/**
 * Builds an in-memory S3 adapter and its observable call state.
 *
 * @param contentLength - HeadObject size returned to ScanProcessor.
 * @returns Object store and mutable operation counters.
 */
function objectStoreFixture(contentLength: number): {
  copies: Array<[S3Location, string, string]>;
  deletes: S3Location[];
  openCount: { value: number };
  store: ObjectStore;
} {
  const copies: Array<[S3Location, string, string]> = [];
  const deletes: S3Location[] = [];
  const openCount = { value: 0 };
  const store: ObjectStore = {
    copy(source, bucket, key) {
      copies.push([source, bucket, key]);
      return Promise.resolve();
    },
    delete(source) {
      deletes.push(source);
      return Promise.resolve();
    },
    getMetadata() {
      return Promise.resolve({ contentLength });
    },
    openReadStream() {
      openCount.value += 1;
      return Promise.resolve(Readable.from([Buffer.from("file")]));
    },
  };
  return { copies, deletes, openCount, store };
}

/**
 * Builds a result-handler double that captures terminal payloads.
 *
 * @returns Captured payload array and ResultHandler implementation.
 */
function resultFixture(): {
  handled: ScanPayload[];
  results: ResultHandler;
} {
  const handled: ScanPayload[] = [];
  return {
    handled,
    results: {
      handle(payload) {
        handled.push(payload);
        return Promise.resolve({ payload: externalResult(payload) });
      },
    },
  };
}

void test("ScanProcessor streams a clean object and produces scanned=false", async () => {
  const config = testConfig();
  const { store, openCount } = objectStoreFixture(4);
  const { results, handled } = resultFixture();
  let scanCount = 0;
  const scanner: AntivirusScanner = {
    async ping() {},
    async scan(stream) {
      scanCount += 1;
      for await (const chunk of stream) {
        // Consume the stream as clamd would.
        void chunk;
      }
      return false;
    },
  };
  const processor = new ScanProcessor(config, store, scanner, results, logger);

  const result = await processor.process(scanEvent());

  assert.equal(openCount.value, 1);
  assert.equal(scanCount, 1);
  assert.equal(handled[0]?.status, "scanned");
  assert.equal(handled[0]?.isInfected, false);
  assert.equal(result.payload.submissionId, "submission-id");
});

void test("ScanProcessor rejects oversized metadata without opening S3", async () => {
  const config = testConfig({ MAX_SCAN_FILE_SIZE_BYTES: "3" });
  const { store, openCount } = objectStoreFixture(4);
  const { results, handled } = resultFixture();
  const scanner: AntivirusScanner = {
    async ping() {},
    scan() {
      return Promise.reject(new Error("scan must not run"));
    },
  };
  const processor = new ScanProcessor(config, store, scanner, results, logger);

  await processor.process(scanEvent());

  assert.equal(openCount.value, 0);
  assert.equal(handled[0]?.status, "scan-failed");
  assert.equal(handled[0]?.isInfected, true);
  assert.equal(handled[0]?.scanError, "file-size-exceeded");
});

void test("ScanProcessor converts only ClamAV size limits to fail-closed results", async () => {
  const config = testConfig();
  const { store } = objectStoreFixture(4);
  const { results, handled } = resultFixture();
  const scanner: AntivirusScanner = {
    async ping() {},
    scan() {
      return Promise.reject(
        new ClamAvSizeLimitError("INSTREAM size limit exceeded"),
      );
    },
  };
  const processor = new ScanProcessor(config, store, scanner, results, logger);

  await processor.process(scanEvent());

  assert.equal(handled[0]?.status, "scan-failed");
  assert.equal(handled[0]?.isInfected, true);
  assert.match(String(handled[0]?.scanErrorMessage), /INSTREAM size limit/);
});

void test("ScanProcessor propagates non-size ClamAV errors without a result", async () => {
  const config = testConfig();
  const { store } = objectStoreFixture(4);
  const { results, handled } = resultFixture();
  const scanner: AntivirusScanner = {
    async ping() {},
    scan() {
      return Promise.reject(new ClamAvError("clamd unavailable"));
    },
  };
  const processor = new ScanProcessor(config, store, scanner, results, logger);

  await assert.rejects(processor.process(scanEvent()), /clamd unavailable/);
  assert.equal(handled.length, 0);
});

void test("ScanProcessor enforces configured scan concurrency", async () => {
  const config = testConfig({ SCAN_CONCURRENCY: "1" });
  const { store } = objectStoreFixture(4);
  const { results } = resultFixture();
  const gates: Array<() => void> = [];
  let active = 0;
  let maximumActive = 0;
  const scanner: AntivirusScanner = {
    async ping() {},
    async scan() {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      await new Promise<void>((resolve) => gates.push(resolve));
      active -= 1;
      return false;
    },
  };
  const processor = new ScanProcessor(config, store, scanner, results, logger);

  const first = processor.process(scanEvent({ fileName: "first.zip" }));
  const second = processor.process(scanEvent({ fileName: "second.zip" }));
  while (gates.length < 1) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.equal(gates.length, 1);
  gates.shift()?.();
  while (gates.length < 1) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  gates.shift()?.();
  await Promise.all([first, second]);

  assert.equal(maximumActive, 1);
});

interface RecordedRequest {
  data: unknown;
  headers: Record<string, unknown>;
  method?: string;
  url?: string;
}

/**
 * Creates an Axios adapter that records transformed requests and serves tokens.
 *
 * @param requests - Array receiving each request made by production clients.
 * @param failingUrls - URLs that should simulate a remote failure.
 * @returns Axios adapter suitable for axios.create.
 */
function recordingAdapter(
  requests: RecordedRequest[],
  failingUrls: Set<string> = new Set(),
): AxiosAdapter {
  return (request: InternalAxiosRequestConfig) => {
    const record: RecordedRequest = {
      data:
        typeof request.data === "string"
          ? (JSON.parse(request.data) as unknown)
          : request.data,
      headers: request.headers.toJSON(),
      method: request.method,
      url: request.url,
    };
    requests.push(record);
    if (request.url && failingUrls.has(request.url)) {
      return Promise.reject(new Error(`Simulated failure for ${request.url}`));
    }
    return Promise.resolve({
      config: request,
      data:
        request.url === "https://auth.example/token"
          ? { access_token: "m2m-token", expires_in: 3600 }
          : { ok: true },
      headers: new AxiosHeaders(),
      status: 200,
      statusText: "OK",
    });
  };
}

void test("M2MTokenProvider does not cache an expired JWT without expires_in", async () => {
  const config = testConfig({
    AUTH0_AUDIENCE: "https://bus.example",
    AUTH0_CLIENT_ID: "client-id",
    AUTH0_CLIENT_SECRET: "client-secret",
    AUTH0_URL: "https://auth.example/token",
  });
  const expiredToken = testJwt(Math.floor(Date.now() / 1000) - 1);
  const requests: RecordedRequest[] = [];
  const adapter: AxiosAdapter = (request: InternalAxiosRequestConfig) => {
    requests.push({
      data: request.data,
      headers: request.headers.toJSON(),
      method: request.method,
      url: request.url,
    });
    return Promise.resolve({
      config: request,
      data: { access_token: expiredToken },
      headers: new AxiosHeaders(),
      status: 200,
      statusText: "OK",
    });
  };
  const tokens = new M2MTokenProvider(config.auth, axios.create({ adapter }));

  assert.equal(jwtCacheLifetimeSeconds(expiredToken), 0);
  await tokens.getToken();
  await tokens.getToken();

  assert.equal(requests.length, 2);
});

void test("ScanResultHandler stages a clean copy and posts the complete Bus envelope", async () => {
  const config = testConfig({
    AUTH0_AUDIENCE: "https://bus.example",
    AUTH0_CLIENT_ID: "client-id",
    AUTH0_CLIENT_SECRET: "client-secret",
    AUTH0_URL: "https://auth.example/token",
    BUSAPI_EVENTS_URL: "https://bus.example/events",
  });
  const requests: RecordedRequest[] = [];
  const http = axios.create({ adapter: recordingAdapter(requests) });
  const { store, copies, deletes } = objectStoreFixture(4);
  const tokens = new M2MTokenProvider(config.auth, http);
  const handler = new ScanResultHandler(config, store, tokens, http);
  const payload: ScanPayload = {
    callbackKafkaTopic: "submission.scan.complete",
    callbackOption: CallbackOptions.Kafka,
    cleanDestinationBucket: "clean-bucket",
    fileName: "folder name.zip",
    isInfected: false,
    moveFile: true,
    quarantineDestinationBucket: "quarantine-bucket",
    status: "scanned",
    submissionId: "submission-id",
    url: "s3://source-bucket/source.zip",
  };

  const processed = await handler.handle(payload, {
    bucket: "source-bucket",
    key: "source.zip",
    region: "us-east-1",
  });
  const result = processed.payload;

  assert.equal(copies.length, 1);
  assert.equal(copies[0]?.[1], "clean-bucket");
  assert.equal(copies[0]?.[2], "folder name.zip");
  assert.equal(deletes.length, 0);
  assert.equal(
    result.url,
    "https://s3.amazonaws.com/clean-bucket/folder%20name.zip",
  );
  assert.equal("moveFile" in result, false);
  assert.equal("callbackOption" in result, false);
  assert.equal(requests.length, 2);
  assert.equal(requests[1]?.url, "https://bus.example/events");
  assert.equal(requests[1]?.headers.Authorization, "Bearer m2m-token");
  const envelope = requests[1]?.data as Record<string, unknown>;
  assert.equal(envelope.topic, "submission.scan.complete");
  assert.equal(envelope.originator, "file-scanner-processor");
  assert.deepEqual(envelope.payload, result);

  await processed.afterCommit?.();
  assert.deepEqual(deletes, [
    { bucket: "source-bucket", key: "source.zip", region: "us-east-1" },
  ]);
});

void test("ScanResultHandler retains the source when callback delivery fails", async () => {
  const config = testConfig({
    AUTH0_AUDIENCE: "https://bus.example",
    AUTH0_CLIENT_ID: "client-id",
    AUTH0_CLIENT_SECRET: "client-secret",
    AUTH0_URL: "https://auth.example/token",
    BUSAPI_EVENTS_URL: "https://bus.example/events",
  });
  const requests: RecordedRequest[] = [];
  const http = axios.create({
    adapter: recordingAdapter(
      requests,
      new Set(["https://bus.example/events"]),
    ),
  });
  const { store, copies, deletes } = objectStoreFixture(4);
  const handler = new ScanResultHandler(
    config,
    store,
    new M2MTokenProvider(config.auth, http),
    http,
  );

  await assert.rejects(
    handler.handle(
      {
        callbackKafkaTopic: "submission.scan.complete",
        callbackOption: CallbackOptions.Kafka,
        cleanDestinationBucket: "clean-bucket",
        fileName: "submission.zip",
        isInfected: false,
        moveFile: true,
        quarantineDestinationBucket: "quarantine-bucket",
        status: "scanned",
        url: "s3://source-bucket/source.zip",
      },
      { bucket: "source-bucket", key: "source.zip", region: "us-east-1" },
    ),
    /Simulated failure/,
  );

  assert.equal(copies.length, 1);
  assert.equal(deletes.length, 0);
});

void test("ScanResultHandler preserves webhook API-key authentication and payload shape", async () => {
  const config = testConfig();
  const requests: RecordedRequest[] = [];
  const http = axios.create({ adapter: recordingAdapter(requests) });
  const { store } = objectStoreFixture(4);
  const tokens = new M2MTokenProvider(config.auth, http);
  const handler = new ScanResultHandler(config, store, tokens, http);

  const processed = await handler.handle(
    {
      callbackHook: {
        auth: WebhookAuthMethods.ApiKey,
        method: WebhookMethods.Post,
        secret: "webhook-secret",
        url: "https://callback.example/scan",
      },
      callbackOption: CallbackOptions.Webhook,
      fileName: "submission.zip",
      isInfected: true,
      moveFile: false,
      status: "scanned",
      submissionId: "submission-id",
      url: "s3://source-bucket/submission.zip",
    },
    { bucket: "source-bucket", key: "submission.zip", region: "us-east-1" },
  );
  const result = processed.payload;

  assert.equal(requests.length, 1);
  assert.equal(requests[0]?.headers["X-API-Key"], "webhook-secret");
  assert.deepEqual(requests[0]?.data, result);
  assert.equal("callbackHook" in result, false);
});

/**
 * Creates a Kafka message test double and observable commit count.
 *
 * @param value - Raw Kafka message text.
 * @param topic - Actual Kafka record topic.
 * @param partition - Kafka partition for scheduler tests.
 * @param offset - Kafka offset for ordering assertions.
 * @returns Consumable message and mutable commit counter.
 */
function kafkaMessage(
  value: string,
  topic = "avscan.action.scan",
  partition = 2,
  offset = 12n,
): { commits: { value: number }; message: ConsumableKafkaMessage } {
  const commits = { value: 0 };
  return {
    commits,
    message: {
      commit() {
        commits.value += 1;
        return Promise.resolve();
      },
      offset,
      partition,
      topic,
      value: Buffer.from(value),
    },
  };
}

void test("KafkaMessageHandler commits malformed JSON", async () => {
  const config = testConfig();
  const processor: ScanEventProcessor = {
    process() {
      return Promise.reject(new Error("processor must not run"));
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message, commits } = kafkaMessage("{broken");

  await handler.handle(message);

  assert.equal(commits.value, 1);
});

void test("KafkaMessageHandler commits tombstones without processing", async () => {
  const config = testConfig();
  let processed = 0;
  const processor: ScanEventProcessor = {
    process() {
      processed += 1;
      return Promise.resolve(processedResult());
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message, commits } = kafkaMessage("");
  message.value = undefined;

  await handler.handle(message);

  assert.equal(processed, 0);
  assert.equal(commits.value, 1);
});

void test("KafkaMessageHandler commits non-buffer values without processing", async () => {
  const config = testConfig();
  let processed = 0;
  const processor: ScanEventProcessor = {
    process() {
      processed += 1;
      return Promise.resolve(processedResult());
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message, commits } = kafkaMessage("");
  const unexpectedMessage = message as unknown as { value: unknown };
  unexpectedMessage.value = "not decoded bytes";

  await handler.handle(message);

  assert.equal(processed, 0);
  assert.equal(commits.value, 1);
});

void test("KafkaMessageHandler commits envelope-topic mismatches", async () => {
  const config = testConfig();
  const processor: ScanEventProcessor = {
    process() {
      return Promise.reject(new Error("processor must not run"));
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message, commits } = kafkaMessage(
    JSON.stringify({ topic: "another-topic" }),
  );

  await handler.handle(message);

  assert.equal(commits.value, 1);
});

void test("KafkaMessageHandler commits only after successful processing", async () => {
  const config = testConfig();
  let processed = 0;
  const processor: ScanEventProcessor = {
    process() {
      processed += 1;
      return Promise.resolve(processedResult());
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message, commits } = kafkaMessage(JSON.stringify(scanEvent()));

  await handler.handle(message);

  assert.equal(processed, 1);
  assert.equal(commits.value, 1);
});

void test("KafkaMessageHandler runs source cleanup only after commit", async () => {
  const config = testConfig();
  const order: string[] = [];
  const processor: ScanEventProcessor = {
    process() {
      return Promise.resolve({
        ...processedResult(),
        afterCommit: () => {
          order.push("cleanup");
          return Promise.resolve();
        },
      });
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message } = kafkaMessage(JSON.stringify(scanEvent()));
  message.commit = () => {
    order.push("commit");
    return Promise.resolve();
  };

  await handler.handle(message);

  assert.deepEqual(order, ["commit", "cleanup"]);
});

void test("KafkaMessageHandler skips source cleanup when commit fails", async () => {
  const config = testConfig();
  let cleanupCalled = false;
  const processor: ScanEventProcessor = {
    process() {
      return Promise.resolve({
        ...processedResult(),
        afterCommit: () => {
          cleanupCalled = true;
          return Promise.resolve();
        },
      });
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message } = kafkaMessage(JSON.stringify(scanEvent()));
  message.commit = () => Promise.reject(new Error("commit unavailable"));

  await assert.rejects(handler.handle(message), /commit unavailable/);

  assert.equal(cleanupCalled, false);
});

void test("KafkaMessageHandler leaves operational failures uncommitted", async () => {
  const config = testConfig();
  const processor: ScanEventProcessor = {
    process() {
      return Promise.reject(new Error("temporary S3 failure"));
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const { message, commits } = kafkaMessage(JSON.stringify(scanEvent()));

  await assert.rejects(handler.handle(message), /temporary S3 failure/);

  assert.equal(commits.value, 0);
});

void test("KafkaMessageHandler commits deterministic schema failures", async () => {
  const config = testConfig();
  const processor: ScanEventProcessor = {
    process() {
      return Promise.resolve(processedResult());
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);
  const invalid = scanEvent();
  delete (invalid.payload as Partial<ScanPayload>).callbackOption;
  const { message, commits } = kafkaMessage(JSON.stringify(invalid));

  await handler.handle(message);

  assert.equal(commits.value, 1);
});

void test("KafkaMessageHandler commits deterministic S3 URL failures", async () => {
  const config = testConfig();
  let processed = 0;
  const processor: ScanEventProcessor = {
    process() {
      processed += 1;
      return Promise.resolve(processedResult());
    },
  };
  const handler = new KafkaMessageHandler(config, processor, logger);

  for (const url of [
    "https://example.com/source-bucket/file.zip",
    "https://source-bucket.s3.us-west-2.amazonaws.com/file.zip",
    "s3://source-bucket",
    "https://s3.amazonaws.com/source-bucket/bad%ZZ.zip",
  ]) {
    const { message, commits } = kafkaMessage(
      JSON.stringify(scanEvent({ url })),
    );

    await handler.handle(message);

    assert.equal(commits.value, 1);
  }

  assert.equal(processed, 0);
});

void test("processMessageStream bounds work across Kafka partitions", async () => {
  let active = 0;
  let maximumActive = 0;
  let started = 0;
  let releaseHandlers: () => void = () => undefined;
  const handlerGate = new Promise<void>((resolve) => {
    releaseHandlers = resolve;
  });
  const handler: ConsumableKafkaMessageHandler = {
    async handle() {
      started += 1;
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      await handlerGate;
      active -= 1;
    },
  };
  const messages = [0, 1, 2, 3].map(
    (partition) =>
      kafkaMessage(
        JSON.stringify(scanEvent()),
        "avscan.action.scan",
        partition,
        0n,
      ).message,
  );

  const run = processMessageStream(Readable.from(messages), handler, 2);
  while (started < 2) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }

  assert.equal(active, 2);
  assert.equal(started, 2);
  releaseHandlers();
  await run;

  assert.equal(started, 4);
  assert.equal(maximumActive, 2);
});

void test("processMessageStream preserves order within one partition", async () => {
  const started: bigint[] = [];
  let releaseFirst: () => void = () => undefined;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const handler: ConsumableKafkaMessageHandler = {
    async handle(message) {
      started.push(message.offset);
      if (message.offset === 0n) {
        await firstGate;
      }
    },
  };
  const messages = [0n, 1n, 2n].map(
    (offset) =>
      kafkaMessage(JSON.stringify(scanEvent()), "avscan.action.scan", 0, offset)
        .message,
  );

  const run = processMessageStream(Readable.from(messages), handler, 3);
  while (started.length === 0) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(started, [0n]);

  releaseFirst();
  await run;

  assert.deepEqual(started, [0n, 1n, 2n]);
});

void test("processMessageStream drains active work but skips queued work on stop", async () => {
  const handled: bigint[] = [];
  let releaseActive: () => void = () => undefined;
  const activeGate = new Promise<void>((resolve) => {
    releaseActive = resolve;
  });
  const handler: ConsumableKafkaMessageHandler = {
    async handle(message) {
      handled.push(message.offset);
      await activeGate;
    },
  };
  const messages = [0n, 1n, 2n, 3n].map(
    (offset) =>
      kafkaMessage(
        JSON.stringify(scanEvent()),
        "avscan.action.scan",
        Number(offset),
        offset,
      ).message,
  );
  const stopController = new AbortController();

  const run = processMessageStream(
    Readable.from(messages),
    handler,
    1,
    stopController.signal,
  );
  while (handled.length === 0) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  stopController.abort();
  releaseActive();
  await run;

  assert.deepEqual(handled, [0n]);
});

void test("processMessageStream blocks later partition offsets after failure", async () => {
  const handled: Array<[number, bigint]> = [];
  const handler: ConsumableKafkaMessageHandler = {
    handle(message) {
      handled.push([message.partition, message.offset]);
      return message.partition === 0 && message.offset === 0n
        ? Promise.reject(new Error("first partition record failed"))
        : Promise.resolve();
    },
  };
  const messages = [
    kafkaMessage(JSON.stringify(scanEvent()), "avscan.action.scan", 0, 0n)
      .message,
    kafkaMessage(JSON.stringify(scanEvent()), "avscan.action.scan", 0, 1n)
      .message,
    kafkaMessage(JSON.stringify(scanEvent()), "avscan.action.scan", 1, 0n)
      .message,
  ];

  await assert.rejects(
    processMessageStream(Readable.from(messages), handler, 2),
    /first partition record failed/,
  );

  assert.equal(
    handled.some(([partition, offset]) => partition === 0 && offset === 1n),
    false,
  );
});

void test("processMessageStream stops queued work after an iterator failure", async () => {
  const handled: bigint[] = [];
  let iteratorFailed = false;
  let releaseActive: () => void = () => undefined;
  const activeGate = new Promise<void>((resolve) => {
    releaseActive = resolve;
  });
  const first = kafkaMessage(
    JSON.stringify(scanEvent()),
    "avscan.action.scan",
    0,
    0n,
  ).message;
  const second = kafkaMessage(
    JSON.stringify(scanEvent()),
    "avscan.action.scan",
    0,
    1n,
  ).message;
  const source: AsyncIterable<ConsumableKafkaMessage> = {
    /** Yields two records and then simulates a Kafka stream failure. */
    async *[Symbol.asyncIterator]() {
      await Promise.resolve();
      yield first;
      yield second;
      iteratorFailed = true;
      throw new Error("Kafka stream failed");
    },
  };
  const handler: ConsumableKafkaMessageHandler = {
    async handle(message) {
      handled.push(message.offset);
      await activeGate;
    },
  };

  const run = processMessageStream(source, handler, 1);
  while (!iteratorFailed) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  releaseActive();

  await assert.rejects(run, /Kafka stream failed/);
  assert.deepEqual(handled, [0n]);
});
