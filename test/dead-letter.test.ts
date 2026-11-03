/* eslint-disable @typescript-eslint/require-await -- Async doubles model promise-based I/O without real network calls. */
import assert from "node:assert/strict";
import { test } from "node:test";
import { Admin, Producer } from "@platformatic/kafka";
import axios from "axios";
import { createLogger } from "winston";

import { M2MTokenProvider } from "../src/auth.js";
import { loadConfig } from "../src/config.js";
import {
  KafkaDeadLetterPublisher,
  type FailedCallback,
} from "../src/dead-letter.js";
import { S3ObjectNotFoundError } from "../src/errors.js";
import {
  KafkaMessageHandler,
  processMessageStream,
  type ConsumableKafkaMessage,
} from "../src/kafka.js";
import {
  CallbackDeliveryError,
  ScanResultHandler,
} from "../src/result-handler.js";
import { ScanProcessor } from "../src/scan-processor.js";
import type { ObjectStore, ScanEvent, ScanPayload } from "../src/types.js";

const logger = createLogger({ silent: true });
const config = loadConfig({
  S3_NOT_FOUND_MAX_ATTEMPTS: "1",
  CALLBACK_RETRY_BASE_DELAY_MS: "1",
});
const source = { bucket: "source-bucket", key: "x.bin", region: "us-east-1" };
const event: ScanEvent = {
  "mime-type": "application/json",
  originator: "test",
  timestamp: "2026-11-03T00:00:00Z",
  topic: config.kafka.topic,
  payload: {
    fileName: "x.bin",
    url: "s3://source-bucket/x.bin",
    moveFile: false,
    callbackOption: "webhook",
    callbackHook: {
      url: "https://callback.example/result",
      auth: "bearer",
      secret: "test-secret-never-log",
      method: "post",
    },
  },
};
const terminal: ScanPayload = {
  ...event.payload,
  isInfected: true,
  status: "scan-failed",
  scanError: "s3-object-not-found",
};

function message(
  offset: bigint,
  commit: () => Promise<void>,
): ConsumableKafkaMessage {
  return {
    metadata: {},
    offset,
    partition: 3,
    topic: event.topic,
    value: Buffer.from(JSON.stringify(event)),
    commit,
  };
}

void test("missing source and exhausted webhook are retained before commit; next record runs", async () => {
  const order: string[] = [];
  let deliveries = 0;
  const http = axios.create({
    adapter: async () => {
      deliveries++;
      throw new Error("timeout with secret HTTP config");
    },
  });
  const store: ObjectStore = {
    async getMetadata() {
      throw new S3ObjectNotFoundError("HeadObject", source);
    },
    async openReadStream() {
      throw new Error("must not open missing source");
    },
    async copy() {
      throw new Error("must not copy missing source");
    },
    async delete() {
      throw new Error("must retain source for replay");
    },
  };
  const resultHandler = new ScanResultHandler(
    config,
    store,
    new M2MTokenProvider(config.auth, http),
    http,
    undefined,
    async () => {},
  );
  const processor = new ScanProcessor(
    config,
    store,
    {
      async ping() {},
      async scan() {
        throw new Error("must not scan missing source");
      },
    },
    resultHandler,
    logger,
  );
  const retained: FailedCallback[] = [];
  const handler = new KafkaMessageHandler(config, processor, logger, {
    async publish(record) {
      retained.push(record);
      order.push("retain:" + record.source.offset);
    },
  });
  async function* records() {
    yield message(1331n, async () => {
      order.push("commit:1331");
    });
    yield message(1332n, async () => {
      order.push("commit:1332");
    });
  }
  await processMessageStream(records(), handler, 1);
  assert.equal(deliveries, 6);
  assert.deepEqual(order, [
    "retain:1331",
    "commit:1331",
    "retain:1332",
    "commit:1332",
  ]);
  assert.equal(retained[0]?.result.isInfected, true);
  assert.equal(retained[0]?.result.status, "scan-failed");
  assert.deepEqual(retained[0]?.originalEvent, {
    ...event,
    timestamp: new Date(event.timestamp),
  });
  assert.equal(retained[0]?.attempts, 3);
  assert.equal(retained[0]?.id, `${event.topic}:3:1331`);
  assert.ok(!JSON.stringify(retained).includes("secret HTTP config"));
});

void test("failed dead-letter publication leaves input uncommitted", async () => {
  let commits = 0;
  const handler = new KafkaMessageHandler(
    config,
    {
      async process() {
        throw new CallbackDeliveryError(terminal, 3);
      },
    },
    logger,
    {
      async publish() {
        throw new Error("broker unavailable");
      },
    },
  );
  await assert.rejects(
    handler.handle(
      message(1n, async () => {
        commits++;
      }),
    ),
    /broker unavailable/,
  );
  assert.equal(commits, 0);
});

void test("input commit failure leaves a replayable record with a stable deduplication key", async () => {
  const ids: string[] = [];
  const handler = new KafkaMessageHandler(
    config,
    {
      async process() {
        throw new CallbackDeliveryError(terminal, 3);
      },
    },
    logger,
    {
      async publish(record) {
        ids.push(record.id);
      },
    },
  );
  for (let i = 0; i < 2; i++) {
    await assert.rejects(
      handler.handle(
        message(8n, async () => {
          throw new Error("stale assignment");
        }),
      ),
      /stale assignment/,
    );
  }
  assert.deepEqual(ids, [`${event.topic}:3:8`, `${event.topic}:3:8`]);
});

void test("ordinary scan failures are not committed or dead-lettered as callback failures", async () => {
  const handler = new KafkaMessageHandler(
    config,
    {
      async process() {
        throw new Error("S3 access denied");
      },
    },
    logger,
    {
      async publish() {
        assert.fail("not a callback failure");
      },
    },
  );
  await assert.rejects(
    handler.handle(
      message(1n, async () => {
        assert.fail("must not commit");
      }),
    ),
    /S3 access denied/,
  );
});

void test("transient callback succeeds on retry, copies once and deletes only after commit", async () => {
  const order: string[] = [];
  let calls = 0;
  const waits: number[] = [];
  const http = axios.create({
    adapter: async (request) => {
      if (++calls < 3) throw new Error("temporary timeout");
      return {
        data: {},
        headers: {},
        config: request,
        status: 200,
        statusText: "OK",
      };
    },
  });
  const store: ObjectStore = {
    async copy() {
      order.push("copy");
    },
    async delete() {
      order.push("delete");
    },
    async getMetadata() {
      throw new Error("unused");
    },
    async openReadStream() {
      throw new Error("unused");
    },
  };
  const results = new ScanResultHandler(
    config,
    store,
    new M2MTokenProvider(config.auth, http),
    http,
    undefined,
    async (ms) => {
      waits.push(ms);
    },
  );
  const processed = await results.handle(
    {
      ...terminal,
      moveFile: true,
      quarantineDestinationBucket: "quarantine-bucket",
    },
    source,
  );
  assert.deepEqual(order, ["copy"]);
  assert.equal(calls, 3);
  assert.deepEqual(waits, [1, 2]);
  await processed.afterCommit?.();
  assert.deepEqual(order, ["copy", "delete"]);
});

void test("dead-letter producer requires broker offsets and all-replica acknowledgements", async (t) => {
  const producer = new Producer({
    bootstrapBrokers: ["localhost:9092"],
    clientId: "test",
  });
  t.after(() => producer.close());
  let result = {};
  t.mock.method(
    producer,
    "send",
    async (options: { acks: number; autocreateTopics: boolean }) => {
      assert.equal(options.acks, -1);
      assert.equal(options.autocreateTopics, false);
      return result;
    },
  );
  const publisher = new KafkaDeadLetterPublisher(config, logger, producer);
  const record: FailedCallback = {
    schemaVersion: 1,
    id: "test:3:1",
    failedAt: "2026-11-03T00:00:00Z",
    source: { topic: event.topic, partition: 3, offset: "1" },
    originalEvent: event,
    result: terminal,
    attempts: 3,
  };
  await assert.rejects(publisher.publish(record), /did not acknowledge/);
  result = {
    offsets: [{ topic: config.deadLetter.topic, partition: 0, offset: 1n }],
  };
  await publisher.publish(record);
  result = { ...result, unwritableNodes: [1] };
  await assert.rejects(publisher.publish(record), /did not acknowledge/);
});

void test("topic initialization uses explicit retention and replication and closes admin", async (t) => {
  const producer = new Producer({
    bootstrapBrokers: ["localhost:9092"],
    clientId: "test",
  });
  const admin = new Admin({
    bootstrapBrokers: ["localhost:9092"],
    clientId: "test-admin",
  });
  t.after(() => producer.close());
  t.mock.method(admin, "listTopics", async () => []);
  const create = t.mock.method(admin, "createTopics", async () => []);
  const close = t.mock.method(admin, "close", async () => {});
  const publisher = new KafkaDeadLetterPublisher(
    config,
    logger,
    producer,
    () => admin,
  );
  await publisher.initialize();
  assert.deepEqual(create.mock.calls[0]?.arguments[0], {
    topics: [config.deadLetter.topic],
    partitions: 1,
    replicas: 2,
    configs: [
      { name: "cleanup.policy", value: "delete" },
      { name: "retention.ms", value: "604800000" },
      { name: "min.insync.replicas", value: "2" },
    ],
  });
  assert.equal(close.mock.callCount(), 1);
});

void test("dead-letter configuration rejects feedback loops and unbounded retry counts", () => {
  assert.throws(
    () => loadConfig({ AVSCAN_DLQ_TOPIC: "avscan.action.scan" }),
    /distinct/,
  );
  assert.throws(
    () => loadConfig({ CALLBACK_MAX_ATTEMPTS: "6" }),
    /cannot exceed/,
  );
});
