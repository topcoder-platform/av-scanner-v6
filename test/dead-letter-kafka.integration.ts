/** Run only against an isolated Kafka broker: KAFKA_URL=localhost:9092 pnpm test:dead-letter-integration. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Admin, Consumer } from "@platformatic/kafka";
import { createLogger } from "winston";
import { loadConfig } from "../src/config.js";
import {
  KafkaDeadLetterPublisher,
  type FailedCallback,
} from "../src/dead-letter.js";
import { withTimeout } from "../src/index.js";

if (!process.env.KAFKA_URL)
  throw new Error("Set KAFKA_URL to an isolated test broker");
const id = randomUUID();
const config = loadConfig({
  ...process.env,
  AVSCAN_DLQ_TOPIC: `avscan-test-${id}`,
  AVSCAN_DLQ_REPLICAS: "1",
});
const logger = createLogger({ silent: true });
const publisher = new KafkaDeadLetterPublisher(config, logger);
const consumer = new Consumer({
  bootstrapBrokers: config.kafka.brokers,
  clientId: `test-${id}`,
  groupId: `test-${id}`,
});
const admin = new Admin({
  bootstrapBrokers: config.kafka.brokers,
  clientId: `test-admin-${id}`,
});
consumer.on("error", () => undefined);
admin.on("error", () => undefined);
try {
  await publisher.initialize();
  const payload = {
    callbackOption: "no-callback" as const,
    moveFile: false,
    fileName: "x.bin",
    url: "s3://test/x.bin",
  };
  const record: FailedCallback = {
    schemaVersion: 1,
    id: "avscan.action.scan:3:1331",
    failedAt: new Date().toISOString(),
    source: { topic: config.kafka.topic, partition: 3, offset: "1331" },
    attempts: 3,
    originalEvent: {
      "mime-type": "application/json",
      originator: "integration-test",
      timestamp: new Date().toISOString(),
      topic: config.kafka.topic,
      payload,
    },
    result: { ...payload, status: "scan-failed", isInfected: true },
  };
  await publisher.publish(record);
  const stream = await consumer.consume({
    topics: [config.deadLetter.topic],
    mode: "earliest",
    autocommit: false,
  });
  try {
    const next = await withTimeout(
      stream[Symbol.asyncIterator]().next(),
      15_000,
      "Read retained callback",
    );
    assert.equal(next.done, false);
    assert.equal(next.value?.key?.toString(), record.id);
    assert.deepEqual(JSON.parse(next.value.value.toString()), record);
    console.log(
      "PASS: provisioned topic, broker acknowledged failed callback, independent consumer read exact replay record",
    );
  } finally {
    await stream.close();
  }
} finally {
  await publisher.close();
  await consumer.close();
  await admin.deleteTopics({ topics: [config.deadLetter.topic] });
  await admin.close();
}
