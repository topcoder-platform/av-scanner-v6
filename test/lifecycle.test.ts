import assert from "node:assert/strict";
import test from "node:test";

import { createLogger } from "winston";

import { loadConfig } from "../src/config.js";
import { closeApplicationResources, withTimeout } from "../src/index.js";
import {
  KafkaConsumerRunner,
  type ConsumableKafkaMessageHandler,
  type KafkaConsumerFactory,
} from "../src/kafka.js";

void test("withTimeout preserves successful results", async () => {
  const result = await withTimeout(Promise.resolve("closed"), 100, "test");

  assert.equal(result, "closed");
});

void test("withTimeout rejects stalled lifecycle operations", async () => {
  await assert.rejects(
    withTimeout(new Promise<void>(() => undefined), 10, "stalled close"),
    /stalled close timed out after 10ms/,
  );
});

void test("closeApplicationResources stops health when Kafka close stalls", async () => {
  let healthStopped = false;
  const consumer = {
    /** Simulates a Kafka client whose close callback never settles. */
    close(): Promise<void> {
      return new Promise<void>(() => undefined);
    },
  };
  const health = {
    /** Records the independent health-server close attempt. */
    stop(): Promise<void> {
      healthStopped = true;
      return Promise.resolve();
    },
  };

  await assert.rejects(
    closeApplicationResources(consumer, health, 10),
    (error: unknown) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors.length, 1);
      assert.match(String(error.errors[0]), /Kafka resource close timed out/);
      return true;
    },
  );
  assert.equal(healthStopped, true);
});

void test("closeApplicationResources retains both close failures", async () => {
  const consumerError = new Error("consumer close failed");
  const healthError = new Error("health close failed");
  const consumer = {
    /** Rejects the Kafka close operation for aggregation coverage. */
    close(): Promise<void> {
      return Promise.reject(consumerError);
    },
  };
  const health = {
    /** Rejects the health close operation for aggregation coverage. */
    stop(): Promise<void> {
      return Promise.reject(healthError);
    },
  };

  await assert.rejects(
    closeApplicationResources(consumer, health, 100),
    (error: unknown) => {
      assert.ok(error instanceof AggregateError);
      assert.deepEqual(error.errors, [consumerError, healthError]);
      return true;
    },
  );
});

void test("KafkaConsumerRunner stops promptly while consume setup is pending", async () => {
  let signalConsumeStarted: () => void = () => undefined;
  const consumeStarted = new Promise<void>((resolve) => {
    signalConsumeStarted = resolve;
  });
  let closeCalls = 0;
  const pendingConsume = new Promise<never>(() => undefined);
  const fakeConsumer = {
    /** Records setup and deliberately leaves the consume request pending. */
    consume(): Promise<never> {
      signalConsumeStarted();
      return pendingConsume;
    },
    /** Records the setup-time client close requested by stopIntake. */
    close(): Promise<void> {
      closeCalls += 1;
      return Promise.resolve();
    },
  } as unknown as ReturnType<KafkaConsumerFactory>;
  const consumerFactory: KafkaConsumerFactory = () => fakeConsumer;
  const config = loadConfig({ KAFKA_URL: "unanswered.test:9092" });
  const handler: ConsumableKafkaMessageHandler = {
    /** No record can arrive while the fake consume request is pending. */
    handle(): Promise<void> {
      return Promise.resolve();
    },
  };
  const runner = new KafkaConsumerRunner(
    config.kafka,
    handler,
    1,
    createLogger({ silent: true }),
    consumerFactory,
  );

  const run = runner.run();
  await consumeStarted;
  await runner.stopIntake();
  await withTimeout(run, 100, "Kafka setup stop");
  await withTimeout(runner.close(), 100, "Kafka setup close");

  assert.equal(closeCalls, 1);
});
