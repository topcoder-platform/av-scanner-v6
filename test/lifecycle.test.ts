import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import test from "node:test";

import {
  Consumer,
  consumerHeartbeatChannel,
  type fetchV17,
  kGetApi,
  MessagesStream,
} from "@platformatic/kafka";
import { createLogger } from "winston";

import { loadConfig } from "../src/config.js";
import { closeApplicationResources, withTimeout } from "../src/index.js";
import {
  fenceKafkaFetchResponses,
  KafkaAssignmentFence,
  KafkaConsumerRunner,
  processMessageStream,
  StaleKafkaAssignmentError,
  type ConsumableKafkaMessage,
  type ConsumableKafkaMessageHandler,
  type KafkaFencedCommitter,
  type KafkaConsumerFactory,
} from "../src/kafka.js";

interface FakeFenceConsumer extends EventEmitter {
  [kGetApi](
    name: string,
    callback: (error: Error | null, api?: unknown) => void,
  ): void;
  active: boolean;
  assignments: Array<{ partitions: number[]; topic: string }> | null;
  connectToBrokers(nodeIds: number[]): Promise<Map<number, unknown>>;
  coordinatorId: number | null;
  generationId: number;
  groupId: string;
  groupInstanceId: string | null;
  isActive(): boolean;
  memberId: string | null;
}

/** Creates mutable classic-group state for assignment-fence lifecycle tests. */
function fakeFenceConsumer(): FakeFenceConsumer {
  const consumer = new EventEmitter() as FakeFenceConsumer;
  consumer.active = true;
  consumer.assignments = [{ partitions: [0, 1], topic: "avscan.action.scan" }];
  consumer.connectToBrokers = () =>
    Promise.reject(new Error("unexpected coordinator connection"));
  consumer.coordinatorId = 7;
  consumer.generationId = 3;
  consumer.groupId = "avscan-group";
  consumer.groupInstanceId = null;
  consumer.isActive = () => consumer.active;
  consumer.memberId = "member-a";
  consumer[kGetApi] = (_name, callback) => {
    callback(new Error("unexpected OffsetCommit API lookup"));
  };
  return consumer;
}

/** Creates a minimal record while retaining use of its original commit. */
function fenceMessage(
  partition: number,
  offset: bigint,
): { commits: { value: number }; message: ConsumableKafkaMessage } {
  const commits = { value: 0 };
  return {
    commits,
    message: {
      commit() {
        commits.value += 1;
        return Promise.resolve();
      },
      metadata: {
        consumer: {
          coordinatorId: 7,
          generationId: 3,
          groupId: "avscan-group",
          memberId: "member-a",
        },
      },
      offset,
      partition,
      topic: "avscan.action.scan",
      value: Buffer.from("{}"),
    },
  };
}

/** Constructs a real fence around mutable test doubles. */
function createFence(
  consumer: FakeFenceConsumer,
  commitOffset?: KafkaFencedCommitter,
  onFatalHeartbeatError?: (error: Error) => void,
): {
  fence: KafkaAssignmentFence;
  offsetsCommitted: Map<string, bigint>;
} {
  const offsetsCommitted = new Map<string, bigint>();
  const fence = new KafkaAssignmentFence(
    consumer as unknown as ConstructorParameters<
      typeof KafkaAssignmentFence
    >[0],
    { offsetsCommitted },
    commitOffset,
    onFatalHeartbeatError,
  );
  return { fence, offsetsCommitted };
}

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
  const fakeConsumer = Object.assign(new EventEmitter(), {
    /** Records setup and deliberately leaves the consume request pending. */
    consume(): Promise<never> {
      signalConsumeStarted();
      return pendingConsume;
    },
    /** Exists for the request-time fetch fence but is never called in setup. */
    fetch(): Promise<never> {
      return Promise.reject(new Error("fetch must not start during setup"));
    },
    /** Records the setup-time client close requested by stopIntake. */
    close(): Promise<void> {
      closeCalls += 1;
      return Promise.resolve();
    },
  }) as unknown as ReturnType<KafkaConsumerFactory>;
  let constructedOptions: Parameters<KafkaConsumerFactory>[0] | undefined;
  const consumerFactory: KafkaConsumerFactory = (options) => {
    constructedOptions = options;
    return fakeConsumer;
  };
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
  assert.equal(runner.isReady(), false);
  await runner.stopIntake();
  await withTimeout(run, 100, "Kafka setup stop");
  await withTimeout(runner.close(), 100, "Kafka setup close");

  assert.equal(closeCalls, 1);
  assert.ok(constructedOptions);
  assert.ok("protocols" in constructedOptions);
  assert.deepEqual(constructedOptions.protocols, [
    { name: "DefaultAssignmentStrategy", version: 0 },
  ]);
});

void test("KafkaConsumerRunner routes setup-time client errors through cleanup", async () => {
  let signalConsumeStarted: () => void = () => undefined;
  const consumeStarted = new Promise<void>((resolve) => {
    signalConsumeStarted = resolve;
  });
  let closeCalls = 0;
  const fakeConsumer = Object.assign(new EventEmitter(), {
    /** Records setup and deliberately leaves the consume request pending. */
    consume(): Promise<never> {
      signalConsumeStarted();
      return new Promise<never>(() => undefined);
    },
    /** Exists for the request-time fetch fence but is not used before failure. */
    fetch(): Promise<never> {
      return Promise.reject(new Error("fetch must not start during setup"));
    },
    /** Records controlled cleanup after the client-level failure. */
    close(): Promise<void> {
      closeCalls += 1;
      return Promise.resolve();
    },
  }) as unknown as ReturnType<KafkaConsumerFactory>;
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
    () => fakeConsumer,
  );
  const clientFailure = new Error("consumer coordinator failed");

  const run = runner.run();
  await consumeStarted;
  fakeConsumer.emit("error", clientFailure);

  await assert.rejects(run, clientFailure);
  await runner.close();
  assert.equal(closeCalls, 1);
  assert.equal(fakeConsumer.listenerCount("error"), 0);
});

void test("fetch fencing discards a response completed under a newer membership", async () => {
  const fetchResponse: fetchV17.FetchResponse = {
    errorCode: 0,
    responses: [{ partitions: [], topicId: "topic-id" }],
    sessionId: 1,
    throttleTimeMs: 0,
  };
  let finishFetch:
    | ((error: Error | null, response?: typeof fetchResponse) => void)
    | undefined;
  let discardCount = 0;
  const consumer = {
    coordinatorId: 7,
    fetch(
      _options: unknown,
      callback?: (error: Error | null, response?: typeof fetchResponse) => void,
    ): Promise<typeof fetchResponse> | void {
      if (!callback) {
        return Promise.resolve(fetchResponse);
      }
      finishFetch = callback;
    },
    generationId: 3,
    groupId: "avscan-group",
    isActive: () => true,
    memberId: "member-a" as string | null,
  } as unknown as Parameters<typeof fenceKafkaFetchResponses>[0];
  fenceKafkaFetchResponses(consumer, () => {
    discardCount += 1;
  });

  const delivered = new Promise<typeof fetchResponse>((resolve, reject) => {
    consumer.fetch({} as never, (error, response) => {
      if (error || !response) {
        reject(error ?? new Error("missing Fetch response"));
        return;
      }
      resolve(response);
    });
  });
  consumer.generationId += 1;
  finishFetch?.(null, fetchResponse);

  const staleResponse = await delivered;
  assert.deepEqual(staleResponse.responses, []);
  assert.equal(fetchResponse.responses.length, 1);
  assert.equal(discardCount, 1);

  const currentResponse = await consumer.fetch({} as never);
  assert.equal(currentResponse, fetchResponse);
  assert.equal(discardCount, 1);
});

void test("fetch fencing discards a request started before assignment activation", async () => {
  const fetchResponse: fetchV17.FetchResponse = {
    errorCode: 0,
    responses: [{ partitions: [], topicId: "topic-id" }],
    sessionId: 1,
    throttleTimeMs: 0,
  };
  let active = false;
  let finishFetch: ((response: typeof fetchResponse) => void) | undefined;
  const pendingFetch = new Promise<typeof fetchResponse>((resolve) => {
    finishFetch = resolve;
  });
  let discardCount = 0;
  const consumer = {
    coordinatorId: 7,
    fetch(): Promise<typeof fetchResponse> {
      return pendingFetch;
    },
    generationId: 4,
    groupId: "avscan-group",
    isActive: () => active,
    memberId: "member-b",
  } as unknown as Parameters<typeof fenceKafkaFetchResponses>[0];
  fenceKafkaFetchResponses(consumer, () => {
    discardCount += 1;
  });

  const delivered = consumer.fetch({} as never);
  active = true;
  finishFetch?.(fetchResponse);

  const staleResponse = await delivered;
  assert.deepEqual(staleResponse.responses, []);
  assert.equal(fetchResponse.responses.length, 1);
  assert.equal(discardCount, 1);
});

void test("patched Platformatic serializes a group join behind its initial offset refresh", async () => {
  const topic = "initial-refresh-race";
  const consumer = new Consumer({
    bootstrapBrokers: ["localhost:1"],
    clientId: "initial-refresh-race-consumer",
    groupId: "initial-refresh-race-group",
    protocols: [{ name: "DefaultAssignmentStrategy", version: 0 }],
  });
  consumer.assignments = [{ partitions: [0], topic }];

  let signalInitialLookupStarted: () => void = () => undefined;
  const initialLookupStarted = new Promise<void>((resolve) => {
    signalInitialLookupStarted = resolve;
  });
  let releaseInitialLookup: () => void = () => undefined;
  const initialLookupGate = new Promise<void>((resolve) => {
    releaseInitialLookup = resolve;
  });
  let signalJoinLookupCompleted: () => void = () => undefined;
  const joinLookupCompleted = new Promise<void>((resolve) => {
    signalJoinLookupCompleted = resolve;
  });
  let committedLookups = 0;
  consumer.listOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    callback(null, new Map([[topic, [0n]]]));
  }) as typeof consumer.listOffsets;
  consumer.listCommittedOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    committedLookups += 1;
    if (committedLookups === 1) {
      signalInitialLookupStarted();
      void initialLookupGate.then(() => {
        callback(null, new Map([[topic, [10n]]]));
      });
      return;
    }
    callback(null, new Map([[topic, [200n]]]));
    signalJoinLookupCompleted();
  }) as typeof consumer.listCommittedOffsets;
  consumer.metadata = ((
    _options: unknown,
    callback: (error: Error | null, metadata?: unknown) => void,
  ) => {
    callback(null, {
      topics: new Map([
        [
          topic,
          {
            id: "topic-id",
            partitions: [{ leader: 1, leaderEpoch: 1 }],
          },
        ],
      ]),
    });
  }) as typeof consumer.metadata;
  let resolveFetch: (offset: bigint) => void = () => undefined;
  const fetched = new Promise<bigint>((resolve) => {
    resolveFetch = resolve;
  });
  consumer.fetch = ((
    options: {
      topics: Array<{
        partitions: Array<{ fetchOffset: bigint }>;
      }>;
    },
    callback: (error: Error | null, response?: fetchV17.FetchResponse) => void,
  ) => {
    const offset = options.topics[0]?.partitions[0]?.fetchOffset;
    if (offset !== undefined) {
      resolveFetch(offset);
    }
    callback(null, {
      errorCode: 0,
      responses: [],
      sessionId: 0,
      throttleTimeMs: 0,
    });
  }) as unknown as typeof consumer.fetch;

  const stream = new MessagesStream(consumer, {
    autocommit: false,
    fallbackMode: "earliest",
    highWaterMark: 1,
    maxBytes: 1024,
    maxFetches: 1,
    maxWaitTime: 100,
    mode: "committed",
    topics: [topic],
  });

  try {
    stream.read(0);
    await initialLookupStarted;
    stream.pause();

    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 2,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(
      committedLookups,
      1,
      "the join refresh must wait for the initial refresh",
    );

    releaseInitialLookup();
    await withTimeout(joinLookupCompleted, 100, "join offset refresh");
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(stream.offsetsToFetch.get(`${topic}:0`), 200n);

    stream.resume();
    assert.equal(
      await withTimeout(fetched, 100, "serialized initial offset refresh"),
      200n,
    );
  } finally {
    releaseInitialLookup();
    await stream.close();
    await consumer.close();
  }
});

void test("patched Platformatic fails startup when a pending nonfatal refresh errors", async () => {
  const topic = "initial-refresh-error";
  const consumer = new Consumer({
    bootstrapBrokers: ["localhost:1"],
    clientId: "initial-refresh-error-consumer",
    groupId: "initial-refresh-error-group",
    protocols: [{ name: "DefaultAssignmentStrategy", version: 0 }],
  });
  consumer.assignments = [{ partitions: [0], topic }];

  let signalInitialLookupStarted: () => void = () => undefined;
  const initialLookupStarted = new Promise<void>((resolve) => {
    signalInitialLookupStarted = resolve;
  });
  let releaseInitialLookup: () => void = () => undefined;
  const initialLookupGate = new Promise<void>((resolve) => {
    releaseInitialLookup = resolve;
  });
  let committedLookups = 0;
  consumer.listOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    callback(null, new Map([[topic, [0n]]]));
  }) as typeof consumer.listOffsets;
  consumer.listCommittedOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    committedLookups += 1;
    if (committedLookups === 1) {
      signalInitialLookupStarted();
      void initialLookupGate.then(() => {
        callback(null, new Map([[topic, [10n]]]));
      });
      return;
    }
    callback(new Error("replacement offset refresh failed"));
  }) as typeof consumer.listCommittedOffsets;

  const stream = new MessagesStream(consumer, {
    autocommit: false,
    fallbackMode: "earliest",
    highWaterMark: 1,
    maxBytes: 1024,
    maxFetches: 1,
    maxWaitTime: 100,
    mode: "committed",
    topics: [topic],
  });
  const streamError = new Promise<Error>((resolve) => {
    stream.once("error", (error: Error) => resolve(error));
  });
  const streamClosed = new Promise<void>((resolve) => {
    stream.once("close", resolve);
  });

  try {
    stream.read(0);
    await initialLookupStarted;

    const refreshOffsetsAndFetch = Object.getOwnPropertySymbols(
      MessagesStream.prototype,
    ).find(
      (symbol) =>
        symbol.description ===
        "plt.kafka.messagesStream.refreshOffsetsAndFetch",
    );
    assert.ok(refreshOffsetsAndFetch);
    const refreshPendingOffsets = (
      stream as unknown as {
        [key: symbol]: () => void;
      }
    )[refreshOffsetsAndFetch];
    assert.ok(refreshPendingOffsets);
    refreshPendingOffsets.call(stream);
    assert.equal(
      committedLookups,
      1,
      "the nonfatal assignment refresh must be queued behind construction",
    );

    releaseInitialLookup();
    const error = await withTimeout(
      streamError,
      100,
      "replacement offset refresh failure",
    );
    assert.equal(error.message, "replacement offset refresh failed");
    await withTimeout(streamClosed, 100, "stream close after refresh failure");
    assert.equal(committedLookups, 2);
    assert.equal(stream.destroyed, true);
  } finally {
    releaseInitialLookup();
    await stream.close();
    await consumer.close();
  }
});

void test("patched Platformatic close skips a queued initial offset refresh", async () => {
  const topic = "initial-refresh-close";
  const consumer = new Consumer({
    bootstrapBrokers: ["localhost:1"],
    clientId: "initial-refresh-close-consumer",
    groupId: "initial-refresh-close-group",
    protocols: [{ name: "DefaultAssignmentStrategy", version: 0 }],
  });
  consumer.assignments = [{ partitions: [0], topic }];

  let signalInitialLookupStarted: () => void = () => undefined;
  const initialLookupStarted = new Promise<void>((resolve) => {
    signalInitialLookupStarted = resolve;
  });
  let releaseInitialLookup: () => void = () => undefined;
  const initialLookupGate = new Promise<void>((resolve) => {
    releaseInitialLookup = resolve;
  });
  let releaseQueuedLookup: () => void = () => undefined;
  const queuedLookupGate = new Promise<void>((resolve) => {
    releaseQueuedLookup = resolve;
  });
  let committedLookups = 0;
  consumer.listOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    callback(null, new Map([[topic, [0n]]]));
  }) as typeof consumer.listOffsets;
  consumer.listCommittedOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    committedLookups += 1;
    if (committedLookups === 1) {
      signalInitialLookupStarted();
      void initialLookupGate.then(() => {
        callback(null, new Map([[topic, [10n]]]));
      });
      return;
    }
    void queuedLookupGate.then(() => {
      callback(null, new Map([[topic, [200n]]]));
    });
  }) as typeof consumer.listCommittedOffsets;

  const stream = new MessagesStream(consumer, {
    autocommit: false,
    fallbackMode: "earliest",
    highWaterMark: 1,
    maxBytes: 1024,
    maxFetches: 1,
    maxWaitTime: 100,
    mode: "committed",
    topics: [topic],
  });
  let closePromise: Promise<void> | undefined;

  try {
    stream.read(0);
    await initialLookupStarted;

    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 2,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(
      committedLookups,
      1,
      "the join refresh must be queued behind construction",
    );

    closePromise = stream.close();
    releaseInitialLookup();
    await withTimeout(closePromise, 100, "close during initial offset refresh");
    assert.equal(
      committedLookups,
      1,
      "closing must discard the queued refresh",
    );
  } finally {
    releaseInitialLookup();
    releaseQueuedLookup();
    await (closePromise ?? stream.close());
    await consumer.close();
  }
});

void test("patched Platformatic close does not wait for a queued offset refresh", async () => {
  const topic = "queued-refresh-close";
  const consumer = new Consumer({
    bootstrapBrokers: ["localhost:1"],
    clientId: "queued-refresh-close-consumer",
    groupId: "queued-refresh-close-group",
    protocols: [{ name: "DefaultAssignmentStrategy", version: 0 }],
  });
  consumer.assignments = [{ partitions: [0], topic }];

  let signalInitialLookupStarted: () => void = () => undefined;
  const initialLookupStarted = new Promise<void>((resolve) => {
    signalInitialLookupStarted = resolve;
  });
  let releaseInitialLookup: () => void = () => undefined;
  const initialLookupGate = new Promise<void>((resolve) => {
    releaseInitialLookup = resolve;
  });
  let signalQueuedLookupStarted: () => void = () => undefined;
  const queuedLookupStarted = new Promise<void>((resolve) => {
    signalQueuedLookupStarted = resolve;
  });
  let releaseQueuedLookup: () => void = () => undefined;
  const queuedLookupGate = new Promise<void>((resolve) => {
    releaseQueuedLookup = resolve;
  });
  let committedLookups = 0;
  consumer.listOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    callback(null, new Map([[topic, [0n]]]));
  }) as typeof consumer.listOffsets;
  consumer.listCommittedOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    committedLookups += 1;
    if (committedLookups === 1) {
      signalInitialLookupStarted();
      void initialLookupGate.then(() => {
        callback(null, new Map([[topic, [10n]]]));
      });
      return;
    }
    signalQueuedLookupStarted();
    void queuedLookupGate.then(() => {
      callback(null, new Map([[topic, [200n]]]));
    });
  }) as typeof consumer.listCommittedOffsets;

  const stream = new MessagesStream(consumer, {
    autocommit: false,
    fallbackMode: "earliest",
    highWaterMark: 1,
    maxBytes: 1024,
    maxFetches: 1,
    maxWaitTime: 100,
    mode: "committed",
    topics: [topic],
  });
  let closePromise: Promise<void> | undefined;

  try {
    stream.read(0);
    await initialLookupStarted;

    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 2,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(
      committedLookups,
      1,
      "the join refresh must be queued behind construction",
    );

    releaseInitialLookup();
    await withTimeout(queuedLookupStarted, 100, "queued offset refresh");
    assert.equal(committedLookups, 2);

    closePromise = stream.close();
    await withTimeout(closePromise, 100, "close during queued offset refresh");
    assert.equal(
      committedLookups,
      2,
      "closing must not start another offset refresh",
    );
  } finally {
    releaseInitialLookup();
    releaseQueuedLookup();
    await (closePromise ?? stream.close());
    await consumer.close();
  }
});

void test("patched Platformatic drops a pending offset refresh replay after close", async () => {
  const topic = "pending-refresh-close";
  const consumer = new Consumer({
    bootstrapBrokers: ["localhost:1"],
    clientId: "pending-refresh-close-consumer",
    groupId: "pending-refresh-close-group",
    protocols: [{ name: "DefaultAssignmentStrategy", version: 0 }],
  });
  consumer.assignments = [{ partitions: [0], topic }];

  let signalInitialLookupStarted: () => void = () => undefined;
  const initialLookupStarted = new Promise<void>((resolve) => {
    signalInitialLookupStarted = resolve;
  });
  let releaseInitialLookup: () => void = () => undefined;
  const initialLookupGate = new Promise<void>((resolve) => {
    releaseInitialLookup = resolve;
  });
  let signalSecondLookupStarted: () => void = () => undefined;
  const secondLookupStarted = new Promise<void>((resolve) => {
    signalSecondLookupStarted = resolve;
  });
  let releaseSecondLookup: () => void = () => undefined;
  const secondLookupGate = new Promise<void>((resolve) => {
    releaseSecondLookup = resolve;
  });
  let committedLookups = 0;
  consumer.listOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    callback(null, new Map([[topic, [0n]]]));
  }) as typeof consumer.listOffsets;
  consumer.listCommittedOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    committedLookups += 1;
    if (committedLookups === 1) {
      signalInitialLookupStarted();
      void initialLookupGate.then(() => {
        callback(null, new Map([[topic, [10n]]]));
      });
      return;
    }
    if (committedLookups === 2) {
      signalSecondLookupStarted();
      void secondLookupGate.then(() => {
        callback(null, new Map([[topic, [200n]]]));
      });
      return;
    }
    callback(null, new Map([[topic, [300n]]]));
  }) as typeof consumer.listCommittedOffsets;

  const stream = new MessagesStream(consumer, {
    autocommit: false,
    fallbackMode: "earliest",
    highWaterMark: 1,
    maxBytes: 1024,
    maxFetches: 1,
    maxWaitTime: 100,
    mode: "committed",
    topics: [topic],
  });
  let closePromise: Promise<void> | undefined;

  try {
    stream.read(0);
    await initialLookupStarted;

    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 2,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    assert.equal(committedLookups, 1);

    releaseInitialLookup();
    await withTimeout(secondLookupStarted, 100, "second offset refresh");
    assert.equal(committedLookups, 2);

    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 3,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(
      committedLookups,
      2,
      "the third refresh must be queued behind the second",
    );

    closePromise = stream.close();
    await withTimeout(closePromise, 100, "close before pending refresh replay");

    releaseSecondLookup();
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(
      committedLookups,
      2,
      "closing must discard the pending refresh replay",
    );
  } finally {
    releaseInitialLookup();
    releaseSecondLookup();
    await (closePromise ?? stream.close());
    await consumer.close();
  }
});

void test("patched Platformatic refreshes offsets after overlapping group joins", async () => {
  const topic = "refresh-race";
  const consumer = new Consumer({
    bootstrapBrokers: ["localhost:1"],
    clientId: "refresh-race-consumer",
    groupId: "refresh-race-group",
    protocols: [{ name: "DefaultAssignmentStrategy", version: 0 }],
  });
  consumer.assignments = [{ partitions: [0], topic }];

  const committedLookups: Array<
    Array<{ partitions: number[]; topic: string }>
  > = [];
  let releaseDelayedLookup: () => void = () => undefined;
  const delayedLookup = new Promise<void>((resolve) => {
    releaseDelayedLookup = resolve;
  });
  consumer.listOffsets = ((
    _options: unknown,
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    callback(null, new Map([[topic, [0n, 0n]]]));
  }) as typeof consumer.listOffsets;
  consumer.listCommittedOffsets = ((
    options: { topics: Array<{ partitions: number[]; topic: string }> },
    callback: (error: Error | null, offsets?: Map<string, bigint[]>) => void,
  ) => {
    committedLookups.push(structuredClone(options.topics));
    if (committedLookups.length === 1) {
      callback(null, new Map([[topic, [10n, -1n]]]));
      return;
    }
    if (committedLookups.length === 2) {
      void delayedLookup.then(() => {
        callback(null, new Map([[topic, [11n, -1n]]]));
      });
      return;
    }
    callback(null, new Map([[topic, [-1n, 200n]]]));
  }) as typeof consumer.listCommittedOffsets;
  consumer.metadata = ((
    _options: unknown,
    callback: (error: Error | null, metadata?: unknown) => void,
  ) => {
    callback(null, {
      topics: new Map([
        [
          topic,
          {
            id: "topic-id",
            partitions: [
              { leader: 1, leaderEpoch: 1 },
              { leader: 1, leaderEpoch: 1 },
            ],
          },
        ],
      ]),
    });
  }) as typeof consumer.metadata;
  let resolveFetch: (
    request: Array<{ offset: bigint; partition: number }>,
  ) => void = () => undefined;
  const fetched = new Promise<Array<{ offset: bigint; partition: number }>>(
    (resolve) => {
      resolveFetch = resolve;
    },
  );
  consumer.fetch = ((
    options: {
      topics: Array<{
        partitions: Array<{ fetchOffset: bigint; partition: number }>;
      }>;
    },
    callback: (error: Error | null, response?: fetchV17.FetchResponse) => void,
  ) => {
    resolveFetch(
      options.topics.flatMap((entry) =>
        entry.partitions.map((partition) => ({
          offset: partition.fetchOffset,
          partition: partition.partition,
        })),
      ),
    );
    callback(null, {
      errorCode: 0,
      responses: [],
      sessionId: 0,
      throttleTimeMs: 0,
    });
  }) as unknown as typeof consumer.fetch;

  const stream = new MessagesStream(consumer, {
    autocommit: false,
    fallbackMode: "earliest",
    highWaterMark: 1,
    maxBytes: 1024,
    maxFetches: 1,
    maxWaitTime: 100,
    mode: "committed",
    topics: [topic],
  });

  try {
    stream.read(0);
    while (committedLookups.length < 1) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    stream.pause();

    consumer.assignments = [{ partitions: [0], topic }];
    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 2,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    while (committedLookups.length < 2) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }

    consumer.assignments = [{ partitions: [1], topic }];
    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: 3,
      groupId: consumer.groupId,
      memberId: "member-a",
    });
    stream.resume();
    releaseDelayedLookup();

    const fetchRequest = await withTimeout(
      fetched,
      100,
      "patched offset refresh",
    );
    assert.deepEqual(committedLookups, [
      [{ partitions: [0], topic }],
      [{ partitions: [0], topic }],
      [{ partitions: [1], topic }],
    ]);
    assert.deepEqual(fetchRequest, [{ offset: 200n, partition: 1 }]);
  } finally {
    await stream.close();
    await consumer.close();
  }
});

void test("assignment fencing prevents revoked queued records from starting", async () => {
  const consumer = fakeFenceConsumer();
  const { fence } = createFence(consumer, () => Promise.resolve());
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
  const messages = [fenceMessage(0, 0n).message, fenceMessage(1, 0n).message];

  const run = processMessageStream(
    Readable.from(messages),
    handler,
    1,
    undefined,
    fence,
  );
  while (handled.length === 0) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  consumer.assignments = [{ partitions: [0], topic: "avscan.action.scan" }];
  consumer.emit("consumer:group:rebalance", { groupId: consumer.groupId });
  releaseActive();

  await assert.rejects(run, StaleKafkaAssignmentError);
  fence.dispose();
  assert.deepEqual(handled, [0n]);
});

void test("assignment fencing rejects a buffered old-generation record", () => {
  const consumer = fakeFenceConsumer();
  consumer.generationId = 4;
  const { fence } = createFence(consumer, () => Promise.resolve());
  const { commits, message } = fenceMessage(0, 5n);

  assert.throws(() => fence.capture(message), StaleKafkaAssignmentError);

  fence.dispose();
  assert.equal(commits.value, 0);
});

void test("assignment fencing rejects an in-flight commit after rebalance", async () => {
  const consumer = fakeFenceConsumer();
  let fencedCommitCalls = 0;
  const { fence } = createFence(consumer, () => {
    fencedCommitCalls += 1;
    return Promise.resolve();
  });
  const { commits, message } = fenceMessage(0, 9n);
  let signalHandlerStarted: () => void = () => undefined;
  const handlerStarted = new Promise<void>((resolve) => {
    signalHandlerStarted = resolve;
  });
  let releaseHandler: () => void = () => undefined;
  const handlerGate = new Promise<void>((resolve) => {
    releaseHandler = resolve;
  });
  const handler: ConsumableKafkaMessageHandler = {
    async handle(record) {
      signalHandlerStarted();
      await handlerGate;
      await record.commit();
    },
  };

  const run = processMessageStream(
    Readable.from([message]),
    handler,
    1,
    undefined,
    fence,
  );
  await handlerStarted;
  consumer.generationId += 1;
  consumer.emit("consumer:group:join", {
    assignments: consumer.assignments,
    generationId: consumer.generationId,
    groupId: consumer.groupId,
    memberId: consumer.memberId,
  });
  releaseHandler();

  await assert.rejects(run, StaleKafkaAssignmentError);
  fence.dispose();
  assert.equal(fencedCommitCalls, 0);
  assert.equal(commits.value, 0);
});

void test("assignment fencing preserves a broker-accepted commit across local invalidation", async () => {
  const consumer = fakeFenceConsumer();
  const { fence, offsetsCommitted } = createFence(consumer, () => {
    consumer.generationId += 1;
    consumer.emit("consumer:group:join", {
      assignments: consumer.assignments,
      generationId: consumer.generationId,
      groupId: consumer.groupId,
      memberId: consumer.memberId,
    });
    return Promise.resolve();
  });
  const { message } = fenceMessage(0, 10n);
  const fenced = fence.capture(message);

  await fenced.message.commit();

  assert.equal(offsetsCommitted.get("avscan.action.scan:0"), 11n);
  fence.dispose();
});

void test("assignment fencing ignores stream-pool broker disconnects", async () => {
  const consumer = fakeFenceConsumer();
  let commitCalls = 0;
  const { fence } = createFence(consumer, () => {
    commitCalls += 1;
    return Promise.resolve();
  });
  const { message } = fenceMessage(0, 14n);
  const fenced = fence.capture(message);

  consumer.emit("client:broker:disconnect", { broker: { nodeId: 7 } });
  await fenced.message.commit();

  assert.equal(commitCalls, 1);
  fence.dispose();
});

void test("assignment fencing restarts on a non-rejoin heartbeat failure", () => {
  const consumer = fakeFenceConsumer();
  const fatalErrors: Error[] = [];
  const { fence } = createFence(
    consumer,
    () => Promise.resolve(),
    (error) => fatalErrors.push(error),
  );
  const heartbeatError = new Error("heartbeat transport failed");

  consumerHeartbeatChannel.error.publish({
    client: consumer,
    error: heartbeatError,
  });

  assert.deepEqual(fatalErrors, [heartbeatError]);
  assert.throws(
    () => fence.capture(fenceMessage(0, 15n).message),
    StaleKafkaAssignmentError,
  );
  fence.dispose();
});

void test("assignment fencing lets group heartbeat failures rejoin", () => {
  const consumer = fakeFenceConsumer();
  const fatalErrors: Error[] = [];
  const { fence } = createFence(
    consumer,
    () => Promise.resolve(),
    (error) => fatalErrors.push(error),
  );
  const heartbeatError = Object.assign(new Error("rebalance in progress"), {
    findBy(
      property: string,
      value: unknown,
    ): { needsRejoin: true } | undefined {
      return property === "needsRejoin" && value === true
        ? { needsRejoin: true }
        : undefined;
    },
  });

  consumerHeartbeatChannel.error.publish({
    client: consumer,
    error: heartbeatError,
  });

  assert.deepEqual(fatalErrors, []);
  assert.throws(
    () => fence.capture(fenceMessage(0, 16n).message),
    StaleKafkaAssignmentError,
  );
  fence.dispose();
});

void test("assignment fencing commits once with the captured generation", async () => {
  const consumer = fakeFenceConsumer();
  const connection = {};
  const requests: unknown[][] = [];
  const commitResult: { error?: Error } = {};
  consumer.connectToBrokers = (nodeIds) => {
    assert.deepEqual(nodeIds, [7]);
    return Promise.resolve(new Map([[7, connection]]));
  };
  consumer[kGetApi] = (name, callback) => {
    assert.equal(name, "OffsetCommit");
    const api = (...args: unknown[]): void => {
      const response = args.at(-1);
      assert.equal(typeof response, "function");
      requests.push(args.slice(0, -1));
      (response as (error: Error | null) => void)(commitResult.error ?? null);
    };
    callback(null, api);
  };
  const { fence, offsetsCommitted } = createFence(consumer);
  const first = fenceMessage(0, 12n);
  const firstFenced = fence.capture(first.message);

  await firstFenced.message.commit();

  assert.equal(first.commits.value, 0);
  assert.equal(requests.length, 1);
  assert.equal(requests[0]?.[0], connection);
  assert.equal(requests[0]?.[1], "avscan-group");
  assert.equal(requests[0]?.[2], 3);
  assert.equal(requests[0]?.[3], "member-a");
  assert.equal(requests[0]?.[4], null);
  assert.deepEqual(requests[0]?.[5], [
    {
      name: "avscan.action.scan",
      partitions: [
        {
          committedLeaderEpoch: -1,
          committedMetadata: null,
          committedOffset: 13n,
          partitionIndex: 0,
        },
      ],
    },
  ]);
  assert.equal(offsetsCommitted.get("avscan.action.scan:0"), 13n);

  commitResult.error = new Error("stale generation rejected by broker");
  const second = fenceMessage(0, 13n);
  const secondFenced = fence.capture(second.message);
  await assert.rejects(
    Promise.resolve(secondFenced.message.commit()),
    /stale generation rejected by broker/,
  );

  fence.dispose();
  assert.equal(requests.length, 2);
  assert.equal(second.commits.value, 0);
  assert.equal(offsetsCommitted.get("avscan.action.scan:0"), 13n);
});
