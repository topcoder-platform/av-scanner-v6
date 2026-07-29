import {
  type CallbackWithPromise,
  Consumer,
  consumerHeartbeatChannel,
  type FetchOptions,
  type fetchV17,
  kGetApi,
  type API,
  type Connection,
  type ConsumerOptions,
  type MessagesStream,
} from "@platformatic/kafka";
import type { Logger } from "winston";

import type { AppConfig } from "./config.js";
import { InvalidScanEventError } from "./errors.js";
import { Semaphore } from "./semaphore.js";
import type { ProcessedScan } from "./types.js";
import { validateScanEvent } from "./validation.js";

type PlatformaticStream = MessagesStream<Buffer, Buffer, Buffer, Buffer>;
type PlatformaticConsumer = Consumer<Buffer, Buffer, Buffer, Buffer>;
type PlatformaticFetchOptions = FetchOptions<Buffer, Buffer, Buffer, Buffer>;
type PlatformaticFetchResponse = fetchV17.FetchResponse;
const QUEUED_MESSAGES_PER_WORKER = 32;
const LEGACY_GROUP_PROTOCOL = {
  name: "DefaultAssignmentStrategy",
  version: 0,
};

/** Factory boundary used to construct the Platformatic consumer. */
export type KafkaConsumerFactory = (
  options: ConsumerOptions<Buffer, Buffer, Buffer, Buffer>,
) => PlatformaticConsumer;

/** Minimal Platformatic record fields used by the scanner message handler. */
export interface ConsumableKafkaMessage {
  /**
   * Commits this record's next group offset after all retryable work succeeds.
   *
   * @returns A promise when Platformatic performs an asynchronous commit.
   * @throws When the group offset cannot be committed.
   */
  commit(): Promise<void> | void;
  /** Platformatic fetch-time membership metadata, validated by the fence. */
  metadata: Record<string, unknown>;
  offset: bigint;
  partition: number;
  topic: string;
  /** Decoded record bytes, or undefined when Kafka supplies a tombstone. */
  value: Buffer | undefined;
}

/** Membership identity captured when a record enters the local scheduler. */
export interface KafkaAssignmentSnapshot {
  coordinatorId: number;
  epoch: number;
  generationId: number;
  groupId: string;
  memberId: string;
  partition: number;
  topic: string;
}

interface KafkaFetchedAssignment {
  coordinatorId: number;
  generationId: number;
  groupId: string;
  memberId: string;
}

interface KafkaFetchMembership {
  active: boolean;
  coordinatorId: number | null;
  generationId: number;
  groupId: string;
  memberId: string | null;
}

interface KafkaFetchConsumer {
  coordinatorId: number | null;
  fetch(
    options: PlatformaticFetchOptions,
    callback: CallbackWithPromise<PlatformaticFetchResponse>,
  ): void;
  fetch(options: PlatformaticFetchOptions): Promise<PlatformaticFetchResponse>;
  generationId: number;
  groupId: string;
  isActive(): boolean;
  memberId: string | null;
}

/**
 * Discards Fetch responses whose request began under an older membership.
 *
 * Platformatic 1.34 records consumer metadata when it pushes a response, not
 * when it sends the Fetch request. A response from generation N can therefore
 * arrive after generation N+1 has refreshed its committed offsets and be
 * mislabeled as current. A request can also start after JoinGroup supplies the
 * new identity but before SyncGroup activates its assignment. Returning the
 * same response envelope with no topic responses makes MessagesStream schedule
 * a fresh Fetch without advancing its local offsets or admitting stale records.
 *
 * @param consumer - Consumer whose Fetch boundary should be fenced.
 * @param onDiscard - Optional observability hook for a stale successful Fetch.
 */
export function fenceKafkaFetchResponses(
  consumer: KafkaFetchConsumer,
  onDiscard: () => void = () => undefined,
): void {
  const originalFetch = consumer.fetch.bind(consumer);
  const captureMembership = (): KafkaFetchMembership => ({
    active: consumer.isActive(),
    coordinatorId: consumer.coordinatorId,
    generationId: consumer.generationId,
    groupId: consumer.groupId,
    memberId: consumer.memberId,
  });
  const isCurrent = (snapshot: KafkaFetchMembership): boolean =>
    snapshot.active &&
    consumer.isActive() &&
    consumer.coordinatorId === snapshot.coordinatorId &&
    consumer.generationId === snapshot.generationId &&
    consumer.groupId === snapshot.groupId &&
    consumer.memberId === snapshot.memberId;
  const discardIfStale = (
    snapshot: KafkaFetchMembership,
    response: PlatformaticFetchResponse,
  ): PlatformaticFetchResponse => {
    if (isCurrent(snapshot)) {
      return response;
    }
    onDiscard();
    return { ...response, responses: [] };
  };

  consumer.fetch = ((
    options: PlatformaticFetchOptions,
    callback?: CallbackWithPromise<PlatformaticFetchResponse>,
  ): Promise<PlatformaticFetchResponse> | void => {
    const snapshot = captureMembership();
    if (!callback) {
      return originalFetch(options).then((response) =>
        discardIfStale(snapshot, response),
      );
    }
    originalFetch(options, (error, response) => {
      if (error || !response) {
        callback(error, response);
        return;
      }
      callback(null, discardIfStale(snapshot, response));
    });
  }) as KafkaFetchConsumer["fetch"];
}

/** A record whose assignment can be checked again immediately before work. */
export interface FencedKafkaMessage {
  assertCurrent(): void;
  message: ConsumableKafkaMessage;
}

/** Captures an assignment fence for each record admitted from Kafka. */
export interface KafkaMessageFence {
  capture(message: ConsumableKafkaMessage): FencedKafkaMessage;
}

/** One-shot offset commit boundary, injectable for focused fencing tests. */
export type KafkaFencedCommitter = (
  snapshot: KafkaAssignmentSnapshot,
  message: ConsumableKafkaMessage,
) => Promise<void>;

/** Raised when work belongs to a Kafka membership that is no longer current. */
export class StaleKafkaAssignmentError extends Error {
  constructor(snapshot: KafkaAssignmentSnapshot) {
    super(
      `Kafka assignment changed before ${snapshot.topic}:${snapshot.partition} offset processing completed`,
    );
    this.name = "StaleKafkaAssignmentError";
  }
}

interface OffsetCommitRequestTopic {
  name: string;
  partitions: Array<{
    committedLeaderEpoch: number;
    committedMetadata: null;
    committedOffset: bigint;
    partitionIndex: number;
  }>;
}

type OffsetCommitArguments = [
  groupId: string,
  generationId: number,
  memberId: string,
  groupInstanceId: string | null,
  topics: OffsetCommitRequestTopic[],
];
type OffsetCommitApi = API<OffsetCommitArguments, unknown>;

/**
 * Fences scheduled work and commits to the membership that fetched it.
 *
 * Platformatic's high-level `message.commit()` transparently rejoins and
 * retries when Kafka rejects an old generation. That is convenient for normal
 * consumers, but unsafe after this scanner has already produced external side
 * effects: the old record could then commit successfully as a new member. This
 * fence instead sends one negotiated OffsetCommit request with the captured
 * generation and member ID. Kafka rejects it if a rebalance won the race.
 */
export class KafkaAssignmentFence implements KafkaMessageFence {
  private disposed = false;
  private epoch = 0;
  private readonly commitOffset: KafkaFencedCommitter;
  private valid: boolean;

  private readonly onAssignmentInvalidated = (): void => {
    this.epoch += 1;
    this.valid = false;
  };

  private readonly onGroupJoin = (): void => {
    this.epoch += 1;
    this.valid = this.consumer.isActive();
  };

  private readonly onHeartbeatError = (context: unknown): void => {
    if (
      typeof context !== "object" ||
      context === null ||
      !("client" in context) ||
      context.client !== this.consumer
    ) {
      return;
    }

    this.onAssignmentInvalidated();
    const diagnosticError =
      "error" in context ? context.error : new Error("Kafka heartbeat failed");
    if (this.requiresGroupRejoin(diagnosticError)) {
      return;
    }
    this.onFatalHeartbeatError(
      diagnosticError instanceof Error
        ? diagnosticError
        : new Error(String(diagnosticError)),
    );
  };

  /**
   * Creates a fence for one active Platformatic stream.
   *
   * @param consumer - Consumer exposing current classic-group membership.
   * @param stream - Stream whose committed-offset metric is kept in sync.
   * @param commitOffset - Optional one-shot commit seam for unit tests.
   * @param onFatalHeartbeatError - Restarts a stream whose heartbeat cannot
   * recover through a group rejoin.
   */
  constructor(
    private readonly consumer: PlatformaticConsumer,
    private readonly stream: Pick<PlatformaticStream, "offsetsCommitted">,
    commitOffset?: KafkaFencedCommitter,
    private readonly onFatalHeartbeatError: (error: Error) => void = () =>
      undefined,
  ) {
    this.valid = consumer.isActive();
    this.commitOffset =
      commitOffset ?? this.commitOffsetForCapturedGeneration.bind(this);

    consumer.on("consumer:group:join", this.onGroupJoin);
    consumer.on("consumer:group:leave", this.onAssignmentInvalidated);
    consumer.on("consumer:group:rebalance", this.onAssignmentInvalidated);
    consumerHeartbeatChannel.error.subscribe(this.onHeartbeatError);
  }

  /**
   * Captures current membership and replaces the retrying message commit.
   *
   * @param message - Record admitted from the Platformatic stream.
   * @returns The record, a one-shot pinned commit, and a pre-work assertion.
   */
  capture(message: ConsumableKafkaMessage): FencedKafkaMessage {
    const coordinatorId = this.consumer.coordinatorId;
    const memberId = this.consumer.memberId;
    if (coordinatorId === null || memberId === null) {
      throw this.staleAssignment(message);
    }

    const snapshot: KafkaAssignmentSnapshot = {
      coordinatorId,
      epoch: this.epoch,
      generationId: this.consumer.generationId,
      groupId: this.consumer.groupId,
      memberId,
      partition: message.partition,
      topic: message.topic,
    };
    this.assertSnapshotCurrent(snapshot);
    this.assertFetchedBySnapshot(message, snapshot);

    let commitPromise: Promise<void> | undefined;
    return {
      assertCurrent: () => this.assertSnapshotCurrent(snapshot),
      message: {
        ...message,
        commit: () => {
          commitPromise ??= this.commitFenced(snapshot, message);
          return commitPromise;
        },
      },
    };
  }

  /** Removes all consumer and diagnostic listeners owned by this fence. */
  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.consumer.removeListener("consumer:group:join", this.onGroupJoin);
    this.consumer.removeListener(
      "consumer:group:leave",
      this.onAssignmentInvalidated,
    );
    this.consumer.removeListener(
      "consumer:group:rebalance",
      this.onAssignmentInvalidated,
    );
    consumerHeartbeatChannel.error.unsubscribe(this.onHeartbeatError);
  }

  /**
   * Verifies every locally observable component of the captured assignment.
   *
   * @param snapshot - Membership captured when the record was scheduled.
   * @throws StaleKafkaAssignmentError when work must not start or commit.
   */
  private assertSnapshotCurrent(snapshot: KafkaAssignmentSnapshot): void {
    const assignmentCurrent = this.consumer.assignments?.some(
      (assignment) =>
        assignment.topic === snapshot.topic &&
        assignment.partitions.includes(snapshot.partition),
    );
    if (
      this.disposed ||
      !this.valid ||
      !this.consumer.isActive() ||
      this.epoch !== snapshot.epoch ||
      this.consumer.groupId !== snapshot.groupId ||
      this.consumer.generationId !== snapshot.generationId ||
      this.consumer.memberId !== snapshot.memberId ||
      this.consumer.coordinatorId !== snapshot.coordinatorId ||
      assignmentCurrent !== true
    ) {
      throw new StaleKafkaAssignmentError(snapshot);
    }
  }

  /**
   * Rejects records buffered by Platformatic under an older membership.
   *
   * Platformatic includes the consumer identity present when it pushes a
   * fetched record into its readable buffer. A record can remain in that
   * buffer across a rejoin, so current consumer state alone is insufficient.
   *
   * @param message - Record carrying Platformatic membership metadata.
   * @param snapshot - Current membership captured by this fence.
   * @throws StaleKafkaAssignmentError for missing or mismatched identity.
   */
  private assertFetchedBySnapshot(
    message: ConsumableKafkaMessage,
    snapshot: KafkaAssignmentSnapshot,
  ): void {
    const fetched = this.readFetchedAssignment(message.metadata);
    if (
      !fetched ||
      fetched.groupId !== snapshot.groupId ||
      fetched.generationId !== snapshot.generationId ||
      fetched.memberId !== snapshot.memberId ||
      fetched.coordinatorId !== snapshot.coordinatorId
    ) {
      throw new StaleKafkaAssignmentError(snapshot);
    }
  }

  /** Parses Platformatic's otherwise open-ended message metadata record. */
  private readFetchedAssignment(
    metadata: Record<string, unknown>,
  ): KafkaFetchedAssignment | undefined {
    const consumer = metadata.consumer;
    if (typeof consumer !== "object" || consumer === null) {
      return undefined;
    }
    if (
      !("groupId" in consumer) ||
      typeof consumer.groupId !== "string" ||
      !("generationId" in consumer) ||
      typeof consumer.generationId !== "number" ||
      !("memberId" in consumer) ||
      typeof consumer.memberId !== "string" ||
      !("coordinatorId" in consumer) ||
      typeof consumer.coordinatorId !== "number"
    ) {
      return undefined;
    }
    return {
      coordinatorId: consumer.coordinatorId,
      generationId: consumer.generationId,
      groupId: consumer.groupId,
      memberId: consumer.memberId,
    };
  }

  /** Detects heartbeat failures that Platformatic will repair by rejoining. */
  private requiresGroupRejoin(error: unknown): boolean {
    if (typeof error !== "object" || error === null) {
      return false;
    }
    const candidate = error as {
      findBy?: (property: string, value: unknown) => unknown;
    };
    if (typeof candidate.findBy !== "function") {
      return false;
    }
    try {
      return Boolean(candidate.findBy("needsRejoin", true));
    } catch {
      return false;
    }
  }

  /**
   * Commits once and updates Platformatic lag state after broker acceptance.
   *
   * @param snapshot - Original membership and topic-partition identity.
   * @param message - Original record providing the next offset.
   */
  private async commitFenced(
    snapshot: KafkaAssignmentSnapshot,
    message: ConsumableKafkaMessage,
  ): Promise<void> {
    this.assertSnapshotCurrent(snapshot);
    await this.commitOffset(snapshot, message);

    const key = `${snapshot.topic}:${snapshot.partition}`;
    const nextOffset = message.offset + 1n;
    const previousOffset = this.stream.offsetsCommitted.get(key);
    if (previousOffset === undefined || previousOffset < nextOffset) {
      this.stream.offsetsCommitted.set(key, nextOffset);
    }
  }

  /**
   * Sends exactly one raw OffsetCommit request with captured membership.
   *
   * @param snapshot - Group generation and member Kafka must validate.
   * @param message - Record whose next offset becomes the group position.
   */
  private async commitOffsetForCapturedGeneration(
    snapshot: KafkaAssignmentSnapshot,
    message: ConsumableKafkaMessage,
  ): Promise<void> {
    const [api, connections] = await Promise.all([
      this.getOffsetCommitApi(),
      this.consumer.connectToBrokers([snapshot.coordinatorId]),
    ]);
    this.assertSnapshotCurrent(snapshot);

    const connection = connections.get(snapshot.coordinatorId);
    if (!connection) {
      throw new Error(
        `Kafka group coordinator ${snapshot.coordinatorId} is unavailable`,
      );
    }

    await this.sendOffsetCommit(api, connection, snapshot, message.offset + 1n);
  }

  /** Resolves the broker-negotiated OffsetCommit API without performing it. */
  private getOffsetCommitApi(): Promise<OffsetCommitApi> {
    return new Promise<OffsetCommitApi>((resolve, reject) => {
      this.consumer[kGetApi]<OffsetCommitArguments, unknown>(
        "OffsetCommit",
        (error, api) => {
          if (error) {
            reject(error);
            return;
          }
          if (!api) {
            reject(new Error("Kafka OffsetCommit API is unavailable"));
            return;
          }
          resolve(api);
        },
      );
    });
  }

  /** Performs the negotiated API callback once; no consumer retry is involved. */
  private sendOffsetCommit(
    api: OffsetCommitApi,
    connection: Connection,
    snapshot: KafkaAssignmentSnapshot,
    offset: bigint,
  ): Promise<void> {
    const topics: OffsetCommitRequestTopic[] = [
      {
        name: snapshot.topic,
        partitions: [
          {
            committedLeaderEpoch: -1,
            committedMetadata: null,
            committedOffset: offset,
            partitionIndex: snapshot.partition,
          },
        ],
      },
    ];
    return new Promise<void>((resolve, reject) => {
      api(
        connection,
        snapshot.groupId,
        snapshot.generationId,
        snapshot.memberId,
        this.consumer.groupInstanceId,
        topics,
        (error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        },
      );
    });
  }

  /** Creates a consistent stale-assignment error before a snapshot exists. */
  private staleAssignment(
    message: ConsumableKafkaMessage,
  ): StaleKafkaAssignmentError {
    return new StaleKafkaAssignmentError({
      coordinatorId: this.consumer.coordinatorId ?? -1,
      epoch: this.epoch,
      generationId: this.consumer.generationId,
      groupId: this.consumer.groupId,
      memberId: this.consumer.memberId ?? "",
      partition: message.partition,
      topic: message.topic,
    });
  }
}

/** Scan processor boundary used by KafkaMessageHandler and focused tests. */
export interface ScanEventProcessor {
  /**
   * Processes one validated scan event through all required side effects.
   *
   * @param event - Validated Topcoder antivirus event.
   * @returns The completed payload and optional post-commit cleanup.
   * @throws When the offset must remain uncommitted for retry.
   */
  process(event: ReturnType<typeof validateScanEvent>): Promise<ProcessedScan>;
}

/** Parses, validates, processes, and explicitly commits one Kafka message. */
export class KafkaMessageHandler {
  /**
   * Creates the message handler used by the partition-aware consumer loop.
   *
   * @param config - Expected topic and event-validation settings.
   * @param processor - Antivirus scan orchestrator.
   * @param logger - Structured process logger.
   */
  constructor(
    private readonly config: AppConfig,
    private readonly processor: ScanEventProcessor,
    private readonly logger: Logger,
  ) {}

  /**
   * Handles one Platformatic message with legacy discard behavior.
   * Tombstones, non-buffer values, malformed JSON, and envelope-topic
   * mismatches are intentionally committed. Deterministically invalid events
   * are also discarded and committed so they cannot poison the consumer group.
   * Operational failures propagate without a commit, stopping consumption
   * before later records in that partition run.
   *
   * @param message - Platformatic Kafka message with a manual commit callback.
   * @returns A promise resolving only after any required commit succeeds.
   * @throws When scanning, side effects, or offset commit fails.
   */
  async handle(message: ConsumableKafkaMessage): Promise<void> {
    const metadata = {
      offset: message.offset.toString(),
      partition: message.partition,
      topic: message.topic,
    };
    this.logger.info("Received antivirus scan event", metadata);

    if (!Buffer.isBuffer(message.value)) {
      this.logger.warn("Discarding Kafka tombstone or non-buffer value", {
        ...metadata,
        valueType:
          message.value === undefined ? "undefined" : typeof message.value,
      });
      await message.commit();
      return;
    }

    let decoded: unknown;
    try {
      decoded = JSON.parse(message.value.toString("utf8")) as unknown;
    } catch (error) {
      this.logger.warn("Discarding malformed Kafka JSON", {
        ...metadata,
        error: error instanceof Error ? error.message : String(error),
      });
      await message.commit();
      return;
    }

    const envelopeTopic =
      typeof decoded === "object" && decoded !== null && "topic" in decoded
        ? decoded.topic
        : undefined;
    if (envelopeTopic !== message.topic) {
      this.logger.warn("Discarding Kafka event with a mismatched topic", {
        ...metadata,
        envelopeTopic:
          typeof envelopeTopic === "string" ? envelopeTopic : undefined,
      });
      await message.commit();
      return;
    }

    let event: ReturnType<typeof validateScanEvent>;
    try {
      event = validateScanEvent(decoded, this.config);
    } catch (error) {
      if (!(error instanceof InvalidScanEventError)) {
        throw error;
      }
      this.logger.warn("Discarding invalid antivirus scan event", {
        ...metadata,
        error: error.message,
      });
      await message.commit();
      return;
    }

    const processed = await this.processor.process(event);
    await message.commit();
    if (processed.afterCommit) {
      try {
        await processed.afterCommit();
      } catch (error) {
        this.logger.error("Post-commit source cleanup failed", {
          ...metadata,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
    this.logger.info("Committed antivirus scan event", metadata);
  }
}

/** Minimal message-handler boundary used by the bounded stream scheduler. */
export interface ConsumableKafkaMessageHandler {
  /**
   * Processes one message through its explicit commit boundary.
   *
   * @param message - Kafka record to process and, when successful, commit.
   * @returns A promise resolving after processing and commit complete.
   * @throws When consumption must stop without advancing this partition.
   */
  handle(message: ConsumableKafkaMessage): Promise<void>;
}

/**
 * Consumes Kafka records with bounded parallelism and per-partition ordering.
 * A small bounded read-ahead allows records from other partitions to reach
 * available workers even when one partition has a backlog. The first handler
 * failure prevents queued work from starting and is propagated after already
 * active handlers settle. Chaining each topic-partition guarantees that later
 * offsets cannot commit past an earlier failure.
 *
 * @param messages - Platformatic message stream or compatible async iterable.
 * @param handler - Explicit-commit record handler.
 * @param concurrency - Maximum number of handlers active at once.
 * @param stopSignal - Optional graceful-stop signal. Tasks that have not begun
 * handling a record when it aborts settle without processing or committing it.
 * @param messageFence - Optional membership fence captured when each record is
 * scheduled and asserted again immediately before its handler starts.
 * @returns A promise resolving after the stream and scheduled work complete.
 * @throws The first handler/iterator failure, or RangeError for bad concurrency.
 */
export async function processMessageStream(
  messages: AsyncIterable<ConsumableKafkaMessage>,
  handler: ConsumableKafkaMessageHandler,
  concurrency: number,
  stopSignal?: AbortSignal,
  messageFence?: KafkaMessageFence,
): Promise<void> {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new RangeError("Kafka processing concurrency must be positive");
  }

  const semaphore = new Semaphore(concurrency);
  const maximumPending = concurrency * QUEUED_MESSAGES_PER_WORKER;
  const pending = new Set<Promise<void>>();
  const partitionTails = new Map<string, Promise<void>>();
  const iterator = messages[Symbol.asyncIterator]();
  let failureRecorded = false;
  let firstFailure: Error | undefined;
  let rejectFailure: (error: unknown) => void = () => undefined;
  const failureSignal = new Promise<never>((_resolve, reject) => {
    rejectFailure = reject;
  });
  // The signal is also consumed by races below; this guard covers the brief
  // interval between a handler rejecting and the loop reaching its next race.
  void failureSignal.catch(() => undefined);

  /**
   * Stores and signals the first stream or handler failure exactly once.
   *
   * @param error - Arbitrary rejected value to normalize for propagation.
   * @returns Nothing; queued tasks observe the shared failure state.
   */
  const recordFailure = (error: unknown): void => {
    if (failureRecorded) {
      return;
    }
    failureRecorded = true;
    firstFailure = error instanceof Error ? error : new Error(String(error));
    rejectFailure(firstFailure);
  };

  /**
   * Adds one record to its ordered partition lane and global work bound.
   *
   * @param message - Kafka record admitted from the async iterator.
   * @returns Nothing; completion is tracked in the pending task set.
   */
  const schedule = (message: ConsumableKafkaMessage): void => {
    const fencedMessage = messageFence?.capture(message);
    const partitionKey = `${message.topic}:${message.partition}`;
    const previous = partitionTails.get(partitionKey) ?? Promise.resolve();
    const task = previous.then(async () => {
      if (failureRecorded) {
        throw firstFailure ?? new Error("Kafka message processing failed");
      }
      if (stopSignal?.aborted) {
        return;
      }

      const release = await semaphore.acquire();
      try {
        if (failureRecorded) {
          throw firstFailure ?? new Error("Kafka message processing failed");
        }
        if (stopSignal?.aborted) {
          return;
        }
        fencedMessage?.assertCurrent();
        await handler.handle(fencedMessage?.message ?? message);
      } catch (error) {
        recordFailure(error);
        throw firstFailure ?? new Error("Kafka message processing failed");
      } finally {
        release();
      }
    });

    partitionTails.set(partitionKey, task);
    pending.add(task);
    void task.catch(recordFailure).finally(() => {
      pending.delete(task);
      if (partitionTails.get(partitionKey) === task) {
        partitionTails.delete(partitionKey);
      }
    });
  };

  try {
    while (true) {
      while (pending.size >= maximumPending) {
        await Promise.race([
          Promise.race([...pending].map((task) => task.then(() => undefined))),
          failureSignal,
        ]);
      }

      const next = await Promise.race([iterator.next(), failureSignal]);
      if (next.done) {
        break;
      }
      schedule(next.value);
    }

    await Promise.race([Promise.all([...pending]), failureSignal]);
  } catch (error) {
    recordFailure(error);
    if (iterator.return) {
      void iterator.return().catch(() => undefined);
    }
    await Promise.allSettled([...pending]);
    throw firstFailure ?? new Error("Kafka message stream failed");
  }
}

/** Platformatic Kafka consumer lifecycle for ordered, manual-offset scans. */
export class KafkaConsumerRunner {
  private consumer?: PlatformaticConsumer;
  private consumerClosePromise?: Promise<void>;
  private fullClosePromise?: Promise<void>;
  private intakeStopPromise?: Promise<void>;
  private readonly processingStopController = new AbortController();
  private resolveStopRequest: () => void = () => undefined;
  private readonly stopRequest = new Promise<void>((resolve) => {
    this.resolveStopRequest = resolve;
  });
  private stream?: PlatformaticStream;
  private streamClosePromise?: Promise<void>;
  private ready = false;
  private stopRequested = false;

  /**
   * Creates the long-running Kafka consumer.
   *
   * @param config - Broker, TLS, group, topic, and fetch settings.
   * @param handler - Per-message scan and explicit-commit handler.
   * @param processingConcurrency - Maximum records processed across partitions.
   * @param logger - Structured process logger.
   * @param consumerFactory - Platformatic client constructor, replaceable in
   * lifecycle tests without opening broker sockets.
   */
  constructor(
    private readonly config: AppConfig["kafka"],
    private readonly handler: ConsumableKafkaMessageHandler,
    private readonly processingConcurrency: number,
    private readonly logger: Logger,
    private readonly consumerFactory: KafkaConsumerFactory = (options) =>
      new Consumer(options),
  ) {}

  /**
   * Reports whether the initial Kafka consume stream has joined successfully.
   * Readiness is intentionally a one-way latch: later broker reconnects and
   * group rebalances must not make ECS churn an otherwise healthy task.
   *
   * @returns True after the first consume stream has been established.
   */
  isReady(): boolean {
    return this.ready;
  }

  /**
   * Connects to Kafka and consumes until shutdown or an uncommitted failure.
   * Platformatic retries broker operations indefinitely; handler failures leave
   * the process so ECS can restart from the last committed group offset.
   *
   * @returns A promise resolving on graceful stream closure.
   * @throws When Kafka setup, streaming, or message processing fails.
   */
  async run(): Promise<void> {
    if (this.stopRequested) {
      return;
    }

    const options: ConsumerOptions<Buffer, Buffer, Buffer, Buffer> = {
      bootstrapBrokers: this.config.brokers,
      clientId: this.config.clientId,
      groupId: this.config.groupId,
      heartbeatInterval: this.config.heartbeatIntervalMs,
      highWaterMark: 1,
      // no-kafka advertises this protocol name and metadata version. Retaining
      // them lets a rolling v6 deployment share the existing consumer group
      // with the legacy scanner until ECS drains the old task.
      protocols: [LEGACY_GROUP_PROTOCOL],
      retries: true,
      sessionTimeout: this.config.sessionTimeoutMs,
      ...(this.config.tls ? { tls: this.config.tls } : {}),
    };
    this.consumer = this.consumerFactory(options);
    fenceKafkaFetchResponses(this.consumer, () => {
      this.logger.warn(
        "Discarded stale Kafka fetch response after assignment change",
      );
    });
    if (this.stopRequested) {
      await this.closeConsumer();
      return;
    }

    const consumePromise = this.consumer.consume({
      autocommit: false,
      fallbackMode: "latest",
      maxBytes: this.config.maxBytes,
      maxWaitTime: this.config.maxWaitTimeMs,
      mode: "committed",
      topics: [this.config.topic],
    });
    const consumeOutcome = await Promise.race([
      consumePromise.then((stream) => ({ stream, type: "stream" as const })),
      this.stopRequest.then(() => ({ type: "stopped" as const })),
    ]);
    if (consumeOutcome.type === "stopped") {
      void consumePromise
        .then((stream) => this.closeLateStream(stream))
        .catch(() => undefined);
      return;
    }

    this.stream = consumeOutcome.stream;
    if (this.stopRequested) {
      await this.closeStream();
      return;
    }
    this.stream.on("error", (error) => {
      this.logger.error("Kafka consumer stream error", {
        error: error.message,
      });
    });
    const assignmentFence = new KafkaAssignmentFence(
      this.consumer,
      this.stream,
      undefined,
      (error) => this.stream?.destroy(error),
    );
    this.ready = true;

    this.logger.info("Kafka consumer started", {
      brokers: this.config.brokers,
      groupId: this.config.groupId,
      topic: this.config.topic,
    });
    try {
      await processMessageStream(
        this.stream,
        this.handler,
        this.processingConcurrency,
        this.processingStopController.signal,
        assignmentFence,
      );
    } finally {
      assignmentFence.dispose();
    }
  }

  /**
   * Stops admitting Kafka records while preserving the consumer connection for
   * commits from handlers that are already active. During connection setup,
   * where no handler can be active, the consumer itself is closed to interrupt
   * Platformatic's retry loop.
   *
   * @returns A promise resolving after record intake has stopped.
   * @throws When Platformatic cannot close an active message stream.
   */
  async stopIntake(): Promise<void> {
    this.markStopRequested();
    this.intakeStopPromise ??= this.stopCurrentIntake();
    await this.intakeStopPromise;
  }

  /**
   * Stops the current stream, or initiates setup-time consumer shutdown when no
   * handler can yet be active.
   *
   * @returns A promise resolving when an active stream stops admitting records.
   * @throws When Platformatic cannot close an active message stream.
   */
  private async stopCurrentIntake(): Promise<void> {
    if (this.stream) {
      await this.closeStream();
      return;
    }
    if (this.consumer) {
      const consumerClose = this.closeConsumer();
      void consumerClose.catch(() => undefined);
    }
  }

  /**
   * Closes the active stream and consumer after message handlers have drained.
   *
   * @returns A promise resolving after Platformatic releases its connections.
   * @throws When stream or consumer shutdown fails.
   */
  async close(): Promise<void> {
    this.markStopRequested();
    this.fullClosePromise ??= this.closeAllResources();
    await this.fullClosePromise;
  }

  /**
   * Marks graceful stop exactly once and wakes setup and scheduler waiters.
   *
   * @returns Nothing after the shared stop signals have been triggered.
   */
  private markStopRequested(): void {
    if (this.stopRequested) {
      return;
    }
    this.stopRequested = true;
    this.processingStopController.abort();
    this.resolveStopRequest();
  }

  /**
   * Releases the stream first and the consumer second, retaining both failures.
   *
   * @returns A promise resolving after every Kafka resource close attempt.
   * @throws AggregateError when either Platformatic resource cannot close.
   */
  private async closeAllResources(): Promise<void> {
    const streamResult = await Promise.allSettled([this.closeStream()]);
    const consumerResult = await Promise.allSettled([this.closeConsumer()]);
    const failures = [...streamResult, ...consumerResult]
      .filter(
        (result): result is PromiseRejectedResult =>
          result.status === "rejected",
      )
      .map((result) =>
        result.reason instanceof Error
          ? result.reason
          : new Error(String(result.reason)),
      );
    if (failures.length > 0) {
      throw new AggregateError(failures, "Kafka shutdown failed");
    }
  }

  /**
   * Closes the current Platformatic consumer at most once.
   *
   * @returns A shared promise for consumer connection shutdown.
   * @throws When Platformatic cannot close the consumer.
   */
  private closeConsumer(): Promise<void> {
    if (this.consumerClosePromise) {
      return this.consumerClosePromise;
    }
    const consumer = this.consumer;
    if (!consumer) {
      return Promise.resolve();
    }
    this.consumerClosePromise = (async () => {
      try {
        await Promise.resolve(consumer.close(true));
      } finally {
        if (this.consumer === consumer) {
          this.consumer = undefined;
        }
      }
    })();
    return this.consumerClosePromise;
  }

  /**
   * Closes the current Platformatic message stream at most once.
   *
   * @returns A shared promise for stream shutdown.
   * @throws When Platformatic cannot close the stream.
   */
  private closeStream(): Promise<void> {
    if (this.streamClosePromise) {
      return this.streamClosePromise;
    }
    const stream = this.stream;
    if (!stream) {
      return Promise.resolve();
    }
    this.streamClosePromise = (async () => {
      try {
        await stream.close();
      } finally {
        stream.removeAllListeners();
        if (this.stream === stream) {
          this.stream = undefined;
        }
      }
    })();
    return this.streamClosePromise;
  }

  /**
   * Disposes a stream that resolves after setup was already stopped.
   *
   * @param stream - Late Platformatic stream never exposed to the scheduler.
   * @returns A promise resolving after the unused stream releases connections.
   * @throws When Platformatic cannot close the late stream.
   */
  private async closeLateStream(stream: PlatformaticStream): Promise<void> {
    try {
      await stream.close();
    } finally {
      stream.removeAllListeners();
    }
  }
}
