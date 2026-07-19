import {
  Consumer,
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
const QUEUED_MESSAGES_PER_WORKER = 32;

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
  offset: bigint;
  partition: number;
  topic: string;
  /** Decoded record bytes, or undefined when Kafka supplies a tombstone. */
  value: Buffer | undefined;
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
 * @returns A promise resolving after the stream and scheduled work complete.
 * @throws The first handler/iterator failure, or RangeError for bad concurrency.
 */
export async function processMessageStream(
  messages: AsyncIterable<ConsumableKafkaMessage>,
  handler: ConsumableKafkaMessageHandler,
  concurrency: number,
  stopSignal?: AbortSignal,
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
        await handler.handle(message);
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
      retries: true,
      sessionTimeout: this.config.sessionTimeoutMs,
      ...(this.config.tls ? { tls: this.config.tls } : {}),
    };
    this.consumer = this.consumerFactory(options);
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

    this.logger.info("Kafka consumer started", {
      brokers: this.config.brokers,
      groupId: this.config.groupId,
      topic: this.config.topic,
    });
    await processMessageStream(
      this.stream,
      this.handler,
      this.processingConcurrency,
      this.processingStopController.signal,
    );
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
