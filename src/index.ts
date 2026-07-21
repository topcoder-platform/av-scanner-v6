import axios from "axios";
import { pathToFileURL } from "node:url";

import { M2MTokenProvider } from "./auth.js";
import { ClamAvClient } from "./clamav.js";
import { loadConfig } from "./config.js";
import { HealthServer } from "./health-server.js";
import { KafkaConsumerRunner, KafkaMessageHandler } from "./kafka.js";
import { createAppLogger } from "./logger.js";
import { ScanResultHandler } from "./result-handler.js";
import { S3ObjectStore } from "./s3.js";
import { ScanProcessor } from "./scan-processor.js";

const KAFKA_DRAIN_TIMEOUT_MS = 100_000;
const RESOURCE_CLOSE_TIMEOUT_MS = 10_000;

/** Converts arbitrary rejected values into errors safe to aggregate and throw. */
function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Bounds an asynchronous lifecycle operation without hiding its own failure.
 *
 * @param operation - Already-started operation to await.
 * @param timeoutMs - Positive deadline in milliseconds.
 * @param description - Operation name included in a timeout error.
 * @returns The operation's resolved value.
 * @throws The operation error, or an Error when the deadline expires.
 */
export async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new RangeError("Lifecycle timeout must be positive");
  }

  let timer: NodeJS.Timeout | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${description} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([operation, deadline]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

/**
 * Closes Kafka and HTTP resources concurrently so either failure cannot prevent
 * the other close attempt. Each close has its own deadline.
 *
 * @param consumer - Kafka runner whose handlers have already drained or timed out.
 * @param health - Health server to stop before application termination.
 * @param timeoutMs - Per-resource close deadline in milliseconds.
 * @returns A promise resolving after both close operations succeed.
 * @throws AggregateError containing every close or timeout failure.
 */
export async function closeApplicationResources(
  consumer: Pick<KafkaConsumerRunner, "close">,
  health: Pick<HealthServer, "stop">,
  timeoutMs = RESOURCE_CLOSE_TIMEOUT_MS,
): Promise<void> {
  const results = await Promise.allSettled([
    withTimeout(
      Promise.resolve().then(() => consumer.close()),
      timeoutMs,
      "Kafka resource close",
    ),
    withTimeout(
      Promise.resolve().then(() => health.stop()),
      timeoutMs,
      "Health server close",
    ),
  ]);
  const failures = results
    .filter(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    )
    .map((result) => normalizeError(result.reason));
  if (failures.length > 0) {
    throw new AggregateError(failures, "Application resource shutdown failed");
  }
}

/**
 * Constructs and runs the antivirus processor until shutdown or fatal failure.
 *
 * @returns A promise resolving after graceful consumer and health-server close.
 * @throws When configuration, HTTP bind, Kafka, or processing fails.
 */
export async function runApplication(): Promise<void> {
  const config = loadConfig();
  const logger = createAppLogger(config.logLevel);
  const http = axios.create({ timeout: config.webhooks.timeoutMs });
  const objectStore = S3ObjectStore.forRegion(config.aws.region);
  const scanner = new ClamAvClient(config.clamAv);
  const tokenProvider = new M2MTokenProvider(config.auth, http);
  const resultHandler = new ScanResultHandler(
    config,
    objectStore,
    tokenProvider,
    http,
  );
  const processor = new ScanProcessor(
    config,
    objectStore,
    scanner,
    resultHandler,
    logger,
  );
  const messageHandler = new KafkaMessageHandler(config, processor, logger);
  const consumer = new KafkaConsumerRunner(
    config.kafka,
    messageHandler,
    config.scan.concurrency,
    logger,
  );
  const health = new HealthServer(config.http, scanner, logger, () =>
    consumer.isReady(),
  );
  let resolveShutdownRequested: () => void = () => undefined;
  const shutdownRequested = new Promise<void>((resolve) => {
    resolveShutdownRequested = resolve;
  });
  let rejectShutdownFailure: (error: Error) => void = () => undefined;
  const shutdownFailure = new Promise<never>((_resolve, reject) => {
    rejectShutdownFailure = reject;
  });
  void shutdownFailure.catch(() => undefined);
  let shutdownPromise: Promise<void> | undefined;

  /**
   * Starts a single intake-stop operation for process termination signals.
   * Active handlers retain the consumer connection until they finish commits.
   *
   * @param signal - Signal name included in lifecycle logs.
   * @returns A shared promise resolving after Kafka intake has stopped.
   * @throws When Platformatic cannot stop Kafka intake.
   */
  const requestShutdown = (signal: string): Promise<void> => {
    if (!shutdownPromise) {
      logger.info("Shutdown requested", { signal });
      resolveShutdownRequested();
      shutdownPromise = consumer.stopIntake();
    }
    return shutdownPromise;
  };

  /** Requests graceful Kafka drain for SIGTERM and reports intake failures. */
  const onSigterm = () => {
    void requestShutdown("SIGTERM").catch((error: unknown) => {
      logger.error("SIGTERM shutdown failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      rejectShutdownFailure(normalizeError(error));
    });
  };
  /** Requests graceful Kafka drain for SIGINT and reports intake failures. */
  const onSigint = () => {
    void requestShutdown("SIGINT").catch((error: unknown) => {
      logger.error("SIGINT shutdown failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      rejectShutdownFailure(normalizeError(error));
    });
  };
  process.once("SIGTERM", onSigterm);
  process.once("SIGINT", onSigint);

  let applicationFailure: Error | undefined;
  try {
    await health.start();
    const consumerRun = consumer.run();
    const drainDeadline = shutdownRequested.then(() =>
      withTimeout(
        consumerRun,
        KAFKA_DRAIN_TIMEOUT_MS,
        "In-flight Kafka handler drain",
      ),
    );
    await Promise.race([consumerRun, shutdownFailure, drainDeadline]);
    if (!shutdownPromise) {
      throw new Error("Kafka consumer stopped unexpectedly");
    }
  } catch (error) {
    applicationFailure = normalizeError(error);
  }

  process.off("SIGTERM", onSigterm);
  process.off("SIGINT", onSigint);
  let cleanupFailure: Error | undefined;
  try {
    await closeApplicationResources(consumer, health);
  } catch (error) {
    cleanupFailure = normalizeError(error);
  }
  if (applicationFailure && cleanupFailure) {
    throw new AggregateError(
      [applicationFailure, cleanupFailure],
      "Application failed and resource shutdown was incomplete",
    );
  }
  if (cleanupFailure) {
    throw cleanupFailure;
  }
  if (applicationFailure) {
    throw applicationFailure;
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  void runApplication().then(
    () => process.exit(0),
    (error: unknown) => {
      const message = error instanceof Error ? error.stack : String(error);
      process.stderr.write(`Fatal av-scanner-v6 error: ${message}\n`);
      process.exit(1);
    },
  );
}
