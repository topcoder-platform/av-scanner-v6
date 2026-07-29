import { setTimeout as wait } from "node:timers/promises";
import type { Logger } from "winston";

import type { AppConfig } from "./config.js";
import { ClamAvSizeLimitError, S3ObjectNotFoundError } from "./errors.js";
import { parseS3Url } from "./s3.js";
import { Semaphore } from "./semaphore.js";
import type {
  AntivirusScanner,
  ObjectStore,
  ProcessedScan,
  ResultHandler,
  S3Location,
  ScanEvent,
  ScanPayload,
} from "./types.js";

const FILE_SIZE_EXCEEDED_CODE = "file-size-exceeded";
const S3_OBJECT_NOT_FOUND_CODE = "s3-object-not-found";
const MAX_S3_NOT_FOUND_RETRY_DELAY_MS = 5_000;

/** Injectable retry wait used to keep backoff tests deterministic and fast. */
export type RetryWait = (delayMs: number) => Promise<void>;

/**
 * Builds the legacy fail-closed payload used for oversized or rejected streams.
 *
 * @param payload - Original scan payload to preserve business fields from.
 * @param message - Human-readable size failure detail.
 * @returns A new terminal payload treated as infected by downstream services.
 */
function failedSizePayload(payload: ScanPayload, message: string): ScanPayload {
  return {
    ...payload,
    isInfected: true,
    scanError: FILE_SIZE_EXCEEDED_CODE,
    scanErrorMessage: message,
    status: "scan-failed",
  };
}

/**
 * Builds a fail-closed result for an input that remained absent after retries.
 * Movement is disabled only on the derived result because copying or deleting
 * a missing source would turn this terminal outcome back into a retry loop.
 *
 * @param payload - Original validated scan request.
 * @param attempts - Total source-read attempts that were exhausted.
 * @returns Terminal callback payload retaining the original source URL.
 */
function missingSourcePayload(
  payload: ScanPayload,
  attempts: number,
): ScanPayload {
  return {
    ...payload,
    isInfected: true,
    moveFile: false,
    scanError: S3_OBJECT_NOT_FOUND_CODE,
    scanErrorMessage:
      `Source object was not found after ${attempts} ` +
      `${attempts === 1 ? "attempt" : "attempts"}`,
    status: "scan-failed",
  };
}

/** Coordinates S3 preflight, bounded ClamAV scanning, and result side effects. */
export class ScanProcessor {
  private readonly scanSlots: Semaphore;

  /**
   * Creates the antivirus request processor.
   *
   * @param config - Region, maximum size, and concurrency settings.
   * @param objectStore - Streaming S3 implementation.
   * @param scanner - Clamd protocol client.
   * @param results - Result copy, callback, and post-commit cleanup handler.
   * @param logger - Structured process logger.
   */
  constructor(
    private readonly config: AppConfig,
    private readonly objectStore: ObjectStore,
    private readonly scanner: AntivirusScanner,
    private readonly results: ResultHandler,
    private readonly logger: Logger,
    private readonly retryWait: RetryWait = (delayMs) => wait(delayMs),
  ) {
    this.scanSlots = new Semaphore(config.scan.concurrency);
  }

  /**
   * Processes one validated Kafka scan event to a terminal external payload.
   *
   * @param event - Validated legacy scan event envelope.
   * @returns Callback-safe payload plus any post-commit cleanup.
   * @throws For invalid S3 URLs/regions, S3 failures, non-size ClamAV failures,
   * destination-copy failures, and callback delivery failures.
   */
  async process(event: ScanEvent): Promise<ProcessedScan> {
    const source = parseS3Url(event.payload.url, this.config.aws.region);
    try {
      return await this.processWithSourceRetries(event, source);
    } catch (error) {
      if (!(error instanceof S3ObjectNotFoundError)) {
        throw error;
      }

      const attempts = this.config.scan.s3NotFoundMaxAttempts;
      this.logger.error("S3 source object was not found after retries", {
        attempts,
        bucket: source.bucket,
        key: source.key,
        operation: error.operation,
      });
      return this.results.handle(
        missingSourcePayload(event.payload, attempts),
        source,
      );
    }
  }

  /**
   * Repeats a complete source-open attempt only for typed S3 missing-object
   * responses. A GetObject race restarts at HeadObject so the size preflight is
   * never reused for a newly recreated key.
   *
   * @param event - Validated scan event.
   * @param source - Parsed source object location.
   * @returns A completed scan or terminal size result.
   * @throws The last missing-object error after exhaustion, or any other error.
   */
  private async processWithSourceRetries(
    event: ScanEvent,
    source: S3Location,
  ): Promise<ProcessedScan> {
    const maximumAttempts = this.config.scan.s3NotFoundMaxAttempts;
    for (let attempt = 1; attempt <= maximumAttempts; attempt += 1) {
      try {
        return await this.processSourceAttempt(event, source);
      } catch (error) {
        if (
          !(error instanceof S3ObjectNotFoundError) ||
          attempt === maximumAttempts
        ) {
          throw error;
        }

        const retryDelayMs = Math.min(
          this.config.scan.s3NotFoundRetryBaseDelayMs *
            2 ** Math.min(attempt - 1, 20),
          MAX_S3_NOT_FOUND_RETRY_DELAY_MS,
        );
        this.logger.warn("S3 source object not found; retrying", {
          attempt,
          bucket: source.bucket,
          key: source.key,
          maximumAttempts,
          operation: error.operation,
          retryDelayMs,
        });
        await this.retryWait(retryDelayMs);
      }
    }

    throw new Error("S3 missing-source retry loop exhausted unexpectedly");
  }

  /**
   * Performs one complete HeadObject, size check, GetObject, scan, and result
   * attempt. Only the source reads translate missing-object failures.
   *
   * @param event - Validated scan event.
   * @param source - Parsed source object location.
   * @returns A processed result.
   */
  private async processSourceAttempt(
    event: ScanEvent,
    source: S3Location,
  ): Promise<ProcessedScan> {
    const metadata = await this.objectStore.getMetadata(source);
    if (metadata.contentLength > this.config.scan.maxFileSizeBytes) {
      const message =
        `File size ${metadata.contentLength} bytes exceeds ` +
        `MAX_SCAN_FILE_SIZE_BYTES ${this.config.scan.maxFileSizeBytes} bytes`;
      this.logger.warn(message, { bucket: source.bucket, key: source.key });
      return this.results.handle(
        failedSizePayload(event.payload, message),
        source,
      );
    }

    const release = await this.scanSlots.acquire();
    try {
      const stream = await this.objectStore.openReadStream(source);
      try {
        const isInfected = await this.scanner.scan(stream);
        return await this.results.handle(
          {
            ...event.payload,
            isInfected,
            status: "scanned",
          },
          source,
        );
      } catch (error) {
        if (!(error instanceof ClamAvSizeLimitError)) {
          throw error;
        }

        const message =
          "ClamAV rejected the stream because it exceeded the configured " +
          `scan size limit: ${error.message}`;
        this.logger.warn(message, { bucket: source.bucket, key: source.key });
        return await this.results.handle(
          failedSizePayload(event.payload, message),
          source,
        );
      }
    } finally {
      release();
    }
  }
}
