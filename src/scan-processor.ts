import type { Logger } from "winston";

import type { AppConfig } from "./config.js";
import { ClamAvSizeLimitError } from "./errors.js";
import { parseS3Url } from "./s3.js";
import { Semaphore } from "./semaphore.js";
import type {
  AntivirusScanner,
  ObjectStore,
  ProcessedScan,
  ResultHandler,
  ScanEvent,
  ScanPayload,
} from "./types.js";

const FILE_SIZE_EXCEEDED_CODE = "file-size-exceeded";

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
