import type { Readable } from "node:stream";

/** Supported result-delivery modes from the existing scanner event contract. */
export const CallbackOptions = {
  Kafka: "kafka",
  NoCallback: "no-callback",
  Webhook: "webhook",
} as const;

/** A result-delivery mode accepted in an antivirus scan request. */
export type CallbackOption =
  (typeof CallbackOptions)[keyof typeof CallbackOptions];

/** Supported webhook HTTP methods from the existing scanner contract. */
export const WebhookMethods = {
  Get: "get",
  Post: "post",
} as const;

/** A webhook HTTP method accepted in a scan request. */
export type WebhookMethod =
  (typeof WebhookMethods)[keyof typeof WebhookMethods];

/** Supported webhook authentication modes from the existing scanner contract. */
export const WebhookAuthMethods = {
  ApiKey: "api-key",
  Basic: "basic",
  Bearer: "bearer",
  NoAuth: "no-auth",
} as const;

/** A webhook authentication mode accepted in a scan request. */
export type WebhookAuthMethod =
  (typeof WebhookAuthMethods)[keyof typeof WebhookAuthMethods];

/** Webhook delivery settings carried by a scan request. */
export interface CallbackHook {
  auth: WebhookAuthMethod;
  method: WebhookMethod;
  secret?: string;
  url: string;
}

/**
 * Payload carried inside an antivirus scan event.
 * Unknown business fields, such as submissionId, are preserved in callbacks.
 */
export interface ScanPayload {
  [key: string]: unknown;
  callbackHook?: CallbackHook;
  callbackKafkaTopic?: string;
  callbackOption: CallbackOption;
  cleanDestinationBucket?: string;
  fileName: string;
  isInfected?: boolean;
  moveFile: boolean;
  quarantineDestinationBucket?: string;
  scanError?: string;
  scanErrorMessage?: string;
  status?: string;
  url: string;
}

/** Callback payload after internal movement and delivery controls are removed. */
export interface ScanResultPayload {
  [key: string]: unknown;
  fileName: string;
  isInfected: boolean;
  scanError?: string;
  scanErrorMessage?: string;
  status: string;
  url: string;
}

/** Topcoder event envelope consumed from the configured antivirus Kafka topic. */
export interface ScanEvent {
  "mime-type": string;
  originator: string;
  payload: ScanPayload;
  timestamp: Date | string;
  topic: string;
}

/** Parsed location of an S3 object referenced by a scan request. */
export interface S3Location {
  bucket: string;
  key: string;
  region: string;
}

/** Metadata needed before opening an S3 object body. */
export interface ObjectMetadata {
  contentLength: number;
}

/** Storage operations used by scan and result processing. */
export interface ObjectStore {
  /**
   * Reads object metadata without downloading its body.
   *
   * @param location - Source S3 object.
   * @returns The content length used by the pre-scan limit check.
   * @throws When S3 cannot find or inspect the object.
   */
  getMetadata(location: S3Location): Promise<ObjectMetadata>;

  /**
   * Opens the source object as a Node readable stream.
   *
   * @param location - Source S3 object.
   * @returns The streaming object body used by ClamAV.
   * @throws When S3 cannot open a readable object body.
   */
  openReadStream(location: S3Location): Promise<Readable>;

  /**
   * Copies an object to a clean or quarantine destination without deleting it.
   * Result handling uses this before callbacks so a failed callback can retry
   * from the original source.
   *
   * @param source - Existing source object.
   * @param destinationBucket - Clean or quarantine bucket.
   * @param destinationKey - Destination object key.
   * @returns A promise that resolves after the destination copy is durable.
   * @throws When S3 cannot copy the source object.
   */
  copy(
    source: S3Location,
    destinationBucket: string,
    destinationKey: string,
  ): Promise<void>;

  /**
   * Deletes a source object after its result and Kafka offset are committed.
   *
   * @param source - Original S3 object that has already been copied.
   * @returns A promise that resolves after S3 confirms deletion.
   * @throws When S3 cannot delete the source object.
   */
  delete(source: S3Location): Promise<void>;
}

/** ClamAV operations required by scan processing and health reporting. */
export interface AntivirusScanner {
  /**
   * Performs a fresh bounded clamd PING command.
   *
   * @returns A promise that resolves only for an exact PONG response.
   * @throws When clamd is unavailable, times out, or responds incorrectly.
   */
  ping(): Promise<void>;

  /**
   * Streams a file through clamd's INSTREAM protocol.
   *
   * @param stream - S3 object content to scan.
   * @returns True for infected content and false for clean content.
   * @throws When the socket, stream, protocol, or clamd scan fails.
   */
  scan(stream: Readable): Promise<boolean>;
}

/** Completed result plus optional cleanup that is safe only after Kafka commit. */
export interface ProcessedScan {
  /**
   * Deletes or finalizes source state after the result offset is committed.
   *
   * @returns A promise resolving when post-commit cleanup completes.
   * @throws When cleanup fails; callers log this because the offset is already
   * committed and must not be retried past the completed result.
   */
  afterCommit?: () => Promise<void>;
  payload: ScanResultPayload;
}

/** Result side effects performed after an antivirus verdict is available. */
export interface ResultHandler {
  /**
   * Copies and delivers the final scan result.
   *
   * @param payload - Final payload including scan status and infection flag.
   * @param source - Original S3 object location.
   * @returns The callback-safe result plus optional post-commit source cleanup.
   * @throws When destination copying or required callback delivery fails.
   */
  handle(payload: ScanPayload, source: S3Location): Promise<ProcessedScan>;
}
