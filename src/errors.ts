import type { S3Location } from "./types.js";

/** Base error returned when clamd cannot complete a protocol operation. */
export class ClamAvError extends Error {
  /**
   * Creates a ClamAV operation error.
   *
   * @param message - Safe diagnostic describing the failed operation.
   * @param options - Optional native error cause.
   */
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ClamAvError";
  }
}

/** ClamAV error used when INSTREAM rejects content over its configured limit. */
export class ClamAvSizeLimitError extends ClamAvError {
  /**
   * Creates a typed size-limit failure used for fail-closed result handling.
   *
   * @param message - Clamd's size-limit diagnostic.
   */
  constructor(message: string) {
    super(message);
    this.name = "ClamAvSizeLimitError";
  }
}

/** Validation error used to discard a structurally invalid scan event. */
export class InvalidScanEventError extends Error {
  /**
   * Creates an invalid-event error the Kafka handler logs before committing.
   *
   * @param message - Concise schema failure detail.
   */
  constructor(message: string) {
    super(message);
    this.name = "InvalidScanEventError";
  }
}

/** S3 source-object operation that can prove the requested input is absent. */
export type S3SourceOperation = "GetObject" | "HeadObject";

/** Typed missing-source failure translated from modeled AWS S3 404 errors. */
export class S3ObjectNotFoundError extends Error {
  /**
   * Creates a retryable source-absence error without classifying other S3
   * failures such as access denial, service errors, or malformed responses.
   *
   * @param operation - S3 read operation that returned the modeled 404.
   * @param location - Source object requested by the scan event.
   * @param options - Native error cause returned by the AWS SDK.
   */
  constructor(
    readonly operation: S3SourceOperation,
    readonly location: S3Location,
    options?: ErrorOptions,
  ) {
    super(
      `S3 ${operation} could not find s3://${location.bucket}/${location.key}`,
      options,
    );
    this.name = "S3ObjectNotFoundError";
  }
}
