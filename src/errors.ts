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
