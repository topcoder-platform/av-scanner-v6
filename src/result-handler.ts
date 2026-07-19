import type { AxiosInstance, AxiosRequestConfig } from "axios";
import type { Logger } from "winston";

import { M2MTokenProvider } from "./auth.js";
import type { AppConfig } from "./config.js";
import { buildS3Url } from "./s3.js";
import {
  CallbackOptions,
  WebhookAuthMethods,
  type ObjectStore,
  type ProcessedScan,
  type ResultHandler,
  type S3Location,
  type ScanPayload,
  type ScanResultPayload,
} from "./types.js";

const CONTROL_FIELDS = [
  "moveFile",
  "cleanDestinationBucket",
  "quarantineDestinationBucket",
  "callbackOption",
  "callbackHook",
  "callbackKafkaTopic",
] as const;

/**
 * Removes scanner routing controls from externally delivered result payloads.
 *
 * @param payload - Internal result payload with callback and bucket settings.
 * @returns A shallow copy safe to deliver to downstream consumers.
 */
export function callbackPayload(payload: ScanPayload): ScanResultPayload {
  const output: Record<string, unknown> = { ...payload };
  for (const field of CONTROL_FIELDS) {
    delete output[field];
  }
  if (
    typeof output.fileName !== "string" ||
    typeof output.url !== "string" ||
    typeof output.status !== "string" ||
    typeof output.isInfected !== "boolean"
  ) {
    throw new Error("Scan result is missing required callback fields");
  }

  return output as ScanResultPayload;
}

/** Handles post-scan file copying, alerting, and callback delivery. */
export class ScanResultHandler implements ResultHandler {
  /**
   * Creates the result handler used by ScanProcessor.
   *
   * @param config - Bus API, Opsgenie, and service identity settings.
   * @param objectStore - S3 copy/delete implementation.
   * @param tokenProvider - Auth0 token source used for Bus API events.
   * @param http - Axios client used for webhooks, Bus API, and Opsgenie.
   * @param logger - Structured process logger.
   */
  constructor(
    private readonly config: AppConfig,
    private readonly objectStore: ObjectStore,
    private readonly tokenProvider: M2MTokenProvider,
    private readonly http: AxiosInstance,
    private readonly logger: Logger,
  ) {}

  /**
   * Copies any moved result, schedules best-effort alerting, and delivers its
   * callback. Source deletion is returned separately for the post-commit step.
   *
   * @param payload - Terminal scan payload.
   * @param source - Original S3 object location.
   * @returns Callback-safe payload and any work safe only after Kafka commit.
   * @throws When destination copying or required callback delivery fails.
   * Opsgenie remains best-effort and never prevents offset commit.
   */
  async handle(
    payload: ScanPayload,
    source: S3Location,
  ): Promise<ProcessedScan> {
    const result = { ...payload };

    if (result.moveFile) {
      const destinationBucket = result.isInfected
        ? result.quarantineDestinationBucket
        : result.cleanDestinationBucket;
      if (!destinationBucket) {
        throw new Error("Scan result is missing its destination bucket");
      }

      await this.objectStore.copy(source, destinationBucket, result.fileName);
      result.url = buildS3Url(destinationBucket, result.fileName);
    }

    const externalPayload = callbackPayload(result);
    if (result.callbackOption === CallbackOptions.Kafka) {
      await this.postBusEvent(result, externalPayload);
    } else if (result.callbackOption === CallbackOptions.Webhook) {
      await this.postWebhook(result, externalPayload);
    }

    const alertAfterCommit = result.isInfected && this.config.opsgenie.enabled;
    return {
      payload: externalPayload,
      ...(result.moveFile || alertAfterCommit
        ? {
            afterCommit: async () => {
              if (alertAfterCommit) {
                void this.alertBestEffort(result);
              }
              if (result.moveFile) {
                await this.objectStore.delete(source);
              }
            },
          }
        : {}),
    };
  }

  /**
   * Posts the full Topcoder callback envelope through the authenticated Bus API.
   *
   * @param result - Internal payload containing callbackKafkaTopic.
   * @param externalPayload - Result payload with scanner controls omitted.
   * @returns A promise resolving after Bus API accepts the event.
   * @throws When the callback topic is absent, authentication fails, or Bus API
   * returns an error.
   */
  private async postBusEvent(
    result: ScanPayload,
    externalPayload: ScanResultPayload,
  ): Promise<void> {
    if (!result.callbackKafkaTopic) {
      throw new Error("Kafka callback is missing callbackKafkaTopic");
    }

    const token = await this.tokenProvider.getToken();
    await this.http.post(
      this.config.busApi.eventsUrl,
      {
        "mime-type": "application/json",
        originator: this.config.busApi.originator,
        payload: externalPayload,
        timestamp: new Date().toISOString(),
        topic: result.callbackKafkaTopic,
      },
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      },
    );
  }

  /**
   * Delivers a direct webhook with legacy method and authentication behavior.
   *
   * @param result - Internal payload containing callbackHook.
   * @param externalPayload - Result payload with scanner controls omitted.
   * @returns A promise resolving after the webhook succeeds.
   * @throws When the callback hook is missing or the remote request fails.
   */
  private async postWebhook(
    result: ScanPayload,
    externalPayload: ScanResultPayload,
  ): Promise<void> {
    const hook = result.callbackHook;
    if (!hook) {
      throw new Error("Webhook callback is missing callbackHook");
    }

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (hook.auth === WebhookAuthMethods.Bearer) {
      headers.Authorization = `Bearer ${hook.secret ?? ""}`;
    } else if (hook.auth === WebhookAuthMethods.Basic) {
      headers.Authorization = `Basic ${hook.secret ?? ""}`;
    } else if (hook.auth === WebhookAuthMethods.ApiKey) {
      headers["X-API-Key"] = hook.secret ?? "";
    }

    const request: AxiosRequestConfig = {
      data: externalPayload,
      headers,
      method: hook.method,
      url: hook.url,
    };
    await this.http.request(request);
  }

  /**
   * Sends a malicious-file alert without making alert delivery transactional.
   *
   * @param result - Infected or fail-closed scan result.
   * @returns A promise that always resolves after success or logged failure.
   */
  private async alertBestEffort(result: ScanPayload): Promise<void> {
    try {
      if (!this.config.opsgenie.apiKey) {
        throw new Error("OPSGENIE_API_KEY is required when alerts are enabled");
      }

      const moveDetail = result.moveFile
        ? ` The file has been moved to the ${result.quarantineDestinationBucket} bucket.`
        : "";
      await this.http.post(
        this.config.opsgenie.apiUrl,
        {
          message: `Malicious file detected: File at ${result.url} is malicious.${moveDetail}`,
          priority: result.moveFile ? "P2" : "P1",
          source: this.config.opsgenie.source,
        },
        {
          headers: {
            Authorization: `GenieKey ${this.config.opsgenie.apiKey}`,
            "Content-Type": "application/json",
          },
        },
      );
    } catch (error) {
      this.logger.error("Failed to post Opsgenie alert", {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
}
