import { z } from "zod";

import type { AppConfig } from "./config.js";
import { InvalidScanEventError } from "./errors.js";
import { parseS3Url } from "./s3.js";
import {
  CallbackOptions,
  WebhookAuthMethods,
  WebhookMethods,
  type ScanEvent,
} from "./types.js";

/**
 * Creates a case-insensitive string enum while returning canonical lowercase.
 *
 * @param values - Non-empty tuple of accepted lowercase enum values.
 * @returns A Zod schema that normalizes before validating the value.
 */
function normalizedEnum<T extends readonly [string, ...string[]]>(values: T) {
  return z
    .string()
    .transform((value) => value.toLowerCase())
    .pipe(z.enum(values));
}

const legacyBooleanSchema = z.preprocess((value) => {
  if (typeof value !== "string") {
    return value;
  }
  if (value.toLowerCase() === "true") {
    return true;
  }
  if (value.toLowerCase() === "false") {
    return false;
  }
  return value;
}, z.boolean());

const callbackHookSchema = z
  .object({
    auth: normalizedEnum([
      WebhookAuthMethods.NoAuth,
      WebhookAuthMethods.Bearer,
      WebhookAuthMethods.ApiKey,
      WebhookAuthMethods.Basic,
    ]),
    method: normalizedEnum([WebhookMethods.Get, WebhookMethods.Post]),
    secret: z.string().min(1).optional(),
    url: z.string().url(),
  })
  .strict()
  .superRefine((hook, context) => {
    if (hook.auth === WebhookAuthMethods.NoAuth && hook.secret !== undefined) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "secret is forbidden when callbackHook.auth is no-auth",
        path: ["secret"],
      });
    }
    if (hook.auth !== WebhookAuthMethods.NoAuth && hook.secret === undefined) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "secret is required for authenticated webhooks",
        path: ["secret"],
      });
    }
  });

const timestampSchema = z
  .union([z.string(), z.number(), z.date()])
  .transform((value, context) => {
    let date: Date;
    if (value instanceof Date) {
      date = value;
    } else if (typeof value === "number") {
      date = new Date(value);
    } else if (/^[-+]?\d+(?:\.\d+)?$/.test(value)) {
      date = new Date(Number(value));
    } else {
      date = new Date(value);
    }
    if (Number.isNaN(date.getTime())) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "timestamp must be a valid date",
      });
      return z.NEVER;
    }
    return date;
  });

/**
 * Adds a custom validation issue at the requested payload field.
 *
 * @param context - Active Zod refinement context.
 * @param path - Payload property associated with the failure.
 * @param message - Human-readable validation failure.
 * @returns Nothing; the issue is appended to the current parse result.
 */
function addIssue(
  context: z.RefinementCtx,
  path: string,
  message: string,
): void {
  context.addIssue({
    code: z.ZodIssueCode.custom,
    message,
    path: [path],
  });
}

/**
 * Checks a configured allowlist while preserving legacy empty-list behavior.
 * An empty whitelist means unrestricted, matching Joi's previous zero-value
 * valid() call.
 *
 * @param value - Bucket or topic supplied by the scan request.
 * @param whitelist - Configured allowed values.
 * @returns True when unrestricted or explicitly allowed.
 */
function isAllowed(value: string, whitelist: string[]): boolean {
  return whitelist.length === 0 || whitelist.includes(value);
}

/**
 * Creates the environment-aware scan event schema.
 *
 * @param config - Configuration containing clean, quarantine, and topic lists.
 * @returns A strict envelope schema with a passthrough business payload.
 */
function createSchema(config: AppConfig) {
  return z
    .object({
      "mime-type": z.string().min(1),
      originator: z.string().min(1),
      payload: z
        .object({
          callbackHook: callbackHookSchema.optional(),
          callbackKafkaTopic: z.string().min(1).optional(),
          callbackOption: normalizedEnum([
            CallbackOptions.NoCallback,
            CallbackOptions.Webhook,
            CallbackOptions.Kafka,
          ]),
          cleanDestinationBucket: z.string().min(1).optional(),
          fileName: z.string().min(1),
          moveFile: legacyBooleanSchema,
          quarantineDestinationBucket: z.string().min(1).optional(),
          url: z.string().url(),
        })
        .passthrough()
        .superRefine((payload, context) => {
          if (payload.moveFile) {
            if (!payload.cleanDestinationBucket) {
              addIssue(
                context,
                "cleanDestinationBucket",
                "cleanDestinationBucket is required when moveFile is true",
              );
            } else if (
              !isAllowed(
                payload.cleanDestinationBucket,
                config.whitelists.cleanBuckets,
              )
            ) {
              addIssue(
                context,
                "cleanDestinationBucket",
                "cleanDestinationBucket is not whitelisted",
              );
            }

            if (!payload.quarantineDestinationBucket) {
              addIssue(
                context,
                "quarantineDestinationBucket",
                "quarantineDestinationBucket is required when moveFile is true",
              );
            } else if (
              !isAllowed(
                payload.quarantineDestinationBucket,
                config.whitelists.quarantineBuckets,
              )
            ) {
              addIssue(
                context,
                "quarantineDestinationBucket",
                "quarantineDestinationBucket is not whitelisted",
              );
            }
          } else {
            if (payload.cleanDestinationBucket !== undefined) {
              addIssue(
                context,
                "cleanDestinationBucket",
                "cleanDestinationBucket is forbidden when moveFile is false",
              );
            }
            if (payload.quarantineDestinationBucket !== undefined) {
              addIssue(
                context,
                "quarantineDestinationBucket",
                "quarantineDestinationBucket is forbidden when moveFile is false",
              );
            }
          }

          if (payload.callbackOption === CallbackOptions.Webhook) {
            if (!payload.callbackHook) {
              addIssue(
                context,
                "callbackHook",
                "callbackHook is required for webhook callbacks",
              );
            }
            if (payload.callbackKafkaTopic !== undefined) {
              addIssue(
                context,
                "callbackKafkaTopic",
                "callbackKafkaTopic is forbidden for webhook callbacks",
              );
            }
          } else if (payload.callbackOption === CallbackOptions.Kafka) {
            if (!payload.callbackKafkaTopic) {
              addIssue(
                context,
                "callbackKafkaTopic",
                "callbackKafkaTopic is required for kafka callbacks",
              );
            } else if (
              !isAllowed(
                payload.callbackKafkaTopic,
                config.whitelists.kafkaTopics,
              )
            ) {
              addIssue(
                context,
                "callbackKafkaTopic",
                "callbackKafkaTopic is not whitelisted",
              );
            }
            if (payload.callbackHook !== undefined) {
              addIssue(
                context,
                "callbackHook",
                "callbackHook is forbidden for kafka callbacks",
              );
            }
          } else {
            if (payload.callbackHook !== undefined) {
              addIssue(
                context,
                "callbackHook",
                "callbackHook is forbidden when callbacks are disabled",
              );
            }
            if (payload.callbackKafkaTopic !== undefined) {
              addIssue(
                context,
                "callbackKafkaTopic",
                "callbackKafkaTopic is forbidden when callbacks are disabled",
              );
            }
          }
        }),
      timestamp: timestampSchema,
      topic: z.string().min(1),
    })
    .strict();
}

/**
 * Validates and normalizes a decoded Kafka scan event.
 * KafkaMessageHandler uses the returned event before invoking ScanProcessor.
 *
 * @param value - Parsed JSON value from Kafka.
 * @param config - Environment-aware schema settings.
 * @returns A validated event with normalized callback enum values.
 * @throws InvalidScanEventError when the envelope, payload, or S3 source URL
 * is invalid for the configured AWS region.
 */
export function validateScanEvent(
  value: unknown,
  config: AppConfig,
): ScanEvent {
  const result = createSchema(config).safeParse(value);
  if (!result.success) {
    const details = result.error.issues
      .map((issue) => `${issue.path.join(".") || "event"}: ${issue.message}`)
      .join("; ");
    throw new InvalidScanEventError(`Invalid scan event: ${details}`);
  }

  try {
    parseS3Url(result.data.payload.url, config.aws.region);
  } catch (error) {
    const details = error instanceof Error ? error.message : String(error);
    throw new InvalidScanEventError(
      `Invalid scan event: payload.url: ${details}`,
    );
  }

  return result.data;
}
