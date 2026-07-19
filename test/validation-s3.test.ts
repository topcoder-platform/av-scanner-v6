import assert from "node:assert/strict";
import test from "node:test";

import { loadConfig } from "../src/config.js";
import { InvalidScanEventError } from "../src/errors.js";
import { buildS3Url, parseS3Url } from "../src/s3.js";
import { validateScanEvent } from "../src/validation.js";

const restrictedConfig = loadConfig({
  WHITELISTED_CLEAN_BUCKETS: "allowed-clean",
  WHITELISTED_KAFKA_TOPICS: "allowed.callback.topic",
  WHITELISTED_QUARANTINE_BUCKETS: "allowed-quarantine",
});

const unrestrictedConfig = loadConfig({});

/**
 * Creates a canonical event in the active Kafka scan-request format.
 * Tests supply payload overrides to exercise one validation branch at a time.
 *
 * @param payloadOverrides - Payload properties that replace canonical defaults.
 * @returns A decoded Kafka event suitable for validateScanEvent.
 */
function createScanEvent(
  payloadOverrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    "mime-type": "application/json",
    originator: "submission-api",
    payload: {
      callbackOption: "no-callback",
      fileName: "submission.zip",
      moveFile: false,
      submissionId: "submission-123",
      url: "https://s3.amazonaws.com/source-bucket/submissions/submission.zip",
      ...payloadOverrides,
    },
    timestamp: "2026-07-19T04:00:00.000Z",
    topic: "submission.notification.create",
  };
}

/**
 * Verifies that an event is rejected with a typed, field-specific diagnostic.
 * This helper is used by conditional-schema tests to prevent generic rejection
 * from satisfying a test intended for a particular rule.
 *
 * @param event - Decoded Kafka event expected to fail validation.
 * @param messagePattern - Pattern that must match the validation diagnostic.
 * @returns Nothing; assertions fail when validation succeeds or reports another rule.
 */
function assertInvalidEvent(event: unknown, messagePattern: RegExp): void {
  assert.throws(
    () => validateScanEvent(event, restrictedConfig),
    (error: unknown) => {
      assert.ok(error instanceof InvalidScanEventError);
      assert.match(error.message, messagePattern);
      return true;
    },
  );
}

void test("validates an active canonical scan request", () => {
  const event = createScanEvent({
    callbackKafkaTopic: "allowed.callback.topic",
    callbackOption: "kafka",
    cleanDestinationBucket: "allowed-clean",
    moveFile: true,
    quarantineDestinationBucket: "allowed-quarantine",
  });

  const result = validateScanEvent(event, restrictedConfig);

  assert.equal(result.topic, "submission.notification.create");
  assert.equal(result.payload.fileName, "submission.zip");
  assert.equal(result.payload.callbackOption, "kafka");
  assert.equal(result.payload.callbackKafkaTopic, "allowed.callback.topic");
  assert.equal(result.payload.cleanDestinationBucket, "allowed-clean");
  assert.equal(
    result.payload.quarantineDestinationBucket,
    "allowed-quarantine",
  );
});

void test("normalizes callback, webhook method, and webhook auth values", () => {
  const result = validateScanEvent(
    createScanEvent({
      callbackHook: {
        auth: "BeArEr",
        method: "PoSt",
        secret: "token",
        url: "https://callback.example.com/results",
      },
      callbackOption: "WeBhOoK",
    }),
    restrictedConfig,
  );

  assert.equal(result.payload.callbackOption, "webhook");
  assert.equal(result.payload.callbackHook?.method, "post");
  assert.equal(result.payload.callbackHook?.auth, "bearer");
});

void test("preserves legacy boolean-string coercion for moveFile", () => {
  const moved = validateScanEvent(
    createScanEvent({
      cleanDestinationBucket: "allowed-clean",
      moveFile: "TrUe",
      quarantineDestinationBucket: "allowed-quarantine",
    }),
    restrictedConfig,
  );
  const retained = validateScanEvent(
    createScanEvent({ moveFile: "FaLsE" }),
    restrictedConfig,
  );

  assert.equal(moved.payload.moveFile, true);
  assert.equal(retained.payload.moveFile, false);
  assertInvalidEvent(createScanEvent({ moveFile: 1 }), /Expected boolean/);
  assertInvalidEvent(createScanEvent({ moveFile: "yes" }), /Expected boolean/);
});

void test("preserves legacy string and epoch timestamp coercion", () => {
  for (const [timestamp, expectedMilliseconds] of [
    [0, 0],
    [-1, -1],
    ["1721347200000", 1_721_347_200_000],
  ] as const) {
    const result = validateScanEvent(
      { ...createScanEvent(), timestamp },
      restrictedConfig,
    );
    assert.ok(result.timestamp instanceof Date);
    assert.equal(result.timestamp.getTime(), expectedMilliseconds);
  }

  assertInvalidEvent(
    { ...createScanEvent(), timestamp: "not-a-date" },
    /timestamp must be a valid date/,
  );
});

void test("normalizes plaintext Kafka URL schemes for Platformatic", () => {
  const config = loadConfig({
    KAFKA_URL: "kafka://broker-one:9092,broker-two:9093",
  });

  assert.deepEqual(config.kafka.brokers, [
    "broker-one:9092",
    "broker-two:9093",
  ]);
  assert.equal(config.kafka.tls, undefined);
  assert.throws(
    () => loadConfig({ KAFKA_URL: "https://broker:9092/path" }),
    /invalid broker/,
  );
});

void test("enables verified TLS for explicit kafka+ssl brokers", () => {
  const config = loadConfig({
    KAFKA_URL: "kafka+ssl://broker-one:9092,broker-two:9093",
  });

  assert.deepEqual(config.kafka.brokers, [
    "broker-one:9092",
    "broker-two:9093",
  ]);
  assert.deepEqual(config.kafka.tls, { rejectUnauthorized: true });
});

void test("rejects contradictory explicit Kafka broker transports", () => {
  assert.throws(
    () =>
      loadConfig({
        KAFKA_URL: "kafka://broker-one:9092,kafka+ssl://broker-two:9093",
      }),
    /cannot mix explicit kafka:\/\/ and kafka\+ssl:\/\//,
  );
});

void test("enables Kafka TLS when verification is configured alone", () => {
  const verified = loadConfig({
    KAFKA_REJECT_UNAUTHORIZED: "true",
    KAFKA_URL: "broker:9092",
  });
  const unverified = loadConfig({
    KAFKA_REJECT_UNAUTHORIZED: "false",
    KAFKA_URL: "broker:9092",
  });

  assert.deepEqual(verified.kafka.tls, { rejectUnauthorized: true });
  assert.deepEqual(unverified.kafka.tls, { rejectUnauthorized: false });
});

void test("keeps explicit kafka+ssl verification secure with TLS material", () => {
  const config = loadConfig({
    KAFKA_CLIENT_CA: "-----BEGIN CERTIFICATE-----\nlegacy-ca",
    KAFKA_URL: "kafka+ssl://broker:9092",
  });

  assert.equal(config.kafka.tls?.ca?.includes("legacy-ca"), true);
  assert.equal(config.kafka.tls?.rejectUnauthorized, true);
});

void test("supports legacy CA-only Kafka TLS and verification defaults", () => {
  const config = loadConfig({
    KAFKA_CA_CERT: "",
    KAFKA_CLIENT_CA: "-----BEGIN CERTIFICATE-----\nlegacy-ca",
  });

  assert.equal(config.kafka.tls?.ca?.includes("legacy-ca"), true);
  assert.equal(config.kafka.tls?.cert, undefined);
  assert.equal(config.kafka.tls?.key, undefined);
  assert.equal(config.kafka.tls?.rejectUnauthorized, false);
});

void test("enforces destination requirements and configured bucket whitelists", () => {
  const missingDestinations = createScanEvent({ moveFile: true });
  assertInvalidEvent(
    missingDestinations,
    /cleanDestinationBucket is required when moveFile is true/,
  );
  assertInvalidEvent(
    missingDestinations,
    /quarantineDestinationBucket is required when moveFile is true/,
  );

  assertInvalidEvent(
    createScanEvent({
      cleanDestinationBucket: "not-allowed",
      moveFile: true,
      quarantineDestinationBucket: "allowed-quarantine",
    }),
    /cleanDestinationBucket is not whitelisted/,
  );
  assertInvalidEvent(
    createScanEvent({
      cleanDestinationBucket: "allowed-clean",
      moveFile: true,
      quarantineDestinationBucket: "not-allowed",
    }),
    /quarantineDestinationBucket is not whitelisted/,
  );
});

void test("forbids destination buckets when moveFile is false", () => {
  assertInvalidEvent(
    createScanEvent({ cleanDestinationBucket: "allowed-clean" }),
    /cleanDestinationBucket is forbidden when moveFile is false/,
  );
  assertInvalidEvent(
    createScanEvent({ quarantineDestinationBucket: "allowed-quarantine" }),
    /quarantineDestinationBucket is forbidden when moveFile is false/,
  );
});

void test("enforces webhook callback fields and secret rules", () => {
  assertInvalidEvent(
    createScanEvent({ callbackOption: "webhook" }),
    /callbackHook is required for webhook callbacks/,
  );
  assertInvalidEvent(
    createScanEvent({
      callbackHook: {
        auth: "bearer",
        method: "post",
        url: "https://callback.example.com/results",
      },
      callbackOption: "webhook",
    }),
    /secret is required for authenticated webhooks/,
  );
  assertInvalidEvent(
    createScanEvent({
      callbackHook: {
        auth: "no-auth",
        method: "get",
        secret: "unexpected",
        url: "https://callback.example.com/results",
      },
      callbackOption: "webhook",
    }),
    /secret is forbidden when callbackHook.auth is no-auth/,
  );
  assertInvalidEvent(
    createScanEvent({
      callbackHook: {
        auth: "no-auth",
        method: "get",
        url: "https://callback.example.com/results",
      },
      callbackKafkaTopic: "allowed.callback.topic",
      callbackOption: "webhook",
    }),
    /callbackKafkaTopic is forbidden for webhook callbacks/,
  );
});

void test("enforces kafka callback fields and its configured topic whitelist", () => {
  assertInvalidEvent(
    createScanEvent({ callbackOption: "kafka" }),
    /callbackKafkaTopic is required for kafka callbacks/,
  );
  assertInvalidEvent(
    createScanEvent({
      callbackKafkaTopic: "not.allowed",
      callbackOption: "kafka",
    }),
    /callbackKafkaTopic is not whitelisted/,
  );
  assertInvalidEvent(
    createScanEvent({
      callbackHook: {
        auth: "no-auth",
        method: "post",
        url: "https://callback.example.com/results",
      },
      callbackKafkaTopic: "allowed.callback.topic",
      callbackOption: "kafka",
    }),
    /callbackHook is forbidden for kafka callbacks/,
  );
});

void test("forbids delivery settings when callbacks are disabled", () => {
  assertInvalidEvent(
    createScanEvent({
      callbackHook: {
        auth: "no-auth",
        method: "post",
        url: "https://callback.example.com/results",
      },
    }),
    /callbackHook is forbidden when callbacks are disabled/,
  );
  assertInvalidEvent(
    createScanEvent({ callbackKafkaTopic: "allowed.callback.topic" }),
    /callbackKafkaTopic is forbidden when callbacks are disabled/,
  );
});

void test("treats empty bucket and topic whitelists as unrestricted", () => {
  const result = validateScanEvent(
    createScanEvent({
      callbackKafkaTopic: "any.callback.topic",
      callbackOption: "kafka",
      cleanDestinationBucket: "any-clean-bucket",
      moveFile: true,
      quarantineDestinationBucket: "any-quarantine-bucket",
    }),
    unrestrictedConfig,
  );

  assert.equal(result.payload.cleanDestinationBucket, "any-clean-bucket");
  assert.equal(
    result.payload.quarantineDestinationBucket,
    "any-quarantine-bucket",
  );
  assert.equal(result.payload.callbackKafkaTopic, "any.callback.topic");
});

void test("keeps the envelope strict while passing through business payload fields", () => {
  assertInvalidEvent(
    { ...createScanEvent(), unexpectedEnvelopeField: true },
    /Unrecognized key.*unexpectedEnvelopeField/,
  );

  const result = validateScanEvent(
    createScanEvent({
      challengeId: "challenge-456",
      metadata: { submittedBy: "handle" },
    }),
    restrictedConfig,
  );

  assert.equal(result.payload.submissionId, "submission-123");
  assert.equal(result.payload.challengeId, "challenge-456");
  assert.deepEqual(result.payload.metadata, { submittedBy: "handle" });
});

void test("accepts s3 URIs and rejects malformed source URLs in schema", () => {
  const result = validateScanEvent(
    createScanEvent({ url: "s3://source-bucket/submission.zip" }),
    restrictedConfig,
  );

  assert.equal(result.payload.url, "s3://source-bucket/submission.zip");
  assertInvalidEvent(
    createScanEvent({ url: "not an object URL" }),
    /Invalid url/,
  );
});

void test("rejects deterministic S3 source failures during event validation", () => {
  for (const [url, messagePattern] of [
    ["https://example.com/source-bucket/file.zip", /not a valid S3 URI/],
    [
      "https://source-bucket.s3.us-west-2.amazonaws.com/file.zip",
      /S3 object region must be us-east-1/,
    ],
    ["s3://source-bucket", /include both a bucket and object key/],
    [
      "https://s3.amazonaws.com/source-bucket/bad%ZZ.zip",
      /invalid percent encoding/,
    ],
  ] as const) {
    assertInvalidEvent(createScanEvent({ url }), messagePattern);
  }
});

void test("parses s3 URIs and decodes their object keys", () => {
  assert.deepEqual(
    parseS3Url(
      "s3://source-bucket/folder/My%20submission%2Bnotes.zip",
      "us-east-1",
    ),
    {
      bucket: "source-bucket",
      key: "folder/My submission+notes.zip",
      region: "us-east-1",
    },
  );
});

void test("parses global and regional path-style S3 URLs", () => {
  assert.deepEqual(
    parseS3Url(
      "https://s3.amazonaws.com/source-bucket/folder/file%20one.zip?versionId=1",
      "us-east-1",
    ),
    {
      bucket: "source-bucket",
      key: "folder/file one.zip",
      region: "us-east-1",
    },
  );
  assert.deepEqual(
    parseS3Url(
      "https://s3.ap-southeast-2.amazonaws.com/source-bucket/folder/file.zip",
      "ap-southeast-2",
    ),
    {
      bucket: "source-bucket",
      key: "folder/file.zip",
      region: "ap-southeast-2",
    },
  );
});

void test("parses global and regional virtual-hosted S3 URLs", () => {
  assert.deepEqual(
    parseS3Url(
      "https://source-bucket.s3.amazonaws.com/folder/file.zip",
      "us-east-1",
    ),
    {
      bucket: "source-bucket",
      key: "folder/file.zip",
      region: "us-east-1",
    },
  );
  assert.deepEqual(
    parseS3Url(
      "https://source-bucket.s3.ap-southeast-2.amazonaws.com/folder/file%20two.zip",
      "ap-southeast-2",
    ),
    {
      bucket: "source-bucket",
      key: "folder/file two.zip",
      region: "ap-southeast-2",
    },
  );
});

void test("rejects S3 URLs from a region other than the configured region", () => {
  assert.throws(
    () =>
      parseS3Url(
        "https://source-bucket.s3.us-west-2.amazonaws.com/folder/file.zip",
        "ap-southeast-2",
      ),
    /S3 object region must be ap-southeast-2/,
  );
  assert.throws(
    () => parseS3Url("s3://source-bucket/folder/file.zip", "eu-west-1"),
    /S3 object region must be eu-west-1/,
  );
});

void test("rejects non-S3 and spoofed object URLs", () => {
  for (const value of [
    "https://example.com/source-bucket/folder/file.zip",
    "https://s3.amazonaws.com.evil.example/source-bucket/folder/file.zip",
    "ftp://source-bucket/folder/file.zip",
    "not a URL",
  ]) {
    assert.throws(
      () => parseS3Url(value, "us-east-1"),
      /is not a valid S3 URI/,
    );
  }
});

void test("buildS3Url encodes key segments while preserving path separators", () => {
  assert.equal(
    buildS3Url("clean-bucket", "folder A/résumé #1?.zip"),
    "https://s3.amazonaws.com/clean-bucket/folder%20A/r%C3%A9sum%C3%A9%20%231%3F.zip",
  );
});
