import { existsSync, readFileSync } from "node:fs";

/** Optional server-authenticated or mutual-TLS settings for Kafka. */
export interface KafkaTlsConfig {
  ca?: string;
  cert?: string;
  key?: string;
  rejectUnauthorized: boolean;
}

/** A normalized Kafka bootstrap broker and its optional explicit transport. */
interface ParsedKafkaBroker {
  address: string;
  transport?: "plaintext" | "tls";
}

/** Validated application configuration derived from environment variables. */
export interface AppConfig {
  auth: {
    audience?: string;
    clientId?: string;
    clientSecret?: string;
    proxyUrl?: string;
    tokenCacheSeconds: number;
    url?: string;
  };
  aws: {
    region: string;
  };
  busApi: {
    eventsUrl: string;
    originator: string;
  };
  clamAv: {
    healthTimeoutMs: number;
    host: string;
    port: number;
    scanTimeoutMs: number;
  };
  http: {
    host: string;
    port: number;
  };
  kafka: {
    brokers: string[];
    clientId: string;
    groupId: string;
    heartbeatIntervalMs: number;
    maxBytes: number;
    maxWaitTimeMs: number;
    sessionTimeoutMs: number;
    tls?: KafkaTlsConfig;
    topic: string;
  };
  logLevel: string;
  scan: {
    concurrency: number;
    maxFileSizeBytes: number;
    s3NotFoundMaxAttempts: number;
    s3NotFoundRetryBaseDelayMs: number;
  };
  webhooks: {
    timeoutMs: number;
  };
  whitelists: {
    cleanBuckets: string[];
    kafkaTopics: string[];
    quarantineBuckets: string[];
  };
}

/**
 * Parses a positive integer environment setting.
 *
 * @param value - Raw environment value, or undefined to use the default.
 * @param defaultValue - Value returned when the setting is absent.
 * @param name - Environment variable name used in validation errors.
 * @returns A validated positive integer.
 * @throws When a supplied value is not a positive integer.
 */
function positiveInteger(
  value: string | undefined,
  defaultValue: number,
  name: string,
): number {
  if (value === undefined || value.trim() === "") {
    return defaultValue;
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
}

/**
 * Parses a non-negative integer environment setting.
 *
 * @param value - Raw environment value, or undefined to use the default.
 * @param defaultValue - Value returned when the setting is absent.
 * @param name - Environment variable name used in validation errors.
 * @returns A validated non-negative integer.
 * @throws When a supplied value is not a non-negative integer.
 */
function nonNegativeInteger(
  value: string | undefined,
  defaultValue: number,
  name: string,
): number {
  if (value === undefined || value.trim() === "") {
    return defaultValue;
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }

  return parsed;
}

/**
 * Parses a strict boolean environment setting.
 *
 * @param value - Raw environment value, or undefined to use the default.
 * @param defaultValue - Value returned when the setting is absent.
 * @param name - Environment variable name used in validation errors.
 * @returns The parsed boolean.
 * @throws When a supplied value is neither true nor false.
 */
function booleanValue(
  value: string | undefined,
  defaultValue: boolean,
  name: string,
): boolean {
  if (value === undefined || value.trim() === "") {
    return defaultValue;
  }

  if (value.toLowerCase() === "true") {
    return true;
  }
  if (value.toLowerCase() === "false") {
    return false;
  }

  throw new Error(`${name} must be either true or false`);
}

/**
 * Splits a comma-separated environment setting and removes blank entries.
 *
 * @param value - Raw comma-separated value.
 * @returns Trimmed non-empty entries, or an empty array when absent.
 */
function csv(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

/**
 * Converts legacy no-kafka broker URLs to Platformatic's host:port format.
 *
 * @param value - One KAFKA_URL entry, optionally prefixed by kafka:// or
 * kafka+ssl://.
 * @returns A broker accepted by Platformatic's bootstrap parser and the
 * explicitly requested transport, when present.
 * @throws When host or explicit port is malformed.
 */
function kafkaBroker(value: string): ParsedKafkaBroker {
  const transport = /^kafka\+ssl:\/\//i.test(value)
    ? "tls"
    : /^kafka:\/\//i.test(value)
      ? "plaintext"
      : undefined;
  const broker = value.replace(/^kafka(?:\+ssl)?:\/\//i, "");
  const components = broker.split(":");
  if (
    !components[0] ||
    components.length > 2 ||
    /[\s/@?#]/.test(broker) ||
    (value.includes("://") && broker === value)
  ) {
    throw new Error(`KAFKA_URL contains an invalid broker: ${value}`);
  }
  if (components[1] !== undefined) {
    const port = Number(components[1]);
    if (!Number.isInteger(port) || port < 1 || port > 65_535) {
      throw new Error(`KAFKA_URL contains an invalid broker port: ${value}`);
    }
  }

  return {
    address: broker,
    ...(transport ? { transport } : {}),
  };
}

/**
 * Normalizes inline PEM newlines or loads legacy file-path TLS values.
 *
 * @param value - PEM text or path supplied by an environment variable.
 * @param name - Environment variable name used in validation errors.
 * @returns PEM contents accepted by Node's TLS client.
 * @throws When a path-like value does not identify a readable file.
 */
function tlsMaterial(value: string, name: string): string {
  const normalized = value.replaceAll("\\n", "\n");
  if (normalized.includes("-----BEGIN") || normalized.includes("\n")) {
    return normalized;
  }

  if (existsSync(normalized)) {
    return readFileSync(normalized, "utf8");
  }

  throw new Error(
    `${name} must contain PEM data or reference an existing certificate file`,
  );
}

/**
 * Builds optional Kafka TLS configuration while preventing partial setup.
 * An explicit kafka+ssl broker uses verified TLS with the system trust store
 * by default. Legacy certificate-triggered TLS retains its historical
 * unverified default unless KAFKA_REJECT_UNAUTHORIZED is supplied.
 *
 * @param environment - Process environment containing Kafka TLS settings.
 * @param schemeRequestsTls - Whether KAFKA_URL explicitly selected TLS.
 * @returns Complete TLS settings, or undefined for plaintext Kafka.
 * @throws When only one of the required certificate and private key is set.
 */
function kafkaTls(
  environment: NodeJS.ProcessEnv,
  schemeRequestsTls: boolean,
): KafkaTlsConfig | undefined {
  const cert = environment.KAFKA_CLIENT_CERT;
  const key = environment.KAFKA_CLIENT_CERT_KEY;
  const ca = environment.KAFKA_CA_CERT || environment.KAFKA_CLIENT_CA;
  const rejectUnauthorizedValue = environment.KAFKA_REJECT_UNAUTHORIZED;
  const hasRejectUnauthorizedSetting =
    rejectUnauthorizedValue !== undefined &&
    rejectUnauthorizedValue.trim() !== "";

  if (
    !schemeRequestsTls &&
    !cert &&
    !key &&
    !ca &&
    !hasRejectUnauthorizedSetting
  ) {
    return undefined;
  }
  if ((cert || key) && (!cert || !key)) {
    throw new Error(
      "KAFKA_CLIENT_CERT and KAFKA_CLIENT_CERT_KEY must be provided together",
    );
  }

  return {
    rejectUnauthorized: booleanValue(
      rejectUnauthorizedValue,
      schemeRequestsTls,
      "KAFKA_REJECT_UNAUTHORIZED",
    ),
    ...(cert ? { cert: tlsMaterial(cert, "KAFKA_CLIENT_CERT") } : {}),
    ...(key ? { key: tlsMaterial(key, "KAFKA_CLIENT_CERT_KEY") } : {}),
    ...(ca
      ? {
          ca: tlsMaterial(
            ca,
            environment.KAFKA_CA_CERT ? "KAFKA_CA_CERT" : "KAFKA_CLIENT_CA",
          ),
        }
      : {}),
  };
}

/**
 * Loads and validates all scanner configuration from the process environment.
 * The application calls this once before constructing network clients.
 *
 * @param environment - Environment map, replaceable in focused tests.
 * @returns Immutable-by-convention application configuration.
 * @throws When numeric, boolean, broker, or TLS settings are invalid.
 */
export function loadConfig(
  environment: NodeJS.ProcessEnv = process.env,
): AppConfig {
  const parsedBrokers = csv(environment.KAFKA_URL ?? "localhost:9092").map(
    kafkaBroker,
  );
  if (parsedBrokers.length === 0) {
    throw new Error("KAFKA_URL must contain at least one broker");
  }

  const explicitTransports = new Set(
    parsedBrokers.flatMap(({ transport }) =>
      transport === undefined ? [] : [transport],
    ),
  );
  if (explicitTransports.size > 1) {
    throw new Error(
      "KAFKA_URL cannot mix explicit kafka:// and kafka+ssl:// brokers",
    );
  }

  const brokers = parsedBrokers.map(({ address }) => address);
  const schemeRequestsTls = explicitTransports.has("tls");

  const authUrl = environment.AUTH0_URL;
  const tls = kafkaTls(environment, schemeRequestsTls);

  return {
    auth: {
      tokenCacheSeconds: nonNegativeInteger(
        environment.TOKEN_CACHE_TIME,
        300,
        "TOKEN_CACHE_TIME",
      ),
      ...(authUrl ? { url: authUrl } : {}),
      ...(environment.AUTH0_AUDIENCE
        ? { audience: environment.AUTH0_AUDIENCE }
        : {}),
      ...(environment.AUTH0_CLIENT_ID
        ? { clientId: environment.AUTH0_CLIENT_ID }
        : {}),
      ...(environment.AUTH0_CLIENT_SECRET
        ? { clientSecret: environment.AUTH0_CLIENT_SECRET }
        : {}),
      ...(environment.AUTH0_PROXY_SERVER_URL
        ? { proxyUrl: environment.AUTH0_PROXY_SERVER_URL }
        : {}),
    },
    aws: {
      region: environment.AWS_REGION ?? "us-east-1",
    },
    busApi: {
      eventsUrl:
        environment.BUSAPI_EVENTS_URL ??
        environment.BUS_API_URL ??
        "https://api.topcoder-dev.com/v5/bus/events",
      originator: environment.KAFKA_ORIGINATOR ?? "file-scanner-processor",
    },
    clamAv: {
      healthTimeoutMs: positiveInteger(
        environment.CLAMAV_HEALTH_TIMEOUT_MS,
        2_000,
        "CLAMAV_HEALTH_TIMEOUT_MS",
      ),
      host: environment.CLAMAV_HOST ?? "filescanner",
      port: positiveInteger(environment.CLAMAV_PORT, 3310, "CLAMAV_PORT"),
      scanTimeoutMs: positiveInteger(
        environment.CLAMAV_SCAN_TIMEOUT_MS,
        300_000,
        "CLAMAV_SCAN_TIMEOUT_MS",
      ),
    },
    http: {
      host: environment.HOST ?? "0.0.0.0",
      port: nonNegativeInteger(environment.PORT, 3000, "PORT"),
    },
    kafka: {
      brokers,
      clientId: environment.KAFKA_CLIENT_ID ?? "av-scanner-v6",
      groupId: environment.KAFKA_GROUP_ID ?? "file-scanner-processor",
      heartbeatIntervalMs: positiveInteger(
        environment.KAFKA_HEARTBEAT_INTERVAL_MS,
        3_000,
        "KAFKA_HEARTBEAT_INTERVAL_MS",
      ),
      maxBytes: positiveInteger(
        environment.KAFKA_MAX_BYTES,
        10 * 1024 * 1024,
        "KAFKA_MAX_BYTES",
      ),
      maxWaitTimeMs: positiveInteger(
        environment.KAFKA_MAX_WAIT_TIME_MS,
        500,
        "KAFKA_MAX_WAIT_TIME_MS",
      ),
      sessionTimeoutMs: positiveInteger(
        environment.KAFKA_SESSION_TIMEOUT_MS,
        30_000,
        "KAFKA_SESSION_TIMEOUT_MS",
      ),
      topic: environment.AVSCAN_TOPIC ?? "avscan.action.scan",
      ...(tls ? { tls } : {}),
    },
    logLevel: environment.LOG_LEVEL ?? "info",
    scan: {
      concurrency: positiveInteger(
        environment.SCAN_CONCURRENCY,
        1,
        "SCAN_CONCURRENCY",
      ),
      maxFileSizeBytes: positiveInteger(
        environment.MAX_SCAN_FILE_SIZE_BYTES,
        500 * 1024 * 1024,
        "MAX_SCAN_FILE_SIZE_BYTES",
      ),
      s3NotFoundMaxAttempts: positiveInteger(
        environment.S3_NOT_FOUND_MAX_ATTEMPTS,
        5,
        "S3_NOT_FOUND_MAX_ATTEMPTS",
      ),
      s3NotFoundRetryBaseDelayMs: positiveInteger(
        environment.S3_NOT_FOUND_RETRY_BASE_DELAY_MS,
        1_000,
        "S3_NOT_FOUND_RETRY_BASE_DELAY_MS",
      ),
    },
    webhooks: {
      timeoutMs: positiveInteger(
        environment.WEBHOOK_TIMEOUT_MS,
        15_000,
        "WEBHOOK_TIMEOUT_MS",
      ),
    },
    whitelists: {
      cleanBuckets: csv(environment.WHITELISTED_CLEAN_BUCKETS),
      kafkaTopics: csv(environment.WHITELISTED_KAFKA_TOPICS),
      quarantineBuckets: csv(environment.WHITELISTED_QUARANTINE_BUCKETS),
    },
  };
}
