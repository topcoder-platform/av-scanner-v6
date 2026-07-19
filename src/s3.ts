import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { Readable } from "node:stream";

import type { ObjectMetadata, ObjectStore, S3Location } from "./types.js";

/**
 * Decodes an S3 URL path and converts malformed escapes into a safe error.
 *
 * @param path - URL pathname without query or fragment.
 * @returns Decoded S3 key or path-style bucket/key string.
 * @throws When the URL contains invalid percent encoding.
 */
function decodePath(path: string): string {
  try {
    return decodeURIComponent(path.replace(/^\/+/, ""));
  } catch (error) {
    throw new Error("S3 URL contains invalid percent encoding", {
      cause: error,
    });
  }
}

/**
 * Validates the final bucket, key, and region parsed from an S3 URL.
 *
 * @param bucket - Parsed S3 bucket.
 * @param key - Parsed object key.
 * @param region - Region encoded by the URL form.
 * @param expectedRegion - Only region this scanner is configured to access.
 * @returns A normalized S3 location.
 * @throws When a component is missing or the region does not match.
 */
function validateLocation(
  bucket: string,
  key: string,
  region: string,
  expectedRegion: string,
): S3Location {
  if (!bucket || !key) {
    throw new Error("S3 URL must include both a bucket and object key");
  }
  if (region !== expectedRegion) {
    throw new Error(`S3 object region must be ${expectedRegion}`);
  }

  return { bucket, key, region };
}

/**
 * Parses the S3 URL forms accepted by the legacy scanner.
 * Supported forms include s3://, global path/virtual-hosted URLs, and
 * region-specific path/virtual-hosted URLs.
 *
 * @param value - S3 URI or HTTPS S3 object URL from the event payload.
 * @param expectedRegion - Configured AWS region required by this task.
 * @returns Bucket, decoded key, and parsed region.
 * @throws When the URL is not an S3 object reference or uses another region.
 */
export function parseS3Url(value: string, expectedRegion: string): S3Location {
  let url: URL;
  try {
    url = new URL(value);
  } catch (error) {
    throw new Error(`${value} is not a valid S3 URI`, { cause: error });
  }

  if (url.protocol === "s3:") {
    return validateLocation(
      url.hostname,
      decodePath(url.pathname),
      "us-east-1",
      expectedRegion,
    );
  }

  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw new Error(`${value} is not a valid S3 URI`);
  }

  const hostname = url.hostname.toLowerCase();
  const pathStyle = hostname.match(
    /^s3(?:[.-]([a-z0-9-]+))?\.amazonaws\.com(?:\.cn)?$/,
  );
  if (pathStyle) {
    const decoded = decodePath(url.pathname);
    const separator = decoded.indexOf("/");
    const bucket = separator === -1 ? decoded : decoded.slice(0, separator);
    const key = separator === -1 ? "" : decoded.slice(separator + 1);
    return validateLocation(
      bucket,
      key,
      pathStyle[1] ?? "us-east-1",
      expectedRegion,
    );
  }

  const virtualHosted = hostname.match(
    /^(.+)\.s3(?:[.-]([a-z0-9-]+))?\.amazonaws\.com(?:\.cn)?$/,
  );
  if (virtualHosted) {
    return validateLocation(
      virtualHosted[1] ?? "",
      decodePath(url.pathname),
      virtualHosted[2] ?? "us-east-1",
      expectedRegion,
    );
  }

  throw new Error(`${value} is not a valid S3 URI`);
}

/**
 * Encodes an S3 key for use in an HTTPS path while retaining key separators.
 *
 * @param key - Raw object key.
 * @returns URL-safe key path.
 */
function encodeKey(key: string): string {
  return key
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
}

/**
 * Builds the destination URL format emitted by the legacy scanner.
 *
 * @param bucket - Destination clean or quarantine bucket.
 * @param key - Destination key, normally the request fileName.
 * @returns A globally addressed S3 HTTPS URL.
 */
export function buildS3Url(bucket: string, key: string): string {
  return `https://s3.amazonaws.com/${bucket}/${encodeKey(key)}`;
}

/** AWS SDK-backed implementation of the scanner's streaming object operations. */
export class S3ObjectStore implements ObjectStore {
  /**
   * Creates an object store in the configured scanner region.
   *
   * @param client - AWS S3 client; replaceable for focused tests.
   */
  constructor(private readonly client: S3Client) {}

  /**
   * Creates a production S3 object store for one AWS region.
   *
   * @param region - Region enforced by scan URL parsing.
   * @returns An object store using the AWS default credential provider chain.
   */
  static forRegion(region: string): S3ObjectStore {
    return new S3ObjectStore(new S3Client({ region }));
  }

  /**
   * Reads object size through HeadObject before any body is downloaded.
   *
   * @param location - Source S3 object.
   * @returns Object content length in bytes.
   * @throws When S3 fails or omits ContentLength.
   */
  async getMetadata(location: S3Location): Promise<ObjectMetadata> {
    const output = await this.client.send(
      new HeadObjectCommand({ Bucket: location.bucket, Key: location.key }),
    );
    if (output.ContentLength === undefined) {
      throw new Error(
        `S3 object ${location.bucket}/${location.key} did not return ContentLength`,
      );
    }

    return { contentLength: output.ContentLength };
  }

  /**
   * Opens an S3 body without buffering it into task memory.
   *
   * @param location - Source S3 object.
   * @returns Node readable body accepted by the ClamAV client.
   * @throws When the SDK response does not contain a Node readable stream.
   */
  async openReadStream(location: S3Location): Promise<Readable> {
    const output = await this.client.send(
      new GetObjectCommand({ Bucket: location.bucket, Key: location.key }),
    );
    if (!(output.Body instanceof Readable)) {
      throw new Error(
        `S3 object ${location.bucket}/${location.key} did not include a readable body`,
      );
    }

    return output.Body;
  }

  /**
   * Copies an S3 object while retaining the source for callback/commit retry.
   *
   * @param source - Existing S3 source object.
   * @param destinationBucket - Clean or quarantine bucket.
   * @param destinationKey - Final key derived from request fileName.
   * @returns A promise resolving after the copy completes.
   * @throws When S3 cannot copy the source object.
   */
  async copy(
    source: S3Location,
    destinationBucket: string,
    destinationKey: string,
  ): Promise<void> {
    const copySource = `${source.bucket}/${encodeKey(source.key)}`;
    await this.client.send(
      new CopyObjectCommand({
        Bucket: destinationBucket,
        CopySource: copySource,
        Key: destinationKey,
      }),
    );
  }

  /**
   * Deletes the original S3 object after Kafka commit makes the result durable.
   *
   * @param source - Original source object that has already been copied.
   * @returns A promise resolving after deletion completes.
   * @throws When S3 cannot delete the object.
   */
  async delete(source: S3Location): Promise<void> {
    await this.client.send(
      new DeleteObjectCommand({ Bucket: source.bucket, Key: source.key }),
    );
  }
}
