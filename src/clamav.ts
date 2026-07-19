import { createConnection, type Socket } from "node:net";
import type { Readable } from "node:stream";

import { ClamAvError, ClamAvSizeLimitError } from "./errors.js";
import type { AntivirusScanner } from "./types.js";

const COMMAND_PREFIX = "z";
const COMMAND_TERMINATOR = "\0";
const MAX_CHUNK_BYTES = 64 * 1024;
const MAX_RESPONSE_BYTES = 64 * 1024;

/** Network and timeout settings for the clamd TCP client. */
export interface ClamAvClientOptions {
  healthTimeoutMs: number;
  host: string;
  port: number;
  scanTimeoutMs: number;
}

/**
 * Converts an arbitrary thrown value into a stable Error instance.
 *
 * @param error - Value caught from a stream or socket operation.
 * @returns The original Error or a new Error containing its string value.
 */
function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Converts a readable-stream chunk to a Buffer accepted by the clamd protocol.
 *
 * @param chunk - Value emitted by a Node readable stream.
 * @returns Binary content for one or more INSTREAM frames.
 * @throws ClamAvError when the stream emits an unsupported object-mode value.
 */
function normalizeChunk(chunk: unknown): Buffer {
  if (Buffer.isBuffer(chunk)) {
    return chunk;
  }
  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk);
  }
  if (typeof chunk === "string") {
    return Buffer.from(chunk);
  }

  throw new ClamAvError("The scan stream emitted a non-binary chunk");
}

/**
 * Writes one protocol buffer and waits until Node has handed it to the socket.
 *
 * @param socket - Connected clamd TCP socket.
 * @param data - Command or framed file bytes.
 * @returns A promise resolving when the write callback succeeds.
 * @throws ClamAvError when the socket closes or rejects the write.
 */
async function writeSocket(socket: Socket, data: Buffer): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      socket.off("close", onClose);
      socket.off("error", onError);
    };
    const finish = (error?: Error | null) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      if (error) {
        reject(new ClamAvError("Failed to write to ClamAV", { cause: error }));
      } else {
        resolve();
      }
    };
    const onClose = () => {
      finish(new Error("ClamAV closed the connection during a write"));
    };
    const onError = (error: Error) => {
      finish(error);
    };

    socket.once("close", onClose);
    socket.once("error", onError);
    socket.write(data, finish);
  });
}

/**
 * Reads one NUL- or newline-terminated clamd response with a memory bound.
 *
 * @param socket - Connected clamd TCP socket.
 * @returns The response text without its protocol terminator.
 * @throws ClamAvError for socket failure, truncation, or oversized responses.
 */
async function readResponse(socket: Socket): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    let length = 0;
    let settled = false;

    const cleanup = () => {
      socket.off("data", onData);
      socket.off("close", onClose);
      socket.off("end", onEnd);
      socket.off("error", onError);
    };
    const fail = (error: Error) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(error);
    };
    const onError = (error: Error) => {
      fail(
        new ClamAvError("Failed to read the ClamAV response", { cause: error }),
      );
    };
    const onEnd = () => {
      fail(new ClamAvError("ClamAV closed without a terminated response"));
    };
    const onClose = () => {
      fail(new ClamAvError("ClamAV closed without a terminated response"));
    };
    const onData = (chunk: Buffer) => {
      chunks.push(chunk);
      length += chunk.length;
      if (length > MAX_RESPONSE_BYTES) {
        fail(new ClamAvError("ClamAV returned an oversized response"));
        return;
      }

      const combined = Buffer.concat(chunks, length);
      const nullIndex = combined.indexOf(0);
      const newlineIndex = combined.indexOf(10);
      const terminators = [nullIndex, newlineIndex].filter(
        (index) => index >= 0,
      );
      if (terminators.length === 0) {
        return;
      }

      settled = true;
      cleanup();
      resolve(combined.subarray(0, Math.min(...terminators)).toString("utf8"));
    };

    socket.on("data", onData);
    socket.once("close", onClose);
    socket.once("end", onEnd);
    socket.once("error", onError);
  });
}

/**
 * Parses the terminal response from a clamd INSTREAM command.
 *
 * @param response - Terminator-free clamd response text.
 * @returns True when a signature was found and false for an OK result.
 * @throws ClamAvSizeLimitError for configured stream limits, otherwise
 * ClamAvError for scan or protocol failures.
 */
function parseScanResponse(response: string): boolean {
  const normalized = response.trim();
  if (/^[^:]+:\s+OK$/i.test(normalized)) {
    return false;
  }
  if (/^[^:]+:\s+.+\s+FOUND$/i.test(normalized)) {
    return true;
  }
  if (/INSTREAM size limit exceeded|Size limit reached/i.test(normalized)) {
    throw new ClamAvSizeLimitError(normalized);
  }
  if (/\sERROR$/i.test(normalized)) {
    throw new ClamAvError(`ClamAV scan failed: ${normalized}`);
  }

  throw new ClamAvError(`Unexpected ClamAV scan response: ${normalized}`);
}

/** Pure TCP implementation of clamd PING and INSTREAM for Node 22. */
export class ClamAvClient implements AntivirusScanner {
  /**
   * Creates a ClamAV client with independent health and scan deadlines.
   *
   * @param options - Clamd address and operation timeouts.
   */
  constructor(private readonly options: ClamAvClientOptions) {}

  /**
   * Performs a fresh clamd PING and requires an exact PONG response.
   * The health endpoint invokes this method on every request.
   *
   * @returns A promise resolving only when clamd is responsive.
   * @throws ClamAvError on refusal, timeout, socket failure, or bad response.
   */
  async ping(): Promise<void> {
    const response = await this.executeCommand(
      "PING",
      this.options.healthTimeoutMs,
    );
    if (response !== "PONG") {
      throw new ClamAvError(`Unexpected ClamAV PING response: ${response}`);
    }
  }

  /**
   * Streams a readable body through clamd using bounded binary frames.
   *
   * @param stream - S3 object body to scan.
   * @returns True when infected and false when clean.
   * @throws ClamAvSizeLimitError for INSTREAM limits or ClamAvError for all
   * other network, stream, timeout, and protocol failures.
   */
  async scan(stream: Readable): Promise<boolean> {
    const socket = await this.openSocket(this.options.scanTimeoutMs);
    const responsePromise = readResponse(socket);
    let receivedResponse: string | undefined;

    void responsePromise.then(
      (response) => {
        receivedResponse = response;
      },
      () => undefined,
    );

    try {
      await writeSocket(
        socket,
        Buffer.from(`${COMMAND_PREFIX}INSTREAM${COMMAND_TERMINATOR}`),
      );

      const iterator = (stream as AsyncIterable<unknown>)[
        Symbol.asyncIterator
      ]();
      while (true) {
        const outcome = await Promise.race([
          iterator
            .next()
            .then((result) => ({ result, type: "chunk" as const })),
          responsePromise.then((response) => ({
            response,
            type: "response" as const,
          })),
        ]);
        if (outcome.type === "response") {
          return parseScanResponse(outcome.response);
        }
        if (outcome.result.done) {
          break;
        }

        const chunk = normalizeChunk(outcome.result.value);
        for (let offset = 0; offset < chunk.length; offset += MAX_CHUNK_BYTES) {
          const frame = chunk.subarray(offset, offset + MAX_CHUNK_BYTES);
          const header = Buffer.allocUnsafe(4);
          header.writeUInt32BE(frame.length);
          await writeSocket(socket, header);
          await writeSocket(socket, frame);
        }
      }

      await writeSocket(socket, Buffer.alloc(4));
      return parseScanResponse(receivedResponse ?? (await responsePromise));
    } catch (error) {
      if (!stream.destroyed) {
        stream.destroy();
      }
      if (receivedResponse !== undefined) {
        return parseScanResponse(receivedResponse);
      }

      const normalized = normalizeError(error);
      if (normalized instanceof ClamAvError) {
        throw normalized;
      }
      throw new ClamAvError("ClamAV scan did not complete", {
        cause: normalized,
      });
    } finally {
      if (!stream.destroyed) {
        stream.destroy();
      }
      socket.destroy();
      await responsePromise.catch(() => undefined);
    }
  }

  /**
   * Runs a simple NUL-terminated clamd command and reads one response.
   *
   * @param command - Command name without framing bytes.
   * @param timeoutMs - Complete socket inactivity timeout.
   * @returns Terminator-free clamd response.
   * @throws ClamAvError for connection, write, timeout, or response failures.
   */
  private async executeCommand(
    command: string,
    timeoutMs: number,
  ): Promise<string> {
    const socket = await this.openSocket(timeoutMs);
    const responsePromise = readResponse(socket);
    try {
      await writeSocket(
        socket,
        Buffer.from(`${COMMAND_PREFIX}${command}${COMMAND_TERMINATOR}`),
      );
      return await responsePromise;
    } finally {
      socket.destroy();
      await responsePromise.catch(() => undefined);
    }
  }

  /**
   * Opens a clamd socket and applies an absolute operation deadline.
   *
   * @param timeoutMs - Milliseconds before the complete operation is aborted.
   * @returns A connected TCP socket.
   * @throws ClamAvError when the connection cannot be established.
   */
  private async openSocket(timeoutMs: number): Promise<Socket> {
    return new Promise<Socket>((resolve, reject) => {
      const socket = createConnection({
        host: this.options.host,
        port: this.options.port,
      });
      let connected = false;

      const timeout = setTimeout(() => {
        socket.destroy(
          new ClamAvError(`ClamAV operation timed out after ${timeoutMs}ms`),
        );
      }, timeoutMs);
      timeout.unref();
      socket.once("close", () => clearTimeout(timeout));
      socket.once("connect", () => {
        connected = true;
        resolve(socket);
      });
      socket.once("error", (error) => {
        if (!connected) {
          reject(
            new ClamAvError(
              `Unable to connect to ClamAV at ${this.options.host}:${this.options.port}`,
              { cause: error },
            ),
          );
        }
      });
    });
  }
}
