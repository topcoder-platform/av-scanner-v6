import assert from "node:assert/strict";
import { request } from "node:http";
import { createServer, type Server, type Socket } from "node:net";
import { Readable } from "node:stream";
import { test } from "node:test";

import { createLogger } from "winston";

import { ClamAvClient, type ClamAvClientOptions } from "../src/clamav.js";
import { ClamAvError, ClamAvSizeLimitError } from "../src/errors.js";
import { HealthServer } from "../src/health-server.js";
import type { AntivirusScanner } from "../src/types.js";

const TEST_HOST = "127.0.0.1";

/** One complete command decoded by the fake clamd TCP server. */
interface FakeClamdRequest {
  command: string;
  payload: Buffer;
}

/** Handle returned for an ephemeral fake clamd TCP server. */
interface FakeClamdServer {
  /** Closes accepted sockets and then stops the TCP listener. */
  close(): Promise<void>;
  /** The first complete command received by the server. */
  nextRequest: Promise<FakeClamdRequest>;
  /** Ephemeral TCP port selected by the operating system. */
  port: number;
}

/** Captured response from one request to the HTTP health server. */
interface HealthHttpResponse {
  body: string;
  statusCode: number | undefined;
}

/**
 * Decodes a complete clamd PING or length-framed INSTREAM request when enough
 * TCP bytes have arrived.
 *
 * @param buffer - Accumulated bytes from one clamd client connection.
 * @returns The decoded request, or undefined while the request is incomplete.
 */
function parseClamdRequest(buffer: Buffer): FakeClamdRequest | undefined {
  const commandEnd = buffer.indexOf(0);
  if (commandEnd < 0) {
    return undefined;
  }

  const command = buffer.subarray(1, commandEnd).toString("utf8");
  if (command !== "INSTREAM") {
    return { command, payload: Buffer.alloc(0) };
  }

  const payloadChunks: Buffer[] = [];
  let payloadLength = 0;
  let offset = commandEnd + 1;
  while (true) {
    if (buffer.length < offset + 4) {
      return undefined;
    }

    const frameLength = buffer.readUInt32BE(offset);
    offset += 4;
    if (frameLength === 0) {
      return {
        command,
        payload: Buffer.concat(payloadChunks, payloadLength),
      };
    }
    if (buffer.length < offset + frameLength) {
      return undefined;
    }

    payloadChunks.push(buffer.subarray(offset, offset + frameLength));
    payloadLength += frameLength;
    offset += frameLength;
  }
}

/**
 * Starts a protocol-aware fake clamd listener on an operating-system-selected
 * loopback port.
 *
 * @param respond - Selects the clamd response for the decoded request. Returning
 * undefined deliberately leaves the connection idle for timeout tests.
 * @returns The bound port, first decoded request, and an idempotent close method.
 * @throws When the loopback TCP listener cannot be started or stopped.
 */
async function startFakeClamd(
  respond: (request: FakeClamdRequest) => string | undefined,
): Promise<FakeClamdServer> {
  const sockets = new Set<Socket>();
  let resolveRequest: (request: FakeClamdRequest) => void = () => undefined;
  const nextRequest = new Promise<FakeClamdRequest>((resolve) => {
    resolveRequest = resolve;
  });

  const server: Server = createServer((socket) => {
    sockets.add(socket);
    let buffer = Buffer.alloc(0);
    let handled = false;

    socket.once("error", () => undefined);
    socket.once("close", () => {
      sockets.delete(socket);
    });
    socket.on("data", (chunk: Buffer) => {
      if (handled) {
        return;
      }

      buffer = Buffer.concat([buffer, chunk]);
      const decoded = parseClamdRequest(buffer);
      if (!decoded) {
        return;
      }

      handled = true;
      resolveRequest(decoded);
      const response = respond(decoded);
      if (response !== undefined) {
        socket.end(Buffer.from(`${response}\0`));
      }
    });
  });

  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };

    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(0, TEST_HOST);
  });

  const address = server.address();
  assert.ok(address && typeof address !== "string");

  return {
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      if (!server.listening) {
        return;
      }
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
    nextRequest,
    port: address.port,
  };
}

/**
 * Creates a ClamAV client for a fake loopback listener.
 *
 * @param port - Ephemeral fake-clamd port.
 * @param overrides - Optional timeout overrides for failure-path tests.
 * @returns A client configured with short test-safe deadlines.
 */
function createTestClient(
  port: number,
  overrides: Partial<ClamAvClientOptions> = {},
): ClamAvClient {
  return new ClamAvClient({
    healthTimeoutMs: 250,
    host: TEST_HOST,
    port,
    scanTimeoutMs: 250,
    ...overrides,
  });
}

/**
 * Issues a GET request to an ephemeral HealthServer and captures its response.
 *
 * @param port - Bound HealthServer port.
 * @returns The HTTP status and complete UTF-8 response body.
 * @throws When the request or response stream fails.
 */
async function requestHealth(port: number): Promise<HealthHttpResponse> {
  return new Promise<HealthHttpResponse>((resolve, reject) => {
    const healthRequest = request(
      {
        host: TEST_HOST,
        method: "GET",
        path: "/health",
        port,
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => {
          chunks.push(chunk);
        });
        response.once("error", reject);
        response.once("end", () => {
          resolve({
            body: Buffer.concat(chunks).toString("utf8"),
            statusCode: response.statusCode,
          });
        });
      },
    );
    healthRequest.once("error", reject);
    healthRequest.end();
  });
}

void test("ClamAvClient PING accepts a terminated PONG response", async (t) => {
  const fakeClamd = await startFakeClamd(() => "PONG");
  t.after(() => fakeClamd.close());

  await createTestClient(fakeClamd.port).ping();

  const received = await fakeClamd.nextRequest;
  assert.equal(received.command, "PING");
  assert.equal(received.payload.length, 0);
});

void test("ClamAvClient PING rejects a malformed response", async (t) => {
  const fakeClamd = await startFakeClamd(() => "NOT PONG");
  t.after(() => fakeClamd.close());

  await assert.rejects(
    createTestClient(fakeClamd.port).ping(),
    /Unexpected ClamAV PING response: NOT PONG/,
  );
});

void test("ClamAvClient PING requires an exact PONG response", async (t) => {
  const fakeClamd = await startFakeClamd(() => " PONG ");
  t.after(() => fakeClamd.close());

  await assert.rejects(
    createTestClient(fakeClamd.port).ping(),
    /Unexpected ClamAV PING response/,
  );
});

void test("ClamAvClient PING times out when clamd stays silent", async (t) => {
  const fakeClamd = await startFakeClamd(() => undefined);
  t.after(() => fakeClamd.close());

  await assert.rejects(
    createTestClient(fakeClamd.port, { healthTimeoutMs: 40 }).ping(),
    (error: unknown) => {
      assert.ok(error instanceof ClamAvError);
      assert.match(`${error.message} ${String(error.cause)}`, /timed out/i);
      return true;
    },
  );
  assert.equal((await fakeClamd.nextRequest).command, "PING");
});

void test("ClamAvClient PING reports a refused connection", async () => {
  const closedServer = await startFakeClamd(() => undefined);
  const closedPort = closedServer.port;
  await closedServer.close();

  await assert.rejects(
    createTestClient(closedPort).ping(),
    (error: unknown) => {
      assert.ok(error instanceof ClamAvError);
      assert.match(error.message, /Unable to connect to ClamAV/);
      return true;
    },
  );
});

void test("ClamAvClient INSTREAM returns false for clean content", async (t) => {
  const fakeClamd = await startFakeClamd(() => "stream: OK");
  t.after(() => fakeClamd.close());
  const content = Buffer.from("clean-file-content");

  const infected = await createTestClient(fakeClamd.port).scan(
    Readable.from([content.subarray(0, 5), content.subarray(5)]),
  );

  assert.equal(infected, false);
  const received = await fakeClamd.nextRequest;
  assert.equal(received.command, "INSTREAM");
  assert.deepEqual(received.payload, content);
});

void test("ClamAvClient INSTREAM returns true for infected content", async (t) => {
  const fakeClamd = await startFakeClamd(
    () => "stream: Eicar-Test-Signature FOUND",
  );
  t.after(() => fakeClamd.close());

  const infected = await createTestClient(fakeClamd.port).scan(
    Readable.from(Buffer.from("infected-file-content")),
  );

  assert.equal(infected, true);
  assert.equal((await fakeClamd.nextRequest).command, "INSTREAM");
});

void test("ClamAvClient INSTREAM maps clamd size failures to a typed error", async (t) => {
  const fakeClamd = await startFakeClamd(
    () => "INSTREAM size limit exceeded. ERROR",
  );
  t.after(() => fakeClamd.close());

  await assert.rejects(
    createTestClient(fakeClamd.port).scan(Readable.from("too-large")),
    ClamAvSizeLimitError,
  );
});

void test("ClamAvClient scan deadline aborts a stalled input stream", async (t) => {
  const fakeClamd = await startFakeClamd(() => undefined);
  t.after(() => fakeClamd.close());
  let sentChunk = false;
  const stalledStream = new Readable({
    read() {
      if (!sentChunk) {
        sentChunk = true;
        this.push(Buffer.from("partial-content"));
      }
    },
  });
  const startedAt = Date.now();

  await assert.rejects(
    createTestClient(fakeClamd.port, { scanTimeoutMs: 40 }).scan(stalledStream),
    (error: unknown) => {
      assert.ok(error instanceof ClamAvError);
      assert.match(`${error.message} ${String(error.cause)}`, /timed out/i);
      return true;
    },
  );

  assert.ok(Date.now() - startedAt < 500);
  assert.equal(stalledStream.destroyed, true);
});

void test("HealthServer returns 200 when a fresh ClamAV probe succeeds", async (t) => {
  let pingCalls = 0;
  const scanner: AntivirusScanner = {
    ping: () => {
      pingCalls += 1;
      return Promise.resolve();
    },
    scan: () => Promise.resolve(false),
  };
  const healthServer = new HealthServer(
    { host: TEST_HOST, port: 0 },
    scanner,
    createLogger({ silent: true }),
  );
  const address = await healthServer.start();
  t.after(() => healthServer.stop());

  const response = await requestHealth(address.port);

  assert.equal(response.statusCode, 200);
  assert.equal(
    response.body,
    '{"checks":{"clamav":{"status":"up"}},"status":"ok"}',
  );
  assert.equal(pingCalls, 1);
});

void test("HealthServer returns 503 until the initial Kafka join succeeds", async (t) => {
  let kafkaReady = false;
  const scanner: AntivirusScanner = {
    ping: () => Promise.resolve(),
    scan: () => Promise.resolve(false),
  };
  const healthServer = new HealthServer(
    { host: TEST_HOST, port: 0 },
    scanner,
    createLogger({ silent: true }),
    () => kafkaReady,
  );
  const address = await healthServer.start();
  t.after(() => healthServer.stop());

  const starting = await requestHealth(address.port);
  kafkaReady = true;
  const ready = await requestHealth(address.port);

  assert.equal(starting.statusCode, 503);
  assert.equal(
    starting.body,
    '{"checks":{"clamav":{"status":"up"},"kafka":{"status":"starting"}},"status":"unhealthy"}',
  );
  assert.equal(ready.statusCode, 200);
  assert.equal(
    ready.body,
    '{"checks":{"clamav":{"status":"up"}},"status":"ok"}',
  );
});

void test("HealthServer returns 503 and freshly recovers on the next request", async (t) => {
  let pingCalls = 0;
  const scanner: AntivirusScanner = {
    ping: () => {
      pingCalls += 1;
      return pingCalls === 1
        ? Promise.reject(new Error("clamd unavailable"))
        : Promise.resolve();
    },
    scan: () => Promise.resolve(false),
  };
  const healthServer = new HealthServer(
    { host: TEST_HOST, port: 0 },
    scanner,
    createLogger({ silent: true }),
  );
  const address = await healthServer.start();
  t.after(() => healthServer.stop());

  const unhealthy = await requestHealth(address.port);
  const recovered = await requestHealth(address.port);

  assert.equal(unhealthy.statusCode, 503);
  assert.equal(
    unhealthy.body,
    '{"checks":{"clamav":{"status":"down"}},"status":"unhealthy"}',
  );
  assert.equal(recovered.statusCode, 200);
  assert.equal(
    recovered.body,
    '{"checks":{"clamav":{"status":"up"}},"status":"ok"}',
  );
  assert.equal(pingCalls, 2);
});
