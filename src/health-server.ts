import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import type { Logger } from "winston";

import type { AppConfig } from "./config.js";
import type { AntivirusScanner } from "./types.js";

/** JSON response written by the scanner health endpoint. */
interface HealthResponse {
  checks: {
    clamav: {
      status: "down" | "up";
    };
    kafka?: {
      status: "starting";
    };
  };
  status: "ok" | "unhealthy";
}

/**
 * Writes a small JSON response with correct HEAD request behavior.
 *
 * @param response - Node HTTP response being completed.
 * @param requestMethod - Incoming HTTP method, used to suppress HEAD bodies.
 * @param statusCode - HTTP response status.
 * @param payload - Serializable response body.
 * @returns Nothing after the response has ended.
 */
function writeJson(
  response: ServerResponse,
  requestMethod: string | undefined,
  statusCode: number,
  payload: HealthResponse | { status: string },
): void {
  const body = Buffer.from(JSON.stringify(payload));
  response.writeHead(statusCode, {
    "Content-Length": body.length,
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(requestMethod === "HEAD" ? undefined : body);
}

/** HTTP server with initial Kafka readiness and fresh bounded ClamAV probes. */
export class HealthServer {
  private readonly server: Server;

  /**
   * Creates the health server used by Docker and ECS task health checks.
   *
   * @param config - HTTP bind address.
   * @param scanner - ClamAV client whose PING has its own short timeout.
   * @param logger - Structured process logger.
   * @param isReady - One-way initial Kafka readiness signal.
   */
  constructor(
    private readonly config: AppConfig["http"],
    private readonly scanner: AntivirusScanner,
    private readonly logger: Logger,
    private readonly isReady: () => boolean = () => true,
  ) {
    this.server = createServer((request, response) => {
      void this.handleRequest(request, response);
    });
  }

  /**
   * Starts listening before Kafka connection attempts begin.
   *
   * @returns The effective TCP address, including an ephemeral test port.
   * @throws When the configured host or port cannot be bound.
   */
  async start(): Promise<AddressInfo> {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        this.server.off("listening", onListening);
        reject(error);
      };
      const onListening = () => {
        this.server.off("error", onError);
        resolve();
      };
      this.server.once("error", onError);
      this.server.once("listening", onListening);
      this.server.listen(this.config.port, this.config.host);
    });

    const address = this.server.address();
    if (!address || typeof address === "string") {
      throw new Error("Health server did not bind a TCP address");
    }
    this.logger.info("Health server listening", {
      host: address.address,
      port: address.port,
    });
    return address;
  }

  /**
   * Stops accepting health requests during task shutdown.
   *
   * @returns A promise resolving once the HTTP server has closed.
   * @throws When Node reports an HTTP close error.
   */
  async stop(): Promise<void> {
    if (!this.server.listening) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * Routes one HTTP request and probes clamd only for GET/HEAD /health.
   *
   * @param request - Incoming HTTP request.
   * @param response - Response completed by this method.
   * @returns A promise resolving after the response has been sent.
   */
  private async handleRequest(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    const pathname = new URL(request.url ?? "/", "http://localhost").pathname;
    if (pathname !== "/health") {
      writeJson(response, request.method, 404, { status: "not-found" });
      return;
    }
    if (request.method !== "GET" && request.method !== "HEAD") {
      response.setHeader("Allow", "GET, HEAD");
      writeJson(response, request.method, 405, {
        status: "method-not-allowed",
      });
      return;
    }

    try {
      await this.scanner.ping();
      if (!this.isReady()) {
        writeJson(response, request.method, 503, {
          checks: {
            clamav: { status: "up" },
            kafka: { status: "starting" },
          },
          status: "unhealthy",
        });
        return;
      }
      writeJson(response, request.method, 200, {
        checks: { clamav: { status: "up" } },
        status: "ok",
      });
    } catch (error) {
      this.logger.warn("ClamAV health probe failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      writeJson(response, request.method, 503, {
        checks: { clamav: { status: "down" } },
        status: "unhealthy",
      });
    }
  }
}
