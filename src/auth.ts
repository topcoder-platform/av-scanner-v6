import { createM2MClient, type M2MClient } from "tc-core-library-js";
import type { AxiosInstance } from "axios";

import type { AppConfig } from "./config.js";


/**
 * Reads a JWT expiry without verifying the token, solely to bound local cache.
 * Authentication still occurs at Bus API; this parser never trusts JWT claims
 * for authorization decisions.
 *
 * @param token - Auth0 access token, which may be opaque rather than a JWT.
 * @param nowMs - Current epoch time used to calculate remaining lifetime.
 * @returns Whole cacheable seconds with a 60-second safety margin, or undefined
 * when the token has no readable numeric exp claim.
 */
export function jwtCacheLifetimeSeconds(
  token: string,
  nowMs = Date.now(),
): number | undefined {
  const payloadPart = token.split(".")[1];
  if (!payloadPart) {
    return undefined;
  }

  try {
    const decoded = JSON.parse(
      Buffer.from(payloadPart, "base64url").toString("utf8"),
    ) as unknown;
    if (
      typeof decoded !== "object" ||
      decoded === null ||
      !("exp" in decoded) ||
      typeof decoded.exp !== "number" ||
      !Number.isFinite(decoded.exp)
    ) {
      return undefined;
    }
    return Math.max(0, Math.floor(decoded.exp - nowMs / 1000 - 60));
  } catch {
    return undefined;
  }
}

/** Auth0 client-credentials token provider with bounded in-memory caching. */
export class M2MTokenProvider {
  private client?: M2MClient;

  /** Accepts scanner auth settings and its existing HTTP dependency; no I/O or errors at construction. */
  constructor(private readonly config: AppConfig["auth"], http: AxiosInstance) { void http; }

  /** Returns the service token through tc-core; rejects on missing credentials or token endpoint failure. */
  async getToken(): Promise<string> {
    this.client ??= createM2MClient({
      AUTH0_URL: this.config.url,
      AUTH0_AUDIENCE: this.config.audience,
      AUTH0_PROXY_SERVER_URL: this.config.proxyUrl,
      TOKEN_CACHE_TIME: this.config.tokenCacheSeconds,
    });
    return this.client.getMachineToken(this.config.clientId || '', this.config.clientSecret || '');
  }
}
