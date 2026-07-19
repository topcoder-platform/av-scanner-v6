import type { AxiosInstance } from "axios";

import type { AppConfig } from "./config.js";

interface TokenResponse {
  access_token?: string;
  expires_in?: number;
}

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
  private cachedToken?: string;
  private cacheExpiresAt = 0;

  /**
   * Creates a token provider compatible with direct Auth0 and Topcoder's proxy.
   *
   * @param config - Auth0 URL, audience, credentials, proxy, and cache settings.
   * @param http - Axios client with the application's request timeout.
   */
  constructor(
    private readonly config: AppConfig["auth"],
    private readonly http: AxiosInstance,
  ) {}

  /**
   * Gets a cached bearer token or requests a new client-credentials token.
   * ResultService uses this immediately before posting an event to Bus API.
   *
   * @returns Auth0 access token without a Bearer prefix.
   * @throws When credentials are missing or the token endpoint fails.
   */
  async getToken(): Promise<string> {
    if (this.cachedToken && Date.now() < this.cacheExpiresAt) {
      return this.cachedToken;
    }

    const { audience, clientId, clientSecret, proxyUrl, url } = this.config;
    if (!url || !audience || !clientId || !clientSecret) {
      throw new Error(
        "AUTH0_URL, AUTH0_AUDIENCE, AUTH0_CLIENT_ID, and AUTH0_CLIENT_SECRET are required for kafka callbacks",
      );
    }

    const response = await this.http.post<TokenResponse>(proxyUrl ?? url, {
      audience,
      auth0_url: url,
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: "client_credentials",
    });
    if (!response.data.access_token) {
      throw new Error("Auth0 token response did not include access_token");
    }

    const jwtLifetime = jwtCacheLifetimeSeconds(response.data.access_token);
    const responseLifetime =
      typeof response.data.expires_in === "number"
        ? Math.max(0, response.data.expires_in - 60)
        : undefined;
    const knownTokenLifetime = responseLifetime ?? jwtLifetime ?? 0;
    const cacheLifetime = Math.min(
      knownTokenLifetime,
      jwtLifetime ?? knownTokenLifetime,
      this.config.tokenCacheSeconds,
    );

    this.cachedToken = response.data.access_token;
    this.cacheExpiresAt = Date.now() + cacheLifetime * 1000;
    return this.cachedToken;
  }
}
