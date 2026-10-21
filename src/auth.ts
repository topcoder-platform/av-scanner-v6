import core from "tc-core-library-js";
import type { AppConfig } from "./config.js";

/** Shared Keycloak token provider for Bus callbacks, with validated tokens and isolated caching. */
export class M2MTokenProvider {
  private provider?: ReturnType<typeof core.auth.m2m>;

  /** Stores explicit trust/credential settings; validation occurs on the first Kafka callback. */
  constructor(private readonly config: AppConfig["auth"]) {}

  /**
   * Gets a verified, cached Keycloak token immediately before publishing a Bus event.
   * Takes no arguments; webhook-only callbacks do not request a token.
   * @returns Bearer access token without a prefix.
   * @throws Error for missing/invalid settings or failed issuance/verification; never falls back to Auth0.
   */
  getToken(): Promise<string> {
    this.provider ??= core.auth.m2m(this.config);
    return this.provider.getMachineToken();
  }
}
