# Keycloak M2M migration branch

This code is isolated on `keycloak_m2M`. Do not deploy this branch until the
workload and its callers are ready. Existing DEV services continue using Auth0.
The core dependency is pinned to a reviewed Keycloak prerelease commit, including
transitive wrapper uses through the pnpm override. No npm latest dependency is used.

Required settings are `M2M_PROVIDER=keycloak`, `KEYCLOAK_ISSUER`,
`KEYCLOAK_AUDIENCE`, and `KEYCLOAK_TOKEN_URL`; issuing workloads additionally need
`KEYCLOAK_CLIENT_ID` and `KEYCLOAK_CLIENT_SECRET` from their own encrypted secret
references. Optional `KEYCLOAK_SCOPE` narrows the client's approved defaults.
The DEV issuer is `https://keycloak.topcoder-dev.com/realms/topcoder-m2m-dev`,
audience `https://api.topcoder-dev.com`, and managed endpoint
`https://keycloak.topcoder-dev.com/m2m/token`. Wait for the managed endpoint's
recorded deployment acceptance before enabling this configuration.

Member sessions remain Auth0. Keep `VALID_ISSUERS` and `AUTH_SECRET` where legacy
member HMAC tokens are supported. Set `AUTH_USER_AUDIENCES` to a JSON array, or
explicitly retain existing user-token audience behavior with
`ALLOW_LEGACY_USER_AUDIENCE=true`. Never apply that exception to M2M tokens.
During an approved overlap, `LEGACY_M2M_ISSUERS` and `LEGACY_M2M_AUDIENCE` allow
old Auth0 machine tokens to pass the same normalizer. Issuance remains Keycloak-only.

`tc-core-library-js` owns signature, issuer, audience, lifetime, and machine/user
classification. Service adapters must use its verified `kind`, `isMachine`, and
`scopes` rather than decode/reclassify claims. Scope guards delegate exact matching
to the library with explicit all/any policy. Existing business roles and documented
scope aliases remain endpoint policy; adding OAuth scopes to a user token never
grants machine access. No permissive fallback or synthetic token path is available.

Before rollout run this project's `nvm use`, `pnpm lint`, `pnpm build`, and targeted
authentication tests. Validate real authorized/unauthorized routes with both a
Keycloak service token and a member token, including wrong audiences and insufficient
scopes. Switch only after every downstream receiver accepts the issuer. Rollback
requires the prior application/dependency and credentials with equivalent grants;
never automatically retry issuance against Auth0.

The scanner is an outbound-only consumer. Keycloak configuration is required when
Kafka callbacks publish through Bus API. The shared provider owns all token
verification/caching; no decoded-token or opaque-token cache path remains.

Before rollout, populate the scanner runtime parameter prefix with M2M_PROVIDER,
KEYCLOAK_ISSUER/TOKEN_URL/AUDIENCE/CLIENT_ID/CLIENT_SECRET. The branch templates
reference those dedicated names; existing common Auth0 parameters and CI deploy
script credentials remain unchanged. Templates have not been applied.

Validation: Lint/build and all 94 tests pass, including signed RSA issuance/caching and expired-returned-token rejection. CloudFormation template lint passes. Compiled provider issued and cached a real DEV Keycloak token. No application or template deployment.
