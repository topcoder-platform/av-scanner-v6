import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import test from "node:test";
import nock from "nock";
import { M2MTokenProvider } from "../src/auth.js";
import { loadConfig } from "../src/config.js";

/**
 * Signs synthetic RSA service claims for shared provider integration checks.
 * @param exp Token expiry in seconds. @returns Token and corresponding public JWK.
 * @throws Crypto errors if local key generation fails; no network or real credentials.
 */
function fixture(exp: number) {
  const keys = generateKeyPairSync("rsa", {modulusLength:2048});
  const header = Buffer.from(JSON.stringify({alg:"RS256",kid:"test-key"})).toString("base64url");
  const body = Buffer.from(JSON.stringify({iss:"https://keycloak.test/realms/test",aud:"https://bus.test",
    sub:"service-account",azp:"test-client",gty:"client-credentials",scope:"write:bus_api",
    iat:Math.floor(Date.now()/1000)-10,exp})).toString("base64url");
  const value = `${header}.${body}`;
  return {token:`${value}.${sign("RSA-SHA256",Buffer.from(value),keys.privateKey).toString("base64url")}`,
    jwk:{...keys.publicKey.export({format:"jwk"}),kid:"test-key",alg:"RS256",use:"sig"}};
}

const config = loadConfig({M2M_PROVIDER:"keycloak",KEYCLOAK_ISSUER:"https://keycloak.test/realms/test",
  KEYCLOAK_AUDIENCE:"https://bus.test",KEYCLOAK_TOKEN_URL:"https://keycloak.test/m2m/token",
  KEYCLOAK_CLIENT_ID:"test-client",KEYCLOAK_CLIENT_SECRET:"synthetic-secret",KEYCLOAK_SCOPE:"write:bus_api"});

void test("scanner issues only through the fixed Keycloak endpoint and caches verified tokens", async (t) => {
  const data = fixture(Math.floor(Date.now()/1000)+300);
  nock("https://keycloak.test").get("/realms/test/protocol/openid-connect/certs")
    .reply(200,{keys:[data.jwk]});
  t.after(() => nock.cleanAll());
  let issues=0;
  t.mock.method(globalThis,"fetch",(input: string | URL | Request, init?: RequestInit) => {
    const url=typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    assert.equal(url,config.auth.KEYCLOAK_TOKEN_URL);
    assert.equal(typeof init?.body,"string");
    const body=JSON.parse(init?.body as string) as Record<string,unknown>;
    assert.deepEqual(body,{grant_type:"client_credentials",client_id:"test-client",client_secret:"synthetic-secret",audience:"https://bus.test",scope:"write:bus_api"});
    assert.equal(init?.redirect,"error");
    issues++;
    return Promise.resolve(Response.json({access_token:data.token,expires_in:300}));
  });
  const provider=new M2MTokenProvider(config.auth);
  assert.equal(await provider.getToken(),data.token);
  assert.equal(await provider.getToken(),data.token);
  assert.equal(issues,1);
});

void test("scanner rejects expired returned tokens instead of caching or forwarding them", async (t) => {
  const data=fixture(Math.floor(Date.now()/1000)-120);
  nock("https://keycloak.test").get("/realms/test/protocol/openid-connect/certs")
    .reply(200,{keys:[data.jwk]});
  t.after(() => nock.cleanAll());
  let issues=0;
  t.mock.method(globalThis,"fetch",() => {
    issues++;
    return Promise.resolve(Response.json({access_token:data.token,expires_in:300}));
  });
  const provider=new M2MTokenProvider(config.auth);
  await assert.rejects(provider.getToken(),/Keycloak M2M token request failed/);
  await assert.rejects(provider.getToken(),/Keycloak M2M token request failed/);
  assert.equal(issues,2);
});

void test("scanner refuses Auth0-only configuration", () => {
  const provider=new M2MTokenProvider(loadConfig({AUTH0_URL:"https://auth0.test/token"}).auth);
  assert.throws(()=>provider.getToken(),/M2M_PROVIDER/);
});
