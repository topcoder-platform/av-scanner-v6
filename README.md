# AV Scanner v6

`av-scanner-v6` is the Node 22/TypeScript replacement for
`submission-scanner-processor`. It consumes the existing `avscan.action.scan`
contract with `@platformatic/kafka`, streams S3 objects through a ClamAV
sidecar, optionally moves them to clean or quarantine storage, and delivers the
same Bus API or webhook result contract.

The `/health` endpoint gates initial readiness on both ClamAV and Kafka. Every
request sends a new bounded `PING` command to `clamd`, and the endpoint remains
503 until the first Kafka consume stream has joined successfully. Kafka
readiness is then latched so later broker reconnects and group rebalances do not
cause ECS task churn.

## Runtime and processing flow

- Node.js `22.23.1`, pinned by `.nvmrc` and the container image manifest digest.
- ClamAV `1.5.3`, pinned by the sidecar image manifest digest.
- TypeScript with ESM output.
- `@platformatic/kafka` with `autocommit: false`, committed offsets, and the
  legacy `latest` fallback for a new consumer group.
- AWS SDK v3 `HeadObject` before `GetObject`; object bodies are never buffered.
- Partition-aware Kafka scheduling preserves offset order within each partition
  while allowing up to `SCAN_CONCURRENCY` records from different partitions to
  progress. A second FIFO semaphore applies the same hard limit at clamd.
- Fetch requests capture request-time membership and assignment activity. A
  response that completes after a membership change—or began before SyncGroup
  activated the assignment—is discarded before stream offsets advance. A
  pinned Platformatic patch serializes the initial and replacement
  committed-offset refreshes, preserves refreshes queued by overlapping joins,
  and suppresses terminal replay during stream shutdown. The first Fetch for a
  replacement assignment therefore cannot run with stale offsets or stall on
  a failed refresh.
- Every scheduled record is then fenced to its fetch-time group generation,
  member, coordinator, and partition assignment. Commits use one
  generation-pinned OffsetCommit request, so revoked queued work cannot start
  and an in-flight record cannot silently rejoin and commit under a newer
  generation. Broker acceptance is final even if local membership changes
  immediately afterward.
- Clamd `INSTREAM` frames are bounded to 64 KiB and the complete scan has a
  configurable timeout.
- Invalid JSON, envelope-topic mismatches, and deterministic schema failures are
  logged, discarded, and committed so one poison event cannot restart-loop the
  task. Operational failures stop intake and leave the failed offset
  uncommitted; already-active work in other partitions is allowed to settle.

## Install, verify, and run

Always select the project Node version first:

```bash
nvm use
pnpm install --frozen-lockfile
pnpm lint
pnpm typecheck
pnpm build
pnpm test
pnpm start
```

For local ClamAV plus the app, copy `.env.example` to `.env`, supply Kafka and
AWS/Auth0 values, then run:

```bash
docker compose up --build
```

The dev deployment runs as the dedicated `av-scanner-v6` ECS service and task
family in the `av-scanner-service-serverless` cluster, owned by the
`av-scanner-v6-dev` CloudFormation stack. The legacy
`file-scanning-processor-svc` service is retained at desired count zero only as
a rollback source. See [ECS_Deployment.md](ECS_Deployment.md) for the topology,
zero-overlap cutover, validation, and rollback procedure.

ClamAV may take several minutes to download and load its initial signature
database. The Compose and ECS examples use a 180-second health start period.
Compose health checks provide readiness gating and health visibility; an
unhealthy status alone does not trigger `restart: unless-stopped`. That restart
policy acts when a container process exits. `depends_on` waits for the initial
ClamAV healthy state before starting the app, but does not restart the app if
ClamAV later becomes unhealthy.

## Input event contract

The active `review-api-v6` and admin UI producers send this shape:

```json
{
  "topic": "avscan.action.scan",
  "originator": "review-api-v6",
  "timestamp": "2026-07-19T00:00:00.000Z",
  "mime-type": "application/json",
  "payload": {
    "submissionId": "submission-id",
    "url": "https://s3.amazonaws.com/source-bucket/submission.zip",
    "fileName": "submission.zip",
    "moveFile": true,
    "cleanDestinationBucket": "clean-bucket",
    "quarantineDestinationBucket": "quarantine-bucket",
    "callbackOption": "kafka",
    "callbackKafkaTopic": "submission.scan.complete"
  }
}
```

The envelope is strict. Unknown payload business fields are retained. Callback
mode, webhook method, and webhook authentication values are normalized to
lowercase.

Supported callback modes are:

- `kafka`: post a complete result envelope to `BUSAPI_EVENTS_URL`, authenticated
  with an Auth0 client-credentials token. Bus API publishes it to
  `callbackKafkaTopic`.
- `webhook`: send the result payload directly with `get` or `post` and
  `no-auth`, `bearer`, `basic`, or `api-key` authentication.
- `no-callback`: complete scanning and optional movement without notification.

When `moveFile` is true, both destination buckets are required. The source is
copied to the selected bucket under `fileName`, the callback succeeds, and the
Kafka offset commits before the original source is deleted. This ordering keeps
the source available if callback delivery or commit must retry. Whitelists
restrict values when configured; an empty list retains legacy unrestricted
behavior.

## Results and failure behavior

A normal callback to `submission.scan.complete` retains the existing envelope:

```json
{
  "topic": "submission.scan.complete",
  "originator": "file-scanner-processor",
  "timestamp": "2026-07-19T00:00:01.000Z",
  "mime-type": "application/json",
  "payload": {
    "submissionId": "submission-id",
    "url": "https://s3.amazonaws.com/clean-bucket/submission.zip",
    "fileName": "submission.zip",
    "status": "scanned",
    "isInfected": false
  }
}
```

The callback payload omits scanner controls (`moveFile`, destination buckets,
and callback settings). Infected objects use `isInfected: true` and are routed
to quarantine when movement is enabled.

Moved result URLs percent-encode each `fileName` path segment. This intentionally
hardens the legacy raw-string URL for keys containing spaces, Unicode, `#`, `?`,
or `%`; downstream consumers should treat the value as a URL rather than compare
it byte-for-byte with the request filename.

The default maximum is exactly 500 MiB. A larger S3 object is rejected from
metadata without opening its body. A clamd `INSTREAM` size-limit error is
handled the same way. Both produce a fail-closed result:

```json
{
  "status": "scan-failed",
  "isInfected": true,
  "scanError": "file-size-exceeded",
  "scanErrorMessage": "diagnostic detail"
}
```

S3, ClamAV, destination-copy, callback, and commit errors leave the failed Kafka
offset uncommitted. Per-partition ordering prevents a later record in that
partition from committing past it. Callback or commit failure leaves the source
object in place; repeating the destination copy is safe because it targets the
same bucket and key. After a successful commit, source deletion is best-effort:
a deletion failure is logged and can leave the original copy behind, but the
completed event is not replayed.

Kafka processing and external callbacks are intentionally at-least-once. A
process crash or forced task stop after a remote callback succeeds but before
its offset commits can deliver that callback again on replay. Callback
consumers should deduplicate using stable business identifiers such as
`submissionId`; no Kafka consumer can make an arbitrary HTTP side effect and a
Kafka offset commit atomic.

## Health and ECS replacement

`GET /health` and `HEAD /health` return 200 only after a fresh clamd PING and
the initial Kafka consume stream has joined:

```json
{ "status": "ok", "checks": { "clamav": { "status": "up" } } }
```

If clamd is dead, starting, unreachable, slow, or returns malformed protocol
data, the endpoint returns 503:

```json
{ "status": "unhealthy", "checks": { "clamav": { "status": "down" } } }
```

While ClamAV is up but the initial Kafka join is still pending, it returns 503:

```json
{
  "status": "unhealthy",
  "checks": {
    "clamav": { "status": "up" },
    "kafka": { "status": "starting" }
  }
}
```

The app and ClamAV containers must both be `essential` in the ECS task. ECS does
not import a Dockerfile health check automatically, so the registered task
definition must declare the app container health command. Ready-to-adapt
artifacts are provided as a registration-ready Fargate skeleton in
`deployment/ecs-task-definition.example.json` and as a container-only fragment
in `deployment/ecs-container-definitions.example.json`. Replace their account,
region, role, image, logging, environment, and secret placeholders before use.
Remove optional secret entries that the deployment does not use, such as mTLS
material when that feature is disabled.

The examples set the app container's `stopTimeout` to the Fargate maximum of
120 seconds. This allows the SIGTERM handler's bounded in-flight Kafka drain
and resource-close phases to finish before ECS sends SIGKILL. ECS reverses the
startup dependency during a normal task shutdown, so the app is stopped before
its `filescanner` dependency and clamd remains available while active scans
settle. Use Fargate Linux platform version 1.3.0 or later for `stopTimeout`;
this ordering cannot preserve clamd if the sidecar itself crashes.

On SIGTERM, the app closes Kafka intake, skips prefetched work that has not
started, and gives already-running handlers up to 100 seconds to finish and
commit. Resource close then has a separate 10-second deadline. A single scan
may be configured for longer than that window (the default scan deadline is 300
seconds), so `stopTimeout` is a bounded best effort rather than an exactly-once
guarantee: ECS can still force-stop a long-running handler, leaving its offset
for the replacement task under the at-least-once behavior above.

The health command checks `/health` every 30 seconds with a five-second ECS
timeout; the application probe itself times out sooner
(`CLAMAV_HEALTH_TIMEOUT_MS`, default 2000 ms). After three failures, ECS marks
the essential app container unhealthy. An ECS Service with a desired count
greater than zero then stops and replaces that task. Health failure alone does
not stop or replace a standalone `run-task` task; it remains unhealthy until the
container recovers, exits, or external automation stops it. A ClamAV container
exit stops the task because the sidecar is essential. Confirm that the live
Service is using the newly registered task-definition revision—the example
files do not deploy themselves.

For ECS `awsvpc` tasks, set `CLAMAV_HOST=127.0.0.1` because containers share a
network namespace. Docker Compose uses the `filescanner` service hostname.
The ClamAV image defaults to `127.0.0.1:3310`, keeping its unauthenticated ECS
listener off the task ENI. Docker Compose explicitly builds it with
`CLAMAV_TCP_ADDR=0.0.0.0` so the app can reach it across the private Compose
bridge, and deliberately publishes no host port. Do not add an ECS port mapping
or security-group ingress for 3310; allow only the traffic needed for the app
health endpoint or load balancer.

Before registering the task definition, provide real values for every
placeholder, ensure the execution role can pull images and resolve referenced
secrets, and ensure the task role can read, copy, and delete the configured S3
objects. Run the scanner as an ECS Service when unhealthy-task replacement is
required. The initial Kafka join participates in readiness; after that one-way
latch, alarm separately on Kafka consumer errors or lag.

## Configuration

| Variable                                      | Default                  | Purpose                                                    |
| --------------------------------------------- | ------------------------ | ---------------------------------------------------------- |
| `LOG_LEVEL`                                   | `info`                   | JSON logger level                                          |
| `HOST` / `PORT`                               | `0.0.0.0` / `3000`       | Health server bind address                                 |
| `KAFKA_URL`                                   | `localhost:9092`         | Broker list; `kafka://` and `kafka+ssl://` cannot be mixed |
| `KAFKA_CLIENT_ID`                             | `av-scanner-v6`          | Kafka client identifier                                    |
| `KAFKA_GROUP_ID`                              | `file-scanner-processor` | Existing consumer group                                    |
| `KAFKA_ORIGINATOR`                            | `file-scanner-processor` | Bus callback originator                                    |
| `AVSCAN_TOPIC`                                | `avscan.action.scan`     | Input topic                                                |
| `KAFKA_CLIENT_CERT` / `KAFKA_CLIENT_CERT_KEY` | unset                    | Optional mTLS PEM or file paths; must be paired            |
| `KAFKA_CA_CERT`                               | unset                    | Optional CA PEM or file path                               |
| `KAFKA_CLIENT_CA`                             | unset                    | Legacy alias for `KAFKA_CA_CERT`                           |
| `KAFKA_REJECT_UNAUTHORIZED`                   | unset                    | TLS verification override; setting it also enables TLS     |
| `KAFKA_SESSION_TIMEOUT_MS`                    | `30000`                  | Consumer group session timeout                             |
| `KAFKA_HEARTBEAT_INTERVAL_MS`                 | `3000`                   | Consumer heartbeat interval                                |
| `KAFKA_MAX_BYTES`                             | `10485760`               | Maximum Kafka fetch size                                   |
| `KAFKA_MAX_WAIT_TIME_MS`                      | `500`                    | Maximum Kafka fetch wait                                   |
| `AWS_REGION`                                  | `us-east-1`              | Required S3 URL and client region                          |
| `MAX_SCAN_FILE_SIZE_BYTES`                    | `524288000`              | Preflight object limit                                     |
| `SCAN_CONCURRENCY`                            | `1`                      | Concurrent scans per task                                  |
| `CLAMAV_HOST` / `CLAMAV_PORT`                 | `filescanner` / `3310`   | Clamd TCP endpoint                                         |
| `CLAMAV_HEALTH_TIMEOUT_MS`                    | `2000`                   | Per-request PING timeout                                   |
| `CLAMAV_SCAN_TIMEOUT_MS`                      | `300000`                 | Complete scan deadline                                     |
| `WHITELISTED_CLEAN_BUCKETS`                   | empty                    | Comma-separated clean buckets                              |
| `WHITELISTED_QUARANTINE_BUCKETS`              | empty                    | Comma-separated quarantine buckets                         |
| `WHITELISTED_KAFKA_TOPICS`                    | empty                    | Comma-separated callback topics                            |
| `BUSAPI_EVENTS_URL`                           | Topcoder dev events URL  | Complete Bus API events endpoint                           |
| `AUTH0_URL`                                   | unset                    | Complete Auth0 token endpoint                              |
| `AUTH0_AUDIENCE`                              | unset                    | Bus API token audience                                     |
| `AUTH0_CLIENT_ID` / `AUTH0_CLIENT_SECRET`     | unset                    | Client credentials for kafka callbacks                     |
| `AUTH0_PROXY_SERVER_URL`                      | unset                    | Optional Topcoder token proxy endpoint                     |
| `TOKEN_CACHE_TIME`                            | `300`                    | Maximum token cache lifetime in seconds                    |
| `WEBHOOK_TIMEOUT_MS`                          | `15000`                  | HTTP callback/Auth0 timeout                                |

`kafka+ssl://` enables TLS and defaults to peer verification with Node's system
trust store. Bare brokers alongside it inherit the same TLS transport because
the Kafka client has one transport setting; explicit `kafka://` and
`kafka+ssl://` brokers therefore cannot be mixed. Setting
`KAFKA_REJECT_UNAUTHORIZED` by itself also enables TLS. Supplying legacy TLS
material to a bare or `kafka://` broker remains supported and retains the
historical `false` verification default when no override is set. Production
deployments using that compatibility path should supply a trusted CA and set
`KAFKA_REJECT_UNAUTHORIZED=true`; CA-only server-authenticated TLS is supported
without a client certificate.

AWS credentials use the standard AWS SDK provider chain and are not scanner
configuration.
