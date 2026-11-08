# Rustash

**An ultra-lightweight, open-source QStash alternative built for self-hosted LobeHub.**

Rustash is a drop-in queue for a single self-hosted [LobeHub](https://github.com/lobehub/lobehub) deployment. It replaces the hosted QStash queue that LobeHub uses for async agent steps, webhooks, schedules and workflows, with **one Rust binary + one SQLite file in WAL mode + one Docker image**, and **no external service dependencies**: no external queue, broker or Redis. It is built for one LobeHub instance on one machine, with ~10 MiB idle RSS; it is not a multi-node or multi-tenant cluster.

- **License:** MIT — see [LICENSE](LICENSE)
- **Toolchain:** Rust 1.98.1 (pinned in `rust-toolchain.toml`)
- **Image:** `hardyone/rustash:latest`

> Rustash implements the QStash **HTTP API surface that LobeHub uses** (`/v2` mirrored under `/v1`), not the Upstash cloud control plane. See **§8 Scope and limits**.

---

## 1. Configure LobeHub to use Rustash

Point LobeHub at Rustash and share **one token plus one signing-key pair**. Every value below is a placeholder you generate yourself (see *Generate the secrets*).

### LobeHub side

Configure the official LobeHub container with its standard QStash and application URL variables. In Docker Compose, use the service names on the shared network:

| LobeHub env var | Set it to | What it does |
|---|---|---|
| `QSTASH_URL` | `http://rustash:8080` | Base URL of Rustash. The `@upstash/qstash` SDK reads it directly; **do not** append `/v2/publish`. |
| `QSTASH_TOKEN` | `<your-secret-token>` | Bearer token LobeHub uses to publish. Must be **identical** to Rustash's `QSTASH_TOKEN`. |
| `QSTASH_CURRENT_SIGNING_KEY` | `<your-signing-key>` | LobeHub's `Receiver` verifies `Upstash-Signature` on incoming deliveries. Must be **identical** to Rustash's value. |
| `QSTASH_NEXT_SIGNING_KEY` | `<your-next-signing-key>` | Rotation key for the same `Receiver`. Must be **identical** to Rustash's value. |
| `INTERNAL_APP_URL` | `http://lobe:3210` | Internal LobeHub base URL used to build workflow callbacks inside the Compose network. |
| `APP_URL` | `http://localhost:3210` | Public LobeHub base URL. Keep this set to the address users use to reach LobeHub in your deployment. |
| `AGENT_RUNTIME_MODE` | `queue` | Required by queue based Agent Runtime deployments; it enables LobeHub to publish to the configured QStash endpoint. |

The official LobeHub image uses `INTERNAL_APP_URL || APP_URL` when choosing the application URL for internal calls. With `INTERNAL_APP_URL=http://lobe:3210`, workflow callback URLs such as `http://lobe:3210/api/workflows/...` stay on the Docker network and do not loop through an external reverse proxy. Rustash must have `ALLOW_PRIVATE_DESTINATIONS=true` to deliver to this private container address.

### Rustash side

| Rustash env var | Set it to | Note |
|---|---|---|
| `QSTASH_TOKEN` | same `<your-secret-token>` | Bootstraps the default tenant; without it every API call returns 401. Also used by `rs-check`. |
| `QSTASH_CURRENT_SIGNING_KEY` | same `<your-signing-key>` | HS256 key Rustash signs `Upstash-Signature` with. |
| `QSTASH_NEXT_SIGNING_KEY` | same `<your-next-signing-key>` | Rotation key. |
| `ALLOW_PRIVATE_DESTINATIONS` | `true` | **Required for self-hosted LobeHub.** See the next subsection. |
| `DATABASE_URL` | `sqlite:///data/rstash.sqlite3` | Persistent SQLite path on a writable volume. |

**The one rule that matters:** the token and both signing keys are **the same values on both sides**. LobeHub signs nothing; it *verifies* what Rustash signs. If the two sides disagree, LobeHub rejects every delivery as an invalid signature.

### Why `ALLOW_PRIVATE_DESTINATIONS=true` is required

Rustash refuses, by default, to deliver to loopback and private (RFC1918) addresses — an SSRF guard. In the Compose deployment, LobeHub workflow callbacks use `http://lobe:3210` on the private Docker network. With the default `false`, Rustash accepts the publish and then refuses to POST the delivery to LobeHub, so async steps and webhooks never run.

Set `ALLOW_PRIVATE_DESTINATIONS=true` on **Rustash** so it can deliver into your private network. Cloud metadata ranges (for example `169.254.169.254`) stay blocked regardless. Only enable it on a network you control, and keep Rustash's port off the public internet: in Compose it needs no host port at all (see **§2**).

### Callback reachability (Docker Compose)

The official LobeHub image uses `INTERNAL_APP_URL` in preference to `APP_URL` for internal application URLs. Set `INTERNAL_APP_URL=http://lobe:3210` so workflow callbacks target the LobeHub service directly, for example `http://lobe:3210/api/workflows/...`. This avoids sending callbacks through an external reverse proxy and back into the same Compose stack. Rustash must be configured with `ALLOW_PRIVATE_DESTINATIONS=true` to permit delivery to that private address.

`QSTASH_URL` (LobeHub → Rustash) and `INTERNAL_APP_URL` (Rustash → LobeHub callback destination as selected by LobeHub) serve opposite directions; each must resolve from the container that opens the connection.

### Generate the secrets

Use three independent random values; never commit them (`.env` is git-ignored):

```sh
openssl rand -hex 32   # QSTASH_TOKEN
openssl rand -hex 32   # QSTASH_CURRENT_SIGNING_KEY
openssl rand -hex 32   # QSTASH_NEXT_SIGNING_KEY
```

> **Set the keys before Rustash's first start.** Rustash writes the signing keys into SQLite when it first creates the default tenant; changing the env vars later does **not** update an existing database (use `POST /v2/keys/rotate`, or start from a fresh database, which discards pending messages). This is the most common cause of an "invalid signature" after a first boot without keys.

### Verify the wiring

```sh
# Rustash reachable from inside the Compose network (the LobeHub image has Node)
docker compose exec lobe node -e "fetch('http://rustash:8080/v2/health').then(r=>r.text()).then(console.log)"
# {"status":"ok"}

# From the host, if you kept the optional 8081 mapping
curl -fsS http://127.0.0.1:8081/v2/health

# Rustash-side delivery health (uses QSTASH_TOKEN from the service env)
docker compose exec rustash rs-check --all
```

---

## 2. Docker Compose: LobeHub + Rustash together

The repository ships [`docker-compose.yml`](docker-compose.yml), a Compose **overlay** for LobeHub's official deployment. It merges the QStash wiring into the official `lobe` service and reuses its `lobe-network`; the official PostgreSQL, Redis, S3/RustFS and gateway services remain defined by LobeHub.

In the official deployment directory, append these shared values to the existing `.env` (git-ignored). Keep your real LobeHub database, Redis, S3 and gateway settings from the official deployment:

```dotenv
QSTASH_TOKEN=replace-with-a-long-random-token
QSTASH_CURRENT_SIGNING_KEY=replace-with-a-random-signing-key
QSTASH_NEXT_SIGNING_KEY=replace-with-a-random-rotation-key
APP_URL=http://localhost:3210
```

Then, from LobeHub's `docker-compose/deploy` directory, start both compose files together (replace the absolute path with your clone location):

```sh
docker compose \
  -f docker-compose.yml \
  -f /path/to/rustash-public/docker-compose.yml \
  up -d
docker compose ps
docker compose exec rustash rs-check --all
```

In the overlay:

- `rustash` uses `DATABASE_URL=sqlite:///data/rstash.sqlite3` on a named volume and `ALLOW_PRIVATE_DESTINATIONS=true` so it can deliver to private addresses on the Docker network.
- `lobe` gets `QSTASH_URL=http://rustash:8080`, `AGENT_RUNTIME_MODE=queue`, the shared token/signing keys, `INTERNAL_APP_URL=http://lobe:3210`, and `APP_URL=http://localhost:3210`.
- LobeHub uses the internal service URL to build workflow callback targets such as `http://lobe:3210/api/workflows/...`; Rustash's `ALLOW_PRIVATE_DESTINATIONS=true` permits those callbacks without routing through an external reverse proxy.
- `lobe` waits for `rustash: service_healthy` (the Rustash image has a built-in healthcheck).

If LobeHub runs **outside** Compose (bare metal or another host), set `QSTASH_URL=http://<rustash-host>:8080`, publish Rustash's 8080 on a host port, and set `INTERNAL_APP_URL` to an address reachable from Rustash for workflow callbacks. Keep `APP_URL` set to the public address used by LobeHub.

---

## 3. Run Rustash on its own

```sh
docker run -d --name rustash --restart unless-stopped \
  -p 8080:8080 -v rustash-data:/data \
  -e DATABASE_URL='sqlite:///data/rstash.sqlite3' \
  -e QSTASH_TOKEN='<your-secret-token>' \
  -e QSTASH_CURRENT_SIGNING_KEY='<your-signing-key>' \
  -e QSTASH_NEXT_SIGNING_KEY='<your-next-signing-key>' \
  -e ALLOW_PRIVATE_DESTINATIONS='true' \
  hardyone/rustash:latest

curl -fsS http://127.0.0.1:8080/v2/health   # {"status":"ok"}
```

The image runs as UID 10001 and writes to `/data`; a named volume inherits that ownership, so no `chown` is needed. With a bind mount, make the host directory writable by UID 10001 (add `:z` on SELinux hosts). A ready-made single-service file is [`compose.rustash.yml`](compose.rustash.yml).

Publish a message:

```sh
curl -sS -X POST 'http://127.0.0.1:8080/v2/publish/http://127.0.0.1:9000/hook' \
  -H "Authorization: Bearer <your-secret-token>" \
  -H 'Content-Type: application/json' -d '{"hello":"world"}'
# {"messageId":"msg_..."}
```

---

## 4. Features

- **QStash v2 / v1 API compatible** — the same route table is served under `/v2` and mirrored under `/v1`: publish, batch publish, enqueue, messages, DLQ, keys, queues, topics, schedules, logs, events and workflow runs.
- **Delayed delivery & schedules** — `Upstash-Not-Before` / delay, cron schedules with pause and resume.
- **Automatic retries** — exponential backoff, `Upstash-Retry-Delay` expressions, `Retry-After` and `X-RateLimit-*` overrides, non-retryable markers, fenced leases with renewal.
- **Flow control** — per-queue and per-key parallelism, token-bucket rate limits, pause/resume, pin/unpin and rate reset.
- **Deduplication** — `Upstash-Deduplication-Id` / `Idempotency-Key` and content-based deduplication with a configurable window.
- **Dead letter queue** — exhausted retries keep a full snapshot (body, headers, error); list, inspect and requeue.
- **Workflows** — `Upstash-Workflow-RunId` step replay, `/wait` + `/notify` with a timeout sweep, run listing / replay / cancel.
- **Signature verification & destination safety** — `Upstash-Signature` HS256 JWTs over the delivered bytes, two-key rotation, Bearer or query-token auth, ingest and DNS-resolution SSRF checks, always-blocked cloud metadata, `Upstash-Redact-Fields` masking.
- **Callbacks** — success/failure callbacks with redaction, persisted with fenced completion.
- **`rs-check` health probe** — delivery health, DLQ size, stuck messages, schedules and workflow runs, with JSON output and script-friendly exit codes.

---

## 5. Configuration (Rustash environment variables)

All Rustash settings are environment variables; the authoritative list and defaults live in `crates/rustash/src/config.rs`.

| Variable | Default | Purpose |
|---|---|---|
| `QSTASH_TOKEN` | unset | Bearer token; bootstraps the default tenant. **Required.** |
| `QSTASH_CURRENT_SIGNING_KEY` | generated | HS256 key for `Upstash-Signature`. Set it explicitly, and identically on LobeHub. |
| `QSTASH_NEXT_SIGNING_KEY` | generated | Rotation key. Keys are read on **first** tenant creation only; rotate via `POST /v2/keys/rotate` afterwards. |
| `PORT` | `8080` | HTTP listen port. |
| `DATABASE_URL` | `:memory:` | SQLite path. Accepts a bare path, `sqlite:PATH`, `sqlite://PATH` or `file:PATH`. Use a persistent file on a volume in production. |
| `ALLOW_PRIVATE_DESTINATIONS` | `false` | Allow loopback / RFC1918 destinations. Set `true` for self-hosted LobeHub. |
| `DESTINATION_ALLOW_CIDRS`, `DESTINATION_BLOCK_CIDRS` | empty | Optional CIDR allow/block lists for destinations. |
| `DEFAULT_MAX_RETRIES` (alias `DEFAULT_RETRY`) | `3` | Default retry budget. |
| `MAX_RETRIES` | `100` | Hard upper bound accepted for a message retry count. |
| `MAX_HTTP_TIMEOUT_MS` | `7200000` | Maximum outbound request duration (2 hours). |
| `LEASE_MS` | `960000` | Claim lifetime. `LEASE_RENEW_INTERVAL_MS` defaults to a derived interval when needed; `0` disables renewal and requires the lease to exceed the maximum delivery timeout. |
| `MAX_BODY_BYTES` | `10485760` | Inbound and queued body ceiling (10 MiB). |
| `MAX_RESPONSE_BODY_BYTES` | `1048576` | Maximum endpoint response body retained for delivery processing (1 MiB). |
| `EVENT_BODY_MAX_BYTES` | `32768` | Maximum body copied into a delivery-event row (32 KiB). |
| `DEDUPE_WINDOW_SECS` | `600` | Deduplication window. |
| `DISPATCH_TICK_MS`, `SCHEDULER_TICK_MS` | `500`, `1000` | Dispatch and schedule loop intervals. |
| `RETENTION_TICK_MS` | `60000` | Retention sweep interval (1 minute). |
| `DELIVERY_SLOTS`, `DISPATCH_BATCH_LIMIT` | `256`, `32` | In-flight delivery slots / claim batch. |
| `TENANT_PARALLELISM_MAX` | `1024` | Maximum global parallel deliveries per tenant. |
| `DELIVERY_POOL_MAX_IDLE`, `DELIVERY_POOL_IDLE_MS` | `64`, `4000` | Keep-alive sockets retained per destination / idle timeout (ms). |
| `LOG_RETENTION_DAYS`, `DLQ_RETENTION_DAYS` | `14`, `7` | Retention for event rows and dead letters. |
| `WORKFLOW_RUN_STUCK_HOURS`, `WORKFLOW_RUN_RETENTION_DAYS` | `24`, `7` | Workflow-run stuck detection / retention. |
| `RUN_MIGRATIONS_ON_START` | `true` | Apply SQLite migrations at startup. |
| `ADMIN_TOKEN` | unset | When set, mounts `/admin/tenants`. Leave unset otherwise. |
| `REGION` | `us-east-1` | Default region stored on newly created tenant records. |
| `LOG_LEVEL` | `info` | Tracing filter. |

Keep `QSTASH_TOKEN` and both signing keys out of source control.

---

## 6. Architecture and performance

- **Pure Rust, single crate.** `crates/rustash` builds two binaries: the `rustash` server and `rs-check`. No language runtime, no external database, no broker.
- **SQLite WAL, single writer.** One `Connection` behind one `Mutex` is the only writer, with WAL and `synchronous=NORMAL`. A bounded prepared-statement cache is flushed after migrations, so the hot publish / claim / event path reuses statements instead of re-preparing SQL on every call.
- **Sub-second, millisecond-configurable scheduling.** The dispatch loop ticks every 500 ms and the scheduler every 1000 ms by default (both tunable); fenced leases and lease renewal keep at-least-once delivery from turning into duplicate runs.
- **Keep-alive delivery.** An HTTP/1.1 pool reuses sockets per destination; the SSRF policy is built once at startup.
- **Lightweight.** A local single-node publish benchmark with a fake 200-OK sink recorded **2,500+ requests/second**. Separately, an idle Rustash process used about **10 MiB RSS**. These are workload and host specific observations, not a guaranteed rate or service-level objective. Throughput and RSS vary with hardware, payload size and workload; Rustash targets one LobeHub instance, not a cluster.

---

## 7. Health check (`rs-check`)

`rs-check` queries Rustash with `QSTASH_TOKEN` and reports delivery and event statistics, DLQ size, stuck messages, schedule state and workflow-run summaries. Use `--json` for machine consumption.

```sh
rs-check                                  # http://127.0.0.1:8080, 10m window
rs-check --window 1h --all --json
docker compose exec rustash rs-check --all
```

Options: `--window 5m|10m|1h|6h|1d|all`, `--all`, `--json`, `--url`, `--token`. Exit codes: `0` healthy, `1` delivery issues, `2` check or configuration failure.

---

## 8. Scope and limits

- **Single node, single writer.** No horizontal scaling, no HA, no multi-node claim. One SQLite file, one LobeHub instance.
- **SQLite only.** No PostgreSQL mode; start from a fresh SQLite file.
- **Body size.** Inbound bodies are capped at 10 MiB (`MAX_BODY_BYTES`); the application still owns payload budgeting.
- **Private destinations.** `ALLOW_PRIVATE_DESTINATIONS=true` opens loopback and private delivery; keep Rustash off the public internet and use CIDR allow/block lists to narrow it.
- **API surface.** Covers the QStash API LobeHub uses, not the full Upstash cloud control plane.

---

## 9. Build from source

```sh
git clone https://github.com/DSH244/rustash.git
cd rustash
cargo build --release --locked       # target/release/rustash and target/release/rs-check
cargo test --locked                  # 129 tests
./run_server.sh                      # dev: fresh temporary SQLite on :8080, token printed to stderr
```

`rust-toolchain.toml` pins Rust 1.98.1 with `rustfmt` and `clippy`. Building and testing need no database server, broker or network service. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.

---

## License

MIT. See [LICENSE](LICENSE).
