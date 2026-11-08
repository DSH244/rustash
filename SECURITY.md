# Security policy

## Supported versions

Rustash is pre-1.0 and ships from `main` only. Fixes land on `main`; there are no
maintenance branches, so **only the tip of `main` is supported**. If you run a fork or a
pinned image tag, you are responsible for rebasing onto `main` to pick up a fix.

| Version | Supported |
|---|---|
| `main` (tip) | yes |
| any older commit, tag, or fork | no |

## Reporting a vulnerability

> Report vulnerabilities privately through **GitHub Security Advisories** on this
> repository (*Security → Report a vulnerability*). Do not open a public issue first.

Do not open a public issue for a suspected vulnerability, and do not include live tokens,
signing keys, or customer endpoint URLs in a report. A useful report contains:

- the affected commit (`git rev-parse HEAD`) and how you ran it (binary, `cargo test`, or
  container image);
- the smallest request sequence that reproduces the problem;
- what you expected versus what happened, and why you believe it is a security issue rather
  than a bug (e.g. "the destination gate let this through", not "the request 500s").

We will acknowledge within a reasonable period and coordinate disclosure with you. Given the
project's size, there is no bug bounty.

## Security model and boundaries

This section states what Rustash does and does not defend against. It is deliberately
concrete: everything below is enforced by code in this repository, and the file that
enforces it is named.

### 1. Delivery targets — SSRF policy

A queue that will POST to any URL you name is an SSRF primitive unless it refuses the
addresses that matter. The gate lives in `crates/rustash/src/ssrf.rs` and has two stages.

**Ingest time (structural, `validate_destination_url`).** The destination must parse as a
URL, must be `http` or `https`, and must not carry credentials in the URL. A *literal* IP
host is checked against the policy immediately.

**Delivery time (resolved, `check_resolved_allowed`).** A hostname destination is resolved
before the request is sent and at least one resolved address must be allowed. This is a
best-effort mitigation against DNS rebinding on a single-node deployment: a name that
resolves to a public address at ingest and a private one microseconds later can still slip
through between the resolution check and the connection. There is no connection pinning to
the resolved address in this version. Treat "destination hostname is attacker-controlled"
as out of scope for the current single-tenant deployment.

**What is always blocked**, regardless of configuration
(`METADATA_STR`): `169.254.0.0/16` (link-local, which is where cloud instance metadata
lives) and `fe80::/10`.

**What is blocked by default and can be opened up** (`PRIVATE_STR`): `127.0.0.0/8`,
`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `100.64.0.0/10` (CGNAT),
`0.0.0.0/8`, `::1/128`, `::/128`, `fc00::/7`. These are allowed only when
`ALLOW_PRIVATE_DESTINATIONS=true`. **LobeHub's deployment needs this on**, because the
queue delivers to services on the same Docker network. That single flag is the difference
between a queue and an open request forwarder on the internal network — set it only when
the queue is not reachable by untrusted callers, and never on a host whose private network
you do not control.

`DESTINATION_ALLOW_CIDRS` re-allows a specific range inside a blocked one;
`DESTINATION_BLOCK_CIDRS` always wins over both the allowlist and the allow-private flag
(`is_ip_allowed` checks `block_cidrs` first). Explicit allow/block beats the built-in
ranges in that order.

### 2. Tokens and signing keys

- **`QSTASH_TOKEN` is the sole credential for the `/v2` surface.** It is stored as a hash
  (`ids::hash_token`), never in plaintext; the request token is hashed and looked up in
  `tenants`. It bootstraps one "default" tenant on first start; changing it after the fact
  does not rewrite the stored hash, so rotate through the admin API instead of editing env.
- **Signing keys (`QSTASH_CURRENT_SIGNING_KEY` / `QSTASH_NEXT_SIGNING_KEY`)** are used to
  produce the `Upstash-Signature` JWT on each delivery. `CURRENT` signs; `NEXT` exists so
  receivers can be rotated without a delivery outage. Treat both as secrets: anyone holding
  a signing key can produce a delivery that verifies against your receiver.
- Tokens and keys are logged nowhere. `LOG_LEVEL=debug` does not print them; do not add
  logging that does.

### 3. `ADMIN_TOKEN` — exposure surface

`ADMIN_TOKEN` unlocks tenant management: create, get, delete, rotate-token and rotate-keys
under `/admin/tenants*` (note: `/admin/…`, **not** `/v2/admin/…`; see
`crates/rustash/src/http/admin.rs` and `crates/rustash/src/http/router.rs`).

- **The admin router is not mounted at all when `ADMIN_TOKEN` is unset.** Leaving it unset
  is the safest configuration for a single-tenant deployment and is what
  `compose.rustash.yml` does.
- When set, anyone who can reach the port and present that token can **read signing keys**
  for any tenant, **rotate a tenant's token** (locking out the caller), and **rotate
  signing keys** (breaking every receiver until they adopt the new key). It is a
  full-control credential, equivalent in power to rewriting the tenant table.
- There is no separate admin port and no IP allowlist. Exposure is therefore bounded only
  by the network path to the port: bind to loopback, or keep it on a private Docker
  network, and never publish it to the internet.
- Admin requests are authenticated by Bearer-token comparison; they are not session-based
  and there is no CSRF surface because there are no cookies.

### 4. What this project does not defend against

State these plainly rather than implying protection:

- **Untrusted multi-tenancy.** There is a tenant column and a tenant-scoped query path, but
  the deployment model is one tenant with one token. Isolation between mutually hostile
  tenants on one instance has not been assessed.
- **Compromised receivers.** A destination endpoint that returns attacker-chosen headers
  can steer retry timing (`Retry-After` / `x-ratelimit-reset` headers are honoured), which
  the policy clamps to `[0, 86_400_000] ms` but does not otherwise distrust.
- **Disk-level attackers.** The SQLite file contains message bodies and headers in
  plaintext. File permissions and volume encryption are the operator's responsibility.
- **Denial of service.** Body size (`MAX_BODY_BYTES`, default 10 MiB) and concurrency
  (`DELIVERY_SLOTS`, `TENANT_PARALLELISM_MAX`) are bounded, but there is no per-client rate
  limiting. Do not expose the port to the internet without a proxy that does it.
- **Supply chain.** `Cargo.lock` is committed and builds use `--locked`, but there is no
  audit step in CI. `cargo audit` is a reasonable thing to run before a release; it is not
  currently a gate.

If you find a boundary above that you believe is wrong or insufficient for a deployment you
operate, that is a valid security report — say which boundary and what you expected.
