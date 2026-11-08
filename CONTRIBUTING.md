# Contributing to Rustash

Rustash is a QStash-compatible queue on SQLite. The bar for a change is that it keeps the
LobeHub call surface working **and** that it can be verified by whoever reviews it, without
access to your machine.

## Development environment

| Requirement | Version used and pinned by this repo |
|---|---|
| Rust | `1.98.1` — `rust-toolchain.toml` pins it, so `rustup` installs it on first use |
| rustfmt / clippy | installed by the same file (`components = ["rustfmt", "clippy"]`) |
| Docker (optional) | only needed for the container path below |

No database server, broker, or network service is required. Every test opens its own
in-memory or temporary SQLite file.

```sh
git clone <this repo>
cd rustash
cargo build                # debug
cargo run --release        # or: ./run_server.sh (fresh temp DB on :8080)
```

`./run_server.sh` starts the release binary against a fresh temporary SQLite file with a
development token, for smoke testing by hand. It is a development convenience, not a
deployment path — use [`compose.rustash.yml`](compose.rustash.yml) for that.

## Build and test entry points

```sh
cargo build --release --locked   # the shipping build; --locked must not change Cargo.lock
cargo test --locked              # 97 unit + 29 integration + 3 contract tests (129 total)
```

`cargo test` is the real behavioural suite: the integration file starts an actual HTTP sink
and asserts that deliveries arrive with the right method, headers, signature and bytes. It
does not reach the network, so it runs offline.

## Before you commit — the same four commands CI runs

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --release --locked
```

These are exactly the commands in `.github/workflows/ci.yml`, run in that order. A change
that fails any of them will fail CI, so run them locally first rather than pushing to find
out. If you change a command here, change it in the workflow in the same commit.

Container path (also run by CI):

```sh
docker build -t rustash:verify .
```

## Code style

The gate, not opinion:

- **Formatting** — `cargo fmt` with the defaults. `rustfmt.toml` sets `edition` only; it is
  deliberately minimal, because each added option rewrites every file at once.
- **Lints** — `cargo clippy --all-targets --locked -- -D warnings`. Warnings are errors.
  Fix the lint; do not add `#[allow(...)]` to silence a real one. A targeted allow is
  acceptable when the pattern is intentional and the reason is written in a comment beside
  it.
- **Comments explain *why*.** A comment that restates the code is noise. A comment that
  records a non-obvious constraint — a SQLite plan that had to be pinned, a QStash
  behaviour that looks wrong but is documented, a lease invariant — is required.

Behaviour changes need a test that fails before the change and passes after. Prefer a test
that asserts the observable outcome (a delivery arrived with these bytes) over one that
asserts an internal detail.

## Reporting a defect

Open a GitHub issue with:

1. The version or commit (`git rev-parse HEAD`).
2. The exact command you ran and the environment (`rustc --version`, OS, and whether this
   was the binary, `cargo test`, or the container).
3. What you expected, what happened, and the server stderr — Rustash logs every open,
   migration and delivery decision at `LOG_LEVEL=info`.
4. If it is a delivery defect, the relevant rows from `/v2/logs` or `/v2/events` for the
   message id. A `messageId` plus a time window is enough to reconstruct the trail.

Please do not paste live tokens or signing keys into an issue; redact them.

Security-sensitive reports do not go in the public issue tracker — see
[SECURITY.md](SECURITY.md).

## Submitting a change

1. Branch from `main`. Keep the change focused: one behaviour per pull request.
2. Run the four gate commands above.
3. Write the commit message so the *reason* is in it. A subject line stating what changed
   plus a body explaining why the constraint exists is what makes a later bisect useful.
4. Open a pull request describing: what behaviour changed, how you verified it (paste the
   command and its output), and what you did *not* verify.
5. Do not bump the version or edit `Cargo.lock` by hand. If a dependency change is the
   point of the PR, say so explicitly in the description.

Changes that require a migration must state how an existing SQLite file reaches the new
schema (the startup DDL runs `MIGRATIONS` in order and records each in `__migrations`) and
what happens to in-flight messages during the upgrade.
