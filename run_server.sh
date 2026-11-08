#!/usr/bin/env bash
# Start Rustash against a FRESH temporary SQLite file (deleted on exit).
#
#   ./run_server.sh              # listens on :8080 (override with PORT=...)
#   PORT=9000 ./run_server.sh
#
# Env overrides honored by the binary (see README.md for the full table):
# DATABASE_URL, PORT, QSTASH_TOKEN, ALLOW_PRIVATE_DESTINATIONS, DEFAULT_MAX_RETRIES, ...
set -euo pipefail
cd "$(dirname "$0")"

DB_FILE="$(mktemp -u "${TMPDIR:-/tmp}/rustash-XXXXXXXX.sqlite3")"
cleanup() { rm -f "$DB_FILE" "$DB_FILE-wal" "$DB_FILE-shm"; }
# EXIT is the only place cleanup runs, so it has to survive every way out of
# this script: a normal child exit, a forwarded signal, or a `set -e` abort.
trap cleanup EXIT

export DATABASE_URL="${DATABASE_URL:-sqlite://$DB_FILE}"
export PORT="${PORT:-8080}"
# Dev-only token (>=16 chars), generated per run so no credential is committed:
# deliveries to loopback sinks need ALLOW_PRIVATE_DESTINATIONS=true.
export QSTASH_TOKEN="${QSTASH_TOKEN:-dev-$(date +%s)-$$}"
export ALLOW_PRIVATE_DESTINATIONS="${ALLOW_PRIVATE_DESTINATIONS:-true}"

BIN="target/release/rustash"
[ -x "$BIN" ] || BIN="target/debug/rustash"
if [ ! -x "$BIN" ]; then
  echo "building release binary..." >&2
  cargo build --release >&2
  BIN="target/release/rustash"
fi

echo "Rustash: sqlite=$DATABASE_URL port=$PORT" >&2
echo "token: $QSTASH_TOKEN" >&2

# Run the server as a child instead of `exec`-ing it. `exec` replaced this shell
# with the binary, so the EXIT trap could never fire and every invocation left
# its temporary database (and the -wal beside it) behind. The child stays in
# this shell's process group, so a terminal Ctrl-C reaches it directly; a signal
# aimed at this script is forwarded. cleanup() runs only once the server is
# gone, which is what "deleted on exit" requires.
child=""
forward() { kill -TERM "$child" 2>/dev/null || true; }

"$BIN" &
child=$!
trap forward INT TERM

status=0
while :; do
  # A trapped signal makes `wait` return early (>128) while the child is still
  # running; in that case the trap has already forwarded the signal, so wait
  # again for the child's real exit status instead of trusting the 128+n.
  if wait "$child"; then status=0; else status=$?; fi
  kill -0 "$child" 2>/dev/null || break
done

exit "$status"
