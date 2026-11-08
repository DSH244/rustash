#!/usr/bin/env bash
# The one entry point for the wire-protocol regression check.
#
#   tools/wire-golden/run.sh --golden tools/wire-golden/fixtures/wire-golden.json
#       exit 0  the capture matches the stored anchor (structure aside)
#       exit 1  the capture differs; every difference path is printed
#       exit 2  the harness could not run (missing binary, build failure, bad anchor sum)
#
#   tools/wire-golden/run.sh --write tools/wire-golden/fixtures/wire-golden.json
#       (re)capture and store the anchor + its .sha256 + the un-normalized mirror
#
#   tools/wire-golden/run.sh --tamper-check
#       prove the comparator fails on a one-field tamper (the comparator sensitivity check)
#
# Environment:
#   SKIP_BUILD=1            reuse target/release/rustash instead of rebuilding
#   RUSTASH_BIN=/path/bin   capture against a specific binary
#   SERVICE_PORT / SINK_PORT pin ports (default: an ephemeral free port)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
cd "$REPO" || exit 2

if ! command -v node >/dev/null 2>&1; then
  echo "node is required (harness is Node-only, no npm dependencies)" >&2
  exit 2
fi

case "${1:-}" in
  --tamper-check)
    exec node "$HERE/tamper-check.mjs"
    ;;
  --coverage|--golden|--write|--compare-only|"")
    exec node "$HERE/runner.mjs" "$@"
    ;;
  -h|--help)
    sed -n '2,20p' "$0"
    exit 0
    ;;
  *)
    echo "unknown argument: $1" >&2
    sed -n '2,20p' "$0" >&2
    exit 2
    ;;
esac
