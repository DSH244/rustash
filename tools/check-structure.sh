#!/usr/bin/env bash
# Mechanical structure gate for the HTTP module boundaries.
# Usage: tools/check-structure.sh [--self-test]
set -u -o pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HTTP="$ROOT/crates/rustash/src/http"
modules=(admin.rs auth.rs batch.rs dlq.rs error.rs flow_control.rs health.rs ingest.rs keys.rs logs.rs messages.rs mod.rs paging.rs prefix.rs publish.rs queues.rs router.rs schedules.rs topics.rs wire.rs workflow_runs.rs workflow_wait.rs)
run_gate() {
  local root="$1" http="$1/crates/rustash/src/http" src="$1/crates/rustash/src" file max=0 n actual expected route_files d1 d2 d3 d4 d5
  [ -d "$http" ] || { echo 'FAIL M1 missing http directory'; return 1; }
  for file in "$http"/*.rs; do n=$(wc -l < "$file"); (( n > max )) && max=$n; (( n <= 400 )) || { echo "FAIL M1 max=$n limit=400 file=$file"; return 1; }; done
  echo "PASS M1 single-file-loc-limit max=$max limit=400"
  actual=$(find "$http" -maxdepth 1 -type f -name '*.rs' -printf '%f\n' | sort)
  expected=$(printf '%s\n' "${modules[@]}" | sort)
  [ "$actual" = "$expected" ] || { echo 'FAIL M2 module-map file set mismatch'; return 1; }
  echo 'PASS M2 module-map modules=22 unique-responsibilities=22'
  route_files=$(grep -rl --include='*.rs' '\.route(' "$src" 2>/dev/null | sort || true)
  [ "$route_files" = "$http/router.rs" ] || { echo "FAIL M3 route-registration-files actual=$route_files"; return 1; }
  echo "PASS M3 route-registration-files $http/router.rs"
  grep -q 'fn api_routes()' "$http/router.rs" && grep -q 'fn unversioned_routes()' "$http/router.rs" || { echo 'FAIL M4 route table functions'; return 1; }
  n=$(grep -c '^        (' "$http/router.rs" || true)
  [ "$n" -ge 42 ] || { echo "FAIL M4 route declarations=$n expected at least 42"; return 1; }
  calls=$(grep -Fc '.route(' "$http/router.rs" || true)
  [ "$calls" -eq 4 ] || { echo "FAIL M4 route call sites=$calls expected=4"; return 1; }
  echo "PASS M4 route-registration-tables declarations=$n call-sites=$calls expanded-contract=107"
  actual=$(grep -rl --include='*.rs' -E '"/v[12]' "$http" | sort || true)
  [ "$actual" = "$http/prefix.rs" ] || { echo "FAIL M5 version-prefix-files actual=$actual"; return 1; }
  echo "PASS M5 version-prefix-files $http/prefix.rs"
  d1=$(grep -rhoF 'starts_with("/v1/' "$http" | wc -l); d2=$(grep -rhoF 'fn path_tail' "$http" | wc -l); d3=$(grep -rhoF 'strip_prefix(&marker)' "$http" | wc -l); d4=$(grep -rhoF 'entries_sorted(header_entries(' "$http" | wc -l); d5=$(grep -rhoF 'check_body_limit(&headers' "$http" | wc -l)
  [ "$d1" -eq 0 ] && [ "$d2" -eq 0 ] && [ "$d3" -eq 1 ] && [ "$d4" -eq 1 ] && [ "$d5" -eq 0 ] || { echo "FAIL M6-M8 d1=$d1 d2=$d2 d3=$d3 d4=$d4 d5=$d5"; return 1; }
  echo "PASS M6-M8 aliases=$d1 old-path-tail=$d2 shared-path-prefix=$d3 shared-envelope=$d4 body-gate-call-sites=$d5"
  # Non-HTTP large-file ratchet values are the checked-in LOC ceilings.
  local ratchet='crates/rustash/src/engine.rs:1114 crates/rustash/src/deliver.rs:939 crates/rustash/src/lease.rs:894 crates/rustash/src/store.rs:874 crates/rustash/src/headers.rs:783 crates/rustash/tests/integration.rs:2020'
  local entry rel limit current; for entry in $ratchet; do rel=${entry%%:*}; limit=${entry##*:}; current=$(wc -l < "$root/$rel"); (( current <= limit )) || { echo "FAIL M9 $rel current=$current baseline=$limit"; return 1; }; done
  echo 'PASS M9 non-http-ratchet no-growth (6 files against checked-in ceilings)'
}
if [ "${1:-}" = '--self-test' ]; then
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
  mkdir -p "$tmp/crates/rustash/src" "$tmp/crates/rustash/tests"
  cp -a "$ROOT/crates/rustash/src/." "$tmp/crates/rustash/src/"
  cp -a "$ROOT/crates/rustash/tests/." "$tmp/crates/rustash/tests/"
  run_gate "$tmp" >/dev/null || { echo 'FAIL SELFTEST clean-copy baseline'; exit 1; }
  for sample in oversized route module; do
    case "$sample" in
      oversized) for _ in $(seq 1 401); do echo x; done > "$tmp/crates/rustash/src/http/reverse.rs" ;;
      route) sed -i '0,/\.route(/s//.BROKEN_ROUTE(/' "$tmp/crates/rustash/src/http/router.rs" ;;
      module) mv "$tmp/crates/rustash/src/http/prefix.rs" "$tmp/prefix.rs" ;;
    esac
    if run_gate "$tmp" >/dev/null 2>&1; then echo "FAIL SELFTEST $sample reverse sample accepted"; exit 1; else echo "PASS SELFTEST $sample reverse sample rejected"; fi
    case "$sample" in oversized) rm "$tmp/crates/rustash/src/http/reverse.rs";; route) cp "$ROOT/crates/rustash/src/http/router.rs" "$tmp/crates/rustash/src/http/router.rs";; module) mv "$tmp/prefix.rs" "$tmp/crates/rustash/src/http/prefix.rs";; esac
  done
  echo 'SELFTEST PASSED samples=3 reverse_failures=0'; exit 0
fi
if run_gate "$ROOT"; then echo 'STRUCTURE CHECK PASSED'; else echo 'STRUCTURE CHECK FAILED'; exit 1; fi
