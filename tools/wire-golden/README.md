# Wire protocol regression fixture

`fixtures/wire-golden.json` is a captured, normalized HTTP exchange corpus and
`fixtures/wire-golden.sha256` is its byte digest. `fixtures/wire-golden.raw.json`
keeps the un-normalized capture. Together they anchor status codes, response
headers, JSON bodies, error text and the delivery set for the API surface
Rustash exposes. Do not edit the fixture to make a failing comparison pass.

Run from the repository root:

```sh
env SKIP_BUILD=1 tools/wire-golden/run.sh --golden tools/wire-golden/fixtures/wire-golden.json
node tools/wire-golden/tamper-check.mjs
```

`run.sh` starts the release binary on a temporary SQLite database, starts a local
sink, replays `corpus.mjs`, normalizes volatile fields and compares the result.
`--write` regenerates the fixture; use it only when the protocol intentionally
changes, and update all three fixture files together. `tamper-check.mjs` proves
the comparator rejects single-field changes, including status, headers, bodies,
delivery digests, delivery count and corpus version.
