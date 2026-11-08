#!/usr/bin/env node
// Wire-golden capture harness.
//
//   node runner.mjs --write tools/wire-golden/fixtures/wire-golden.json
//   node runner.mjs --golden tools/wire-golden/fixtures/wire-golden.json
//   node runner.mjs --compare-only <golden.json> <candidate.json>
//
// This file only DRIVES the product: it starts the release binary on a temp SQLite DB
// and a temp port, starts the sink, replays corpus.mjs in order, and records what came
// back. It never reads or patches a Rust source file.
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createServer } from 'node:net';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { corpus } from './corpus.mjs';
import { createNormalizer, normalizeHeaders, normalizePath, normalizeText } from './normalize.mjs';
import { compareDocs, stableJson } from './verify.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../..');
const BIN = process.env.RUSTASH_BIN || path.join(REPO, 'target/release/rustash');
const SINK_SCRIPT = path.join(HERE, 'sink.mjs');

// Fixed corpus inputs. None of these may drift between runs, or the anchor stops meaning
// anything: the token and signing keys are constants of the corpus, not deployment data.
const TOKEN = 'wire-golden-token-0123456789abcdef';
const SIGNING_KEY_CURRENT = 'wire-golden-current-key-0123456789';
const SIGNING_KEY_NEXT = 'wire-golden-next-key-0123456789abc';
// MAX_BODY_BYTES is shrunk so the 412 case does not have to write a real 10 MiB body.
// This is a HARNESS setting, not a product default (see the harness settings above).
const MAX_BODY_BYTES = 4096;
const OVERSIZE_BODY = 5000;

const argv = process.argv.slice(2);
function arg(name, fallback = null) {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : fallback;
}
const writeTo = arg('write');
const goldenPath = arg('golden');
const rawPath = arg('raw');
const compareOnly = argv.includes('--compare-only');
const coverageOnly = argv.includes('--coverage');
const skipBuild = process.env.SKIP_BUILD === '1';

function die(code, msg) {
  process.stderr.write(`${msg}\n`);
  process.exit(code);
}

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const p = srv.address().port;
      srv.close(() => resolve(p));
    });
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** Minimal HTTP client: keeps the raw header list so the capture is auditable. */
function request(base, method, urlPath, { headers = {}, body = null, timeoutMs = 20000 } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlPath, base);
    const payload = body === null ? null : Buffer.from(body, 'utf8');
    const hdrs = { ...headers };
    if (payload) hdrs['content-length'] = String(payload.length);
    const req = http.request(
      {
        host: url.hostname,
        port: url.port,
        method,
        path: url.pathname + url.search,
        headers: hdrs,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () =>
          resolve({
            status: res.statusCode,
            statusText: res.statusMessage || '',
            rawHeaders: res.rawHeaders.slice(),
            body: Buffer.concat(chunks),
          }),
        );
      },
    );
    req.on('timeout', () => req.destroy(new Error(`timeout ${method} ${urlPath}`)));
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function waitHealthy(url, ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    try {
      const r = await request(url, 'GET', '/v2/health', { timeoutMs: 1000 });
      if (r.status === 200) return true;
    } catch {
      /* not up yet */
    }
    await sleep(100);
  }
  return false;
}

/** Wait until the sink's delivery count stops moving. */
async function quiesce(sinkBase, { quietMs = 700, maxMs = 20000 } = {}) {
  const deadline = Date.now() + maxMs;
  let last = -1;
  let stableSince = Date.now();
  while (Date.now() < deadline) {
    let count = -1;
    try {
      const r = await request(sinkBase, 'GET', '/__health', { timeoutMs: 2000 });
      count = JSON.parse(r.body.toString('utf8')).deliveries;
    } catch {
      count = last;
    }
    if (count !== last) {
      last = count;
      stableSince = Date.now();
    } else if (Date.now() - stableSince >= quietMs) {
      return count;
    }
    await sleep(100);
  }
  return last;
}

async function startProcess(cmd, args, { env, logFile }) {
  const out = fs.openSync(logFile, 'a');
  const child = spawn(cmd, args, { env: { ...process.env, ...env }, stdio: ['ignore', out, out] });
  return child;
}

async function runCapture(workDir) {
  if (!fs.existsSync(BIN)) die(2, `missing binary: ${BIN} (build it, or pass RUSTASH_BIN)`);
  const sinkPort = Number(process.env.SINK_PORT || (await freePort()));
  const servicePort = Number(process.env.SERVICE_PORT || (await freePort()));
  const dbFile = path.join(workDir, 'wire-golden.sqlite3');
  const sinkLog = path.join(workDir, 'sink.log');
  const serverLog = path.join(workDir, 'server.log');

  const sink = await startProcess(process.execPath, [SINK_SCRIPT], {
    env: { PORT: String(sinkPort), BIND: '127.0.0.1' },
    logFile: sinkLog,
  });
  const sinkBase = `http://127.0.0.1:${sinkPort}`;
  // sink readiness
  {
    const deadline = Date.now() + 15000;
    let ok = false;
    while (Date.now() < deadline && !ok) {
      try {
        await request(sinkBase, 'GET', '/__health', { timeoutMs: 500 });
        ok = true;
      } catch {
        await sleep(100);
      }
    }
    if (!ok) die(2, 'sink did not start');
  }

  const server = await startProcess(BIN, [], {
    env: {
      DATABASE_URL: `sqlite://${dbFile}`,
      PORT: String(servicePort),
      QSTASH_TOKEN: TOKEN,
      QSTASH_CURRENT_SIGNING_KEY: SIGNING_KEY_CURRENT,
      QSTASH_NEXT_SIGNING_KEY: SIGNING_KEY_NEXT,
      ALLOW_PRIVATE_DESTINATIONS: 'true',
      MAX_BODY_BYTES: String(MAX_BODY_BYTES),
      REGION: 'us-east-1',
      // Short tick so a settle window is short; the value is part of the anchor.
      DISPATCH_TICK_MS: '100',
      SCHEDULER_TICK_MS: '500',
      LOG_LEVEL: 'warn',
    },
    logFile: serverLog,
  });

  const serviceBase = `http://127.0.0.1:${servicePort}`;
  const cleanup = () => {
    try {
      server.kill('SIGTERM');
    } catch {}
    try {
      sink.kill('SIGTERM');
    } catch {}
  };
  process.on('exit', cleanup);

  if (!(await waitHealthy(serviceBase, 30000))) {
    cleanup();
    die(2, `server did not become healthy; see ${serverLog}`);
  }

  const st = createNormalizer({
    serviceOrigin: serviceBase,
    servicePort: String(servicePort),
    sinkOrigin: sinkBase,
    sinkPort: String(sinkPort),
    signingKeyCurrent: SIGNING_KEY_CURRENT,
    signingKeyNext: SIGNING_KEY_NEXT,
    // The corpus sends this as X-Forwarded-For; the delivered Upstash-Caller-Ip carries
    // it verbatim, so it is normalised like any other environment-derived value.
    callerIp: process.env.WIRE_GOLDEN_CALLER_IP || '203.0.113.7',
  });

  const steps = [];
  const bag = {};
  const corpusDef = corpus({ service: serviceBase, sink: sinkBase, token: TOKEN, oversizeBytes: OVERSIZE_BODY });
  let index = 0;
  for (const step of corpusDef.steps) {
    index += 1;
    if (process.env.WIRE_GOLDEN_PROGRESS === '1') {
      process.stderr.write(`[${index}/${corpusDef.steps.length}] ${step.id}\n`);
    }
    const started = Date.now();
    const call = await step.run({ request, base: serviceBase, sink: sinkBase, bag });
    if (process.env.WIRE_GOLDEN_PROGRESS === '1' && Date.now() - started > 1500) {
      process.stderr.write(`   slow ${Date.now() - started}ms\n`);
    }
    const rawBodyText = call.response.body.toString('utf8');
    const bodyForNormalize = rawBodyText;
    const normalizedBody = normalizeText(bodyForNormalize, st);
    const record = {
      index,
      id: step.id,
      family: step.family,
      path_kind: step.kind, // "ok" | "error"
      request: {
        method: call.method,
        path: normalizePath(call.path, st),
        headers: normalizeHeaders(call.requestHeaderPairs, st),
      },
      response: {
        status: call.response.status,
        headers: normalizeHeaders(call.response.rawHeaders, st),
        body: normalizedBody,
      },
    };
    steps.push(record);
    if (step.settle) {
      await quiesce(sinkBase);
    }
    // Raw (un-normalized) capture for auditability of the normalization itself.
    st.raw = st.raw || [];
    st.raw.push({ index, id: step.id, status: call.response.status, rawHeaders: call.response.rawHeaders, rawBody: rawBodyText });
  }

  // Final settle: one extra quiet window so no delivery is in flight when the sink is read.
  await quiesce(sinkBase, { quietMs: 1200 });
  await sleep(300);
  const sinkDump = JSON.parse((await request(sinkBase, 'GET', '/__deliveries')).body.toString('utf8'));

  cleanup();

  const deliveries = sinkDump
    .map((d) => {
      const bodyText = Buffer.from(d.bodyBase64, 'base64').toString('utf8');
      return {
        method: d.method,
        path: normalizePath(d.path, st),
        headers: normalizeHeaders(d.rawHeaders, st),
        bodySha256: createHash('sha256').update(Buffer.from(d.bodyBase64, 'base64')).digest('hex'),
        bodyBytes: d.bodyBytes,
        body: bodyText.length ? normalizeText(bodyText, st) : null,
      };
    })
    .sort((a, b) => {
      const ka = `${a.headers['upstash-message-id'] || ''}|${a.path}|${a.bodySha256}`;
      const kb = `${b.headers['upstash-message-id'] || ''}|${b.path}|${b.bodySha256}`;
      return ka < kb ? -1 : ka > kb ? 1 : 0;
    });

  const doc = {
    schema: 'rustash-wire-golden/1',
    corpus: corpusDef.version,
    capture: {
      binary: 'rustash (release)',
      service: 'rustash',
      region: 'us-east-1',
      maxBodyBytes: MAX_BODY_BYTES,
      maxBodyBytesIsShrunk: true,
      dispatchTickMs: 100,
      schedulerTickMs: 500,
      settle: 'sink delivery count flat for 700 ms (1200 ms on the final read)',
      unnormalizedResponses: 'the mirror of this run is written next to it as <name>.raw.json',
    },
    steps,
    deliveries,
  };

  return { doc, raw: st.raw, tokens: [...st.tokens.keys()] };
}

function sha256File(file) {
  return createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function printCoverage(file) {
  const doc = JSON.parse(fs.readFileSync(file, 'utf8'));
  const families = new Map();
  for (const step of doc.steps) {
    if (!families.has(step.family)) families.set(step.family, { ok: [], error: [], headers: new Set() });
    const f = families.get(step.family);
    f[step.path_kind === 'ok' ? 'ok' : 'error'].push(step.id);
    for (const name of Object.keys(step.response.headers || {})) f.headers.add(name);
  }
  process.stdout.write(`${'family'.padEnd(14)} ${'ok'.padStart(3)} ${'err'.padStart(4)}  steps\n`);
  let okTotal = 0;
  let errTotal = 0;
  for (const [name, f] of families) {
    okTotal += f.ok.length;
    errTotal += f.error.length;
    process.stdout.write(
      `${name.padEnd(14)} ${String(f.ok.length).padStart(3)} ${String(f.error.length).padStart(4)}  ${[...f.ok, ...f.error].join(', ')}\n`,
    );
  }
  process.stdout.write(
    `${'TOTAL'.padEnd(14)} ${String(okTotal).padStart(3)} ${String(errTotal).padStart(4)}  ${doc.steps.length} steps, ${doc.deliveries.length} deliveries\n`,
  );
  const statuses = {};
  for (const s of doc.steps) statuses[s.response.status] = (statuses[s.response.status] || 0) + 1;
  process.stdout.write(`statuses: ${JSON.stringify(statuses)}\n`);
  const headerNames = new Set();
  for (const s of doc.steps) for (const h of Object.keys(s.response.headers || {})) headerNames.add(h);
  process.stdout.write(`response header names seen: ${[...headerNames].sort().join(', ')}\n`);
  return 0;
}

async function main() {
  if (coverageOnly) {
    const rel = arg('file', 'tools/wire-golden/fixtures/wire-golden.json');
    return printCoverage(path.resolve(REPO, rel));
  }
  if (compareOnly) {
    const [golden, candidate] = argv.filter((a) => !a.startsWith('--')).slice(-2);
    if (!golden || !candidate) die(2, 'usage: --compare-only <golden.json> <candidate.json>');
    const g = JSON.parse(fs.readFileSync(golden, 'utf8'));
    const c = JSON.parse(fs.readFileSync(candidate, 'utf8'));
    const diffs = compareDocs(g, c);
    if (diffs.length === 0) {
      process.stdout.write('WIRE-GOLDEN MATCH\n');
      return 0;
    }
    process.stdout.write(`WIRE-GOLDEN MISMATCH (${diffs.length} difference(s))\n`);
    for (const d of diffs.slice(0, 40)) process.stdout.write(`  ${d.path}\n    golden: ${d.golden}\n    actual: ${d.actual}\n`);
    return 1;
  }

  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wire-golden-'));
  if (!skipBuild) {
    process.stderr.write('building release binary (set SKIP_BUILD=1 to reuse)\n');
    const code = await new Promise((resolve) => {
      const p = spawn('cargo', ['build', '--release', '--locked'], { cwd: REPO, stdio: 'inherit' });
      p.on('exit', resolve);
    });
    if (code !== 0) die(2, 'cargo build --release failed');
  }

  const { doc, raw, tokens } = await runCapture(workDir);
  const pretty = stableJson(doc);

  if (writeTo) {
    const abs = path.resolve(REPO, writeTo);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, pretty);
    const digest = sha256File(abs);
    const sumPath = `${abs.replace(/\.json$/, '')}.sha256`;
    fs.writeFileSync(sumPath, `${digest}  ${path.basename(abs)}\n`);
    const rawAbs = rawPath ? path.resolve(REPO, rawPath) : `${abs.replace(/\.json$/, '')}.raw.json`;
    fs.writeFileSync(rawAbs, stableJson({ note: 'un-normalized capture of the same run', steps: raw, mintedIds: tokens }));
    process.stdout.write(`WROTE ${abs}\nSHA256 ${digest}\nSUM ${sumPath}\nRAW ${rawAbs}\n`);
    return 0;
  }

  if (goldenPath) {
    const abs = path.resolve(REPO, goldenPath);
    const sumPath = `${abs.replace(/\.json$/, '')}.sha256`;
    if (fs.existsSync(sumPath)) {
      const want = fs.readFileSync(sumPath, 'utf8').trim().split(/\s+/)[0];
      const got = sha256File(abs);
      if (want !== got) {
        process.stderr.write(`anchor file does not match its .sha256\n  file: ${got}\n  sum:  ${want}\n`);
        return 2;
      }
    }
    const g = JSON.parse(fs.readFileSync(abs, 'utf8'));
    const diffs = compareDocs(g, doc);
    const candidatePath = path.join(workDir, 'capture.json');
    fs.writeFileSync(candidatePath, pretty);
    if (diffs.length === 0) {
      process.stdout.write(`WIRE-GOLDEN MATCH (${doc.steps.length} steps, ${doc.deliveries.length} deliveries)\n`);
      process.stdout.write(`candidate sha256 ${sha256File(candidatePath)}\n`);
      return 0;
    }
    process.stdout.write(`WIRE-GOLDEN MISMATCH (${diffs.length} difference(s)); candidate kept at ${candidatePath}\n`);
    for (const d of diffs.slice(0, 40)) process.stdout.write(`  ${d.path}\n    golden: ${d.golden}\n    actual: ${d.actual}\n`);
    return 1;
  }

  // No destination given: print the fingerprint only.
  const candidatePath = path.join(workDir, 'capture.json');
  fs.writeFileSync(candidatePath, pretty);
  process.stdout.write(`${sha256File(candidatePath)}  ${candidatePath}\n`);
  return 0;
}

main().then(
  (code) => process.exit(code),
  (err) => {
    process.stderr.write(`${err && err.stack ? err.stack : err}\n`);
    process.exit(2);
  },
);
