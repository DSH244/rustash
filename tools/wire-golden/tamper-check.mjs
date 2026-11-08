#!/usr/bin/env node
// Sensitivity proof for the wire-golden comparator.
//
// A comparator that always returns 0 is worthless. This script tampers the STORED anchor
// one field at a time, runs the real comparison entry point (`--compare-only`) on each
// tampered copy, and requires exit code 1 plus a difference path that names the field that
// was changed. It also runs the unmodified anchor once and requires exit code 0.
//
// Exit 0 = the comparator is sensitive where it must be and silent where it must be.

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../..');
const GOLDEN = process.env.WIRE_GOLDEN || path.join(REPO, 'tools/wire-golden/fixtures/wire-golden.json');
const SUM = GOLDEN.replace(/\.json$/, '.sha256');

function sha256(file) {
  return createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function compare(candidatePath) {
  const r = spawnSync(
    process.execPath,
    [path.join(HERE, 'runner.mjs'), '--compare-only', GOLDEN, candidatePath],
    { encoding: 'utf8' },
  );
  return { code: r.status, out: `${r.stdout || ''}${r.stderr || ''}` };
}

function writeTmp(dir, name, doc) {
  const p = path.join(dir, name);
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`);
  return p;
}

function clone(doc) {
  return JSON.parse(JSON.stringify(doc));
}

function main() {
  if (!fs.existsSync(GOLDEN)) {
    console.error(`missing anchor: ${GOLDEN}`);
    return 2;
  }
  const failures = [];

  // 0. The anchor must match its recorded digest, otherwise every comparison below is
  //    against a file nobody can reproduce.
  if (fs.existsSync(SUM)) {
    const want = fs.readFileSync(SUM, 'utf8').trim().split(/\s+/)[0];
    const got = sha256(GOLDEN);
    const ok = want === got;
    console.log(`${ok ? 'PASS' : 'FAIL'} anchor sha256 ${got}${ok ? '' : ` != ${want}`}`);
    if (!ok) failures.push('anchor sha256');
  } else {
    console.log(`SKIP anchor sha256 (no ${path.basename(SUM)})`);
  }

  const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wire-golden-tamper-'));

  // 1. Identity: the anchor compared with itself must be clean.
  {
    const r = compare(writeTmp(dir, 'identity.json', golden));
    const ok = r.code === 0 && r.out.includes('WIRE-GOLDEN MATCH');
    console.log(`${ok ? 'PASS' : 'FAIL'} identity comparison exits 0`);
    if (!ok) failures.push(`identity: rc=${r.code} ${r.out.trim()}`);
  }

  // 2. Directed tampers. Each entry: name, mutate(doc) -> expected path fragment.
  const tampers = [
    [
      'status-201-to-202',
      (doc) => {
        const step = doc.steps.find((s) => s.response.status === 201);
        if (!step) throw new Error('no 201 response in the anchor');
        step.response.status = 202;
        return `steps[${doc.steps.indexOf(step)}](${step.id}).response.status`;
      },
    ],
    [
      'drop-a-normalized-header',
      (doc) => {
        const step = doc.steps.find(
          (s) => s.response.headers && Object.keys(s.response.headers).length > 0,
        );
        if (!step) throw new Error('no response headers in the anchor');
        const key = Object.keys(step.response.headers).sort()[0];
        delete step.response.headers[key];
        return `steps[${doc.steps.indexOf(step)}](${step.id}).response.headers.${key}`;
      },
    ],
    [
      'change-a-response-body',
      (doc) => {
        const step = doc.steps.find((s) => s.response.body && s.response.body.includes('messageId'));
        if (!step) throw new Error('no publish response body in the anchor');
        step.response.body = step.response.body.replace('messageId', 'messageid');
        return `steps[${doc.steps.indexOf(step)}](${step.id}).response.body`;
      },
    ],
    [
      'change-a-delivery-body-hash',
      (doc) => {
        const d = doc.deliveries.find((x) => x.bodySha256);
        if (!d) throw new Error('no delivery in the anchor');
        const i = doc.deliveries.indexOf(d);
        d.bodySha256 = d.bodySha256.replace(/^./, d.bodySha256[0] === 'a' ? 'b' : 'a');
        return `deliveries[${i}]`;
      },
    ],
    [
      'add-an-extra-delivery',
      (doc) => {
        if (!doc.deliveries.length) throw new Error('no delivery in the anchor');
        doc.deliveries.push({ ...clone(doc.deliveries[0]), path: '/ok/extra' });
        return 'deliveries.length';
      },
    ],
    [
      'bump-corpus-version',
      (doc) => {
        doc.corpus = `${doc.corpus}-mutated`;
        return 'corpus';
      },
    ],
  ];

  for (const [name, mutate] of tampers) {
    const doc = clone(golden);
    let expected;
    try {
      expected = mutate(doc);
    } catch (e) {
      console.log(`SKIP ${name} (${e.message})`);
      continue;
    }
    const file = writeTmp(dir, `${name}.json`, doc);
    const r = compare(file);
    const namedPath = r.out.includes(expected);
    const ok = r.code === 1 && r.out.includes('WIRE-GOLDEN MISMATCH') && namedPath;
    console.log(`${ok ? 'PASS' : 'FAIL'} tamper=${name} rc=${r.code} expected-path=${expected}`);
    console.log(
      r.out
        .split('\n')
        .filter((l) => l.trim())
        .slice(0, 8)
        .map((l) => `    ${l}`)
        .join('\n'),
    );
    if (!ok) failures.push(`${name}: rc=${r.code} named=${namedPath}`);
  }

  console.log(`tampered copies kept at ${dir}`);
  if (failures.length) {
    console.log(`TAMPER-CHECK FAILED (${failures.length}): ${failures.join('; ')}`);
    return 1;
  }
  console.log('TAMPER-CHECK PASSED');
  return 0;
}

process.exit(main());
