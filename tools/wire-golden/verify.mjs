// Structural comparison for wire-golden documents.
//
// Two responsibilities:
//   stableJson  — a serialization whose bytes are the fingerprint subject.
//   compareDocs — find every difference by JSON path, so a mismatch names the location
//                 (`steps[7].response.status`) instead of just saying "not equal".
//
// `compareDocs` must FAIL on a real difference: the harness proves this by tampering a
// stored anchor one field at a time (see tamper-check.mjs).

export function stableJson(value) {
  return `${JSON.stringify(sortDeep(value), null, 2)}\n`;
}

function sortDeep(value) {
  if (Array.isArray(value)) return value.map(sortDeep);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) out[key] = sortDeep(value[key]);
    return out;
  }
  return value;
}

function kindOf(v) {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  return typeof v;
}

function clip(v) {
  const s = typeof v === 'string' ? v : JSON.stringify(v);
  if (s === undefined) return 'undefined';
  return s.length > 220 ? `${s.slice(0, 220)}…(${s.length} chars)` : s;
}

/** Deep difference list. Paths use `a.b[3].c` form. */
export function diffDeep(a, b, path = '$', out = []) {
  const ka = kindOf(a);
  const kb = kindOf(b);
  if (ka !== kb) {
    out.push({ path, golden: clip(a), actual: clip(b), reason: `type ${ka} != ${kb}` });
    return out;
  }
  if (ka === 'array') {
    if (a.length !== b.length) {
      out.push({ path: `${path}.length`, golden: String(a.length), actual: String(b.length), reason: 'length' });
    }
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i += 1) diffDeep(a[i], b[i], `${path}[${i}]`, out);
    return out;
  }
  if (ka === 'object') {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const key of [...keys].sort()) {
      if (!(key in a)) {
        out.push({ path: `${path}.${key}`, golden: '<absent>', actual: clip(b[key]), reason: 'missing in golden' });
      } else if (!(key in b)) {
        out.push({ path: `${path}.${key}`, golden: clip(a[key]), actual: '<absent>', reason: 'missing in capture' });
      } else {
        diffDeep(a[key], b[key], `${path}.${key}`, out);
      }
    }
    return out;
  }
  if (a !== b) {
    out.push({ path, golden: clip(a), actual: clip(b), reason: 'value' });
  }
  return out;
}

/** Compare two wire-golden documents. Returns [] when equivalent. */
export function compareDocs(golden, candidate) {
  const diffs = [];

  // 1. Corpus identity: a changed corpus is not a behavioural regression, it is a
  //    different experiment — refuse to compare at all.
  if (golden.corpus !== candidate.corpus) {
    diffs.push({
      path: '$.corpus',
      golden: String(golden.corpus),
      actual: String(candidate.corpus),
      reason: 'corpus version differs: the anchor was captured from another corpus',
    });
    return diffs;
  }

  // 2. Step-by-step, by position and by id, so an inserted corpus step is visible.
  const gs = golden.steps || [];
  const cs = candidate.steps || [];
  if (gs.length !== cs.length) {
    diffs.push({ path: '$.steps.length', golden: String(gs.length), actual: String(cs.length), reason: 'length' });
  }
  const n = Math.min(gs.length, cs.length);
  for (let i = 0; i < n; i += 1) {
    if (gs[i].id !== cs[i].id) {
      diffs.push({ path: `$.steps[${i}].id`, golden: gs[i].id, actual: cs[i].id, reason: 'corpus step order' });
      continue;
    }
    diffDeep(gs[i].response, cs[i].response, `$.steps[${i}](${gs[i].id}).response`, diffs);
    diffDeep(gs[i].request, cs[i].request, `$.steps[${i}](${gs[i].id}).request`, diffs);
  }

  // 3. Deliveries: a multiset. Their arrival order at the sink is a scheduling
  //    accident, so they are matched by content, and a mismatch names the delivery that
  //    has no counterpart plus the field that differs inside the closest pair.
  const gd = golden.deliveries || [];
  const cd = candidate.deliveries || [];
  if (gd.length !== cd.length) {
    diffs.push({ path: '$.deliveries.length', golden: String(gd.length), actual: String(cd.length), reason: 'length' });
  }
  const canon = (d) => JSON.stringify(sortDeep(d));
  const keyOf = (d) => `${d.path}|${d.method}`;
  const remaining = cd.map((d) => ({ d, c: canon(d), used: false }));
  const missing = [];
  for (const g of gd) {
    const hit = remaining.find((r) => !r.used && r.c === canon(g));
    if (hit) {
      hit.used = true;
    } else {
      missing.push(g);
    }
  }
  const extra = remaining.filter((r) => !r.used).map((r) => r.d);
  for (const g of missing) {
    // Pair it with the same-path candidate that is also unmatched, so the report names
    // the field instead of "one delivery vanished, another appeared".
    const partner = extra.find((d) => keyOf(d) === keyOf(g)) || null;
    if (partner) {
      const gKey = canon(g);
      const cKey = canon(partner);
      if (gKey === cKey) continue;
      diffDeep(g, partner, `$.deliveries[${gd.indexOf(g)}](${g.path} ${g.method})`, diffs);
      extra.splice(extra.indexOf(partner), 1);
    } else {
      diffs.push({
        path: `$.deliveries[${gd.indexOf(g)}](${g.path} ${g.method})`,
        golden: clip(canon(g)),
        actual: '<absent>',
        reason: 'delivery missing from the candidate',
      });
    }
  }
  for (const d of extra) {
    diffs.push({
      path: `$.deliveries(<${d.path} ${d.method}>)`,
      golden: '<absent>',
      actual: clip(canon(d)),
      reason: 'delivery not in the golden anchor',
    });
  }
  return diffs;
}
