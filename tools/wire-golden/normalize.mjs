// Wire-golden normalization.
//
// Every rule below exists because the masked part of a response changes between two
// runs of the SAME binary. A rule that hides a real behaviour change is a bug: the
// rule list is justified one by one in tools/wire-golden/README.md.

/** Deterministic serialization: object keys sorted, so a fingerprint never depends on
 *  field construction order. */
export function stableStringify(value) {
  if (value === undefined) return 'null';
  if (value === null) return 'null';
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return `{${keys
      .map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

// ULIDs are Crockford base32, 26 chars, uppercase. Message/DLQ/schedule ids are minted
// per request, so their value is random but their shape is part of the contract.
const ULID = '[0-9A-HJKMNP-TV-Z]{26}';
const STRICT_ID_RULES = [
  ['msg', new RegExp(`msg_${ULID}`, 'g')],
  ['dlq', new RegExp(`dlq_${ULID}`, 'g')],
  ['scd', new RegExp(`scd_${ULID}`, 'g')],
  // Waiter ids and signature jtis are random bytes, not ULIDs.
  ['wtr', /wtr_[A-Za-z0-9_-]{16,}/g],
  ['jwt', /jwt_[A-Za-z0-9_-]{16,}/g],
];

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;
const JWS_RE = /eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g;
const HTTP_DATE_RE =
  /^[A-Z][a-z]{2}, \d\d [A-Z][a-z]{2} \d{4} \d\d:\d\d:\d\d GMT$/;
const ISO_RE = /\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z/g;
// 13-digit milliseconds and 10-digit seconds, only in the plausible epoch band.
const MS_RE = /(?<!\d)(1[6-9]\d{11})(?!\d)/g;
const SEC_RE = /(?<!\d)(1[6-9]\d{8})(?!\d)/g;
// A JSON string field holding a body that the API returns base64-encoded.
const B64_FIELD_RE = /"(body|responseBody)":"([A-Za-z0-9+/]{8,}={0,2})"/g;
// The same pattern without anchors, for values nested in a JSON string.
const HTTP_DATE_RE_G = /[A-Z][a-z]{2}, \d\d [A-Z][a-z]{2} \d{4} \d\d:\d\d:\d\d GMT/g;

export function createNormalizer(opts) {
  return {
    opts,
    tokens: new Map(), // raw value -> placeholder, first-seen order
    counts: Object.create(null),
  };
}

function next(st, kind) {
  st.counts[kind] = (st.counts[kind] || 0) + 1;
  return `<${kind}#${st.counts[kind]}>`;
}

function token(st, kind, raw) {
  if (!st.tokens.has(raw)) st.tokens.set(raw, next(st, kind));
  return st.tokens.get(raw);
}

/** Normalize one string (a body, a path, or a header value). */
export function normalizeText(input, st, hint = {}) {
  if (input === null || input === undefined) return input;
  let s = String(input);

  // 1. Service and sink origins. The harness picks ephemeral ports, so an origin is
  //    never stable across runs; the PATH under it is, and stays untouched.
  s = s.split(st.opts.sinkOrigin).join('http://sink.invalid');
  s = s.split(st.opts.serviceOrigin).join('http://service.invalid');
  for (const [port, host] of [
    [st.opts.sinkPort, 'sink.invalid'],
    [st.opts.servicePort, 'service.invalid'],
  ]) {
    s = s.split(`127.0.0.1:${port}`).join(host);
    s = s.split(`localhost:${port}`).join(host);
  }

  // 2. Base64-encoded bodies: decode, normalize, re-encode. Without this the ids and
  //    timestamps *inside* a workflow step body would be opaque random bytes.
  if ((hint.depth || 0) < 3) {
    s = s.replace(B64_FIELD_RE, (m, key, b64) => {
      if (Buffer.from(b64, 'base64').toString('base64') !== b64) return m; // not base64
      const text = Buffer.from(b64, 'base64').toString('utf8');
      const inner = normalizeText(text, st, { depth: (hint.depth || 0) + 1 });
      return `"${key}":"${Buffer.from(inner, 'utf8').toString('base64')}"`;
    });
  }

  // 3. Minted identifiers.
  for (const [kind, re] of STRICT_ID_RULES) {
    s = s.replace(re, (raw) => token(st, kind, raw));
  }

  // 4. Signing keys: the two configured ones are named, a rotated (fresh random) one is
  //    named as rotated so `current`/`next` relations stay readable.
  if (st.opts.signingKeyCurrent) s = s.split(st.opts.signingKeyCurrent).join('<signing_key:current>');
  if (st.opts.signingKeyNext) s = s.split(st.opts.signingKeyNext).join('<signing_key:next>');
  // base64url WITH padding: `-` and `_` are part of the alphabet, so the class must
  // include them or the tail of a rotated key survives as a random suffix.
  s = s.replace(/sig_[A-Za-z0-9_=/+-]{8,}/g, (raw) => token(st, 'signing_key_rotated', raw));

  // 5. Delivery signature (JWS carries iat/exp/jti and an HMAC over them).
  s = s.replace(JWS_RE, '<upstash_signature>');

  // 6. Caller IP as observed by the service (varies with the harness host / proxy hop).
  if (st.opts.callerIp) s = s.split(st.opts.callerIp).join('<caller_ip>');

  // 7. Tenant / internal UUIDs.
  s = s.replace(UUID_RE, (raw) => token(st, 'uuid', raw));

  // 8. Base64url pagination cursors: base64(ms + ":" + id) — encodes two unstable parts.
  s = s.replace(/"cursor":"([A-Za-z0-9_-]{8,})"/g, (m, raw) => `"cursor":"${token(st, 'cursor', raw)}"`);

  // 9. Time. An HTTP-date is replaced everywhere it appears, not only in a `date`
  //    header: the DLQ and event rows embed the endpoint's `date` header verbatim.
  if (hint.headerName === 'date' && HTTP_DATE_RE.test(s)) return '<http_date>';
  s = s.replace(HTTP_DATE_RE_G, '<http_date>');
  s = s.replace(ISO_RE, '<ts_iso>');
  s = s.replace(MS_RE, '<ts_ms>');
  s = s.replace(SEC_RE, '<ts_s>');

  return s;
}

/** Normalize a header value; `user-agent` and `date` get their own rules. */
export function normalizeHeaderValue(name, value, st) {
  const lower = name.toLowerCase();
  if (lower === 'authorization') return '<bearer_token>';
  if (lower === 'date') return normalizeText(value, st, { headerName: 'date' });
  let v = normalizeText(value, st);
  if (lower === 'user-agent') {
    // Keep the product tokens, drop the version digits: a rebuild must not change this.
    v = v.replace(/\b([A-Za-z][\w.-]*)\/(\d[\w.+~-]*)/g, '$1/<version>');
    v = v.replace(/(\bv?\d+\.\d+(?:\.\d+)*)/g, '<version>');
  }
  return v;
}

/** Fold a raw header list ([name, value, name, value, ...]) into a normalized object.
 *  Names are lowercased; repeated names keep wire order in a comma-joined value. */
export function normalizeHeaders(rawHeaders, st) {
  const out = {};
  for (let i = 0; i < rawHeaders.length; i += 2) {
    const name = String(rawHeaders[i]);
    const value = String(rawHeaders[i + 1] ?? '');
    const key = name.toLowerCase();
    const norm = normalizeHeaderValue(name, value, st);
    if (key in out) {
      out[key] = `${out[key]}, ${norm}`;
    } else {
      out[key] = norm;
    }
  }
  return out;
}

export function normalizePath(path, st) {
  let p = normalizeText(path, st).replace(/qstash_token=[^&]*/g, 'qstash_token=<redacted>');
  // A pagination cursor sitting in a query string: base64url(ms:id), i.e. two unstable
  // parts in one opaque token.
  p = p.replace(/([?&]cursor=)([A-Za-z0-9_/+=%-]{6,})/g, (m, pre, raw) => `${pre}${token(st, 'cursor', raw)}`);
  return p;
}
