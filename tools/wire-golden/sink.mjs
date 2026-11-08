// Controlled delivery sink for the wire-golden harness.
//
// Every request it receives is recorded (method, path, raw header list, body bytes).
// It answers from a fixed table so a delivery's outcome is part of the anchor, not a
// timing accident. Control endpoints under /__ are never recorded.
import http from 'node:http';

const port = Number(process.env.PORT || 0);
const bind = process.env.BIND || '127.0.0.1';

/** path -> [status, body, extra headers] */
const ROUTES = {
  '/ok': [200, '{"ok":true}', { 'content-type': 'application/json; charset=utf-8' }],
  '/alt': [200, 'alt-response', { 'content-type': 'text/plain' }],
  '/fail': [500, '{"fail":true}', { 'content-type': 'application/json; charset=utf-8' }],
  '/wf': [200, '{"wf":true}', { 'content-type': 'application/json; charset=utf-8' }],
};

const deliveries = [];

const server = http.createServer((req, res) => {
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    const body = Buffer.concat(chunks);
    const path = String(req.url || '').split('?')[0];

    if (path === '/__deliveries') {
      const out = JSON.stringify(
        deliveries.map((d) => ({
          method: d.method,
          path: d.path,
          rawHeaders: d.rawHeaders,
          bodyBase64: d.bodyBase64,
          bodyBytes: d.bodyBytes,
        })),
      );
      res.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(out) });
      res.end(out);
      return;
    }
    if (path === '/__health') {
      const out = JSON.stringify({ ok: true, deliveries: deliveries.length });
      res.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(out) });
      res.end(out);
      return;
    }

    deliveries.push({
      method: req.method,
      path: req.url,
      rawHeaders: req.rawHeaders.slice(),
      bodyBase64: body.toString('base64'),
      bodyBytes: body.length,
    });

    const route = ROUTES[path] || [404, '{"sink":"no-route"}', { 'content-type': 'application/json' }];
    const [status, text, headers] = route;
    res.writeHead(status, { ...headers, 'x-sink-mark': 'wire-golden' });
    res.end(text);
  });
});

server.listen(port, bind, () => {
  process.stdout.write(`SINK_LISTENING ${bind}:${server.address().port}\n`);
});

for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => server.close(() => process.exit(0)));
