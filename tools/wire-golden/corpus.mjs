// The wire-golden request corpus.
//
// One ordered list of requests. Every entry is a fixed input: no randomness, no reading
// of the clock, no dependence on a previous run's output (values produced by an earlier
// step travel through `bag`). The corpus version is part of the fingerprint: changing
// any step requires bumping `version`, which makes `verify.mjs` refuse to compare.
//
// Families and their normal/error pair are tabulated in tools/wire-golden/README.md.

export const CORPUS_VERSION = 'wire-golden/2026-11-06.1';

const BLOCK = 'x'.repeat(5000);

export function corpus({ service, sink, token, oversizeBytes }) {
  const auth = { authorization: `Bearer ${token}` };
  const steps = [];
  let settleAll = 0;

  function add(step) {
    steps.push(step);
    return step;
  }

  /** Wrap a raw call so the recorded request headers are exactly what was sent. */
  async function call(request, ctx, method, path, { headers = {}, body = null } = {}) {
    const sent = { ...headers };
    if (body !== null) sent['content-length'] = String(Buffer.byteLength(body));
    const pairs = [];
    for (const [k, v] of Object.entries(sent)) pairs.push(k, v);
    const response = await request(ctx.base, method, path, { headers: sent, body });
    return { method, path, requestHeaderPairs: pairs, response };
  }

  // -------------------------------------------------------------------------
  // 1. health — public endpoints (no auth) and one unknown sub-path
  // -------------------------------------------------------------------------
  add({
    id: 'health-v2',
    family: 'health',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/health'),
  });
  add({
    id: 'health-readyz',
    family: 'health',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/readyz'),
  });
  add({
    id: 'health-v1-alias',
    family: 'health',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v1/health'),
  });
  add({
    id: 'health-unknown-subpath',
    family: 'health',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/healthz/extra'),
  });

  // -------------------------------------------------------------------------
  // 2. publish
  // -------------------------------------------------------------------------
  add({
    id: 'publish-url',
    family: 'publish',
    kind: 'ok',
    settle: true,
    run: async (c) => {
      const r = await call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'content-type': 'application/json' },
        body: '{"hello":"world"}',
      });
      c.bag.publishedId = JSON.parse(r.response.body.toString('utf8')).messageId;
      return r;
    },
  });
  add({
    id: 'publish-delay',
    family: 'publish',
    kind: 'ok',
    run: async (c) => {
      const r = await call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-delay': '600' },
        body: 'delay-body',
      });
      c.bag.delayedId = JSON.parse(r.response.body.toString('utf8')).messageId;
      return r;
    },
  });
  add({
    id: 'publish-deduplication-id-first',
    family: 'publish',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-deduplication-id': 'wire-dedupe-1' },
        body: 'dedupe-body',
      }),
  });
  add({
    id: 'publish-deduplication-id-repeat',
    family: 'publish',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-deduplication-id': 'wire-dedupe-1' },
        body: 'dedupe-body',
      }),
  });
  add({
    id: 'publish-content-based-dedupe-first',
    family: 'publish',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-content-based-deduplication': 'true' },
        body: 'content-dedupe-body',
      }),
  });
  add({
    id: 'publish-content-based-dedupe-repeat',
    family: 'publish',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-content-based-deduplication': 'true' },
        body: 'content-dedupe-body',
      }),
  });
  add({
    id: 'publish-flow-control',
    family: 'publish',
    kind: 'ok',
    // Delayed an hour: the row exists (and configures the key) but is never claimed, so
    // the flow-control counters below it stay deterministic.
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: {
          ...auth,
          'upstash-flow-control-key': 'wire-fc-quiet',
          'upstash-flow-control-value': 'parallelism=2,rate=10,period=1s',
          'upstash-delay': '3600',
        },
        body: 'fc-body',
      }),
  });
  add({
    id: 'publish-forward-headers',
    family: 'publish',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: {
          ...auth,
          'upstash-forward-x-wire': 'alpha',
          'upstash-forward-content-type': 'text/plain',
          'upstash-method': 'PUT',
        },
        body: 'forward-body',
      }),
  });
  add({
    id: 'publish-header-forward',
    family: 'publish',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/alt`, {
        headers: { ...auth, 'upstash-header-forward': 'true', 'x-wire-trace': 'hf-1' },
        body: 'header-forward-body',
      }),
  });
  add({
    id: 'publish-caller-ip',
    family: 'publish',
    kind: 'ok',
    settle: true,
    // The only way the delivered `Upstash-Caller-Ip` header is observable: the carrier
    // hop supplies it. The harness sends a fixed value and normalizes it, because the same
    // header carries the ingress address of wherever this corpus is replayed.
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'x-forwarded-for': '203.0.113.7' },
        body: 'caller-ip-body',
      }),
  });
  add({
    id: 'publish-missing-destination',
    family: 'publish',
    kind: 'error',
    run: (c) => call(c.request, c, 'POST', '/v2/publish/', { headers: auth }),
  });
  add({
    id: 'publish-invalid-delay',
    family: 'publish',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-delay': '5x' },
      }),
  });
  add({
    id: 'publish-unknown-topic',
    family: 'publish',
    kind: 'error',
    run: (c) => call(c.request, c, 'POST', '/v2/publish/wire-topic-missing', { headers: auth }),
  });
  add({
    id: 'publish-blocked-destination',
    family: 'publish',
    kind: 'error',
    // 169.254.169.254 is the metadata range: blocked even with
    // ALLOW_PRIVATE_DESTINATIONS=true, so the gate is deterministic and no delivery is
    // ever attempted (a blocked-private address would be accepted and then hang).
    run: (c) =>
      call(c.request, c, 'POST', '/v2/publish/http://169.254.169.254/wire', { headers: auth }),
  });

  // -------------------------------------------------------------------------
  // 3. enqueue
  // -------------------------------------------------------------------------
  add({
    id: 'enqueue-ok',
    family: 'enqueue',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', `/v2/enqueue/wire-enq/${c.sink}/ok`, {
        headers: auth,
        body: 'enqueue-body',
      }),
  });
  add({
    id: 'enqueue-missing-destination',
    family: 'enqueue',
    kind: 'error',
    run: (c) => call(c.request, c, 'POST', '/v2/enqueue/wire-enq', { headers: auth }),
  });
  add({
    id: 'enqueue-oversize-body',
    family: 'enqueue',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/enqueue/wire-enq/${c.sink}/ok`, {
        headers: auth,
        body: BLOCK,
      }),
  });

  // -------------------------------------------------------------------------
  // 4. batch / batchPublish
  // -------------------------------------------------------------------------
  add({
    id: 'batch-two-items',
    family: 'batch',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', '/v2/batch', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify([
          { destination: `${c.sink}/ok`, body: 'batch-a', delay: 0 },
          { destination: `${c.sink}/alt`, body: 'batch-b', headers: { 'upstash-forward-x-b': '2' } },
        ]),
      }),
  });
  add({
    id: 'batchpublish-alias',
    family: 'batch',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', '/v2/batchPublish', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify([{ destination: `${c.sink}/ok`, body: 'batchpublish-alias' }]),
      }),
  });
  add({
    id: 'batch-item-failure',
    family: 'batch',
    kind: 'error',
    // One failing item aborts the whole batch: publish_to_destination returns Err and the
    // handler propagates it. The anchor records that shape rather than an invented one.
    run: (c) =>
      call(c.request, c, 'POST', '/v2/batch', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify([
          { destination: `${c.sink}/ok`, body: 'batch-ok' },
          { destination: 'wire-topic-missing', body: 'batch-missing-group' },
        ]),
      }),
  });
  add({
    id: 'batch-oversize-item',
    family: 'batch',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/batch', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify([{ destination: `${c.sink}/ok`, body: BLOCK }]),
      }),
  });
  add({
    id: 'batch-not-an-array',
    family: 'batch',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/batch', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: '{"destination":"x"}',
      }),
  });
  add({
    id: 'batch-invalid-json',
    family: 'batch',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/batch', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: 'not json',
      }),
  });

  // -------------------------------------------------------------------------
  // 5. messages (query, single delete, bulk delete)
  // -------------------------------------------------------------------------
  add({
    id: 'messages-get-delayed',
    family: 'messages',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', `/v2/messages/${c.bag.delayedId}`, { headers: auth }),
  });
  add({
    id: 'messages-delete-single',
    family: 'messages',
    kind: 'ok',
    run: (c) => call(c.request, c, 'DELETE', `/v2/messages/${c.bag.delayedId}`, { headers: auth }),
  });
  add({
    id: 'messages-get-after-delete',
    family: 'messages',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', `/v2/messages/${c.bag.delayedId}`, { headers: auth }),
  });
  add({
    id: 'messages-delete-unknown',
    family: 'messages',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'DELETE', '/v2/messages/msg_01WIREGOLDENUNKNOWNAAAAAAA', {
        headers: auth,
      }),
  });
  add({
    id: 'messages-bulk-cancel-seed',
    family: 'messages',
    kind: 'ok',
    run: async (c) => {
      const ids = [];
      for (const body of ['bulk-1', 'bulk-2']) {
        const r = await call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
          headers: { ...auth, 'upstash-delay': '900' },
          body,
        });
        ids.push(JSON.parse(r.response.body.toString('utf8')).messageId);
      }
      c.bag.bulkIds = ids;
      // The seed is two publishes; the recorded step is the second one.
      return call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-delay': '900' },
        body: 'bulk-3',
      });
    },
  });
  add({
    id: 'messages-bulk-cancel',
    family: 'messages',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'DELETE', '/v2/messages', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ messageIds: c.bag.bulkIds }),
      }),
  });

  // -------------------------------------------------------------------------
  // 6. DLQ (list, get, single retry, bulk delete)
  // -------------------------------------------------------------------------
  add({
    id: 'dlq-seed-failure',
    family: 'dlq',
    kind: 'ok',
    settle: true,
    // `Upstash-Retries: 0` + a 500 from the endpoint ⇒ one DLQ row, no retry schedule.
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/fail`, {
        headers: { ...auth, 'upstash-retries': '0' },
        body: 'dlq-body',
      }),
  });
  add({
    id: 'dlq-list',
    family: 'dlq',
    kind: 'ok',
    run: async (c) => {
      const r = await call(c.request, c, 'GET', '/v2/dlq?count=10', { headers: auth });
      const parsed = JSON.parse(r.response.body.toString('utf8'));
      c.bag.dlqId = parsed.messages[0].dlqId;
      return r;
    },
  });
  add({
    id: 'dlq-get',
    family: 'dlq',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', `/v2/dlq/${c.bag.dlqId}`, { headers: auth }),
  });
  add({
    id: 'dlq-retry-single',
    family: 'dlq',
    kind: 'ok',
    settle: true,
    run: (c) => call(c.request, c, 'POST', `/v2/dlq/retry/${c.bag.dlqId}`, { headers: auth }),
  });
  add({
    id: 'dlq-list-after-retry',
    family: 'dlq',
    kind: 'ok',
    run: async (c) => {
      const r = await call(c.request, c, 'GET', '/v2/dlq?count=10', { headers: auth });
      const parsed = JSON.parse(r.response.body.toString('utf8'));
      c.bag.dlqId2 = parsed.messages[0] ? parsed.messages[0].dlqId : null;
      return r;
    },
  });
  add({
    id: 'dlq-bulk-delete',
    family: 'dlq',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'DELETE', '/v2/dlq', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ dlqIds: c.bag.dlqId2 ? [c.bag.dlqId2] : [] }),
      }),
  });
  add({
    id: 'dlq-list-empty',
    family: 'dlq',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/dlq', { headers: auth }),
  });
  add({
    id: 'dlq-get-unknown',
    family: 'dlq',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'GET', '/v2/dlq/dlq_01WIREGOLDENUNKNOWNAAAAAAA', { headers: auth }),
  });
  add({
    id: 'dlq-retry-unknown',
    family: 'dlq',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/dlq/retry/dlq_01WIREGOLDENUNKNOWNAAAAAAA', { headers: auth }),
  });

  // -------------------------------------------------------------------------
  // 7. keys
  // -------------------------------------------------------------------------
  add({
    id: 'keys-get',
    family: 'keys',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/keys', { headers: auth }),
  });
  add({
    id: 'keys-rotate',
    family: 'keys',
    kind: 'ok',
    run: (c) => call(c.request, c, 'POST', '/v2/keys/rotate', { headers: auth }),
  });
  add({
    id: 'keys-get-after-rotate',
    family: 'keys',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/keys', { headers: auth }),
  });
  add({
    id: 'keys-bad-token',
    family: 'keys',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'GET', '/v2/keys', { headers: { authorization: 'Bearer wire-wrong-token' } }),
  });

  // -------------------------------------------------------------------------
  // 8. queues + pause/resume
  // -------------------------------------------------------------------------
  add({
    id: 'queues-create',
    family: 'queues',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/queues', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ queueName: 'wire-q', parallelism: 2 }),
      }),
  });
  add({
    id: 'queues-list',
    family: 'queues',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/queues', { headers: auth }),
  });
  add({
    id: 'queues-get',
    family: 'queues',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/queues/wire-q', { headers: auth }),
  });
  add({
    id: 'queues-pause',
    family: 'queues',
    kind: 'ok',
    run: (c) => call(c.request, c, 'POST', '/v2/queues/wire-q/pause', { headers: auth }),
  });
  add({
    id: 'queues-get-paused',
    family: 'queues',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/queues/wire-q', { headers: auth }),
  });
  add({
    id: 'queues-paused-publish',
    family: 'queues',
    kind: 'ok',
    settle: true,
    // Published while the queue is paused: the row stays pending, so `lag` is 1.
    run: (c) =>
      call(c.request, c, 'POST', `/v2/enqueue/wire-q/${c.sink}/ok`, {
        headers: auth,
        body: 'paused-body',
      }),
  });
  add({
    id: 'queues-get-lag',
    family: 'queues',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/queues/wire-q', { headers: auth }),
  });
  add({
    id: 'queues-resume',
    family: 'queues',
    kind: 'ok',
    settle: true,
    run: (c) => call(c.request, c, 'POST', '/v2/queues/wire-q/resume', { headers: auth }),
  });
  add({
    id: 'queues-delete',
    family: 'queues',
    kind: 'ok',
    run: (c) => call(c.request, c, 'DELETE', '/v2/queues/wire-q', { headers: auth }),
  });
  add({
    id: 'queues-get-after-delete',
    family: 'queues',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/queues/wire-q', { headers: auth }),
  });
  add({
    id: 'queues-pause-unknown',
    family: 'queues',
    kind: 'error',
    run: (c) => call(c.request, c, 'POST', '/v2/queues/wire-missing/pause', { headers: auth }),
  });
  add({
    id: 'queues-invalid-parallelism',
    family: 'queues',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/queues', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ queueName: 'wire-bad', parallelism: 0 }),
      }),
  });
  add({
    id: 'queues-invalid-name',
    family: 'queues',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/queues', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ queueName: 'wire bad name' }),
      }),
  });

  // -------------------------------------------------------------------------
  // 9. topics (URL groups) + endpoints
  // -------------------------------------------------------------------------
  add({
    id: 'topics-add-endpoints',
    family: 'topics',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/topics/wire-topic/endpoints', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({
          endpoints: [
            { name: 'primary', url: `${c.sink}/ok` },
            { name: 'secondary', url: `${c.sink}/alt` },
          ],
        }),
      }),
  });
  add({
    id: 'topics-list',
    family: 'topics',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/topics', { headers: auth }),
  });
  add({
    id: 'topics-get',
    family: 'topics',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/topics/wire-topic', { headers: auth }),
  });
  add({
    id: 'topics-publish-fanout',
    family: 'topics',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', '/v2/publish/wire-topic', {
        headers: auth,
        body: 'fanout-body',
      }),
  });
  add({
    id: 'topics-remove-one-endpoint',
    family: 'topics',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'DELETE', '/v2/topics/wire-topic/endpoints', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ endpoints: [{ url: `${c.sink}/alt` }] }),
      }),
  });
  add({
    id: 'topics-get-after-remove',
    family: 'topics',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/topics/wire-topic', { headers: auth }),
  });
  add({
    id: 'topics-delete',
    family: 'topics',
    kind: 'ok',
    run: (c) => call(c.request, c, 'DELETE', '/v2/topics/wire-topic', { headers: auth }),
  });
  add({
    id: 'topics-get-after-delete',
    family: 'topics',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/topics/wire-topic', { headers: auth }),
  });
  add({
    id: 'topics-empty-endpoints',
    family: 'topics',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/topics/wire-topic-2/endpoints', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ endpoints: [] }),
      }),
  });
  add({
    id: 'topics-blocked-endpoint',
    family: 'topics',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/topics/wire-topic-3/endpoints', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ endpoints: [{ url: 'http://169.254.169.254/blocked' }] }),
      }),
  });

  // -------------------------------------------------------------------------
  // 10. schedules (create, list, get, pause, resume, delete)
  // -------------------------------------------------------------------------
  add({
    id: 'schedules-create',
    family: 'schedules',
    kind: 'ok',
    // Fires on 1 January: never during a run, so `nextScheduleTime` is a stable shape and
    // no delivery is produced.
    run: async (c) => {
      const r = await call(c.request, c, 'POST', `/v2/schedules/${c.sink}/ok`, {
        headers: {
          ...auth,
          'upstash-cron': '0 0 1 1 *',
          'upstash-method': 'POST',
          'content-type': 'application/json',
        },
        body: '{"schedule":true}',
      });
      c.bag.scheduleId = JSON.parse(r.response.body.toString('utf8')).scheduleId;
      return r;
    },
  });
  add({
    id: 'schedules-list',
    family: 'schedules',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/schedules', { headers: auth }),
  });
  add({
    id: 'schedules-get',
    family: 'schedules',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', `/v2/schedules/${c.bag.scheduleId}`, { headers: auth }),
  });
  add({
    id: 'schedules-pause',
    family: 'schedules',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/schedules/${c.bag.scheduleId}/pause`, { headers: auth }),
  });
  add({
    id: 'schedules-get-paused',
    family: 'schedules',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', `/v2/schedules/${c.bag.scheduleId}`, { headers: auth }),
  });
  add({
    id: 'schedules-resume',
    family: 'schedules',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/schedules/${c.bag.scheduleId}/resume`, { headers: auth }),
  });
  add({
    id: 'schedules-delete',
    family: 'schedules',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'DELETE', `/v2/schedules/${c.bag.scheduleId}`, { headers: auth }),
  });
  add({
    id: 'schedules-get-after-delete',
    family: 'schedules',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', `/v2/schedules/${c.bag.scheduleId}`, { headers: auth }),
  });
  add({
    id: 'schedules-missing-cron',
    family: 'schedules',
    kind: 'error',
    run: (c) => call(c.request, c, 'POST', `/v2/schedules/${c.sink}/ok`, { headers: auth }),
  });
  add({
    id: 'schedules-invalid-cron',
    family: 'schedules',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/schedules/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-cron': 'not a cron' },
      }),
  });
  add({
    id: 'schedules-missing-destination',
    family: 'schedules',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/schedules/', {
        headers: { ...auth, 'upstash-cron': '0 0 1 1 *' },
      }),
  });

  // -------------------------------------------------------------------------
  // 11. flow control (query, pause, resume, pin, resetRate, unpin)
  // -------------------------------------------------------------------------
  add({
    id: 'flowcontrol-get',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/flowControl/wire-fc-quiet', { headers: auth }),
  });
  add({
    id: 'flowcontrol-pause',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) => call(c.request, c, 'POST', '/v2/flowControl/wire-fc-quiet/pause', { headers: auth }),
  });
  add({
    id: 'flowcontrol-get-paused',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/flowControl/wire-fc-quiet', { headers: auth }),
  });
  add({
    id: 'flowcontrol-resume',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/flowControl/wire-fc-quiet/resume', { headers: auth }),
  });
  add({
    id: 'flowcontrol-pin',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/flowControl/wire-fc-quiet/pin?parallelism=3&rate=7', {
        headers: auth,
      }),
  });
  add({
    id: 'flowcontrol-get-pinned',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/flowControl/wire-fc-quiet', { headers: auth }),
  });
  add({
    id: 'flowcontrol-reset-rate',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/flowControl/wire-fc-quiet/resetRate', { headers: auth }),
  });
  add({
    id: 'flowcontrol-unpin',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/flowControl/wire-fc-quiet/unpin', { headers: auth }),
  });
  add({
    id: 'flowcontrol-get-unpinned',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/flowControl/wire-fc-quiet', { headers: auth }),
  });
  add({
    id: 'flowcontrol-global-parallelism',
    family: 'flowcontrol',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/globalParallelism', { headers: auth }),
  });
  add({
    id: 'flowcontrol-get-unknown',
    family: 'flowcontrol',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/flowControl/wire-fc-missing', { headers: auth }),
  });
  add({
    id: 'flowcontrol-pin-bad-period',
    family: 'flowcontrol',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/flowControl/wire-fc-quiet/pin?period=5x', { headers: auth }),
  });

  // -------------------------------------------------------------------------
  // 12. workflow: wait / notify / run query / cancel
  // -------------------------------------------------------------------------
  add({
    id: 'workflow-wait',
    family: 'workflow',
    kind: 'ok',
    run: async (c) => {
      const r = await call(c.request, c, 'POST', '/v2/wait/wire-event-1', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({
          url: `${c.sink}/wf`,
          timeout: '60s',
          step: { stepId: 1, stepName: 'wait-for-wire', concurrent: 1, targetStep: 0 },
        }),
      });
      c.bag.waiterId = JSON.parse(r.response.body.toString('utf8')).waiterId;
      return r;
    },
  });
  add({
    id: 'workflow-waiters',
    family: 'workflow',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/waiters/wire-event-1', { headers: auth }),
  });
  add({
    id: 'workflow-notify',
    family: 'workflow',
    kind: 'ok',
    settle: true,
    run: (c) =>
      call(c.request, c, 'POST', '/v2/notify/wire-event-1', {
        headers: { ...auth, 'content-type': 'text/plain' },
        body: 'wire-event-payload',
      }),
  });
  add({
    id: 'workflow-notify-unknown-event',
    family: 'workflow',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/notify/wire-event-missing', {
        headers: { ...auth, 'content-type': 'text/plain' },
        body: 'no-waiter',
      }),
  });
  add({
    id: 'workflow-run-seed',
    family: 'workflow',
    kind: 'ok',
    // Delayed so the step row exists while no delivery has happened: later reads and the
    // cancel below are then independent of delivery timing.
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/wf`, {
        headers: {
          ...auth,
          'content-type': 'application/json',
          'upstash-workflow-runid': 'wire-run-1',
          'upstash-delay': '3600',
        },
        body: '{"step":"init"}',
      }),
  });
  add({
    id: 'workflow-run-get',
    family: 'workflow',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/workflows/runs/wire-run-1', { headers: auth }),
  });
  add({
    id: 'workflow-logs-by-run',
    family: 'workflow',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'GET', '/v2/workflows/logs?workflowRunId=wire-run-1', { headers: auth }),
  });
  add({
    id: 'workflow-logs-all',
    family: 'workflow',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/workflows/logs', { headers: auth }),
  });
  add({
    id: 'workflow-run-cancel',
    family: 'workflow',
    kind: 'ok',
    run: (c) => call(c.request, c, 'DELETE', '/v2/workflows/runs/wire-run-1', { headers: auth }),
  });
  add({
    id: 'workflow-run-get-after-cancel',
    family: 'workflow',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/workflows/runs/wire-run-1', { headers: auth }),
  });
  add({
    id: 'workflow-run-get-unknown',
    family: 'workflow',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/workflows/runs/wire-run-missing', { headers: auth }),
  });
  add({
    id: 'workflow-bulk-cancel-none',
    family: 'workflow',
    kind: 'ok',
    run: (c) =>
      call(c.request, c, 'DELETE', '/v2/workflows/runs', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ workflowRunIds: ['wire-run-missing'] }),
      }),
  });
  add({
    id: 'workflow-wait-missing-url',
    family: 'workflow',
    kind: 'error',
    // `url` is NOT NULL upstream: a missing url is a 500, not a 400. Recorded as-is.
    run: (c) =>
      call(c.request, c, 'POST', '/v2/wait/wire-event-2', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({ timeout: '60s' }),
      }),
  });
  add({
    id: 'workflow-wait-invalid-json',
    family: 'workflow',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', '/v2/wait/wire-event-3', {
        headers: { ...auth, 'content-type': 'application/json' },
        body: 'not json',
      }),
  });

  // -------------------------------------------------------------------------
  // 13. logs / events with cursor pagination (ordered last so every delivery above has
  //     settled and the event set is fixed)
  // -------------------------------------------------------------------------
  add({
    id: 'logs-by-message-page-1',
    family: 'logs',
    kind: 'ok',
    run: async (c) => {
      const r = await call(
        c.request,
        c,
        'GET',
        `/v2/logs?messageId=${c.bag.publishedId}&count=1`,
        { headers: auth },
      );
      const parsed = JSON.parse(r.response.body.toString('utf8'));
      c.bag.logCursor = parsed.cursor || null;
      return r;
    },
  });
  add({
    id: 'logs-by-message-page-2',
    family: 'logs',
    kind: 'ok',
    run: (c) =>
      call(
        c.request,
        c,
        'GET',
        `/v2/logs?messageId=${c.bag.publishedId}&count=1&cursor=${c.bag.logCursor}`,
        { headers: auth },
      ),
  });
  add({
    id: 'events-filter-delivered',
    family: 'logs',
    kind: 'ok',
    // Scoped to ONE message on purpose: two fan-out deliveries of the same publish land in
    // the same millisecond, and their relative order inside one page is a scheduling
    // accident, not contract. §6 lists that as a blind spot instead of hiding it here.
    run: (c) =>
      call(c.request, c, 'GET', `/v2/events?count=5&state=DELIVERED&messageId=${c.bag.publishedId}`, {
        headers: auth,
      }),
  });
  add({
    id: 'events-state-empty',
    family: 'logs',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/events?count=2&state=RETRY', { headers: auth }),
  });
  add({
    id: 'events-empty-state',
    family: 'logs',
    kind: 'ok',
    run: (c) => call(c.request, c, 'GET', '/v2/events?state=NO_SUCH_STATE', { headers: auth }),
  });
  add({
    id: 'logs-unauthorized',
    family: 'logs',
    kind: 'error',
    // No per-input failure exists on this endpoint (an unknown cursor is ignored), so the
    // family's error path is the auth gate. Documented in the coverage table.
    run: (c) => call(c.request, c, 'GET', '/v2/logs', {}),
  });

  // -------------------------------------------------------------------------
  // 14. shared error envelope: 401, 404, 412
  // -------------------------------------------------------------------------
  add({
    id: 'error-401-missing-token',
    family: 'errors',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/messages/msg_01WIREGOLDENUNKNOWNAAAAAAA', {}),
  });
  add({
    id: 'error-401-query-token-bad',
    family: 'errors',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'GET', '/v2/keys?qstash_token=wire-wrong-token', {}),
  });
  add({
    id: 'error-404-unknown-route',
    family: 'errors',
    kind: 'error',
    run: (c) => call(c.request, c, 'GET', '/v2/no-such-route', { headers: auth }),
  });
  add({
    id: 'error-412-oversize-publish',
    family: 'errors',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: auth,
        body: 'z'.repeat(oversizeBytes),
      }),
  });
  add({
    id: 'error-400-invalid-method',
    family: 'errors',
    kind: 'error',
    run: (c) =>
      call(c.request, c, 'POST', `/v2/publish/${c.sink}/ok`, {
        headers: { ...auth, 'upstash-method': 'TRACE' },
        body: 'trace-body',
      }),
  });

  void service;
  void settleAll;

  return {
    version: CORPUS_VERSION,
    steps,
    /** Every family the contract requires, with the ids that cover its two directions. */
    families: [
      'health',
      'publish',
      'enqueue',
      'batch',
      'messages',
      'dlq',
      'keys',
      'queues',
      'topics',
      'schedules',
      'flowcontrol',
      'workflow',
      'logs',
      'errors',
    ],
  };
}
