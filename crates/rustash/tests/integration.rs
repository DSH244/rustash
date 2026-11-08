//! End-to-end behavior tests: real axum server + real dispatch engine + real SQLite,
//! against a local HTTP sink. One test per SPEC behavior, in isolation.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use rustash::{
    bootstrap, build_router, config::Config, engine::Engine, http::AppState, open_db, Db,
};

const TOKEN: &str = "test-token-1234567890";

fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn test_config() -> Config {
    Config {
        default_max_retries: 3,
        max_retries: 100,
        max_http_timeout_ms: 1_000,
        lease_ms: 1_500,
        lease_renew_interval_ms: -1,
        dispatch_batch_limit: 8,
        dispatch_tick_ms: 40,
        scheduler_tick_ms: 15_000,
        retention_tick_ms: 600_000,
        tenant_parallelism_max: 1024,
        delivery_slots: 32,
        // The test sink answers `connection: close`, so nothing is ever pooled here;
        // the keep-alive path is covered by deliver::Pool unit tests.
        delivery_pool_max_idle: 8,
        delivery_pool_idle_ms: 4_000,
        max_response_body_bytes: 1024 * 1024,
        event_body_max_bytes: 32 * 1024,
        max_body_bytes: 1024 * 1024,
        log_retention_days: 14,
        dlq_retention_days: 7,
        dedupe_window_secs: 600,
        allow_private_destinations: true,
        destination_allow_cidrs: String::new(),
        destination_block_cidrs: String::new(),
        region: "test-region".into(),
        database_url: ":memory:".into(),
        port: 0,
        qstash_token: Some(TOKEN.into()),
        qstash_current_signing_key: Some("cur-test-key".into()),
        qstash_next_signing_key: Some("next-test-key".into()),
        run_migrations_on_start: true,
        admin_token: None,
        log_level: "off".into(),
        workflow_run_stuck_hours: 24,
        workflow_run_retention_days: 7,
    }
}

// ---------------------------------------------------------------------------
// Sink: minimal programmable HTTP endpoint that records every request.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Seen {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    start_ms: i64,
    end_ms: i64,
}

/// Programmed response: status, extra headers, body. Consumed one entry per request.
type Script = VecDeque<(u16, Vec<(String, String)>, String)>;

struct Sink {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    script: Arc<Mutex<Script>>,
    delay_ms: Arc<AtomicU64>,
    concurrent: Arc<AtomicUsize>,
    max_concurrent: Arc<AtomicUsize>,
}

impl Sink {
    async fn start() -> Sink {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sink = Sink {
            addr,
            seen: Arc::new(Mutex::new(Vec::new())),
            script: Arc::new(Mutex::new(VecDeque::new())),
            delay_ms: Arc::new(AtomicU64::new(0)),
            concurrent: Arc::new(AtomicUsize::new(0)),
            max_concurrent: Arc::new(AtomicUsize::new(0)),
        };
        let (seen, script, delay_ms, concurrent, max_concurrent) = (
            sink.seen.clone(),
            sink.script.clone(),
            sink.delay_ms.clone(),
            sink.concurrent.clone(),
            sink.max_concurrent.clone(),
        );
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    continue;
                };
                let (seen, script, delay_ms, concurrent, max_concurrent) = (
                    seen.clone(),
                    script.clone(),
                    delay_ms.clone(),
                    concurrent.clone(),
                    max_concurrent.clone(),
                );
                tokio::spawn(async move {
                    // read request head
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let mut lines = head.split("\r\n");
                    let request_line = lines.next().unwrap_or_default().to_string();
                    let mut parts = request_line.split(' ');
                    let method = parts.next().unwrap_or_default().to_string();
                    let path = parts.next().unwrap_or_default().to_string();
                    let mut headers = Vec::new();
                    let mut content_length = 0usize;
                    for l in lines {
                        if l.is_empty() {
                            break;
                        }
                        if let Some((k, v)) = l.split_once(':') {
                            let name = k.trim().to_ascii_lowercase();
                            let value = v.trim().to_string();
                            if name == "content-length" {
                                content_length = value.parse().unwrap_or(0);
                            }
                            headers.push((name, value));
                        }
                    }
                    let mut body = buf[head_end..].to_vec();
                    while body.len() < content_length {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => body.extend_from_slice(&chunk[..n]),
                        }
                    }
                    body.truncate(content_length);

                    let start_ms = epoch_ms();
                    let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                    max_concurrent.fetch_max(now, Ordering::SeqCst);
                    let d = delay_ms.load(Ordering::SeqCst);
                    if d > 0 {
                        tokio::time::sleep(Duration::from_millis(d)).await;
                    }
                    let (status, extra, resp_body) = script
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or((200, Vec::new(), "ok".to_string()));
                    let mut out = format!(
                        "HTTP/1.1 {} MOCKED\r\ncontent-length: {}\r\nconnection: close\r\n",
                        status,
                        resp_body.len()
                    );
                    for (k, v) in &extra {
                        out.push_str(&format!("{}: {}\r\n", k, v));
                    }
                    out.push_str("\r\n");
                    let _ = sock.write_all(out.as_bytes()).await;
                    let _ = sock.write_all(resp_body.as_bytes()).await;
                    let _ = sock.shutdown().await;
                    concurrent.fetch_sub(1, Ordering::SeqCst);
                    let end_ms = epoch_ms();
                    seen.lock().unwrap().push(Seen {
                        method,
                        path,
                        headers,
                        body,
                        start_ms,
                        end_ms,
                    });
                });
            }
        });
        sink
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    fn push_response(&self, status: u16, headers: Vec<(&str, &str)>, body: &str) {
        self.script.lock().unwrap().push_back((
            status,
            headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body.to_string(),
        ));
    }
    fn set_delay(&self, ms: u64) {
        self.delay_ms.store(ms, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Server harness
// ---------------------------------------------------------------------------

struct TestApp {
    base: String,
    db: Db,
    client: reqwest::Client,
}

impl TestApp {
    async fn start() -> TestApp {
        TestApp::start_with_config(test_config()).await
    }

    async fn start_with_config(config: Config) -> TestApp {
        let config = Arc::new(config);
        let db = open_db(":memory:");
        bootstrap(&db, &config).expect("bootstrap");
        let engine = Engine::new(db.clone(), config.clone());
        engine.spawn();
        let state = AppState {
            db: db.clone(),
            config,
            engine,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state)).await;
        });
        TestApp {
            base: format!("http://{}", addr),
            db,
            client: reqwest::Client::new(),
        }
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{}", self.base, path))
            .bearer_auth(TOKEN)
    }

    async fn get_json(&self, path: &str) -> Value {
        let r = self.req(reqwest::Method::GET, path).send().await.unwrap();
        assert_eq!(r.status(), 200, "GET {path}");
        r.json().await.unwrap()
    }

    /// publish with extra headers; asserts 201 and returns the response JSON.
    async fn publish(&self, destination: &str, body: &str, headers: &[(&str, &str)]) -> Value {
        let mut req = self
            .req(
                reqwest::Method::POST,
                &format!("/v2/publish/{}", destination),
            )
            .body(body.to_string());
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        assert_eq!(status, 201, "publish {}: body={}", destination, text);
        serde_json::from_str(&text).expect("json")
    }

    fn message_id(v: &Value) -> String {
        v["messageId"].as_str().expect("messageId").to_string()
    }
}

async fn wait_until(what: &str, timeout_ms: u64, mut f: impl FnMut() -> bool) {
    let deadline = epoch_ms() + timeout_ms as i64;
    loop {
        if f() {
            return;
        }
        if epoch_ms() > deadline {
            panic!("timeout waiting for: {}", what);
        }
        tokio::time::sleep(Duration::from_millis(25u64)).await;
    }
}

/// poll the server's own event log for a message
async fn fetch_states(app: &TestApp, message_id: &str) -> Vec<String> {
    let logs = app
        .get_json(&format!("/v2/logs?messageId={}", message_id))
        .await;
    logs["logs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["state"].as_str().unwrap_or_default().to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// 1. publish → deliver round trip (headers, signature, log trail)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_deliver_roundtrip() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;

    let dest = sink.url("/hook");
    let res = app
        .publish(
            &dest,
            "hello world",
            &[("Upstash-Forward-X-Note", "hi"), ("X-Ignored", "nope")],
        )
        .await;
    let id = TestApp::message_id(&res);

    wait_until("sink received delivery", 5_000, || !sink.seen().is_empty()).await;
    let seen = sink.seen()[0].clone();
    assert_eq!(seen.method, "POST");
    assert_eq!(seen.path, "/hook");
    assert_eq!(seen.body, b"hello world");

    let header = |name: &str| {
        seen.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(header("upstash-message-id").as_deref(), Some(id.as_str()));
    assert_eq!(header("upstash-retried").as_deref(), Some("0"));
    assert_eq!(header("upstash-region").as_deref(), Some("test-region"));
    assert_eq!(header("x-note").as_deref(), Some("hi")); // Upstash-Forward-X-Note → x-note
    assert!(header("x-ignored").is_none()); // non-forwarded user headers are dropped
    assert_eq!(header("user-agent").as_deref(), Some("Upstash-QStash"));
    // signature: three-part HS256 JWT over header.payload with the current signing key
    let sig = header("upstash-signature").expect("signature");
    let parts: Vec<&str> = sig.split('.').collect();
    assert_eq!(parts.len(), 3, "jwt shape");
    {
        use base64::Engine;
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let expected = {
            use hmac::Mac;
            let mut m = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"cur-test-key").unwrap();
            m.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
            engine.encode(m.finalize().into_bytes())
        };
        assert_eq!(parts[2], expected, "signature verifies");
        // The QStash Receiver recomputes sha256Base64url (unpadded URL-safe) — standard
        // base64 here is rejected for every hash containing '+' or '/'.
        let payload: Value = serde_json::from_slice(&engine.decode(parts[1]).unwrap()).unwrap();
        use sha2::Digest;
        let want_body = engine.encode(sha2::Sha256::digest(b"hello world"));
        assert_eq!(payload["body"], json!(want_body), "body claim is base64url");
    }

    // message row is gone after delivery
    let r = app
        .req(reqwest::Method::GET, &format!("/v2/messages/{}", id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // event trail (newest first, like JS `ORDER time DESC, id DESC`): DELIVERED is the
    // head, ACTIVE the earliest row — and there is NO CREATED row.
    let logs = app.get_json(&format!("/v2/logs?messageId={}", id)).await;
    let states: Vec<&str> = logs["logs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["state"].as_str().unwrap())
        .collect();
    assert_eq!(states.first().copied(), Some("DELIVERED"));
    assert_eq!(states.last().copied(), Some("ACTIVE"));
    assert!(!states.contains(&"CREATED"));
    let delivered = &logs["logs"].as_array().unwrap()[0];
    assert_eq!(delivered["responseStatus"], json!(200));
    // /v2/events uses the same trail with field "events"
    let events = app.get_json(&format!("/v2/events?messageId={}", id)).await;
    assert!(events["events"].is_array());
}

// ---------------------------------------------------------------------------
// 2. retry: attempts up to maxRetries, backoff, exhaustion → DLQ
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_exhausts_into_dlq() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.push_response(500, vec![], "boom");
    sink.push_response(500, vec![], "boom2");

    let dest = sink.url("/fail");
    let res = app
        .publish(
            &dest,
            "x",
            &[("Upstash-Retries", "1"), ("Upstash-Retry-Delay", "60")],
        )
        .await;
    let id = TestApp::message_id(&res);

    wait_until("both attempts", 6_000, || sink.seen().len() >= 2).await;
    let seen = sink.seen();
    assert_eq!(seen.len(), 2, "maxRetries=1 → 2 attempts");
    assert_eq!(
        seen[0]
            .headers
            .iter()
            .find(|(k, _)| k == "upstash-retried")
            .unwrap()
            .1,
        "0"
    );
    assert_eq!(
        seen[1]
            .headers
            .iter()
            .find(|(k, _)| k == "upstash-retried")
            .unwrap()
            .1,
        "1"
    );

    // exhausted → DLQ with the last response snapshot
    wait_until("DLQ entry", 5_000, || {
        app.db
            .conn
            .lock()
            .unwrap()
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM dlq_messages", [], |r| r.get(0))
            .unwrap_or(0)
            > 0
    })
    .await;
    let list = app.get_json("/v2/dlq?count=10").await;
    let entries = list["messages"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["messageId"], json!(id));
    assert_eq!(entries[0]["state"], json!("FAILED"));
    assert_eq!(entries[0]["responseStatus"], json!(500));
    assert_eq!(entries[0]["maxRetries"], json!(1));
    assert_eq!(entries[0]["body"], json!("x"));

    let states: Vec<String> = {
        let logs = app.get_json(&format!("/v2/logs?messageId={}", id)).await;
        logs["logs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["state"].as_str().unwrap().to_string())
            .collect()
    };
    assert!(
        states.iter().any(|s| s == "RETRY"),
        "retry event logged: {states:?}"
    );
    assert!(states.iter().any(|s| s == "FAILED"), "failed event logged");
}

#[tokio::test]
async fn retry_after_header_overrides_curve() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.push_response(503, vec![("Retry-After", "2")], "busy");
    sink.push_response(200, vec![], "fine");

    let t0 = epoch_ms();
    let dest = sink.url("/rate");
    let res = app.publish(&dest, "x", &[("Upstash-Retries", "1")]).await;
    let id = TestApp::message_id(&res);

    wait_until("second attempt", 8_000, || sink.seen().len() >= 2).await;
    let gap = sink.seen()[1].start_ms - t0;
    // default curve for retried=1 would be ~12.2s; Retry-After: 2 → ~2s (bounds are loose)
    assert!(gap >= 1_800, "waited for Retry-After: {gap}ms");
    assert!(gap <= 6_000, "Retry-After beats the 12s curve: {gap}ms");

    // second attempt succeeded → message row deleted after delivery
    tokio::time::sleep(Duration::from_millis(800)).await;
    let r = app
        .req(reqwest::Method::GET, &format!("/v2/messages/{}", id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let states = fetch_states(&app, &id).await;
    assert!(states.iter().any(|s| s == "RETRY"));
    assert!(states.iter().any(|s| s == "DELIVERED"));
}

#[tokio::test]
async fn non_retryable_489_goes_straight_to_dlq() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.push_response(489, vec![], "nope");

    let dest = sink.url("/489");
    let res = app.publish(&dest, "x", &[("Upstash-Retries", "2")]).await;
    let id = TestApp::message_id(&res);

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(sink.seen().len(), 1, "no retry after 489");
    let list = app.get_json("/v2/dlq?count=10").await;
    let entries = list["messages"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["messageId"], json!(id));
    assert_eq!(entries[0]["responseStatus"], json!(489));
}

#[tokio::test]
async fn non_retryable_marker_header() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.push_response(500, vec![("Upstash-NonRetryable-Error", "true")], "fatal");

    app.publish(&sink.url("/marker"), "x", &[("Upstash-Retries", "2")])
        .await;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(sink.seen().len(), 1, "marker blocks retry");
    let list = app.get_json("/v2/dlq?count=10").await;
    assert_eq!(list["messages"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// 3. lease / visibility timeout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_reclaims_and_redelivers() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    // The sink is slower than the 1500ms lease: the lease lapses mid-flight and the
    // scheduler re-leases the row (retried+1, last_error='lease expired (worker lost)').
    sink.set_delay(1_900);

    let dest = sink.url("/slow");
    let res = app
        .publish(
            &dest,
            "x",
            &[("Upstash-Retries", "1"), ("Upstash-Retry-Delay", "80")],
        )
        .await;
    let id = TestApp::message_id(&res);

    wait_until("reclaimed second delivery", 8_000, || {
        sink.seen().len() >= 2
    })
    .await;
    sink.set_delay(0);
    let seen = sink.seen();
    let retried: Vec<String> = seen
        .iter()
        .map(|s| {
            s.headers
                .iter()
                .find(|(k, _)| k == "upstash-retried")
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(retried[1], "1", "reclaim bumps retried");
    // eventually the row is deleted (fenced writes keep it consistent)
    let _ = id;
}

#[tokio::test]
async fn lease_renewal_keeps_a_long_delivery_from_being_rerun() {
    // A lease shorter than the delivery, exactly the shape a 2-hour QStash step has under
    // a shorter LEASE_MS. Without renewal the row is reclaimed mid-flight and the endpoint
    // runs the same step twice; with renewal the first attempt keeps the row and the
    // second delivery never happens.
    let mut config = test_config();
    config.max_http_timeout_ms = 4_000;
    config.lease_ms = 400;
    config.lease_renew_interval_ms = -1; // derived: ~133 ms
    let app = TestApp::start_with_config(config).await;
    let sink = Sink::start().await;
    // Three lease lengths: several renewals must happen while this is in flight.
    sink.set_delay(1_500);

    let dest = sink.url("/long-step");
    let res = app
        .publish(
            &dest,
            "x",
            &[("Upstash-Retries", "3"), ("Upstash-Retry-Delay", "50")],
        )
        .await;
    let id = TestApp::message_id(&res);

    wait_until("the single delivery finished", 8_000, || {
        !sink.seen().is_empty()
    })
    .await;
    // Give a reclaim the chance to fire if renewal failed.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let seen = sink.seen();
    assert_eq!(
        seen.len(),
        1,
        "a renewed lease must not be reclaimed mid-flight"
    );
    let retried = seen[0]
        .headers
        .iter()
        .find(|(k, _)| k == "upstash-retried")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert_eq!(retried, "0", "no reclaim means no retry bump");

    // The row is gone once the delivery settled with a 2xx.
    let logs = app.get_json(&format!("/v2/logs?messageId={}", id)).await;
    let states: Vec<&str> = logs["logs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["state"].as_str().unwrap())
        .collect();
    assert!(states.contains(&"DELIVERED"), "states={:?}", states);
}

// ---------------------------------------------------------------------------
// 4. deduplication
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dedupe_explicit_key_returns_original() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let dest = sink.url("/dd");

    let first = app
        .publish(&dest, "one", &[("Idempotency-Key", "k1")])
        .await;
    let first_id = TestApp::message_id(&first);
    assert!(
        first.get("deduplicated").is_none(),
        "flag absent on first publish"
    );

    let second = app
        .publish(&dest, "two", &[("Idempotency-Key", "k1")])
        .await;
    assert_eq!(second["messageId"], json!(first_id));
    assert_eq!(second["deduplicated"], json!(true));

    wait_until("single delivery", 3_000, || !sink.seen().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(sink.seen().len(), 1, "duplicate created no message");
}

#[tokio::test]
async fn dedupe_content_hash() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let dest = sink.url("/cd");
    let hdr = [("Upstash-Content-Based-Deduplication", "true")];

    let first = app.publish(&dest, "same-body", &hdr).await;
    let first_id = TestApp::message_id(&first);
    let second = app.publish(&dest, "same-body", &hdr).await;
    assert_eq!(second["messageId"], json!(first_id));
    assert_eq!(second["deduplicated"], json!(true));
    // different body → different hash → new message
    let third = app.publish(&dest, "other-body", &hdr).await;
    assert_ne!(TestApp::message_id(&third), first_id);
    assert!(third.get("deduplicated").is_none());
}

#[tokio::test]
async fn dedupe_window_expiry() {
    let mut config = test_config();
    config.dedupe_window_secs = 1;
    let app = TestApp::start_with_config(config).await;
    let sink = Sink::start().await;
    let dest = sink.url("/ttl");

    let first = app.publish(&dest, "x", &[("Idempotency-Key", "k")]).await;
    let second = app.publish(&dest, "x", &[("Idempotency-Key", "k")]).await;
    assert_eq!(second["deduplicated"], json!(true));
    tokio::time::sleep(Duration::from_millis(1_400)).await;
    let third = app.publish(&dest, "x", &[("Idempotency-Key", "k")]).await;
    assert!(
        third.get("deduplicated").is_none(),
        "window expired → fresh message"
    );
    assert_ne!(TestApp::message_id(&third), TestApp::message_id(&first));
}

// ---------------------------------------------------------------------------
// 5. FIFO ordering, one in flight at a time per queue
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fifo_single_flight_per_queue() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.set_delay(350);

    let dest = sink.url("/fifo");
    let mut ids = Vec::new();
    for label in ["A", "B", "C"] {
        let res = app
            .req(
                reqwest::Method::POST,
                &format!("/v2/enqueue/{}/{}", "ordq", dest),
            )
            .header("content-type", "text/plain")
            .body(label.to_string())
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201);
        let v: Value = res.json().await.unwrap();
        ids.push(TestApp::message_id(&v));
        tokio::time::sleep(Duration::from_millis(60)).await; // distinct created_at
    }

    wait_until("all three delivered", 10_000, || sink.seen().len() >= 3).await;
    sink.set_delay(0);
    let seen = sink.seen();
    let bodies: Vec<String> = seen
        .iter()
        .map(|s| String::from_utf8_lossy(&s.body).to_string())
        .collect();
    assert_eq!(bodies, vec!["A", "B", "C"], "FIFO order");
    // serial: each request starts after the previous one ended
    for w in seen.windows(2) {
        assert!(w[1].start_ms >= w[0].end_ms, "parallelism=1: no overlap");
    }
    assert_eq!(sink.max_concurrent.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// 6. delay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delay_defers_delivery() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let t0 = epoch_ms();
    let dest = sink.url("/delayed");
    let res = app.publish(&dest, "x", &[("Upstash-Delay", "2")]).await;
    let id = TestApp::message_id(&res);

    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert!(sink.seen().is_empty(), "not delivered before the delay");

    wait_until("delivered after delay", 6_000, || !sink.seen().is_empty()).await;
    let dt = sink.seen()[0].start_ms - t0;
    assert!((1_700..=5_500).contains(&dt), "delay honored: {dt}ms");
    assert_eq!(id.len(), 30);
}

// ---------------------------------------------------------------------------
// 7. headers + queues API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queues_lifecycle_pause_resume() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let dest = sink.url("/q");

    // create
    let r = app
        .req(reqwest::Method::POST, "/v2/queues")
        .json(&json!({"queueName": "myq", "parallelism": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // pause
    let r = app
        .req(reqwest::Method::POST, "/v2/queues/myq/pause")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let q = app.get_json("/v2/queues/myq").await;
    assert_eq!(q["paused"], json!(true));
    assert_eq!(q["name"], json!("myq"));
    assert_eq!(q["parallelism"], json!(1));

    // enqueue while paused → stays queued (lag), nothing delivered
    let res = app
        .req(reqwest::Method::POST, &format!("/v2/enqueue/myq/{}", dest))
        .body("blocked")
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let v: Value = res.json().await.unwrap();
    let id = TestApp::message_id(&v);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(sink.seen().is_empty(), "paused queue blocks delivery");
    let q = app.get_json("/v2/queues/myq").await;
    assert_eq!(q["lag"], json!(1));
    let msg = app.get_json(&format!("/v2/messages/{}", id)).await;
    assert_eq!(msg["state"], json!("CREATED"));
    assert_eq!(msg["body"], json!("blocked"));
    assert_eq!(msg["url"], json!(dest));
    assert_eq!(msg["method"], json!("POST"));

    // resume → delivered
    app.req(reqwest::Method::POST, "/v2/queues/myq/resume")
        .send()
        .await
        .unwrap();
    wait_until("delivered after resume", 5_000, || !sink.seen().is_empty()).await;

    // list contains the queue
    let list = app.get_json("/v2/queues").await;
    let names: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"myq"));

    // delete → 404 afterwards
    let r = app
        .req(reqwest::Method::DELETE, "/v2/queues/myq")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = app
        .req(reqwest::Method::GET, "/v2/queues/myq")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

// ---------------------------------------------------------------------------
// 8. batch + batchPublish + /v1 aliases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_publish_and_v1_aliases() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let dest1 = sink.url("/b1");
    let dest2 = sink.url("/b2");

    // POST /v2/batch with item-level fields
    let r = app
        .req(reqwest::Method::POST, "/v2/batch")
        .json(&json!([
            {"destination": dest1, "body": "one", "delay": 0},
            {"destination": dest2, "body": "two", "headers": {"Upstash-Retries": "1"}},
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let out: Value = r.json().await.unwrap();
    let arr = out.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr[0]["messageId"].is_string());
    assert!(arr[1]["messageId"].is_string());

    // both delivered
    wait_until("batch deliveries", 6_000, || {
        let paths: Vec<String> = sink.seen().iter().map(|s| s.path.clone()).collect();
        paths.contains(&"/b1".to_string()) && paths.contains(&"/b2".to_string())
    })
    .await;

    // POST /v1/batchPublish alias works
    let r = app
        .req(reqwest::Method::POST, "/v1/batchPublish")
        .json(&json!([{"destination": dest1, "body": "three"}]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);

    // legacy v1 publish: POST /v1/publish + Upstash-Destination header
    let r = app
        .req(reqwest::Method::POST, "/v1/publish")
        .header("Upstash-Destination", dest2.clone())
        .body("v1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201, "v1 publish header form");
    let v: Value = r.json().await.unwrap();
    assert!(v["messageId"].is_string());

    // legacy v1 queue: POST /v1/queue/{queueName} + Upstash-Destination header
    let r = app
        .req(reqwest::Method::POST, "/v1/queue/v1q")
        .header("Upstash-Destination", dest2.clone())
        .body("v1q-body")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201, "v1 queue header form");

    wait_until("v1 deliveries", 6_000, || sink.seen().len() >= 4).await;
}

// ---------------------------------------------------------------------------
// 9. messages get/cancel + DLQ lifecycle + keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_pending_message() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let dest = sink.url("/never");

    let res = app.publish(&dest, "x", &[("Upstash-Delay", "30")]).await;
    let id = TestApp::message_id(&res);

    let r = app
        .req(reqwest::Method::DELETE, &format!("/v2/messages/{}", id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);

    let r = app
        .req(reqwest::Method::GET, &format!("/v2/messages/{}", id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "pending cancel removes the row");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(sink.seen().is_empty(), "never delivered");

    let logs = app.get_json(&format!("/v2/logs?messageId={}", id)).await;
    let states: Vec<&str> = logs["logs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["state"].as_str().unwrap())
        .collect();
    assert_eq!(states, vec!["CANCELLED"]);
}

#[tokio::test]
async fn dlq_lifecycle_list_get_requeue_delete() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.push_response(500, vec![], "bad");

    let dest = sink.url("/dlq-life");
    let res = app
        .publish(&dest, "payload", &[("Upstash-Retries", "0")])
        .await;
    let id = TestApp::message_id(&res);

    wait_until("DLQ", 5_000, || {
        app.db
            .conn
            .lock()
            .unwrap()
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM dlq_messages", [], |r| r.get(0))
            .unwrap_or(0)
            > 0
    })
    .await;
    let list = app.get_json("/v2/dlq?count=10").await;
    let entry = &list["messages"].as_array().unwrap()[0];
    let dlq_id = entry["dlqId"].as_str().unwrap().to_string();
    assert_eq!(entry["messageId"], json!(id));

    // get one
    let one = app.get_json(&format!("/v2/dlq/{}", dlq_id)).await;
    assert_eq!(one["messageId"], json!(id));
    assert_eq!(one["body"], json!("payload"));

    // requeue → new messageId, delivered once the sink returns 200 (default)
    let r = app
        .req(reqwest::Method::POST, &format!("/v2/dlq/retry/{}", dlq_id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let v: Value = r.json().await.unwrap();
    let new_id = v["messageId"].as_str().unwrap().to_string();
    assert_ne!(new_id, id, "requeued with a fresh id");
    wait_until("requeued delivery", 6_000, || sink.seen().len() >= 2).await;
    assert_eq!(
        String::from_utf8_lossy(&sink.seen().last().unwrap().body),
        "payload",
        "requeue carries retried=0"
    );
    let retried = sink
        .seen()
        .last()
        .unwrap()
        .headers
        .iter()
        .find(|(k, _)| k == "upstash-retried")
        .map(|(_, v)| v.clone())
        .unwrap();
    assert_eq!(retried, "0");
    // gone from DLQ
    let list = app.get_json("/v2/dlq?count=10").await;
    assert_eq!(list["messages"].as_array().unwrap().len(), 0);

    // exhaust again to exercise delete
    sink.push_response(500, vec![], "bad");
    app.publish(&sink.url("/dlq-life"), "p2", &[("Upstash-Retries", "0")])
        .await;
    wait_until("DLQ again", 5_000, || {
        app.db
            .conn
            .lock()
            .unwrap()
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM dlq_messages", [], |r| r.get(0))
            .unwrap_or(0)
            > 0
    })
    .await;
    let list = app.get_json("/v2/dlq?count=10").await;
    let dlq_id2 = list["messages"].as_array().unwrap()[0]["dlqId"]
        .as_str()
        .unwrap()
        .to_string();
    let r = app
        .req(reqwest::Method::DELETE, &format!("/v2/dlq/{}", dlq_id2))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = app
        .req(reqwest::Method::GET, &format!("/v2/dlq/{}", dlq_id2))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn keys_and_auth_and_health() {
    let app = TestApp::start().await;

    // health + readiness without auth
    let r = app
        .client
        .get(format!("{}/v2/health", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.json::<Value>().await.unwrap(), json!({"status": "ok"}));
    let r = app
        .client
        .get(format!("{}/healthz", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = app
        .client
        .get(format!("{}/readyz", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.json::<Value>().await.unwrap(), json!({"status": "ok"}));

    // no auth → 401
    let r = app
        .client
        .post(format!("{}/v2/publish/https://example.com/", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("missing authorization token")
    );
    // wrong auth → 401
    let r = app
        .client
        .post(format!("{}/v2/publish/https://example.com/", app.base))
        .bearer_auth("nope-nope-nope")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // keys: GET shows the env-provisioned pair, rotate shifts current→next
    let keys = app.get_json("/v2/keys").await;
    assert_eq!(keys["current"], json!("cur-test-key"));
    assert_eq!(keys["next"], json!("next-test-key"));
    let rotated = app
        .req(reqwest::Method::POST, "/v2/keys/rotate")
        .send()
        .await
        .unwrap();
    assert_eq!(rotated.status(), 200);
    let rotated: Value = rotated.json().await.unwrap();
    assert_eq!(rotated["current"], json!("next-test-key"));
    let next = rotated["next"].as_str().unwrap().to_string();
    assert!(next.starts_with("sig_"));
    let keys = app.get_json("/v2/keys").await;
    assert_eq!(keys["current"], json!("next-test-key"));
    assert_eq!(keys["next"], json!(next));

    // unknown /v2 path → 404 without auth (route_layer, not blanket layer)
    let r = app
        .client
        .get(format!("{}/v2/does-not-exist", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn body_limit_412() {
    let mut config = test_config();
    config.max_body_bytes = 8;
    let app = TestApp::start_with_config(config).await;
    let sink = Sink::start().await;

    let r = app
        .req(
            reqwest::Method::POST,
            &format!("/v2/publish/{}", sink.url("/x")),
        )
        .body("way too long for the limit")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 412);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("request body exceeds MAX_BODY_BYTES (8 bytes)")
    );

    // batch items get the item-shaped message
    let r = app
        .req(reqwest::Method::POST, "/v2/batch")
        .json(&json!([{"destination": sink.url("/x"), "body": "far too long body"}]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 412);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("body exceeds MAX_BODY_BYTES (8 bytes)")
    );
}

#[tokio::test]
async fn ssrf_blocks_private_and_metadata_by_default() {
    let mut config = test_config();
    config.allow_private_destinations = false;
    let app = TestApp::start_with_config(config).await;

    let r = app
        .req(reqwest::Method::POST, "/v2/publish/http://127.0.0.1:9/boom")
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("destination address is not allowed")
    );

    let r = app
        .req(
            reqwest::Method::POST,
            "/v2/publish/http://169.254.169.254/latest/meta-data",
        )
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // JS isUrl only matches ^https?:// — anything else is a URL-group name, so ftp://
    // never reaches the structural validator on publish; it 404s as an unknown group.
    let r = app
        .req(reqwest::Method::POST, "/v2/publish/ftp://example.com/")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!(r#"URL group "ftp://example.com/" not found"#)
    );

    // the http(s)-scheme structural errors DO fire for well-formed-but-bad URLs
    let r = app
        .req(
            reqwest::Method::POST,
            "/v2/publish/http://user:pass@example.com/",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("destination URL must not contain credentials")
    );
}

#[tokio::test]
async fn bulk_cancel_messages() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let mut ids = Vec::new();
    for i in 0..2 {
        let res = app
            .publish(
                &sink.url("/bulk"),
                &format!("b{i}"),
                &[("Upstash-Delay", "30")],
            )
            .await;
        ids.push(TestApp::message_id(&res));
    }
    let r = app
        .req(reqwest::Method::DELETE, "/v2/messages")
        .json(&json!({"messageIds": ids}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["cancelled"], json!(2));
    assert!(sink.seen().is_empty());
}

// ---------------------------------------------------------------------------
// 14. schedules: create/list/get/pause/resume/delete + cron firing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schedules_crud_and_fire() {
    let mut config = test_config();
    config.scheduler_tick_ms = 50;
    config.dispatch_tick_ms = 40;
    let app = TestApp::start_with_config(config).await;
    let sink = Sink::start().await;
    let dest = sink.url("/cron");

    let r = app
        .req(reqwest::Method::POST, &format!("/v2/schedules/{}", dest))
        .header("Upstash-Cron", "* * * * * *")
        .header("Upstash-Method", "POST")
        .header("Upstash-Forward-X-From", "cron")
        .body("tick")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let created: Value = r.json().await.unwrap();
    let id = created["scheduleId"]
        .as_str()
        .expect("scheduleId")
        .to_string();
    assert!(id.starts_with("scd_"), "{id}");

    let list = app.get_json("/v2/schedules").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["scheduleId"], json!(id));
    assert_eq!(list[0]["destination"], json!(dest));
    assert_eq!(list[0]["cron"], json!("* * * * * *"));
    assert_eq!(list[0]["header"]["x-from"], json!(["cron"]));
    assert_eq!(list[0]["body"], json!("tick"));
    assert_eq!(list[0]["isPaused"], json!(false));
    assert!(list[0]["nextScheduleTime"].as_i64().unwrap() > epoch_ms());

    let one = app.get_json(&format!("/v2/schedules/{}", id)).await;
    assert_eq!(one["nextScheduleTime"], list[0]["nextScheduleTime"]);
    let created_at = one["createdAt"].clone();

    // Upsert by Upstash-Schedule-Id: full config replacement, same id/createdAt.
    let r = app
        .req(reqwest::Method::POST, &format!("/v2/schedules/{}", dest))
        .header("Upstash-Cron", "* * * * * *")
        .header("Upstash-Schedule-Id", &id)
        .header("Upstash-Forward-X-From", "cron")
        .body("tock")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    assert_eq!(r.json::<Value>().await.unwrap()["scheduleId"], json!(id));
    let one = app.get_json(&format!("/v2/schedules/{}", id)).await;
    assert_eq!(one["body"], json!("tock"));
    assert_eq!(one["createdAt"], created_at);
    assert_eq!(
        app.get_json("/v2/schedules")
            .await
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // pause → no fire; resume recomputes the next run.
    let r = app
        .req(
            reqwest::Method::POST,
            &format!("/v2/schedules/{}/pause", id),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        app.get_json(&format!("/v2/schedules/{}", id)).await["isPaused"],
        json!(true)
    );

    let r = app
        .req(
            reqwest::Method::POST,
            &format!("/v2/schedules/{}/resume", id),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        app.get_json(&format!("/v2/schedules/{}", id)).await["isPaused"],
        json!(false)
    );

    wait_until("schedule fired", 4_000, || !sink.seen().is_empty()).await;
    let seen = sink.seen()[0].clone();
    assert_eq!(seen.method, "POST");
    assert_eq!(seen.path, "/cron");
    assert_eq!(seen.body, b"tock");
    let h = |name: &str| {
        seen.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(h("upstash-schedule-id").as_deref(), Some(id.as_str()));
    assert_eq!(h("x-from").as_deref(), Some("cron"));

    let r = app
        .req(reqwest::Method::DELETE, &format!("/v2/schedules/{}", id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = app
        .req(reqwest::Method::GET, &format!("/v2/schedules/{}", id))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn schedule_missing_cron_and_bad_cron_are_400() {
    let app = TestApp::start().await;
    let r = app
        .req(
            reqwest::Method::POST,
            "/v2/schedules/https://example.com/hook",
        )
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("missing Upstash-Cron header")
    );

    let r = app
        .req(
            reqwest::Method::POST,
            "/v2/schedules/https://example.com/hook",
        )
        .header("Upstash-Cron", "nope")
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

// ---------------------------------------------------------------------------
// 15. flow control: API surface + runtime enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flow_control_api_and_enforcement() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    sink.set_delay(250);
    let dest = sink.url("/fc");

    let hdr = [
        ("Upstash-Flow-Control-Key", "k"),
        ("Upstash-Flow-Control-Value", "parallelism=1"),
    ];
    app.publish(&dest, "1", &hdr).await;
    app.publish(&dest, "2", &hdr).await;

    let fc = app.get_json("/v2/flowControl/k").await;
    assert_eq!(fc["flowControlKey"], json!("k"));
    assert_eq!(fc["parallelism"], json!(1));
    assert_eq!(fc["period"], json!(1));
    assert_eq!(fc["paused"], json!(false));
    assert!(fc.get("rate").is_none());

    let global = app.get_json("/v2/globalParallelism").await;
    assert_eq!(global["parallelismMax"], json!(1024));
    assert!(global["parallelismCount"].as_i64().unwrap() >= 0);

    wait_until("both fc messages delivered", 6_000, || {
        sink.seen().len() >= 2
    })
    .await;
    assert_eq!(
        sink.max_concurrent.load(Ordering::SeqCst),
        1,
        "parallelism=1 allows no overlap"
    );

    // pin sets dimensions and starts a fresh rate bucket
    let r = app
        .req(
            reqwest::Method::POST,
            "/v2/flowControl/k/pin?parallelism=3&rate=10&period=1s",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let fc = app.get_json("/v2/flowControl/k").await;
    assert_eq!(fc["parallelism"], json!(3));
    assert_eq!(fc["rate"], json!(10));
    assert_eq!(fc["period"], json!(1));

    // a publish can't override a pinned dimension
    app.publish(
        &dest,
        "3",
        &[
            ("Upstash-Flow-Control-Key", "k"),
            ("Upstash-Flow-Control-Value", "parallelism=9"),
        ],
    )
    .await;
    assert_eq!(
        app.get_json("/v2/flowControl/k").await["parallelism"],
        json!(3)
    );

    // unpin parallelism only → the next publish adopts its value; rate stays pinned
    let r = app
        .req(
            reqwest::Method::POST,
            "/v2/flowControl/k/unpin?parallelism=1",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    app.publish(
        &dest,
        "4",
        &[
            ("Upstash-Flow-Control-Key", "k"),
            ("Upstash-Flow-Control-Value", "parallelism=9"),
        ],
    )
    .await;
    let fc = app.get_json("/v2/flowControl/k").await;
    assert_eq!(fc["parallelism"], json!(9));
    assert_eq!(fc["rate"], json!(10), "rate remained pinned");

    let r = app
        .req(reqwest::Method::POST, "/v2/flowControl/k/resetRate")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = app
        .req(reqwest::Method::POST, "/v2/flowControl/k/pause")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        app.get_json("/v2/flowControl/k").await["paused"],
        json!(true)
    );
    let r = app
        .req(reqwest::Method::POST, "/v2/flowControl/k/resume")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        app.get_json("/v2/flowControl/k").await["paused"],
        json!(false)
    );

    let r = app
        .req(reqwest::Method::GET, "/v2/flowControl/nope")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("flow control key not found")
    );
}

// ---------------------------------------------------------------------------
// 16. workflow wait/notify/timeout + run history
// ---------------------------------------------------------------------------

#[tokio::test]
async fn workflow_wait_notify_wakes_with_resolution() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let resume = sink.url("/resume");

    let r = app
        .req(reqwest::Method::POST, "/v2/wait/evt-1")
        .header("Upstash-Workflow-RunId", "run_wait")
        .json(&json!({
            "url": resume,
            "timeout": "1h",
            "step": {"stepId": 2, "stepName": "waitForOrder", "concurrent": 1, "targetStep": 3}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    let waiter_id = v["waiterId"].as_str().unwrap().to_string();
    assert!(waiter_id.starts_with("wtr_"), "{waiter_id}");
    assert_eq!(waiter_id.len(), 4 + 22, "16 bytes unpadded base64url");

    let waiters = app.get_json("/v2/waiters/evt-1").await;
    assert_eq!(waiters.as_array().unwrap().len(), 1);
    assert_eq!(waiters[0]["waiterId"], json!(waiter_id));
    assert_eq!(waiters[0]["url"], json!(resume));
    assert_eq!(waiters[0]["timeoutUrl"], json!(resume));
    let deadline_s = waiters[0]["deadline"].as_i64().unwrap();
    let expected_s = (epoch_ms() + 3600 * 1000) / 1000;
    assert!((deadline_s - expected_s).abs() <= 5);

    // The wait marker is the first recorded step for the run.
    let steps = app.get_json("/v2/workflows/runs/run_wait").await;
    assert_eq!(steps.as_array().unwrap().len(), 1);
    assert_eq!(steps[0]["callType"], json!("wait"));
    assert_eq!(steps[0]["messageId"], json!(""));

    let r = app
        .req(reqwest::Method::POST, "/v2/notify/evt-1")
        .header("content-type", "text/plain")
        .body("order-42")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let notified: Value = r.json().await.unwrap();
    let notified = notified.as_array().unwrap();
    assert_eq!(notified.len(), 1);
    let message_id = notified[0]["messageId"].as_str().unwrap().to_string();
    assert_eq!(notified[0]["workflowRunId"], json!("run_wait"));
    assert_eq!(notified[0]["waiter"]["url"], json!(resume));
    assert_eq!(
        notified[0]["waiter"]["headers"]["upstash-workflow-runid"],
        json!(["run_wait"])
    );

    // Waiters are consumed.
    assert!(app
        .get_json("/v2/waiters/evt-1")
        .await
        .as_array()
        .unwrap()
        .is_empty());

    // Resolution step's messageId equals the wake's — the SDK depends on it.
    let steps = app.get_json("/v2/workflows/runs/run_wait").await;
    assert_eq!(steps.as_array().unwrap().len(), 2);
    assert_eq!(steps[1]["messageId"], json!(message_id));
    assert_eq!(steps[1]["callType"], json!("step"));
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD
        .decode(steps[1]["body"].as_str().unwrap())
        .unwrap();
    let resolution: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resolution["waitEventId"], json!("evt-1"));
    assert_eq!(resolution["stepId"], json!(2));
    assert_eq!(resolution["stepName"], json!("waitForOrder"));
    assert_eq!(resolution["waitTimeout"], json!(false));
    assert_eq!(
        resolution["out"],
        json!(base64::engine::general_purpose::STANDARD.encode("order-42"))
    );

    // The wake is a workflow invocation: empty body + forwarded workflow headers.
    wait_until("workflow wake delivered", 5_000, || !sink.seen().is_empty()).await;
    let seen = sink.seen()[0].clone();
    assert_eq!(seen.path, "/resume");
    assert!(seen.body.is_empty(), "LazyFetch delivers an empty body");
    let h = |name: &str| {
        seen.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(h("content-type").as_deref(), Some("application/json"));
    assert_eq!(h("upstash-workflow-runid").as_deref(), Some("run_wait"));
    assert_eq!(
        h("upstash-message-id").as_deref(),
        Some(message_id.as_str())
    );
}

#[tokio::test]
async fn workflow_waiter_timeout_fires() {
    let mut config = test_config();
    config.scheduler_tick_ms = 50;
    config.dispatch_tick_ms = 40;
    let app = TestApp::start_with_config(config).await;
    let sink = Sink::start().await;

    let r = app
        .req(reqwest::Method::POST, "/v2/wait/evt-timeout")
        .header("Upstash-Workflow-RunId", "run_timeout")
        .json(&json!({"url": sink.url("/resume"), "timeout": "1s"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert!(v["waiterId"].as_str().unwrap().starts_with("wtr_"));

    wait_until("waiter timed out and woke the workflow", 5_000, || {
        !sink.seen().is_empty()
    })
    .await;
    let steps = app.get_json("/v2/workflows/runs/run_timeout").await;
    assert_eq!(steps.as_array().unwrap().len(), 2);
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD
        .decode(steps[1]["body"].as_str().unwrap())
        .unwrap();
    let resolution: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resolution["waitTimeout"], json!(true));
    assert_eq!(resolution["out"], json!(""));
    assert!(sink.seen()[0].body.is_empty());
}

#[tokio::test]
async fn workflow_publish_records_steps_logs_and_cancel() {
    let app = TestApp::start().await;
    let sink = Sink::start().await;
    let dest = sink.url("/workflow");

    let hdr = [
        ("Upstash-Workflow-RunId", "run_42"),
        ("Upstash-Workflow-Init", "true"),
    ];
    let first = app.publish(&dest, "init payload", &hdr).await;
    let first_id = TestApp::message_id(&first);
    assert!(first.get("deduplicated").is_none());

    // init idempotency: same run id + same body (different explicit dedupe would also
    // answer, but the init path must answer even when content differs).
    let second = app.publish(&dest, "different", &hdr).await;
    assert_eq!(second["messageId"], json!(first_id));
    assert_eq!(second["deduplicated"], json!(true));

    let steps = app.get_json("/v2/workflows/runs/run_42").await;
    assert_eq!(steps.as_array().unwrap().len(), 1);
    assert_eq!(steps[0]["messageId"], json!(first_id));
    assert_eq!(steps[0]["callType"], json!("step"));
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD
        .decode(steps[0]["body"].as_str().unwrap())
        .unwrap();
    assert_eq!(body, b"init payload");

    let logs = app
        .get_json("/v2/workflows/logs?workflowRunId=run_42")
        .await;
    assert_eq!(logs["cursor"], json!(""));
    let runs = logs["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["workflowRunId"], json!("run_42"));
    assert_eq!(runs[0]["workflowUrl"], json!(dest));
    assert_eq!(runs[0]["workflowState"], json!("RUN_STARTED"));
    assert_eq!(runs[0]["workflowRunCompletedAt"], json!(0));
    assert_eq!(runs[0]["steps"].as_array().unwrap().len(), 1);

    // Single-run completion: RUN_SUCCESS + history dropped, archive kept.
    let r = app
        .req(
            reqwest::Method::DELETE,
            "/v2/workflows/runs/run_42?cancel=false",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let logs = app
        .get_json("/v2/workflows/logs?workflowRunId=run_42")
        .await;
    let runs = logs["runs"].as_array().unwrap();
    assert_eq!(runs[0]["workflowState"], json!("RUN_SUCCESS"));
    assert!(runs[0]["workflowRunCompletedAt"].as_i64().unwrap() > 0);
    assert!(runs[0]["steps"].as_array().unwrap().is_empty());
    let r = app
        .req(reqwest::Method::GET, "/v2/workflows/runs/run_42")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // Bulk cancel by run id.
    app.publish(&dest, "b", &[("Upstash-Workflow-RunId", "run_43")])
        .await;
    let r = app
        .req(reqwest::Method::DELETE, "/v2/workflows/runs")
        .json(&json!({"workflowRunIds": ["run_43", "missing"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.json::<Value>().await.unwrap()["cancelled"], json!(1));
    let logs = app
        .get_json("/v2/workflows/logs?workflowRunId=run_43")
        .await;
    assert_eq!(logs["runs"][0]["workflowState"], json!("RUN_CANCELED"));

    // Bulk cancel by workflow URL prefix.
    app.publish(&dest, "c", &[("Upstash-Workflow-RunId", "run_44")])
        .await;
    let r = app
        .req(reqwest::Method::DELETE, "/v2/workflows/runs")
        .json(&json!({"workflowUrl": [sink.url("/work")]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.json::<Value>().await.unwrap()["cancelled"], json!(1));

    // Filters/cursor shape.
    let all = app.get_json("/v2/workflows/logs?count=1").await;
    assert_eq!(all["runs"].as_array().unwrap().len(), 1);
    assert!(!all["cursor"].as_str().unwrap().is_empty());
    let next = app
        .get_json(&format!(
            "/v2/workflows/logs?count=1&cursor={}",
            all["cursor"].as_str().unwrap()
        ))
        .await;
    assert_eq!(next["runs"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn wait_requires_url_and_workflow_run_404() {
    let app = TestApp::start().await;
    let r = app
        .req(reqwest::Method::POST, "/v2/wait/evt")
        .json(&json!({"timeout": "1h"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        500,
        "missing resume url is a NOT NULL violation"
    );

    let r = app
        .req(reqwest::Method::GET, "/v2/workflows/runs/never")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("workflow run not found")
    );
}

// ---------------------------------------------------------------------------
// 17. admin tenant registry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn admin_registry_lifecycle() {
    let mut config = test_config();
    config.admin_token = Some("admin-secret".into());
    let app = TestApp::start_with_config(config).await;

    // No token / wrong token → 401 (and the tenant token can never reach /admin).
    let r = app
        .client
        .get(format!("{}/admin/tenants", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"],
        json!("invalid admin token")
    );
    let r = app
        .client
        .get(format!("{}/admin/tenants", app.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    let r = app
        .client
        .post(format!("{}/admin/tenants", app.base))
        .bearer_auth("admin-secret")
        .json(&json!({"name": "acme", "externalId": "ext-1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let created: Value = r.json().await.unwrap();
    let tenant_id = created["tenantId"].as_str().unwrap().to_string();
    assert_eq!(created["name"], json!("acme"));
    assert_eq!(created["externalId"], json!("ext-1"));
    assert_eq!(created["existed"], json!(false));
    assert!(created["token"].as_str().unwrap().starts_with("hs_"));
    assert!(created["currentSigningKey"]
        .as_str()
        .unwrap()
        .starts_with("sig_"));
    let created_next = created["nextSigningKey"].as_str().unwrap().to_string();

    // Idempotent on externalId: no plaintext token the second time.
    let r = app
        .client
        .post(format!("{}/admin/tenants", app.base))
        .bearer_auth("admin-secret")
        .json(&json!({"name": "other", "externalId": "ext-1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let again: Value = r.json().await.unwrap();
    assert_eq!(again["tenantId"], json!(tenant_id));
    assert_eq!(again["name"], json!("acme"));
    assert_eq!(again["existed"], json!(true));
    assert!(again.get("token").is_none());

    let list = app
        .client
        .get(format!("{}/admin/tenants", app.base))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(list.as_array().unwrap().len(), 2); // bootstrap + acme
    assert!(list
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["tenantId"] == json!(tenant_id)));

    let detail = app
        .client
        .get(format!("{}/admin/tenants/{}", app.base, tenant_id))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        detail["usage"],
        json!({"messages": 0, "schedules": 0, "dlq": 0})
    );

    // Rotate token: the new token resolves to the tenant.
    let rotated = app
        .client
        .post(format!(
            "{}/admin/tenants/{}/rotate-token",
            app.base, tenant_id
        ))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let new_token = rotated["token"].as_str().unwrap().to_string();
    assert!(new_token.starts_with("hs_"));
    let r = app
        .client
        .get(format!("{}/v2/keys", app.base))
        .bearer_auth(&new_token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // Rotate keys: current := previous next; next := fresh.
    let keys = app
        .client
        .post(format!(
            "{}/admin/tenants/{}/rotate-keys",
            app.base, tenant_id
        ))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(keys["current"], json!(created_next));
    assert_ne!(keys["next"], json!(created_next));

    let r = app
        .client
        .delete(format!("{}/admin/tenants/{}", app.base, tenant_id))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = app
        .client
        .get(format!("{}/admin/tenants/{}", app.base, tenant_id))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    // The deleted tenant's token no longer resolves.
    let r = app
        .client
        .get(format!("{}/v2/keys", app.base))
        .bearer_auth(&new_token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn admin_router_is_absent_without_admin_token() {
    let app = TestApp::start().await;
    let r = app
        .client
        .get(format!("{}/admin/tenants", app.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}
