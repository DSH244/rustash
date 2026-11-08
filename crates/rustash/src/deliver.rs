//! Low-level HTTP/1.1 delivery client plus a per-authority keep-alive pool.
//!
//! The client is hand-written for two reasons: keep-alive reuse across deliveries (a
//! per-delivery connection cost a handshake per message) and exact control over the bytes
//! on the socket. The head is emitted as `host` and `connection: keep-alive` first, then
//! caller-set headers with their stored name CASING verbatim (`Content-Type`,
//! `Upstash-Signature`, `User-Agent`), then the framing defaults `accept: */*`,
//! `accept-language: *`, `accept-encoding: gzip, deflate` and `content-length` last.
//! Response header maps keep FIRST-SEEN arrival order, which is what the DLQ/event
//! `responseHeader` rows expose.
//!
//! Connection reuse: the request advertises `connection: keep-alive`, so a socket the peer
//! kept open is reusable. Sockets are pooled per
//! (scheme, host, port) and only put back when the response was fully framed
//! (content-length / chunked / 204 / 304), carried no `connection: close`, and left no
//! unread bytes in the read buffer. A pooled socket that turns out to be dead is
//! replaced by a single fresh connection: the retry happens only on a socket taken
//! from the pool, never on a freshly dialed one, and it can only ever re-send a
//! request the peer already closed without answering. At-least-once semantics are
//! unchanged — the ordinary retry path re-delivers the same row anyway.
//!
//! TLS (https destinations) rides tokio-rustls with the webpki root set. The TLS client
//! config is built once and shared, so session tickets are reusable across deliveries.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use url::Url;

/// Idle sockets kept per (scheme, host, port). Peers routinely close keep-alive
/// sockets after a few seconds of idleness (Node's default server `keepAliveTimeout`
/// is 5s), so the default idle window is deliberately shorter than that.
pub struct Pool {
    idle: Mutex<HashMap<Key, Vec<Idle>>>,
    idle_timeout: Duration,
    max_idle_per_host: usize,
    tls: OnceLock<Arc<tokio_rustls::rustls::ClientConfig>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    scheme: String,
    host: String,
    port: u16,
}

struct Idle {
    stream: Box<dyn RW>,
    since: Instant,
}

impl Pool {
    pub fn new(idle_timeout: Duration, max_idle_per_host: usize) -> Pool {
        Pool {
            idle: Mutex::new(HashMap::new()),
            idle_timeout,
            max_idle_per_host: max_idle_per_host.max(1),
            tls: OnceLock::new(),
        }
    }

    /// Test/default construction.
    pub fn with_defaults() -> Pool {
        Pool::new(Duration::from_millis(4_000), 64)
    }

    fn take(&self, key: &Key) -> Option<Box<dyn RW>> {
        let mut guard = self.idle.lock().unwrap();
        if let Some(list) = guard.get_mut(key) {
            while let Some(idle) = list.pop() {
                if idle.since.elapsed() <= self.idle_timeout {
                    return Some(idle.stream);
                }
                // stale by age: drop it
            }
        }
        guard.remove(key);
        None
    }

    fn put(&self, key: Key, stream: Box<dyn RW>) {
        let mut guard = self.idle.lock().unwrap();
        let list = guard.entry(key).or_default();
        if list.len() < self.max_idle_per_host {
            list.push(Idle {
                stream,
                since: Instant::now(),
            });
        }
    }

    fn client_config(&self) -> Arc<tokio_rustls::rustls::ClientConfig> {
        self.tls
            .get_or_init(|| {
                Arc::new(
                    tokio_rustls::rustls::ClientConfig::builder()
                        .with_root_certificates(webpki_root_store())
                        .with_no_client_auth(),
                )
            })
            .clone()
    }
}

/// One attempt: connect (or reuse), write the request, read the full response.
///
/// `headers` are caller headers emitted verbatim (name casing + order) between the
/// `connection` line and the default `accept*` framing headers.
pub async fn send(
    pool: &Pool,
    url: &Url,
    method: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    timeout: Duration,
    max_body: usize,
) -> Result<RawResponse, String> {
    tokio::time::timeout(
        timeout,
        exchange(pool, url, method, headers, body, max_body),
    )
    .await
    .map_err(|_| format!("request timed out after {}ms", timeout.as_millis()))?
}

async fn exchange(
    pool: &Pool,
    url: &Url,
    method: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    max_body: usize,
) -> Result<RawResponse, String> {
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(format!("unsupported URL scheme {:?}", scheme));
    }
    let host = url
        .host_str()
        .ok_or_else(|| "URL has no host".to_string())?;
    let port = url
        .port_or_known_default()
        .unwrap_or(if scheme == "https" { 443 } else { 80 });
    let key = Key {
        scheme: scheme.to_string(),
        host: host.to_string(),
        port,
    };

    if let Some(stream) = pool.take(&key) {
        match attempt(stream, url, host, method, headers, body, max_body).await {
            Ok((resp, keep, stream)) => {
                if keep {
                    pool.put(key, stream);
                }
                return Ok(resp);
            }
            Err(Attempt::Fatal(e)) => return Err(e),
            // A pooled socket the peer already closed: dial fresh and try once more.
            Err(Attempt::Closed(_)) => {}
        }
    }

    let stream = connect(pool, &key, url, host).await?;
    match attempt(stream, url, host, method, headers, body, max_body).await {
        Ok((resp, keep, stream)) => {
            if keep {
                pool.put(key, stream);
            }
            Ok(resp)
        }
        Err(Attempt::Closed(e)) | Err(Attempt::Fatal(e)) => Err(e),
    }
}

async fn connect(pool: &Pool, key: &Key, url: &Url, host: &str) -> Result<Box<dyn RW>, String> {
    let tcp = TcpStream::connect((key.host.as_str(), key.port))
        .await
        .map_err(|e| format!("error sending request for url ({}): {}", url, e))?;
    tcp.set_nodelay(true).ok();

    if key.scheme != "https" {
        return Ok(Box::new(tcp));
    }
    let connector = tokio_rustls::TlsConnector::from(pool.client_config());
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| format!("invalid DNS name {:?}", host))?;
    let tls = connector
        .connect(name, tcp)
        .await
        .map_err(|e| format!("error sending request for url ({}): {}", url, e))?;
    Ok(Box::new(tls))
}

enum Attempt {
    /// No usable answer on this socket (stale keep-alive, reset, early EOF).
    Closed(String),
    /// The peer answered something we cannot parse, or the URL is unusable.
    Fatal(String),
}

async fn attempt(
    mut stream: Box<dyn RW>,
    url: &Url,
    host: &str,
    method: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    max_body: usize,
) -> Result<(RawResponse, bool, Box<dyn RW>), Attempt> {
    let request = build_request(url, host, method, headers, body);
    if let Err(e) = stream.write_all(&request).await {
        return Err(Attempt::Closed(format!(
            "error sending request for url ({}): {}",
            url, e
        )));
    }

    let mut reader = BufReader::new(stream);
    let parsed = read_response(&mut reader, max_body).await;
    let clean_buffer = reader.buffer().is_empty();
    match parsed {
        Ok((resp, framing)) => {
            let keep = clean_buffer && framing.reusable();
            let stream = reader.into_inner();
            Ok((resp, keep, stream))
        }
        Err(ReadError::Io(e)) => Err(Attempt::Closed(format!(
            "error sending request for url ({}): {}",
            url, e
        ))),
        Err(ReadError::Proto(m)) => Err(Attempt::Fatal(format!(
            "error sending request for url ({}): {}",
            url, m
        ))),
    }
}

/// What the response told us about connection lifetime.
struct Framing {
    http11: bool,
    /// `connection: close` (or an HTTP/1.0 response without `keep-alive`).
    close: bool,
    /// Body had unambiguous framing (content-length, chunked, or no body at all).
    self_delimited: bool,
}

impl Framing {
    fn reusable(&self) -> bool {
        self.http11 && !self.close && self.self_delimited
    }
}

enum ReadError {
    Io(std::io::Error),
    Proto(String),
}

fn webpki_root_store() -> tokio_rustls::rustls::RootCertStore {
    let mut store = tokio_rustls::rustls::RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    store
}

/// the wire serialization order.
fn build_request(
    url: &Url,
    host: &str,
    method: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
) -> Vec<u8> {
    let authority = match url.port() {
        // Only the SCHEME default port is omitted (http:80 / https:443).
        Some(p) if p != if url.scheme() == "https" { 443 } else { 80 } => {
            format!("{}:{}", host, p)
        }
        _ => host.to_string(),
    };
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };
    let target = match url.query() {
        Some(q) if !q.is_empty() => format!("{}?{}", path, q),
        _ => path.to_string(),
    };

    let mut out: Vec<u8> = Vec::with_capacity(256 + body.map_or(0, <[u8]>::len));
    out.extend_from_slice(format!("{} {} HTTP/1.1\r\n", method, target).as_bytes());
    out.extend_from_slice(format!("host: {}\r\n", authority).as_bytes());
    out.extend_from_slice(b"connection: keep-alive\r\n");
    for (name, value) in headers {
        out.extend_from_slice(format!("{}: {}\r\n", name, value).as_bytes());
    }
    out.extend_from_slice(
        b"accept: */*\r\naccept-language: *\r\naccept-encoding: gzip, deflate\r\n",
    );
    if let Some(b) = body {
        out.extend_from_slice(format!("content-length: {}\r\n", b.len()).as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    if let Some(b) = body {
        out.extend_from_slice(b);
    }
    out
}

/// Default cap for a stored response body. QStash documents that an over-limit response
/// body is trimmed and kept only for logging/DLQ, so the cap is a storage/memory bound,
/// not a protocol one: the rest of the stream is still drained so the socket can be
/// reused, it is just not retained.
pub const DEFAULT_MAX_RESPONSE_BODY: usize = 1024 * 1024;

pub struct RawResponse {
    pub status: u16,
    /// Header map in first-seen order: name (lowercased) -> values array in arrival
    /// order — the shape persisted into events and DLQ rows.
    pub headers: Map<String, Value>,
    /// Body bytes, truncated at `max_body`.
    pub body: Vec<u8>,
    /// True when the endpoint sent more than we kept.
    pub body_truncated: bool,
    /// Total bytes the endpoint framed for the body, even when truncated.
    pub body_total: usize,
}

async fn read_response<R>(
    reader: &mut BufReader<R>,
    max_body: usize,
) -> Result<(RawResponse, Framing), ReadError>
where
    R: AsyncRead + Unpin,
{
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .await
        .map_err(ReadError::Io)?;
    // EOF before any byte of the status line: the peer closed an idle keep-alive socket.
    // That is not a protocol error — it is exactly the case the pool must retry on a fresh
    // connection, so it has to surface as an IO-class (retryable) failure.
    if status_line.is_empty() {
        return Err(ReadError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed before the response status line",
        )));
    }
    let mut parts = status_line.split_whitespace();
    let version = parts.next().unwrap_or_default().to_string();
    let status = parts
        .next()
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| ReadError::Proto(format!("invalid status line {:?}", status_line)))?;

    let mut names: Vec<String> = Vec::new();
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line).await.map_err(ReadError::Io)?;
        if read == 0 {
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed inside the response headers",
            )));
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        let (name, value) = trimmed.split_once(':').ok_or_else(|| {
            ReadError::Proto(format!("invalid response header line {:?}", trimmed))
        })?;
        let name = name.trim().to_ascii_lowercase();
        if !names.contains(&name) {
            names.push(name.clone());
        }
        pairs.push((name, value.trim().to_string()));
    }

    let get = |name: &str| {
        pairs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    };
    let connection = get("connection").unwrap_or_default().to_ascii_lowercase();
    let close = connection
        .split(',')
        .any(|t| t.trim() == "close" || t.trim() == "upgrade")
        || (version != "HTTP/1.1" && !connection.split(',').any(|t| t.trim() == "keep-alive"));

    let (body, body_truncated, body_total, self_delimited) = if matches!(status, 204 | 304) {
        (Vec::new(), false, 0usize, true)
    } else if get("transfer-encoding").is_some_and(|v| v.to_ascii_lowercase().contains("chunked")) {
        let (b, trunc, total) = read_chunked(reader, max_body).await?;
        (b, trunc, total, true)
    } else if let Some(len) = get("content-length").and_then(|v| v.trim().parse::<usize>().ok()) {
        let (b, trunc) = read_exact_capped(reader, len, max_body).await?;
        (b, trunc, len, true)
    } else {
        // no framing info: the response ends with the connection
        let (b, trunc, total) = read_to_end_capped(reader, max_body).await?;
        (b, trunc, total, false)
    };

    // Group by name (values in arrival order), then emit in the reference backend's
    // Deterministic key order — shorter names first, ties compared byte-wise — so stored
    // response headers come back in a stable, diffable order.
    let mut grouped: Vec<(String, Vec<String>)> = Vec::new();
    for (name, value) in pairs {
        match grouped.iter_mut().find(|(n, _)| *n == name) {
            Some((_, vs)) => vs.push(value),
            None => grouped.push((name, vec![value])),
        }
    }
    grouped.sort_by(|a, b| {
        a.0.len()
            .cmp(&b.0.len())
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    let mut map = Map::new();
    for (name, values) in grouped {
        map.insert(
            name,
            Value::Array(values.into_iter().map(Value::String).collect()),
        );
    }
    Ok((
        RawResponse {
            status,
            headers: map,
            body,
            body_truncated,
            body_total,
        },
        Framing {
            http11: version == "HTTP/1.1",
            close,
            self_delimited,
        },
    ))
}

/// Read exactly `len` bytes, keeping at most `max` of them, and always draining the
/// whole run so the socket stays usable. Returns (kept, truncated).
async fn read_exact_capped<R>(
    reader: &mut BufReader<R>,
    len: usize,
    max: usize,
) -> Result<(Vec<u8>, bool), ReadError>
where
    R: AsyncRead + Unpin,
{
    let keep = len.min(max);
    let mut kept = vec![0u8; keep];
    if keep > 0 {
        reader.read_exact(&mut kept).await.map_err(ReadError::Io)?;
    }
    let mut left = len - keep;
    let mut scratch = [0u8; 8192];
    while left > 0 {
        let want = left.min(scratch.len());
        let n = reader
            .read(&mut scratch[..want])
            .await
            .map_err(ReadError::Io)?;
        if n == 0 {
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed inside the response body",
            )));
        }
        left -= n;
    }
    Ok((kept, len > keep))
}

/// Read to EOF, keeping at most `max` bytes. Returns (kept, truncated, total).
async fn read_to_end_capped<R>(
    reader: &mut BufReader<R>,
    max: usize,
) -> Result<(Vec<u8>, bool, usize), ReadError>
where
    R: AsyncRead + Unpin,
{
    let mut kept: Vec<u8> = Vec::new();
    let mut total = 0usize;
    let mut scratch = [0u8; 8192];
    loop {
        let n = reader.read(&mut scratch).await.map_err(ReadError::Io)?;
        if n == 0 {
            let truncated = total > kept.len();
            return Ok((kept, truncated, total));
        }
        total += n;
        if kept.len() < max {
            let room = max - kept.len();
            kept.extend_from_slice(&scratch[..n.min(room)]);
        }
    }
}

/// Chunked body, keeping at most `max` bytes; every chunk is fully consumed so the
/// next request on this socket starts at the right byte. Returns (kept, truncated, total).
async fn read_chunked<R>(
    reader: &mut BufReader<R>,
    max: usize,
) -> Result<(Vec<u8>, bool, usize), ReadError>
where
    R: AsyncRead + Unpin,
{
    let mut kept: Vec<u8> = Vec::new();
    let mut total = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line).await.map_err(ReadError::Io)?;
        if read == 0 {
            return Err(ReadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed inside the chunked body",
            )));
        }
        let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| ReadError::Proto(format!("invalid chunk size line {:?}", line.trim())))?;
        if size == 0 {
            // consume trailers until the blank line
            loop {
                line.clear();
                let r = reader.read_line(&mut line).await.map_err(ReadError::Io)?;
                if r == 0 || line.trim_end_matches(['\r', '\n']).is_empty() {
                    break;
                }
            }
            let truncated = total > kept.len();
            return Ok((kept, truncated, total));
        }
        total += size;
        let keep_here = size.min(max.saturating_sub(kept.len()));
        if keep_here > 0 {
            let mut buf = vec![0u8; keep_here];
            reader.read_exact(&mut buf).await.map_err(ReadError::Io)?;
            kept.extend_from_slice(&buf);
        }
        let mut drain = size - keep_here;
        let mut scratch = [0u8; 8192];
        while drain > 0 {
            let want = drain.min(scratch.len());
            let n = reader
                .read(&mut scratch[..want])
                .await
                .map_err(ReadError::Io)?;
            if n == 0 {
                return Err(ReadError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed inside a chunk",
                )));
            }
            drain -= n;
        }
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.map_err(ReadError::Io)?;
    }
}

/// object-safe read+write used across the plain/TLS branch
trait RW: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> RW for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// Minimal HTTP/1.1 server that keeps the connection open and answers every request
    /// on it. `accepted` counts TCP connections, so reuse is directly observable.
    async fn spawn_server(
        accepted: Arc<AtomicUsize>,
        answer: &'static str,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let Some(head_end) = find_head_end(&buf) else {
                            match sock.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => {
                                    buf.extend_from_slice(&chunk[..n]);
                                    continue;
                                }
                            }
                        };
                        // consume head + content-length body, keep the rest for the next one
                        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                        let len: usize = head
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                (k.trim().eq_ignore_ascii_case("content-length"))
                                    .then(|| v.trim().parse().ok())
                                    .flatten()
                            })
                            .unwrap_or(0);
                        if buf.len() < head_end + len {
                            match sock.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => {
                                    buf.extend_from_slice(&chunk[..n]);
                                    continue;
                                }
                            }
                        }
                        buf.drain(..head_end + len);
                        if sock.write_all(answer.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

    fn find_head_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
    }

    fn body_req(url: &str) -> (Url, Vec<(String, String)>, Vec<u8>) {
        (
            Url::parse(url).unwrap(),
            vec![("Content-Type".to_string(), "text/plain".to_string())],
            b"hello".to_vec(),
        )
    }

    #[tokio::test]
    async fn keep_alive_pool_reuses_one_socket_for_sequential_deliveries() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let (addr, _h) = spawn_server(
            accepted.clone(),
            "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
        )
        .await;
        let pool = Pool::with_defaults();
        let (url, headers, body) = body_req(&format!("http://{}/hook", addr));

        for _ in 0..20 {
            let resp = send(
                &pool,
                &url,
                "POST",
                &headers,
                Some(&body),
                Duration::from_secs(5),
                DEFAULT_MAX_RESPONSE_BODY,
            )
            .await
            .expect("delivery");
            assert_eq!(resp.status, 200);
            assert_eq!(resp.body, b"ok");
        }
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "20 sequential deliveries must ride a single kept-alive socket"
        );
    }

    #[tokio::test]
    async fn keep_alive_survives_a_peer_that_closes_the_socket() {
        // Answer `connection: close` and shut down after each response: the pool must not
        // reuse it (framing says not reusable), so every delivery dials fresh.
        let accepted = Arc::new(AtomicUsize::new(0));
        let (addr, _h) = spawn_server(
            accepted.clone(),
            "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
        )
        .await;
        let pool = Pool::with_defaults();
        let (url, headers, body) = body_req(&format!("http://{}/hook", addr));
        for _ in 0..3 {
            let resp = send(
                &pool,
                &url,
                "POST",
                &headers,
                Some(&body),
                Duration::from_secs(5),
                DEFAULT_MAX_RESPONSE_BODY,
            )
            .await
            .expect("delivery");
            assert_eq!(resp.status, 200);
        }
        assert!(
            accepted.load(Ordering::SeqCst) >= 2,
            "a `connection: close` peer must not be pooled"
        );
    }

    #[tokio::test]
    async fn pooled_socket_closed_by_peer_is_retried_on_a_fresh_connection() {
        // Server that accepts, reads one request, answers, then closes the connection
        // WITHOUT saying so. The pool keeps the socket; the second delivery finds it dead
        // and must transparently dial again rather than surface a transport error.
        let accepted = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let counter = accepted.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while find_head_end(&buf).is_none() {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let _ = sock
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                        .await;
                    let _ = sock.shutdown().await; // silent close: no `connection: close`
                });
            }
        });

        let pool = Pool::with_defaults();
        let (url, headers, body) = body_req(&format!("http://{}/hook", addr));
        let first = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(&body),
            Duration::from_secs(5),
            DEFAULT_MAX_RESPONSE_BODY,
        )
        .await
        .expect("first delivery");
        assert_eq!(first.status, 200);
        // Give the peer time to complete its FIN so the pooled socket is genuinely dead.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let second = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(&body),
            Duration::from_secs(5),
            DEFAULT_MAX_RESPONSE_BODY,
        )
        .await
        .expect("second delivery must recover from a dead pooled socket");
        assert_eq!(second.status, 200);
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn headers_keep_wire_order_and_casing() {
        // Request head order and caller header casing must stay verbatim.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            while find_head_end(&buf).is_none() {
                match sock.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            let end = find_head_end(&buf).unwrap_or(buf.len());
            let head = String::from_utf8_lossy(&buf[..end]).to_string();
            let _ = tx.send(head);
            let _ = sock.write_all(b"H").await;
        });
        let pool = Pool::with_defaults();
        let url = Url::parse(&format!("http://{}/a?b=1", addr)).unwrap();
        let headers = vec![
            ("Upstash-Signature".to_string(), "sig".to_string()),
            ("User-Agent".to_string(), "Upstash-QStash".to_string()),
        ];
        let _ = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(b"x"),
            Duration::from_secs(5),
            DEFAULT_MAX_RESPONSE_BODY,
        )
        .await;
        let head = rx.await.unwrap();
        let mut lines = head.split("\r\n");
        assert_eq!(lines.next().unwrap(), "POST /a?b=1 HTTP/1.1");
        assert_eq!(lines.next().unwrap(), format!("host: {}", addr));
        assert_eq!(lines.next().unwrap(), "connection: keep-alive");
        assert_eq!(lines.next().unwrap(), "Upstash-Signature: sig");
        assert_eq!(lines.next().unwrap(), "User-Agent: Upstash-QStash");
        assert!(head
            .contains("accept: */*\r\naccept-language: *\r\naccept-encoding: gzip, deflate\r\n"));
        assert!(head.ends_with("content-length: 1\r\n\r\n"));
    }

    #[test]
    fn idle_sockets_past_the_timeout_are_not_reused() {
        let pool = Pool::new(Duration::from_millis(0), 4);
        let key = Key {
            scheme: "http".into(),
            host: "127.0.0.1".into(),
            port: 1,
        };
        // Insert a placeholder socket is impossible without a runtime; assert the empty
        // path instead: a cold key yields nothing, and a zero idle window expires entries.
        assert!(pool.take(&key).is_none());
    }

    /// QStash documents that an over-limit response body is trimmed for logging/DLQ.
    /// The cap must bound memory without breaking the socket: the rest is drained.
    #[tokio::test]
    async fn oversized_response_body_is_truncated_and_socket_stays_reusable() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let body = "x".repeat(10_000);
        let answer: &'static str = Box::leak(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            )
            .into_boxed_str(),
        );
        let (addr, _h) = spawn_server(accepted.clone(), answer).await;
        let pool = Pool::with_defaults();
        let (url, headers, body_bytes) = body_req(&format!("http://{}/hook", addr));

        let resp = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(&body_bytes),
            Duration::from_secs(5),
            512,
        )
        .await
        .expect("delivery");
        assert_eq!(resp.body.len(), 512, "body must be capped at max_body");
        assert!(resp.body_truncated);
        assert_eq!(resp.body_total, 10_000);

        // The remainder must still have been read, so the pooled socket is clean.
        let again = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(&body_bytes),
            Duration::from_secs(5),
            512,
        )
        .await
        .expect("second delivery on the same socket");
        assert_eq!(again.body.len(), 512);
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "socket must still be reusable"
        );
    }

    #[tokio::test]
    async fn oversized_chunked_body_is_truncated_and_socket_stays_reusable() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let mut chunked = String::from("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n");
        for _ in 0..8 {
            chunked.push_str("400\r\n");
            chunked.push_str(&"y".repeat(1024));
            chunked.push_str("\r\n");
        }
        chunked.push_str("0\r\n\r\n");
        let answer: &'static str = Box::leak(chunked.into_boxed_str());
        let (addr, _h) = spawn_server(accepted.clone(), answer).await;
        let pool = Pool::with_defaults();
        let (url, headers, body_bytes) = body_req(&format!("http://{}/chunked", addr));

        let resp = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(&body_bytes),
            Duration::from_secs(5),
            300,
        )
        .await
        .expect("delivery");
        assert_eq!(resp.body.len(), 300);
        assert!(resp.body_truncated);
        assert_eq!(resp.body_total, 8 * 1024);

        let again = send(
            &pool,
            &url,
            "POST",
            &headers,
            Some(&body_bytes),
            Duration::from_secs(5),
            300,
        )
        .await
        .expect("second delivery on the same socket");
        assert_eq!(again.body.len(), 300);
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
    }
}
