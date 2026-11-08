//! Transport-level response shaping.
//!
//! Node's keep-alive server and hyper disagree on framing, and the wire anchor pins the
//! difference: every response carries `connection`/`keep-alive`, and a `c.body(null)`
//! answer is chunked with no length. Both live here so no handler re-implements them.

use axum::body::Bytes;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

/// Node's keep-alive http server implicitly stamps EVERY response with
/// `connection: keep-alive` + `keep-alive: timeout=5` — consumers see that
/// header set consumer-side, and hyper does not emit them. Applied outermost so
/// every response (including the fallback) gets the framing.
pub(super) async fn stamp_node_framing(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    let headers = res.headers_mut();
    if !headers.contains_key("connection") {
        headers.insert(
            "connection",
            axum::http::HeaderValue::from_static("keep-alive"),
        );
    }
    if !headers.contains_key("keep-alive") {
        headers.insert(
            "keep-alive",
            axum::http::HeaderValue::from_static("timeout=5"),
        );
    }
    res
}

/// `c.body(null, status)` over a keep-alive Node server serializes as
/// `transfer-encoding: chunked` with NO content-length/content-type (zero-length
/// chunk body). Reproduced with an unknown-size stream body.
pub(super) fn empty_chunked(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(axum::body::Body::from_stream(
            futures_util::stream::empty::<Result<Bytes, std::io::Error>>(),
        ))
        .unwrap()
}
