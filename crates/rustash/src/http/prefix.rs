//! The API version surface.
//!
//! This is the only file that names the prefixes. Every route in `router::api_routes` and
//! every path tail a handler parses goes through [`API_PREFIXES`], so an alias is added
//! here once. [`resource_tail`] is also the only path-tail parser: a resource whose v1
//! alias uses a different word (`/enqueue` vs `/queue`) passes both suffixes as data
//! instead of branching on the prefix inside the handler.

use axum::http::Uri;

/// Keep only the query pairs that address the destination. `qstash_token` and every
/// `upstash-*` key configure the publish call itself (retries, delay, timeout, flow control,
/// header-forward…), so they must not travel to the endpoint.
fn destination_query(query: &str) -> String {
    let mut kept: Vec<String> = Vec::new();
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let lower = name.to_ascii_lowercase();
        if lower == "qstash_token" || lower.starts_with("upstash-") || lower == "content-type" {
            continue;
        }
        kept.push(format!(
            "{}={}",
            percent_encoding::utf8_percent_encode(&name, QUERY_ESCAPE),
            percent_encoding::utf8_percent_encode(&value, QUERY_ESCAPE)
        ));
    }
    if kept.is_empty() {
        String::new()
    } else {
        format!("?{}", kept.join("&"))
    }
}

const QUERY_ESCAPE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn has_pct_escape(s: &str) -> bool {
    let b = s.as_bytes();
    b.windows(3)
        .any(|w| w[0] == b'%' && (w[1].is_ascii_hexdigit() && w[2].is_ascii_hexdigit()))
}

/// The version prefixes, in registration order.
const V2: &str = "/v2";
const V1: &str = "/v1";
pub(super) const API_PREFIXES: [&str; 2] = [V2, V1];

/// Resource suffixes whose v1 alias is spelled differently, declared once.
pub(super) const PUBLISH: &str = "/publish";
pub(super) const ENQUEUE: &str = "/enqueue";
pub(super) const QUEUE: &str = "/queue";
pub(super) const SCHEDULES: &str = "/schedules";

/// Whether the request arrived on the legacy v1 surface (the shape that keeps the
/// `Upstash-Destination` header).
pub(super) fn is_v1(uri: &Uri) -> bool {
    uri.path()
        .strip_prefix(V1)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// Whether a tail carries the destination's query string or is an identifier.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Tail {
    /// Append the destination's own query pairs, minus the control parameters.
    WithQuery,
    /// Path only: route parameters never carry a query string.
    PathOnly,
}

/// `extractDestination`: everything after `{prefix}{suffix}/`, percent-decoded.
///
/// `suffixes` is aligned with [`API_PREFIXES`]: index 0 is the v2 spelling, index 1 the v1
/// one. Control parameters (`qstash_token`, `upstash-*`) never travel to the destination:
/// forwarding them put the queue's own credentials in every delivery.
pub(super) fn resource_tail(
    uri: &Uri,
    suffixes: &[&str; API_PREFIXES.len()],
    tail: Tail,
) -> String {
    let path = uri.path();
    let query = match tail {
        Tail::WithQuery => uri.query().map(destination_query).unwrap_or_default(),
        Tail::PathOnly => String::new(),
    };
    for (prefix, suffix) in API_PREFIXES.iter().zip(suffixes) {
        let marker = format!("{prefix}{suffix}/");
        if let Some(stripped) = path.strip_prefix(&marker) {
            let rest = format!("{}{}", stripped, query);
            if has_pct_escape(&rest) {
                return percent_encoding::percent_decode(rest.as_bytes())
                    .decode_utf8_lossy()
                    .into_owned();
            }
            return rest;
        }
    }
    String::new()
}

/// `{prefix}/publish/` (an empty tail) does not match the `*tail` route but IS the
/// missing-destination request, so the fallback answers it as one.
pub(super) fn is_missing_publish(uri: &Uri) -> bool {
    let path = uri.path();
    API_PREFIXES
        .iter()
        .any(|p| path == format!("{p}{PUBLISH}/"))
}
