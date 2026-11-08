//! The inbound request envelope shared by the publish-shaped routes.
//!
//! Three things live here because the reference server treats them as one decision:
//!
//! * the header pairs the publish grammar sees, in fetch's `Headers.forEach` order;
//! * the body gates — the declared-length check in front of buffering, and the byte-count
//!   check the routes perform;
//! * the query string's configuration role (`upstash-*`, `content-type`) and the legacy
//!   `Upstash-Destination` carrier the v1 shapes use.
//!
//! There is deliberately ONE specification builder rather than one per route: `enqueue`
//! rejects an oversized body *before* parsing headers (the fork's `readLimitedBody` runs
//! first) while `publish` parses first, and the two 412 messages differ in wording. Both
//! facts are named here instead of being re-derived in each handler.

use axum::http::HeaderMap;

use crate::config::Config;
use crate::headers::{entries_sorted, parse_publish_headers, HeaderEntry, MessageSpec, ParseOpts};

use super::error::{ApiResult, AppError};

/// `publish` reads its configuration from the query string as well as the headers
/// (`?upstash-retries=3&upstash-delay=100s`); the other publish-shaped routes read headers
/// only. Spelled as a named constant so a call site states which shape it wants.
pub(super) const QUERY_CONFIG: bool = true;
pub(super) const HEADERS_ONLY: bool = false;

/// Inbound header entries in fetch-`Headers.forEach` order: sorted lowercased names,
/// values per name in wire order.
pub(super) fn header_entries(map: &HeaderMap) -> Vec<HeaderEntry> {
    let mut names: Vec<String> = map
        .keys()
        .map(|k| k.as_str().to_ascii_lowercase())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    let mut pairs = Vec::new();
    for name in names {
        if let Ok(cn) = axum::http::HeaderValue::from_str(&name) {
            let _ = cn;
        }
        let key = axum::http::HeaderName::from_bytes(name.as_bytes());
        let Ok(key) = key else { continue };
        for value in map.get_all(&key) {
            pairs.push((
                name.clone(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            ));
        }
    }
    pairs
}

/// readLimitedBody (fork): declared Content-Length rejected before buffering, then the
/// actual byte count.
pub(super) fn check_body_limit(headers: &HeaderMap, bytes_len: usize, max: usize) -> ApiResult<()> {
    if let Some(cl) = headers.get("content-length").and_then(|v| v.to_str().ok()) {
        if let Ok(declared) = cl.trim().parse::<usize>() {
            if declared > max {
                return Err(AppError::new(
                    412,
                    format!("request body exceeds MAX_BODY_BYTES ({} bytes)", max),
                ));
            }
        }
    }
    if bytes_len > max {
        return Err(AppError::new(
            412,
            format!("request body exceeds MAX_BODY_BYTES ({} bytes)", max),
        ));
    }
    Ok(())
}

pub(super) fn is_url(dest: &str) -> bool {
    let l = dest.to_ascii_lowercase();
    l.starts_with("http://") || l.starts_with("https://")
}

pub(super) fn parse_opts(config: &Config) -> ParseOpts {
    ParseOpts {
        default_max_retries: config.default_max_retries,
        max_retries_cap: config.max_retries,
        max_timeout_ms: config.max_http_timeout_ms,
    }
}

pub(super) fn caller_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or("").trim().to_string())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        })
}

/// QStash webhook form: publish configuration may travel as query parameters using the
/// lowercase header names (`?upstash-retries=3&upstash-delay=100s&upstash-header-forward=true`).
/// Header values win over query values for the same name, so an explicit header is never
/// silently overridden by a copied URL.
pub(super) fn merge_query_config(entries: &mut Vec<HeaderEntry>, query: Option<&str>) {
    let Some(query) = query else { return };
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let name = name.to_ascii_lowercase();
        if !name.starts_with("upstash-") && name != "content-type" {
            continue;
        }
        if entries.iter().any(|(n, _)| *n == name) {
            continue;
        }
        entries.push((name, value.into_owned()));
    }
}

/// The body as an optional payload: an empty body means "no body", not "empty body".
pub(super) fn optional_body(body: &axum::body::Bytes) -> Option<Vec<u8>> {
    if body.is_empty() {
        None
    } else {
        Some(body.to_vec())
    }
}

/// The v1 shapes carry the destination in `Upstash-Destination` instead of the path:
/// `POST /v1/publish` and `POST /v1/queue/<queueName>`.
pub(super) fn legacy_destination(headers: &HeaderMap) -> Option<String> {
    headers
        .get("upstash-destination")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

/// The one publish specification builder: the inbound header pairs, then (only when the
/// route has a query-configuration channel) the query string, then the header grammar.
pub(super) fn spec_from_headers(
    headers: &HeaderMap,
    query: Option<&str>,
    config: &Config,
    query_config: bool,
) -> ApiResult<MessageSpec> {
    let mut entries = entries_sorted(header_entries(headers));
    if query_config {
        merge_query_config(&mut entries, query);
    }
    parse_publish_headers(&entries, &parse_opts(config)).map_err(AppError::from)
}

/// The one publish specification builder for a caller that already holds header pairs
/// (`batch`, whose pairs come from JSON rather than from the request headers).
pub(super) fn spec_from_entries(
    entries: Vec<HeaderEntry>,
    config: &Config,
) -> ApiResult<MessageSpec> {
    let entries = entries_sorted(entries);
    parse_publish_headers(&entries, &parse_opts(config)).map_err(AppError::from)
}

/// Which gate runs first on a publish-shaped request.
///
/// `enqueue` rejects an oversized body before it parses headers — the fork's
/// `readLimitedBody` runs ahead of header parsing — while `publish` parses first. The order
/// is observable: a request that is both oversized and malformed is answered 412 by
/// `enqueue` and 400 by `publish`. Naming it here keeps that difference a decision instead
/// of an accident of copy-paste.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum GateOrder {
    BodyFirst,
    HeadersFirst,
}

/// The complete inbound envelope of a publish-shaped route that carries a body: the body
/// gate and the publish specification, in the order the route requires.
pub(super) fn envelope(
    headers: &HeaderMap,
    body: &axum::body::Bytes,
    query: Option<&str>,
    config: &Config,
    query_config: bool,
    order: GateOrder,
) -> ApiResult<(MessageSpec, Option<Vec<u8>>)> {
    let max = config.max_body_bytes;
    match order {
        GateOrder::BodyFirst => {
            check_body_limit(headers, body.len(), max)?;
            let spec = spec_from_headers(headers, query, config, query_config)?;
            Ok((spec, optional_body(body)))
        }
        GateOrder::HeadersFirst => {
            let spec = spec_from_headers(headers, query, config, query_config)?;
            check_body_limit(headers, body.len(), max)?;
            Ok((spec, optional_body(body)))
        }
    }
}

/// The byte-count gate for a payload a route already holds as bytes (`schedules` create).
/// Wording matches the reference: `body exceeds MAX_BODY_BYTES (N bytes)`.
pub(super) fn check_payload_limit(len: usize, max: usize) -> ApiResult<()> {
    if len > max {
        return Err(AppError::new(
            412,
            format!("body exceeds MAX_BODY_BYTES ({} bytes)", max),
        ));
    }
    Ok(())
}
