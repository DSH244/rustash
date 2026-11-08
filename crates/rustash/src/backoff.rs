//! Retry backoff: the QStash default curve plus endpoint-supplied retry timing
//! (`Retry-After`, `X-RateLimit-Reset*`) and the non-retryable marker, ported from
//! `core/backoff.js` and the fork `engine/dispatcher.js`.

use crate::duration::parse_duration_secs;
use crate::model::MessageRow;

const MAX_BACKOFF_SECS: f64 = 86_400.0;
pub const MAX_RETRY_DELAY_MS: i64 = 86_400_000;

/// `delay_ms = round(min(86400, e^(2.5·retried)) * 1000)` evaluated with the
/// PRE-increment `retried` (matches `defaultBackoffMs` bit-for-bit:
/// 0→1000ms, 1→12182ms, 2→148414ms, 3→1807909ms, 4→22026466ms, 5+→86400000ms).
pub fn default_backoff_ms(retried: i64) -> i64 {
    let secs = MAX_BACKOFF_SECS.min((2.5 * retried as f64).exp());
    (secs * 1000.0).round() as i64
}

/// Case-insensitive first value of a stored header map (name -> [values]).
pub fn first_header(headers: &serde_json::Value, name: &str) -> Option<String> {
    let wanted = name.to_ascii_lowercase();
    let map = headers.as_object()?;
    for (key, values) in map {
        if key.to_ascii_lowercase() == wanted {
            if let Some(arr) = values.as_array() {
                if let Some(v) = arr.first().and_then(|v| v.as_str()) {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}

/// Parse one QStash retry-timing value: bare seconds (`37`), a duration (`6m5s`), or an
/// absolute date. Unparseable → None (fall through to the configured curve).
pub fn parse_retry_timing(value: &str, now_ms: i64) -> Option<i64> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    if !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()) {
        return raw.parse::<i64>().ok().map(|s| s * 1000);
    }
    // Sub-second units are checked first: `ms`/`us`/`µs`/`ns` do not appear in the
    // QStash duration grammar, so they must be handled before it consumes the number.
    if let Some(ms) = parse_subsecond_ms(raw) {
        return Some(ms);
    }
    // /^\d+(\.\d+)?[smhd]/i prefix test, then full duration grammar.
    let first = raw.as_bytes()[0];
    if first.is_ascii_digit() {
        let mut i = 0;
        let b = raw.as_bytes();
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
            i += 1;
        }
        if i < b.len() && matches!(b[i], b's' | b'm' | b'h' | b'd') {
            if let Ok(secs) = parse_duration_secs(raw, "retry timing") {
                return Some(secs * 1000);
            }
            return None;
        }
    }
    if let Ok(d) = httpdate::parse_http_date(raw) {
        let ms = d
            .duration_since(std::time::UNIX_EPOCH)
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);
        return Some((ms - now_ms).max(0));
    }
    if let Ok(d) = chrono::DateTime::parse_from_rfc3339(raw) {
        let ms = d.timestamp_millis();
        return Some((ms - now_ms).max(0));
    }
    None
}

/// `Retry-After` duration units below a second. The Retry doc lists "ns", "us" (or "µs"),
/// "ms", "s", "m", "h" for the duration form, so a sub-second value has to survive with
/// millisecond resolution instead of falling back to the default curve. Returns
/// milliseconds; `None` when the value is a whole-second form the caller handles.
fn parse_subsecond_ms(raw: &str) -> Option<i64> {
    let lower = raw.trim().to_ascii_lowercase();
    let (number, unit) = if let Some(n) = lower.strip_suffix("ms") {
        (n, 1.0_f64)
    } else if let Some(n) = lower
        .strip_suffix("us")
        .or_else(|| lower.strip_suffix("µs"))
    {
        (n, 1e-3)
    } else {
        let n = lower.strip_suffix("ns")?;
        (n, 1e-6)
    };
    let value: f64 = number.trim().parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some((value * unit).round() as i64)
}

/// The endpoint's own retry timing from the response headers QStash documents. First
/// header that parses wins; result rounded+clamped to [0, 1 day] ms.
pub fn retry_delay_from_response(
    response_headers: Option<&serde_json::Value>,
    now_ms: i64,
) -> Option<i64> {
    let headers = response_headers?;
    for name in [
        "retry-after",
        "x-ratelimit-reset",
        "x-ratelimit-reset-requests",
        "x-ratelimit-reset-tokens",
    ] {
        if let Some(v) = first_header(headers, name) {
            if let Some(parsed) = parse_retry_timing(&v, now_ms) {
                return Some(parsed.clamp(0, MAX_RETRY_DELAY_MS));
            }
        }
    }
    None
}

/// QStash: 489, or any status carrying `Upstash-NonRetryable-Error: true`, skips
/// straight to the DLQ.
pub fn is_non_retryable(status: i64, response_headers: Option<&serde_json::Value>) -> bool {
    if status == 489 {
        return true;
    }
    match response_headers.and_then(|h| first_header(h, "upstash-nonretryable-error")) {
        Some(v) => v.trim().eq_ignore_ascii_case("true"),
        None => false,
    }
}

/// Retry delay with a custom expression, falling back to the QStash default curve when
/// the expression is absent or misbehaves (division by zero on later `retried`, etc).
pub fn retry_delay_ms_for(row: &MessageRow) -> i64 {
    if let Some(expr) = &row.retry_delay_expr {
        if let Ok(ms) = crate::retry_expr::eval_retry_delay_ms(expr, row.retried) {
            return ms;
        }
    }
    default_backoff_ms(row.retried)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_curve_matches_spec_table() {
        assert_eq!(default_backoff_ms(0), 1_000);
        assert_eq!(default_backoff_ms(1), 12_182);
        assert_eq!(default_backoff_ms(2), 148_413);
        assert_eq!(default_backoff_ms(3), 1_808_042);
        assert_eq!(default_backoff_ms(4), 22_026_466);
        assert_eq!(default_backoff_ms(5), MAX_RETRY_DELAY_MS);
        assert_eq!(default_backoff_ms(50), MAX_RETRY_DELAY_MS);
    }

    #[test]
    fn retry_timing_shapes() {
        let now = 1_730_591_700_000i64;
        assert_eq!(parse_retry_timing("37", now), Some(37_000));
        assert_eq!(parse_retry_timing("6m5s", now), Some(365_000));
        assert_eq!(parse_retry_timing("2h", now), Some(7_200_000));
        // Absolute date in the past clamps to 0 (now = Sat, 02 Nov 2024 23:55:00 GMT).
        let past = "Fri, 01 Nov 2024 00:00:00 GMT";
        assert_eq!(parse_retry_timing(past, now), Some(0));
        // A future HTTP-date yields its millisecond delta.
        let future = "Sun, 03 Nov 2024 00:00:00 GMT";
        assert_eq!(parse_retry_timing(future, now), Some(300_000));
        assert_eq!(parse_retry_timing("nonsense", now), None);
        assert_eq!(parse_retry_timing("", now), None);
    }

    #[test]
    fn subsecond_retry_after_units_are_millisecond_precise() {
        let now = 1_730_591_700_000i64;
        assert_eq!(parse_retry_timing("250ms", now), Some(250));
        // `s`/`m`/`h`/`d` keep the shared duration grammar, which floors the total
        // (1.5s → 1s): the same rule every other delay field uses.
        assert_eq!(parse_retry_timing("1.5s", now), Some(1_000));
        assert_eq!(parse_retry_timing("500us", now), Some(1)); // 0.5ms rounds to 1ms
        assert_eq!(parse_retry_timing("2ms", now), Some(2));
        assert_eq!(parse_retry_timing("1000ns", now), Some(0));
        // whole-second forms keep going through the shared duration grammar
        assert_eq!(parse_retry_timing("6m5s", now), Some(365_000));
    }

    #[test]
    fn endpoint_timing_first_parsable_header_wins() {
        let h = serde_json::json!({
            "retry-after": ["abc"],
            "x-ratelimit-reset": ["15"]
        });
        assert_eq!(retry_delay_from_response(Some(&h), 0), Some(15_000));
        let h2 = serde_json::json!({"Retry-After": ["37"]});
        assert_eq!(retry_delay_from_response(Some(&h2), 0), Some(37_000));
        assert_eq!(retry_delay_from_response(None, 0), None);
    }

    #[test]
    fn non_retryable_classification() {
        assert!(is_non_retryable(489, None));
        let h = serde_json::json!({"upstash-nonretryable-error": [" TRUE "]});
        assert!(is_non_retryable(500, Some(&h)));
        let h2 = serde_json::json!({"upstash-nonretryable-error": ["false"]});
        assert!(!is_non_retryable(500, Some(&h2)));
        assert!(!is_non_retryable(401, None));
    }
}
