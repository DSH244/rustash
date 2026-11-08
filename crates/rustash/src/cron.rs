//! Cron parsing/scheduling (core/cron.js): `CRON_TZ=<IANA zone>` prefix, 5 or 6
//! whitespace-separated fields, next occurrence computed from a millisecond epoch
//! instant. Evaluated in the named IANA timezone (UTC when absent).

use std::str::FromStr;

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;

use crate::duration::ValidationError;

#[derive(Debug, Clone)]
pub struct ParsedCron {
    /// Expression without the `CRON_TZ=` prefix.
    pub expr: String,
    /// IANA timezone from the prefix; `None` means UTC.
    pub tz: Option<String>,
}

/// `parseCron`: QStash requires 5 or 6 fields and a real IANA timezone when the
/// `CRON_TZ=` prefix is used (cron-parser itself is lenient about both).
pub fn parse_cron(input: &str) -> Result<ParsedCron, ValidationError> {
    let raw = input.trim();
    let (expr, tz) = match raw.split_once(char::is_whitespace) {
        Some((first, rest)) if first.starts_with("CRON_TZ=") && !rest.trim().is_empty() => (
            rest.trim().to_string(),
            Some(first["CRON_TZ=".len()..].to_string()),
        ),
        _ => (raw.to_string(), None),
    };

    let field_count = expr.split_whitespace().count();
    if !(5..=6).contains(&field_count) {
        return Err(ValidationError(format!(
            "invalid cron expression \"{}\": expected 5 or 6 fields",
            input
        )));
    }

    if let Some(zone) = &tz {
        if Tz::from_str(zone).is_err() {
            return Err(ValidationError(format!(
                "invalid CRON_TZ timezone \"{}\"",
                zone
            )));
        }
    }

    Cron::from_str(&expr)
        .map_err(|e| ValidationError(format!("invalid cron expression \"{}\": {}", input, e)))?;

    Ok(ParsedCron { expr, tz })
}

/// `computeNextRun`: next occurrence strictly after `from_ms`, in the cron's zone.
/// The returned instant is a UTC epoch in milliseconds, the storage unit.
pub fn compute_next_run_ms(cron: &ParsedCron, from_ms: i64) -> Result<i64, ValidationError> {
    let parsed = Cron::from_str(&cron.expr)
        .map_err(|e| ValidationError(format!("invalid cron expression: {}", e)))?;
    let next_ms = match &cron.tz {
        Some(zone) => {
            let tz = Tz::from_str(zone)
                .map_err(|_| ValidationError(format!("invalid CRON_TZ timezone \"{}\"", zone)))?;
            let from = tz
                .timestamp_millis_opt(from_ms)
                .single()
                .ok_or_else(|| ValidationError("invalid cron starting instant".into()))?;
            parsed
                .find_next_occurrence(&from, false)
                .map(|d| d.timestamp_millis())
                .map_err(|e| ValidationError(format!("invalid cron expression: {}", e)))?
        }
        None => {
            let from = Utc
                .timestamp_millis_opt(from_ms)
                .single()
                .ok_or_else(|| ValidationError("invalid cron starting instant".into()))?;
            parsed
                .find_next_occurrence(&from, false)
                .map(|d| d.timestamp_millis())
                .map_err(|e| ValidationError(format!("invalid cron expression: {}", e)))?
        }
    };
    Ok(next_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_zoned_forms() {
        let p = parse_cron("0 4 * * *").unwrap();
        assert_eq!(p.expr, "0 4 * * *");
        assert!(p.tz.is_none());

        let p = parse_cron("CRON_TZ=America/New_York 0 4 * * *").unwrap();
        assert_eq!(p.expr, "0 4 * * *");
        assert_eq!(p.tz.as_deref(), Some("America/New_York"));
    }

    #[test]
    fn rejects_bad_field_count_and_timezone() {
        assert_eq!(
            parse_cron("0 4 * *").unwrap_err().0,
            "invalid cron expression \"0 4 * *\": expected 5 or 6 fields"
        );
        assert_eq!(
            parse_cron("CRON_TZ=Not/AZone 0 4 * * *").unwrap_err().0,
            "invalid CRON_TZ timezone \"Not/AZone\""
        );
    }

    #[test]
    fn next_run_is_strictly_after_from() {
        let cron = parse_cron("*/5 * * * *").unwrap();
        // 2024-01-01T00:00:00Z
        let from = 1_704_067_200_000i64;
        let next = compute_next_run_ms(&cron, from).unwrap();
        assert!(next > from);
        assert_eq!(next - from, 5 * 60 * 1000);
    }

    #[test]
    fn six_field_expression_uses_seconds() {
        let cron = parse_cron("*/30 * * * * *").unwrap();
        let from = 1_704_067_200_000i64;
        let next = compute_next_run_ms(&cron, from).unwrap();
        assert_eq!(next - from, 30_000);
    }
}
