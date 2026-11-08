//! Duration grammar ported from `core/duration.js`: concatenated `<number><unit>`
//! segments with unit ∈ {s,m,h,d}; a bare integer is seconds; total floored.

#[derive(Debug, PartialEq, Eq)]
pub struct ValidationError(pub String);

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type VResult<T> = Result<T, ValidationError>;

pub fn invalid_duration(field: &str, input: &str) -> ValidationError {
    ValidationError(format!("invalid {}: \"{}\"", field, input))
}

/// Whole seconds for the QStash duration grammar. `60` → 60, `5s` → 5, `1d1h30m` →
/// 91800 (floored). Mirrors `parseDurationSecs` error strings exactly.
pub fn parse_duration_secs(input: &str, field: &str) -> VResult<i64> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(ValidationError(format!("invalid {}: empty", field)));
    }
    if !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()) {
        return raw
            .parse::<i64>()
            .map_err(|_| invalid_duration(field, input));
    }
    let mut total = 0f64;
    let mut i = 0usize;
    let bytes = raw.as_bytes();
    while i < bytes.len() {
        // (\d+(?:\.\d+)?)([smhd]) at position i, anchored (sticky regex in JS)
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j == i {
            return Err(invalid_duration(field, input));
        }
        let mut k = j;
        if k < bytes.len() && bytes[k] == b'.' {
            let mut frac = k + 1;
            while frac < bytes.len() && bytes[frac].is_ascii_digit() {
                frac += 1;
            }
            if frac == k + 1 {
                return Err(invalid_duration(field, input));
            }
            k = frac;
        }
        if k >= bytes.len() {
            return Err(invalid_duration(field, input));
        }
        let unit_secs: f64 = match bytes[k] {
            b's' => 1.0,
            b'm' => 60.0,
            b'h' => 3600.0,
            b'd' => 86_400.0,
            _ => return Err(invalid_duration(field, input)),
        };
        let num: f64 = raw[i..k]
            .parse()
            .map_err(|_| invalid_duration(field, input))?;
        total += num * unit_secs;
        i = k + 1;
    }
    Ok(total.floor() as i64)
}

pub fn parse_duration_ms(input: &str, field: &str) -> VResult<i64> {
    parse_duration_secs(input, field).map(|s| s * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_number_is_seconds() {
        assert_eq!(parse_duration_secs("60", "Upstash-Delay").unwrap(), 60);
    }

    #[test]
    fn single_and_compound_units() {
        assert_eq!(parse_duration_secs("5s", "d").unwrap(), 5);
        assert_eq!(parse_duration_secs("2m", "d").unwrap(), 120);
        assert_eq!(parse_duration_secs("1h", "d").unwrap(), 3600);
        assert_eq!(parse_duration_secs("1d1h30m", "d").unwrap(), 91_800);
        assert_eq!(parse_duration_secs("1d1h1m", "d").unwrap(), 90_060);
        assert_eq!(parse_duration_secs("1.5s", "d").unwrap(), 1);
    }

    #[test]
    fn rejects_bad_grammar_with_exact_message() {
        assert_eq!(
            parse_duration_secs("", "Upstash-Delay").unwrap_err().0,
            "invalid Upstash-Delay: empty"
        );
        assert_eq!(
            parse_duration_secs("-5", "Upstash-Delay").unwrap_err().0,
            "invalid Upstash-Delay: \"-5\""
        );
        assert_eq!(
            parse_duration_secs("5x", "Upstash-Timeout").unwrap_err().0,
            "invalid Upstash-Timeout: \"5x\""
        );
    }
}
