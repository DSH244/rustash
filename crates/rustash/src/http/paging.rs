//! Opaque list pagination: the base64url `<epoch-ms>:<id>` cursor shared by the DLQ and
//! the logs/events listings.

pub(super) fn decode_cursor(cursor: &str) -> Option<(i64, String)> {
    use base64::Engine;
    for engine in [
        &base64::engine::general_purpose::STANDARD,
        &base64::engine::general_purpose::URL_SAFE,
        &base64::engine::general_purpose::STANDARD_NO_PAD,
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
    ] {
        if let Ok(bytes) = engine.decode(cursor) {
            if let Ok(s) = String::from_utf8(bytes) {
                let mut it = s.splitn(2, ':');
                let ms = it.next().and_then(|x| x.parse::<i64>().ok());
                let id = it.next().unwrap_or("").to_string();
                if let Some(ms) = ms {
                    return Some((ms, id));
                }
            }
        }
    }
    None
}

pub(super) fn encode_cursor(ms: i64, id: &str) -> String {
    use base64::Engine;
    // JS side: base64url WITHOUT padding of `${ms}:${id}` (a padded URL_SAFE encode
    // would be one byte longer and shift content-length in list responses).
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{}:{}", ms, id))
}
