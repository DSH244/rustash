//! ID minting mirroring `core/ids.js`: ULID-based message/schedule/DLQ ids, base64url
//! signing keys and tokens.

use base64::Engine;
use rand::RngCore;

/// 26-char Crockford-base32 ULID (uppercase).
pub fn ulid_str() -> String {
    ulid::Ulid::new().to_string()
}

pub fn new_message_id() -> String {
    format!("msg_{}", ulid_str())
}

pub fn new_schedule_id() -> String {
    format!("scd_{}", ulid_str())
}

pub fn new_dlq_id() -> String {
    format!("dlq_{}", ulid_str())
}

/// Workflow waiter id: `wtr_` + 16 random bytes as unpadded base64url (Node's
/// `randomBytes(16).toString("base64url")`).
pub fn new_waiter_id() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    format!(
        "wtr_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
    )
}

/// `Buffer.from(n).toString("base64url")` style: base64url WITH padding (parity with the
/// JS ids module).
pub fn b64url_padded(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(bytes)
}

pub fn new_signing_key() -> String {
    let mut buf = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("sig_{}", b64url_padded(&buf))
}

pub fn new_tenant_token() -> String {
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("hs_{}", b64url_padded(&buf))
}

pub fn new_jti() -> String {
    let mut buf = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("jwt_{}", b64url_padded(&buf))
}

pub fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Token storage hash (SHA-256 hex, never plaintext).
pub fn hash_token(token: &str) -> String {
    sha256_hex(token.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ids_are_prefixed_ulids() {
        let id = new_message_id();
        assert!(id.starts_with("msg_"), "{id}");
        assert_eq!(id.len(), 4 + 26);
        assert_ne!(id, new_message_id());
    }

    #[test]
    fn token_hash_is_stable_hex_sha256() {
        // echo -n "token" | sha256sum (verified against node crypto)
        assert_eq!(
            hash_token("token"),
            "3c469e9d6c5875d37a43f353d4f88e61fcf812c66eee3457465a40b0da4153e0"
        );
    }
}
