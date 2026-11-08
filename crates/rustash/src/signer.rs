//! Delivery signing (engine/signer.js port): the exact JWT the QStash Receiver verifies.
//! HS256 over URL-safe-base64(header).URL-safe-base64(payload), payload claims
//! `{aud:"", exp, iat, iss:"Upstash", jti, nbf, sub, body}` with
//! `body = base64url(SHA-256(raw_body_bytes))` (unpadded) and `nbf` = Go zero epoch.
//! The QStash Receiver computes `sha256Base64url` and strips padding, so a standard
//! alphabet/padded claim is rejected.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::ids::new_jti;

const NBF_ZERO: i64 = -62_135_596_800; // 0001-01-01 Go zero time: disables nbf checks

/// base64url(SHA-256(bytes)) — URL-safe, unpadded (the empty-body digest is
/// 47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU), exactly what `Receiver.verify` recomputes.
pub fn body_claim(body: Option<&[u8]>) -> String {
    let mut h = Sha256::new();
    h.update(body.unwrap_or(&[]));
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(h.finalize())
}

/// Sign one delivery. `now_ms` = wall clock (defaults to system time).
pub fn sign_delivery(signing_key: &str, url: &str, body: Option<&[u8]>, now_ms: i64) -> String {
    let now_secs = now_ms.div_euclid(1000);
    let mut claims = Map::new();
    claims.insert("aud".into(), json!(""));
    claims.insert("exp".into(), json!(now_secs + 300));
    claims.insert("iat".into(), json!(now_secs));
    claims.insert("iss".into(), json!("Upstash"));
    claims.insert("jti".into(), json!(new_jti()));
    claims.insert("nbf".into(), json!(NBF_ZERO));
    claims.insert("sub".into(), json!(url));
    claims.insert("body".into(), json!(body_claim(body)));

    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header = b64.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = b64.encode(Value::Object(claims).to_string());
    let input = format!("{}.{}", header, payload);
    let mut mac =
        Hmac::<Sha256>::new_from_slice(signing_key.as_bytes()).expect("hmac accepts any key size");
    mac.update(input.as_bytes());
    let sig = b64.encode(mac.finalize().into_bytes());
    format!("{}.{}", input, sig)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_body_hash_is_known_sha256() {
        assert_eq!(
            body_claim(None),
            "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
        );
        assert_eq!(body_claim(Some(b"")), body_claim(None));
    }

    #[test]
    fn jwt_structure_and_signature_verifies() {
        let now = 1_730_591_700_000i64;
        let jwt = sign_delivery("secret-key", "https://example.com/cb", Some(b"hi"), now);
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header: Value = serde_json::from_slice(&b64.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header["alg"], "HS256");
        assert_eq!(header["typ"], "JWT");
        let payload: Value = serde_json::from_slice(&b64.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(payload["iss"], "Upstash");
        assert_eq!(payload["sub"], "https://example.com/cb");
        assert_eq!(payload["aud"], "");
        assert_eq!(payload["iat"], 1_730_591_700u64);
        assert_eq!(payload["exp"], 1_730_592_000u64);
        assert_eq!(payload["nbf"], -62_135_596_800i64);
        assert!(payload["jti"].as_str().unwrap().starts_with("jwt_"));
        // body claim = base64url of sha256("hi")
        use sha2::Digest;
        let want = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(b"hi"));
        assert_eq!(payload["body"], want);
        // Recompute the HMAC — signature must match.
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret-key").unwrap();
        mac.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
        let want_sig = b64.encode(mac.finalize().into_bytes());
        assert_eq!(parts[2], want_sig);
    }
}
