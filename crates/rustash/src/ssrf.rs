//! SSRF gate ported from `security/ssrf.js`: CIDR policy with explicit allow/block,
//! built-in private ranges (gated by `ALLOW_PRIVATE_DESTINATIONS`) and always-blocked
//! link-local/metadata ranges. Ingest-time structural validation produces the
//! strings exactly.

use std::net::{IpAddr, ToSocketAddrs};
use url::Url;

use crate::duration::ValidationError;

#[derive(Clone, Debug)]
pub struct Cidr {
    pub base: u128,
    pub mask: u128,
    pub family: u8,
}

pub fn ip_to_u128(ip: &str) -> Option<(IpAddr, u128, u8)> {
    let addr: IpAddr = ip.parse().ok()?;
    match addr {
        IpAddr::V4(v4) => {
            let octs: Vec<u8> = v4.octets().to_vec();
            let mut value = 0u128;
            for o in octs {
                value = (value << 8) | o as u128;
            }
            Some((addr, value, 4))
        }
        IpAddr::V6(v6) => {
            // v4-mapped (::ffff:a.b.c.d) normalizes to v4, matching ipToBigInt.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ip_to_u128(&v4.to_string());
            }
            let mut value = 0u128;
            for seg in v6.segments() {
                value = (value << 16) | seg as u128;
            }
            Some((addr, value, 6))
        }
    }
}

pub fn parse_cidr(cidr: &str) -> Result<Cidr, String> {
    let (ip, prefix_raw) = match cidr.split_once('/') {
        Some((a, b)) => (a, Some(b)),
        None => (cidr, None),
    };
    let (_, value, family) = ip_to_u128(ip).ok_or_else(|| format!("invalid CIDR \"{}\"", cidr))?;
    let bits: u32 = if family == 4 { 32 } else { 128 };
    let prefix: u32 = match prefix_raw {
        None => bits,
        Some(p) => p
            .parse::<u32>()
            .map_err(|_| format!("invalid CIDR prefix \"{}\"", cidr))?,
    };
    if prefix > bits {
        return Err(format!("invalid CIDR prefix \"{}\"", cidr));
    }
    let mask: u128 = if prefix == 0 {
        0
    } else if prefix >= bits {
        if bits == 128 {
            u128::MAX
        } else {
            (1u128 << bits) - 1
        }
    } else {
        ((1u128 << prefix) - 1) << (bits - prefix)
    };
    Ok(Cidr {
        base: value & mask,
        mask,
        family,
    })
}

pub fn in_cidr(cidr: &Cidr, ip: &str) -> bool {
    match ip_to_u128(ip) {
        Some((_, value, family)) => family == cidr.family && (value & cidr.mask) == cidr.base,
        None => false,
    }
}

const PRIVATE_STR: [&str; 9] = [
    "127.0.0.0/8",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "100.64.0.0/10",
    "0.0.0.0/8",
    "::1/128",
    "::/128",
    "fc00::/7",
];
const METADATA_STR: [&str; 2] = ["169.254.0.0/16", "fe80::/10"];

#[derive(Clone, Debug)]
pub struct SsrfPolicy {
    pub allow_private: bool,
    pub allow_cidrs: Vec<Cidr>,
    pub block_cidrs: Vec<Cidr>,
}

fn parse_list(list: &str) -> Vec<Cidr> {
    list.split(',')
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .filter_map(|c| parse_cidr(c).ok())
        .collect()
}

pub fn create_policy(allow_private: bool, allow: &str, block: &str) -> SsrfPolicy {
    SsrfPolicy {
        allow_private,
        allow_cidrs: parse_list(allow),
        block_cidrs: parse_list(block),
    }
}

pub fn is_ip_allowed(policy: &SsrfPolicy, ip: &str) -> bool {
    if policy.block_cidrs.iter().any(|c| in_cidr(c, ip)) {
        return false;
    }
    if policy.allow_cidrs.iter().any(|c| in_cidr(c, ip)) {
        return true;
    }
    if METADATA_STR
        .iter()
        .any(|s| parse_cidr(s).map(|c| in_cidr(&c, ip)).unwrap_or(false))
    {
        return false;
    }
    if PRIVATE_STR
        .iter()
        .any(|s| parse_cidr(s).map(|c| in_cidr(&c, ip)).unwrap_or(false))
    {
        return policy.allow_private;
    }
    true
}

/// Ingest-time destination gate (structural).
pub fn validate_destination_url(policy: &SsrfPolicy, raw: &str) -> Result<Url, ValidationError> {
    let url = Url::parse(raw)
        .map_err(|_| ValidationError(format!("invalid destination URL \"{}\"", raw)))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(ValidationError(format!(
            "destination must be http(s), got \"{}:\"",
            url.scheme()
        )));
    }
    // Mirrors the preserved upstream typo (`url.username || url.password`): only the
    // username half is ever non-empty, so credentials are caught via username.
    if !url.username().is_empty() {
        return Err(ValidationError(
            "destination URL must not contain credentials".into(),
        ));
    }
    let hostname = url
        .host_str()
        .unwrap_or("")
        .trim_matches(|c| c == '[' || c == ']');
    let is_literal = hostname.parse::<IpAddr>().is_ok();
    if is_literal && !is_ip_allowed(policy, hostname) {
        return Err(ValidationError("destination address is not allowed".into()));
    }
    Ok(url)
}

/// Delivery-time IP check for hostname destinations: resolve and require at least one
/// allowed address (best-effort DNS-rebinding mitigation for the single-node port).
pub fn check_resolved_allowed(policy: &SsrfPolicy, url: &Url) -> Result<(), String> {
    let hostname = match url.host_str() {
        Some(h) => h.trim_matches(|c| c == '[' || c == ']'),
        None => return Err("invalid destination URL".to_string()),
    };
    if hostname.parse::<IpAddr>().is_ok() {
        if !is_ip_allowed(policy, hostname) {
            return Err("destination address is not allowed".to_string());
        }
        return Ok(());
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let addrs: Vec<IpAddr> = match (hostname, port).to_socket_addrs() {
        Ok(it) => it.map(|a| a.ip()).collect(),
        Err(_) => return Ok(()), // let reqwest surface resolution errors as network errors
    };
    if addrs.iter().any(|a| is_ip_allowed(policy, &a.to_string())) {
        Ok(())
    } else {
        Err(format!(
            "destination \"{}\" resolves to a blocked address",
            hostname
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(allow_private: bool) -> SsrfPolicy {
        create_policy(allow_private, "", "")
    }

    #[test]
    fn cidr_membership() {
        assert!(is_ip_allowed(&policy(false), "8.8.8.8"));
        assert!(!is_ip_allowed(&policy(false), "127.0.0.1"));
        assert!(is_ip_allowed(&policy(true), "127.0.0.1"));
        assert!(!is_ip_allowed(&policy(true), "169.254.169.254")); // metadata, even when private allowed
        assert!(!is_ip_allowed(&policy(false), "::1"));
        assert!(is_ip_allowed(&policy(true), "::1"));
        // v4-mapped
        assert!(!is_ip_allowed(&policy(true), "::ffff:169.254.1.1"));
    }

    #[test]
    fn blocklist_wins_over_allowlist() {
        let p = create_policy(true, "10.0.0.0/8", "10.1.2.3/32");
        assert!(is_ip_allowed(&p, "10.9.9.9"));
        assert!(!is_ip_allowed(&p, "10.1.2.3"));
    }

    #[test]
    fn structural_errors_match_js() {
        let p = policy(true);
        assert_eq!(
            validate_destination_url(&p, "not a url").unwrap_err().0,
            "invalid destination URL \"not a url\""
        );
        assert_eq!(
            validate_destination_url(&p, "ftp://example.com/x")
                .unwrap_err()
                .0,
            "destination must be http(s), got \"ftp:\""
        );
        assert_eq!(
            validate_destination_url(&p, "http://user:pw@example.com/")
                .unwrap_err()
                .0,
            "destination URL must not contain credentials"
        );
        let strict = policy(false);
        assert_eq!(
            validate_destination_url(&strict, "http://127.0.0.1:9999/x")
                .unwrap_err()
                .0,
            "destination address is not allowed"
        );
        assert!(validate_destination_url(&p, "http://127.0.0.1:9999/x").is_ok());
    }
}
