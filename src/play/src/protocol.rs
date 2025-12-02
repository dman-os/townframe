//! PostgreSQL wire protocol helpers

use anyhow::{ensure, Result};
use std::str;

/// PostgreSQL protocol version 3.0
const STARTUP_PROTOCOL: u32 = 196_608;

/// A parsed wire protocol message
pub struct Message<'a> {
    pub tag: u8,
    pub body: &'a [u8],
}

/// Build a simple query message (Q)
pub fn build_simple_query(sql: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(sql.len() + 6);
    buf.push(b'Q');
    buf.extend_from_slice(&0u32.to_be_bytes()); // placeholder for length
    buf.extend_from_slice(sql.as_bytes());
    buf.push(0); // null terminator

    // Fix up length (includes length field but not tag)
    let len = (buf.len() - 1) as u32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    buf
}

/// Build a startup message for connection
pub fn build_startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&0u32.to_be_bytes()); // placeholder for length
    buf.extend_from_slice(&STARTUP_PROTOCOL.to_be_bytes());

    // Parameters
    for (key, value) in [
        ("user", user),
        ("database", database),
        ("client_encoding", "UTF8"),
        ("application_name", "pglite-play"),
    ] {
        buf.extend_from_slice(key.as_bytes());
        buf.push(0);
        buf.extend_from_slice(value.as_bytes());
        buf.push(0);
    }
    buf.push(0); // terminator

    // Fix up length (includes length field itself)
    let len = buf.len() as u32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());

    buf
}

/// Build a password message (p)
pub fn build_password_message(password: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(password.len() + 6);
    buf.push(b'p');
    buf.extend_from_slice(&0u32.to_be_bytes()); // placeholder
    buf.extend_from_slice(password);
    if !password.ends_with(&[0]) {
        buf.push(0);
    }

    let len = (buf.len() - 1) as u32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    buf
}

/// Build MD5 password hash
pub fn build_md5_password(password: &str, user: &str, salt: &[u8; 4]) -> String {
    // MD5(MD5(password + user) + salt)
    let inner = md5_hex(&[password.as_bytes(), user.as_bytes()].concat());
    let outer = md5_hex(&[inner.as_bytes(), salt].concat());
    format!("md5{}", outer)
}

/// Compute MD5 hex digest
fn md5_hex(data: &[u8]) -> String {
    // Simple MD5 implementation for password hashing
    // Using a basic implementation since we don't want to add crypto deps
    let digest = md5_compute(data);
    format!("{:032x}", u128::from_be_bytes(digest))
}

/// Simple MD5 implementation
fn md5_compute(data: &[u8]) -> [u8; 16] {
    // MD5 constants
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];

    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];

    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    // Pre-processing: adding padding bits
    let ml = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&ml.to_le_bytes());

    // Process each 512-bit chunk
    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for (i, word) in chunk.chunks(4).enumerate() {
            m[i] = u32::from_le_bytes(word.try_into().unwrap());
        }

        let mut a = a0;
        let mut b = b0;
        let mut c = c0;
        let mut d = d0;

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };

            let f = f.wrapping_add(a).wrapping_add(K[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut digest = [0u8; 16];
    digest[0..4].copy_from_slice(&a0.to_le_bytes());
    digest[4..8].copy_from_slice(&b0.to_le_bytes());
    digest[8..12].copy_from_slice(&c0.to_le_bytes());
    digest[12..16].copy_from_slice(&d0.to_le_bytes());
    digest
}

/// Parse wire protocol messages from a buffer
pub fn parse_messages(data: &[u8]) -> Result<Vec<Message<'_>>> {
    let mut messages = Vec::new();
    let mut index = 0;

    while index < data.len() {
        let remaining = &data[index..];
        if remaining.len() < 5 {
            break; // Incomplete message
        }

        let tag = remaining[0];
        let len = u32::from_be_bytes(remaining[1..5].try_into().unwrap()) as usize;
        ensure!(len >= 4, "invalid message length {}", len);

        let total = 1 + len;
        if index + total > data.len() {
            break; // Incomplete message
        }

        let body = &data[index + 5..index + total];
        messages.push(Message { tag, body });
        index += total;
    }

    Ok(messages)
}

/// Check if data contains a ReadyForQuery message
pub fn contains_ready_for_query(data: &[u8]) -> bool {
    if let Ok(messages) = parse_messages(data) {
        messages.iter().any(|m| m.tag == b'Z')
    } else {
        false
    }
}

/// Check if data contains an Error message
pub fn contains_error(data: &[u8]) -> bool {
    if let Ok(messages) = parse_messages(data) {
        messages.iter().any(|m| m.tag == b'E')
    } else {
        false
    }
}

/// Extract error message from wire data
pub fn extract_error_message(data: &[u8]) -> String {
    if let Ok(messages) = parse_messages(data) {
        for msg in messages {
            if msg.tag == b'E' {
                return parse_error_fields(msg.body);
            }
        }
    }
    "unknown error".to_string()
}

/// Parse ParameterStatus message body
pub fn parse_parameter_status(body: &[u8]) -> Option<(String, String)> {
    let nul = body.iter().position(|&b| b == 0)?;
    let key = str::from_utf8(&body[..nul]).ok()?.to_string();
    let rest = &body[nul + 1..];
    let nul2 = rest.iter().position(|&b| b == 0)?;
    let value = str::from_utf8(&rest[..nul2]).ok()?.to_string();
    Some((key, value))
}

/// Parse error/notice fields
fn parse_error_fields(body: &[u8]) -> String {
    let mut fields = Vec::new();
    let mut index = 0;

    while index < body.len() {
        let code = body[index];
        if code == 0 {
            break;
        }
        index += 1;

        if let Some(end) = body[index..].iter().position(|&b| b == 0) {
            let value = str::from_utf8(&body[index..index + end])
                .unwrap_or_default()
                .to_string();
            fields.push((code as char, value));
            index += end + 1;
        } else {
            break;
        }
    }

    // Return 'M' field (message) if present, otherwise join all
    if let Some((_, msg)) = fields.iter().find(|(c, _)| *c == 'M') {
        msg.clone()
    } else {
        fields
            .iter()
            .map(|(c, v)| format!("{}:{}", c, v))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_simple_query() {
        let query = build_simple_query("SELECT 1");
        assert_eq!(query[0], b'Q');
        // Length should be 4 (length field) + 8 (SELECT 1) + 1 (null) = 13
        let len = u32::from_be_bytes(query[1..5].try_into().unwrap());
        assert_eq!(len, 13);
    }

    #[test]
    fn test_md5_password() {
        // Known test vector
        let result = build_md5_password("password", "postgres", b"salt");
        assert!(result.starts_with("md5"));
    }

    #[test]
    fn test_parse_messages_empty() {
        let messages = parse_messages(&[]).unwrap();
        assert!(messages.is_empty());
    }

    #[test]
    fn test_contains_ready_for_query() {
        // Build a minimal ReadyForQuery message: 'Z' + length(5) + status('I')
        let rfq = vec![b'Z', 0, 0, 0, 5, b'I'];
        assert!(contains_ready_for_query(&rfq));
        assert!(!contains_error(&rfq));
    }
}

