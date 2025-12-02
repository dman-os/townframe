//! PostgreSQL wire protocol helpers

use crate::interlude::*;

use md5::{Digest, Md5};
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
        ("application_name", "wash-pglite"),
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

/// Build MD5 password hash using the md-5 crate
pub fn build_md5_password(password: &str, user: &str, salt: &[u8; 4]) -> String {
    // MD5(MD5(password + user) + salt)
    let inner = md5_hex(&[password.as_bytes(), user.as_bytes()].concat());
    let outer = md5_hex(&[inner.as_bytes(), salt].concat());
    format!("md5{}", outer)
}

/// Compute MD5 hex digest using md-5 crate
fn md5_hex(data: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(data);
    let result = hasher.finalize();
    format!("{:032x}", u128::from_be_bytes(result.into()))
}

/// Parse wire protocol messages from a buffer
pub fn parse_messages(data: &[u8]) -> Res<Vec<Message<'_>>> {
    let mut messages = Vec::new();
    let mut index = 0;

    while index < data.len() {
        let remaining = &data[index..];
        if remaining.len() < 5 {
            break; // Incomplete message
        }

        let tag = remaining[0];
        let len = u32::from_be_bytes(remaining[1..5].try_into().unwrap()) as usize;
        eyre::ensure!(len >= 4, "invalid message length {}", len);

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
