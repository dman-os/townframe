//! Canonical, reversible UTF-8 keys for byte-valued IDs (not Go string literals).
//!
//! Unicode is retained without normalization. Backslashes and Unicode control
//! characters (`char::is_control`) are escaped, as are invalid UTF-8 bytes.
//! Controls use their UTF-8 bytes; LF, TAB, and CR use `\n`, `\t`, and `\r`,
//! and other bytes use lowercase, fixed-width `\xhh`. There are no outer quotes.
//!
//! A generated facet key is looked up literally. Decode only when recovering a
//! byte-valued ID, not when reading arbitrary facet keys. URLs percent-encode the
//! generated key itself; URL resolution only percent-decodes before key lookup.
//! Native Windows names need a separately specified byte representation; Rust's
//! unspecified `OsStr::as_encoded_bytes` encoding is not a persistence format.

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid or truncated byte-key escape at byte offset {offset}")]
    InvalidEscape { offset: usize },
    #[error("byte key is not canonically encoded")]
    NonCanonical,
}

/// Encodes an arbitrary byte ID, including NULs and path separators.
pub fn encode(bytes: &[u8]) -> String {
    let mut key = String::with_capacity(bytes.len());
    let mut remaining = bytes;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(text) => {
                push_text(&mut key, text);
                break;
            }
            Err(error) => {
                let (valid, tail) = remaining.split_at(error.valid_up_to());
                push_text(
                    &mut key,
                    std::str::from_utf8(valid).expect("validated UTF-8 prefix"),
                );
                let invalid_len = error.error_len().unwrap_or(tail.len());
                for &byte in &tail[..invalid_len] {
                    push_escape(&mut key, byte);
                }
                remaining = &tail[invalid_len..];
            }
        }
    }
    key
}

fn push_text(key: &mut String, text: &str) {
    for character in text.chars() {
        if character == '\\' || character.is_control() {
            let mut buffer = [0; 4];
            for &byte in character.encode_utf8(&mut buffer).as_bytes() {
                push_escape(key, byte);
            }
        } else {
            key.push(character);
        }
    }
}

fn push_escape(key: &mut String, byte: u8) {
    match byte {
        b'\\' => key.push_str("\\\\"),
        b'\n' => key.push_str("\\n"),
        b'\t' => key.push_str("\\t"),
        b'\r' => key.push_str("\\r"),
        _ => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            key.push_str("\\x");
            key.push(char::from(HEX[usize::from(byte >> 4)]));
            key.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
}

/// Decodes only canonical byte keys. Validation re-encodes the result once.
pub fn decode(key: &str) -> Result<Vec<u8>, DecodeError> {
    let mut bytes = Vec::with_capacity(key.len());
    let mut encoded = key.bytes().enumerate();
    while let Some((offset, byte)) = encoded.next() {
        if byte != b'\\' {
            bytes.push(byte);
            continue;
        }
        let invalid = || DecodeError::InvalidEscape { offset };
        match encoded.next().map(|(_, byte)| byte) {
            Some(b'\\') => bytes.push(b'\\'),
            Some(b'n') => bytes.push(b'\n'),
            Some(b't') => bytes.push(b'\t'),
            Some(b'r') => bytes.push(b'\r'),
            Some(b'x') => {
                let hex = |byte| match byte {
                    b'0'..=b'9' => Some(byte - b'0'),
                    b'a'..=b'f' => Some(byte - b'a' + 10),
                    _ => None,
                };
                let high = encoded
                    .next()
                    .and_then(|(_, byte)| hex(byte))
                    .ok_or_else(invalid)?;
                let low = encoded
                    .next()
                    .and_then(|(_, byte)| hex(byte))
                    .ok_or_else(invalid)?;
                bytes.push((high << 4) | low);
            }
            _ => return Err(invalid()),
        }
    }
    if encode(&bytes) != key {
        return Err(DecodeError::NonCanonical);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_single_byte_and_two_byte_id_roundtrips_canonically() {
        for first in 0..=u8::MAX {
            let single = [first];
            let key = encode(&single);
            assert_eq!(decode(&key).unwrap(), single);
            assert_eq!(encode(&decode(&key).unwrap()), key);
            for second in 0..=u8::MAX {
                let pair = [first, second];
                let key = encode(&pair);
                assert_eq!(decode(&key).unwrap(), pair);
                assert_eq!(encode(&decode(&key).unwrap()), key);
            }
        }
    }

    #[test]
    fn unicode_invalid_bytes_controls_and_literal_escapes_have_distinct_keys() {
        let cases: &[(&[u8], &str)] = &[
            (b"", ""),
            ("café 日本語 🦀".as_bytes(), "café 日本語 🦀"),
            (b"a\xffb", r"a\xffb"),
            (br"a\xffb", r"a\\xffb"),
            (b"\n\t\r\0\x1b\x7f", r"\n\t\r\x00\x1b\x7f"),
            ("\u{0085}".as_bytes(), r"\xc2\x85"),
            ("\u{200b}".as_bytes(), "\u{200b}"),
            (b"\xc3\xa9\xff\xf0\x9f\xa6\x80\xc3", "é\\xff🦀\\xc3"),
            (b"\xed\xa0\x80", r"\xed\xa0\x80"),
            (b"\xf0\x9f", r"\xf0\x9f"),
            (br#""\"#, r#""\\"#),
        ];
        for &(bytes, expected) in cases {
            assert_eq!(encode(bytes), expected);
            assert_eq!(decode(expected).unwrap(), bytes);
        }
        assert_ne!(encode(&[0xff]), encode(br"\xff"));
        assert_ne!(encode("é".as_bytes()), encode("e\u{0301}".as_bytes()));
    }

    #[test]
    fn nul_separators_percent_and_dot_components_are_not_path_validated() {
        let bytes = b"../part/\0\\next%FF";
        let expected = r"../part/\x00\\next%FF";
        assert_eq!(encode(bytes), expected);
        assert_eq!(decode(expected).unwrap(), bytes);
        assert_eq!(encode(b"/"), "/");
        assert_eq!(encode(b"."), ".");
        assert_eq!(encode(b".."), "..");
    }

    #[test]
    fn malformed_escapes_report_their_byte_offset() {
        for key in [
            "\\", r"\x", r"\xf", r"\xfg", r"\xFF", r"\u00ff", r"\q", r"\0", r"\/",
        ] {
            assert_eq!(decode(key), Err(DecodeError::InvalidEscape { offset: 0 }));
            let prefixed = format!("é{key}");
            assert_eq!(
                decode(&prefixed),
                Err(DecodeError::InvalidEscape { offset: 2 })
            );
        }
    }

    #[test]
    fn alternate_or_nonminimal_spellings_are_rejected() {
        for key in [
            r"\x61",
            r"\x2f",
            r"\x5c",
            r"\x0a",
            r"\x09",
            r"\x0d",
            r"\xc3\xa9",
            "\0",
            "\n",
            "\u{0085}",
        ] {
            assert_eq!(decode(key), Err(DecodeError::NonCanonical), "{key:?}");
        }
        assert_eq!(decode(r"\xff0").unwrap(), [0xff, b'0']);
    }

    #[test]
    fn url_query_roundtrip_preserves_the_literal_key_for_lookup() {
        let key = encode(b"\xff/%FF+\0");
        let mut url = url::Url::parse("https://example.test/").unwrap();
        url.query_pairs_mut().append_pair("facet", &key);
        assert_eq!(
            url.as_str(),
            "https://example.test/?facet=%5Cxff%2F%25FF%2B%5Cx00"
        );
        assert_eq!(
            url.query_pairs().into_owned().collect::<Vec<_>>(),
            vec![("facet".to_owned(), key)]
        );
    }

    #[test]
    fn url_path_component_percent_encodes_the_key_not_the_original_bytes() {
        let escaped = encode(&[0xff]);
        assert_eq!(escaped, r"\xff");
        let mut url = url::Url::parse("https://example.test/facets/").unwrap();
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .push(&escaped);
        assert_eq!(url.as_str(), "https://example.test/facets/%5Cxff");
        assert_eq!(url.path_segments().unwrap().next_back(), Some("%5Cxff"));

        let literal = encode(br"\xff");
        let mut literal_url = url::Url::parse("https://example.test/facets/").unwrap();
        literal_url
            .path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .push(&literal);
        assert_eq!(
            literal_url.as_str(),
            "https://example.test/facets/%5C%5Cxff"
        );
    }
}
