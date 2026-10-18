//! Canonical encoding of a facet value in its native storage form.
//!
//! ADR 015 §4: deterministic CBOR of the materialized subtree, one application-private tag per
//! storage kind, map keys sorted by their encoded bytes, and no object, actor, or operation ids.
//! `Text` is its current string and `Counter` is its current sum, each still distinct from `Str`
//! and `Int`. A float is always binary64; a non-finite value cannot be constructed. The repository
//! reads this tree back out of the transaction and passes the bytes to [`super::cid_of`].

/// A finite binary64. NaN and the infinities are not representable in a signed native value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FiniteF64(f64);

impl FiniteF64 {
    pub fn new(value: f64) -> Option<Self> {
        value.is_finite().then_some(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl Eq for FiniteF64 {}

/// One materialized native value, without Automerge object identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeValue {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(FiniteF64),
    Str(String),
    Bytes(Vec<u8>),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    /// The counter's current sum, not its increments.
    Counter(i64),
    /// The text's current string. A copy does not keep the collaborative history.
    Text(String),
    List(Vec<NativeValue>),
    Map(Vec<(String, NativeValue)>),
}

/// `0x64626b00` (`dbk` plus a zero byte) plus the kind index. Not an IANA-registered tag.
const TAG_BASE: u64 = 0x6462_6b00;

const KIND_NULL: u64 = 0;
const KIND_BOOL: u64 = 1;
const KIND_INT: u64 = 2;
const KIND_UINT: u64 = 3;
const KIND_FLOAT: u64 = 4;
const KIND_STR: u64 = 5;
const KIND_BYTES: u64 = 6;
const KIND_TIMESTAMP: u64 = 7;
const KIND_COUNTER: u64 = 8;
const KIND_TEXT: u64 = 9;
const KIND_LIST: u64 = 10;
const KIND_MAP: u64 = 11;

/// The v1 canonical bytes of `value`.
pub fn encode_native(value: &NativeValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, &mut out);
    out
}

fn encode(value: &NativeValue, out: &mut Vec<u8>) {
    match value {
        NativeValue::Null => {
            tag(out, KIND_NULL);
            out.push(0xf6);
        }
        NativeValue::Bool(stated) => {
            tag(out, KIND_BOOL);
            out.push(if *stated { 0xf5 } else { 0xf4 });
        }
        NativeValue::Int(stated) => {
            tag(out, KIND_INT);
            integer(out, i128::from(*stated));
        }
        NativeValue::Uint(stated) => {
            tag(out, KIND_UINT);
            uint(out, *stated);
        }
        NativeValue::Float(stated) => {
            tag(out, KIND_FLOAT);
            out.push(0xfb);
            out.extend(stated.get().to_be_bytes());
        }
        NativeValue::Str(stated) => {
            tag(out, KIND_STR);
            text(out, stated);
        }
        NativeValue::Bytes(stated) => {
            tag(out, KIND_BYTES);
            bytes(out, stated);
        }
        NativeValue::Timestamp(stated) => {
            tag(out, KIND_TIMESTAMP);
            integer(out, i128::from(*stated));
        }
        NativeValue::Counter(stated) => {
            tag(out, KIND_COUNTER);
            integer(out, i128::from(*stated));
        }
        NativeValue::Text(stated) => {
            tag(out, KIND_TEXT);
            text(out, stated);
        }
        NativeValue::List(items) => {
            tag(out, KIND_LIST);
            head(out, 4, items.len() as u64);
            for item in items {
                encode(item, out);
            }
        }
        NativeValue::Map(entries) => {
            tag(out, KIND_MAP);
            let mut encoded: Vec<(Vec<u8>, &NativeValue)> = entries
                .iter()
                .map(|(key, value)| {
                    let mut encoded_key = Vec::new();
                    text(&mut encoded_key, key);
                    (encoded_key, value)
                })
                .collect();
            encoded.sort_by(|one, other| one.0.cmp(&other.0));
            head(out, 5, encoded.len() as u64);
            for (key, value) in encoded {
                out.extend(key);
                encode(value, out);
            }
        }
    }
}

fn tag(out: &mut Vec<u8>, kind: u64) {
    head(out, 6, TAG_BASE + kind);
}

fn integer(out: &mut Vec<u8>, value: i128) {
    if value < 0 {
        head(out, 1, (-1 - value) as u64);
    } else {
        uint(out, value as u64);
    }
}

fn uint(out: &mut Vec<u8>, value: u64) {
    head(out, 0, value);
}

fn text(out: &mut Vec<u8>, value: &str) {
    head(out, 3, value.len() as u64);
    out.extend(value.as_bytes());
}

fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    head(out, 2, value.len() as u64);
    out.extend(value);
}

fn head(out: &mut Vec<u8>, major: u8, value: u64) {
    if value < 24 {
        out.push((major << 5) | value as u8);
    } else if value <= u8::MAX as u64 {
        out.extend([(major << 5) | 24, value as u8]);
    } else if value <= u16::MAX as u64 {
        out.push((major << 5) | 25);
        out.extend((value as u16).to_be_bytes());
    } else if value <= u32::MAX as u64 {
        out.push((major << 5) | 26);
        out.extend((value as u32).to_be_bytes());
    } else {
        out.push((major << 5) | 27);
        out.extend(value.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_map_is_tagged_and_sorted_by_encoded_key_bytes() {
        let value = NativeValue::Map(vec![
            ("aa".to_owned(), NativeValue::Str("later".to_owned())),
            ("z".to_owned(), NativeValue::Int(1)),
        ]);
        let encoded = encode_native(&value);
        let expected = [
            0xda, 0x64, 0x62, 0x6b, 0x0b, 0xa2, 0x61, b'z', 0xda, 0x64, 0x62, 0x6b, 0x02, 0x01,
            0x62, b'a', b'a', 0xda, 0x64, 0x62, 0x6b, 0x05, 0x65, b'l', b'a', b't', b'e', b'r',
        ];
        assert_eq!(encoded, expected);
        let decoded: ciborium::Value = ciborium::from_reader(encoded.as_slice()).unwrap();
        let ciborium::Value::Tag(tag, _) = decoded else {
            panic!("native value is a tagged item");
        };
        assert_eq!(tag, TAG_BASE + KIND_MAP);
    }

    #[test]
    fn str_text_int_and_counter_stay_distinct() {
        let as_str = encode_native(&NativeValue::Str("x".to_owned()));
        let as_text = encode_native(&NativeValue::Text("x".to_owned()));
        let as_int = encode_native(&NativeValue::Int(1));
        let as_uint = encode_native(&NativeValue::Uint(1));
        let as_counter = encode_native(&NativeValue::Counter(1));
        assert_ne!(as_str, as_text);
        assert_ne!(as_int, as_uint);
        assert_ne!(as_int, as_counter);
    }

    #[test]
    fn a_non_finite_float_cannot_be_encoded() {
        assert!(FiniteF64::new(f64::NAN).is_none());
        assert!(FiniteF64::new(f64::INFINITY).is_none());
        let encoded = encode_native(&NativeValue::Float(FiniteF64::new(1.5).unwrap()));
        assert_eq!(encoded[5], 0xfb);
        assert_eq!(&encoded[6..], &1.5f64.to_be_bytes());
    }
}
