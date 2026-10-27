//! Canonical encoding of a facet value in its native storage form.
//!
//! ADR 015 §4: deterministic CBOR of the materialized subtree, one application-private tag per
//! storage kind, map keys sorted by their encoded bytes, and no object, actor, or operation ids.
//! `Text` is its current string and `Counter` is its current sum, each still distinct from `Str`
//! and `Int`. A float is always binary64 with canonical NaN bits. The repository reads this tree
//! back out of the transaction and passes the bytes to [`super::cid_of`].
// FIXME: so we get a CBOR from the automerge values here, does that mean it's not a drisl
// anymore? more hints we should drop the CID naming which has canonical meaning?

use automerge::ChangeHash;
use automerge::ObjId;
use automerge::ObjType;
use automerge::Prop;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::ScalarValueRef;
use automerge::Value;
use automerge::ValueRef;
use std::collections::BTreeMap;

/// A binary64 with canonical NaN bits and distinct signed zeros.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalF64(u64);

impl CanonicalF64 {
    pub fn new(value: f64) -> Self {
        Self(if value.is_nan() {
            0x7ff8_0000_0000_0000
        } else {
            value.to_bits()
        })
    }

    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

/// One materialized native value, without Automerge object identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeValue {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(CanonicalF64),
    Str(String),
    Bytes(Vec<u8>),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    /// The counter's current sum, not its increments.
    Counter(i64),
    /// The text's current string. A copy does not keep the collaborative history.
    Text(String),
    List(Vec<NativeValue>),
    Map(BTreeMap<String, NativeValue>),
}

#[derive(Debug, thiserror::Error)]
pub enum NativeReadError {
    #[error(transparent)]
    Automerge(#[from] automerge::AutomergeError),
    #[error("unsupported native value: {0}")]
    Unsupported(&'static str),
    #[error("native traversal resource limit exceeded: {0}")]
    ResourceLimit(&'static str),
}

#[derive(Debug, Clone)]
pub struct NativeReadBudget {
    pub nodes: usize,
    pub bytes: usize,
    pub max_depth: usize,
}

impl Default for NativeReadBudget {
    fn default() -> Self {
        Self {
            nodes: Self::MAX_NODES,
            bytes: Self::MAX_BYTES,
            max_depth: Self::MAX_DEPTH,
        }
    }
}

impl NativeReadBudget {
    const MAX_DEPTH: usize = 64;
    const MAX_NODES: usize = 16_384;
    const MAX_BYTES: usize = 16 * 1024 * 1024;

    fn visit(&mut self, depth: usize) -> Result<(), NativeReadError> {
        if depth >= self.max_depth {
            return Err(NativeReadError::ResourceLimit("depth"));
        }
        self.nodes = self
            .nodes
            .checked_sub(1)
            .ok_or(NativeReadError::ResourceLimit("nodes"))?;
        Ok(())
    }

    fn consume_bytes(&mut self, count: usize) -> Result<(), NativeReadError> {
        self.bytes = self
            .bytes
            .checked_sub(count)
            .ok_or(NativeReadError::ResourceLimit("bytes"))?;
        Ok(())
    }
}

impl NativeValue {
    pub fn read_at<D: ReadDoc + ?Sized>(
        doc: &D,
        object: &ObjId,
        prop: impl Into<Prop>,
    ) -> Result<Option<Self>, NativeReadError> {
        Self::read(
            doc,
            object,
            prop.into(),
            None,
            &mut NativeReadBudget::default(),
        )
    }

    pub fn read_at_heads<D: ReadDoc + ?Sized>(
        doc: &D,
        object: &ObjId,
        prop: impl Into<Prop>,
        heads: &[ChangeHash],
        budget: &mut NativeReadBudget,
    ) -> Result<Option<Self>, NativeReadError> {
        Self::read(doc, object, prop.into(), Some(heads), budget)
    }

    fn read<D: ReadDoc + ?Sized>(
        doc: &D,
        object: &ObjId,
        prop: Prop,
        heads: Option<&[ChangeHash]>,
        budget: &mut NativeReadBudget,
    ) -> Result<Option<Self>, NativeReadError> {
        let entry = match heads {
            Some(heads) => doc.get_at(object, prop, heads)?,
            None => doc.get(object, prop)?,
        };
        let Some((value, id)) = entry else {
            return Ok(None);
        };
        let value = match value {
            Value::Object(kind) => ValueRef::Object(kind),
            Value::Scalar(value) => {
                let bytes = match value.as_ref() {
                    ScalarValue::Str(value) => value.len(),
                    ScalarValue::Bytes(value) => value.len(),
                    _ => 0,
                };
                if bytes > budget.bytes {
                    return Err(NativeReadError::ResourceLimit("bytes"));
                }
                if budget.nodes == 0 {
                    return Err(NativeReadError::ResourceLimit("nodes"));
                }
                if budget.max_depth == 0 {
                    return Err(NativeReadError::ResourceLimit("depth"));
                }
                ValueRef::Scalar(value.into_owned().into())
            }
        };
        Self::read_value(doc, &id, value, 0, heads, budget).map(Some)
    }

    fn read_value<D: ReadDoc + ?Sized>(
        doc: &D,
        id: &ObjId,
        value: ValueRef<'_>,
        depth: usize,
        heads: Option<&[ChangeHash]>,
        budget: &mut NativeReadBudget,
    ) -> Result<Self, NativeReadError> {
        budget.visit(depth)?;
        match value {
            ValueRef::Scalar(value) => Ok(match value {
                ScalarValueRef::Null => Self::Null,
                ScalarValueRef::Boolean(value) => Self::Bool(value),
                ScalarValueRef::Int(value) => Self::Int(value),
                ScalarValueRef::Uint(value) => Self::Uint(value),
                ScalarValueRef::F64(value) => Self::Float(CanonicalF64::new(value)),
                ScalarValueRef::Counter(value) => Self::Counter(value),
                ScalarValueRef::Timestamp(value) => Self::Timestamp(value),
                ScalarValueRef::Str(value) => {
                    budget.consume_bytes(value.len())?;
                    Self::Str(value.into_owned())
                }
                ScalarValueRef::Bytes(value) => {
                    budget.consume_bytes(value.len())?;
                    Self::Bytes(value.into_owned())
                }
                ScalarValueRef::Unknown { .. } => {
                    return Err(NativeReadError::Unsupported("unknown scalar kind"));
                }
            }),
            ValueRef::Object(ObjType::Table) => Err(NativeReadError::Unsupported("table")),
            ValueRef::Object(ObjType::Map) => {
                let mut entries = BTreeMap::new();
                let members = match heads {
                    Some(heads) => doc.map_range_at(id, .., heads),
                    None => doc.map_range(id, ..),
                };
                for item in members {
                    budget.consume_bytes(item.key.len())?;
                    let value =
                        Self::read_value(doc, &item.id(), item.value, depth + 1, heads, budget)?;
                    assert!(entries.insert(item.key.into_owned(), value).is_none());
                }
                Ok(Self::Map(entries))
            }
            ValueRef::Object(ObjType::List) => {
                let mut items = Vec::new();
                let entries = match heads {
                    Some(heads) => doc.list_range_at(id, .., heads),
                    None => doc.list_range(id, ..),
                };
                for item in entries {
                    items.push(Self::read_value(
                        doc,
                        &item.id(),
                        item.value,
                        depth + 1,
                        heads,
                        budget,
                    )?);
                }
                Ok(Self::List(items))
            }
            ValueRef::Object(ObjType::Text) => {
                let marks = match heads {
                    Some(heads) => doc.marks_at(id, heads)?,
                    None => doc.marks(id)?,
                };
                if !marks.is_empty() {
                    return Err(NativeReadError::Unsupported("text marks"));
                }
                let entries = match heads {
                    Some(heads) => doc.list_range_at(id, .., heads),
                    None => doc.list_range(id, ..),
                };
                for item in entries {
                    budget.visit(depth)?;
                    let ValueRef::Scalar(ScalarValueRef::Str(value)) = item.value else {
                        return Err(NativeReadError::Unsupported("embedded text value"));
                    };
                    budget.consume_bytes(value.len())?;
                }
                Ok(Self::Text(match heads {
                    Some(heads) => doc.text_at(id, heads)?,
                    None => doc.text(id)?,
                }))
            }
        }
    }
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
    use automerge::AutoCommit;
    use automerge::Automerge;
    use automerge::ScalarValue;
    use automerge::marks::ExpandMark;
    use automerge::marks::Mark;
    use automerge::transaction::Transactable;
    use ciborium::Value as Cbor;

    #[test]
    fn historical_reads_use_one_shared_budget_and_preserve_native_kinds() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let map = tx
            .put_object(automerge::ROOT, "facet", ObjType::Map)
            .unwrap();
        tx.put(&map, "counter", ScalarValue::Counter(3.into()))
            .unwrap();
        let text = tx.put_object(&map, "text", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "old").unwrap();
        tx.commit();
        let heads = doc.get_heads();
        let expected = NativeValue::read_at(&doc, &automerge::ROOT, "facet").unwrap();
        let mut tx = doc.transaction();
        tx.increment(&map, "counter", 2).unwrap();
        tx.splice_text(&text, 0, 3, "new").unwrap();
        tx.commit();
        let mut budget = NativeReadBudget::default();
        assert_eq!(
            NativeValue::read_at_heads(&doc, &automerge::ROOT, "facet", &heads, &mut budget)
                .unwrap(),
            expected
        );
        assert_ne!(
            NativeValue::read_at(&doc, &automerge::ROOT, "facet").unwrap(),
            expected
        );
        let mut budget = NativeReadBudget {
            nodes: 1,
            ..NativeReadBudget::default()
        };
        assert_eq!(
            NativeValue::read_at_heads(&doc, &map, "counter", &heads, &mut budget).unwrap(),
            Some(NativeValue::Counter(3))
        );
        assert!(matches!(
            NativeValue::read_at_heads(&doc, &map, "counter", &heads, &mut budget),
            Err(NativeReadError::ResourceLimit("nodes"))
        ));
    }

    fn assert_fixture(value: &NativeValue, expected: &[u8]) -> Cbor {
        let encoded = encode_native(value);
        assert_eq!(encoded, expected, "{value:?}");
        let mut input = encoded.as_slice();
        let decoded = ciborium::from_reader(&mut input).unwrap();
        assert!(input.is_empty());
        decoded
    }

    #[test]
    fn scalar_kinds_and_empty_containers_have_literal_fixtures() {
        let fixtures: &[(NativeValue, &[u8], u64, Cbor)] = &[
            (
                NativeValue::Null,
                &[0xda, 0x64, 0x62, 0x6b, 0x00, 0xf6],
                0x6462_6b00,
                Cbor::Null,
            ),
            (
                NativeValue::Bool(false),
                &[0xda, 0x64, 0x62, 0x6b, 0x01, 0xf4],
                0x6462_6b01,
                Cbor::Bool(false),
            ),
            (
                NativeValue::Bool(true),
                &[0xda, 0x64, 0x62, 0x6b, 0x01, 0xf5],
                0x6462_6b01,
                Cbor::Bool(true),
            ),
            (
                NativeValue::Int(1),
                &[0xda, 0x64, 0x62, 0x6b, 0x02, 0x01],
                0x6462_6b02,
                Cbor::Integer(1.into()),
            ),
            (
                NativeValue::Int(-1),
                &[0xda, 0x64, 0x62, 0x6b, 0x02, 0x20],
                0x6462_6b02,
                Cbor::Integer((-1).into()),
            ),
            (
                NativeValue::Uint(1),
                &[0xda, 0x64, 0x62, 0x6b, 0x03, 0x01],
                0x6462_6b03,
                Cbor::Integer(1.into()),
            ),
            (
                NativeValue::Str("é".to_owned()),
                &[0xda, 0x64, 0x62, 0x6b, 0x05, 0x62, 0xc3, 0xa9],
                0x6462_6b05,
                Cbor::Text("é".to_owned()),
            ),
            (
                NativeValue::Bytes(vec![0x00, 0xff]),
                &[0xda, 0x64, 0x62, 0x6b, 0x06, 0x42, 0x00, 0xff],
                0x6462_6b06,
                Cbor::Bytes(vec![0x00, 0xff]),
            ),
            (
                NativeValue::Timestamp(-1),
                &[0xda, 0x64, 0x62, 0x6b, 0x07, 0x20],
                0x6462_6b07,
                Cbor::Integer((-1).into()),
            ),
            (
                NativeValue::Counter(1),
                &[0xda, 0x64, 0x62, 0x6b, 0x08, 0x01],
                0x6462_6b08,
                Cbor::Integer(1.into()),
            ),
            (
                NativeValue::Text("é".to_owned()),
                &[0xda, 0x64, 0x62, 0x6b, 0x09, 0x62, 0xc3, 0xa9],
                0x6462_6b09,
                Cbor::Text("é".to_owned()),
            ),
            (
                NativeValue::List(vec![]),
                &[0xda, 0x64, 0x62, 0x6b, 0x0a, 0x80],
                0x6462_6b0a,
                Cbor::Array(vec![]),
            ),
            (
                NativeValue::Map(BTreeMap::new()),
                &[0xda, 0x64, 0x62, 0x6b, 0x0b, 0xa0],
                0x6462_6b0b,
                Cbor::Map(vec![]),
            ),
            (
                NativeValue::Int(i64::MIN),
                &[
                    0xda, 0x64, 0x62, 0x6b, 0x02, 0x3b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff,
                ],
                0x6462_6b02,
                Cbor::Integer(i64::MIN.into()),
            ),
            (
                NativeValue::Uint(u64::MAX),
                &[
                    0xda, 0x64, 0x62, 0x6b, 0x03, 0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff,
                ],
                0x6462_6b03,
                Cbor::Integer(u64::MAX.into()),
            ),
        ];
        for (value, expected, tag, payload) in fixtures {
            assert_eq!(
                assert_fixture(value, expected),
                Cbor::Tag(*tag, Box::new(payload.clone()))
            );
        }
    }

    #[test]
    fn floats_preserve_binary64_encodings_including_signed_zeros_and_infinities() {
        let fixtures = [
            (
                0.0,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
            (
                -0.0,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
            (
                1.5,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
            (
                -1.5,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0xbf, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
            (
                f64::from_bits(1),
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x01,
                ],
            ),
            (
                f64::MIN_POSITIVE,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
            (
                f64::MAX,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x7f, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff,
                ],
            ),
            (
                f64::INFINITY,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x7f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
            (
                f64::NEG_INFINITY,
                [
                    0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00,
                ],
            ),
        ];
        for (value, expected) in fixtures {
            let canonical = CanonicalF64::new(value);
            assert_eq!(canonical.get().to_bits(), value.to_bits());
            assert_eq!(canonical, CanonicalF64::new(canonical.get()));
            let Cbor::Tag(tag, payload) = assert_fixture(&NativeValue::Float(canonical), &expected)
            else {
                panic!("native float must be tagged");
            };
            assert_eq!(tag, 0x6462_6b04);
            let Cbor::Float(decoded) = *payload else {
                panic!("native float must contain a CBOR float");
            };
            assert_eq!(decoded.to_bits(), value.to_bits());
        }
    }

    #[test]
    fn nan_signs_payloads_and_signaling_bits_have_one_encoding_and_equality() {
        let expected = [
            0xda, 0x64, 0x62, 0x6b, 0x04, 0xfb, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let canonical = CanonicalF64::new(f64::NAN);
        for sign in [0, 0x8000_0000_0000_0000] {
            for payload in (0..52)
                .map(|bit| 1 << bit)
                .chain([0x000f_ffff_ffff_ffff, 0x0008_1234_5678_9abc])
            {
                let value =
                    CanonicalF64::new(f64::from_bits(sign | 0x7ff0_0000_0000_0000 | payload));
                assert_eq!(value.get().to_bits(), 0x7ff8_0000_0000_0000);
                assert_eq!(value, canonical);
                assert_eq!(NativeValue::Float(value), NativeValue::Float(canonical));
                assert_eq!(CanonicalF64::new(value.get()), value);
                let Cbor::Tag(tag, payload) = assert_fixture(&NativeValue::Float(value), &expected)
                else {
                    panic!("native float must be tagged");
                };
                assert_eq!(tag, 0x6462_6b04);
                let Cbor::Float(decoded) = *payload else {
                    panic!("native float must contain a CBOR float");
                };
                assert!(decoded.is_nan());
            }
        }
    }

    #[test]
    fn float_equality_matches_canonical_bytes() {
        let values = [
            0.0,
            -0.0,
            1.5,
            -1.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            f64::from_bits(0xfff0_0000_0000_0001),
        ];
        for left in values.map(CanonicalF64::new) {
            for right in values.map(CanonicalF64::new) {
                let same_bits = left.get().to_bits() == right.get().to_bits();
                assert_eq!(left == right, same_bits);
                assert_eq!(
                    NativeValue::Float(left) == NativeValue::Float(right),
                    same_bits
                );
                assert_eq!(
                    encode_native(&NativeValue::Float(left))
                        == encode_native(&NativeValue::Float(right)),
                    same_bits
                );
            }
        }
        assert_ne!(CanonicalF64::new(-0.0), CanonicalF64::new(0.0));
    }

    #[test]
    fn a_map_is_tagged_and_sorted_by_encoded_key_bytes() {
        let entries = [
            ("aa".to_owned(), NativeValue::Str("later".to_owned())),
            ("z".to_owned(), NativeValue::Int(1)),
            ("é".to_owned(), NativeValue::Null),
            ("aaa".to_owned(), NativeValue::Bool(true)),
        ];
        let forward = NativeValue::Map(entries.iter().cloned().collect());
        let reverse = NativeValue::Map(entries.into_iter().rev().collect());
        assert_eq!(forward, reverse);
        let expected = [
            0xda, 0x64, 0x62, 0x6b, 0x0b, 0xa4, 0x61, b'z', 0xda, 0x64, 0x62, 0x6b, 0x02, 0x01,
            0x62, b'a', b'a', 0xda, 0x64, 0x62, 0x6b, 0x05, 0x65, b'l', b'a', b't', b'e', b'r',
            0x62, 0xc3, 0xa9, 0xda, 0x64, 0x62, 0x6b, 0x00, 0xf6, 0x63, b'a', b'a', b'a', 0xda,
            0x64, 0x62, 0x6b, 0x01, 0xf5,
        ];
        let decoded = Cbor::Tag(
            0x6462_6b0b,
            Box::new(Cbor::Map(vec![
                (
                    Cbor::Text("z".to_owned()),
                    Cbor::Tag(0x6462_6b02, Box::new(Cbor::Integer(1.into()))),
                ),
                (
                    Cbor::Text("aa".to_owned()),
                    Cbor::Tag(0x6462_6b05, Box::new(Cbor::Text("later".to_owned()))),
                ),
                (
                    Cbor::Text("é".to_owned()),
                    Cbor::Tag(0x6462_6b00, Box::new(Cbor::Null)),
                ),
                (
                    Cbor::Text("aaa".to_owned()),
                    Cbor::Tag(0x6462_6b01, Box::new(Cbor::Bool(true))),
                ),
            ])),
        );
        assert_eq!(assert_fixture(&forward, &expected), decoded);
        assert_eq!(assert_fixture(&reverse, &expected), decoded);
    }

    #[test]
    fn map_construction_replaces_duplicate_keys_before_encoding() {
        let mut entries = BTreeMap::new();
        assert_eq!(entries.insert("key".to_owned(), NativeValue::Int(1)), None);
        assert_eq!(
            entries.insert("key".to_owned(), NativeValue::Null),
            Some(NativeValue::Int(1))
        );
        let value = NativeValue::Map(entries);
        assert_eq!(
            value,
            NativeValue::Map(BTreeMap::from([("key".to_owned(), NativeValue::Null)]))
        );
        let expected = [
            0xda, 0x64, 0x62, 0x6b, 0x0b, 0xa1, 0x63, b'k', b'e', b'y', 0xda, 0x64, 0x62, 0x6b,
            0x00, 0xf6,
        ];
        assert_eq!(
            assert_fixture(&value, &expected),
            Cbor::Tag(
                0x6462_6b0b,
                Box::new(Cbor::Map(vec![(
                    Cbor::Text("key".to_owned()),
                    Cbor::Tag(0x6462_6b00, Box::new(Cbor::Null))
                ),]))
            )
        );
    }

    #[test]
    fn nested_containers_preserve_list_order_and_kind_tags() {
        let value = NativeValue::List(vec![
            NativeValue::Map(BTreeMap::from([("x".to_owned(), NativeValue::Null)])),
            NativeValue::Bool(true),
        ]);
        let expected = [
            0xda, 0x64, 0x62, 0x6b, 0x0a, 0x82, 0xda, 0x64, 0x62, 0x6b, 0x0b, 0xa1, 0x61, b'x',
            0xda, 0x64, 0x62, 0x6b, 0x00, 0xf6, 0xda, 0x64, 0x62, 0x6b, 0x01, 0xf5,
        ];
        assert_eq!(
            assert_fixture(&value, &expected),
            Cbor::Tag(
                0x6462_6b0a,
                Box::new(Cbor::Array(vec![
                    Cbor::Tag(
                        0x6462_6b0b,
                        Box::new(Cbor::Map(vec![(
                            Cbor::Text("x".to_owned()),
                            Cbor::Tag(0x6462_6b00, Box::new(Cbor::Null))
                        ),]))
                    ),
                    Cbor::Tag(0x6462_6b01, Box::new(Cbor::Bool(true))),
                ]))
            )
        );
    }

    #[test]
    fn read_at_preserves_transaction_values_across_save_load_and_fork_snapshots() {
        let scalars = [
            (ScalarValue::Null, NativeValue::Null),
            (ScalarValue::Boolean(false), NativeValue::Bool(false)),
            (ScalarValue::Boolean(true), NativeValue::Bool(true)),
            (ScalarValue::Int(i64::MIN), NativeValue::Int(i64::MIN)),
            (ScalarValue::Uint(u64::MAX), NativeValue::Uint(u64::MAX)),
            (
                ScalarValue::F64(1.5),
                NativeValue::Float(CanonicalF64::new(1.5)),
            ),
            (
                ScalarValue::F64(-0.0),
                NativeValue::Float(CanonicalF64::new(-0.0)),
            ),
            (
                ScalarValue::F64(0.0),
                NativeValue::Float(CanonicalF64::new(0.0)),
            ),
            (
                ScalarValue::F64(f64::INFINITY),
                NativeValue::Float(CanonicalF64::new(f64::INFINITY)),
            ),
            (
                ScalarValue::F64(f64::NEG_INFINITY),
                NativeValue::Float(CanonicalF64::new(f64::NEG_INFINITY)),
            ),
            (
                ScalarValue::F64(f64::from_bits(0xfff0_0000_0000_0001)),
                NativeValue::Float(CanonicalF64::new(f64::NAN)),
            ),
            (
                ScalarValue::Str("é".into()),
                NativeValue::Str("é".to_owned()),
            ),
            (
                ScalarValue::Bytes(vec![0, 255]),
                NativeValue::Bytes(vec![0, 255]),
            ),
            (ScalarValue::Timestamp(-123), NativeValue::Timestamp(-123)),
            (ScalarValue::counter(7), NativeValue::Counter(12)),
        ];
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let facet = tx
            .put_object(automerge::ROOT, "facet", ObjType::Map)
            .unwrap();
        let list = tx.put_object(&facet, "aa", ObjType::List).unwrap();
        for (index, (scalar, expected)) in scalars.iter().enumerate() {
            tx.insert(&list, index, scalar.clone()).unwrap();
            if matches!(expected, NativeValue::Counter(_)) {
                tx.increment(&list, index, 5).unwrap();
            }
            assert_eq!(
                NativeValue::read_at(&tx, &list, index).unwrap(),
                Some(expected.clone())
            );
        }
        let text = tx.put_object(&facet, "z", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "é𐐀").unwrap();
        tx.splice_text(&text, 0, 0, "a").unwrap();
        let expected = NativeValue::Map(BTreeMap::from([
            (
                "aa".to_owned(),
                NativeValue::List(scalars.into_iter().map(|(_, native)| native).collect()),
            ),
            ("z".to_owned(), NativeValue::Text("aé𐐀".to_owned())),
        ]));
        let before_commit = NativeValue::read_at(&tx, &automerge::ROOT, "facet")
            .unwrap()
            .unwrap();
        assert_eq!(before_commit, expected);
        let encoded = encode_native(&before_commit);
        tx.commit();
        let heads = doc.get_heads();
        let loaded = Automerge::load(&doc.save()).unwrap();
        let mut tx = doc.transaction();
        tx.put(&list, 0, "changed").unwrap();
        tx.splice_text(&text, 0, 0, "changed").unwrap();
        tx.commit();
        let snapshot = doc.fork_at(&heads).unwrap();
        for reconstructed in [&loaded, &snapshot] {
            let native = NativeValue::read_at(reconstructed, &automerge::ROOT, "facet")
                .unwrap()
                .unwrap();
            assert_eq!(native, expected);
            assert_eq!(encode_native(&native), encoded);
        }
        assert_ne!(
            NativeValue::read_at(&doc, &automerge::ROOT, "facet").unwrap(),
            Some(expected)
        );
    }

    #[test]
    fn read_at_map_fixture_is_independent_of_insertion_history() {
        let expected = [
            0xda, 0x64, 0x62, 0x6b, 0x0b, 0xa2, 0x61, b'z', 0xda, 0x64, 0x62, 0x6b, 0x02, 0x01,
            0x62, b'a', b'a', 0xda, 0x64, 0x62, 0x6b, 0x05, 0x65, b'l', b'a', b't', b'e', b'r',
        ];
        for keys in [["aa", "z"], ["z", "aa"]] {
            let mut doc = AutoCommit::new();
            let facet = doc
                .put_object(automerge::ROOT, "facet", ObjType::Map)
                .unwrap();
            for key in keys {
                doc.put(&facet, key, ScalarValue::Null).unwrap();
                let value = if key == "aa" {
                    ScalarValue::Str("later".into())
                } else {
                    ScalarValue::Int(1)
                };
                doc.put(&facet, key, value).unwrap();
            }
            let value = NativeValue::read_at(&doc, &automerge::ROOT, "facet")
                .unwrap()
                .unwrap();
            assert_eq!(encode_native(&value), expected);
        }
    }

    #[test]
    fn read_at_distinguishes_absence_null_and_automerge_errors() {
        let mut doc = AutoCommit::new();
        assert_eq!(
            NativeValue::read_at(&doc, &automerge::ROOT, "value").unwrap(),
            None
        );
        doc.put(automerge::ROOT, "value", ScalarValue::Null)
            .unwrap();
        assert_eq!(
            NativeValue::read_at(&doc, &automerge::ROOT, "value").unwrap(),
            Some(NativeValue::Null)
        );
        doc.delete(automerge::ROOT, "value").unwrap();
        assert_eq!(
            NativeValue::read_at(&doc, &automerge::ROOT, "value").unwrap(),
            None
        );
        let mut other = AutoCommit::new();
        let foreign = other
            .put_object(automerge::ROOT, "foreign", ObjType::Map)
            .unwrap();
        assert!(matches!(
            NativeValue::read_at(&doc, &foreign, "value"),
            Err(NativeReadError::Automerge(_))
        ));
    }

    #[test]
    fn read_at_uses_one_materialized_winner_per_conflicted_map_key() {
        let mut doc = AutoCommit::new();
        let facet = doc
            .put_object(automerge::ROOT, "facet", ObjType::Map)
            .unwrap();
        doc.put(&facet, "key", "base").unwrap();
        let mut other = doc.fork();
        doc.put(&facet, "key", "left").unwrap();
        other.put(&facet, "key", "right").unwrap();
        doc.merge(&mut other).unwrap();
        assert_eq!(doc.get_all(&facet, "key").unwrap().len(), 2);
        let winner = NativeValue::read_at(&doc, &facet, "key").unwrap().unwrap();
        assert_eq!(
            NativeValue::read_at(&doc, &automerge::ROOT, "facet").unwrap(),
            Some(NativeValue::Map(BTreeMap::from([(
                "key".to_owned(),
                winner
            )])))
        );
    }

    #[test]
    fn read_at_rejects_unknown_scalars_and_tables_instead_of_omitting_them() {
        let mut doc = AutoCommit::new();
        let list = doc
            .put_object(automerge::ROOT, "list", ObjType::List)
            .unwrap();
        doc.insert(
            &list,
            0,
            ScalarValue::Unknown {
                type_code: 10,
                bytes: vec![1, 2],
            },
        )
        .unwrap();
        doc.put_object(automerge::ROOT, "table", ObjType::Table)
            .unwrap();
        let loaded = AutoCommit::load(&doc.save()).unwrap();
        for doc in [&doc, &loaded] {
            assert!(matches!(
                NativeValue::read_at(doc, &list, 0),
                Err(NativeReadError::Unsupported("unknown scalar kind"))
            ));
            assert!(matches!(
                NativeValue::read_at(doc, &automerge::ROOT, "list"),
                Err(NativeReadError::Unsupported("unknown scalar kind"))
            ));
            assert!(matches!(
                NativeValue::read_at(doc, &automerge::ROOT, "table"),
                Err(NativeReadError::Unsupported("table"))
            ));
        }
    }

    #[test]
    fn read_at_rejects_text_marks_blocks_and_non_string_elements_before_commit() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let marked = tx
            .put_object(automerge::ROOT, "marked", ObjType::Text)
            .unwrap();
        tx.splice_text(&marked, 0, 0, "abc").unwrap();
        tx.mark(
            &marked,
            Mark::new("bold".to_owned(), true, 0, 2),
            ExpandMark::Both,
        )
        .unwrap();
        let blocks = tx
            .put_object(automerge::ROOT, "blocks", ObjType::Text)
            .unwrap();
        let block = tx.split_block(&blocks, 0).unwrap();
        tx.put(&block, "type", "paragraph").unwrap();
        let scalar = tx
            .put_object(automerge::ROOT, "scalar", ObjType::Text)
            .unwrap();
        tx.insert(&scalar, 0, ScalarValue::Int(1)).unwrap();
        for key in ["marked", "blocks", "scalar"] {
            assert!(
                matches!(
                    NativeValue::read_at(&tx, &automerge::ROOT, key),
                    Err(NativeReadError::Unsupported(_))
                ),
                "{key}"
            );
        }
        tx.commit();
        let loaded = Automerge::load(&doc.save()).unwrap();
        for key in ["marked", "blocks", "scalar"] {
            assert!(
                matches!(
                    NativeValue::read_at(&loaded, &automerge::ROOT, key),
                    Err(NativeReadError::Unsupported(_))
                ),
                "{key}"
            );
        }
    }

    #[test]
    fn read_at_depth_limit_has_an_exact_boundary() {
        let mut doc = AutoCommit::new();
        let mut nested = doc
            .put_object(automerge::ROOT, "facet", ObjType::Map)
            .unwrap();
        for _ in 1..NativeReadBudget::MAX_DEPTH {
            nested = doc.put_object(&nested, "child", ObjType::Map).unwrap();
        }
        assert!(NativeValue::read_at(&doc, &automerge::ROOT, "facet").is_ok());
        doc.put(&nested, "child", ScalarValue::Null).unwrap();
        assert!(matches!(
            NativeValue::read_at(&doc, &automerge::ROOT, "facet"),
            Err(NativeReadError::ResourceLimit("depth"))
        ));
        assert_eq!(
            NativeValue::read_at(&doc, &nested, "child").unwrap(),
            Some(NativeValue::Null)
        );
    }

    #[test]
    fn read_at_node_budget_is_shared_across_siblings() {
        let mut doc = AutoCommit::new();
        let facet = doc
            .put_object(automerge::ROOT, "facet", ObjType::Map)
            .unwrap();
        let left = doc.put_object(&facet, "left", ObjType::List).unwrap();
        let right = doc.put_object(&facet, "right", ObjType::List).unwrap();
        let leaves = NativeReadBudget::MAX_NODES - 3;
        doc.splice(
            &left,
            0,
            0,
            std::iter::repeat_n(ScalarValue::Null, leaves / 2),
        )
        .unwrap();
        doc.splice(
            &right,
            0,
            0,
            std::iter::repeat_n(ScalarValue::Null, leaves - leaves / 2),
        )
        .unwrap();
        assert!(NativeValue::read_at(&doc, &automerge::ROOT, "facet").is_ok());
        doc.insert(&left, 0, ScalarValue::Null).unwrap();
        assert!(matches!(
            NativeValue::read_at(&doc, &automerge::ROOT, "facet"),
            Err(NativeReadError::ResourceLimit("nodes"))
        ));
    }

    #[test]
    fn read_at_text_elements_share_the_node_budget() {
        let mut doc = AutoCommit::new();
        let text = doc
            .put_object(automerge::ROOT, "text", ObjType::Text)
            .unwrap();
        let content = "x".repeat(NativeReadBudget::MAX_NODES - 1);
        doc.splice_text(&text, 0, 0, &content).unwrap();
        assert_eq!(
            NativeValue::read_at(&doc, &automerge::ROOT, "text").unwrap(),
            Some(NativeValue::Text(content))
        );
        doc.splice_text(&text, 0, 0, "x").unwrap();
        assert!(matches!(
            NativeValue::read_at(&doc, &automerge::ROOT, "text"),
            Err(NativeReadError::ResourceLimit("nodes"))
        ));
    }

    #[test]
    fn read_at_byte_budget_counts_keys_and_payloads_across_siblings() {
        let mut doc = AutoCommit::new();
        let facet = doc
            .put_object(automerge::ROOT, "facet", ObjType::Map)
            .unwrap();
        let half = NativeReadBudget::MAX_BYTES / 2 - 1;
        doc.put(&facet, "a", ScalarValue::Bytes(vec![0; half]))
            .unwrap();
        doc.put(&facet, "b", ScalarValue::Str("x".repeat(half).into()))
            .unwrap();
        assert!(NativeValue::read_at(&doc, &automerge::ROOT, "facet").is_ok());
        doc.put(&facet, "b", ScalarValue::Str("x".repeat(half + 1).into()))
            .unwrap();
        assert!(matches!(
            NativeValue::read_at(&doc, &automerge::ROOT, "facet"),
            Err(NativeReadError::ResourceLimit("bytes"))
        ));
    }

    #[test]
    fn automerge_storage_preserves_float_bits_before_native_canonicalization() {
        let bits = [
            0,
            0x8000_0000_0000_0000,
            0x3ff8_0000_0000_0000,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            0x7ff8_1234_5678_9abc,
            0xfff0_0000_0000_0001,
        ];
        let mut doc = AutoCommit::new();
        for (index, bits) in bits.iter().enumerate() {
            doc.put(
                automerge::ROOT,
                index.to_string(),
                ScalarValue::F64(f64::from_bits(*bits)),
            )
            .unwrap();
        }
        let loaded = AutoCommit::load(&doc.save()).unwrap();
        for (index, bits) in bits.iter().enumerate() {
            let (value, _) = loaded
                .get(automerge::ROOT, index.to_string())
                .unwrap()
                .unwrap();
            let ScalarValue::F64(value) = value.into_scalar().unwrap() else {
                panic!("stored value must remain a float");
            };
            assert_eq!(value.to_bits(), *bits);
            assert_eq!(
                CanonicalF64::new(value),
                CanonicalF64::new(f64::from_bits(*bits))
            );
        }
    }

    #[test]
    fn automerge_type_transition_preserves_zero_sign_for_map_and_list_slots() {
        for initial in [0.0, -0.0] {
            let mut doc = Automerge::new();
            let mut tx = doc.transaction();
            let list = tx
                .put_object(automerge::ROOT, "list", ObjType::List)
                .unwrap();
            tx.insert(&list, 0, ScalarValue::F64(initial)).unwrap();
            tx.put(automerge::ROOT, "zero", ScalarValue::F64(initial))
                .unwrap();
            tx.commit();
            let mut tx = doc.transaction();
            for (object, prop) in [
                (&automerge::ROOT, Prop::Map("zero".to_owned())),
                (&list, Prop::Seq(0)),
            ] {
                tx.put(object, prop.clone(), ScalarValue::Null).unwrap();
                tx.put(object, prop.clone(), ScalarValue::F64(-initial))
                    .unwrap();
                assert_eq!(
                    NativeValue::read_at(&tx, object, prop).unwrap(),
                    Some(NativeValue::Float(CanonicalF64::new(-initial)))
                );
            }
            assert_eq!(tx.length(&list), 1);
            tx.commit();
            let loaded = Automerge::load(&doc.save()).unwrap();
            for (object, prop) in [
                (&automerge::ROOT, Prop::Map("zero".to_owned())),
                (&list, Prop::Seq(0)),
            ] {
                assert_eq!(
                    NativeValue::read_at(&loaded, object, prop).unwrap(),
                    Some(NativeValue::Float(CanonicalF64::new(-initial)))
                );
            }
        }
    }

    #[test]
    fn automerge_equal_value_put_does_not_change_a_stored_zero_sign() {
        for initial in [0.0, -0.0] {
            let mut doc = AutoCommit::new();
            doc.put(automerge::ROOT, "zero", ScalarValue::F64(initial))
                .unwrap();
            let heads = doc.get_heads();
            doc.put(automerge::ROOT, "zero", ScalarValue::F64(-initial))
                .unwrap();
            assert_eq!(doc.get_heads(), heads);
            let (value, _) = doc.get(automerge::ROOT, "zero").unwrap().unwrap();
            let ScalarValue::F64(value) = value.into_scalar().unwrap() else {
                panic!("stored value must remain a float");
            };
            assert_eq!(value.to_bits(), initial.to_bits());
        }
    }
}
