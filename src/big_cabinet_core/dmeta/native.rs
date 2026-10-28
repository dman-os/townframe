//! Canonical encoding of a datum value in its native storage form.
//!
//! ADR 020 §4: deterministic CBOR of the materialized subtree, one application-private tag per
//! storage kind, map keys sorted by their encoded bytes, and no object, actor, or operation ids.
//! `Text` is its current string and `Counter` is its current sum, each still distinct from `Str`
//! and `Int`. A float is always binary64 with canonical NaN bits. The repository reads this tree
//! back out of the transaction and passes the bytes to [`super::cid_of`]. The result is a CIDv1
//! raw/BLAKE3 over private-tag CBOR, not DRISL or DAG-CBOR.

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
mod tests;
