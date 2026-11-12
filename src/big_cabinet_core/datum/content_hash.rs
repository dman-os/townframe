//! Content-hash slots: a kind whose key slot is a hash of its own value (ADR 035 §2.9).
//!
//! A sidecar states this on the datum's own definition with the [`CONTENT_HASH_KEYWORD`], the
//! same way it states how a position is stored. A Lexicon has no extension keyword to carry it,
//! and a node only ever trusts the declaration of a kind it has registered, so "this slot is a
//! hash of the value, computed this way" travels with the sidecar a cartridge already carries for
//! each record it holds. The first algorithm is git's object id:
//!
//! ```json
//! { "x-content-hash": { "algorithm": "gitObject", "type": "commit", "content": "raw" } }
//! ```
//!
//! says the slot is the lowercase hex SHA-1 of `"commit <len>\0"` followed by the bytes of the
//! value's `raw` member (a `{ "$bytes": … }` object), which is how git names the object. The value
//! must carry the object's bytes, because a hash can only be checked against the content it was
//! computed from.

use super::value::decode_base64;
use crate::DatumKey;
use serde_json::Value;
use sha1::Digest;
use sha1::Sha1;
use thiserror::Error;

/// The sidecar keyword that declares a kind's slot a content hash.
pub(super) const CONTENT_HASH_KEYWORD: &str = "x-content-hash";

/// The object types git names with its object id.
const GIT_OBJECT_TYPES: [&str; 4] = ["blob", "tree", "commit", "tag"];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContentHashError {
    #[error("`{CONTENT_HASH_KEYWORD}` is invalid: {reason}")]
    Declaration { reason: String },
    #[error("the value's `{member}` member is not a `$bytes` object, so there is nothing to hash")]
    Content { member: String },
    #[error("the slot `{slot}` is not the hash of the value, which hashes to `{actual}`")]
    Mismatch { slot: String, actual: String },
}

/// How a kind's key slot is computed from its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentHash {
    /// git's object id of the bytes in the value's `content` member, as an object of `object_type`.
    GitObject {
        object_type: &'static str,
        content: String,
    },
}

impl ContentHash {
    /// Read the declaration a sidecar's `x-content-hash` states.
    pub fn from_declaration(declaration: &Value) -> Result<Self, ContentHashError> {
        let refused = |reason: &str| ContentHashError::Declaration {
            reason: reason.to_owned(),
        };
        let object = declaration
            .as_object()
            .ok_or_else(|| refused("it is an object"))?;
        if let Some(key) = object
            .keys()
            .find(|key| !matches!(key.as_str(), "algorithm" | "type" | "content"))
        {
            return Err(ContentHashError::Declaration {
                reason: format!("`{key}` is not a member of the declaration"),
            });
        }
        let text = |name: &str| object.get(name).and_then(Value::as_str);
        match text("algorithm") {
            Some("gitObject") => {
                let object_type = text("type")
                    .and_then(|stated| GIT_OBJECT_TYPES.iter().find(|known| **known == stated))
                    .ok_or_else(|| refused("`type` is one of blob, tree, commit or tag"))?;
                let content = text("content")
                    .filter(|member| !member.is_empty())
                    .ok_or_else(|| refused("`content` names the member holding the bytes"))?;
                Ok(Self::GitObject {
                    object_type,
                    content: content.to_owned(),
                })
            }
            _ => Err(refused("`algorithm` is `gitObject`, the only one defined")),
        }
    }

    /// The slot `value` hashes to.
    pub fn slot_of(&self, value: &Value) -> Result<String, ContentHashError> {
        match self {
            Self::GitObject {
                object_type,
                content,
            } => {
                let bytes = value
                    .get(content)
                    .and_then(|member| member.get("$bytes"))
                    .and_then(Value::as_str)
                    .and_then(decode_base64)
                    .ok_or_else(|| ContentHashError::Content {
                        member: content.clone(),
                    })?;
                let mut hasher = Sha1::new();
                hasher.update(format!("{object_type} {}\0", bytes.len()));
                hasher.update(&bytes);
                Ok(data_encoding::HEXLOWER.encode(&hasher.finalize()))
            }
        }
    }

    /// Whether `value` hashes to `key`'s slot.
    pub fn verify(&self, key: &DatumKey, value: &Value) -> Result<(), ContentHashError> {
        let actual = self.slot_of(value)?;
        if key.slot_bytes() == actual.as_bytes() {
            Ok(())
        } else {
            Err(ContentHashError::Mismatch {
                slot: String::from_utf8_lossy(key.slot_bytes()).into_owned(),
                actual,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn declared() -> ContentHash {
        ContentHash::from_declaration(
            &json!({"algorithm": "gitObject", "type": "blob", "content": "raw"}),
        )
        .unwrap()
    }

    /// `git hash-object` of the three bytes `hi\n` (`printf 'hi\n' | git hash-object --stdin`).
    const HI: &str = "45b983be36b73c0788dc9cbcb76cbb80fc7bb057";

    #[test]
    fn a_git_object_slot_is_git_s_own_object_id() {
        let value = json!({"raw": {"$bytes": data_encoding::BASE64.encode(b"hi\n")}});
        assert_eq!(declared().slot_of(&value).unwrap(), HI);
        let key = DatumKey::from_text("org.example.blob", HI).unwrap();
        declared().verify(&key, &value).unwrap();
        let other = DatumKey::from_text("org.example.blob", "00".repeat(20)).unwrap();
        assert!(matches!(
            declared().verify(&other, &value),
            Err(ContentHashError::Mismatch { actual, .. }) if actual == HI
        ));
        // The object type is part of what is hashed.
        let commit = ContentHash::from_declaration(
            &json!({"algorithm": "gitObject", "type": "commit", "content": "raw"}),
        )
        .unwrap();
        assert_ne!(commit.slot_of(&value).unwrap(), HI);
    }

    #[test]
    fn a_value_without_the_bytes_has_nothing_to_hash() {
        for value in [
            json!({}),
            json!({"raw": "aGkK"}),
            json!({"raw": {"$bytes": "!"}}),
        ] {
            assert!(matches!(
                declared().slot_of(&value),
                Err(ContentHashError::Content { .. })
            ));
        }
    }

    #[test]
    fn declarations_are_closed() {
        for declaration in [
            json!("gitObject"),
            json!({"algorithm": "md5", "type": "blob", "content": "raw"}),
            json!({"algorithm": "gitObject", "type": "note", "content": "raw"}),
            json!({"algorithm": "gitObject", "type": "blob"}),
            json!({"algorithm": "gitObject", "type": "blob", "content": "raw", "extra": 1}),
        ] {
            assert!(
                matches!(
                    ContentHash::from_declaration(&declaration),
                    Err(ContentHashError::Declaration { .. })
                ),
                "{declaration}"
            );
        }
    }
}
