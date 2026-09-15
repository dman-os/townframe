//! The stored representation of entries (ADR 010 §3.1).
//!
//! One row is a handful of columns: `kind`, `origin`, `content`, `target`,
//! `avail`, `stat`, `claim`. Each blob column is versioned, length-prefixed, and
//! self-describing, so a future revision can extend the format and either
//! migrate or refuse rather than misread.
//!
//! The one encoding with a *contract* beyond round-tripping is the path: its
//! byte order is the walk order (§3.3). Components are joined with `NUL`, which
//! cannot appear in a component, so a component sorts before any extension of
//! itself (`notes` < `notes.txt`, `notes` < `notes/2024`) exactly as
//! [`RelPath`](crate::path::RelPath) compares. That makes the primary key
//! `(rep, path)` the ordering structure, on every platform.

use crate::entry::{Avail, Entry, Kind, Payload, StatFingerprint, TimeStamp, Token, TokenScheme};
use crate::interlude::*;
use crate::path::{PathError, RelPath};

/// Bumped whenever a blob's layout changes; decoding rejects anything else.
const VERSION: u8 = 1;

const KIND_FILE: i64 = 0;
const KIND_DIR: i64 = 1;
const KIND_SYMLINK: i64 = 2;

const AVAIL_PRESENT: i64 = 0;
const AVAIL_STUB: i64 = 1;

/// Why a recorded row cannot be read back.
///
/// Every variant names the field or the shape it refused: the caller that sees
/// this is a store holding bytes it cannot interpret, and "which column" is the
/// only part of that it can act on.
#[derive(Debug, Clone, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoredError {
    /// the blob is empty
    Empty,
    /// the blob's version is {found}, and this build writes {expected}
    Version {
        /// The version byte that was found.
        found: u8,
        /// The version byte this build writes.
        expected: u8,
    },
    /// the blob needs {wanted} bytes and has {available}
    Truncated {
        /// How many bytes the field wanted.
        wanted: usize,
        /// How many the blob had left.
        available: usize,
    },
    /// the blob has {count} trailing bytes
    Trailing {
        /// How many bytes were left over.
        count: usize,
    },
    /// the {field} tag {tag} is not one this build writes
    Tag {
        /// Which field the tag came from.
        field: &'static str,
        /// The tag that was found.
        tag: i64,
    },
    /// the {field} is {found} bytes and must be {expected}
    Length {
        /// Which field it was.
        field: &'static str,
        /// The length this build writes.
        expected: usize,
        /// The length that was found.
        found: usize,
    },
    /// a {kind} row {detail}
    Shape {
        /// The kind the row claims.
        kind: Kind,
        /// What is wrong with it.
        detail: &'static str,
    },
    /// the {field} is not utf8
    Utf8 {
        /// Which field it was.
        field: &'static str,
    },
    /// the {field} is not {expected}
    Digest {
        /// Which field it was.
        field: &'static str,
        /// What it has to be.
        expected: &'static str,
    },
    /// the recorded path is not one a checkout can hold: {reason}
    Path {
        /// Why it is not.
        reason: PathError,
    },
}

const SCHEME_BLAKE3: u8 = 0;
const SCHEME_OPAQUE: u8 = 1;

/// One entry's columns, as the store holds them.
#[derive(Clone, Debug)]
pub(crate) struct StoredEntry {
    pub kind: i64,
    pub origin: Option<Vec<u8>>,
    pub content: Option<Vec<u8>>,
    pub target: Option<Vec<u8>>,
    pub avail: i64,
    pub stat: Option<Vec<u8>>,
    pub claim: Option<Vec<u8>>,
}

impl StoredEntry {
    /// Project an entry onto its columns.
    pub fn of(entry: &Entry) -> Self {
        let (origin, content, target) = match &entry.payload {
            Payload::File { origin, content } => (
                Some(encode_token(origin)),
                content.as_ref().map(encode_token),
                None,
            ),
            Payload::Dir => (None, None, None),
            Payload::Symlink { target } => (None, None, Some(target.as_encoded_bytes().to_vec())),
        };
        Self {
            kind: match entry.kind() {
                Kind::File => KIND_FILE,
                Kind::Dir => KIND_DIR,
                Kind::Symlink => KIND_SYMLINK,
            },
            origin,
            content,
            target,
            avail: match entry.avail {
                Avail::Present => AVAIL_PRESENT,
                Avail::Stub => AVAIL_STUB,
            },
            stat: entry.stat.map(encode_stat),
            claim: entry.claim.as_ref().map(encode_token),
        }
    }

    /// Rebuild an entry from its columns, refusing any inconsistency.
    pub fn into_entry(self) -> Result<Entry, StoredError> {
        let avail = match self.avail {
            AVAIL_PRESENT => Avail::Present,
            AVAIL_STUB => Avail::Stub,
            tag => {
                return Err(StoredError::Tag {
                    field: "availability",
                    tag,
                });
            }
        };
        let stat = self.stat.map(|bytes| decode_stat(&bytes)).transpose()?;
        let claim = self.claim.map(|bytes| decode_token(&bytes)).transpose()?;
        let payload = match self.kind {
            KIND_FILE => {
                let origin = self.origin.as_deref().ok_or(StoredError::Shape {
                    kind: Kind::File,
                    detail: "has no origin column",
                })?;
                let content = self.content.as_deref().map(decode_token).transpose()?;
                Payload::File {
                    origin: decode_token(origin)?,
                    content,
                }
            }
            KIND_DIR => {
                if self.origin.is_some() || self.content.is_some() || self.target.is_some() {
                    return Err(StoredError::Shape {
                        kind: Kind::Dir,
                        detail: "carries a payload",
                    });
                }
                Payload::Dir
            }
            KIND_SYMLINK => {
                let target = self.target.as_deref().ok_or(StoredError::Shape {
                    kind: Kind::Symlink,
                    detail: "has no target column",
                })?;
                Payload::Symlink {
                    target: os_string_from_bytes(target, "target")?,
                }
            }
            tag => return Err(StoredError::Tag { field: "kind", tag }),
        };
        Ok(Entry {
            payload,
            avail,
            stat,
            claim,
        })
    }
}

/// Encode a path so that its byte order is the walk order.
pub(crate) fn encode_path(path: &RelPath) -> Vec<u8> {
    let mut out = Vec::new();
    for (index, component) in path.components().iter().enumerate() {
        if index > 0 {
            out.push(0);
        }
        out.extend_from_slice(component.as_encoded_bytes());
    }
    out
}

/// Decode a path encoded by [`encode_path`].
pub(crate) fn decode_path(bytes: &[u8]) -> Result<RelPath, StoredError> {
    if bytes.is_empty() {
        return Ok(RelPath::root());
    }
    let components = bytes
        .split(|byte| *byte == 0)
        .map(|component| os_string_from_bytes(component, "path component"))
        .collect::<Result<Vec<_>, _>>()?;
    RelPath::try_new(components).map_err(|reason| StoredError::Path { reason })
}

/// Rebuild a path component or symlink target from its bytes.
///
/// Unix names are opaque bytes and are stored verbatim; elsewhere they must be
/// utf8, because that is all the platform lets us rebuild a name from.
#[cfg(unix)]
fn os_string_from_bytes(bytes: &[u8], _field: &'static str) -> Result<OsString, StoredError> {
    use std::os::unix::ffi::OsStringExt as _;

    Ok(OsString::from_vec(bytes.to_vec()))
}

#[cfg(not(unix))]
fn os_string_from_bytes(bytes: &[u8], field: &'static str) -> Result<OsString, StoredError> {
    let text = String::from_utf8(bytes.to_vec()).map_err(|_| StoredError::Utf8 { field })?;
    Ok(OsString::from(text))
}

fn encode_token(token: &Token) -> Vec<u8> {
    let mut out = vec![VERSION];
    match token.scheme() {
        TokenScheme::Blake3 => out.push(SCHEME_BLAKE3),
        TokenScheme::Opaque(owner) => {
            out.push(SCHEME_OPAQUE);
            put_bytes(&mut out, owner.as_str().as_bytes());
        }
    }
    put_bytes(&mut out, token.as_bytes());
    out
}

fn decode_token(bytes: &[u8]) -> Result<Token, StoredError> {
    let mut at = Decoder::new(bytes)?;
    let scheme = match at.u8()? {
        SCHEME_BLAKE3 => TokenScheme::Blake3,
        SCHEME_OPAQUE => {
            let owner = at.text("token owner")?;
            TokenScheme::Opaque(crate::backend::BackendId::new(owner))
        }
        tag => {
            return Err(StoredError::Tag {
                field: "token scheme",
                tag: i64::from(tag),
            });
        }
    };
    let digest = at.bytes()?.to_vec();
    at.finish()?;
    Ok(match scheme {
        TokenScheme::Blake3 => {
            // A content token is the multihash text itself, so reading one back
            // is reading the digest the rest of the system knows it by.
            let text = std::str::from_utf8(&digest).map_err(|_| StoredError::Utf8 {
                field: "blake3 token",
            })?;
            let digest = utils_rs::hash::decode_base58_multihash_blake3(text).map_err(|_| {
                StoredError::Digest {
                    field: "blake3 token",
                    expected: "a base58btc blake3 multihash",
                }
            })?;
            Token::blake3(digest)
        }
        TokenScheme::Opaque(owner) => Token::opaque(owner, digest),
    })
}

fn encode_stat(stat: StatFingerprint) -> Vec<u8> {
    let mut out = vec![VERSION];
    put_u64(&mut out, stat.len);
    put_u32(&mut out, stat.mode);
    put_i64(&mut out, stat.mtime.secs);
    put_u32(&mut out, stat.mtime.nanos);
    out
}

fn decode_stat(bytes: &[u8]) -> Result<StatFingerprint, StoredError> {
    let mut at = Decoder::new(bytes)?;
    let stat = StatFingerprint {
        len: at.u64()?,
        mode: at.u32()?,
        mtime: TimeStamp {
            secs: at.i64()?,
            nanos: at.u32()?,
        },
    };
    at.finish()?;
    Ok(stat)
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

/// A bounds-checked reader over one stored blob.
struct Decoder<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, StoredError> {
        let Some((&version, rest)) = bytes.split_first() else {
            return Err(StoredError::Empty);
        };
        if version != VERSION {
            return Err(StoredError::Version {
                found: version,
                expected: VERSION,
            });
        }
        Ok(Self { bytes: rest, at: 0 })
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], StoredError> {
        let end = self.at.saturating_add(count);
        let slice = self.bytes.get(self.at..end).ok_or(StoredError::Truncated {
            wanted: count,
            available: self.bytes.len().saturating_sub(self.at),
        })?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, StoredError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StoredError> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect(ERROR_IMPOSSIBLE);
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, StoredError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect(ERROR_IMPOSSIBLE);
        Ok(u64::from_le_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64, StoredError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect(ERROR_IMPOSSIBLE);
        Ok(i64::from_le_bytes(bytes))
    }

    fn bytes(&mut self) -> Result<&'a [u8], StoredError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn text(&mut self, field: &'static str) -> Result<String, StoredError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| StoredError::Utf8 { field })
    }

    fn finish(&self) -> Result<(), StoredError> {
        if self.at != self.bytes.len() {
            return Err(StoredError::Trailing {
                count: self.bytes.len() - self.at,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> crate::backend::BackendId {
        crate::backend::BackendId::new("fs")
    }

    fn stat() -> StatFingerprint {
        StatFingerprint {
            len: 42,
            mode: 0o600,
            mtime: TimeStamp {
                secs: 1_700_000_000,
                nanos: 999,
            },
        }
    }

    /// What a deployment would mint for a rendered path: an encoding of its own
    /// (a doc, a state, a lens, a version) that the core never reads.
    fn produced() -> Token {
        Token::opaque(
            crate::backend::BackendId::new("daybook"),
            b"doc-1|state-4|markdown|3".to_vec(),
        )
    }

    fn samples() -> Vec<Entry> {
        vec![
            Entry::file(Token::blake3([1u8; 32]), stat()),
            Entry::file(Token::opaque(backend(), b"stat+inode".to_vec()), stat())
                .with_content_evidence(Token::blake3([2u8; 32])),
            Entry::file(produced(), stat()).with_claim(produced()),
            Entry::dir(stat()),
            Entry::symlink("../../elsewhere/notes.md", stat()),
            Entry::file(produced(), stat())
                .stubbed()
                .with_claim(produced()),
        ]
    }

    #[test]
    fn entries_round_trip_through_their_columns() {
        for entry in samples() {
            let stored = StoredEntry::of(&entry);
            let decoded = stored
                .clone()
                .into_entry()
                .unwrap_or_else(|err| panic!("{entry:?} failed to decode: {err}"));
            assert_eq!(decoded, entry, "round trip changed {entry:?}");
            assert_eq!(StoredEntry::of(&decoded).kind, stored.kind);
        }
    }

    #[test]
    fn paths_round_trip_and_keep_their_order() {
        let mut paths = vec![
            RelPath::root(),
            RelPath::try_new(vec![OsString::from("notes")]).expect(ERROR_PARSE),
            RelPath::try_new(vec![OsString::from("notes.txt")]).expect(ERROR_PARSE),
            RelPath::try_new(vec![OsString::from("notes"), OsString::from("2024")])
                .expect(ERROR_PARSE),
            RelPath::try_new(vec![
                OsString::from("notes"),
                OsString::from("2024"),
                OsString::from("plan.md"),
            ])
            .expect(ERROR_PARSE),
            RelPath::try_new(vec![OsString::from("a b"), OsString::from("c")]).expect(ERROR_PARSE),
        ];
        paths.sort();
        let mut encoded = paths.iter().map(encode_path).collect::<Vec<_>>();
        let sorted = {
            let mut sorted = encoded.clone();
            sorted.sort();
            sorted
        };
        assert_eq!(encoded, sorted, "byte order must be walk order");
        encoded.dedup();
        assert_eq!(encoded.len(), paths.len(), "encoding must be injective");
        for path in &paths {
            assert_eq!(&decode_path(&encode_path(path)).expect(ERROR_PARSE), path);
        }
    }

    #[test]
    fn garbage_is_refused_rather_than_misread() {
        assert_eq!(
            decode_path(b"a\0"),
            Err(StoredError::Path {
                reason: PathError::Empty
            }),
            "empty trailing component"
        );
        assert_eq!(decode_token(&[]), Err(StoredError::Empty), "empty blob");
        assert_eq!(
            decode_token(&[VERSION + 1, SCHEME_BLAKE3]),
            Err(StoredError::Version {
                found: VERSION + 1,
                expected: VERSION
            }),
            "a version this build does not write"
        );
        assert_eq!(
            decode_token(&[VERSION, 9]),
            Err(StoredError::Tag {
                field: "token scheme",
                tag: 9
            }),
            "a scheme this build does not know"
        );
        let mut trailing = encode_stat(stat());
        trailing.push(0);
        assert_eq!(
            decode_stat(&trailing),
            Err(StoredError::Trailing { count: 1 }),
            "trailing bytes"
        );
        let mut truncated = encode_stat(stat());
        truncated.truncate(5);
        assert_eq!(
            decode_stat(&truncated),
            Err(StoredError::Truncated {
                wanted: 8,
                available: 4
            }),
            "a field that runs off the end"
        );

        let mut wrong_shape = StoredEntry::of(&Entry::dir(stat()));
        wrong_shape.origin = Some(encode_token(&Token::blake3([0u8; 32])));
        assert_eq!(
            wrong_shape.into_entry(),
            Err(StoredError::Shape {
                kind: Kind::Dir,
                detail: "carries a payload"
            }),
            "a directory row carrying a file's origin"
        );
    }
}
