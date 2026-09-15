//! Entries: the per-path state the bridge records for a backend.
//!
//! Identity here is **provenance, not content addressing** (ADR 010 §2.3). An
//! entry never has to produce the bytes it stands for in order to say that it
//! changed: it carries a [`Token`] whose owner promises it is a deterministic
//! function of the content. A blob store hashes for free; a checkout reports its
//! blake3 digest, or a stat-derived marker where it declines to read a large
//! file; a deployment that *produces* content reports what it would produce from.
//! All three are the same thing here — a scheme tag and some bytes — because the
//! core never reads either (ADR 010 §2.3, "who decides"): whether two different
//! tokens stand for the same bytes is a question about schemes, and the backend
//! holding the path is the one that answers it.

use crate::backend::BackendId;
use crate::interlude::*;

/// The shape of an entry, without its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// A regular file with bytes.
    File,
    /// A directory: addressable, and carries no payload.
    Dir,
    /// A symbolic link, identified by its target.
    Symlink,
}

impl fmt::Display for Kind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::File => "file",
            Self::Dir => "directory",
            Self::Symlink => "symlink",
        })
    }
}

/// The content-bearing part of an entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    /// Bytes, and the backend's name for them.
    File {
        /// What the bytes are, in the owning backend's scheme. The core compares
        /// these and never interprets them.
        origin: Token,
        /// Evidence: the digest of bytes this device has actually produced or
        /// ingested ([`Entry::with_content_evidence`]). Optional, and a digest is
        /// the one identity every backend can compare against its own reading of
        /// a file, so it is what lets a backend recognise bytes it already holds
        /// under a name it does not know.
        content: Option<Token>,
    },
    /// A directory.
    Dir,
    /// A symbolic link to `target`, verbatim (never resolved here).
    Symlink { target: OsString },
}

/// Whose tokens these are, and therefore which ones the core may compare.
///
/// Blake3 is the one scheme the core knows, because a digest is the one identity
/// every backend can compare against its own reading of a file. Everything else
/// is a backend's private business.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TokenScheme {
    /// A blake3 content digest, in Daybook's shared multihash encoding (ADR
    /// 001): self-describing and base58btc, so a token minted here equals the
    /// digest a blob facet carries for the same bytes, whichever side
    /// computed it.
    Blake3,
    /// Backend-private, and comparable only within its owner (ADR 010 §2.3);
    /// used where establishing a content hash is refused, not merely deferred.
    Opaque(BackendId),
}

/// A content identity: what a backend says bytes are.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Token {
    scheme: TokenScheme,
    bytes: Box<[u8]>,
}

impl Token {
    /// A blake3 digest, as [`blake3::Hash::as_bytes`] returns it.
    ///
    /// The digest is stored in the shared encoding, so a token built from a
    /// digest another subsystem computed is the *same* token — which is what
    /// lets a blob store and a checkout agree about a file without either
    /// re-hashing it.
    #[must_use]
    pub fn blake3(digest: [u8; 32]) -> Self {
        Self {
            scheme: TokenScheme::Blake3,
            bytes: utils_rs::hash::encode_base58_multihash_blake3(digest)
                .into_bytes()
                .into_boxed_slice(),
        }
    }

    /// Hash bytes with blake3.
    #[must_use]
    pub fn blake3_of(bytes: &[u8]) -> Self {
        Self::blake3(*blake3::hash(bytes).as_bytes())
    }

    /// A token in a scheme of the caller's own.
    ///
    /// This is how a deployment mints identities the core must never read: the
    /// scheme says whose they are and what they mean, and the bytes are whatever
    /// that backend wants them to be. Daybook's recipe — doc, doc state, lens and
    /// version — is one such encoding (ADR 012 §3).
    #[must_use]
    pub fn new(scheme: TokenScheme, bytes: impl Into<Box<[u8]>>) -> Self {
        Self {
            scheme,
            bytes: bytes.into(),
        }
    }

    /// A backend-private token: comparable only against tokens from the same
    /// `owner`. Two backends minting tokens from a stat, an inode, or a
    /// sequence number cannot collide with each other.
    #[must_use]
    pub fn opaque(owner: BackendId, bytes: impl Into<Box<[u8]>>) -> Self {
        Self {
            scheme: TokenScheme::Opaque(owner),
            bytes: bytes.into(),
        }
    }

    /// Which scheme, and whose tokens this is comparable with.
    #[must_use]
    pub fn scheme(&self) -> &TokenScheme {
        &self.scheme
    }

    /// The token's bytes: a content token's multihash text, or a backend's own
    /// encoding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Display for Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.scheme {
            TokenScheme::Blake3 => {
                write!(formatter, "blake3:{}", String::from_utf8_lossy(&self.bytes))
            }
            TokenScheme::Opaque(owner) => write!(formatter, "opaque:{owner}:{}", hex(&self.bytes)),
        }
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Token({self})")
    }
}

/// Whether the bytes are where they need to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Avail {
    /// Locally addressable right now.
    Present,
    /// Known to belong here, but the bytes have not landed yet: "there should
    /// be a file here" (ADR 010 §2.4).
    Stub,
}

/// A wall clock reading, in the representation the store records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimeStamp {
    /// Seconds since the unix epoch.
    pub secs: i64,
    /// Nanoseconds within that second.
    pub nanos: u32,
}

impl From<SystemTime> for TimeStamp {
    fn from(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(since) => Self {
                secs: since.as_secs() as i64,
                nanos: since.subsec_nanos(),
            },
            Err(before) => {
                // Before the epoch: keep the reading, negative.
                let since = before.duration();
                Self {
                    secs: -(since.as_secs() as i64) - i64::from(since.subsec_nanos() != 0),
                    nanos: if since.subsec_nanos() == 0 {
                        0
                    } else {
                        1_000_000_000 - since.subsec_nanos()
                    },
                }
            }
        }
    }
}

impl From<TimeStamp> for SystemTime {
    fn from(stamp: TimeStamp) -> Self {
        if stamp.secs >= 0 {
            UNIX_EPOCH + Duration::new(stamp.secs as u64, stamp.nanos)
        } else {
            let back = Duration::new((-stamp.secs) as u64, 0)
                - Duration::from_nanos(u64::from(stamp.nanos));
            UNIX_EPOCH - back
        }
    }
}

/// A filesystem's change-detection fingerprint: everything that says "this
/// path may have changed" without reading a byte of it (ADR 010 §3.2).
///
/// `atime` is deliberately absent: reading a file must not look like a change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StatFingerprint {
    /// Size in bytes.
    pub len: u64,
    /// Permission bits, as the platform reports them.
    pub mode: u32,
    /// Last modification time.
    pub mtime: TimeStamp,
}

/// One recorded path: what is there, what it is, and what we know about it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The shape and, for files, the provenance.
    pub payload: Payload,
    /// Whether the bytes are here.
    pub avail: Avail,
    /// The backend's stat fingerprint, when the backend has one.
    pub stat: Option<StatFingerprint>,
    /// Who put this path here, when something claims it (ADR 010 §8.6).
    ///
    /// Opaque to the core, like every identity. A deployment that produces
    /// content fills it from the same recipe as the origin, so ownership cannot
    /// drift from what the bytes are; a mirror policy would fill it with the
    /// backend it copied from; a checkout fills it with nothing, which is why its
    /// files are never deleted by a pass. It answers "may this path go when the
    /// source drops it" — and it is why an edit can change what the bytes are
    /// without changing who owns the path.
    pub claim: Option<Token>,
}

impl Entry {
    /// A present file with the given identity.
    #[must_use]
    pub fn file(origin: Token, stat: impl Into<Option<StatFingerprint>>) -> Self {
        Self {
            payload: Payload::File {
                origin,
                content: None,
            },
            avail: Avail::Present,
            stat: stat.into(),
            claim: None,
        }
    }

    /// A present directory.
    #[must_use]
    pub fn dir(stat: impl Into<Option<StatFingerprint>>) -> Self {
        Self {
            payload: Payload::Dir,
            avail: Avail::Present,
            stat: stat.into(),
            claim: None,
        }
    }

    /// A present symlink with the given target.
    #[must_use]
    pub fn symlink(target: impl Into<OsString>, stat: impl Into<Option<StatFingerprint>>) -> Self {
        Self {
            payload: Payload::Symlink {
                target: target.into(),
            },
            avail: Avail::Present,
            stat: stat.into(),
            claim: None,
        }
    }

    /// The entry's shape.
    #[must_use]
    pub fn kind(&self) -> Kind {
        match &self.payload {
            Payload::File { .. } => Kind::File,
            Payload::Dir => Kind::Dir,
            Payload::Symlink { .. } => Kind::Symlink,
        }
    }

    /// The identity a backend named for this entry's bytes, when it has one.
    ///
    /// This is what lets a backend compare what it recorded against what it
    /// would report now without reading any bytes — the whole reason
    /// [`agrees_with`](Self::agrees_with) can be cheap
    /// ([`Backend::accept`](crate::backend::Backend::accept) is where the
    /// expensive answer is given).
    #[must_use]
    pub fn origin(&self) -> Option<&Token> {
        match &self.payload {
            Payload::File { origin, .. } => Some(origin),
            Payload::Dir | Payload::Symlink { .. } => None,
        }
    }

    /// The digest this entry's bytes are known to hash to, when one is known.
    ///
    /// A blake3 origin *is* a content hash, so a file named that way needs no
    /// separate evidence. Any other scheme only has evidence once some device has
    /// had the bytes in hand ([`Entry::with_content_evidence`]) — which is what a
    /// backend compares against its own reading of a file when the identity it
    /// recorded was minted by somebody else.
    #[must_use]
    pub fn content_token(&self) -> Option<&Token> {
        match &self.payload {
            Payload::File { origin, content } => match origin.scheme() {
                TokenScheme::Blake3 => Some(origin),
                TokenScheme::Opaque(_) => content.as_ref(),
            },
            Payload::Dir | Payload::Symlink { .. } => None,
        }
    }

    /// Whether two records agree, as far as the core can tell and cheaply.
    ///
    /// This is **not** the question "are these the same bytes". For files the
    /// core deliberately does not answer that (ADR 010 §2.3): whether two
    /// different identities stand for the same content depends on the schemes in
    /// play, and the same disagreement means "nothing to do" for one pair of
    /// backends and "produce it again" for another. Only the backend holding the
    /// path can tell those apart, so the core asks it
    /// ([`Backend::accept`](crate::backend::Backend::accept)).
    ///
    /// What this answers is "is there anything here worth asking about": the same
    /// shape, the same identity token, and the same availability. A `false` is a
    /// question for the target, never a decision to transfer.
    #[must_use]
    pub fn agrees_with(&self, other: &Self) -> bool {
        if self.avail != other.avail {
            return false;
        }
        match (&self.payload, &other.payload) {
            (Payload::Dir, Payload::Dir) => true,
            (Payload::Symlink { target: mine }, Payload::Symlink { target: theirs }) => {
                mine == theirs
            }
            (Payload::File { origin: mine, .. }, Payload::File { origin: theirs, .. }) => {
                mine == theirs
            }
            _ => false,
        }
    }

    /// Attach the digest of bytes this device produced or ingested.
    ///
    /// This is how identity survives a transfer written under somebody else's
    /// name: bytes written here from a source are recorded with the source's
    /// identity *and* their digest, so the next scan of the file confirms that
    /// identity instead of overwriting it (ADR 010 §2.3, "identity
    /// continuity").
    ///
    /// Only ever set this to a digest of the entry's actual bytes.
    #[must_use]
    pub fn with_content_evidence(mut self, digest: Token) -> Self {
        if let Payload::File { content, .. } = &mut self.payload {
            *content = Some(digest);
        }
        self
    }

    /// Record the stat this device observed, keeping everything else.
    #[must_use]
    pub fn with_stat(mut self, stat: impl Into<Option<StatFingerprint>>) -> Self {
        self.stat = stat.into();
        self
    }

    /// Record what put this path here.
    #[must_use]
    pub fn with_claim(mut self, claim: Token) -> Self {
        self.claim = Some(claim);
        self
    }

    /// Mark the bytes as not here yet.
    #[must_use]
    pub fn stubbed(mut self) -> Self {
        self.avail = Avail::Stub;
        self
    }

    /// Mark the bytes as here.
    #[must_use]
    pub fn present(mut self) -> Self {
        self.avail = Avail::Present;
        self
    }
}

/// Lowercase hex, for the ids that reach logs and error messages.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(name: &str) -> BackendId {
        BackendId::new(name)
    }

    fn stat(len: u64) -> StatFingerprint {
        StatFingerprint {
            len,
            mode: 0o644,
            mtime: TimeStamp { secs: 17, nanos: 0 },
        }
    }

    /// A digest: the one identity every backend can compare against its own
    /// reading of a file.
    fn digest(seed: u8) -> Token {
        Token::blake3([seed; 32])
    }

    /// A deployment's own identity — what daybook would mint for a rendered path.
    /// The bytes are the deployment's business (a doc, a state, a lens, a
    /// version, encoded however it likes); the core only ever compares them.
    fn produced(state: u8) -> Token {
        Token::opaque(backend("daybook"), [state; 4])
    }

    fn external(origin: Token) -> Entry {
        Entry::file(origin, stat(3))
    }

    #[test]
    fn blake3_tokens_compare_across_backends() {
        // The blob store's hash and the local file's hash name the same bytes.
        assert!(external(digest(1)).agrees_with(&external(digest(1))));
        assert!(!external(digest(1)).agrees_with(&external(digest(2))));
    }

    /// The point of the shared encoding: a token minted here *is* the digest the
    /// rest of the system knows a blob by, so neither side has to re-hash a file
    /// to agree about it — and a backend handed a digest string (an adopted
    /// blob, say) mints the very same token from the string alone.
    #[test]
    fn a_content_token_is_the_digest_the_rest_of_the_system_knows() {
        let bytes = b"# plan\n";
        let digest = *blake3::hash(bytes).as_bytes();
        let expected = utils_rs::hash::encode_base58_multihash_blake3(digest);

        assert_eq!(
            Token::blake3_of(bytes).as_bytes(),
            expected.as_bytes(),
            "hashing here mints the shared multihash text",
        );
        assert_eq!(
            Token::blake3(digest),
            Token::new(TokenScheme::Blake3, expected.clone().into_bytes()),
            "a digest string from another subsystem is the same token",
        );
        assert_eq!(
            Token::blake3(digest).to_string(),
            format!("blake3:{expected}"),
            "and it renders as the digest it is",
        );
    }

    #[test]
    fn opaque_tokens_never_cross_owners_or_get_confused_with_content() {
        let mine = external(Token::opaque(backend("fs"), b"stat".to_vec()));
        let same = external(Token::opaque(backend("fs"), b"stat".to_vec()));
        let other = external(Token::opaque(backend("other"), b"stat".to_vec()));
        assert!(mine.agrees_with(&same));
        assert!(!mine.agrees_with(&other));
        // A backend-private token is never equal to a digest, whoever minted it.
        assert!(!mine.agrees_with(&external(digest(9))));

        // Its *evidence* is what another backend compares instead, once this
        // device has had the bytes in hand.
        let verified = mine.clone().with_content_evidence(digest(9));
        assert_eq!(verified.content_token(), Some(&digest(9)));
        assert_eq!(external(digest(9)).content_token(), Some(&digest(9)));
        assert_eq!(
            mine.content_token(),
            None,
            "an opaque token is not a digest"
        );
    }

    #[test]
    fn a_deployments_own_identity_is_compared_verbatim_and_never_interpreted() {
        assert!(external(produced(1)).agrees_with(&external(produced(1))));
        assert!(!external(produced(1)).agrees_with(&external(produced(2))));

        // Two of a deployment's identities can stand for the same bytes and its
        // new version can produce different ones; the core cannot tell which, and
        // does not try. Agreeing digests change nothing here, because one pair of
        // backends reads that as "nothing to do" and another as "produce it
        // again" — the same disagreement, opposite answers. The backend holding
        // the path answers instead (`Backend::accept`).
        let evidence = Token::blake3_of(b"# plan\n");
        let older = external(produced(1)).with_content_evidence(evidence.clone());
        let newer = external(produced(2)).with_content_evidence(evidence);
        assert!(!older.agrees_with(&newer));
    }

    #[test]
    fn shapes_targets_and_availability_are_part_of_the_agreement() {
        assert!(Entry::dir(stat(0)).agrees_with(&Entry::dir(stat(9))));
        assert!(!Entry::dir(stat(0)).agrees_with(&Entry::symlink("x", stat(1))));
        assert!(Entry::symlink("x", stat(1)).agrees_with(&Entry::symlink("x", stat(2))));
        assert!(!Entry::symlink("x", stat(1)).agrees_with(&Entry::symlink("y", stat(1))));
        // A directory and a file are never the same thing, even at one path.
        assert!(!Entry::dir(stat(0)).agrees_with(&external(digest(1))));

        // A stub is a different record from the bytes it promises, even with the
        // same identity: a stub is a path waiting to be filled (ADR 010 §2.4).
        let bytes = external(digest(1));
        assert!(!bytes.clone().stubbed().agrees_with(&bytes));
    }

    #[test]
    fn a_claim_is_not_the_identity() {
        // Two separate questions, recorded separately: an edit moves the bytes
        // and leaves the owner alone, which is what lets a pass remove a path
        // whose bytes its user has been rewriting (ADR 010 §8.6).
        let owned =
            external(digest(1)).with_claim(Token::opaque(backend("daybook"), b"doc-7".to_vec()));
        let edited = external(digest(2)).with_claim(owned.claim.clone().expect("claimed"));
        assert!(!owned.agrees_with(&edited), "the bytes moved");
        assert_eq!(owned.claim, edited.claim, "and the owner did not");
    }

    #[test]
    fn timestamps_round_trip_through_system_time() {
        for stamp in [
            TimeStamp {
                secs: 1_700_000_000,
                nanos: 123_456_789,
            },
            TimeStamp { secs: 0, nanos: 0 },
            TimeStamp {
                secs: -1,
                nanos: 999_999_999,
            },
        ] {
            assert_eq!(TimeStamp::from(SystemTime::from(stamp)), stamp);
        }
    }
}
