// FIXME: consdier using u64 or u128 for ObjKeys since they'll
// be repo scoped

use crate::interlude::*;
use crate::rpc::BuckLevel;

macro_rules! alias_byte_key {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub ByteKey);
        impl std::ops::Deref for $name {
            type Target = ByteKey;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl $name {
            /// Construct from the key's byte string.
            ///
            /// ADR 012 decision 1: identity *is* the byte string, so any length is a
            /// valid key. The 32-byte width this replaced was an artifact of deriving
            /// keys as random digests, and an ordering requirement is a property of the
            /// byte string rather than of the width.
            #[must_use]
            pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
                Self(ByteKey::new(bytes))
            }

            pub fn random() -> Self {
                Self(ByteKey::random())
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, formatter)
            }
        }
        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.0, formatter)
            }
        }
        impl std::str::FromStr for $name {
            type Err = DecodeError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Ok(Self(ByteKey::from_str(value)?))
            }
        }

        #[cfg(feature = "automerge")]
        impl autosurgeon::Reconcile for $name {
            type Key<'a> = autosurgeon::reconcile::NoKey;

            fn reconcile<R: autosurgeon::Reconciler>(
                &self,
                mut reconciler: R,
            ) -> Result<(), R::Error> {
                reconciler.bytes(self.as_bytes())
            }
        }

        #[cfg(feature = "automerge")]
        impl autosurgeon::Hydrate for $name {
            fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
                Ok(Self(ByteKey::new(bytes)))
            }
        }
    };
}

alias_byte_key!(PartKey);
alias_byte_key!(ObjKey);
alias_byte_key!(PeerKey);

impl ObjKey {
    /// The derived part key of this object's single-object part.
    ///
    /// ADR 012 decision 3: an object part is an ordinary one-member part whose key is
    /// derived from the object key, so anyone holding the object key can compute it and
    /// it cannot collide with an unrelated part. The reserved key space of decision 1
    /// makes that the literal `o:{object_key}`: the `o:` scheme keeps the boundary
    /// between scheme and value readable for a key that is itself a path
    /// (`o:/object/path`), which neither a `/o/` prefix (a doubled slash) nor a bare
    /// `/o` (no readable boundary) gives.
    ///
    /// The appended value is the object key's own bytes, not its textual form, so the
    /// derivation is injective on identity: a 32-byte digest renders as `o:z…` and a
    /// textual key as `o:/object/path`, and either round-trips through `ByteKey::from_str`.
    #[must_use]
    pub fn object_part_key(&self) -> PartKey {
        let mut bytes = b"o:".to_vec();
        bytes.extend_from_slice(self.as_bytes());
        PartKey::new(bytes)
    }
}

impl PartKey {
    /// The object key this part is derived from, for an object part.
    ///
    /// ADR 012 decision 3: the derivation is the reserved `o:` scheme followed by the
    /// object key's own bytes, so it round-trips exactly. `None` for a key outside the
    /// reserved object-part space — the reserved scheme is what makes the distinction
    /// decidable without consulting storage.
    #[must_use]
    pub fn object_key(&self) -> Option<ObjKey> {
        self.as_bytes().strip_prefix(b"o:").map(ObjKey::new)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteKey(std::sync::Arc<[u8]>);

impl ByteKey {
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(std::sync::Arc::from(bytes.into()))
    }

    pub fn random() -> Self {
        Self(std::sync::Arc::from(rand::random::<[u8; 32]>().to_vec()))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// The key's bytes as a fixed-width 32-byte array.
    ///
    /// ADR 012 decision 1 makes keys variable-length, but the fixed-width consumers at
    /// the workspace edges — ed25519 verifying keys, `KeyhivePeerId`, keyhive archive
    /// reservations, automerge change hashes — still take a `[u8; 32]`. Every key that
    /// reaches them is a 32-byte digest by construction, so a different length is an
    /// invariant break rather than something to handle.
    #[must_use]
    pub fn to_bytes32(&self) -> [u8; 32] {
        self.as_bytes()
            .try_into()
            .expect("key that must be 32 bytes is not")
    }
}

/// The reserved key spaces of ADR 012 decision 1, which read as the text they are:
/// `/…` collection keys such as `/seds`, and `o:/…` object-part keys over a
/// path-shaped object key.
///
/// Everything else — a group part, an object part over a digest, an object, a peer — is
/// binary or non-textual, and renders as multibase base58btc instead.
fn reserved_text(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.chars().any(char::is_control) {
        return None;
    }
    let reserved = match text.strip_prefix("o:") {
        Some(payload) => payload.starts_with('/'),
        None => text.starts_with('/'),
    };
    reserved.then_some(text)
}

impl std::fmt::Display for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // ADR 012 decision 1: the reserved key spaces read as the text they are, so
        // `"/seds"` and `"o:/object/path"` are their own names and the `o:` scheme stays
        // readable on a key whose payload is itself a path. An object part over a digest
        // keeps the scheme and renders its payload as multibase base58btc (`o:z…`), and
        // every other key renders as multibase outright. `FromStr` is the exact inverse:
        // only a path-shaped payload is read back as literal text, which is what keeps
        // the two total on every key rather than ambiguous on keys that begin with `z`.
        match reserved_text(&self.0) {
            Some(text) => formatter.write_str(text),
            None => match self.0.strip_prefix(b"o:") {
                Some(payload) => write!(
                    formatter,
                    "o:{}",
                    utils_rs::hash::encode_base58_multibase(payload)
                ),
                None => write!(
                    formatter,
                    "{}",
                    utils_rs::hash::encode_base58_multibase(&self.0)
                ),
            },
        }
    }
}

impl std::fmt::Debug for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// Key text is neither a reserved key space nor a multibase base58btc string
pub struct DecodeError;

impl std::str::FromStr for ByteKey {
    type Err = DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // The inverse of `Display`, and deliberately not total: this parse is an
        // external boundary — a blob hash out of a URL or blob metadata, a uniffi
        // caller, an id read back from storage — where text that is not a key has to
        // be an error rather than a fresh identity. A key is the reserved text it reads
        // as (`/seds`, `o:/object/path`), or `o:` followed by a multibase payload, or
        // multibase.
        if reserved_text(value.as_bytes()).is_some() {
            return Ok(Self::new(value.as_bytes()));
        }
        if let Some(payload) = value.strip_prefix("o:") {
            let encoded = payload.strip_prefix('z').ok_or(DecodeError)?;
            let mut bytes = b"o:".to_vec();
            bytes.extend_from_slice(&bs58::decode(encoded).into_vec().map_err(|_| DecodeError)?);
            return Ok(Self::new(bytes));
        }
        let encoded = value.strip_prefix('z').ok_or(DecodeError)?;
        Ok(Self::new(
            bs58::decode(encoded).into_vec().map_err(|_| DecodeError)?,
        ))
    }
}

impl Serialize for ByteKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            utils_rs::hash::encode_base58_multibase(&self.0).serialize(serializer)
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> serde::Deserialize<'de> for ByteKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let str = String::deserialize(deserializer)?;
            // One codec: the human-readable form is `Display`, so a reserved key reads
            // back as the text it is alongside multibase keys. (`FromStr` rather than
            // `decode_base58_multibase`, which indexes the first byte and so panics on
            // the empty string instead of erroring.)
            std::str::FromStr::from_str(&str).map_err(serde::de::Error::custom)
        } else {
            struct MyVisitor;
            impl<'de> serde::de::Visitor<'de> for MyVisitor {
                type Value = Vec<u8>;

                fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
                    fmt.write_str("a byte string")
                }

                fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(val.to_vec())
                }
            }
            deserializer.deserialize_bytes(MyVisitor).map(Self::new)
        }
    }
}

#[cfg(feature = "automerge")]
impl autosurgeon::Reconcile for ByteKey {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(self.as_bytes())
    }
}

#[cfg(feature = "automerge")]
impl autosurgeon::Hydrate for ByteKey {
    fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
        Ok(Self::new(bytes))
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct BuckId(u32);

impl BuckId {
    pub const ROOT: Self = Self::new(0, 0);
    pub const ARITY: u16 = 16;
    pub const BITS_PER_LEVEL: u8 = Self::ARITY.ilog2() as _;
    pub const MAX_LEVEL: u8 = u16::BITS as u8 / Self::BITS_PER_LEVEL;

    #[inline]
    pub const fn new(level: u8, index: u16) -> Self {
        let level = if level > Self::MAX_LEVEL {
            Self::MAX_LEVEL
        } else {
            level
        };
        let index = if level == Self::MAX_LEVEL {
            index
        } else {
            let max_index = (1u32 << (level as u32 * Self::BITS_PER_LEVEL as u32)) as u16;
            if index >= max_index {
                max_index.saturating_sub(1)
            } else {
                index
            }
        };
        Self(((level as u32) << 16) | index as u32)
    }

    #[inline]
    pub const fn level(&self) -> u8 {
        (self.0 >> 16) as _
    }

    #[inline]
    pub const fn index(&self) -> u16 {
        self.0 as _
    }

    #[inline]
    pub const fn parent(&self) -> Self {
        self.to_level(self.level().saturating_sub(1))
    }

    /// When going up levels, returns the parent of the current bucket.
    /// When going deeper, returns the first child at that level.
    #[inline]
    pub const fn to_level(&self, level: u8) -> Self {
        let level = if level > Self::MAX_LEVEL {
            Self::MAX_LEVEL
        } else {
            level
        };
        if level == self.level() {
            *self
        } else if level < self.level() {
            let diff = self.level() - level;
            let shift = (diff as u32) * (Self::BITS_PER_LEVEL as u32);
            Self::new(level, ((self.index() as u32) >> shift) as u16)
        } else {
            let diff = level - self.level();
            let shift = (diff as u32) * (Self::BITS_PER_LEVEL as u32);
            Self::new(level, ((self.index() as u32) << shift) as u16)
        }
    }

    /// The deepest bucket an object key falls into: the full hash-derived index.
    ///
    /// ADR 012 decision 1: distribution is an explicitly chosen hash of the key at the
    /// point of use, never an index inherited from the key's own leading bytes. Keys are
    /// arbitrary byte strings whose prefixes now *mean* something — every object part key
    /// begins `o:`, and a textual key's leading bytes are its path — so an inherited index
    /// would pile unrelated objects into one bucket and degenerate the tree at its top
    /// level (every `o:`-prefixed key would land in the single level-4 bucket `0x6f3a`).
    ///
    /// Hashing first keeps the level/truncation hierarchy intact, because a parent is
    /// still a prefix of its children in *hash* space. It severs only the correspondence
    /// between bucket order and key order, and nothing in the protocol relies on that:
    /// requests, pages and relists are all ordered and compared in bucket order, and the
    /// per-part authorization boundary means a bucket is never a disclosure unit.
    #[inline]
    #[must_use]
    pub fn deepest_from_obj_key(obj_key: &ObjKey) -> Self {
        let hash = blake3::hash(obj_key.as_bytes());
        let bytes = hash.as_bytes();
        Self::new(Self::MAX_LEVEL, u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// The bucket an object key falls into at `level`.
    #[inline]
    #[must_use]
    pub fn from_obj_key(level: BuckLevel, obj_key: &ObjKey) -> Self {
        debug_assert!(level <= Self::MAX_LEVEL);
        Self::deepest_from_obj_key(obj_key).to_level(level)
    }

    /// The next page cursor within this level.
    ///
    /// ADR 012 decision 1 audit: this counts pages *at one level*, it does not walk the
    /// tree, and an increment past this level's last index deliberately leaves the level
    /// field alone. Requests are level-scoped (`get_changed_buckets` matches on
    /// `offset.level()`), so the overflowed index is only ever an exclusive lower bound in
    /// bucket order: the store answers an empty page, `filter_buckets` reads that as
    /// `Done`, and that *is* what "nothing more at this level" looks like. Offsets that do
    /// change level come from a dive (`Relist(dirty.to_level(level + 1))`), which the
    /// machine's `next_page_offset.level() > working_level` check catches. None of this
    /// depends on key order, which is why the hash-derived index needed no change here.
    #[inline]
    pub fn increment(&self) -> Self {
        if *self == Self::new(Self::MAX_LEVEL, u16::MAX) {
            *self
        } else {
            Self(self.0 + 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    /// ADR 012 decision 1: a reserved key space is its own name, and an object part keeps
    /// the `o:` scheme readable over both a path-shaped and a digest object key.
    #[test]
    fn reserved_key_spaces_read_as_their_text() {
        assert_eq!(PartKey::new("/seds").to_string(), "/seds");
        assert_eq!(PartKey::new("/drawer/plans").to_string(), "/drawer/plans");

        let path = ObjKey::new(b"/object/path");
        assert_eq!(path.to_string(), "/object/path");
        assert_eq!(path.object_part_key().to_string(), "o:/object/path");

        // A digest renders as multibase, and the object part keeps the scheme in front of
        // it, so the part reads as an object part rather than as an unrelated digest.
        let digest = ObjKey::new([7; 32]);
        assert!(digest.to_string().starts_with('z'), "{digest}");
        let part = digest.object_part_key().to_string();
        assert!(part.starts_with("o:z"), "{part}");
        assert_eq!(
            part.strip_prefix("o:")
                .and_then(|payload| payload.parse::<ObjKey>().ok()),
            Some(digest)
        );
    }

    /// `Display` and `FromStr` are inverses on every key the crate constructs. The `o:`
    /// payload is only read back as text when it is path-shaped, which is what keeps the
    /// pair total instead of ambiguous on a payload that itself begins with `z`.
    #[test]
    fn keys_round_trip_through_their_text_form() {
        for key in [
            PartKey::new("/seds"),
            PartKey::new(b"/binary\xff\x00"),
            PartKey::new(b"o:zero"),
            PartKey::new(b"o:\x00\x01"),
        ] {
            assert_eq!(key.to_string().parse::<PartKey>().unwrap(), key, "{key}");
        }
        for key in [
            ObjKey::new(b"/object/path"),
            ObjKey::new(b"zero"),
            ObjKey::new([9; 32]),
        ] {
            assert_eq!(key.to_string().parse::<ObjKey>().unwrap(), key, "{key}");
        }
        let peer = PeerKey::new([3; 32]);
        assert_eq!(peer.to_string().parse::<PeerKey>().unwrap(), peer);
    }

    /// Text that is not a key is an error. This parse is an external boundary — a blob
    /// hash out of a URL or blob metadata, a uniffi caller, an id read back from storage —
    /// so a typo has to fail rather than become an identity of its own.
    #[test]
    fn text_that_is_not_a_key_is_rejected() {
        assert!("not_base58_hash".parse::<ObjKey>().is_err());
        assert!("".parse::<PartKey>().is_err());
        assert!("0OIl".parse::<PartKey>().is_err());
        assert!("o:".parse::<PartKey>().is_err());
        assert!("o:notbase58".parse::<PartKey>().is_err());
        assert!("z0OIl!".parse::<PartKey>().is_err());
    }

    /// The human-readable serde form is the same codec, so a stored id reads as itself and
    /// an empty string is a decode error rather than an index panic.
    #[test]
    fn human_readable_serde_reads_the_display_form() {
        let read = |text: &str| {
            PartKey::deserialize(
                serde::de::value::StrDeserializer::<serde::de::value::Error>::new(text),
            )
        };
        assert_eq!(read("/seds").unwrap(), PartKey::new("/seds"));
        assert!(read("").is_err());
        assert!(read("not_base58_hash").is_err());
    }
}
