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

    /// The key's bytes as a fixed-width 32-byte array, or an error naming its width.
    ///
    /// ADR 012 decision 1 makes keys variable-length, but the fixed-width consumers at
    /// the workspace edges — ed25519 verifying keys, `KeyhivePeerId`, keyhive archive
    /// reservations, automerge change hashes — still take a `[u8; 32]`. This is the only
    /// conversion to that width and it is fallible, because the width of a key that
    /// arrived from outside the process — a peer-delivered id, a caller-supplied
    /// argument, a key parsed out of text — is external input rather than an invariant.
    ///
    /// A key this process minted *is* a 32-byte digest by construction, and the site that
    /// knows that says so with `.expect(ERROR_IMPOSSIBLE)`; every site that cannot know it
    /// propagates this error instead of panicking.
    pub fn to_bytes32(&self) -> Res<[u8; 32]> {
        self.as_bytes()
            .try_into()
            .map_err(|_| eyre::eyre!("key is {} bytes wide, expected 32", self.0.len()))
    }
}

/// The key's text form: multibase base58btc, always (`z` followed by the bytes).
///
/// One encoding, with no specialization by key shape. A key is an arbitrary byte string
/// (ADR 012 decision 1) — a reserved name such as `/seds`, an `o:`-prefixed object part, a
/// digest, a peer id — and every one of them renders the same way, so what a key *means* is
/// never inferred from what its bytes happen to spell. `Display` and `FromStr` are exact
/// inverses, text that does not carry the prefix is rejected rather than read as a name, and
/// the `o:` scheme stays part of the key's bytes, encoded like any other prefix byte.
/// Presenting a key as human-readable text is a labelling question, not an encoding one.
impl std::fmt::Display for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // One encoding for every key: base58btc under the multibase `z` prefix. The text form
        // deliberately does not depend on what the key's bytes happen to say.
        write!(
            formatter,
            "{}",
            utils_rs::hash::encode_base58_multibase(&self.0)
        )
    }
}

impl std::fmt::Debug for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// Key text is not a multibase base58btc string
pub struct DecodeError;

impl std::str::FromStr for ByteKey {
    type Err = DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // The inverse of `Display`, and deliberately not total: this parse is an external
        // boundary — a blob hash out of a URL or blob metadata, a uniffi caller, an id read
        // back from storage — where text that is not a key has to be an error rather than a
        // fresh identity. A key is a `z`-prefixed multibase base58btc string and nothing else:
        // a name like `/seds` is a label for a key, not its text form.
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
            // One codec, both directions: the human-readable form is the key's `Display`,
            // base58btc under the multibase `z` prefix. (`FromStr` rather than
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

    /// One encoding for every key: base58btc under the multibase `z` prefix, whatever the
    /// key's bytes happen to spell. The `o:` scheme and the reserved `/…` names are part of
    /// a key's bytes, not a display convention, so they are encoded like any other prefix
    /// byte and no key can be mistaken for raw text.
    #[test]
    fn every_key_renders_as_one_multibase_encoding() {
        for key in [
            PartKey::new("/seds"),
            PartKey::new("/drawer/plans"),
            PartKey::new(b"o:/object/path"),
            PartKey::new(b"o:\x00\x01"),
            PartKey::new([4; 32]),
        ] {
            let text = key.to_string();
            assert!(text.starts_with('z'), "{text} is not multibase base58btc");
            assert!(
                !text.starts_with('/') && !text.starts_with("o:"),
                "{text} reads as text rather than as an encoded key"
            );
            assert_eq!(text.parse::<PartKey>().expect("own text form parses"), key);
        }
        let digest = ObjKey::new([7; 32]);
        assert!(digest.to_string().starts_with('z'), "{digest}");
        // An object part keeps its `o:` scheme only *inside* the bytes: the text form encodes
        // `o:` plus the digest, so decoding recovers both halves rather than a digest alone.
        let mut part_bytes = b"o:".to_vec();
        part_bytes.extend_from_slice(digest.as_bytes());
        let part = PartKey::new(part_bytes.clone());
        assert_eq!(part.to_string().parse::<PartKey>().unwrap(), part);
        assert_eq!(part.as_bytes(), part_bytes.as_slice());
    }

    /// `Display` and `FromStr` are inverses on every key the crate constructs, including keys
    /// whose bytes are text-shaped: the text form is always the encoding of the bytes, never
    /// the bytes read as themselves.
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
        // Text-shaped keys are no longer special: a name is a label, not a text form.
        assert!("/seds".parse::<PartKey>().is_err());
        assert!("/object/path".parse::<ObjKey>().is_err());
        assert!("o:/object/path".parse::<PartKey>().is_err());
        assert!("o:z3abc".parse::<PartKey>().is_err());
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
        let seds = PartKey::new("/seds");
        assert_eq!(read(&seds.to_string()).unwrap(), seds);
        assert!(read("/seds").is_err(), "a label is not a text form");
        assert!(read("").is_err());
        assert!(read("not_base58_hash").is_err());
    }
}
