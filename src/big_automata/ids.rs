use crate::interlude::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ByteKey {
    Inline { len: u8, buf: [u8; 32] },
    Heap(Arc<[u8]>),
}

impl ByteKey {
    pub fn new(bytes: &[u8]) -> Self {
        let len = bytes.len();
        if len > 32 {
            Self::Heap(bytes.into())
        } else {
            let mut buf = [0u8; 32];
            buf.copy_from_slice(bytes);
            Self::Inline { len: len as _, buf }
        }
    }

    pub fn rand32() -> Self {
        Self::Inline {
            len: 32,
            buf: rand::random::<[u8; 32]>(),
        }
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self {
            ByteKey::Inline { len, buf: bytes } => &bytes[0..*len as _],
            ByteKey::Heap(items) => &items,
        }
    }

    #[must_use]
    pub fn into_bytes(self) -> Arc<[u8]> {
        match self {
            ByteKey::Inline { len, buf: bytes } => (&bytes[0..len as _]).into(),
            ByteKey::Heap(items) => items,
        }
    }
}

impl std::fmt::Display for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // One encoding for every key: base58btc under the multibase `z` prefix. The text form
        // deliberately does not depend on what the key's bytes happen to say.
        write!(
            formatter,
            "{}",
            utils_rs::hash::encode_base58_multibase(&self.as_bytes())
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
        let bytes = utils_rs::hash::decode_base58_multibase(value).map_err(|_| DecodeError)?;
        let len = bytes.len();
        if len > 32 {
            Ok(Self::Heap(bytes.into()))
        } else {
            let mut buf = [0u8; 32];
            buf.copy_from_slice(&bytes);
            Ok(Self::Inline { len: len as _, buf })
        }
    }
}

impl Serialize for ByteKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            utils_rs::hash::encode_base58_multibase(&self.as_bytes()).serialize(serializer)
        } else {
            serializer.serialize_bytes(&self.as_bytes())
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
            deserializer
                .deserialize_bytes(MyVisitor)
                .map(|buf| Self::new(&buf))
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
            pub fn new(bytes: &[u8]) -> Self {
                Self(ByteKey::new(bytes))
            }

            pub fn rand32() -> Self {
                Self(ByteKey::rand32())
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

pub type BuckLevel = u8;
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

    #[inline]
    #[must_use]
    pub fn deepest_from_obj_key(obj_key: &ObjKey) -> Self {
        let hash = blake3::hash(obj_key.as_bytes());
        let bytes = hash.as_bytes();
        Self::new(Self::MAX_LEVEL, u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    #[inline]
    #[must_use]
    pub fn from_obj_key(level: BuckLevel, obj_key: &ObjKey) -> Self {
        debug_assert!(level <= Self::MAX_LEVEL);
        Self::deepest_from_obj_key(obj_key).to_level(level)
    }

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

    #[test]
    fn every_key_renders_as_one_multibase_encoding() {
        for key in [
            PartKey::new(b"/seds"),
            PartKey::new(b"/drawer/plans"),
            PartKey::new(b"o:/object/path"),
            PartKey::new(b"o:\x00\x01"),
            PartKey::new(&[4; 32]),
        ] {
            let text = key.to_string();
            assert!(text.starts_with('z'), "{text} is not multibase base58btc");
            assert!(
                !text.starts_with('/') && !text.starts_with("o:"),
                "{text} reads as text rather than as an encoded key"
            );
            assert_eq!(text.parse::<PartKey>().expect("own text form parses"), key);
        }
        let digest = ObjKey::new(&[7; 32]);
        assert!(digest.to_string().starts_with('z'), "{digest}");
        let mut part_bytes = b"o:".to_vec();
        part_bytes.extend_from_slice(digest.as_bytes());
        let part = PartKey::new(&part_bytes);
        assert_eq!(part.to_string().parse::<PartKey>().unwrap(), part);
        assert_eq!(part.as_bytes(), part_bytes.as_slice());
    }

    #[test]
    fn keys_round_trip_through_their_text_form() {
        for key in [
            PartKey::new(b"/seds"),
            PartKey::new(b"/binary\xff\x00"),
            PartKey::new(b"o:zero"),
            PartKey::new(b"o:\x00\x01"),
        ] {
            assert_eq!(key.to_string().parse::<PartKey>().unwrap(), key, "{key}");
        }
        for key in [
            ObjKey::new(b"/object/path"),
            ObjKey::new(b"zero"),
            ObjKey::new(&[9; 32]),
        ] {
            assert_eq!(key.to_string().parse::<ObjKey>().unwrap(), key, "{key}");
        }
        let peer = PeerKey::new(&[3; 32]);
        assert_eq!(peer.to_string().parse::<PeerKey>().unwrap(), peer);
    }

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

    #[test]
    fn human_readable_serde_reads_the_display_form() {
        let read = |text: &str| {
            PartKey::deserialize(
                serde::de::value::StrDeserializer::<serde::de::value::Error>::new(text),
            )
        };
        let seds = PartKey::new(b"/seds");
        assert_eq!(read(&seds.to_string()).unwrap(), seds);
        assert!(read("/seds").is_err(), "a label is not a text form");
        assert!(read("").is_err());
        assert!(read("not_base58_hash").is_err());
    }
}
