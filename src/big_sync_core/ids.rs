// FIXME: consdier using u64 or u128 for ObjKeys since they'll
// be repo scoped

use crate::interlude::*;
use crate::rpc::BuckLevel;

macro_rules! alias_byte_key {
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $name(pub ByteKey);
        impl std::ops::Deref for $name {
            type Target = ByteKey;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl $name {
            #[must_use]
            pub const fn new(bytes: [u8; 32]) -> Self {
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
                reconciler.bytes(self.0.0)
            }
        }

        #[cfg(feature = "automerge")]
        impl autosurgeon::Hydrate for $name {
            fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
                if bytes.len() != 32 {
                    return Err(autosurgeon::HydrateError::unexpected(
                        "version tag in 32 length byte array",
                        format!("version tags has byte length of {}", bytes.len()),
                    ));
                }
                let mut buf = [0_u8; 32];
                buf.copy_from_slice(&bytes[0..32]);
                Ok(Self(ByteKey(buf)))
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
    /// it cannot collide with an unrelated part. The reserved key spaces of decision 1
    /// make this expressible as the literal `o:{object_key}`, but that needs keys to be
    /// variable-length; while they are still fixed 32-byte values the derivation is a
    /// domain-separated digest, which is computable from the key and collision-free
    /// against unrelated parts just the same.
    #[must_use]
    pub fn object_part_key(self) -> PartKey {
        let mut bytes = b"townframe/big-sync/object-part/v1".to_vec();
        bytes.extend_from_slice(self.as_bytes());
        PartKey::new(*blake3::hash(&bytes).as_bytes())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteKey([u8; 32]);

impl ByteKey {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn random() -> Self {
        Self(rand::random())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl std::fmt::Display for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // FIXME: use fixed size stack buffer to write string onto and then write that onto the
        // formatter
        write!(formatter, "{}", bs58::encode(&self.0).into_string())
    }
}

impl std::fmt::Debug for ByteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// Error decoding bs58 string
pub struct DecodeError;

impl std::str::FromStr for ByteKey {
    type Err = DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes: [u8; 32] = bs58::decode(value.as_bytes())
            .into_array_const()
            .map_err(|_| DecodeError)?;
        Ok(Self(bytes))
    }
}

impl Serialize for ByteKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            utils_rs::hash::encode_base58_multibase(self.0).serialize(serializer)
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
            let mut buf = [0u8; 32];
            utils_rs::hash::decode_base58_multibase_onto(&str, &mut buf)
                .map_err(serde::de::Error::custom)?;
            Ok(Self(buf))
        } else {
            struct MyVisitor;
            impl<'de> serde::de::Visitor<'de> for MyVisitor {
                type Value = [u8; 32];

                fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
                    fmt.write_str("a 32 length byte string")
                }

                fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    if val.len() != 32 {
                        return Err(serde::de::Error::invalid_length(
                            val.len(),
                            &"32 length byte array",
                        ));
                    }
                    let mut buf = [0u8; 32];
                    buf.copy_from_slice(val);
                    Ok(buf)
                }
            }
            deserializer.deserialize_bytes(MyVisitor).map(Self)
        }
    }
}

#[cfg(feature = "automerge")]
impl autosurgeon::Reconcile for ByteKey {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(self.0)
    }
}

#[cfg(feature = "automerge")]
impl autosurgeon::Hydrate for ByteKey {
    fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
        if bytes.len() != 32 {
            return Err(autosurgeon::HydrateError::unexpected(
                "byte id in 32 length byte array",
                format!("byte string has byte length of {}", bytes.len()),
            ));
        }
        let mut buf = [0_u8; 32];
        buf.copy_from_slice(&bytes[0..32]);
        Ok(Self(buf))
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

    /// The deepest bucket an object key falls into.
    ///
    /// This inherits the bucket index from the key's leading bytes. ADR 012 decision 1
    /// makes distribution an explicitly chosen hash of the key at the point of use, so
    /// that a key's *ordering* and its *layout* stay independent — which matters as soon
    /// as keys are textual (a reserved or path-shaped key would otherwise pile every
    /// object into the ranges its prefix implies). That change rides with the
    /// variable-length key representation.
    #[inline]
    pub fn from_obj_key(level: BuckLevel, obj_key: &ObjKey) -> Self {
        debug_assert!(level <= Self::MAX_LEVEL);
        let l4_index = u16::from_be_bytes([obj_key.0.0[0], obj_key.0.0[1]]);
        Self::new(4, l4_index).to_level(level)
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
