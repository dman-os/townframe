// FIXME: consdier using u64 or u128 for ObjIds since they'll
// be repo scoped

use crate::{interlude::*, rpc::BuckLevel};

macro_rules! alias_byte32id {
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $name(pub Byte32Id);
        impl std::ops::Deref for $name {
            type Target = Byte32Id;

            fn deref(&self) -> &Self::Target {
                &self.0
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
    };
}

alias_byte32id!(PartId);
alias_byte32id!(ObjId);
alias_byte32id!(PeerId);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Byte32Id([u8; 32]);

impl Byte32Id {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
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

impl std::fmt::Display for Byte32Id {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // FIXME: use fixed size stack buffer to write string onto and then write that onto the
        // formatter
        write!(formatter, "{}", bs58::encode(&self.0).into_string())
    }
}

impl std::fmt::Debug for Byte32Id {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// Error decoding bs58 string
pub struct DecodeError;

impl std::str::FromStr for Byte32Id {
    type Err = DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes: [u8; 32] = bs58::decode(value.as_bytes())
            .into_array_const()
            .map_err(|_| DecodeError)?;
        Ok(Self(bytes))
    }
}

impl Serialize for Byte32Id {
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

impl<'de> serde::Deserialize<'de> for Byte32Id {
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
            deserializer.deserialize_str(MyVisitor).map(|buf| Self(buf))
        }
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

    #[inline]
    pub fn from_obj_id(level: BuckLevel, obj_id: &ObjId) -> Self {
        debug_assert!(level <= Self::MAX_LEVEL);
        let l4_index = u16::from_be_bytes([obj_id.0 .0[0], obj_id.0 .0[1]]);
        Self::new(4, l4_index).to_level(level)
    }

    #[inline]
    pub fn increment(&self) -> Self {
        if self.level() == Self::MAX_LEVEL {
            return *self;
        }
        if self.index() == u16::MAX {
            BuckId::new(self.level() + 1, 0)
        } else {
            BuckId::new(self.level(), self.index() + 1)
        }
    }
}
