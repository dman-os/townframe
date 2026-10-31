use big_sync_core::ObjKey;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId([u8; 32]);

impl BlobId {
    #[must_use]
    pub const fn new(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn to_bytes32(&self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn digest_str(&self) -> String {
        let mut multihash = [0u8; 34];
        multihash[..2].copy_from_slice(&[0x1e, 0x20]);
        multihash[2..].copy_from_slice(&self.0);
        format!("z{}", bs58::encode(multihash).into_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("blob id is neither a BLAKE3 multihash nor a 32-byte base58btc multibase digest")]
pub struct BlobIdDecodeError;

impl From<BlobId> for ObjKey {
    fn from(blob_id: BlobId) -> Self {
        Self::new(blob_id.0)
    }
}

impl TryFrom<&ObjKey> for BlobId {
    type Error = BlobIdDecodeError;

    fn try_from(key: &ObjKey) -> Result<Self, Self::Error> {
        key.as_bytes()
            .try_into()
            .map(Self)
            .map_err(|_| BlobIdDecodeError)
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "z{}", bs58::encode(self.0).into_string())
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl FromStr for BlobId {
    type Err = BlobIdDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let encoded = value.strip_prefix('z').ok_or(BlobIdDecodeError)?;
        if encoded.len() > 46 {
            return Err(BlobIdDecodeError);
        }
        let mut bytes = [0u8; 34];
        let length = bs58::decode(encoded)
            .onto(&mut bytes[..])
            .map_err(|_| BlobIdDecodeError)?;
        let digest = match &bytes[..length] {
            digest @ [..] if digest.len() == 32 => digest,
            [0x1e, 0x20, digest @ ..] if digest.len() == 32 => digest,
            _ => return Err(BlobIdDecodeError),
        };
        Ok(Self(digest.try_into().map_err(|_| BlobIdDecodeError)?))
    }
}

impl Serialize for BlobId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for BlobId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            return String::deserialize(deserializer)?
                .parse()
                .map_err(serde::de::Error::custom);
        }
        struct DigestVisitor;

        impl<'de> serde::de::Visitor<'de> for DigestVisitor {
            type Value = BlobId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("exactly 32 digest bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<BlobId, E> {
                bytes
                    .try_into()
                    .map(BlobId)
                    .map_err(|_| E::invalid_length(bytes.len(), &self))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<BlobId, A::Error> {
                let mut digest = [0u8; 32];
                for (index, byte) in digest.iter_mut().enumerate() {
                    *byte = sequence
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(index, &self))?;
                }
                if sequence.next_element::<u8>()?.is_some() {
                    return Err(serde::de::Error::invalid_length(33, &self));
                }
                Ok(BlobId(digest))
            }
        }

        deserializer.deserialize_bytes(DigestVisitor)
    }
}
