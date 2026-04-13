use crate::interlude::*;

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct DocId32([u8; 32]);

impl DocId32 {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn random() -> Self {
        Self(rand::random::<[u8; 32]>())
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

impl std::fmt::Display for DocId32 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", data_encoding::HEXLOWER.encode(&self.0))
    }
}

impl std::fmt::Debug for DocId32 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::str::FromStr for DocId32 {
    type Err = eyre::Report;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let raw = data_encoding::HEXLOWER
            .decode(value.as_bytes())
            .wrap_err("invalid DocId32 hex")?;
        if raw.len() != 32 {
            eyre::bail!("invalid DocId32 length: {}", raw.len());
        }
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&raw);
        Ok(Self(bytes))
    }
}

impl From<sedimentree_core::id::SedimentreeId> for DocId32 {
    fn from(value: sedimentree_core::id::SedimentreeId) -> Self {
        Self::new(*value.as_bytes())
    }
}

impl From<DocId32> for sedimentree_core::id::SedimentreeId {
    fn from(value: DocId32) -> Self {
        sedimentree_core::id::SedimentreeId::new(value.into_bytes())
    }
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PeerId32([u8; 32]);

impl PeerId32 {
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

impl std::fmt::Display for PeerId32 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", data_encoding::HEXLOWER.encode(&self.0))
    }
}

impl std::fmt::Debug for PeerId32 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::str::FromStr for PeerId32 {
    type Err = eyre::Report;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let raw = data_encoding::HEXLOWER
            .decode(value.as_bytes())
            .wrap_err("invalid PeerId32 hex")?;
        if raw.len() != 32 {
            eyre::bail!("invalid PeerId32 length: {}", raw.len());
        }
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&raw);
        Ok(Self(bytes))
    }
}

impl From<subduction_core::peer::id::PeerId> for PeerId32 {
    fn from(value: subduction_core::peer::id::PeerId) -> Self {
        Self::new(*value.as_bytes())
    }
}

impl From<PeerId32> for subduction_core::peer::id::PeerId {
    fn from(value: PeerId32) -> Self {
        subduction_core::peer::id::PeerId::new(value.into_bytes())
    }
}

#[cfg(feature = "iroh")]
impl From<iroh::PublicKey> for PeerId32 {
    fn from(value: iroh::PublicKey) -> Self {
        Self(*value.as_bytes())
    }
}

#[cfg(feature = "iroh")]
impl From<PeerId32> for iroh::PublicKey {
    fn from(value: PeerId32) -> Self {
        iroh::PublicKey::from_bytes(&value.0).expect("PeerId32 must be 32 bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_id_hex_roundtrip() -> Res<()> {
        let doc_id = DocId32::new([7_u8; 32]);
        let encoded = doc_id.to_string();
        let decoded: DocId32 = encoded.parse()?;
        assert_eq!(decoded, doc_id);
        Ok(())
    }

    #[test]
    fn peer_id_hex_roundtrip() -> Res<()> {
        let peer_id = PeerId32::new([9_u8; 32]);
        let encoded = peer_id.to_string();
        let decoded: PeerId32 = encoded.parse()?;
        assert_eq!(decoded, peer_id);
        Ok(())
    }

    #[cfg(feature = "iroh")]
    #[test]
    fn peer_id_iroh_roundtrip() {
        let peer_id = PeerId32::new([17_u8; 32]);
        let public_key: iroh::PublicKey = peer_id.into();
        let decoded = PeerId32::from(public_key);
        assert_eq!(decoded, peer_id);
    }

    #[test]
    fn doc_id_sedimentree_roundtrip() {
        let doc_id = DocId32::new([23_u8; 32]);
        let sedimentree_id: sedimentree_core::id::SedimentreeId = doc_id.into();
        let decoded = DocId32::from(sedimentree_id);
        assert_eq!(decoded, doc_id);
    }

    #[test]
    fn peer_id_subduction_roundtrip() {
        let peer_id = PeerId32::new([31_u8; 32]);
        let subduction_peer_id: subduction_core::peer::id::PeerId = peer_id.into();
        let decoded = PeerId32::from(subduction_peer_id);
        assert_eq!(decoded, peer_id);
    }
}
