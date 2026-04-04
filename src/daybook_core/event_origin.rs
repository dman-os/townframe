#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum SwitchEventOrigin {
    Local { actor_id: String },
    Remote { peer_id: String },
    Bootstrap,
}

impl SwitchEventOrigin {
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}
