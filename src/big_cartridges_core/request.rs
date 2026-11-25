//! Shared open vocabularies used by cartridge-level permbinds.

/// Whether an access is needed for the cartridge to start.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Necessity {
    #[default]
    Required,
    Optional,
    #[datum(other)]
    Unknown(String),
}

impl Necessity {
    pub(crate) fn is_default(&self) -> bool {
        *self == Self::Required
    }

    pub fn is_required(&self) -> bool {
        !matches!(self, Self::Optional)
    }
}

/// How a drawer set is selected.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Resolution {
    #[default]
    Configured,
    AtRuntime,
    #[datum(other)]
    Unknown(String),
}

impl Resolution {
    pub(crate) fn is_default(&self) -> bool {
        *self == Self::Configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_vocabularies_round_trip_unknown_members() {
        let necessity: Necessity = serde_json::from_str("\"later\"").unwrap();
        assert_eq!(necessity, Necessity::Unknown("later".into()));
        assert_eq!(serde_json::to_string(&necessity).unwrap(), "\"later\"");

        let resolution: Resolution = serde_json::from_str("\"picker\"").unwrap();
        assert_eq!(resolution, Resolution::Unknown("picker".into()));
        assert_eq!(serde_json::to_string(&resolution).unwrap(), "\"picker\"");
    }
}
