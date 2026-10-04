//! Installed-lens identity (ADR 012 §3: "lens identity/version and relevant
//! configuration"). An identity resolves against the node's plug manifest;
//! selection records it verbatim so provenance stays meaningful across
//! upgrades.

use serde::{Deserialize, Serialize};

/// A lens version. Semantic in intent, lexicographic in comparison: ordered
/// versions pick the configured default deterministically, and *installation*
/// of a newer version never changes the selected one (ADR 012 §4 — upgrades
/// are explicit).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct LensVersion(pub String);

/// The installed lens a proposal/recipe names. `config_digest` pins validated
/// lens parameters (`None` when unparameterized) so recipes and provenance are
/// meaningful: the same lens with different parameters is a different recipe.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct LensIdentity {
    /// The registered plug (ADR 007 manifests-as-drawer-docs).
    pub plug_id: String,
    /// Stable lens name within the plug.
    pub lens_name: String,
    pub version: LensVersion,
    pub config_digest: Option<[u8; 32]>,
}

impl LensIdentity {
    /// The `(plug_id, lens_name)` pair that identifies the lens across
    /// versions/configurations: overrides and defaults name it, selection
    /// then records the exact installed version.
    pub fn name(&self) -> (&str, &str) {
        (&self.plug_id, &self.lens_name)
    }
}

impl core::fmt::Display for LensIdentity {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.config_digest {
            Some(digest) => write!(
                formatter,
                "{}/{}/{}@{}",
                self.plug_id,
                self.lens_name,
                self.version.0,
                hex_digest(&digest)
            ),
            None => write!(formatter, "{}/{}/{}", self.plug_id, self.lens_name, self.version.0),
        }
    }
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02X}")).collect()
}