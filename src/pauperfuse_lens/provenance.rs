//! Selection provenance (ADR 011 §5, design §4): the selected lens identity,
//! the production recipe, and the per-output render heads/byte evidence — the
//! durable record that names which lens version and configuration produced
//! existing files. Needs no separate prose log (ADR 011 §3: provenance lives
//! in checkout state).

use serde::{Deserialize, Serialize};

use crate::identity::LensIdentity;
use crate::recipe::Recipe;

/// Acknowledged byte evidence of an output's rendered state (the
/// checkout's existing length/digest currency: length and a BLAKE3 digest,
/// see `FileEvidence` in the filesystem backend).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ByteEvidence {
    pub length: u64,
    pub digest: [u8; 32],
}

/// Durable provenance of one output: its slot, its recorded materialized path,
/// the serialized heads its render was produced at, and the acknowledged bytes
/// evidence (`None` while the render state is not recorded — e.g. a project
/// whose staging did not complete).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OutputProvenance {
    /// The proposal's stable internal slot id (ADR 012 §9).
    pub slot: String,
    /// The materialized output path, exactly as recorded.
    pub path: String,
    /// Serialized commit heads the render was produced at.
    pub heads: Vec<String>,
    /// The acknowledged render bytes, when known.
    pub bytes: Option<ByteEvidence>,
}

/// Per-selection provenance: the selected lens identity + the recipe (both
/// recorded at the render heads), and one per-output record. The current
/// checkout marker v3 stores this for its single output through `projection`,
/// `renderHeads`, and the `Ready` state's length/digest — the marker schema is
/// not grown until a multi-output surface needs it (design §4.1, approved as-is).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SelectionProvenance {
    pub lens: LensIdentity,
    pub recipe: Recipe,
    pub outputs: Vec<OutputProvenance>,
}