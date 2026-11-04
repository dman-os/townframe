//! Nodule admission over [`Plug`] envelopes (ADR 027 §2–4, §6–8).
//!
//! [`admit_plug_nodule`] validates the carrying [`Plug`] envelope, extracts its [`Nodule`]
//! contribution, checks the contribution schema and nodule admission rules, and verifies that
//! every artifact the handler names is declared in the envelope. [`InstalledNodule`] is the admitted
//! result that [`crate::config::NoduleConfig`] resolves, with its enablement, configuration,
//! artifact availability, and readiness.

use crate::Admitted;
use crate::Nodule;
use crate::NoduleError;
use big_cabinet_core::datum::DatumMember;
use big_plugs_core::Artifact;
use big_plugs_core::Plug;
use big_plugs_core::PlugError;
use big_plugs_core::PlugId;
use big_plugs_core::Revision;
use big_plugs_core::Version;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// Availability of an artifact declared in the carrying [`Plug`] envelope.
///
/// Without a blob store wired into this standalone inventory, every artifact the handler references
/// is recorded from the envelope metadata and remains [`Self::Unavailable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactState {
    Unavailable,
}

/// Why an [`InstalledNodule`] cannot yet serve its handler on a standalone node, in priority order:
/// refused configuration first, then disabled, then missing handler artifact bytes, then the
/// absence of a Wasm executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Readiness {
    Refused,
    Disabled,
    ArtifactsUnavailable,
    ExecutorUnavailable,
}

impl Readiness {
    pub fn is_ready(self) -> bool {
        false
    }
}

/// An admitted nodule installed in a standalone [`Inventory`].
#[derive(Debug, Clone, PartialEq)]
pub struct InstalledNodule {
    pub plug_id: PlugId,
    pub version: Version,
    pub revision: Revision,
    pub nodule: Nodule,
    pub admitted: Admitted,
    pub artifacts: BTreeMap<String, (Artifact, ArtifactState)>,
    pub enabled: bool,
    pub config: Option<IndexMap<String, Value>>,
    configured: bool,
}

impl InstalledNodule {
    #[expect(
        clippy::too_many_arguments,
        reason = "crate-private constructor assembling validated fields"
    )]
    pub(crate) fn new(
        plug_id: PlugId,
        version: Version,
        revision: Revision,
        nodule: Nodule,
        admitted: Admitted,
        artifacts: BTreeMap<String, (Artifact, ArtifactState)>,
        enabled: bool,
        config: Option<IndexMap<String, Value>>,
        configured: bool,
    ) -> Self {
        Self {
            plug_id,
            version,
            revision,
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        }
    }

    /// Whether an operator has set `config` explicitly. Unconfigured settings are the selected
    /// revision's defaults and follow an upgrade; configured settings are kept and re-validated.
    pub fn is_configured(&self) -> bool {
        self.configured
    }

    pub fn readiness(&self) -> Readiness {
        if !self.enabled {
            Readiness::Disabled
        } else if self
            .artifacts
            .values()
            .any(|(_, state)| *state == ArtifactState::Unavailable)
        {
            Readiness::ArtifactsUnavailable
        } else {
            Readiness::ExecutorUnavailable
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error(transparent)]
    Plug(#[from] PlugError),
    #[error(transparent)]
    Nodule(#[from] NoduleError),
    #[error("plug `{0}` carries no nodule contribution")]
    MissingContribution(PlugId),
    #[error("plug `{id}` nodule contribution has unsupported schema `{schema}`")]
    UnsupportedSchema { id: PlugId, schema: String },
    #[error("plug `{id}` nodule contribution is not a valid nodule datum: {reason}")]
    InvalidContribution { id: PlugId, reason: String },
    #[error("plug `{id}` handler requires artifact `{artifact}` not declared in plug envelope")]
    MissingArtifact { id: PlugId, artifact: String },
    #[error(
        "nodule `{nodule_id}` in plug `{id}` conflicts with authority `{existing_nodule}` owned by `{existing_plug}`"
    )]
    AuthorityConflict {
        id: PlugId,
        nodule_id: String,
        existing_plug: Box<PlugId>,
        existing_nodule: String,
    },
    #[error("plug `{id}` cannot change nodule authority from `{existing}` to `{incoming}`")]
    AuthorityChanged {
        id: PlugId,
        existing: String,
        incoming: String,
    },
    #[error("nodule for plug `{0}` is not installed")]
    UnknownPlug(PlugId),
    #[error("nodule for plug `{0}` declares no config schema")]
    NoConfigSchema(PlugId),
}

type Result<T> = std::result::Result<T, InventoryError>;

pub(crate) type AdmittedContribution = (
    Nodule,
    Admitted,
    BTreeMap<String, (Artifact, ArtifactState)>,
);

pub(crate) fn admit_plug_nodule(plug: &Plug) -> Result<AdmittedContribution> {
    plug.validate()?;
    let Some(contribution) = plug.nodule() else {
        return Err(InventoryError::MissingContribution(plug.id.clone()));
    };
    if contribution.schema != Nodule::ID {
        return Err(InventoryError::UnsupportedSchema {
            id: plug.id.clone(),
            schema: contribution.schema.clone(),
        });
    }
    let payload = Value::Object(
        contribution
            .description
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );
    let nodule: Nodule =
        serde_json::from_value(payload).map_err(|err| InventoryError::InvalidContribution {
            id: plug.id.clone(),
            reason: err.to_string(),
        })?;
    let admitted = nodule.admit()?;
    let mut artifacts = BTreeMap::new();
    for key in &admitted.artifacts {
        let Some(artifact) = plug.artifact(key) else {
            return Err(InventoryError::MissingArtifact {
                id: plug.id.clone(),
                artifact: key.clone(),
            });
        };
        artifacts.insert(key.clone(), (artifact.clone(), ArtifactState::Unavailable));
    }
    Ok((nodule, admitted, artifacts))
}
