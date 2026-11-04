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
/// Admission records every artifact the handler references as [`Self::Unavailable`] from the
/// envelope metadata alone. Only a node's executor, which loads the bytes, can establish
/// [`Self::Available`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactState {
    Unavailable,
    Available,
}

/// Whether an [`InstalledNodule`] can serve its handler, and if not the first reason in priority
/// order: refused configuration, then disabled, then a required operation request no grant
/// covers, then a required operation request no provider satisfies, then missing handler
/// artifact bytes, then the
/// absence of a handler executor, then initialization. [`Self::Ready`] is the executor's word
/// that the handler can be called now and the node's word that every initializer of this
/// revision and start has succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Readiness {
    Ready,
    Refused,
    Disabled,
    /// A required operation request has no covering grant. Not a fault: configuration (granting
    /// the operation) makes the nodule ready. An optional request never causes this.
    NeedsConfiguration,
    /// A required operation request has no provider that satisfies it: none enabled, more than
    /// one, a revision older than the request's `minRevision`, or a different plug than the one
    /// the operator consented to. The record's `requests` say which and why. Not a fault: the
    /// nodule is enabled and waits (as a Kubernetes pod waits on a missing dependency, a systemd
    /// unit on `Requires=`), and becomes ready once a provider appears or the operator confirms.
    /// An optional request never causes this.
    ProviderMissing,
    ArtifactsUnavailable,
    ExecutorUnavailable,
    /// The handler is hosted but an initializer of this revision and start is still running or
    /// waiting on the provider of a foreign operation. Retryable: callers are not queued.
    Initializing,
    /// An initializer failed; the record carries the error. It runs again on the next reconcile
    /// of the node (boot, config change, enable).
    InitFailed,
}

impl Readiness {
    pub fn is_ready(self) -> bool {
        self == Self::Ready
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

    /// Each declared artifact's availability, by key.
    pub fn artifact_states(&self) -> BTreeMap<String, ArtifactState> {
        self.artifacts
            .iter()
            .map(|(key, (_, state))| (key.clone(), *state))
            .collect()
    }

    /// Whether an operator has set `config` explicitly. Unconfigured settings are the selected
    /// revision's defaults and follow an upgrade; configured settings are kept and re-validated.
    pub fn is_configured(&self) -> bool {
        self.configured
    }

    /// Readiness as the inventory alone can tell it, which is what a node without an executor
    /// reports. An enabled nodule with a handler never yields [`Readiness::Ready`] here, since
    /// only an executor can serve a handler; one without a handler hosts nothing (admission
    /// refuses `implements` without a handler), so there is nothing for it to wait on.
    pub fn readiness(&self) -> Readiness {
        if !self.enabled {
            Readiness::Disabled
        } else if self.nodule.handler.is_none() {
            Readiness::Ready
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
        "plug `{id}` handler artifact `{artifact}` declares no size, which a node needs to \
         reference and fetch its bytes"
    )]
    ArtifactSizeRequired { id: PlugId, artifact: String },
    #[error(
        "plug `{id}` holds authority `{authority}`, which overlaps `{existing_authority}` held by `{existing_plug}`"
    )]
    AuthorityConflict {
        id: PlugId,
        authority: String,
        existing_plug: Box<PlugId>,
        existing_authority: String,
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
    let admitted = nodule.admit(plug.authority.as_deref())?;
    let mut artifacts = BTreeMap::new();
    for key in &admitted.artifacts {
        let Some(artifact) = plug.artifact(key) else {
            return Err(InventoryError::MissingArtifact {
                id: plug.id.clone(),
                artifact: key.clone(),
            });
        };
        if artifact.size.is_none() {
            return Err(InventoryError::ArtifactSizeRequired {
                id: plug.id.clone(),
                artifact: key.clone(),
            });
        }
        artifacts.insert(key.clone(), (artifact.clone(), ArtifactState::Unavailable));
    }
    Ok((nodule, admitted, artifacts))
}
