//! Nodule admission over [`Cartridge`] envelopes (ADR 027 §2–4, §6–8).
//!
//! [`admit_cartridge_nodule`] validates the carrying [`Cartridge`] envelope, extracts its [`Nodule`]
//! contribution, checks the contribution schema and nodule admission rules, and verifies that
//! every artifact the handler names is declared in the envelope. [`InstalledNodule`] is the admitted
//! result that [`crate::config::NoduleConfig`] resolves, with its enablement, artifact availability,
//! and readiness.

use crate::Admitted;
use crate::Nodule;
use crate::NoduleError;
use big_cabinet_core::datum::DatumMember;
use big_cartridges_core::Artifact;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeError;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;
use big_cartridges_core::config::Component;
use big_dispatch_core::Readiness;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// Availability of an artifact declared in the carrying [`Cartridge`] envelope.
///
/// Admission records every artifact the handler references as [`Self::Unavailable`] from the
/// envelope metadata alone. Only a node's executor, which loads the bytes, can establish
/// [`Self::Available`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactState {
    Unavailable,
    Available,
}

/// The cartridge whose code fills a slot under a dev takeover, and the version of it that does.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilledBy {
    pub cartridge_id: CartridgeId,
    pub version: Version,
}

/// An admitted nodule installed in a standalone [`Inventory`].
#[derive(Debug, Clone, PartialEq)]
pub struct InstalledNodule {
    pub cartridge_id: CartridgeId,
    pub version: Version,
    pub revision: Revision,
    pub nodule: Nodule,
    pub admitted: Admitted,
    pub artifacts: BTreeMap<String, (Artifact, ArtifactState)>,
    pub enabled: bool,
    /// Set when a dev takeover fills this slot: `cartridge_id` is the slot, while `version`,
    /// `revision`, `nodule`, `admitted` and `artifacts` are the filler's.
    pub filled_by: Option<FilledBy>,
}

impl InstalledNodule {
    pub(crate) fn new(
        cartridge_id: CartridgeId,
        version: Version,
        revision: Revision,
        nodule: Nodule,
        admitted: Admitted,
        artifacts: BTreeMap<String, (Artifact, ArtifactState)>,
        enabled: bool,
    ) -> Self {
        Self {
            cartridge_id,
            version,
            revision,
            nodule,
            admitted,
            artifacts,
            enabled,
            filled_by: None,
        }
    }

    /// Each declared artifact's availability, by key.
    pub fn artifact_states(&self) -> BTreeMap<String, ArtifactState> {
        self.artifacts
            .iter()
            .map(|(key, (_, state))| (key.clone(), *state))
            .collect()
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
    Cartridge(#[from] CartridgeError),
    #[error(transparent)]
    Nodule(#[from] NoduleError),
    #[error("cartridge `{0}` carries no nodule contribution")]
    MissingContribution(CartridgeId),
    #[error("cartridge `{id}` nodule contribution has unsupported schema `{schema}`")]
    UnsupportedSchema { id: CartridgeId, schema: String },
    #[error("cartridge `{id}` nodule contribution is not a valid nodule datum: {reason}")]
    InvalidContribution { id: CartridgeId, reason: String },
    #[error(
        "cartridge `{id}` handler requires artifact `{artifact}` not declared in cartridge envelope"
    )]
    MissingArtifact { id: CartridgeId, artifact: String },
    #[error(
        "cartridge `{id}` handler artifact `{artifact}` declares no size, which a node needs to \
         reference and fetch its bytes"
    )]
    ArtifactSizeRequired { id: CartridgeId, artifact: String },
    #[error(
        "cartridge `{id}` holds authority `{authority}`, which overlaps `{existing_authority}` held by `{existing_cartridge}`"
    )]
    AuthorityConflict {
        id: CartridgeId,
        authority: String,
        existing_cartridge: Box<CartridgeId>,
        existing_authority: String,
    },
    #[error(
        "cartridge `{id}` carries Lexicon `{nsid}` in a copy that conflicts with the one carried by \
         `{other}`: copies must be identical unless their declared revisions differ, and a \
         vendored copy never out-revises the authority's"
    )]
    LexiconConflict {
        id: CartridgeId,
        nsid: String,
        other: Box<CartridgeId>,
    },
    #[error("cannot take over `{id}`: {reason}")]
    InvalidDevTakeover { id: CartridgeId, reason: String },
    #[error("invalid method route `{pattern}`: {reason}")]
    InvalidRoute { pattern: String, reason: String },
    #[error("nodule for cartridge `{0}` is not installed")]
    UnknownCartridge(CartridgeId),
    #[error(
        "cartridge `{0}` contributes neither a nodule nor a micro, so there is nothing to install"
    )]
    NothingToInstall(CartridgeId),
    #[error("invalid grant `{grant}`: {reason}")]
    InvalidGrant { grant: String, reason: String },
    #[error("cartridge `{id}` has no `{component}` component to {action}")]
    NoComponent {
        id: CartridgeId,
        component: Component,
        action: &'static str,
    },
}

type Result<T> = std::result::Result<T, InventoryError>;

pub(crate) type AdmittedContribution = (
    Nodule,
    Admitted,
    BTreeMap<String, (Artifact, ArtifactState)>,
);

pub(crate) fn admit_cartridge_nodule(cartridge: &Cartridge) -> Result<AdmittedContribution> {
    cartridge.validate()?;
    let Some(contribution) = cartridge.nodule() else {
        return Err(InventoryError::MissingContribution(cartridge.id.clone()));
    };
    if contribution.schema != Nodule::ID {
        return Err(InventoryError::UnsupportedSchema {
            id: cartridge.id.clone(),
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
            id: cartridge.id.clone(),
            reason: err.to_string(),
        })?;
    let admitted = nodule.admit(cartridge.authority.as_deref())?;
    let mut artifacts = BTreeMap::new();
    for key in &admitted.artifacts {
        let Some(artifact) = cartridge.artifact(key) else {
            return Err(InventoryError::MissingArtifact {
                id: cartridge.id.clone(),
                artifact: key.clone(),
            });
        };
        if artifact.size.is_none() {
            return Err(InventoryError::ArtifactSizeRequired {
                id: cartridge.id.clone(),
                artifact: key.clone(),
            });
        }
        artifacts.insert(key.clone(), (artifact.clone(), ArtifactState::Unavailable));
    }
    Ok((nodule, admitted, artifacts))
}
