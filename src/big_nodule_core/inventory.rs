//! Standalone nodule admission and inventory over [`Plug`] envelopes (ADR 027 §2–4, §6–8).
//!
//! [`Inventory`] validates the carrying [`Plug`] envelope, extracts its [`Nodule`] contribution,
//! checks the contribution schema and nodule admission rules, verifies that every artifact a
//! routine names is declared in the envelope, enforces authority uniqueness across installed
//! nodules and authority stability across revisions of the same [`PlugId`], and tracks standalone
//! enablement, configuration, artifact availability, and readiness.

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
/// Without a blob store wired into this standalone inventory, every artifact a routine references
/// is recorded from the envelope metadata and remains [`Self::Unavailable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactState {
    Unavailable,
}

/// Why an [`InstalledNodule`] cannot yet execute routines on a standalone node, in priority order:
/// refused configuration first, then disabled, then missing routine artifact bytes, then the
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
    #[error("plug `{id}` routine requires artifact `{artifact}` not declared in plug envelope")]
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

/// In-memory standalone inventory of admitted nodules, keyed by [`PlugId`].
#[derive(Debug, Clone, Default)]
pub struct Inventory {
    nodules: BTreeMap<PlugId, InstalledNodule>,
}

impl Inventory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates `plug`, admits its nodule contribution, checks artifact references and authority
    /// ownership, and installs or upgrades the entry for `plug.id`.
    pub fn admit(&mut self, plug: &Plug) -> Result<&InstalledNodule> {
        let (nodule, admitted, artifacts) = admit_plug_nodule(plug)?;

        for (existing_plug, existing) in &self.nodules {
            if existing_plug != &plug.id
                && (existing.nodule.owns(&nodule.id) || nodule.owns(&existing.nodule.id))
            {
                return Err(InventoryError::AuthorityConflict {
                    id: plug.id.clone(),
                    nodule_id: nodule.id,
                    existing_plug: Box::new(existing_plug.clone()),
                    existing_nodule: existing.nodule.id.clone(),
                });
            }
        }

        let revision = plug.revision();
        let (enabled, config, configured) = if let Some(existing) = self.nodules.get(&plug.id) {
            if existing.version == plug.version && existing.revision != revision {
                return Err(InventoryError::Plug(PlugError::Conflict {
                    id: plug.id.clone(),
                    version: Box::new(plug.version.clone()),
                    existing: existing.revision,
                    incoming: revision,
                }));
            }
            if existing.nodule.id != nodule.id {
                return Err(InventoryError::AuthorityChanged {
                    id: plug.id.clone(),
                    existing: existing.nodule.id.clone(),
                    incoming: nodule.id,
                });
            }
            if existing.configured {
                if nodule.config.is_none() {
                    return Err(InventoryError::NoConfigSchema(plug.id.clone()));
                }
                let settings = existing
                    .config
                    .as_ref()
                    .expect("configured nodule carries settings");
                nodule.validate_config(settings, "config")?;
                (existing.enabled, Some(settings.clone()), true)
            } else {
                let defaults = nodule
                    .config
                    .as_ref()
                    .and_then(|config| config.defaults.clone());
                (existing.enabled, defaults, false)
            }
        } else {
            let defaults = nodule
                .config
                .as_ref()
                .and_then(|config| config.defaults.clone());
            (false, defaults, false)
        };

        let installed = InstalledNodule {
            plug_id: plug.id.clone(),
            version: plug.version.clone(),
            revision,
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        };
        self.nodules.insert(plug.id.clone(), installed);
        Ok(self.nodules.get(&plug.id).expect("just inserted"))
    }

    pub fn get(&self, id: &PlugId) -> Option<&InstalledNodule> {
        self.nodules.get(id)
    }

    pub fn remove(&mut self, id: &PlugId) -> Option<InstalledNodule> {
        self.nodules.remove(id)
    }

    pub fn clear(&mut self) {
        self.nodules.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PlugId, &InstalledNodule)> {
        self.nodules.iter()
    }

    /// Validates `settings` against the installed nodule's config schema and records them as the
    /// active runtime configuration.
    pub fn configure(
        &mut self,
        id: &PlugId,
        settings: IndexMap<String, Value>,
    ) -> Result<&InstalledNodule> {
        let Some(installed) = self.nodules.get_mut(id) else {
            return Err(InventoryError::UnknownPlug(id.clone()));
        };
        if installed.nodule.config.is_none() {
            return Err(InventoryError::NoConfigSchema(id.clone()));
        }
        installed.nodule.validate_config(&settings, "config")?;
        installed.config = Some(settings);
        installed.configured = true;
        Ok(installed)
    }

    pub fn set_enabled(&mut self, id: &PlugId, enabled: bool) -> Result<&InstalledNodule> {
        let Some(installed) = self.nodules.get_mut(id) else {
            return Err(InventoryError::UnknownPlug(id.clone()));
        };
        installed.enabled = enabled;
        Ok(installed)
    }
}
