//! Portable nodule configuration datums and the [`DatumStore`] projection over a node's config
//! document (ADR 027 §4, §6).
//!
//! Each installed nodule's desired state is stored across up to three independently addressed
//! datums keyed by its [`PlugId`] slot:
//! - [`NoduleInstall`][]: `org.example.daybook.nodule.install/<plugId>`
//! - [`NoduleSettings`][]: `org.example.daybook.nodule.settings/<plugId>`
//! - [`NoduleGrants`][]: `org.example.daybook.nodule.grants/<plugId>`
//!
//! [`NoduleConfig`] hydrates these datums from one [`DatumsRead`] frontier, preserves unknown
//! fields, unknown vocabulary values, and undecodable/rejected/pending entries so unrelated edits
//! never delete them, and resolves each nodule's desired state independently for the reconciler.

use crate::Nodule;
use crate::inventory::InstalledNodule;
use crate::inventory::InventoryError;
use crate::inventory::admit_plug_nodule;
use big_cabinet_core::DatumKey;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::doc::DatumPatch;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumStore;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::EntryAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use big_plugs_core::PlugError;
use big_plugs_core::PlugId;
use big_plugs_core::Revision;
use big_plugs_core::Version;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// `org.example.daybook.nodule.install/<plugId>`: installed package envelope, selected revision,
/// and desired enablement on this node.
#[macros::datum(authority = Nodule::ID, name = "install")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleInstall {
    pub envelope: IndexMap<String, Value>,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub revision: Revision,
    pub enabled: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

impl NoduleInstall {
    pub fn from_plug(plug: &Plug, enabled: bool) -> Self {
        let envelope: IndexMap<String, Value> =
            serde_json::from_value(serde_json::to_value(plug).expect("a plug envelope serializes"))
                .expect("a plug envelope serializes as a JSON object");
        Self {
            envelope,
            revision: plug.revision(),
            enabled,
            extra: Default::default(),
        }
    }

    pub fn decode_plug(&self) -> Result<Plug, String> {
        let value = Value::Object(
            self.envelope
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        let plug: Plug =
            serde_json::from_value(value).map_err(|err| format!("invalid plug envelope: {err}"))?;
        plug.validate().map_err(|err| err.to_string())?;
        let actual = plug.revision();
        if actual != self.revision {
            return Err(format!(
                "plug `{}` revision mismatch: install pins {}, envelope hashes to {}",
                plug.id, self.revision, actual
            ));
        }
        Ok(plug)
    }
}

/// `org.example.daybook.nodule.settings/<plugId>`: explicit operator settings for an installed
/// nodule. Absence means the nodule uses the admitted envelope's defaults.
#[macros::datum(authority = Nodule::ID, name = "settings")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleSettings {
    pub values: IndexMap<String, Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl NoduleSettings {
    pub fn new(values: IndexMap<String, Value>) -> Self {
        Self {
            values,
            extra: Default::default(),
        }
    }
}

/// `org.example.daybook.nodule.grants/<plugId>`: desired local drawer access bindings for an
/// installed nodule, keyed by portable [`DrawerId`].
#[macros::datum(authority = Nodule::ID, name = "grants")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleGrants {
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<String, String>"))]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drawers: BTreeMap<DrawerId, EntryAccess>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl NoduleGrants {
    pub fn from_drawers(drawers: BTreeMap<DrawerId, DrawerAccess>) -> Self {
        Self {
            drawers: drawers
                .into_iter()
                .map(|(drawer_id, access)| (drawer_id, EntryAccess::from(access)))
                .collect(),
            extra: Default::default(),
        }
    }
}

/// The three portable nodule configuration datum declarations.
#[cfg(feature = "schemars")]
pub fn config_universe() -> big_cabinet_core::datum::Universe {
    use big_cabinet_core::datum::Declaration;
    use big_cabinet_core::datum::Universe;
    Universe::collect([
        Declaration::of::<NoduleInstall>().expect("NoduleInstall declaration is valid"),
        Declaration::of::<NoduleSettings>().expect("NoduleSettings declaration is valid"),
        Declaration::of::<NoduleGrants>().expect("NoduleGrants declaration is valid"),
    ])
}

pub fn install_key(plug_id: &PlugId) -> DatumKey {
    DatumKey::from_text(NoduleInstall::ID, plug_id.to_string())
        .expect("a PlugId is a valid datum key slot")
}

pub fn settings_key(plug_id: &PlugId) -> DatumKey {
    DatumKey::from_text(NoduleSettings::ID, plug_id.to_string())
        .expect("a PlugId is a valid datum key slot")
}

pub fn grants_key(plug_id: &PlugId) -> DatumKey {
    DatumKey::from_text(NoduleGrants::ID, plug_id.to_string())
        .expect("a PlugId is a valid datum key slot")
}

/// One datum slot in [`NoduleEntry`], distinguishing absence from accepted values and carried
/// invalid/unaccepted reads.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum DatumSlot<T> {
    #[default]
    Absent,
    Accepted {
        value: T,
        raw: Option<Value>,
    },
    Invalid {
        raw: Option<Value>,
        reason: String,
    },
}

impl<T> DatumSlot<T> {
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }

    pub fn accepted(&self) -> Option<&T> {
        match self {
            Self::Accepted { value, .. } => Some(value),
            Self::Absent | Self::Invalid { .. } => None,
        }
    }
}

impl<T: DeserializeOwned> DatumSlot<T> {
    fn from_read(read: &DatumRead) -> Self {
        match read {
            DatumRead::Absent { .. } => Self::Absent,
            DatumRead::Accepted { value, .. } => match serde_json::from_value::<T>(value.clone()) {
                Ok(decoded) => Self::Accepted {
                    value: decoded,
                    raw: Some(value.clone()),
                },
                Err(err) => Self::Invalid {
                    raw: Some(value.clone()),
                    reason: err.to_string(),
                },
            },
            DatumRead::Pending { reason, value, .. } => Self::Invalid {
                raw: value.clone(),
                reason: format!("pending datum: {reason}"),
            },
            DatumRead::Rejected { reason } => Self::Invalid {
                raw: None,
                reason: format!("rejected datum: {reason}"),
            },
            DatumRead::Undecodable { reason, .. } => Self::Invalid {
                raw: None,
                reason: format!("undecodable datum: {reason}"),
            },
        }
    }
}

/// The three configuration datum slots for one [`PlugId`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NoduleEntry {
    pub install: DatumSlot<NoduleInstall>,
    pub settings: DatumSlot<NoduleSettings>,
    pub grants: DatumSlot<NoduleGrants>,
}

impl NoduleEntry {
    pub fn is_empty(&self) -> bool {
        self.install.is_absent() && self.settings.is_absent() && self.grants.is_absent()
    }
}

/// Per-nodule evaluation of a [`NoduleEntry`] for host reconciliation.
#[expect(
    clippy::large_enum_variant,
    reason = "Admitted carries InstalledNodule inline on the reconciliation path"
)]
#[derive(Debug, Clone, PartialEq)]
pub enum NoduleResolution {
    Admitted {
        installed: InstalledNodule,
        desired_grants: BTreeMap<DrawerId, DrawerAccess>,
    },
    Refused {
        plug_id: PlugId,
        version: Option<Version>,
        revision: Option<Revision>,
        nodule_id: Option<String>,
        enabled: bool,
        config: Option<IndexMap<String, Value>>,
        reason: String,
    },
}

/// Typed [`DatumStore`] projection over a node's nodule configuration datums.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NoduleConfig {
    entries: BTreeMap<PlugId, NoduleEntry>,
    carried_keys: BTreeMap<DatumKey, DatumRead>,
}

impl From<InventoryError> for StoreError {
    fn from(error: InventoryError) -> Self {
        Self::InvalidEdit(error.to_string())
    }
}

impl DatumStore for NoduleConfig {
    fn owns(key: &DatumKey) -> bool {
        matches!(
            key.tag().as_str(),
            NoduleInstall::ID | NoduleSettings::ID | NoduleGrants::ID
        )
    }

    fn hydrate(read: &DatumsRead) -> Self {
        let mut config = Self::default();
        for (key, datum_read) in &read.datums {
            if !Self::owns(key) || matches!(datum_read, DatumRead::Absent { .. }) {
                continue;
            }
            let Some(plug_id) = std::str::from_utf8(key.slot_bytes())
                .ok()
                .and_then(|slot| slot.parse::<PlugId>().ok())
            else {
                config.carried_keys.insert(key.clone(), datum_read.clone());
                continue;
            };
            let entry = config.entries.entry(plug_id).or_default();
            match key.tag().as_str() {
                NoduleInstall::ID => entry.install = DatumSlot::from_read(datum_read),
                NoduleSettings::ID => entry.settings = DatumSlot::from_read(datum_read),
                NoduleGrants::ID => entry.grants = DatumSlot::from_read(datum_read),
                _ => unreachable!("checked by NoduleConfig::owns"),
            }
        }
        config.entries.retain(|_, entry| !entry.is_empty());
        config
    }

    fn diff(before: &Self, after: &Self) -> Result<DatumPatch, StoreError> {
        let mut patch = DatumPatch::default();
        let all_plugs: BTreeSet<&PlugId> =
            before.entries.keys().chain(after.entries.keys()).collect();
        let empty = NoduleEntry::default();
        for plug_id in all_plugs {
            let prev = before.entries.get(plug_id).unwrap_or(&empty);
            let next = after.entries.get(plug_id).unwrap_or(&empty);
            diff_slot(
                install_key(plug_id),
                &prev.install,
                &next.install,
                &mut patch,
            )?;
            diff_slot(
                settings_key(plug_id),
                &prev.settings,
                &next.settings,
                &mut patch,
            )?;
            diff_slot(grants_key(plug_id), &prev.grants, &next.grants, &mut patch)?;
        }
        for key in before.carried_keys.keys() {
            if !after.carried_keys.contains_key(key) {
                patch.remove.insert(key.clone());
            }
        }
        Ok(patch)
    }
}

fn diff_slot<T: PartialEq + Serialize>(
    key: DatumKey,
    before: &DatumSlot<T>,
    after: &DatumSlot<T>,
    patch: &mut DatumPatch,
) -> Result<(), StoreError> {
    if before == after {
        return Ok(());
    }
    match after {
        DatumSlot::Absent => {
            if !before.is_absent() {
                patch.remove.insert(key);
            }
            Ok(())
        }
        DatumSlot::Accepted { value, raw } => {
            if let DatumSlot::Accepted {
                value: before_val,
                raw: before_raw,
            } = before
                && before_val == value
                && (raw.is_none() || before_raw == raw)
            {
                return Ok(());
            }
            let json = match raw {
                Some(raw) => raw.clone(),
                None => serde_json::to_value(value)
                    .map_err(|err| StoreError::InvalidEdit(err.to_string()))?,
            };
            patch.set.insert(key, json);
            Ok(())
        }
        DatumSlot::Invalid { reason, .. } => Err(StoreError::InvalidEdit(format!(
            "cannot write invalid datum `{key}`: {reason}"
        ))),
    }
}

fn apply_grants_slot(
    slot: &mut DatumSlot<NoduleGrants>,
    drawers: BTreeMap<DrawerId, DrawerAccess>,
) {
    let extra = match slot {
        DatumSlot::Accepted { value, .. } => value.extra.clone(),
        _ => Extra::default(),
    };
    let mapped: BTreeMap<DrawerId, EntryAccess> = drawers
        .into_iter()
        .map(|(drawer_id, access)| (drawer_id, EntryAccess::from(access)))
        .collect();
    if mapped.is_empty() && extra.is_empty() {
        *slot = DatumSlot::Absent;
    } else {
        *slot = DatumSlot::Accepted {
            value: NoduleGrants {
                drawers: mapped,
                extra,
            },
            raw: None,
        };
    }
}

impl NoduleConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entry(&self, plug_id: &PlugId) -> Option<&NoduleEntry> {
        self.entries.get(plug_id)
    }

    pub fn entries(&self) -> &BTreeMap<PlugId, NoduleEntry> {
        &self.entries
    }

    pub fn carried_keys(&self) -> &BTreeMap<DatumKey, DatumRead> {
        &self.carried_keys
    }

    pub fn remove_carried_key(&mut self, key: &DatumKey) -> bool {
        self.carried_keys.remove(key).is_some()
    }

    /// Validates `plug`, admits its nodule contribution, checks authority conflicts and existing
    /// settings compatibility, and updates the [`NoduleInstall`] (and optionally [`NoduleGrants`])
    /// slots for `plug.id`.
    pub fn admit(
        &mut self,
        plug: &Plug,
        drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
    ) -> Result<InstalledNodule, InventoryError> {
        let (nodule, admitted, artifacts) = admit_plug_nodule(plug)?;
        for (other_plug, other_entry) in &self.entries {
            if other_plug == &plug.id {
                continue;
            }
            if let Some(other_install) = other_entry.install.accepted()
                && let Ok(other_decoded) = other_install.decode_plug()
                && let Ok((other_nodule, _, _)) = admit_plug_nodule(&other_decoded)
                && (other_nodule.owns(&nodule.id) || nodule.owns(&other_nodule.id))
            {
                return Err(InventoryError::AuthorityConflict {
                    id: plug.id.clone(),
                    nodule_id: nodule.id,
                    existing_plug: Box::new(other_plug.clone()),
                    existing_nodule: other_nodule.id,
                });
            }
        }

        let revision = plug.revision();
        let (enabled, install_extra, config, configured) = if let Some(existing_entry) =
            self.entries.get(&plug.id)
        {
            if let Some(existing_install) = existing_entry.install.accepted()
                && let Ok(existing_plug) = existing_install.decode_plug()
            {
                if existing_plug.version == plug.version && existing_install.revision != revision {
                    return Err(InventoryError::Plug(PlugError::Conflict {
                        id: plug.id.clone(),
                        version: Box::new(plug.version.clone()),
                        existing: existing_install.revision,
                        incoming: revision,
                    }));
                }
                if let Ok((existing_nodule, _, _)) = admit_plug_nodule(&existing_plug)
                    && existing_nodule.id != nodule.id
                {
                    return Err(InventoryError::AuthorityChanged {
                        id: plug.id.clone(),
                        existing: existing_nodule.id,
                        incoming: nodule.id,
                    });
                }
            }
            if drawers.is_none()
                && let DatumSlot::Invalid { reason, .. } = &existing_entry.grants
            {
                return Err(InventoryError::InvalidContribution {
                    id: plug.id.clone(),
                    reason: format!("existing grants datum is invalid: {reason}"),
                });
            }
            let (config, configured) = match &existing_entry.settings {
                DatumSlot::Absent => (
                    nodule
                        .config
                        .as_ref()
                        .and_then(|config| config.defaults.clone()),
                    false,
                ),
                DatumSlot::Accepted { value, .. } => {
                    if nodule.config.is_none() {
                        return Err(InventoryError::NoConfigSchema(plug.id.clone()));
                    }
                    nodule.validate_config(&value.values, "config")?;
                    (Some(value.values.clone()), true)
                }
                DatumSlot::Invalid { reason, .. } => {
                    return Err(InventoryError::InvalidContribution {
                        id: plug.id.clone(),
                        reason: format!("existing settings datum is invalid: {reason}"),
                    });
                }
            };
            let (enabled, extra) = match &existing_entry.install {
                DatumSlot::Accepted { value, .. } => (value.enabled, value.extra.clone()),
                _ => (false, Extra::default()),
            };
            (enabled, extra, config, configured)
        } else {
            let defaults = nodule
                .config
                .as_ref()
                .and_then(|config| config.defaults.clone());
            (false, Extra::default(), defaults, false)
        };

        let mut install = NoduleInstall::from_plug(plug, enabled);
        install.extra = install_extra;
        let entry = self.entries.entry(plug.id.clone()).or_default();
        entry.install = DatumSlot::Accepted {
            value: install,
            raw: None,
        };
        if let Some(drawers) = drawers {
            apply_grants_slot(&mut entry.grants, drawers);
        }

        Ok(InstalledNodule::new(
            plug.id.clone(),
            plug.version.clone(),
            revision,
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        ))
    }

    pub fn configure(
        &mut self,
        plug_id: &PlugId,
        settings: IndexMap<String, Value>,
    ) -> Result<InstalledNodule, InventoryError> {
        let (plug, nodule, admitted, artifacts, enabled) = self.validate_installed_plug(plug_id)?;
        if nodule.config.is_none() {
            return Err(InventoryError::NoConfigSchema(plug_id.clone()));
        }
        nodule.validate_config(&settings, "config")?;
        let entry = self.entries.get_mut(plug_id).expect("verified installed");
        let extra = match &entry.settings {
            DatumSlot::Accepted { value, .. } => value.extra.clone(),
            _ => Extra::default(),
        };
        entry.settings = DatumSlot::Accepted {
            value: NoduleSettings {
                values: settings.clone(),
                extra,
            },
            raw: None,
        };
        Ok(InstalledNodule::new(
            plug.id.clone(),
            plug.version.clone(),
            plug.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            Some(settings),
            true,
        ))
    }

    pub fn set_enabled(
        &mut self,
        plug_id: &PlugId,
        enabled: bool,
    ) -> Result<InstalledNodule, InventoryError> {
        let (plug, nodule, admitted, artifacts, _, (config, configured)) =
            self.validate_installed(plug_id)?;
        let entry = self.entries.get_mut(plug_id).expect("verified installed");
        if let DatumSlot::Accepted { value, raw } = &mut entry.install
            && value.enabled != enabled
        {
            value.enabled = enabled;
            *raw = None;
        }
        Ok(InstalledNodule::new(
            plug.id.clone(),
            plug.version.clone(),
            plug.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        ))
    }

    pub fn set_drawer_grants(
        &mut self,
        plug_id: &PlugId,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
    ) -> Result<InstalledNodule, InventoryError> {
        let (plug, nodule, admitted, artifacts, enabled, (config, configured)) =
            self.validate_installed(plug_id)?;
        let entry = self.entries.get_mut(plug_id).expect("verified installed");
        apply_grants_slot(&mut entry.grants, drawers);
        Ok(InstalledNodule::new(
            plug.id.clone(),
            plug.version.clone(),
            plug.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        ))
    }

    pub fn uninstall(&mut self, plug_id: &PlugId) -> Result<(), InventoryError> {
        let Some(entry) = self.entries.remove(plug_id) else {
            return Err(InventoryError::UnknownPlug(plug_id.clone()));
        };
        if entry.is_empty() {
            return Err(InventoryError::UnknownPlug(plug_id.clone()));
        }
        Ok(())
    }

    /// Evaluates every installed nodule entry independently, reporting per-nodule refusals without
    /// failing unrelated valid nodules. Orphan settings or grants (where `install` is absent) do
    /// not install a nodule.
    pub fn resolve(&self) -> BTreeMap<PlugId, NoduleResolution> {
        let mut results = BTreeMap::new();
        let mut valid_authorities: BTreeMap<PlugId, String> = BTreeMap::new();

        for (plug_id, entry) in &self.entries {
            let Some(resolution) = resolve_entry(plug_id, entry) else {
                continue;
            };
            if let NoduleResolution::Admitted { installed, .. } = &resolution {
                valid_authorities.insert(plug_id.clone(), installed.nodule.id.clone());
            }
            results.insert(plug_id.clone(), resolution);
        }

        let mut conflicts: BTreeMap<PlugId, String> = BTreeMap::new();
        for (plug_id, resolution) in &results {
            let NoduleResolution::Admitted { installed, .. } = resolution else {
                continue;
            };
            for (other_plug, other_nodule_id) in &valid_authorities {
                if other_plug != plug_id
                    && (installed.nodule.owns(other_nodule_id)
                        || owns_nsid(other_nodule_id, &installed.nodule.id))
                {
                    let err = InventoryError::AuthorityConflict {
                        id: plug_id.clone(),
                        nodule_id: installed.nodule.id.clone(),
                        existing_plug: Box::new(other_plug.clone()),
                        existing_nodule: other_nodule_id.clone(),
                    };
                    conflicts.insert(plug_id.clone(), err.to_string());
                    break;
                }
            }
        }

        for (plug_id, reason) in conflicts {
            if let Some(NoduleResolution::Admitted { installed, .. }) = results.remove(&plug_id) {
                results.insert(
                    plug_id.clone(),
                    NoduleResolution::Refused {
                        plug_id,
                        version: Some(installed.version),
                        revision: Some(installed.revision),
                        nodule_id: Some(installed.nodule.id),
                        enabled: installed.enabled,
                        config: installed.config,
                        reason,
                    },
                );
            }
        }

        results
    }

    #[expect(
        clippy::type_complexity,
        reason = "internal helper returning decoded plug components"
    )]
    fn validate_installed_plug(
        &self,
        plug_id: &PlugId,
    ) -> Result<
        (
            Plug,
            Nodule,
            crate::Admitted,
            BTreeMap<String, (big_plugs_core::Artifact, crate::inventory::ArtifactState)>,
            bool,
        ),
        InventoryError,
    > {
        let Some(entry) = self.entries.get(plug_id) else {
            return Err(InventoryError::UnknownPlug(plug_id.clone()));
        };
        let install = match &entry.install {
            DatumSlot::Absent => return Err(InventoryError::UnknownPlug(plug_id.clone())),
            DatumSlot::Invalid { reason, .. } => {
                return Err(InventoryError::InvalidContribution {
                    id: plug_id.clone(),
                    reason: format!("install datum is invalid: {reason}"),
                });
            }
            DatumSlot::Accepted { value, .. } => value,
        };
        let plug = install
            .decode_plug()
            .map_err(|reason| InventoryError::InvalidContribution {
                id: plug_id.clone(),
                reason,
            })?;
        if &plug.id != plug_id {
            return Err(InventoryError::InvalidContribution {
                id: plug_id.clone(),
                reason: format!(
                    "install datum key `{plug_id}` does not match envelope id `{}`",
                    plug.id
                ),
            });
        }
        let (nodule, admitted, artifacts) = admit_plug_nodule(&plug)?;
        Ok((plug, nodule, admitted, artifacts, install.enabled))
    }

    #[expect(
        clippy::type_complexity,
        reason = "internal helper returning decoded plug and settings components"
    )]
    fn validate_installed(
        &self,
        plug_id: &PlugId,
    ) -> Result<
        (
            Plug,
            Nodule,
            crate::Admitted,
            BTreeMap<String, (big_plugs_core::Artifact, crate::inventory::ArtifactState)>,
            bool,
            (Option<IndexMap<String, Value>>, bool),
        ),
        InventoryError,
    > {
        let (plug, nodule, admitted, artifacts, enabled) = self.validate_installed_plug(plug_id)?;
        let entry = self.entries.get(plug_id).expect("verified installed");
        let config_state = match &entry.settings {
            DatumSlot::Absent => (
                nodule
                    .config
                    .as_ref()
                    .and_then(|config| config.defaults.clone()),
                false,
            ),
            DatumSlot::Accepted { value, .. } => {
                if nodule.config.is_none() {
                    return Err(InventoryError::NoConfigSchema(plug_id.clone()));
                }
                nodule.validate_config(&value.values, "config")?;
                (Some(value.values.clone()), true)
            }
            DatumSlot::Invalid { reason, .. } => {
                return Err(InventoryError::InvalidContribution {
                    id: plug_id.clone(),
                    reason: format!("settings datum is invalid: {reason}"),
                });
            }
        };
        Ok((plug, nodule, admitted, artifacts, enabled, config_state))
    }
}

fn owns_nsid(authority: &str, nsid: &str) -> bool {
    nsid == authority
        || nsid
            .strip_prefix(authority)
            .is_some_and(|rest| rest.starts_with('.'))
}

/// The nodule id as written in a raw envelope, for refusals whose envelope cannot be decoded.
/// Only the nodule contribution names the nodule: a micro's `description.id` is a GUI key. The
/// raw envelope tags each contribution with `$type`.
fn envelope_nodule_id(envelope: &IndexMap<String, Value>) -> Option<String> {
    envelope
        .get("contributions")
        .and_then(Value::as_array)?
        .iter()
        .filter(|item| item.get("$type").and_then(Value::as_str) == Some(NoduleContribution::ID))
        .find_map(|item| {
            item.get("description")?
                .get("id")?
                .as_str()
                .map(str::to_owned)
        })
}

fn resolve_entry(plug_id: &PlugId, entry: &NoduleEntry) -> Option<NoduleResolution> {
    let install = match &entry.install {
        DatumSlot::Absent => return None,
        DatumSlot::Invalid { reason, .. } => {
            return Some(NoduleResolution::Refused {
                plug_id: plug_id.clone(),
                version: None,
                revision: None,
                nodule_id: None,
                enabled: false,
                config: None,
                reason: format!("invalid install datum: {reason}"),
            });
        }
        DatumSlot::Accepted { value, .. } => value,
    };

    let raw_version = install
        .envelope
        .get("version")
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<Version>().ok());
    let raw_nodule_id = envelope_nodule_id(&install.envelope);
    let raw_config = entry.settings.accepted().map(|s| s.values.clone());

    let refuse = |version: Option<Version>,
                  nodule_id: Option<String>,
                  config: Option<IndexMap<String, Value>>,
                  reason: String| {
        NoduleResolution::Refused {
            plug_id: plug_id.clone(),
            version,
            revision: Some(install.revision),
            nodule_id,
            enabled: install.enabled,
            config,
            reason,
        }
    };

    let plug = match install.decode_plug() {
        Ok(plug) => plug,
        Err(reason) => return Some(refuse(raw_version, raw_nodule_id, raw_config, reason)),
    };
    if &plug.id != plug_id {
        return Some(refuse(
            Some(plug.version),
            raw_nodule_id,
            raw_config,
            format!(
                "install datum key `{plug_id}` does not match envelope id `{}`",
                plug.id
            ),
        ));
    }
    let (nodule, admitted, artifacts) = match admit_plug_nodule(&plug) {
        Ok(admitted) => admitted,
        Err(err) => {
            return Some(refuse(
                Some(plug.version),
                raw_nodule_id,
                raw_config,
                err.to_string(),
            ));
        }
    };

    let (config, configured) = match &entry.settings {
        DatumSlot::Absent => (
            nodule
                .config
                .as_ref()
                .and_then(|config| config.defaults.clone()),
            false,
        ),
        DatumSlot::Invalid { reason, .. } => {
            return Some(refuse(
                Some(plug.version),
                Some(nodule.id),
                None,
                format!("invalid settings datum: {reason}"),
            ));
        }
        DatumSlot::Accepted { value, .. } => {
            if nodule.config.is_none() {
                return Some(refuse(
                    Some(plug.version),
                    Some(nodule.id),
                    Some(value.values.clone()),
                    InventoryError::NoConfigSchema(plug_id.clone()).to_string(),
                ));
            }
            if let Err(err) = nodule.validate_config(&value.values, "config") {
                return Some(refuse(
                    Some(plug.version),
                    Some(nodule.id),
                    Some(value.values.clone()),
                    err.to_string(),
                ));
            }
            (Some(value.values.clone()), true)
        }
    };

    let mut desired_grants = BTreeMap::new();
    match &entry.grants {
        DatumSlot::Absent => {}
        DatumSlot::Invalid { reason, .. } => {
            return Some(refuse(
                Some(plug.version),
                Some(nodule.id),
                config,
                format!("invalid grants datum: {reason}"),
            ));
        }
        DatumSlot::Accepted { value, .. } => {
            for (drawer_id, access) in &value.drawers {
                let Some(known) = access.known() else {
                    return Some(refuse(
                        Some(plug.version),
                        Some(nodule.id),
                        config,
                        format!(
                            "unknown drawer access level `{}` for drawer `{drawer_id}`",
                            access.as_str()
                        ),
                    ));
                };
                desired_grants.insert(drawer_id.clone(), known);
            }
        }
    }

    Some(NoduleResolution::Admitted {
        installed: InstalledNodule::new(
            plug.id,
            plug.version,
            install.revision,
            nodule,
            admitted,
            artifacts,
            install.enabled,
            config,
            configured,
        ),
        desired_grants,
    })
}
