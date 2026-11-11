//! Portable nodule configuration datums and the [`DatumStore`] projection over a node's config
//! document (ADR 027 §4, §6).
//!
//! Each installed nodule's desired state is stored across up to three independently addressed
//! datums keyed by its [`CartridgeId`] slot:
//! - [`NoduleInstall`][]: `org.example.daybook.nodule.install/<cartridgeId>`
//! - [`NoduleSettings`][]: `org.example.daybook.nodule.settings/<cartridgeId>`
//! - [`NoduleGrants`][]: `org.example.daybook.nodule.grants/<cartridgeId>`
//! - [`NoduleDevTakeover`][]: `org.example.daybook.nodule.dev_takeover/<cartridgeId>`, present only
//!   while another installed cartridge fills the slot
//!
//! Beside them, each installed nodule's handler artifact is referenced by a
//! `org.example.daybook.blob/nodule.artifact/<cartridgeId>/<artifactKey>` [`Blob`] datum (see
//! [`artifact_key`]), which is what makes big_blobs retain and fetch the bytes.
//!
//! [`NoduleConfig`] hydrates these datums from one [`DatumsRead`] frontier, preserves unknown
//! fields, unknown vocabulary values, and undecodable/rejected/pending entries so unrelated edits
//! never delete them, and resolves each nodule's desired state independently for the reconciler.

use crate::Nodule;
use crate::inventory::FilledBy;
use crate::inventory::InstalledNodule;
use crate::inventory::InventoryError;
use crate::inventory::admit_cartridge_nodule;
use big_blobs_core::Blob;
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
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeError;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;
use big_dispatch_core::DispatchScope;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Mutex;

/// `org.example.daybook.nodule.install/<cartridgeId>`: installed package envelope, selected revision,
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
    pub fn from_cartridge(cartridge: &Cartridge, enabled: bool) -> Self {
        let envelope: IndexMap<String, Value> = serde_json::from_value(
            serde_json::to_value(cartridge).expect("a cartridge envelope serializes"),
        )
        .expect("a cartridge envelope serializes as a JSON object");
        Self {
            envelope,
            revision: cartridge.revision(),
            enabled,
            extra: Default::default(),
        }
    }

    pub fn decode_cartridge(&self) -> Result<Cartridge, String> {
        let value = Value::Object(
            self.envelope
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        let cartridge: Cartridge = serde_json::from_value(value)
            .map_err(|err| format!("invalid cartridge envelope: {err}"))?;
        cartridge.validate().map_err(|err| err.to_string())?;
        let actual = cartridge.revision();
        if actual != self.revision {
            return Err(format!(
                "cartridge `{}` revision mismatch: install pins {}, envelope hashes to {}",
                cartridge.id, self.revision, actual
            ));
        }
        Ok(cartridge)
    }
}

/// `org.example.daybook.nodule.settings/<cartridgeId>`: explicit operator settings for an installed
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

/// `org.example.daybook.nodule.grants/<cartridgeId>`: desired local drawer access bindings for an
/// installed nodule, keyed by portable [`DrawerId`], and the methods it may invoke through
/// dispatch.
#[macros::datum(authority = Nodule::ID, name = "grants")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleGrants {
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<String, String>"))]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drawers: BTreeMap<DrawerId, EntryAccess>,
    /// The methods the nodule's own actor may invoke through dispatch, as the scope of the
    /// capability the nodule service mints for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<MethodGrants>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The methods a nodule may invoke through dispatch: the wire form of a [`DispatchScope`], as
/// arrays because datums carry no sets. Authority prefixes match whole NSID segments, exactly as
/// in any other scope.
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MethodGrants {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub methods: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorities: Vec<String>,
    /// Whether the nodule may invoke privileged methods (`XrpcMethod::privileged`). The nodule
    /// service holds no privilege, so a grant that sets this is reported as not applied.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub privileged: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

impl From<MethodGrants> for DispatchScope {
    fn from(grants: MethodGrants) -> Self {
        Self {
            operations: grants.methods.into_iter().collect(),
            authorities: grants.authorities.into_iter().collect(),
            privileged: grants.privileged,
        }
    }
}

impl From<DispatchScope> for MethodGrants {
    fn from(scope: DispatchScope) -> Self {
        Self {
            methods: scope.operations.into_iter().collect(),
            authorities: scope.authorities.into_iter().collect(),
            privileged: scope.privileged,
            extra: Default::default(),
        }
    }
}

impl NoduleGrants {
    /// Whether the datum grants nothing and carries nothing: it is then deleted rather than
    /// stored.
    fn is_empty(&self) -> bool {
        self.drawers.is_empty() && self.methods.is_none() && self.extra.is_empty()
    }
}

/// `org.example.daybook.nodule.devTakeover/<cartridgeId>`: the dev hook that runs the installed
/// cartridge `by` *as* this cartridge, to test a dev build against real data (ADR 045 §5). `by`'s
/// code and manifest take the slot over, and every other slot state (actor, drawer grants, method
/// grants narrowed by `substituted_grants`, operation caps, initializer done-ness, the resolution
/// lockfile, dispatch registrations) stays keyed by this cartridge id, so the dev build holds the
/// original's data and actor. Only `setNoduleDevTakeover` writes it, and that is refused unless
/// the node's `node.json` sets `dev.allow_slot_takeover`. Absence means the slot runs its own
/// install; deleting the datum reinstalls it.
#[macros::datum(authority = Nodule::ID, name = "devTakeover")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleDevTakeover {
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub by: CartridgeId,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `org.example.daybook.dispatch.route/<pattern>`: the node-local route of an exact NSID or an
/// authority prefix to the provider `provider` (ADR 045). Dispatch's router applies it; the
/// nodule service holds the datum only until the node has a per-node store of its own, so this
/// datum is not keyed by a cartridge and has no slot in a [`NoduleEntry`].
#[macros::datum(authority = "org.example.daybook.dispatch", name = "route")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MethodRoute {
    pub provider: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The portable nodule configuration datum declarations.
#[cfg(feature = "schemars")]
pub fn config_universe() -> big_cabinet_core::datum::Universe {
    use big_cabinet_core::datum::Declaration;
    use big_cabinet_core::datum::Universe;
    Universe::collect([
        Declaration::of::<NoduleInstall>().expect("NoduleInstall declaration is valid"),
        Declaration::of::<NoduleSettings>().expect("NoduleSettings declaration is valid"),
        Declaration::of::<NoduleGrants>().expect("NoduleGrants declaration is valid"),
        Declaration::of::<NoduleDevTakeover>().expect("NoduleDevTakeover declaration is valid"),
        Declaration::of::<MethodRoute>().expect("MethodRoute declaration is valid"),
    ])
}

pub fn install_key(cartridge_id: &CartridgeId) -> DatumKey {
    DatumKey::from_text(NoduleInstall::ID, cartridge_id.to_string())
        .expect("a CartridgeId is a valid datum key slot")
}

pub fn settings_key(cartridge_id: &CartridgeId) -> DatumKey {
    DatumKey::from_text(NoduleSettings::ID, cartridge_id.to_string())
        .expect("a CartridgeId is a valid datum key slot")
}

pub fn grants_key(cartridge_id: &CartridgeId) -> DatumKey {
    DatumKey::from_text(NoduleGrants::ID, cartridge_id.to_string())
        .expect("a CartridgeId is a valid datum key slot")
}

pub fn dev_takeover_key(cartridge_id: &CartridgeId) -> DatumKey {
    DatumKey::from_text(NoduleDevTakeover::ID, cartridge_id.to_string())
        .expect("a CartridgeId is a valid datum key slot")
}

pub fn route_key(pattern: &str) -> DatumKey {
    DatumKey::from_text(MethodRoute::ID, pattern)
        .expect("a validated route pattern is a valid datum key slot")
}

/// The slot prefix that namespaces the [`Blob`] datums this projection owns inside the node config
/// document. `owns` claims a blob-tagged key only under this prefix, so a `Blob` datum another
/// service later writes into the same document is neither read nor deleted here.
const ARTIFACT_SLOT_PREFIX: &str = "nodule.artifact/";

/// `org.example.daybook.blob/nodule.artifact/<cartridgeId>/<artifactKey>`: the [`Blob`] datum that
/// references one artifact of an installed cartridge. big_blobs retains and fetches the bytes of every
/// `Blob` in the document, so this is the nodule's artifact pin. A cartridge id is exactly
/// `<did>/<name>` (neither part contains `/`), so the artifact key, which may contain `/`, is
/// whatever follows the second `/`. The DID's `:` and `%` are legal in a key slot as they are.
pub fn artifact_key(cartridge_id: &CartridgeId, artifact: &str) -> DatumKey {
    DatumKey::from_text(
        Blob::ID,
        format!("{ARTIFACT_SLOT_PREFIX}{cartridge_id}/{artifact}"),
    )
    .expect("a cartridge id and a non-empty artifact key are a valid datum key slot")
}

/// The cartridge and artifact key an [`artifact_key`] slot names, `None` when the slot carries the
/// prefix but not that shape.
fn parse_artifact_slot(slot: &[u8]) -> Option<(CartridgeId, String)> {
    let rest = std::str::from_utf8(slot)
        .ok()?
        .strip_prefix(ARTIFACT_SLOT_PREFIX)?;
    let (namespace, rest) = rest.split_once('/')?;
    let (name, artifact) = rest.split_once('/')?;
    if artifact.is_empty() {
        return None;
    }
    let cartridge_id = format!("{namespace}/{name}").parse().ok()?;
    Some((cartridge_id, artifact.to_owned()))
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

/// The configuration datum slots for one [`CartridgeId`]: the three nodule datums and the artifact
/// [`Blob`] references, by artifact key. An artifact slot present here is always in the document
/// or about to be; absence from the map is absence from the document.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NoduleEntry {
    pub install: DatumSlot<NoduleInstall>,
    pub settings: DatumSlot<NoduleSettings>,
    pub grants: DatumSlot<NoduleGrants>,
    pub dev_takeover: DatumSlot<NoduleDevTakeover>,
    pub artifacts: BTreeMap<String, DatumSlot<Blob>>,
}

impl NoduleEntry {
    pub fn is_empty(&self) -> bool {
        self.install.is_absent()
            && self.settings.is_absent()
            && self.grants.is_absent()
            && self.dev_takeover.is_absent()
            && self.artifacts.is_empty()
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
        desired_operations: DispatchScope,
    },
    Refused {
        cartridge_id: CartridgeId,
        version: Option<Version>,
        revision: Option<Revision>,
        enabled: bool,
        config: Option<IndexMap<String, Value>>,
        reason: String,
    },
}

/// What admitting one stored install envelope established, kept so the same envelope is not
/// decoded and admitted again on every reconcile.
#[expect(
    clippy::large_enum_variant,
    reason = "always held behind an Arc in the admission cache"
)]
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    Admitted {
        version: Version,
        nodule: Nodule,
        admitted: crate::Admitted,
        artifacts: BTreeMap<
            String,
            (
                big_cartridges_core::Artifact,
                crate::inventory::ArtifactState,
            ),
        >,
    },
    /// The envelope cannot be installed; `version` is a best-effort read of the raw envelope for
    /// the refusal record.
    Refused {
        version: Option<Version>,
        reason: String,
    },
}

struct CachedAdmission {
    envelope: IndexMap<String, Value>,
    revision: Revision,
    admission: Arc<Admission>,
}

/// Memo of [`Admission`] outcomes per installed cartridge, owned by whoever reconciles (the nodule state
/// actor). An entry is reused only while the stored install datum carries the same revision and a
/// byte-equal envelope, so a changed or tampered envelope is always re-admitted. [`NoduleConfig::resolve`]
/// drops entries for cartridges that are no longer installed. The lock is held only around map access.
#[derive(Default)]
pub struct AdmissionCache {
    entries: Mutex<BTreeMap<CartridgeId, CachedAdmission>>,
}

impl AdmissionCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The admission outcome of `install` as stored under `cartridge_id`, computed at most once per
    /// `(cartridge_id, revision, envelope)`.
    pub fn admission(&self, cartridge_id: &CartridgeId, install: &NoduleInstall) -> Arc<Admission> {
        let mut entries = self.entries.lock().expect("admission cache lock");
        if let Some(cached) = entries.get(cartridge_id)
            && cached.revision == install.revision
            && cached.envelope == install.envelope
        {
            return Arc::clone(&cached.admission);
        }
        let admission = Arc::new(Self::compute(cartridge_id, install));
        entries.insert(
            cartridge_id.clone(),
            CachedAdmission {
                envelope: install.envelope.clone(),
                revision: install.revision,
                admission: Arc::clone(&admission),
            },
        );
        admission
    }

    fn retain<'a>(&self, installed: impl Iterator<Item = &'a CartridgeId>) {
        let keep: BTreeSet<&CartridgeId> = installed.collect();
        self.entries
            .lock()
            .expect("admission cache lock")
            .retain(|cartridge_id, _| keep.contains(cartridge_id));
    }

    fn compute(cartridge_id: &CartridgeId, install: &NoduleInstall) -> Admission {
        let cartridge = match install.decode_cartridge() {
            Ok(cartridge) => cartridge,
            Err(reason) => {
                return Admission::Refused {
                    version: install
                        .envelope
                        .get("version")
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse::<Version>().ok()),
                    reason,
                };
            }
        };
        if &cartridge.id != cartridge_id {
            return Admission::Refused {
                version: Some(cartridge.version),
                reason: format!(
                    "install datum key `{cartridge_id}` does not match envelope id `{}`",
                    cartridge.id
                ),
            };
        }
        match admit_cartridge_nodule(&cartridge) {
            Ok((nodule, admitted, artifacts)) => Admission::Admitted {
                version: cartridge.version,
                nodule,
                admitted,
                artifacts,
            },
            Err(err) => Admission::Refused {
                version: Some(cartridge.version),
                reason: err.to_string(),
            },
        }
    }
}

/// Typed [`DatumStore`] projection over a node's nodule configuration datums.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NoduleConfig {
    entries: BTreeMap<CartridgeId, NoduleEntry>,
    /// The route datums by pattern: node-wide, so they belong to no cartridge's entry.
    routes: BTreeMap<String, DatumSlot<MethodRoute>>,
    carried_keys: BTreeMap<DatumKey, DatumRead>,
}

impl From<InventoryError> for StoreError {
    fn from(error: InventoryError) -> Self {
        Self::InvalidEdit(error.to_string())
    }
}

impl DatumStore for NoduleConfig {
    fn owns(key: &DatumKey) -> bool {
        match key.tag().as_str() {
            NoduleInstall::ID
            | NoduleSettings::ID
            | NoduleGrants::ID
            | NoduleDevTakeover::ID
            | MethodRoute::ID => true,
            Blob::ID => key
                .slot_bytes()
                .starts_with(ARTIFACT_SLOT_PREFIX.as_bytes()),
            _ => false,
        }
    }

    fn hydrate(read: &DatumsRead) -> Self {
        let mut config = Self::default();
        for (key, datum_read) in &read.datums {
            if !Self::owns(key) || matches!(datum_read, DatumRead::Absent { .. }) {
                continue;
            }
            if key.tag().as_str() == Blob::ID {
                let Some((cartridge_id, artifact)) = parse_artifact_slot(key.slot_bytes()) else {
                    config.carried_keys.insert(key.clone(), datum_read.clone());
                    continue;
                };
                config
                    .entries
                    .entry(cartridge_id)
                    .or_default()
                    .artifacts
                    .insert(artifact, DatumSlot::from_read(datum_read));
                continue;
            }
            if key.tag().as_str() == MethodRoute::ID {
                match std::str::from_utf8(key.slot_bytes()) {
                    Ok(pattern) => {
                        config
                            .routes
                            .insert(pattern.to_owned(), DatumSlot::from_read(datum_read));
                    }
                    Err(_) => {
                        config.carried_keys.insert(key.clone(), datum_read.clone());
                    }
                }
                continue;
            }
            let Some(cartridge_id) = std::str::from_utf8(key.slot_bytes())
                .ok()
                .and_then(|slot| slot.parse::<CartridgeId>().ok())
            else {
                config.carried_keys.insert(key.clone(), datum_read.clone());
                continue;
            };
            let entry = config.entries.entry(cartridge_id).or_default();
            match key.tag().as_str() {
                NoduleInstall::ID => entry.install = DatumSlot::from_read(datum_read),
                NoduleSettings::ID => entry.settings = DatumSlot::from_read(datum_read),
                NoduleGrants::ID => entry.grants = DatumSlot::from_read(datum_read),
                NoduleDevTakeover::ID => entry.dev_takeover = DatumSlot::from_read(datum_read),
                _ => unreachable!("checked by NoduleConfig::owns"),
            }
        }
        config.entries.retain(|_, entry| !entry.is_empty());
        config
    }

    fn diff(before: &Self, after: &Self) -> Result<DatumPatch, StoreError> {
        let mut patch = DatumPatch::default();
        let all_cartridges: BTreeSet<&CartridgeId> =
            before.entries.keys().chain(after.entries.keys()).collect();
        let empty = NoduleEntry::default();
        for cartridge_id in all_cartridges {
            let prev = before.entries.get(cartridge_id).unwrap_or(&empty);
            let next = after.entries.get(cartridge_id).unwrap_or(&empty);
            diff_slot(
                install_key(cartridge_id),
                &prev.install,
                &next.install,
                &mut patch,
            )?;
            diff_slot(
                settings_key(cartridge_id),
                &prev.settings,
                &next.settings,
                &mut patch,
            )?;
            diff_slot(
                grants_key(cartridge_id),
                &prev.grants,
                &next.grants,
                &mut patch,
            )?;
            diff_slot(
                dev_takeover_key(cartridge_id),
                &prev.dev_takeover,
                &next.dev_takeover,
                &mut patch,
            )?;
            let absent = DatumSlot::Absent;
            let artifacts: BTreeSet<&String> =
                prev.artifacts.keys().chain(next.artifacts.keys()).collect();
            for artifact in artifacts {
                diff_slot(
                    artifact_key(cartridge_id, artifact),
                    prev.artifacts.get(artifact).unwrap_or(&absent),
                    next.artifacts.get(artifact).unwrap_or(&absent),
                    &mut patch,
                )?;
            }
        }
        let absent = DatumSlot::Absent;
        let patterns: BTreeSet<&String> = before.routes.keys().chain(after.routes.keys()).collect();
        for pattern in patterns {
            diff_slot(
                route_key(pattern),
                before.routes.get(pattern).unwrap_or(&absent),
                after.routes.get(pattern).unwrap_or(&absent),
                &mut patch,
            )?;
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

/// The artifact [`Blob`] slots an install of `artifacts` leaves: exactly one per artifact, and
/// none for a key the previous revision had and this one lacks, so the diff removes the stale
/// reference together with the install datum. A slot whose stored blob already says the same is
/// kept as stored (no write), a changed one keeps its stored unknown fields, and a stored slot
/// that did not decode is replaced, which is how it is repaired.
fn artifact_slots(
    stored: &BTreeMap<String, DatumSlot<Blob>>,
    artifacts: &BTreeMap<
        String,
        (
            big_cartridges_core::Artifact,
            crate::inventory::ArtifactState,
        ),
    >,
) -> BTreeMap<String, DatumSlot<Blob>> {
    artifacts
        .iter()
        .map(|(key, (artifact, _))| {
            let (extra, kept) = match stored.get(key) {
                Some(DatumSlot::Accepted { value, raw }) => {
                    (value.extra.clone(), Some((value, raw)))
                }
                _ => (Extra::default(), None),
            };
            let desired = Blob {
                mime: artifact
                    .media_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".to_owned()),
                length_octets: artifact
                    .size
                    .expect("admission refuses an artifact without a size"),
                digest: artifact.digest.to_string(),
                inline: None,
                urls: None,
                extra,
            };
            let slot = match kept {
                Some((value, _)) if *value == desired => stored[key].clone(),
                _ => {
                    // The datum wire form omits an unset option rather than writing `null`.
                    let mut raw = serde_json::to_value(&desired).expect("a blob serializes");
                    raw.as_object_mut()
                        .expect("a blob serializes as an object")
                        .retain(|_, value| !value.is_null());
                    DatumSlot::Accepted {
                        value: desired,
                        raw: Some(raw),
                    }
                }
            };
            (key.clone(), slot)
        })
        .collect()
}

/// Rewrites one part of the grants datum, keeping what `edit` does not touch (the other grant
/// kind and unknown members). An invalid or absent datum starts from empty, as a rewrite of it
/// always has.
fn apply_grants_slot(slot: &mut DatumSlot<NoduleGrants>, edit: impl FnOnce(&mut NoduleGrants)) {
    let mut grants = match slot {
        DatumSlot::Accepted { value, .. } => value.clone(),
        _ => NoduleGrants::default(),
    };
    edit(&mut grants);
    *slot = if grants.is_empty() {
        DatumSlot::Absent
    } else {
        DatumSlot::Accepted {
            value: grants,
            raw: None,
        }
    };
}

fn drawer_entries(drawers: BTreeMap<DrawerId, DrawerAccess>) -> BTreeMap<DrawerId, EntryAccess> {
    drawers
        .into_iter()
        .map(|(drawer_id, access)| (drawer_id, EntryAccess::from(access)))
        .collect()
}

impl NoduleConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entry(&self, cartridge_id: &CartridgeId) -> Option<&NoduleEntry> {
        self.entries.get(cartridge_id)
    }

    pub fn entries(&self) -> &BTreeMap<CartridgeId, NoduleEntry> {
        &self.entries
    }

    pub fn carried_keys(&self) -> &BTreeMap<DatumKey, DatumRead> {
        &self.carried_keys
    }

    pub fn remove_carried_key(&mut self, key: &DatumKey) -> bool {
        self.carried_keys.remove(key).is_some()
    }

    /// Validates `cartridge`, admits its nodule contribution, checks authority conflicts and existing
    /// settings compatibility, and updates the [`NoduleInstall`] (and optionally [`NoduleGrants`])
    /// slots for `cartridge.id`.
    pub fn admit(
        &mut self,
        cache: &AdmissionCache,
        cartridge: &Cartridge,
        drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
    ) -> Result<InstalledNodule, InventoryError> {
        let (nodule, admitted, artifacts) = admit_cartridge_nodule(cartridge)?;
        let revision = cartridge.revision();
        let (enabled, install_extra, config, configured) =
            if let Some(existing_entry) = self.entries.get(&cartridge.id) {
                if let Some(existing_install) = existing_entry.install.accepted()
                    && let Ok(existing_cartridge) = existing_install.decode_cartridge()
                    && existing_cartridge.version == cartridge.version
                    && existing_install.revision != revision
                {
                    return Err(InventoryError::Cartridge(CartridgeError::Conflict {
                        id: cartridge.id.clone(),
                        version: Box::new(cartridge.version.clone()),
                        existing: existing_install.revision,
                        incoming: revision,
                    }));
                }
                if drawers.is_none()
                    && let DatumSlot::Invalid { reason, .. } = &existing_entry.grants
                {
                    return Err(InventoryError::InvalidContribution {
                        id: cartridge.id.clone(),
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
                    // Explicit settings are carried verbatim even when the incoming revision's
                    // schema no longer accepts them (or it declares none): the stored datum is
                    // untouched, `resolve` refuses the nodule with the schema error, and
                    // `configure` validates against this newly installed revision so rewriting the
                    // settings repairs it. Rejecting the upgrade here would wedge the operator,
                    // who could not repair settings against the old revision.
                    DatumSlot::Accepted { value, .. } => (Some(value.values.clone()), true),
                    DatumSlot::Invalid { reason, .. } => {
                        return Err(InventoryError::InvalidContribution {
                            id: cartridge.id.clone(),
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

        let mut install = NoduleInstall::from_cartridge(cartridge, enabled);
        install.extra = install_extra;
        let entry = self.entries.entry(cartridge.id.clone()).or_default();
        entry.install = DatumSlot::Accepted {
            value: install,
            raw: None,
        };
        if let Some(drawers) = drawers {
            apply_grants_slot(&mut entry.grants, |grants| {
                grants.drawers = drawer_entries(drawers);
            });
        }
        entry.artifacts = artifact_slots(&entry.artifacts, &artifacts);
        // A cartridge installed disabled holds nothing yet; an upgrade of an enabled one must not
        // take a prefix another enabled slot holds.
        if enabled {
            self.ensure_no_authority_conflict(cache, &cartridge.id)?;
        }

        Ok(InstalledNodule::new(
            cartridge.id.clone(),
            cartridge.version.clone(),
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
        cartridge_id: &CartridgeId,
        settings: IndexMap<String, Value>,
    ) -> Result<InstalledNodule, InventoryError> {
        let (cartridge, nodule, admitted, artifacts, enabled) =
            self.validate_installed_cartridge(cartridge_id)?;
        if nodule.config.is_none() {
            return Err(InventoryError::NoConfigSchema(cartridge_id.clone()));
        }
        nodule.validate_config(&settings, "config")?;
        let entry = self
            .entries
            .get_mut(cartridge_id)
            .expect("verified installed");
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
            cartridge.id.clone(),
            cartridge.version.clone(),
            cartridge.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            Some(settings),
            true,
        ))
    }

    /// Enabling is where two holders of one prefix are refused (ADR 028 §5), and where a cartridge
    /// that fills another slot is refused its own: one code lineage per slot.
    pub fn set_enabled(
        &mut self,
        cache: &AdmissionCache,
        cartridge_id: &CartridgeId,
        enabled: bool,
    ) -> Result<InstalledNodule, InventoryError> {
        let (cartridge, nodule, admitted, artifacts, _, (config, configured)) =
            self.validate_installed(cartridge_id)?;
        if enabled && let Some(slot) = self.slot_filled_by(cartridge_id) {
            return Err(InventoryError::InvalidDevTakeover {
                id: slot,
                reason: format!(
                    "`{cartridge_id}` fills it, so it cannot also be enabled on its own"
                ),
            });
        }
        let entry = self
            .entries
            .get_mut(cartridge_id)
            .expect("verified installed");
        if let DatumSlot::Accepted { value, raw } = &mut entry.install
            && value.enabled != enabled
        {
            value.enabled = enabled;
            *raw = None;
        }
        if enabled {
            self.ensure_no_authority_conflict(cache, cartridge_id)?;
        }
        Ok(InstalledNodule::new(
            cartridge.id.clone(),
            cartridge.version.clone(),
            cartridge.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        ))
    }

    /// Makes every stored dev takeover refuse its slot when the slot is resolved, as a datum that
    /// does not decode does: a node that did not opt into takeovers (ADR 045 §5) must not run
    /// another cartridge as a slot because a datum arrived in its config document some other way.
    /// A view only: it never reaches `diff`, and `set_dev_takeover(.., None)` clears the datum.
    pub fn refuse_dev_takeover(&mut self) {
        for entry in self.entries.values_mut() {
            if let DatumSlot::Accepted { raw, .. } = &entry.dev_takeover {
                entry.dev_takeover = DatumSlot::Invalid {
                    raw: raw.clone(),
                    reason: "dev slot takeover is off on this node".to_owned(),
                };
            }
        }
    }

    /// The routes in force, pattern to provider id. A route datum that does not decode is an
    /// error rather than a skipped route: dropping it would silently send its calls to whichever
    /// provider claims them. `set_route` with no provider clears it.
    pub fn routes(&self) -> std::result::Result<BTreeMap<String, String>, InventoryError> {
        self.routes
            .iter()
            .filter_map(|(pattern, slot)| match slot {
                DatumSlot::Absent => None,
                DatumSlot::Accepted { value, .. } => {
                    Some(Ok((pattern.clone(), value.provider.clone())))
                }
                DatumSlot::Invalid { reason, .. } => Some(Err(InventoryError::InvalidRoute {
                    pattern: pattern.clone(),
                    reason: reason.clone(),
                })),
            })
            .collect()
    }

    /// Sets or, with `provider` absent, clears the route of `pattern`: an exact NSID or an
    /// authority prefix, dotted segments of letters, digits and hyphens. Whether the provider
    /// exists is not checked: a route may be set before its target registers, and callers are
    /// told `RouteTargetMissing` until it does.
    pub fn set_route(
        &mut self,
        pattern: &str,
        provider: Option<String>,
    ) -> std::result::Result<(), InventoryError> {
        let invalid = |reason: &str| InventoryError::InvalidRoute {
            pattern: pattern.to_owned(),
            reason: reason.to_owned(),
        };
        let Some(provider) = provider else {
            self.routes.remove(pattern);
            return Ok(());
        };
        if !pattern.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        }) {
            return Err(invalid(
                "a pattern is dotted segments of letters, digits and hyphens",
            ));
        }
        if provider.is_empty() {
            return Err(invalid("the provider id is empty"));
        }
        let extra = match self.routes.get(pattern) {
            Some(DatumSlot::Accepted { value, .. }) => value.extra.clone(),
            _ => Extra::default(),
        };
        self.routes.insert(
            pattern.to_owned(),
            DatumSlot::Accepted {
                value: MethodRoute { provider, extra },
                raw: None,
            },
        );
        Ok(())
    }

    /// The slot `cartridge_id`'s code fills under a dev takeover, if any.
    fn slot_filled_by(&self, cartridge_id: &CartridgeId) -> Option<CartridgeId> {
        self.entries.iter().find_map(|(slot, entry)| {
            entry
                .dev_takeover
                .accepted()
                .is_some_and(|takeover| &takeover.by == cartridge_id)
                .then(|| slot.clone())
        })
    }

    /// Sets or, with `by` absent, clears the dev takeover of slot `cartridge_id`: from now on `by`'s
    /// code and manifest run in the slot, which keeps everything else it has (actor, settings,
    /// grants, enablement, initializer done-ness, lockfile, dispatch registrations). Clearing
    /// reinstalls the slot's own envelope, which was never touched. Whether the node allows
    /// takeovers is the caller's gate, not checked here.
    ///
    /// Drawer grants are inherited in full, since testing a dev build against the real data is
    /// the hook's purpose. Method grants never widen silently (ADR 028 §3): a different
    /// publisher's manifest asks for a different set of things, and a granted authority prefix
    /// would otherwise cover whatever it newly asks for. So what reaches the filler is narrowed
    /// when the slot is resolved (`substituted_grants`) to exactly the operations it requests
    /// *identically* (binding, contract and access) to a request of the slot's own install, and
    /// every other request it makes is ungranted (a required one holds the nodule at
    /// `NeedsConfiguration`) until the operator grants it. The stored grants are untouched, so
    /// clearing the takeover restores exactly what was granted.
    pub fn set_dev_takeover(
        &mut self,
        cache: &AdmissionCache,
        cartridge_id: &CartridgeId,
        by: Option<CartridgeId>,
    ) -> Result<(), InventoryError> {
        let invalid = |reason: String| InventoryError::InvalidDevTakeover {
            id: cartridge_id.clone(),
            reason,
        };
        self.validate_installed_cartridge(cartridge_id)?;
        let Some(by) = by else {
            self.entries
                .get_mut(cartridge_id)
                .expect("verified installed")
                .dev_takeover = DatumSlot::Absent;
            return Ok(());
        };
        if &by == cartridge_id {
            return Err(invalid("a cartridge cannot take over itself".into()));
        }
        // Reuses `resolve`'s checks, so what is refused here is exactly what would be refused
        // when reconciling.
        let entry = &self.entries[cartridge_id];
        let install = entry.install.accepted().expect("verified installed");
        filler(cartridge_id, install, &by, &self.entries, cache).map_err(invalid)?;
        self.validate_installed_cartridge(&by)?;
        if self.entries[&by]
            .install
            .accepted()
            .is_some_and(|install| install.enabled)
        {
            return Err(invalid(format!(
                "`{by}` is enabled on its own, and one code lineage cannot fill the slot while \
                 running beside it"
            )));
        }
        // The slot's stored grants are left as configured: what reaches the filler is narrowed
        // when the slot is resolved (`resolve_entry`), so clearing the takeover restores them.
        let entry = self
            .entries
            .get_mut(cartridge_id)
            .expect("verified installed");
        let extra = match &entry.dev_takeover {
            DatumSlot::Accepted { value, .. } => value.extra.clone(),
            _ => Extra::default(),
        };
        entry.dev_takeover = DatumSlot::Accepted {
            value: NoduleDevTakeover { by, extra },
            raw: None,
        };
        if self.entries[cartridge_id]
            .install
            .accepted()
            .is_some_and(|install| install.enabled)
        {
            self.ensure_no_authority_conflict(cache, cartridge_id)?;
        }
        Ok(())
    }

    pub fn set_drawer_grants(
        &mut self,
        cartridge_id: &CartridgeId,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
    ) -> Result<InstalledNodule, InventoryError> {
        let (cartridge, nodule, admitted, artifacts, enabled, (config, configured)) =
            self.validate_installed(cartridge_id)?;
        let entry = self
            .entries
            .get_mut(cartridge_id)
            .expect("verified installed");
        apply_grants_slot(&mut entry.grants, |grants| {
            grants.drawers = drawer_entries(drawers);
        });
        Ok(InstalledNodule::new(
            cartridge.id.clone(),
            cartridge.version.clone(),
            cartridge.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        ))
    }

    /// Replaces the methods the nodule may invoke through dispatch, keeping its drawer grants.
    pub fn set_method_grants(
        &mut self,
        cartridge_id: &CartridgeId,
        methods: DispatchScope,
    ) -> Result<InstalledNodule, InventoryError> {
        let (cartridge, nodule, admitted, artifacts, enabled, (config, configured)) =
            self.validate_installed(cartridge_id)?;
        let entry = self
            .entries
            .get_mut(cartridge_id)
            .expect("verified installed");
        apply_grants_slot(&mut entry.grants, |grants| {
            // An empty scope is stored as no value at all; a rewrite keeps the unknown members of
            // the previous value while there is one.
            let extra = grants
                .methods
                .take()
                .map(|previous| previous.extra)
                .unwrap_or_default();
            grants.methods = (methods != DispatchScope::default()).then(|| MethodGrants {
                extra,
                ..MethodGrants::from(methods)
            });
        });
        Ok(InstalledNodule::new(
            cartridge.id.clone(),
            cartridge.version.clone(),
            cartridge.revision(),
            nodule,
            admitted,
            artifacts,
            enabled,
            config,
            configured,
        ))
    }

    pub fn uninstall(&mut self, cartridge_id: &CartridgeId) -> Result<(), InventoryError> {
        let Some(entry) = self.entries.remove(cartridge_id) else {
            return Err(InventoryError::UnknownCartridge(cartridge_id.clone()));
        };
        if entry.is_empty() {
            return Err(InventoryError::UnknownCartridge(cartridge_id.clone()));
        }
        Ok(())
    }

    /// Evaluates every installed nodule entry independently, reporting per-nodule refusals without
    /// failing unrelated valid nodules. Orphan settings or grants (where `install` is absent) do
    /// not install a nodule.
    pub fn resolve(&self, cache: &AdmissionCache) -> BTreeMap<CartridgeId, NoduleResolution> {
        let mut results = self.resolve_slots(cache);
        // Authority conflicts first: a refused slot holds nothing, so it does not take part in
        // the Lexicon comparison below.
        for conflicts in [authority_conflicts, lexicon_conflicts] {
            for (cartridge_id, error) in conflicts(&results) {
                if let Some(NoduleResolution::Admitted { installed, .. }) =
                    results.remove(&cartridge_id)
                {
                    results.insert(
                        cartridge_id.clone(),
                        NoduleResolution::Refused {
                            cartridge_id,
                            version: Some(installed.version),
                            revision: Some(installed.revision),
                            enabled: installed.enabled,
                            config: installed.config,
                            reason: error.to_string(),
                        },
                    );
                }
            }
        }
        cache.retain(self.entries.keys());

        results
    }

    fn resolve_slots(&self, cache: &AdmissionCache) -> BTreeMap<CartridgeId, NoduleResolution> {
        let mut results = BTreeMap::new();
        for (cartridge_id, entry) in &self.entries {
            if let Some(resolution) = resolve_entry(cartridge_id, entry, &self.entries, cache) {
                results.insert(cartridge_id.clone(), resolution);
            }
        }
        results
    }

    /// Fails when `cartridge_id`'s slot holds an authority prefix that another enabled slot also
    /// holds.
    fn ensure_no_authority_conflict(
        &self,
        cache: &AdmissionCache,
        cartridge_id: &CartridgeId,
    ) -> Result<(), InventoryError> {
        match authority_conflicts(&self.resolve_slots(cache)).remove(cartridge_id) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    #[expect(
        clippy::type_complexity,
        reason = "internal helper returning decoded cartridge components"
    )]
    fn validate_installed_cartridge(
        &self,
        cartridge_id: &CartridgeId,
    ) -> Result<
        (
            Cartridge,
            Nodule,
            crate::Admitted,
            BTreeMap<
                String,
                (
                    big_cartridges_core::Artifact,
                    crate::inventory::ArtifactState,
                ),
            >,
            bool,
        ),
        InventoryError,
    > {
        let Some(entry) = self.entries.get(cartridge_id) else {
            return Err(InventoryError::UnknownCartridge(cartridge_id.clone()));
        };
        let install = match &entry.install {
            DatumSlot::Absent => {
                return Err(InventoryError::UnknownCartridge(cartridge_id.clone()));
            }
            DatumSlot::Invalid { reason, .. } => {
                return Err(InventoryError::InvalidContribution {
                    id: cartridge_id.clone(),
                    reason: format!("install datum is invalid: {reason}"),
                });
            }
            DatumSlot::Accepted { value, .. } => value,
        };
        let cartridge =
            install
                .decode_cartridge()
                .map_err(|reason| InventoryError::InvalidContribution {
                    id: cartridge_id.clone(),
                    reason,
                })?;
        if &cartridge.id != cartridge_id {
            return Err(InventoryError::InvalidContribution {
                id: cartridge_id.clone(),
                reason: format!(
                    "install datum key `{cartridge_id}` does not match envelope id `{}`",
                    cartridge.id
                ),
            });
        }
        let (nodule, admitted, artifacts) = admit_cartridge_nodule(&cartridge)?;
        Ok((cartridge, nodule, admitted, artifacts, install.enabled))
    }

    #[expect(
        clippy::type_complexity,
        reason = "internal helper returning decoded cartridge and settings components"
    )]
    fn validate_installed(
        &self,
        cartridge_id: &CartridgeId,
    ) -> Result<
        (
            Cartridge,
            Nodule,
            crate::Admitted,
            BTreeMap<
                String,
                (
                    big_cartridges_core::Artifact,
                    crate::inventory::ArtifactState,
                ),
            >,
            bool,
            (Option<IndexMap<String, Value>>, bool),
        ),
        InventoryError,
    > {
        let (cartridge, nodule, admitted, artifacts, enabled) =
            self.validate_installed_cartridge(cartridge_id)?;
        let entry = self.entries.get(cartridge_id).expect("verified installed");
        let config_state = match &entry.settings {
            DatumSlot::Absent => (
                nodule
                    .config
                    .as_ref()
                    .and_then(|config| config.defaults.clone()),
                false,
            ),
            // Settings that do not conform to this revision are reported by `resolve` as a
            // refusal; they must not block enabling or granting, only `configure` repairs them.
            DatumSlot::Accepted { value, .. } => (Some(value.values.clone()), true),
            DatumSlot::Invalid { reason, .. } => {
                return Err(InventoryError::InvalidContribution {
                    id: cartridge_id.clone(),
                    reason: format!("settings datum is invalid: {reason}"),
                });
            }
        };
        Ok((
            cartridge,
            nodule,
            admitted,
            artifacts,
            enabled,
            config_state,
        ))
    }
}

/// The slots whose enabled authority equals or nests with another enabled slot's. The authority
/// of a slot is the one its running code holds (a filler's, under a dev takeover), so a
/// takeover is exempt from the rule exactly because the original install is not running.
fn authority_conflicts(
    results: &BTreeMap<CartridgeId, NoduleResolution>,
) -> BTreeMap<CartridgeId, InventoryError> {
    // A cartridge conflicts with another when one authority equals or dot-prefixes the other.
    // Looking each authority up by itself, by each of its dotted prefixes and by the key range
    // of its descendants is O(n * depth) rather than comparing every pair. The smallest
    // conflicting `CartridgeId` is reported so the reason is stable. Every NSID a nodule holds is
    // under its authority, so this subsumes overlap between held NSIDs.
    let enabled_authority = |resolution: &'_ NoduleResolution| match resolution {
        NoduleResolution::Admitted { installed, .. } if installed.enabled => {
            installed.admitted.authority.clone()
        }
        _ => None,
    };
    let mut authorities: BTreeMap<String, Vec<&CartridgeId>> = BTreeMap::new();
    for (cartridge_id, resolution) in results {
        if let Some(authority) = enabled_authority(resolution) {
            authorities.entry(authority).or_default().push(cartridge_id);
        }
    }
    results
        .iter()
        .filter_map(|(cartridge_id, resolution)| {
            let id = enabled_authority(resolution)?;
            let ancestors = id
                .match_indices('.')
                .filter_map(|(dot, _)| authorities.get_key_value(&id[..dot]));
            let same_or_descendants = authorities
                .range::<str, _>((Bound::Included(id.as_str()), Bound::Unbounded))
                .take_while(|(other, _)| DispatchScope::authority_owns(&id, other));
            let (other_cartridge, other_authority) = ancestors
                .chain(same_or_descendants)
                .flat_map(|(other, holders)| {
                    holders
                        .iter()
                        .filter(|holder| **holder != cartridge_id)
                        .map(move |holder| (*holder, other))
                })
                .min_by_key(|(holder, _)| *holder)?;
            Some((
                cartridge_id.clone(),
                InventoryError::AuthorityConflict {
                    id: cartridge_id.clone(),
                    authority: id.clone(),
                    existing_cartridge: Box::new(other_cartridge.clone()),
                    existing_authority: other_authority.clone(),
                },
            ))
        })
        .collect()
}

/// The enabled slots whose copy of a Lexicon conflicts with another enabled slot's. Every cartridge
/// vendors the Lexicons it uses or provides, so one NSID can arrive as several copies, and the
/// registry never picks between copies that disagree: it refuses the ones it cannot reconcile.
///
/// Two copies agree when they are identical, or when both declare a `revision` and the revisions
/// differ: within an NSID only compatible growth is allowed (ADR 028 §4), so the lower revision
/// is a subset of the higher and each consumer's `minRevision` decides which it needs (the
/// npm/OSGi reading of a version range, with the AT Protocol rule that an NSID names one
/// evolving definition). Anything else, a different body at one revision or a body with no
/// revision to order it by, is a conflict. The cartridge holding the NSID's authority (ADR 028 §5,
/// implementing is not owning) is the definition: it is never refused for a vendor's copy, and a
/// vendored copy that differs from it, or that declares a higher revision than it, is refused.
/// With no holder enabled, every copy in a conflicting pair is refused. The smallest conflicting
/// `CartridgeId` is reported so the reason is stable.
fn lexicon_conflicts(
    results: &BTreeMap<CartridgeId, NoduleResolution>,
) -> BTreeMap<CartridgeId, InventoryError> {
    struct Copy<'a> {
        cartridge_id: &'a CartridgeId,
        held: bool,
        revision: Option<u32>,
        file: &'a IndexMap<String, Value>,
    }
    let mut copies: BTreeMap<&str, Vec<Copy<'_>>> = BTreeMap::new();
    for (cartridge_id, resolution) in results {
        let NoduleResolution::Admitted { installed, .. } = resolution else {
            continue;
        };
        if !installed.enabled {
            continue;
        }
        let authority = installed.admitted.authority.as_deref();
        for file in &installed.nodule.lexicons {
            let nsid = file
                .get("id")
                .and_then(Value::as_str)
                .expect("admission checked every Lexicon states its NSID");
            copies.entry(nsid).or_default().push(Copy {
                cartridge_id,
                held: authority
                    .is_some_and(|authority| DispatchScope::authority_owns(authority, nsid)),
                revision: installed.nodule.lexicon_revision(nsid),
                file,
            });
        }
    }
    let mut conflicts: BTreeMap<CartridgeId, InventoryError> = BTreeMap::new();
    for (nsid, group) in &copies {
        for copy in group {
            // The copy `copy` is refused against: the first (smallest cartridge id) copy it cannot
            // coexist with, where the holder's copy is never the one refused.
            let against = group
                .iter()
                .filter(|other| other.cartridge_id != copy.cartridge_id)
                .filter(|other| !(copy.held && !other.held))
                .find(|other| {
                    if other.file == copy.file {
                        return false;
                    }
                    let ordered = matches!(
                        (copy.revision, other.revision),
                        (Some(mine), Some(theirs)) if mine != theirs
                    );
                    if !ordered {
                        return true;
                    }
                    // Ordered copies coexist, but a vendor may not out-revise the holder.
                    other.held && copy.revision > other.revision
                });
            if let Some(other) = against {
                conflicts
                    .entry(copy.cartridge_id.clone())
                    .or_insert_with(|| InventoryError::LexiconConflict {
                        id: copy.cartridge_id.clone(),
                        nsid: (*nsid).to_owned(),
                        other: Box::new(other.cartridge_id.clone()),
                    });
            }
        }
    }
    conflicts
}

/// The install whose code fills slot `cartridge_id` under a dev takeover by `by`, with its admission.
///
/// The filler must be installed and admissible, and its NSID authority must be absent or equal
/// to the authority the slot's own install holds: only a takeover may hold another cartridge's
/// prefix (ADR 028 §5), and a fork that renamed its prefix is a different slot, not a drop-in.
fn filler<'a>(
    cartridge_id: &CartridgeId,
    install: &NoduleInstall,
    by: &CartridgeId,
    entries: &'a BTreeMap<CartridgeId, NoduleEntry>,
    cache: &AdmissionCache,
) -> Result<(&'a NoduleInstall, Arc<Admission>), String> {
    let Some(filler_install) = entries.get(by).and_then(|entry| entry.install.accepted()) else {
        return Err(format!("dev build `{by}` is not installed"));
    };
    let admission = cache.admission(by, filler_install);
    let Admission::Admitted {
        admitted: filler_admitted,
        ..
    } = &*admission
    else {
        return Err(format!("dev build `{by}` is not admissible"));
    };
    let Admission::Admitted {
        admitted: own_admitted,
        ..
    } = &*cache.admission(cartridge_id, install)
    else {
        return Err(format!(
            "the slot's own install is not admissible, so dev build `{by}` cannot be checked against it"
        ));
    };
    if filler_admitted.authority.is_some() && filler_admitted.authority != own_admitted.authority {
        return Err(format!(
            "dev build `{by}` holds authority `{}`, not this cartridge's ({}): a takeover \
             holds the same prefix or none",
            filler_admitted.authority.as_deref().unwrap_or_default(),
            own_admitted
                .authority
                .as_deref()
                .map_or("none".to_owned(), |authority| format!("`{authority}`")),
        ));
    }
    Ok((filler_install, admission))
}

/// The method grants that reach the code filling a slot under a dev takeover. The slot's grants were
/// given for its own code's requests: a request the filler shares with that code (same binding,
/// contract and access) keeps whatever grant covered it, while one the filler adds or widens is
/// covered only by a grant naming that exact operation. A prefix grant is broad consent given for
/// the slot's own code and never silently stretches to the filler's additions (ADR 028 §3), so
/// granting one means naming it. This is computed, never written back: the stored grants are
/// untouched, and clearing the takeover restores exactly what was granted.
fn substituted_grants(own: &Nodule, filler: &Nodule, granted: &DispatchScope) -> DispatchScope {
    DispatchScope {
        operations: filler
            .operation_requests()
            .filter(|request| {
                let shared = own.requires.iter().any(|own| {
                    (&own.binding, &own.contract, &own.access)
                        == (&request.binding, &request.contract, &request.access)
                });
                if shared {
                    granted.permits(&request.contract)
                } else {
                    granted.operations.contains(&request.contract)
                }
            })
            .map(|request| request.contract.clone())
            .collect(),
        authorities: BTreeSet::new(),
        privileged: granted.privileged,
    }
}

/// The install whose code runs in slot `cartridge_id`, its admission and, under a dev takeover, the
/// cartridge filling it: the slot's own install unless its takeover datum names another.
fn code_source<'a>(
    cartridge_id: &CartridgeId,
    entry: &'a NoduleEntry,
    install: &'a NoduleInstall,
    entries: &'a BTreeMap<CartridgeId, NoduleEntry>,
    cache: &AdmissionCache,
) -> Result<(&'a NoduleInstall, Arc<Admission>, Option<CartridgeId>), String> {
    match &entry.dev_takeover {
        DatumSlot::Absent => Ok((install, cache.admission(cartridge_id, install), None)),
        DatumSlot::Invalid { reason, .. } => Err(format!("invalid devTakeover datum: {reason}")),
        DatumSlot::Accepted { value, .. } => {
            let (source, admission) = filler(cartridge_id, install, &value.by, entries, cache)?;
            Ok((source, admission, Some(value.by.clone())))
        }
    }
}

fn resolve_entry(
    cartridge_id: &CartridgeId,
    entry: &NoduleEntry,
    entries: &BTreeMap<CartridgeId, NoduleEntry>,
    cache: &AdmissionCache,
) -> Option<NoduleResolution> {
    let install = match &entry.install {
        DatumSlot::Absent => return None,
        DatumSlot::Invalid { reason, .. } => {
            return Some(NoduleResolution::Refused {
                cartridge_id: cartridge_id.clone(),
                version: None,
                revision: None,
                enabled: false,
                config: None,
                reason: format!("invalid install datum: {reason}"),
            });
        }
        DatumSlot::Accepted { value, .. } => value,
    };

    let refuse_at = |revision: Revision,
                     version: Option<Version>,
                     config: Option<IndexMap<String, Value>>,
                     reason: String| {
        NoduleResolution::Refused {
            cartridge_id: cartridge_id.clone(),
            version,
            revision: Some(revision),
            enabled: install.enabled,
            config,
            reason,
        }
    };

    // The code that runs in this slot: its own install, or the filler's under a dev takeover.
    // Everything else about the slot (settings, grants, enablement) is the slot's own.
    let (source, admission, filled_from) =
        match code_source(cartridge_id, entry, install, entries, cache) {
            Ok(source) => source,
            Err(reason) => {
                return Some(refuse_at(
                    install.revision,
                    None,
                    entry.settings.accepted().map(|s| s.values.clone()),
                    reason,
                ));
            }
        };
    let refuse =
        |version: Option<Version>, config: Option<IndexMap<String, Value>>, reason: String| {
            refuse_at(source.revision, version, config, reason)
        };
    let (version, nodule, admitted, artifacts) = match &*admission {
        Admission::Refused { version, reason } => {
            return Some(refuse(
                version.clone(),
                entry.settings.accepted().map(|s| s.values.clone()),
                reason.clone(),
            ));
        }
        Admission::Admitted {
            version,
            nodule,
            admitted,
            artifacts,
        } => (
            version.clone(),
            nodule.clone(),
            admitted.clone(),
            artifacts.clone(),
        ),
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
                Some(version.clone()),
                None,
                format!("invalid settings datum: {reason}"),
            ));
        }
        DatumSlot::Accepted { value, .. } => {
            if nodule.config.is_none() {
                return Some(refuse(
                    Some(version.clone()),
                    Some(value.values.clone()),
                    InventoryError::NoConfigSchema(cartridge_id.clone()).to_string(),
                ));
            }
            if let Err(err) = nodule.validate_config(&value.values, "config") {
                return Some(refuse(
                    Some(version.clone()),
                    Some(value.values.clone()),
                    err.to_string(),
                ));
            }
            (Some(value.values.clone()), true)
        }
    };

    let mut desired_grants = BTreeMap::new();
    let mut desired_operations = DispatchScope::default();
    match &entry.grants {
        DatumSlot::Absent => {}
        DatumSlot::Invalid { reason, .. } => {
            return Some(refuse(
                Some(version.clone()),
                config,
                format!("invalid grants datum: {reason}"),
            ));
        }
        DatumSlot::Accepted { value, .. } => {
            for (drawer_id, access) in &value.drawers {
                let Some(known) = access.known() else {
                    return Some(refuse(
                        Some(version.clone()),
                        config,
                        format!(
                            "unknown drawer access level `{}` for drawer `{drawer_id}`",
                            access.as_str()
                        ),
                    ));
                };
                desired_grants.insert(drawer_id.clone(), known);
            }
            desired_operations = value.methods.clone().map(Into::into).unwrap_or_default();
        }
    }
    if filled_from.is_some() {
        let Admission::Admitted { nodule: own, .. } = &*cache.admission(cartridge_id, install)
        else {
            unreachable!("code_source admitted the slot's own install while checking the filler");
        };
        desired_operations = substituted_grants(own, &nodule, &desired_operations);
    }

    let mut installed = InstalledNodule::new(
        cartridge_id.clone(),
        version.clone(),
        source.revision,
        nodule,
        admitted,
        artifacts,
        install.enabled,
        config,
        configured,
    );
    installed.filled_by = filled_from.map(|filler| FilledBy {
        cartridge_id: filler,
        version: version.clone(),
    });
    Some(NoduleResolution::Admitted {
        installed,
        desired_grants,
        desired_operations,
    })
}
