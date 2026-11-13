//! Portable nodule configuration datums and the [`DatumStore`] projections over the two kinds of
//! document that hold them (ADR 027 §4, §6; ADR 044).
//!
//! The node's config document ([`NoduleConfig`], this node only) holds, per installed cartridge:
//! - [`CartridgeInstall`]: `org.example.daybook.cartridges.install/<cartridgeId>`, a pin of the
//!   published version and each component's enablement on this node;
//! - [`NoduleDevTakeover`]: `org.example.daybook.nodule.devTakeover/<cartridgeId>`, present only
//!   while another installed cartridge fills the slot;
//! - the handler artifact pins, `org.example.daybook.blob/nodule.artifact/<cartridgeId>/<artifactKey>`
//!   [`Blob`] datums (see [`artifact_key`]), which is what makes big_blobs retain and fetch the
//!   bytes;
//! - the node-local method routes ([`MethodRoute`]).
//!
//! The cartridge's own config document ([`CartridgeConfig`], one per cartridge in the shared
//! config drawer, so it follows the user to sister nodes) holds its settings datums, which are
//! ordinary datums under the NSIDs its manifest declares and are never interpreted here, and one
//! grant datum per grant (see [`GrantKey`]).
//!
//! [`NoduleConfig`] hydrates from the node document and is handed every cartridge document an
//! install names ([`NoduleConfig::attach_cartridge_docs`]). Both projections preserve unknown
//! fields, unknown vocabulary values, and undecodable/rejected/pending entries so unrelated edits
//! never delete them, and [`NoduleConfig::resolve`] evaluates each nodule's desired state
//! independently for the reconciler.

use crate::Nodule;
use crate::NoduleAdmission;
use crate::inventory::FilledBy;
use crate::inventory::InstalledNodule;
use crate::inventory::InventoryError;
use crate::inventory::admit_cartridge_nodule;
use big_blobs_core::Blob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumPin;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::doc::DatumPatch;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumStore;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeError;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;
use big_cartridges_core::config::CartridgeConfigIdentity;
use big_cartridges_core::config::CartridgeInstall;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::ComponentInstall;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::config::MicroGrant;
use big_cartridges_core::config::NoduleGrant;
use big_cartridges_core::config::config_identity_key;
use big_cartridges_core::config::install_key;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Route;
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

/// The portable nodule configuration datum declarations: the cartridge-level install and grant
/// datums, the dev takeover and the route.
#[cfg(feature = "schemars")]
pub fn config_universe() -> big_cabinet_core::datum::Universe {
    use big_cabinet_core::datum::Declaration;
    use big_cabinet_core::datum::Universe;
    let mut declarations = big_cartridges_core::config::config_declarations();
    declarations.extend([
        Declaration::of::<NoduleDevTakeover>().expect("NoduleDevTakeover declaration is valid"),
        Declaration::of::<MethodRoute>().expect("MethodRoute declaration is valid"),
    ]);
    Universe::collect(declarations)
}

pub fn dev_takeover_key(cartridge_id: &CartridgeId) -> DatumKey {
    DatumKey::from_text(NoduleDevTakeover::ID, cartridge_id.to_string())
        .expect("a CartridgeId is a valid datum key slot")
}

pub fn route_key(pattern: &str) -> DatumKey {
    DatumKey::from_text(MethodRoute::ID, pattern)
        .expect("a validated route pattern is a valid datum key slot")
}

/// Why `pattern` cannot be a route pattern: dotted segments of letters, digits and hyphens, so
/// it is an exact NSID or an authority prefix and never `*`.
fn pattern_error(pattern: &str) -> Option<&'static str> {
    (!pattern.split('.').all(|segment| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    }))
    .then_some("a pattern is dotted segments of letters, digits and hyphens")
}

/// Why `pattern` routed to `provider` is not a valid route.
fn route_error(pattern: &str, provider: &str) -> Option<&'static str> {
    pattern_error(pattern).or_else(|| provider.is_empty().then_some("the provider id is empty"))
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

/// The configuration datum slots for one [`CartridgeId`] in the node's config document: its
/// install, its dev takeover, and the artifact [`Blob`] references by artifact key. An artifact
/// slot present here is always in the document or about to be; absence from the map is absence
/// from the document.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NoduleEntry {
    pub install: DatumSlot<CartridgeInstall>,
    pub dev_takeover: DatumSlot<NoduleDevTakeover>,
    pub artifacts: BTreeMap<String, DatumSlot<Blob>>,
}

impl NoduleEntry {
    pub fn is_empty(&self) -> bool {
        self.install.is_absent() && self.dev_takeover.is_absent() && self.artifacts.is_empty()
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
        reason: String,
    },
}

/// What admitting one pinned cartridge version established, kept so the same version is not
/// admitted again on every reconcile.
#[expect(
    clippy::large_enum_variant,
    reason = "always held behind an Arc in the admission cache"
)]
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    Admitted {
        version: Version,
        revision: Revision,
        resolved: ResolvedCartridge,
        nodule: Nodule,
        admitted: crate::Admitted,
        artifacts: BTreeMap<String, (crate::inventory::Artifact, crate::inventory::ArtifactState)>,
    },
    /// The version cannot be installed as a nodule, or could not be read; `version` and `revision`
    /// are what is known of it for the refusal record.
    Refused {
        version: Option<Version>,
        revision: Option<Revision>,
        reason: String,
    },
}

impl Admission {
    /// What admitting `cartridge`'s nodule contribution establishes.
    pub fn of(cartridge: &Cartridge) -> Self {
        let revision = cartridge.revision();
        match admit_cartridge_nodule(cartridge) {
            Ok((resolved, nodule, admitted, artifacts)) => Self::Admitted {
                version: cartridge.version.clone(),
                revision,
                resolved,
                nodule,
                admitted,
                artifacts,
            },
            Err(err) => Self::Refused {
                version: Some(cartridge.version.clone()),
                revision: Some(revision),
                reason: err.to_string(),
            },
        }
    }

    pub fn version(&self) -> Option<&Version> {
        match self {
            Self::Admitted { version, .. } => Some(version),
            Self::Refused { version, .. } => version.as_ref(),
        }
    }

    pub fn revision(&self) -> Option<Revision> {
        match self {
            Self::Admitted { revision, .. } => Some(*revision),
            Self::Refused { revision, .. } => *revision,
        }
    }
}

struct CachedAdmission {
    pin: DatumPin,
    admission: Arc<Admission>,
    /// Whether the outcome is final for this pin. A pin whose heads have not reached this node
    /// yet is unsettled, and is read again on the next reconcile; a version that was read and
    /// refused is as final as one that was admitted.
    settled: bool,
}

/// The cache entry an [`AdmissionCache::insert`] replaced.
pub struct Displaced(Option<CachedAdmission>);

/// Memo of [`Admission`] outcomes per installed cartridge, owned by whoever reconciles (the nodule
/// state actor), which reads each pinned version once and fills it in. An entry serves only the
/// pin it was filled for, so a re-pin is read and admitted again; [`NoduleConfig::resolve`] drops
/// entries for cartridges that are no longer installed. The lock is held only around map access.
#[derive(Default)]
pub struct AdmissionCache {
    entries: Mutex<BTreeMap<CartridgeId, CachedAdmission>>,
}

impl AdmissionCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `pin` of `cartridge_id` has no settled outcome yet, so its version must be read.
    pub fn needs_read(&self, cartridge_id: &CartridgeId, pin: &DatumPin) -> bool {
        self.entries
            .lock()
            .expect("admission cache lock")
            .get(cartridge_id)
            .is_none_or(|cached| &cached.pin != pin || !cached.settled)
    }

    /// Records the outcome of reading and admitting `pin` of `cartridge_id`, and returns what it
    /// displaced.
    pub fn insert(
        &self,
        cartridge_id: &CartridgeId,
        pin: DatumPin,
        admission: Admission,
        settled: bool,
    ) -> Displaced {
        Displaced(self.entries.lock().expect("admission cache lock").insert(
            cartridge_id.clone(),
            CachedAdmission {
                pin,
                admission: Arc::new(admission),
                settled,
            },
        ))
    }

    /// Puts back what an [`Self::insert`] displaced, for an edit that failed after it.
    pub fn restore(&self, cartridge_id: &CartridgeId, displaced: Displaced) {
        let mut entries = self.entries.lock().expect("admission cache lock");
        match displaced.0 {
            Some(cached) => entries.insert(cartridge_id.clone(), cached),
            None => entries.remove(cartridge_id),
        };
    }

    /// The admission outcome of `install`'s pinned version, or a refusal while it has not been
    /// read.
    pub fn admission(
        &self,
        cartridge_id: &CartridgeId,
        install: &CartridgeInstall,
    ) -> Arc<Admission> {
        match self
            .entries
            .lock()
            .expect("admission cache lock")
            .get(cartridge_id)
        {
            Some(cached) if cached.pin == install.cartridge => Arc::clone(&cached.admission),
            _ => Arc::new(Admission::Refused {
                version: None,
                revision: None,
                reason: "the pinned cartridge version has not been read".to_owned(),
            }),
        }
    }

    fn retain<'a>(&self, installed: impl Iterator<Item = &'a CartridgeId>) {
        let keep: BTreeSet<&CartridgeId> = installed.collect();
        self.entries
            .lock()
            .expect("admission cache lock")
            .retain(|cartridge_id, _| keep.contains(cartridge_id));
    }
}

/// Typed [`DatumStore`] projection over a node's nodule configuration datums: the installs, dev
/// takeovers, artifact pins and routes of its config document. The cartridge config documents the
/// installs name ride along, attached for resolution, and are neither read from nor written back
/// to this document.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NoduleConfig {
    entries: BTreeMap<CartridgeId, NoduleEntry>,
    /// The route datums by pattern: node-wide, so they belong to no cartridge's entry.
    routes: BTreeMap<String, DatumSlot<MethodRoute>>,
    carried_keys: BTreeMap<DatumKey, DatumRead>,
    cartridge_docs: BTreeMap<CartridgeId, CartridgeConfig>,
}

impl From<InventoryError> for StoreError {
    fn from(error: InventoryError) -> Self {
        Self::InvalidEdit(error.to_string())
    }
}

impl DatumStore for NoduleConfig {
    fn owns(key: &DatumKey) -> bool {
        match key.tag().as_str() {
            CartridgeInstall::ID | NoduleDevTakeover::ID | MethodRoute::ID => true,
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
                CartridgeInstall::ID => entry.install = DatumSlot::from_read(datum_read),
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

/// Typed [`DatumStore`] projection over a cartridge's config document: its grants, keyed by
/// component and [`GrantKey`], and the cartridge identity. Everything else the document holds is
/// the cartridge's settings (ordinary datums under the NSIDs its manifest declares, written by
/// the nodule, its micro or a generic editor), carried verbatim and never decoded here, so this
/// projection owns the whole document without interpreting what it does not know.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CartridgeConfig {
    identity: DatumSlot<CartridgeConfigIdentity>,
    grants: BTreeMap<(Component, GrantKey), DatumSlot<GrantBody>>,
    /// The settings datums, and grant-tagged datums whose slot is not a grant key.
    carried: BTreeMap<DatumKey, DatumRead>,
}

impl DatumStore for CartridgeConfig {
    fn owns(_: &DatumKey) -> bool {
        true
    }

    fn hydrate(read: &DatumsRead) -> Self {
        let mut config = Self::default();
        for (key, datum_read) in &read.datums {
            if matches!(datum_read, DatumRead::Absent { .. }) {
                continue;
            }
            if *key == config_identity_key() {
                config.identity = DatumSlot::from_read(datum_read);
                continue;
            }
            let component = match key.tag().as_str() {
                NoduleGrant::ID => Some(Component::Nodule),
                MicroGrant::ID => Some(Component::Micro),
                _ => None,
            };
            match component.zip(GrantKey::from_slot(key.slot_bytes())) {
                Some((component, grant)) => {
                    config
                        .grants
                        .insert((component, grant), DatumSlot::from_read(datum_read));
                }
                None => {
                    config.carried.insert(key.clone(), datum_read.clone());
                }
            }
        }
        config
    }

    fn diff(before: &Self, after: &Self) -> Result<DatumPatch, StoreError> {
        let mut patch = DatumPatch::default();
        diff_slot(
            config_identity_key(),
            &before.identity,
            &after.identity,
            &mut patch,
        )?;
        let absent = DatumSlot::Absent;
        let grants: BTreeSet<&(Component, GrantKey)> =
            before.grants.keys().chain(after.grants.keys()).collect();
        for grant in grants {
            diff_slot(
                grant.1.datum_key(grant.0),
                before.grants.get(grant).unwrap_or(&absent),
                after.grants.get(grant).unwrap_or(&absent),
                &mut patch,
            )?;
        }
        for key in before.carried.keys() {
            if !after.carried.contains_key(key) {
                patch.remove.insert(key.clone());
            }
        }
        Ok(patch)
    }
}

impl CartridgeConfig {
    pub fn identity(&self) -> &DatumSlot<CartridgeConfigIdentity> {
        &self.identity
    }

    /// The grants of `component`, by key, with whether each decoded.
    pub fn grants(
        &self,
        component: Component,
    ) -> impl Iterator<Item = (&GrantKey, &DatumSlot<GrantBody>)> {
        self.grants
            .iter()
            .filter(move |((owner, _), _)| *owner == component)
            .map(|((_, grant), slot)| (grant, slot))
    }

    /// Sets, or with `value` absent revokes, one grant of `component`. A rewrite keeps the members
    /// of the stored datum this build does not know.
    pub fn set_grant(
        &mut self,
        component: Component,
        grant: GrantKey,
        value: Option<GrantBody>,
    ) -> Result<(), InventoryError> {
        let slot = (component, grant);
        let Some(body) = value else {
            self.grants.remove(&slot);
            return Ok(());
        };
        if !slot.1.accepts(&body) {
            return Err(InventoryError::InvalidGrant {
                grant: slot.1.to_string(),
                reason: "a drawer grant states its access and nothing else, and a method or \
                         authority grant states no access"
                    .to_owned(),
            });
        }
        // The stored datum's other members survive the rewrite; the two this build writes are
        // replaced, and one the new value leaves out is dropped.
        let mut raw = match self.grants.get(&slot) {
            Some(DatumSlot::Accepted {
                raw: Some(Value::Object(raw)),
                ..
            }) => raw.clone(),
            _ => serde_json::Map::new(),
        };
        raw.remove("access");
        raw.remove("privileged");
        let Value::Object(written) = serde_json::to_value(&body).expect("a grant body serializes")
        else {
            unreachable!("a grant body serializes as an object");
        };
        raw.extend(written);
        self.grants.insert(
            slot,
            DatumSlot::Accepted {
                value: body,
                raw: Some(Value::Object(raw)),
            },
        );
        Ok(())
    }

    /// Drops the settings and every grant, nodule and micro. The identity stays: it is what lets
    /// the document be found again.
    pub fn clear(&mut self) {
        self.grants.clear();
        self.carried.clear();
    }

    /// What `component`'s grants ask for: drawer access by drawer, and the scope of the dispatch
    /// capability minted for it. A grant that does not decode, or states an access level this
    /// build does not know, refuses the component with the reason, as an invalid datum always has.
    pub fn resolve_grants(
        &self,
        component: Component,
    ) -> Result<(BTreeMap<DrawerId, DrawerAccess>, DispatchScope), String> {
        let mut drawers = BTreeMap::new();
        let mut scope = DispatchScope::default();
        for (grant, slot) in self.grants(component) {
            let body = match slot {
                DatumSlot::Absent => continue,
                DatumSlot::Invalid { reason, .. } => {
                    return Err(format!("invalid grant datum `{grant}`: {reason}"));
                }
                DatumSlot::Accepted { value, .. } => value,
            };
            if !grant.accepts(body) {
                return Err(format!(
                    "grant datum `{grant}` states a value that kind of grant does not take"
                ));
            }
            match grant {
                GrantKey::Drawer(drawer_id) => {
                    let access = body.access.as_ref().expect("checked by `accepts`");
                    let Some(known) = access.known() else {
                        return Err(format!(
                            "unknown drawer access level `{}` for drawer `{drawer_id}`",
                            access.as_str()
                        ));
                    };
                    drawers.insert(drawer_id.clone(), known);
                }
                GrantKey::Method(nsid) => {
                    scope.operations.insert(nsid.clone());
                }
                GrantKey::Authority(prefix) => {
                    scope.authorities.insert(prefix.clone());
                }
            }
            scope.privileged |= body.privileged;
        }
        Ok((drawers, scope))
    }
}

/// Whether the nodule member of `install` is enabled.
fn nodule_enabled(install: &CartridgeInstall) -> bool {
    install.nodule.as_ref().is_some_and(|nodule| nodule.enabled)
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

    /// Hands resolution the cartridge config documents the installs name, replacing any attached
    /// before. A cartridge without one resolves with no grants.
    pub fn attach_cartridge_docs(&mut self, docs: BTreeMap<CartridgeId, CartridgeConfig>) {
        self.cartridge_docs = docs;
    }

    /// The config document each installed cartridge names.
    pub fn config_docs(&self) -> BTreeMap<CartridgeId, DocId> {
        self.entries
            .iter()
            .filter_map(|(cartridge_id, entry)| {
                entry
                    .install
                    .accepted()
                    .map(|install| (cartridge_id.clone(), install.config_doc.clone()))
            })
            .collect()
    }

    /// The installs this document holds, for the install surface.
    pub fn installs(&self) -> Vec<CartridgeInstallRecord> {
        self.entries
            .iter()
            .filter_map(|(cartridge_id, entry)| {
                entry
                    .install
                    .accepted()
                    .map(|install| CartridgeInstallRecord {
                        cartridge_id: cartridge_id.clone(),
                        install: install.clone(),
                    })
            })
            .collect()
    }

    /// Records the install of `cartridge`, read from `pin`: admits its nodule contribution if it
    /// has one, checks it does not collide with the version already installed, and updates the
    /// [`CartridgeInstall`] and the handler artifact references. A component is present exactly
    /// when the envelope contributes it. An install that exists keeps each component's enablement
    /// and its unknown members, so this is also the upgrade; a new one starts disabled. The
    /// admission of `pin` is entered in `cache` here, and put back as it was if the install is
    /// refused.
    pub fn install(
        &mut self,
        cache: &AdmissionCache,
        cartridge: &Cartridge,
        pin: DatumPin,
        config_doc: DocId,
    ) -> Result<(), InventoryError> {
        let has_nodule = cartridge.nodule().is_some();
        let has_micro = cartridge.micro().is_some();
        if !has_nodule && !has_micro {
            return Err(InventoryError::NothingToInstall(cartridge.id.clone()));
        }
        let nodule = has_nodule
            .then(|| admit_cartridge_nodule(cartridge))
            .transpose()?;
        let existing = self
            .entries
            .get(&cartridge.id)
            .and_then(|entry| entry.install.accepted())
            .cloned();
        if let Some(existing) = &existing {
            let previous = cache.admission(&cartridge.id, existing);
            let revision = cartridge.revision();
            if previous.version() == Some(&cartridge.version)
                && let Some(known) = previous.revision()
                && known != revision
            {
                return Err(InventoryError::Cartridge(CartridgeError::Conflict {
                    id: cartridge.id.clone(),
                    version: Box::new(cartridge.version.clone()),
                    existing: known,
                    incoming: revision,
                }));
            }
        }
        let member = |present: bool, kept: Option<&ComponentInstall>| {
            present.then(|| {
                kept.cloned()
                    .unwrap_or_else(|| ComponentInstall::new(false))
            })
        };
        let install = CartridgeInstall {
            cartridge: pin,
            config_doc,
            nodule: member(
                has_nodule,
                existing
                    .as_ref()
                    .and_then(|install| install.nodule.as_ref()),
            ),
            micro: member(
                has_micro,
                existing.as_ref().and_then(|install| install.micro.as_ref()),
            ),
            extra: existing.map(|install| install.extra).unwrap_or_default(),
        };
        let enabled = nodule_enabled(&install);
        let displaced = cache.insert(
            &cartridge.id,
            install.cartridge.clone(),
            Admission::of(cartridge),
            true,
        );
        let entry = self.entries.entry(cartridge.id.clone()).or_default();
        entry.install = DatumSlot::Accepted {
            value: install,
            raw: None,
        };
        entry.artifacts = match nodule {
            Some((_, _, _, artifacts)) => artifact_slots(&entry.artifacts, &artifacts),
            None => BTreeMap::new(),
        };
        // A cartridge installed disabled holds nothing yet; an upgrade of an enabled one must not
        // take a prefix another enabled slot holds.
        if enabled && let Err(error) = self.ensure_no_authority_conflict(cache, &cartridge.id) {
            cache.restore(&cartridge.id, displaced);
            return Err(error);
        }
        Ok(())
    }

    /// Enabling the nodule is where two holders of one prefix are refused (ADR 028 §5), and where
    /// a cartridge that fills another slot is refused its own: one code lineage per slot.
    pub fn set_enabled(
        &mut self,
        cache: &AdmissionCache,
        cartridge_id: &CartridgeId,
        component: Component,
        enabled: bool,
    ) -> Result<(), InventoryError> {
        if component == Component::Nodule
            && enabled
            && let Some(slot) = self.slot_filled_by(cartridge_id)
        {
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
            .ok_or_else(|| InventoryError::UnknownCartridge(cartridge_id.clone()))?;
        let install = match &mut entry.install {
            DatumSlot::Absent => {
                return Err(InventoryError::UnknownCartridge(cartridge_id.clone()));
            }
            DatumSlot::Invalid { reason, .. } => {
                return Err(InventoryError::InvalidContribution {
                    id: cartridge_id.clone(),
                    reason: format!("install datum is invalid: {reason}"),
                });
            }
            DatumSlot::Accepted { value, raw } => {
                *raw = None;
                value
            }
        };
        let Some(member) = install.component_mut(component) else {
            return Err(InventoryError::NoComponent {
                id: cartridge_id.clone(),
                component,
                action: "enable or disable",
            });
        };
        member.enabled = enabled;
        if component == Component::Nodule && enabled {
            self.ensure_no_authority_conflict(cache, cartridge_id)?;
        }
        Ok(())
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

    /// The routes in force, and what could not be read. The config document is written by any
    /// client holding a capability on it, so its route datums are external input: one that does
    /// not decode, or names an empty provider, becomes `Route::Invalid`, which refuses exactly the
    /// calls and requests its pattern covers while every other route and nodule carries on. A
    /// datum whose pattern is itself unusable (not an NSID prefix, or a key that is not UTF-8)
    /// covers nothing knowable, so it is left out of the table and described in the second
    /// element for the caller to report. `set_route` with no provider clears either kind.
    pub fn routes(&self) -> (BTreeMap<String, Route>, Vec<String>) {
        let mut table = BTreeMap::new();
        let mut unreadable: Vec<String> = self
            .carried_keys
            .keys()
            .filter(|key| key.tag().as_str() == MethodRoute::ID)
            .map(|key| format!("route datum with a key that is not UTF-8: {key}"))
            .collect();
        for (pattern, slot) in &self.routes {
            let route = match slot {
                DatumSlot::Absent => continue,
                DatumSlot::Accepted { value, .. } => match route_error(pattern, &value.provider) {
                    None => Route::Provider(value.provider.clone()),
                    Some(reason) => Route::Invalid(format!("route `{pattern}`: {reason}")),
                },
                DatumSlot::Invalid { reason, .. } => {
                    Route::Invalid(format!("route `{pattern}` is damaged: {reason}"))
                }
            };
            if let Some(reason) = pattern_error(pattern) {
                unreadable.push(format!("route datum `{pattern}`: {reason}"));
            } else {
                table.insert(pattern.clone(), route);
            }
        }
        (table, unreadable)
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
        let Some(provider) = provider else {
            self.routes.remove(pattern);
            return Ok(());
        };
        if let Some(reason) = route_error(pattern, &provider) {
            return Err(InventoryError::InvalidRoute {
                pattern: pattern.to_owned(),
                reason: reason.to_owned(),
            });
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
        self.installed_nodule(cartridge_id)?;
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
        let install = self.entries[cartridge_id]
            .install
            .accepted()
            .expect("verified installed");
        filler(cartridge_id, install, &by, &self.entries, cache).map_err(invalid)?;
        if self
            .installed_nodule(&by)?
            .nodule
            .as_ref()
            .is_some_and(|nodule| nodule.enabled)
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
            .is_some_and(nodule_enabled)
        {
            self.ensure_no_authority_conflict(cache, cartridge_id)?;
        }
        Ok(())
    }

    /// Removes the install, its artifact references and any dev takeover. The cartridge's config
    /// document is not this document's: it stays, with the settings and grants in it.
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
    /// failing unrelated valid nodules. A cartridge whose install has no nodule member is not a
    /// nodule and is left out.
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
            if let Some(resolution) = resolve_entry(
                cartridge_id,
                entry,
                &self.entries,
                self.cartridge_docs.get(cartridge_id),
                cache,
            ) {
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

    /// The stored install of a cartridge that has a nodule member.
    fn installed_nodule(
        &self,
        cartridge_id: &CartridgeId,
    ) -> Result<&CartridgeInstall, InventoryError> {
        let install = match self.entries.get(cartridge_id).map(|entry| &entry.install) {
            None | Some(DatumSlot::Absent) => {
                return Err(InventoryError::UnknownCartridge(cartridge_id.clone()));
            }
            Some(DatumSlot::Invalid { reason, .. }) => {
                return Err(InventoryError::InvalidContribution {
                    id: cartridge_id.clone(),
                    reason: format!("install datum is invalid: {reason}"),
                });
            }
            Some(DatumSlot::Accepted { value, .. }) => value,
        };
        if install.nodule.is_none() {
            return Err(InventoryError::NoComponent {
                id: cartridge_id.clone(),
                component: Component::Nodule,
                action: "run as a slot",
            });
        }
        Ok(install)
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
    artifacts: &BTreeMap<String, (crate::inventory::Artifact, crate::inventory::ArtifactState)>,
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
        for file in installed.resolved.lexicons.values() {
            let nsid = file
                .get("id")
                .and_then(Value::as_str)
                .expect("admission checked every Lexicon states its NSID");
            copies.entry(nsid).or_default().push(Copy {
                cartridge_id,
                held: authority
                    .is_some_and(|authority| DispatchScope::authority_owns(authority, nsid)),
                revision: installed.resolved.lexicon_revision(nsid),
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
    install: &CartridgeInstall,
    by: &CartridgeId,
    entries: &'a BTreeMap<CartridgeId, NoduleEntry>,
    cache: &AdmissionCache,
) -> Result<(&'a CartridgeInstall, Arc<Admission>), String> {
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
fn substituted_grants(
    own: &ResolvedCartridge,
    filler: &ResolvedCartridge,
    granted: &DispatchScope,
) -> DispatchScope {
    DispatchScope {
        operations: filler
            .operation_requests()
            .into_iter()
            .filter(|request| {
                let shared = own.operation_requests().iter().any(|own| {
                    (
                        &own.binding,
                        &own.contract,
                        &own.necessity,
                        &own.min_revision,
                    ) == (
                        &request.binding,
                        &request.contract,
                        &request.necessity,
                        &request.min_revision,
                    )
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

/// The admission of the code that runs in slot `cartridge_id` and, under a dev takeover, the
/// cartridge filling it: the slot's own install unless its takeover datum names another.
fn code_source(
    cartridge_id: &CartridgeId,
    entry: &NoduleEntry,
    install: &CartridgeInstall,
    entries: &BTreeMap<CartridgeId, NoduleEntry>,
    cache: &AdmissionCache,
) -> Result<(Arc<Admission>, Option<CartridgeId>), String> {
    match &entry.dev_takeover {
        DatumSlot::Absent => Ok((cache.admission(cartridge_id, install), None)),
        DatumSlot::Invalid { reason, .. } => Err(format!("invalid devTakeover datum: {reason}")),
        DatumSlot::Accepted { value, .. } => {
            let (_, admission) = filler(cartridge_id, install, &value.by, entries, cache)?;
            Ok((admission, Some(value.by.clone())))
        }
    }
}

fn resolve_entry(
    cartridge_id: &CartridgeId,
    entry: &NoduleEntry,
    entries: &BTreeMap<CartridgeId, NoduleEntry>,
    docs: Option<&CartridgeConfig>,
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
                reason: format!("invalid install datum: {reason}"),
            });
        }
        DatumSlot::Accepted { value, .. } => value,
    };
    // A cartridge that contributes only a micro is not a nodule.
    let enabled = install.nodule.as_ref()?.enabled;

    let refuse = |version: Option<Version>, revision: Option<Revision>, reason: String| {
        NoduleResolution::Refused {
            cartridge_id: cartridge_id.clone(),
            version,
            revision,
            enabled,
            reason,
        }
    };

    // The code that runs in this slot: its own install, or the filler's under a dev takeover.
    // Everything else about the slot (settings, grants, enablement) is the slot's own.
    let (admission, filled_from) = match code_source(cartridge_id, entry, install, entries, cache) {
        Ok(source) => source,
        Err(reason) => {
            let own = cache.admission(cartridge_id, install);
            return Some(refuse(None, own.revision(), reason));
        }
    };
    let (version, revision, resolved, nodule, admitted, artifacts) = match &*admission {
        Admission::Refused {
            version,
            revision,
            reason,
        } => return Some(refuse(version.clone(), *revision, reason.clone())),
        Admission::Admitted {
            version,
            revision,
            nodule,
            resolved,
            admitted,
            artifacts,
        } => (
            version.clone(),
            *revision,
            resolved.clone(),
            nodule.clone(),
            admitted.clone(),
            artifacts.clone(),
        ),
    };

    let (desired_grants, mut desired_operations) = match docs {
        None => Default::default(),
        Some(docs) => match docs.resolve_grants(Component::Nodule) {
            Ok(grants) => grants,
            Err(reason) => return Some(refuse(Some(version), Some(revision), reason)),
        },
    };
    if filled_from.is_some() {
        let Admission::Admitted { resolved: own, .. } = &*cache.admission(cartridge_id, install)
        else {
            unreachable!("code_source admitted the slot's own install while checking the filler");
        };
        desired_operations = substituted_grants(own, &resolved, &desired_operations);
    }

    let mut installed = InstalledNodule::new(
        cartridge_id.clone(),
        version.clone(),
        revision,
        resolved,
        nodule,
        admitted,
        artifacts,
        enabled,
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
