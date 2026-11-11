//! The nodule management protocol (ADR 027 §4, §6): the wire contract between the node-side
//! nodule service and whoever administers it.
//!
//! Every operation names its caller by the request's `Authorization: Bearer` credential (the
//! envelope's HTTP metadata, `Authorization::from_metadata`), never by a body field; a request
//! without one fails with `ContextError::Unauthenticated`. The credential is either the operator's own identity
//! capability, or a handle this domain minted through [`MintNoduleCapability`] that carries a
//! [`NoduleScope`]. Identity is not access by itself: an arbitrary actor presenting its identity
//! is refused, only the operator's identity and scoped handles narrowed from it administer
//! nodules. Nothing here serves a handler; a record reports readiness so a caller can see that
//! execution is unavailable rather than be told a dispatch succeeded.

use crate::inventory::ArtifactState;
use crate::inventory::FilledBy;
use crate::inventory::InventoryError;
use crate::providers::RequestStatus;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use indexmap::IndexMap;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// What a minted management handle may do. `cartridges` names the packages the holder may read, and
/// whose enablement and settings it may change. `admit` additionally lets it install or upgrade
/// those same packages and no others, so a handle can never introduce a cartridge id it was not minted
/// for. The operator's identity is unrestricted without a scope.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoduleScope {
    #[serde(default)]
    pub admit: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub cartridges: BTreeSet<CartridgeId>,
}

impl NoduleScope {
    pub fn covers(&self, other: &Self) -> bool {
        (self.admit || !other.admit) && other.cartridges.is_subset(&self.cartridges)
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintNoduleCapability {
    pub scope: NoduleScope,
    pub rights: Rights,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmitNodule {
    pub cartridge: Cartridge,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureNodule {
    pub cartridge_id: CartridgeId,
    pub settings: IndexMap<String, Value>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleEnabled {
    pub cartridge_id: CartridgeId,
    pub enabled: bool,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleDrawerGrants {
    pub cartridge_id: CartridgeId,
    pub drawers: BTreeMap<DrawerId, DrawerAccess>,
}

/// Replaces the methods the nodule's actor may invoke through dispatch. The service mints one
/// dispatch capability for the actor from it (see [`NoduleMethodGrantStatus`]); an empty scope
/// revokes it.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleMethodGrants {
    pub cartridge_id: CartridgeId,
    pub methods: DispatchScope,
}

/// Confirms that the nodule's request `binding` is to be served by the cartridge that now provides its
/// contract, replacing the one the operator consented to earlier (ADR 028 §3). Refused unless the
/// request is held at `ProviderChanged`, so a confirmation cannot be applied to a state the
/// operator did not see.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmNoduleResolution {
    pub cartridge_id: CartridgeId,
    pub binding: String,
}

/// Sets, or with `by` absent clears, the node-local substitution of slot `cartridge_id`: the cartridge `by`
/// (installed, and holding the slot's authority or none) is run *as* `cartridge_id` (ADR 028 §3). The
/// caller must manage both cartridges, since `by`'s code runs with the slot's grants.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleSubstitution {
    pub cartridge_id: CartridgeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<CartridgeId>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UninstallNodule {
    pub cartridge_id: CartridgeId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetNodule {
    pub cartridge_id: CartridgeId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNodules {}

/// The durable actor an installed nodule acts as. Its capability is not here: the service holds
/// it, and a caller that could read it could sign as the nodule.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleActor {
    pub actor_id: Uuid7,
    pub url: String,
    pub public_key: MultibaseKey,
}

/// Desired and effective state of one local drawer grant binding for a nodule.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleGrantStatus {
    pub desired: DrawerAccess,
    pub granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Desired and effective state of the dispatch capability a nodule's method grants mint.
/// `desired` is the configured grants bounded by the manifest: only operations the nodule
/// requests (see [`crate::Nodule::bind_operations`]).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleMethodGrantStatus {
    pub desired: DispatchScope,
    /// Configured grant entries that cover no requested operation. They are not applied: the
    /// manifest bounds what configuration can confer.
    #[serde(default, skip_serializing_if = "DispatchScope::is_empty")]
    pub unrequested: DispatchScope,
    pub granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// How a nodule's initializers have fared in its current start (see [`NoduleRecord::init`]).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleInitStatus {
    /// Invocations made of the initializer that has not succeeded yet, since the previous one
    /// succeeded (or since the start began). A wait for a foreign provider is not an attempt.
    pub attempts: u32,
    /// The last failure of that initializer, kept while it is retried and dropped once it
    /// succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The foreign operation the current initializer is waiting for a ready provider of: not
    /// installed, not enabled, or still initializing itself. Present while it waits, even
    /// before any attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_on: Option<String>,
}

/// The result of [`ListNodules`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleList {
    pub nodules: Vec<NoduleRecord>,
}

/// One installed nodule as the management surface reports it.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleRecord {
    pub cartridge_id: CartridgeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<Revision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<NoduleActor>,
    /// The cartridge whose code runs in this slot under a substitution. `version` and `revision`
    /// above are then the filler's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filled_by: Option<FilledBy>,
    pub enabled: bool,
    pub readiness: Readiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<IndexMap<String, Value>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub artifacts: BTreeMap<String, ArtifactState>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drawers: BTreeMap<DrawerId, NoduleGrantStatus>,
    /// Absent when the nodule has no method grants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<NoduleMethodGrantStatus>,
    /// Where each operation request stands, by binding, and the lockfile entry (provider cartridge
    /// id, version and Lexicon revision) it is pinned to. Node-local and durable; empty while
    /// the nodule is disabled or refused. Data and schema requests are not listed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, RequestStatus>,
    /// Absent until an initializer of the current start has been invoked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init: Option<NoduleInitStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub observed_heads: Vec<ChangeHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<Vec<String>>"))]
    pub applied_heads: Option<Vec<ChangeHash>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum NodulesError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("authority failed: {0}")]
    Authority(AuthorityServiceError),
    #[error("authority transport failed: {0}")]
    AuthorityUnavailable(String),
    #[error("nodule state failed: {0}")]
    Storage(String),
    #[error("the package was refused: {0}")]
    Rejected(String),
    #[error("nodule for cartridge `{0}` is not installed")]
    UnknownCartridge(CartridgeId),
    #[error("nodule for cartridge `{0}` declares no config schema")]
    NoConfigSchema(CartridgeId),
}

/// Inventory refusals cross the wire as text: the caller gets the reason, not the
/// inventory's error tree, except the two it can act on by naming a different cartridge.
impl From<InventoryError> for NodulesError {
    fn from(error: InventoryError) -> Self {
        match error {
            InventoryError::UnknownCartridge(id) => NodulesError::UnknownCartridge(id),
            InventoryError::NoConfigSchema(id) => NodulesError::NoConfigSchema(id),
            other => NodulesError::Rejected(other.to_string()),
        }
    }
}

// Every method that changes nodules (install, enable, grants, substitution) is privileged: a
// nodule's dispatch scope never reaches them, which is what keeps a nodule from widening its own
// grants. The reads (`getNodule`, `listNodules`) confer nothing, so a scope naming them reaches
// them, and minting a nodule handle only narrows the presented one (`scope.covers`).
method_core::methods! {
    error = NodulesError, nsid_prefix = "org.example.daybook.nodules.";
    MintNoduleCapability => Capability, procedure, "mintNoduleCapability";
    AdmitNodule => NoduleRecord, procedure, "admitNodule", privileged;
    ConfigureNodule => NoduleRecord, procedure, "configureNodule", privileged;
    SetNoduleEnabled => NoduleRecord, procedure, "setNoduleEnabled", privileged;
    SetNoduleDrawerGrants => NoduleRecord, procedure, "setNoduleDrawerGrants", privileged;
    SetNoduleMethodGrants => NoduleRecord, procedure, "setNoduleMethodGrants", privileged;
    ConfirmNoduleResolution => NoduleRecord, procedure, "confirmNoduleResolution", privileged;
    SetNoduleSubstitution => NoduleRecord, procedure, "setNoduleSubstitution", privileged;
    UninstallNodule => (), procedure, "uninstallNodule", privileged;
    GetNodule => NoduleRecord, query, "getNodule";
    ListNodules => NoduleList, query, "listNodules";
}

declare_service_protocol! {
    pub enum NodulesProtocol => NodulesMessage {
        unary {
            MintNoduleCapability(MintNoduleCapability),
            AdmitNodule(AdmitNodule),
            ConfigureNodule(ConfigureNodule),
            SetNoduleEnabled(SetNoduleEnabled),
            SetNoduleDrawerGrants(SetNoduleDrawerGrants),
            SetNoduleMethodGrants(SetNoduleMethodGrants),
            ConfirmNoduleResolution(ConfirmNoduleResolution),
            SetNoduleSubstitution(SetNoduleSubstitution),
            UninstallNodule(UninstallNodule),
            GetNodule(GetNodule),
            ListNodules(ListNodules)
        }
        stream {}
    }
}
