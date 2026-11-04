//! The nodule management protocol (ADR 027 §4, §6): the wire contract between the node-side
//! nodule service and whoever administers it.
//!
//! Every operation names its caller by an [`Authorization`]: either the operator's own identity
//! capability, or a handle this domain minted through [`MintNoduleCapability`] that carries a
//! [`NoduleScope`]. Identity is not access by itself: an arbitrary actor presenting its identity
//! is refused, only the operator's identity and scoped handles narrowed from it administer
//! nodules. Nothing here serves a handler; a record reports readiness so a caller can see that
//! execution is unavailable rather than be told a dispatch succeeded.

use crate::dispatch::DispatchScope;
use crate::inventory::ArtifactState;
use crate::inventory::InventoryError;
use crate::inventory::Readiness;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_plugs_core::Plug;
use big_plugs_core::PlugId;
use big_plugs_core::Revision;
use big_plugs_core::Version;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_core_rs::ContextError;
use service_core_rs::Operation;
use service_core_rs::XrpcOperation;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// What a minted management handle may do. `plugs` names the packages the holder may read, and
/// whose enablement and settings it may change. `admit` additionally lets it install or upgrade
/// those same packages and no others, so a handle can never introduce a plug id it was not minted
/// for. The operator's identity is unrestricted without a scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoduleScope {
    #[serde(default)]
    pub admit: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub plugs: BTreeSet<PlugId>,
}

impl NoduleScope {
    pub fn covers(&self, other: &Self) -> bool {
        (self.admit || !other.admit) && other.plugs.is_subset(&self.plugs)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintNoduleCapability {
    pub authorization: Authorization,
    pub scope: NoduleScope,
    pub rights: Rights,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmitNodule {
    pub authorization: Authorization,
    pub plug: Plug,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureNodule {
    pub authorization: Authorization,
    pub plug_id: PlugId,
    pub settings: IndexMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleEnabled {
    pub authorization: Authorization,
    pub plug_id: PlugId,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleDrawerGrants {
    pub authorization: Authorization,
    pub plug_id: PlugId,
    pub drawers: BTreeMap<DrawerId, DrawerAccess>,
}

/// Replaces the operations the nodule's actor may invoke through dispatch. The service mints one
/// dispatch capability for the actor from it (see [`NoduleOperationGrantStatus`]); an empty scope
/// revokes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleOperationGrants {
    pub authorization: Authorization,
    pub plug_id: PlugId,
    pub operations: DispatchScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UninstallNodule {
    pub authorization: Authorization,
    pub plug_id: PlugId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetNodule {
    pub authorization: Authorization,
    pub plug_id: PlugId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNodules {
    pub authorization: Authorization,
}

/// The durable actor an installed nodule acts as. Its capability is not here: the service holds
/// it, and a caller that could read it could sign as the nodule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleActor {
    pub actor_id: Uuid7,
    pub url: String,
    pub public_key: MultibaseKey,
}

/// Desired and effective state of one local drawer grant binding for a nodule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleGrantStatus {
    pub desired: DrawerAccess,
    pub granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Desired and effective state of the dispatch capability a nodule's operation grants mint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleOperationGrantStatus {
    pub desired: DispatchScope,
    pub granted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// How a nodule's initializers have fared in its current start (see [`NoduleRecord::init`]).
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

/// One installed nodule as the management surface reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoduleRecord {
    pub plug_id: PlugId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<Revision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<NoduleActor>,
    pub enabled: bool,
    pub readiness: Readiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<IndexMap<String, Value>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub artifacts: BTreeMap<String, ArtifactState>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drawers: BTreeMap<DrawerId, NoduleGrantStatus>,
    /// Absent when the nodule has no operation grants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operations: Option<NoduleOperationGrantStatus>,
    /// Absent until an initializer of the current start has been invoked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init: Option<NoduleInitStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_heads: Vec<ChangeHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_heads: Option<Vec<ChangeHash>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

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
    #[error("nodule for plug `{0}` is not installed")]
    UnknownPlug(PlugId),
    #[error("nodule for plug `{0}` declares no config schema")]
    NoConfigSchema(PlugId),
}

/// Inventory refusals cross the wire as text: the caller gets the reason, not the
/// inventory's error tree, except the two it can act on by naming a different plug.
impl From<InventoryError> for NodulesError {
    fn from(error: InventoryError) -> Self {
        match error {
            InventoryError::UnknownPlug(id) => NodulesError::UnknownPlug(id),
            InventoryError::NoConfigSchema(id) => NodulesError::NoConfigSchema(id),
            other => NodulesError::Rejected(other.to_string()),
        }
    }
}

macro_rules! operations {
    ($($request:ident => $output:ty, $kind:ident, $nsid:literal;)*) => {$(
        impl Operation for $request {
            type Output = $output;
            type Error = NodulesError;
            const XRPC: XrpcOperation =
                XrpcOperation::$kind(concat!("org.example.daybook.nodules.", $nsid))
                    .with_schemas(stringify!($output), stringify!(NodulesError));
        }
    )*};
}
operations! {
    MintNoduleCapability => Capability, procedure, "mintNoduleCapability";
    AdmitNodule => NoduleRecord, procedure, "admitNodule";
    ConfigureNodule => NoduleRecord, procedure, "configureNodule";
    SetNoduleEnabled => NoduleRecord, procedure, "setNoduleEnabled";
    SetNoduleDrawerGrants => NoduleRecord, procedure, "setNoduleDrawerGrants";
    SetNoduleOperationGrants => NoduleRecord, procedure, "setNoduleOperationGrants";
    UninstallNodule => (), procedure, "uninstallNodule";
    GetNodule => NoduleRecord, query, "getNodule";
    ListNodules => Vec<NoduleRecord>, query, "listNodules";
}

declare_service_protocol! {
    pub enum NodulesProtocol => NodulesMessage {
        unary {
            MintNoduleCapability(MintNoduleCapability),
            AdmitNodule(AdmitNodule),
            ConfigureNodule(ConfigureNodule),
            SetNoduleEnabled(SetNoduleEnabled),
            SetNoduleDrawerGrants(SetNoduleDrawerGrants),
            SetNoduleOperationGrants(SetNoduleOperationGrants),
            UninstallNodule(UninstallNodule),
            GetNodule(GetNodule),
            ListNodules(ListNodules)
        }
        stream {}
    }
}
