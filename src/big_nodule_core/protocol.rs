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
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::ClearCartridgeConfig;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::ObserveCartridgeInstalls;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::protocol::UninstallCartridge;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// What a minted management handle may do. `cartridges` names the packages the holder may read, and
/// whose enablement and grants it may change. `admit` additionally lets it install or upgrade
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

/// Sets, or with `by` absent clears, the dev takeover of slot `cartridge_id`: the cartridge `by`
/// (installed, and holding the slot's authority or none) is run *as* `cartridge_id`, with its actor,
/// data and drawer grants, to test a dev build against real data (ADR 045 §5). The node refuses it
/// unless `node.json` sets `dev.allow_slot_takeover`, and the caller must manage both cartridges,
/// since `by`'s code runs with the slot's grants.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetNoduleDevTakeover {
    pub cartridge_id: CartridgeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<CartridgeId>,
}

/// Sets, or with `provider` absent clears, the node-local route of `pattern`: an exact NSID or an
/// authority prefix whose calls dispatch sends to the provider with that id, a built-in's or a
/// cartridge's (ADR 045). The operator's route is also the consent of every dependent: the
/// resolution locks of the requests it covers move to the routed provider. Served by the nodule
/// service only until the node has a per-node store of its own.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetMethodRoute {
    pub pattern: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
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
    /// The cartridge whose code runs in this slot under a dev takeover. `version` and `revision`
    /// above are then the filler's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filled_by: Option<FilledBy>,
    pub enabled: bool,
    pub readiness: Readiness,
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
}

/// Inventory refusals cross the wire as text: the caller gets the reason, not the
/// inventory's error tree, except the one it can act on by naming a different cartridge.
impl From<InventoryError> for NodulesError {
    fn from(error: InventoryError) -> Self {
        match error {
            InventoryError::UnknownCartridge(id) => NodulesError::UnknownCartridge(id),
            other => NodulesError::Rejected(other.to_string()),
        }
    }
}

// Every method that changes nodules (dev takeover, routes) is privileged, as are the cartridge-level
// methods that install, enable and grant (see `big_cartridges_core::protocol`): a nodule's dispatch
// scope never reaches them, which is what keeps a nodule from widening its own grants. The reads
// (`getNodule`, `listNodules`) confer nothing, so a scope naming them reaches them, and minting a
// nodule handle only narrows the presented one (`scope.covers`).
method_core::methods! {
    error = NodulesError, nsid_prefix = "org.example.daybook.nodules.";
    MintNoduleCapability => Capability, procedure, "mintNoduleCapability";
    ConfirmNoduleResolution => NoduleRecord, procedure, "confirmNoduleResolution", privileged;
    SetNoduleDevTakeover => NoduleRecord, procedure, "setNoduleDevTakeover", privileged;
    SetMethodRoute => (), procedure, "setMethodRoute", privileged;
    GetNodule => NoduleRecord, query, "getNodule";
    ListNodules => NoduleList, query, "listNodules";
}

/// The cartridge-level methods fail with the cartridges service's error, which the nodule service
/// translates its own into.
impl From<NodulesError> for CartridgesError {
    fn from(error: NodulesError) -> Self {
        match error {
            NodulesError::Context(error) => Self::Context(error),
            NodulesError::Denied => Self::Denied,
            NodulesError::Authority(error) => Self::Authority(error),
            NodulesError::AuthorityUnavailable(message) => Self::AuthorityUnavailable(message),
            NodulesError::Storage(message) => Self::Storage(message),
            NodulesError::Rejected(reason) => Self::Rejected(reason),
            NodulesError::UnknownCartridge(id) => Self::UnknownCartridge(id),
        }
    }
}

declare_service_protocol! {
    pub enum NodulesProtocol => NodulesMessage {
        unary {
            MintNoduleCapability(MintNoduleCapability),
            InstallCartridge(InstallCartridge),
            SetCartridgeEnabled(SetCartridgeEnabled),
            UninstallCartridge(UninstallCartridge),
            SetCartridgeGrant(SetCartridgeGrant),
            ClearCartridgeConfig(ClearCartridgeConfig),
            ListCartridgeInstalls(ListCartridgeInstalls),
            ConfirmNoduleResolution(ConfirmNoduleResolution),
            SetNoduleDevTakeover(SetNoduleDevTakeover),
            SetMethodRoute(SetMethodRoute),
            GetNodule(GetNodule),
            ListNodules(ListNodules)
        }
        stream {
            ObserveCartridgeInstalls(ObserveCartridgeInstalls)
        }
    }
}
