use super::Drawer;
use super::DrawerAccess as Access;
use super::DrawerGroups;
use super::DrawerLayout;
use super::GroupKey;
use super::LayoutKind;
use super::MetaRoot;
use crate::authority::AuthorityServiceError;
use crate::authority::CapId;
use crate::authority::Capability;
use crate::authority::Rights;
use crate::dmeta::DocId;
use crate::dmeta::Uuid7;
use crate::doc::CommandId;
use crate::doc::DocsError;
use crate::doc::WriteReceipt;
use automerge::ChangeHash;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;

pub use super::listing::DocDrawer;
pub use super::listing::DocDrawers;
pub use super::listing::KnownDrawer;
pub use super::listing::KnownDrawers;
pub use super::listing::ListedDoc;
pub use super::listing::ListedDocs;
pub use super::listing::ListedDrawer;
pub use super::listing::ListedDrawers;

pub type DrawerId = DocId;

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalDrawers {
    pub present_id: DrawerId,
    pub originated_id: DrawerId,
    pub known_drawers_id: DrawerId,
    pub config_id: DrawerId,
}

impl LocalDrawers {
    pub fn both(&self) -> [&DrawerId; 2] {
        [&self.present_id, &self.originated_id]
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerRecord {
    pub drawer_id: DrawerId,
    pub descriptor: Drawer,
}

impl DrawerRecord {
    pub fn layout_kind(&self) -> Option<LayoutKind> {
        self.descriptor.layout.known()
    }

    pub fn groups(&self) -> Option<DrawerGroups> {
        self.descriptor.groups()
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DrawersError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("authority failed: {0}")]
    Authority(AuthorityServiceError),
    #[error("authority transport failed: {0}")]
    AuthorityUnavailable(String),
    #[error("document repository failed: {0}")]
    Docs(DocsError),
    #[error("document repository transport failed: {0}")]
    DocsUnavailable(String),
    #[error("keyhive failed: {0}")]
    Keyhive(String),
    #[error("drawer state failed: {0}")]
    Storage(String),
    #[error("{0} is not a drawer this node knows")]
    UnknownDrawer(DrawerId),
    /// A peer this node has not met: its keyhive identity arrives with a live connection.
    #[error("unknown peer {0}")]
    UnknownPeer(GroupKey),
    #[error("{0} is a self-doc drawer: it holds only itself and cannot take entries or members")]
    SelfDoc(DrawerId),
    #[error("{0} is already a drawer")]
    AlreadyDrawer(DocId),
    #[error("a drawer cannot be its own child")]
    SelfChild,
    #[error("{0} is not an entry of this drawer")]
    NotAnEntry(DocId),
    #[error("the drawer's descriptor states a layout this build does not know: {0}")]
    ForeignLayout(String),
    #[error("command {command} is still in flight; retry once it resolves")]
    CommandInFlight { command: CommandId },
    #[error("command {command} was already used for a different request")]
    CommandReused { command: CommandId },
    #[error("the drawer's roster could not be read: {0}")]
    RosterUnreadable(String),
    #[error("{0} is not a valid meta-doc kind/name slot")]
    InvalidMetaDocSlot(String),
    #[error("successor resolution from {start} revisited {revisited}: the chain is a cycle")]
    SuccessorCycle {
        start: DrawerId,
        revisited: DrawerId,
    },
    #[error("{drawer_id} names itself as its successor")]
    SuccessorSelf { drawer_id: DrawerId },
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "layout",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CreateLayout {
    SelfDoc { doc_id: DocId },
    SingleGroup,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DrawerScope {
    pub drawers: BTreeMap<DrawerId, Access>,
    pub create: bool,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintDrawerCapability {
    pub scope: DrawerScope,
    pub rights: Rights,
    /// The actor the new handle is bound to; `None` binds it to the presented handle's own
    /// subject. Naming another actor is an introduction: the presented handle needs
    /// `Rights::DELEGATE`, and the actor must be live and a strict descendant of the presented
    /// handle's subject in the actor tree (anyone else is `Escalation`).
    pub subject: Option<Uuid7>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDrawer {
    pub capability: CapId,
    pub command: CommandId,
    #[serde(flatten)]
    pub layout: CreateLayout,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerCreated {
    pub drawer_id: DrawerId,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
}
/// Enters `doc_id` in `drawer_id`'s roster at `access`. Two authorities are checked, because
/// two different things are given away: `capability` must hold Write on the drawer (Admin to
/// confer Admin), and `reach` must reach the document at `access` or above, since entering it
/// delegates that access to the drawer's members. They are separate so a service can enter a
/// document into a drawer it owns using a handle its caller lent it for the document alone; a
/// caller holding both passes the same handle twice.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddDoc {
    pub capability: CapId,
    pub reach: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub doc_id: DocId,
    pub access: Access,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMetaDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub kind: String,
    /// One slot inside `kind`: the reference lands on `metaDoc/<kind>/<name>`, so two names
    /// of one kind name two docs and concurrent creators of a name converge on one.
    pub name: String,
    /// Which metadata root publishes the reference, which decides the group granted on the
    /// new doc and the group whose delegation admits the reference in projection.
    pub root: MetaRoot,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaDocCreated {
    pub drawer_id: DrawerId,
    pub kind: String,
    pub name: String,
    pub root: MetaRoot,
    pub doc_id: DocId,
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub heads: Vec<ChangeHash>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub doc_id: DocId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddChild {
    pub capability: CapId,
    pub command: CommandId,
    pub parent_id: DrawerId,
    pub child_id: DrawerId,
    pub access: Access,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveChild {
    pub capability: CapId,
    pub command: CommandId,
    pub parent_id: DrawerId,
    pub child_id: DrawerId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetDrawerPolicy {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub mirror: bool,
}
/// Publish a successor pointer on a drawer's descriptor-bearing doc. Write+ on that drawer;
/// the successor itself is never granted or copied by this operation.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetDrawerSuccessor {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub successor: DrawerId,
}
/// Follow the successor chain with Read access at every hop; stop with AccessRequired before
/// reading an inaccessible hop. Cycle-checked; creates nothing.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveDrawerSuccessor {
    pub capability: CapId,
    pub drawer_id: DrawerId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum SuccessorResolution {
    NoSuccessor,
    Resolved { drawer_id: DrawerId },
    AccessRequired { drawer_id: DrawerId },
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerView {
    pub drawer_id: DrawerId,
    pub layout: DrawerLayout,
    pub descriptor: Drawer,
    pub access: Access,
    pub docs: BTreeMap<DocId, Access>,
    pub children: BTreeMap<DrawerId, Access>,
    /// The admitted `metaDoc` references, per root and keyed by their `kind/name` slot. The
    /// roots are kept apart because a name on the public root and the same name on the
    /// sensitive root name different docs.
    pub meta_docs: BTreeMap<MetaRoot, BTreeMap<String, DocId>>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDocs {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    #[serde(default)]
    pub offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RememberDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    #[serde(default)]
    pub mirror: bool,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForgetDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListKnownDrawers {
    pub capability: CapId,
}
/// The drawers through which the presented authority reaches `doc_id`, each with the access it
/// gets there. Authorized from the request's `Authorization` metadata rather than a body
/// `capability`, because the caller asking has no scoped handle on the doc yet: it holds an
/// identity or some scoped drawers handle, and a doc it reaches through no drawer answers an
/// empty list, so "not in a drawer" and "not visible" look the same.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDocDrawers {
    pub doc_id: DocId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDrawers {
    pub capability: CapId,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub subject_id: Uuid7,
    pub access: Access,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub subject_id: Uuid7,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Principal {
    Peer { peer_id: GroupKey },
    Group { group_id: GroupKey },
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitePrincipal {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub principal: Principal,
    pub access: Access,
}
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokePrincipal {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub principal: Principal,
}

method_core::methods! {
    error = DrawersError, nsid_prefix = "org.example.daybook.drawers.";
    MintDrawerCapability => Capability, procedure, "mintDrawerCapability";
    CreateDrawer => DrawerCreated, procedure, "createDrawer";
    AddDoc => WriteReceipt, procedure, "addDoc";
    CreateMetaDoc => MetaDocCreated, procedure, "createMetaDoc";
    RemoveDoc => WriteReceipt, procedure, "removeDoc";
    AddChild => WriteReceipt, procedure, "addChild";
    RemoveChild => WriteReceipt, procedure, "removeChild";
    GetDrawer => DrawerView, query, "getDrawer";
    ListDocs => ListedDocs, procedure, "listDocs";
    RememberDrawer => (), procedure, "rememberDrawer";
    ForgetDrawer => (), procedure, "forgetDrawer";
    ListKnownDrawers => KnownDrawers, query, "listKnownDrawers";
    SetDrawerPolicy => (), procedure, "setDrawerPolicy";
    SetDrawerSuccessor => (), procedure, "setDrawerSuccessor";
    ResolveDrawerSuccessor => SuccessorResolution, query, "resolveDrawerSuccessor";
    ListDrawers => ListedDrawers, query, "listDrawers";
    ListDocDrawers => DocDrawers, query, "listDocDrawers";
    GrantDrawer => (), procedure, "grantDrawer";
    RevokeDrawer => (), procedure, "revokeDrawer";
    InvitePrincipal => (), procedure, "invitePrincipal";
    RevokePrincipal => (), procedure, "revokePrincipal";
}

declare_service_protocol! {
    pub enum DrawersProtocol => DrawersMessage {
        unary {
            MintDrawerCapability(MintDrawerCapability),
            CreateDrawer(CreateDrawer),
            AddDoc(AddDoc),
            CreateMetaDoc(CreateMetaDoc),
            RemoveDoc(RemoveDoc),
            AddChild(AddChild),
            RemoveChild(RemoveChild),
            GetDrawer(GetDrawer),
            ListDocs(ListDocs),
            RememberDrawer(RememberDrawer),
            ForgetDrawer(ForgetDrawer),
            ListKnownDrawers(ListKnownDrawers),
            SetDrawerPolicy(SetDrawerPolicy),
            SetDrawerSuccessor(SetDrawerSuccessor),
            ResolveDrawerSuccessor(ResolveDrawerSuccessor),
            ListDrawers(ListDrawers),
            ListDocDrawers(ListDocDrawers),
            GrantDrawer(GrantDrawer),
            RevokeDrawer(RevokeDrawer),
            InvitePrincipal(InvitePrincipal),
            RevokePrincipal(RevokePrincipal)
        }
        stream {}
    }
}

pub const CREATE_DRAWER_OBJECT: &str = "create drawer";
pub fn drawer_object(access: Access, drawer: &DrawerId) -> String {
    let verb = match access {
        Access::Relay => "relay",
        Access::Read => "read",
        Access::Write => "write",
        Access::Admin => "admin",
    };
    format!("{verb} drawer {drawer}")
}
pub fn drawer_placement(drawer: &DrawerId) -> String {
    format!("drawer {drawer}")
}
