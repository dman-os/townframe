use super::Drawer;
use super::DrawerAccess as Access;
use super::DrawerGroups;
use super::DrawerLayout;
use super::GroupKey;
use super::LayoutKind;
use crate::authority::AuthorityServiceError;
use crate::authority::Authorization;
use crate::authority::CapId;
use crate::authority::Capability;
use crate::authority::Rights;
use crate::dmeta::DocId;
use crate::dmeta::Uuid7;
use crate::doc::CommandId;
use crate::doc::DocsError;
use crate::doc::WriteReceipt;
use automerge::ChangeHash;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::ContextError;
use service_core_rs::Operation;
use service_core_rs::XrpcOperation;
use service_core_rs::declare_service_protocol;
use std::collections::BTreeMap;

pub type DrawerId = DocId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalDrawers {
    pub present_id: DrawerId,
    pub originated_id: DrawerId,
    pub registry_id: DrawerId,
}

impl LocalDrawers {
    pub fn both(&self) -> [&DrawerId; 2] {
        [&self.present_id, &self.originated_id]
    }
}

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
}

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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DrawerScope {
    pub drawers: BTreeMap<DrawerId, Access>,
    pub create: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintDrawerCapability {
    pub authorization: Authorization,
    pub scope: DrawerScope,
    pub rights: Rights,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDrawer {
    pub capability: CapId,
    pub command: CommandId,
    #[serde(flatten)]
    pub layout: CreateLayout,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerCreated {
    pub drawer_id: DrawerId,
    pub heads: Vec<ChangeHash>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub doc_id: DocId,
    pub access: Access,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub doc_id: DocId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddChild {
    pub capability: CapId,
    pub command: CommandId,
    pub parent_id: DrawerId,
    pub child_id: DrawerId,
    pub access: Access,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveChild {
    pub capability: CapId,
    pub command: CommandId,
    pub parent_id: DrawerId,
    pub child_id: DrawerId,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerView {
    pub drawer_id: DrawerId,
    pub layout: DrawerLayout,
    pub descriptor: Drawer,
    pub access: Access,
    pub docs: BTreeMap<DocId, Access>,
    pub children: BTreeMap<DrawerId, Access>,
}
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDoc {
    pub doc_id: DocId,
    pub access: Access,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<DrawerId>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RememberDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    #[serde(default)]
    pub mirror: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForgetDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListKnownDrawers {
    pub capability: CapId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownDrawer {
    pub drawer_id: DrawerId,
    pub mirror: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetDrawerPolicy {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub mirror: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDrawers {
    pub capability: CapId,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDrawer {
    pub drawer_id: DrawerId,
    pub layout: DrawerLayout,
    pub descriptor: Drawer,
    pub access: Access,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub subject_id: Uuid7,
    pub access: Access,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub subject_id: Uuid7,
}
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvitePrincipal {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub principal: Principal,
    pub access: Access,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokePrincipal {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub principal: Principal,
}

macro_rules! operations {
    ($($request:ident => $output:ty, $kind:ident, $nsid:literal;)*) => {$(
        impl Operation for $request {
            type Output = $output;
            type Error = DrawersError;
            const XRPC: XrpcOperation =
                XrpcOperation::$kind(concat!("org.example.daybook.drawers.", $nsid))
                    .with_schemas(stringify!($output), stringify!(DrawersError));
        }
    )*};
}
operations! {
    MintDrawerCapability => Capability, procedure, "mintDrawerCapability";
    CreateDrawer => DrawerCreated, procedure, "createDrawer";
    AddDoc => WriteReceipt, procedure, "addDoc";
    RemoveDoc => WriteReceipt, procedure, "removeDoc";
    AddChild => WriteReceipt, procedure, "addChild";
    RemoveChild => WriteReceipt, procedure, "removeChild";
    GetDrawer => DrawerView, query, "getDrawer";
    ListDocs => Vec<ListedDoc>, procedure, "listDocs";
    RememberDrawer => (), procedure, "rememberDrawer";
    ForgetDrawer => (), procedure, "forgetDrawer";
    ListKnownDrawers => Vec<KnownDrawer>, query, "listKnownDrawers";
    SetDrawerPolicy => (), procedure, "setDrawerPolicy";
    ListDrawers => Vec<ListedDrawer>, query, "listDrawers";
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
            RemoveDoc(RemoveDoc),
            AddChild(AddChild),
            RemoveChild(RemoveChild),
            GetDrawer(GetDrawer),
            ListDocs(ListDocs),
            RememberDrawer(RememberDrawer),
            ForgetDrawer(ForgetDrawer),
            ListKnownDrawers(ListKnownDrawers),
            SetDrawerPolicy(SetDrawerPolicy),
            ListDrawers(ListDrawers),
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
