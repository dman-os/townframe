use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::Nsid;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::reference::DatumPin;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::AUTHORITY;
use super::Digest;
use super::Necessity;
use super::Resolution;
use crate::config::InvalidNetTarget;
use crate::config::NetTarget;

const PERMBIND_AUTHORITY: &str = "org.example.daybook.cartridge";
const COMPONENT_AUTHORITY: &str = "org.example.daybook.cartridge";
#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum Permbind {
    Datum(PermbindDatum),
    Method(PermbindMethod),
    Drawers(PermbindDrawers),
    Net(PermbindNet),
    #[datum(other)]
    Unknown(big_cabinet_core::datum::UnknownMember),
}

#[macros::datum(authority = PERMBIND_AUTHORITY, name = "datum")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermbindDatum {
    pub nsid: Nsid,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<Necessity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<Necessity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<Necessity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete: Option<Necessity>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(authority = PERMBIND_AUTHORITY, name = "method")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermbindMethod {
    pub nsid: Nsid,
    pub description: String,
    #[serde(default, skip_serializing_if = "Necessity::is_default")]
    pub necessity: Necessity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_revision: Option<u32>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `#net { host, port?, methods? }`: a host a routine may use the network to reach (ADR 049 §2).
/// It gates the routine's outgoing `wasi:http` requests to URLs that are not the node's own
/// `dispatch.local`, and is the permbind raw `wasi:sockets` will use. A routine that takes it
/// reaches the host only once the cartridge also holds the matching `grant/net` datum.
///
/// Matching is exact (no wildcards): the host case-insensitively, the port as stated (none means
/// the HTTPS port, 443, see [`NetTarget`]), and `methods` (HTTP verbs, upper case) when present.
#[macros::datum(authority = PERMBIND_AUTHORITY, name = "net")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermbindNet {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// The HTTP verbs the routine may use; every verb when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<BTreeSet<String>>,
    pub description: String,
    #[serde(flatten)]
    pub extra: Extra,
}

impl PermbindNet {
    /// The host and port this permbind names, as [`Cartridge::validate`] checked them.
    pub fn target(&self) -> Result<NetTarget, InvalidNetTarget> {
        NetTarget::new(&self.host, self.port)
    }

    /// Whether `method` is among the verbs this permbind allows.
    pub fn allows_method(&self, method: &str) -> bool {
        self.methods
            .as_ref()
            .is_none_or(|methods| methods.contains(method))
    }
}

#[macros::datum(authority = PERMBIND_AUTHORITY, name = "drawers")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermbindDrawers {
    pub description: String,
    #[serde(default, skip_serializing_if = "Resolution::is_default")]
    pub resolution: Resolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<Necessity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<Necessity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<Necessity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete: Option<Necessity>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl PermbindDrawers {
    /// The most access to an existing drawer this permbind declares: reading is `Read`, and
    /// writing or deleting (which has no access level of its own) is `Write`. `None` when it
    /// declares neither, as for a permbind that only creates drawers, which no drawer grant
    /// satisfies (a create-only routine reaches no existing drawer).
    pub fn declared_access(&self) -> Option<DrawerAccess> {
        [
            (self.read.is_some(), DrawerAccess::Read),
            (self.write.is_some(), DrawerAccess::Write),
            (self.delete.is_some(), DrawerAccess::Write),
        ]
        .into_iter()
        .filter_map(|(declared, access)| declared.then_some(access))
        .max()
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "WASM manifest members carry their typed component metadata inline"
)]
#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum Component {
    Wasm(WasmComponent),
    #[datum(other)]
    Unknown(big_cabinet_core::datum::UnknownMember),
}

#[macros::datum(authority = COMPONENT_AUTHORITY, name = "wasm")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmComponent {
    pub bytes: DatumPin,
    pub digest: Digest,
    pub size: u64,
    pub media_type: String,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(authority = AUTHORITY, name = "imageResource")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageResource {
    pub src: DatumPin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sizes: Option<String>,
    #[serde(rename = "type")]
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(authority = AUTHORITY, name = "nodule")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nodule {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routines: BTreeMap<String, Routine>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub revisioned_stores: BTreeMap<String, RevisionedStore>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub local_state: BTreeMap<String, LocalState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initialization: Vec<Initializer>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub bindings: IndexMap<String, serde_json::Value>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Routine {
    pub r#impl: RoutineImpl,
    pub implements: Nsid,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub uses: BTreeMap<String, RoutineUse>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum RoutineImpl {
    ComponentExport(ComponentExport),
    #[datum(other)]
    Unknown(big_cabinet_core::datum::UnknownMember),
}

#[macros::datum(authority = AUTHORITY, name = "componentExport")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentExport {
    pub component: String,
    pub export: String,
    #[serde(flatten)]
    pub extra: Extra,
}

fn is_false(value: &bool) -> bool {
    !*value
}
#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutineUse {
    #[serde(default, skip_serializing_if = "is_false")]
    pub read: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub write: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub create: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub delete: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevisionedStore {
    pub selector: Nsid,
    pub entry: Nsid,
    pub mode: StoreMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Nsid>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StoreMode {
    Frontier,
    Log,
    #[datum(other)]
    Unknown(String),
}

#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Initializer {
    pub key: String,
    pub target: Nsid,
    pub mode: RunMode,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RunMode {
    Once,
    EveryStart,
    #[datum(other)]
    Unknown(String),
}

#[macros::datum]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalState {
    pub kind: StateKind,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StateKind {
    Sqlite,
    Kv,
    #[datum(other)]
    Unknown(String),
}
