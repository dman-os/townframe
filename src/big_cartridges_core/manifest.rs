use big_cabinet_core::datum::Extra;
use big_cabinet_core::datum::Nsid;
use big_cabinet_core::reference::DatumPin;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

use super::AUTHORITY;
use super::Digest;
use super::Necessity;
use super::Resolution;

const PERMBIND_AUTHORITY: &str = "org.example.daybook.cartridge";
const COMPONENT_AUTHORITY: &str = "org.example.daybook.cartridge";
#[macros::datum]
#[derive(Debug, Clone, PartialEq)]
pub enum Permbind {
    Datum(PermbindDatum),
    Method(PermbindMethod),
    Drawers(PermbindDrawers),
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub initialization: BTreeMap<String, Initializer>,
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
