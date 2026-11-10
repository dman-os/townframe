use super::DocsError;
use crate::authority::CapId;
use crate::dmeta::DocId;
use serde::Deserialize;
use serde::Serialize;

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalDocUsability {
    NotObserved,
    NoLocalContent,
    PendingMaterialization,
    Usable,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocAvailability {
    pub doc_id: DocId,
    pub local_usability: LocalDocUsability,
    pub sync_demand: bool,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDocAvailability {
    pub capability: CapId,
    pub doc_id: DocId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveDocAvailability {
    pub capability: CapId,
    pub doc_id: DocId,
}
service_core_rs::service_method!(ObserveDocAvailability => stream DocAvailability, DocsError, "org.example.daybook.docs.observeDocAvailability");
