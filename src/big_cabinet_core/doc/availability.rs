use super::DocsError;
use crate::authority::CapId;
use crate::dmeta::DocId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalDocUsability {
    NotObserved,
    NoLocalContent,
    PendingMaterialization,
    Usable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocAvailability {
    pub doc_id: DocId,
    pub local_usability: LocalDocUsability,
    pub sync_demand: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDocAvailability {
    pub capability: CapId,
    pub doc_id: DocId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserveDocAvailability {
    pub capability: CapId,
    pub doc_id: DocId,
}
service_core_rs::service_operation!(ObserveDocAvailability => stream DocAvailability, DocsError, "org.example.daybook.docs.observeDocAvailability");
