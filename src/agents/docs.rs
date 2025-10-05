use crate::interlude::*;

use std::collections::HashSet;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocsCreatedEvent {
    pub id: String,
    pub tickets: HashSet<String>,
}

#[restate_sdk::service]
pub trait DocsPipeline {
    async fn created(request: Json<DocsCreatedEvent>) -> Result<bool, HandlerError>;
}

pub struct DocPipelineImpl;

impl DocsPipeline for DocPipelineImpl {
    async fn created(
        &self,
        _ctx: Context<'_>,
        Json(DocsCreatedEvent { id: _, tickets: _ }): Json<DocsCreatedEvent>,
    ) -> Result<bool, HandlerError> {
        use daybook_types::types::doc::Doc;
        Ok(true)
    }
}
