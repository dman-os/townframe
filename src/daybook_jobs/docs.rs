use crate::interlude::*;

use crate::gen::doc::{Doc, DocAddedEvent};

#[restate_sdk::workflow]
pub trait DocsPipeline {
    async fn run(event: Json<DocAddedEvent>) -> Result<(), HandlerError>;
}

pub struct DocPipelineImpl {
    pub cx: SharedCtx,
}

impl DocsPipeline for DocPipelineImpl {
    #[tracing::instrument(skip(self, rcx), fields(key = rcx.key()), err(Debug))]
    async fn run(
        &self,
        rcx: WorkflowContext<'_>,
        Json(event): Json<DocAddedEvent>,
    ) -> Result<(), HandlerError> {
        let heads = utils_rs::am::parse_commit_heads(&event.heads).map_err(|err| {
            TerminalError::new_with_code(StatusCode::BAD_REQUEST.as_u16(), format!("{err}"))
        })?;
        let am_doc_id = samod::DocumentId::from_str(&event.id).map_err(|err| {
            TerminalError::new_with_code(
                StatusCode::BAD_REQUEST.as_u16(),
                format!("error parsing doc_id: {err}"),
            )
        })?;

        let Json(doc) = rcx
            .run(|| async {
                match self
                    .cx
                    .acx
                    .hydrate_path_at_head::<Doc>(&am_doc_id, &heads, automerge::ROOT, vec![])
                    .await
                {
                    Ok(Some(doc)) => Ok(Json(dbg!(doc))),
                    Ok(None) => Err(TerminalError::new_with_code(
                        400,
                        format!("doc {am_doc_id} is not a valid daybook doc"),
                    ))?,
                    Err(utils_rs::am::HydrateAtHeadError::HashNotFound(hash)) => {
                        Err(TerminalError::new_with_code(
                            400,
                            format!("hash {hash} was not found in daybook doc {am_doc_id}"),
                        ))?
                    }
                    Err(utils_rs::am::HydrateAtHeadError::Other(err)) => {
                        Err(HandlerError::from(format!("{err:}")))
                    }
                }
            })
            .await?;
        rcx.run(|| async {
            let log = vec![llm::chat::ChatMessage::user()
                .content(format!("What do you think of {doc:?}"))
                .build()];
            let response = self
                .cx
                .llm_provider
                .chat(&log)
                .await
                .map_err(HandlerError::from)?;
            info!(?response, "LLM call");
            Ok(())
        })
        .await?;
        Ok(())
    }
}
