//! MergeDoc: merge one history-sharing document into another.

use super::commands::{Detail, Kind, State};
use super::pipeline::digest;
use super::*;

/// Merge `source` as of `at` into `target`: Automerge's merge of the source's changes, which adds
/// no change of its own. Both must share history (a branch and its origin, or two branches of one
/// origin), and every facet of the source at `at` must be accepted. The receipt names the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeDoc {
    pub capability: CapId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandId>,
    pub source_id: DocId,
    pub at: At,
    pub target_id: DocId,
}

impl Operation for MergeDoc {
    type Output = WriteReceipt;
    type Error = DocsError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.docs.mergeDoc")
        .with_schemas("WriteReceipt", "DocsError");
}

impl DocsService {
    async fn merge(
        &self,
        context: &RequestContext,
        params: MergeDoc,
    ) -> Result<WriteReceipt, DocsError> {
        let actor = self.actor(context, params.capability).await?;
        self.authorize(context, &actor, AccessRequest::Read(&params.source_id))
            .await?;
        self.authorize(context, &actor, AccessRequest::Write(&params.target_id))
            .await?;
        let request_digest = digest(&(
            "merge",
            actor.uuid,
            &params.source_id,
            &params.at,
            &params.target_id,
        ));
        if let Some(command) = &params.command
            && let Some(receipt) = self.replay(command, request_digest).await?
        {
            return Ok(receipt);
        }
        let source = self.handle(&params.source_id).await?;
        let (heads, mut fork) = source
            .with_document_read(|doc| -> Result<_, DocsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                // A merge brings the source's history into the target unexamined, so it is held to
                // what a write's base is held to: every facet it brings — including one whose
                // content was deleted — must be accepted.
                let gate = Gate::new(doc, &heads);
                let keys: BTreeSet<_> = present_keys(doc, &heads)?
                    .into_iter()
                    .chain(gate.keys())
                    .collect();
                for key in keys {
                    unaccepted(gate.verify(&key)).map_err(|verdict| {
                        DocsError::BaseNotAccepted {
                            facet_key: key,
                            verdict,
                        }
                    })?;
                }
                Ok((heads.clone(), doc.fork_at(&heads).map_err(repository)?))
            })
            .await?;
        let target = self.handle(&params.target_id).await?;
        if let Some(command) = &params.command {
            self.commands
                .begin(
                    command,
                    Kind::Merge,
                    request_digest,
                    actor.uuid,
                    Some(&params.target_id),
                    None,
                    Some(&Detail::Merge {
                        source: params.source_id.clone(),
                        heads: heads.clone(),
                    }),
                )
                .await?;
        }
        let merged = target
            .with_document(|doc| -> Result<_, DocsError> {
                // Every document starts from its creating change, which depends on nothing. Two
                // documents share history exactly when the target holds the source's.
                let related = fork
                    .get_changes(&[])
                    .iter()
                    .filter(|change| change.deps().is_empty())
                    .all(|root| doc.get_change_by_hash(&root.hash()).is_some());
                if !related {
                    return Err(DocsError::UnrelatedHistory);
                }
                doc.merge(&mut fork).map_err(repository)?;
                Ok(sorted(doc.get_heads()))
            })
            .await;
        match (&params.command, merged) {
            (_, Ok(Ok(heads))) => {
                if let Some(command) = &params.command {
                    self.commands.advance(command, State::Done, &heads).await?;
                }
                Ok(WriteReceipt {
                    doc_id: params.target_id,
                    heads,
                })
            }
            (Some(command), Ok(Err(error))) => {
                self.commands.discard(command).await?;
                Err(error)
            }
            (None, Ok(Err(error))) => Err(error),
            // Persistence failed after the merge may have been applied in memory: the row stays
            // for the boot pass, which reads the outcome off the target.
            (_, Err(error)) => Err(repository(error)),
        }
    }
}

impl Handler<MergeDoc> for DocsService {
    async fn call(
        &self,
        request: Envelope<MergeDoc>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocsError> {
        Ok(self.merge(&request.context, request.params).await?.into())
    }
}
