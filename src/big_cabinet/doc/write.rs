//! WriteFacets: set and remove whole facet values under an explicit write condition.

use super::commands::Kind;
use super::commands::State;
use super::pipeline::Failure;
use super::pipeline::Plan;
use super::pipeline::digest;
use super::*;

impl DocsService {
    async fn write(
        &self,
        context: &RequestContext,
        params: WriteFacets,
    ) -> Result<WriteReceipt, DocsError> {
        let caller = self.invocation(context, params.capability).await?;
        let actor = &caller.signer;
        self.authorize(
            context,
            &caller.authority,
            AccessRequest::Write(&params.doc_id),
        )
        .await?;
        let request_digest = digest(&(
            "write",
            actor.actor_id,
            &params.doc_id,
            &params.mode,
            &params.set,
            &params.remove,
        ));
        if let Some(command) = &params.command
            && let Some(receipt) = self.replay(command, request_digest, actor.actor_id).await?
        {
            return Ok(receipt);
        }
        let set = self.schemas(params.set, &params.remove)?;
        if let Some(command) = &params.command {
            self.commands
                .begin(
                    command,
                    Kind::Write,
                    request_digest,
                    actor.actor_id,
                    Some(&params.doc_id),
                    None,
                    None,
                )
                .await?;
        }
        let mut plan = Plan {
            document: &params.doc_id,
            doc_uuid: Uuid7::new(),
            actor,
            set,
            remove: params.remove,
            now: jiff::Timestamp::now(),
            predecessors: BTreeMap::new(),
            aliases: BTreeMap::new(),
        };
        let result = self
            .commit_write(
                context,
                actor.capability,
                &params.mode,
                &mut plan,
                params.command.as_ref(),
            )
            .await;
        match (&params.command, result) {
            (Some(command), Ok(heads)) => {
                self.commands.advance(command, State::Done, &heads).await?;
                Ok(WriteReceipt {
                    doc_id: params.doc_id,
                    heads,
                })
            }
            (None, Ok(heads)) => Ok(WriteReceipt {
                doc_id: params.doc_id,
                heads,
            }),
            (Some(command), Err(Failure::Refused(error))) => {
                self.commands.discard(command).await?;
                Err(error)
            }
            (_, Err(failure)) => Err(failure.into_error()),
        }
    }

    pub(super) async fn commit_write(
        &self,
        context: &RequestContext,
        capability: CapId,
        mode: &WriteMode,
        plan: &mut Plan<'_>,
        command: Option<&CommandId>,
    ) -> Result<Vec<ChangeHash>, Failure> {
        let document = self.handle(plan.document).await?;
        let staged = document
            .with_document(|doc| -> Result<_, DocsError> {
                let base = Self::condition(doc, mode)?;
                let gate = Gate::new(doc, &base);
                for key in plan.keys() {
                    unaccepted(gate.verify(key)).map_err(|verdict| DocsError::BaseNotAccepted {
                        facet_key: key.clone(),
                        verdict,
                    })?;
                }
                let mut predecessors = BTreeMap::new();
                for key in plan.keys() {
                    let ids = predecessor_ids(doc, key, &base, HistoryBudget::default()).map_err(
                        |error| DocsError::BaseNotAccepted {
                            facet_key: key.clone(),
                            verdict: Unaccepted::Pending {
                                reason: format!("predecessors: {error:?}"),
                            },
                        },
                    )?;
                    predecessors.insert(key.clone(), ids);
                }
                plan.predecessors = predecessors;
                let tx = doc
                    .transaction_at(PatchLog::null(), &base)
                    .expect("a null patch log matches any document");
                let (tx, claims) = self.stage(tx, plan, None)?;
                tx.rollback();
                Ok((base, claims))
            })
            .await
            .map_err(repository)??;
        let (base, claims) = staged;
        let signatures = self.sign(context, capability, &claims).await?;
        let committed = document
            .with_document(|doc| -> Result<_, DocsError> {
                Self::condition(doc, mode)?;
                doc.set_actor(doc_actor(&self.node_issuer, plan.document));
                let tx = doc
                    .transaction_at(PatchLog::null(), &base)
                    .expect("a null patch log matches any document");
                let (tx, signed) = self.stage(tx, plan, Some(&signatures))?;
                assert_eq!(signed, claims, "a plan stages the same claims twice");
                let options = match command {
                    Some(command) => CommitOptions::default().with_message(command.as_str()),
                    None => CommitOptions::default(),
                };
                tx.commit_with(options);
                Ok(sorted(doc.get_heads()))
            })
            .await
            .map_err(|error| Failure::Ambiguous(repository(error)))?;
        Ok(committed?)
    }
}

impl Handler<WriteFacets> for DocsService {
    async fn call(
        &self,
        request: Envelope<WriteFacets>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocsError> {
        Ok(self.write(&request.context, request.params).await?.into())
    }
}
