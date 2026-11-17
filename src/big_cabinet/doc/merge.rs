//! MergeDoc: merge one history-sharing document into another.

use super::commands::Detail;
use super::commands::Kind;
use super::commands::State;
use super::pipeline::digest;
use super::*;

impl DocsService {
    async fn merge(
        &self,
        context: &RequestContext,
        metadata: &HttpMetadata,
        params: MergeDoc,
    ) -> Result<WriteReceipt, DocsError> {
        let caller = self
            .signing_invocation(context, metadata, params.capability)
            .await?;
        let actor = &caller.signer;
        // A merge is held to what it changes, not to the whole documents: each datum it changes
        // on the target must be readable on the source and writable on the target at the
        // actor's kinds. The delta is only known once the source is forked against the target,
        // so the kinds are taken here and applied there.
        let source_kinds = self
            .authorize_read(context, &caller.authority, &params.source_id)
            .await?;
        let target_kinds = self
            .authorize_write(context, &caller.authority, &params.target_id)
            .await?;
        let request_digest = digest(&(
            "merge",
            actor.actor_id,
            &params.source_id,
            &params.at,
            &params.target_id,
        ));
        if let Some(command) = &params.command
            && let Some(receipt) = self.replay(command, request_digest, actor.actor_id).await?
        {
            return Ok(receipt);
        }
        let source = self.handle(&params.source_id).await?;
        let (heads, mut fork) = source
            .with_document_read(|doc| -> Result<_, DocsError> {
                context.validate()?;
                let heads = resolve(doc, &params.at)?;
                // A merge brings the source's history into the target unexamined, so it is held to
                // what a write's base is held to: every datum it brings — including one whose
                // content was removed but whose signatures remain — must be accepted.
                let gate = Gate::new(doc, &heads);
                for key in gate.keys() {
                    unaccepted(gate.verify(&key)).map_err(|verdict| {
                        DocsError::BaseNotAccepted {
                            datum_key: key,
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
                    actor.actor_id,
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
                // documents share history exactly when the target holds the source's. Holding is
                // tested on change metadata: `get_change_by_hash` re-encodes the change by
                // scanning every operation in the document.
                let related = fork
                    .get_changes(&[])
                    .iter()
                    .filter(|change| change.deps().is_empty())
                    .all(|root| doc.get_change_meta_by_hash(&root.hash()).is_some());
                if !related {
                    return Err(DocsError::UnrelatedHistory);
                }
                check_delta(doc, &mut fork, &source_kinds, &target_kinds)?;
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
            (command, Err(error)) => match commit_refusal(&error) {
                // Keyhive refused the commit for this node's standing: nothing persisted, so
                // the command is settled as refused rather than left for the boot pass.
                Some(denied) => {
                    if let Some(command) = command {
                        self.commands.discard(command).await?;
                    }
                    Err(denied)
                }
                // Persistence failed after the merge may have been applied in memory: the row
                // stays for the boot pass, which reads the outcome off the target.
                None => Err(repository(error)),
            },
        }
    }
}

/// Refuses a merge that changes a datum outside the actor's kinds: unreadable on the source or
/// unwritable on the target. The delta is what merging `fork` would change on `doc`, read off a
/// scratch copy of `doc` so a refusal leaves it untouched; a merge that changes nothing needs no
/// kind.
fn check_delta(
    doc: &Automerge,
    fork: &mut Automerge,
    source_kinds: &DatumKinds,
    target_kinds: &DatumKinds,
) -> Result<(), DocsError> {
    let before = sorted(doc.get_heads());
    let mut merged = doc.fork();
    merged.merge(fork).map_err(repository)?;
    let after = sorted(merged.get_heads());
    if after == before {
        return Ok(());
    }
    let mut keys = BTreeSet::new();
    let mut whole = false;
    for patch in merged.diff(&before, &after) {
        fn prop(prop: &automerge::Prop) -> Option<&str> {
            match prop {
                automerge::Prop::Map(key) => Some(key.as_str()),
                automerge::Prop::Seq(_) => None,
            }
        }
        // The patch's position is its object's path plus, for a map action, the key it acts on.
        let mut names: Vec<Option<&str>> = patch.path.iter().map(|(_, step)| prop(step)).collect();
        match &patch.action {
            automerge::PatchAction::PutMap { key, .. }
            | automerge::PatchAction::DeleteMap { key } => names.push(Some(key)),
            automerge::PatchAction::Conflict { prop: step }
            | automerge::PatchAction::Increment { prop: step, .. } => names.push(prop(step)),
            _ => {}
        }
        let name = |index: usize| names.get(index).copied().flatten();
        if name(0) != Some("datums") {
            continue;
        }
        // A datum is a key of `datums`; its signatures live under `dmeta.datums.<key>`, and a
        // change to either is a change to that datum. Aliases and actor records change no datum.
        // A change that names no single datum (a container itself replaced) counts as all.
        match (name(1), name(2), name(3)) {
            (None, _, _) | (Some(DMETA_KEY), None, _) => whole = true,
            (Some(DMETA_KEY), Some("datums"), Some(key)) | (Some(key), _, _)
                if key != DMETA_KEY =>
            {
                keys.extend(key.parse::<DatumKey>().ok());
            }
            _ => {}
        }
    }
    let all = matches!(source_kinds, DatumKinds::All) && matches!(target_kinds, DatumKinds::All);
    if (whole && !all)
        || !keys
            .iter()
            .all(|key| source_kinds.allows(key) && target_kinds.allows(key))
    {
        return Err(DocsError::Denied);
    }
    Ok(())
}

impl Handler<MergeDoc> for DocsService {
    async fn call(
        &self,
        request: Envelope<MergeDoc>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DocsError> {
        Ok(self
            .merge(&request.context, &request.metadata, request.params)
            .await?
            .into())
    }
}
