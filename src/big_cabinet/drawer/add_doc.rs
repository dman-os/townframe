//! AddDoc: enter a doc in a drawer's roster at an access level (grant first, entry second).

use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerDocEntry;
use big_repo::keyhive_core::principal::identifier::Identifier;
use std::collections::BTreeSet;

fn drawer_access(access: KeyhiveAccess) -> Access {
    match access {
        KeyhiveAccess::Relay => Access::Relay,
        KeyhiveAccess::Read => Access::Read,
        KeyhiveAccess::Edit => Access::Write,
        KeyhiveAccess::Admin => Access::Admin,
    }
}

impl Core {
    /// The level `docs_group` is delegated on `doc` in Keyhive, `None` when it holds no
    /// delegation of its own. Only the direct delegation counts: a child link makes the parent's
    /// docs group reach every doc of the child, and that reach is neither this roster's grant to
    /// revoke nor a substitute for granting it. The doc id is the doc's Keyhive identifier (an
    /// Ed25519 point), so an id of another width or off the curve is refused here rather than at
    /// the grant.
    pub(crate) async fn docs_group_level(
        &self,
        docs_group: &BigKeyhiveGroup,
        doc: &DocId,
    ) -> Result<Option<Access>, DrawersError> {
        let key =
            ed25519_dalek::VerifyingKey::from_bytes(&doc.to_bytes32().map_err(keyhive_error)?)
                .map_err(|_| keyhive_error(format!("{doc} is not a valid Ed25519 point")))?;
        Ok(self
            .big_repo
            .keyhive()
            .direct_access_on(&docs_group.id().into(), Identifier::from(key))
            .await
            .map(drawer_access))
    }

    /// The receipt of an operation that found the roster already saying what it was asked to
    /// say: the drawer doc's current heads, read as this service without verifying any datum.
    pub(crate) async fn roster_receipt(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
    ) -> Result<WriteReceipt, DrawersError> {
        let read = self
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    read: [drawer.clone()].into(),
                    ..Default::default()
                },
                |Authorization {
                     authority: capability,
                     ..
                 }| async move {
                    self.docs()
                        .call(
                            context.child(),
                            ReadDatums {
                                capability,
                                doc_id: drawer.clone(),
                                at: At::Current,
                                datums: Some(BTreeSet::new()),
                                optimistic: false,
                            },
                        )
                        .await
                        .map_err(docs_error)
                },
            )
            .await?;
        Ok(WriteReceipt {
            doc_id: drawer.clone(),
            heads: read.heads,
        })
    }

    /// Publish the entry `doc` at `access` unless the roster already lists it so.
    async fn publish_entry(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
        doc: &DocId,
        access: Access,
    ) -> Result<Option<WriteReceipt>, DrawersError> {
        if self.state.doc_entry(drawer, doc).await? == Some(access) {
            return Ok(None);
        }
        let entry = DrawerDocEntry::new(access);
        let receipt = self
            .write_roster(
                context,
                drawer,
                None,
                BTreeMap::from([(
                    doc_entry_key(doc),
                    serde_json::to_value(entry).expect("a doc entry serializes"),
                )]),
                BTreeSet::new(),
            )
            .await?;
        #[cfg(test)]
        self.crash_at(super::CrashPoint::RosterWritten)?;
        self.state.put_doc(drawer, doc, access).await?;
        Ok(Some(receipt))
    }

    /// Make `doc` an entry of each of `drawers` at `access`, idempotently: only what the Keyhive
    /// grant and the roster do not already say is done, so a retry or a boot replay finishes an
    /// interrupted attempt. Widening grants before it publishes, narrowing publishes before it
    /// re-grants (ADR 019), so an interruption leaves the entry at or below the authority behind
    /// it. Receipts are in `drawers` order: the roster write's, or the drawer's current heads
    /// when nothing had to change.
    ///
    /// The widening grants run concurrently, so their Keyhive reconciliation waits overlap; only
    /// once all of them have reconciled are the roster entries published, concurrently since
    /// they write different drawer docs. That keeps "grant first, entry second": no entry is
    /// published before the grants behind it have reconciled. No grant check here reads another
    /// drawer's grant (each drawer has its own docs group, and `docs_group_level` reads that
    /// group's direct delegation alone), so the grants need not run one after another.
    pub(crate) async fn enter(
        &self,
        context: &RequestContext,
        drawers: &[DrawerId],
        doc: &DocId,
        access: Access,
    ) -> Result<Vec<WriteReceipt>, DrawersError> {
        let instr_start = std::time::Instant::now(); // INSTR
        let mut narrowed = BTreeMap::new();
        let mut to_publish = Vec::with_capacity(drawers.len());
        let mut widen = Vec::new();
        for (index, drawer) in drawers.iter().enumerate() {
            let (_meta, docs_group, _sensitive) = self.groups(drawer).await?;
            let current = self.docs_group_level(&docs_group, doc).await?;
            match current {
                Some(current) if current == access => {}
                Some(current) if current > access => {
                    let written = self.publish_entry(context, drawer, doc, access).await?;
                    self.big_repo
                        .revoke_doc_access(doc.clone(), docs_group.clone())
                        .await
                        .map_err(keyhive_error)?;
                    self.big_repo
                        .grant_doc_access(doc.clone(), docs_group, keyhive_access(access))
                        .await
                        .map_err(keyhive_error)?;
                    narrowed.insert(index, written);
                    continue;
                }
                _ => widen.push(docs_group),
            }
            to_publish.push(index);
        }
        let instr_issued = instr_start.elapsed(); // INSTR
        if !widen.is_empty() {
            // The grants run concurrently: each one's reconciliation wait overlaps the others,
            // so entering several drawers costs about one wait instead of one per drawer.
            futures::future::try_join_all(widen.into_iter().map(|docs_group| {
                self.big_repo
                    .grant_doc_access(doc.clone(), docs_group, keyhive_access(access))
            }))
            .await
            .map_err(keyhive_error)?;
            #[cfg(test)]
            self.crash_at(super::CrashPoint::DocGranted)?;
        }
        let instr_reconciled = instr_start.elapsed(); // INSTR
        let published = futures::future::try_join_all(to_publish.iter().map(|&index| async move {
            let written = self
                .publish_entry(context, &drawers[index], doc, access)
                .await?;
            Ok::<_, DrawersError>((index, written))
        }))
        .await?;
        narrowed.extend(published);
        tracing::debug!(?instr_issued, ?instr_reconciled, published = ?instr_start.elapsed(), drawers = drawers.len(), "INSTR enter timing"); // INSTR
        futures::future::try_join_all(drawers.iter().enumerate().map(|(index, drawer)| {
            let written = narrowed.remove(&index).flatten();
            async move {
                match written {
                    Some(receipt) => Ok(receipt),
                    None => self.roster_receipt(context, drawer).await,
                }
            }
        }))
        .await
    }

    pub(crate) async fn resume_add_doc(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let Op::AddDoc {
            drawer_id,
            doc_id,
            access,
        } = &row.op
        else {
            unreachable!(
                "resume_add_doc is dispatched on an add-doc row: {:?}",
                row.op
            )
        };
        let receipt = self
            .enter(context, std::slice::from_ref(drawer_id), doc_id, *access)
            .await?
            .pop()
            .expect("one drawer yields one receipt");
        self.ledger.finish(&row.command, &receipt).await
    }
}

impl DrawerService {
    async fn add_doc(
        &self,
        context: &RequestContext,
        params: AddDoc,
    ) -> Result<WriteReceipt, DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        let actor = &caller.signer;
        // Conferring Admin on the doc takes Admin on the drawer; any other level takes Write.
        let needed = if params.access == Access::Admin {
            Access::Admin
        } else {
            Access::Write
        };
        core.authorize(context, &caller.authority, &params.drawer_id, needed)
            .await?;
        // Granting the docs group on a doc gives away what the caller holds on it: the `reach`
        // handle must itself reach the doc at the level it confers, through whatever path carries
        // it. It may be a handle lent by whoever asked for the entry, not the drawer's own.
        let reach = core.invocation(context, params.reach).await?;
        if core
            .doc_level(context, &reach.authority, &params.doc_id)
            .await?
            < Some(params.access)
        {
            return Err(DrawersError::Denied);
        }
        let digest = digest(&(
            "addDoc",
            actor.actor_id,
            &params.drawer_id,
            &params.doc_id,
            params.access,
        ));
        let op = Op::AddDoc {
            drawer_id: params.drawer_id.clone(),
            doc_id: params.doc_id.clone(),
            access: params.access,
        };
        core.ledger
            .run(&params.command, digest, actor.actor_id, op, |_| async {
                Ok(core
                    .enter(
                        context,
                        std::slice::from_ref(&params.drawer_id),
                        &params.doc_id,
                        params.access,
                    )
                    .await?
                    .pop()
                    .expect("one drawer yields one receipt"))
            })
            .await
    }
}

impl Handler<AddDoc> for DrawerService {
    async fn call(
        &self,
        request: Envelope<AddDoc>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DrawersError> {
        Ok(self.add_doc(&request.context, request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    utils_rs::table_tests! {
        add_doc_wire_shape,
        (access, expected),
        {
            let request = AddDoc {
                capability: CapId::default(),
                reach: CapId::default(),
                command: CommandId::try_from("add-1".to_owned()).unwrap(),
                drawer_id: DocId::new([1; 32]),
                doc_id: DocId::new([2; 32]),
                access,
            };
            let value = serde_json::to_value(&request).unwrap();
            assert_eq!(
                value,
                json!({
                    "capability": "00000000-0000-0000-0000-000000000000",
                    "reach": "00000000-0000-0000-0000-000000000000",
                    "command": "add-1",
                    "drawerId": DocId::new([1; 32]).to_string(),
                    "docId": DocId::new([2; 32]).to_string(),
                    "access": expected,
                })
            );
            assert_eq!(serde_json::from_value::<AddDoc>(value).unwrap(), request);
        }
    }

    add_doc_wire_shape! {
        relay: (Access::Relay, "Relay"),
        read: (Access::Read, "Read"),
        write: (Access::Write, "Write"),
        admin: (Access::Admin, "Admin"),
    }

    #[test]
    fn an_unknown_access_level_is_refused() {
        let error = serde_json::from_value::<AddDoc>(json!({
            "capability": "00000000-0000-0000-0000-000000000000",
            "reach": "00000000-0000-0000-0000-000000000000",
            "command": "add-1",
            "drawerId": DocId::new([1; 32]).to_string(),
            "docId": DocId::new([2; 32]).to_string(),
            "access": "Pull",
        }))
        .unwrap_err();
        assert!(error.to_string().contains("Pull"), "{error}");
    }

    #[test]
    fn keyhive_access_round_trips_through_drawer_access() {
        for access in Access::ALL {
            assert_eq!(drawer_access(keyhive_access(access)), access);
        }
    }
}
