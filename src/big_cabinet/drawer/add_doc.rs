//! AddDoc: enter a doc in a drawer's roster at an access level (grant first, entry second).

use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use crate::doc::{At, ReadFacets};
use big_cabinet_core::drawer::DrawerDocEntry;
use big_repo::keyhive_core::principal::identifier::Identifier;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub doc_id: DocId,
    pub access: Access,
}

impl Operation for AddDoc {
    type Output = WriteReceipt;
    type Error = DrawersError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.drawers.addDoc")
        .with_schemas("WriteReceipt", "DrawersError");
}

fn drawer_access(access: KeyhiveAccess) -> Access {
    match access {
        KeyhiveAccess::Relay => Access::Relay,
        KeyhiveAccess::Read => Access::Read,
        KeyhiveAccess::Edit => Access::Write,
        KeyhiveAccess::Admin => Access::Admin,
    }
}

impl Core {
    /// The level `docs_group` holds on `doc` in Keyhive, `None` when it holds nothing. The doc id
    /// is the doc's Keyhive identifier (an Ed25519 point), so an id of another width or off the
    /// curve is refused here rather than at the grant.
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
            .agent_access_on(&docs_group.id().into(), Identifier::from(key))
            .await
            .map(drawer_access))
    }

    /// The receipt of an operation that found the roster already saying what it was asked to
    /// say: the drawer doc's current heads, read as this service without verifying any facet.
    pub(crate) async fn roster_receipt(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
    ) -> Result<WriteReceipt, DrawersError> {
        let read = self
            .docs()
            .call(
                context.child(),
                ReadFacets {
                    capability: self.issuer,
                    doc_id: drawer.clone(),
                    at: At::Current,
                    facets: Some(BTreeSet::new()),
                    optimistic: false,
                },
            )
            .await
            .map_err(docs_error)?;
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
        self.state.put_doc(drawer, doc, access).await?;
        Ok(Some(receipt))
    }

    /// Make `doc` an entry of `drawer` at `access`, idempotently: only what the Keyhive grant and
    /// the roster do not already say is done, so a retry or a boot replay finishes an interrupted
    /// attempt. Widening grants before it publishes, narrowing publishes before it re-grants
    /// (ADR 014), so an interruption leaves the entry at or below the authority behind it. The
    /// receipt is the roster write's, or the drawer's current heads when nothing had to change.
    pub(crate) async fn enter(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
        doc: &DocId,
        access: Access,
    ) -> Result<WriteReceipt, DrawersError> {
        let (_meta, docs_group) = self.groups(drawer).await?;
        let current = self.docs_group_level(&docs_group, doc).await?;
        let docs_group = &docs_group;
        let regrant = |access: Access| async move {
            self.big_repo
                .grant_doc_access(doc.clone(), docs_group.clone(), keyhive_access(access))
                .await
                .map_err(keyhive_error)
        };
        let written = match current {
            Some(current) if current == access => {
                self.publish_entry(context, drawer, doc, access).await?
            }
            Some(current) if current > access => {
                let written = self.publish_entry(context, drawer, doc, access).await?;
                self.big_repo
                    .revoke_doc_access(doc.clone(), docs_group.clone())
                    .await
                    .map_err(keyhive_error)?;
                regrant(access).await?;
                written
            }
            _ => {
                regrant(access).await?;
                self.publish_entry(context, drawer, doc, access).await?
            }
        };
        match written {
            Some(receipt) => Ok(receipt),
            None => self.roster_receipt(context, drawer).await,
        }
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
        let receipt = self.enter(context, drawer_id, doc_id, *access).await?;
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
        let actor = core.resolve(context, params.capability).await?;
        // Conferring Admin on the doc takes Admin on the drawer; any other level takes Write.
        let needed = if params.access == Access::Admin {
            Access::Admin
        } else {
            Access::Write
        };
        core.authorize(context, &actor, &params.drawer_id, needed)
            .await?;
        // Granting the docs group on a doc gives away what the caller holds on it: the caller
        // must itself reach the doc at the level it confers, through whatever path carries it.
        if core.reach(context, actor.uuid, &params.doc_id).await? < Some(params.access) {
            return Err(DrawersError::Denied);
        }
        let digest = digest(&(
            "addDoc",
            actor.uuid,
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
            .run(&params.command, digest, actor.uuid, op, |_| {
                core.enter(context, &params.drawer_id, &params.doc_id, params.access)
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
