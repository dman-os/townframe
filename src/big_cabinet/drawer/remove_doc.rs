//! RemoveDoc: delete a doc's roster entry, then revoke the docs group on it.

use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveDoc {
    pub capability: CapId,
    pub command: CommandId,
    pub drawer_id: DrawerId,
    pub doc_id: DocId,
}

impl Operation for RemoveDoc {
    type Output = WriteReceipt;
    type Error = DrawersError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.drawers.removeDoc")
        .with_schemas("WriteReceipt", "DrawersError");
}

impl Core {
    /// Take `doc` out of `drawer`, idempotently: the entry is deleted before the docs group is
    /// revoked on the doc (ADR 014), and each half runs only if its state is still there, so a
    /// retry or a boot replay finishes an interrupted attempt. `None` when `doc` was neither
    /// listed nor granted — not an entry; otherwise the receipt of the roster write, or the
    /// drawer's current heads when only the grant was left to revoke.
    pub(crate) async fn leave(
        &self,
        context: &RequestContext,
        drawer: &DrawerId,
        doc: &DocId,
    ) -> Result<Option<WriteReceipt>, DrawersError> {
        let (_meta, docs_group) = self.groups(drawer).await?;
        let listed = self.state.doc_entry(drawer, doc).await?.is_some();
        let granted = self.docs_group_level(&docs_group, doc).await?.is_some();
        if !listed && !granted {
            return Ok(None);
        }
        let receipt = if listed {
            let receipt = self
                .write_roster(
                    context,
                    drawer,
                    None,
                    BTreeMap::new(),
                    BTreeSet::from([doc_entry_key(doc)]),
                )
                .await?;
            self.state.remove_doc(drawer, doc).await?;
            receipt
        } else {
            self.roster_receipt(context, drawer).await?
        };
        if granted {
            self.big_repo
                .revoke_doc_access(doc.clone(), docs_group)
                .await
                .map_err(keyhive_error)?;
        }
        Ok(Some(receipt))
    }

    pub(crate) async fn resume_remove_doc(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let Op::RemoveDoc { drawer_id, doc_id } = &row.op else {
            unreachable!(
                "resume_remove_doc is dispatched on a remove-doc row: {:?}",
                row.op
            )
        };
        let receipt = match self.leave(context, drawer_id, doc_id).await? {
            Some(receipt) => receipt,
            None => self.roster_receipt(context, drawer_id).await?,
        };
        self.ledger.finish(&row.command, &receipt).await
    }
}

impl DrawerService {
    async fn remove_doc(
        &self,
        context: &RequestContext,
        params: RemoveDoc,
    ) -> Result<WriteReceipt, DrawersError> {
        let core = &self.core;
        let actor = core.resolve(context, params.capability).await?;
        core.authorize(context, &actor, &params.drawer_id, Access::Write)
            .await?;
        let digest = digest(&("removeDoc", actor.uuid, &params.drawer_id, &params.doc_id));
        let op = Op::RemoveDoc {
            drawer_id: params.drawer_id.clone(),
            doc_id: params.doc_id.clone(),
        };
        core.ledger
            .run(&params.command, digest, actor.uuid, op, |_| async {
                core.leave(context, &params.drawer_id, &params.doc_id)
                    .await?
                    .ok_or_else(|| DrawersError::NotAnEntry(params.doc_id.clone()))
            })
            .await
    }
}

impl Handler<RemoveDoc> for DrawerService {
    async fn call(
        &self,
        request: Envelope<RemoveDoc>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DrawersError> {
        Ok(self
            .remove_doc(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn remove_doc_wire_shape() {
        let request = RemoveDoc {
            capability: CapId::default(),
            command: CommandId::try_from("remove-1".to_owned()).unwrap(),
            drawer_id: DocId::new([1; 32]),
            doc_id: DocId::new([2; 32]),
        };
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value,
            json!({
                "capability": "00000000-0000-0000-0000-000000000000",
                "command": "remove-1",
                "drawerId": DocId::new([1; 32]).to_string(),
                "docId": DocId::new([2; 32]).to_string(),
            })
        );
        assert_eq!(serde_json::from_value::<RemoveDoc>(value).unwrap(), request);
    }

    #[test]
    fn a_remove_without_a_command_is_refused() {
        let error = serde_json::from_value::<RemoveDoc>(json!({
            "capability": "00000000-0000-0000-0000-000000000000",
            "drawerId": DocId::new([1; 32]).to_string(),
            "docId": DocId::new([2; 32]).to_string(),
        }))
        .unwrap_err();
        assert!(error.to_string().contains("command"), "{error}");
    }
}
