//! RemoveChild: delete the child entry, then withdraw the parent's groups from the child's groups.

use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use std::collections::BTreeSet;

impl Core {
    /// Take `child` out of `parent`, idempotently: the entry is deleted before the parent's
    /// groups are withdrawn from the child's (ADR 019), and each half runs only if its state is
    /// still there, so a retry or a boot replay finishes an interrupted attempt. `None` when
    /// `child` was neither listed nor delegated to — not an entry; otherwise the receipt of the
    /// roster write, or the parent's current heads when only delegations were left to withdraw.
    pub(crate) async fn unlink(
        &self,
        context: &RequestContext,
        parent: &DrawerId,
        child: &DrawerId,
    ) -> Result<Option<WriteReceipt>, DrawersError> {
        // The level is irrelevant to a removal: only which pairs are delegated at all.
        let delegations = self.child_delegations(parent, child, Access::Relay).await?;
        let listed = self.state.child_entry(parent, child).await?.is_some();
        let mut delegated = Vec::with_capacity(delegations.len());
        for (member, group, _) in delegations {
            if self.group_level(&member, &group).await.is_some() {
                delegated.push((member, group));
            }
        }
        if !listed && delegated.is_empty() {
            return Ok(None);
        }
        let receipt = if listed {
            let receipt = self
                .write_roster(
                    context,
                    parent,
                    None,
                    BTreeMap::new(),
                    BTreeSet::from([child_entry_key(child)]),
                )
                .await?;
            self.state.remove_child(parent, child).await?;
            receipt
        } else {
            self.roster_receipt(context, parent).await?
        };
        for (member, group) in delegated {
            self.big_repo
                .remove_member_from_group(member, &group)
                .await
                .map_err(keyhive_error)?;
        }
        Ok(Some(receipt))
    }

    pub(crate) async fn resume_remove_child(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let Op::RemoveChild {
            parent_id,
            child_id,
        } = &row.op
        else {
            unreachable!(
                "resume_remove_child is dispatched on a remove-child row: {:?}",
                row.op
            )
        };
        let receipt = match self.unlink(context, parent_id, child_id).await? {
            Some(receipt) => receipt,
            None => self.roster_receipt(context, parent_id).await?,
        };
        self.ledger.finish(&row.command, &receipt).await
    }
}

impl DrawerService {
    async fn remove_child(
        &self,
        context: &RequestContext,
        params: RemoveChild,
    ) -> Result<WriteReceipt, DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        let actor = &caller.signer;
        core.authorize(context, &caller.authority, &params.parent_id, Access::Write)
            .await?;
        let digest = digest(&(
            "removeChild",
            actor.actor_id,
            &params.parent_id,
            &params.child_id,
        ));
        let op = Op::RemoveChild {
            parent_id: params.parent_id.clone(),
            child_id: params.child_id.clone(),
        };
        core.ledger
            .run(&params.command, digest, actor.actor_id, op, |_| async {
                core.unlink(context, &params.parent_id, &params.child_id)
                    .await?
                    .ok_or_else(|| DrawersError::NotAnEntry(params.child_id.clone()))
            })
            .await
    }
}

impl Handler<RemoveChild> for DrawerService {
    async fn call(
        &self,
        request: Envelope<RemoveChild>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DrawersError> {
        Ok(self
            .remove_child(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn remove_child_wire_shape() {
        let request = RemoveChild {
            capability: CapId::default(),
            command: CommandId::try_from("unlink-1".to_owned()).unwrap(),
            parent_id: DocId::new([1; 32]),
            child_id: DocId::new([2; 32]),
        };
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value,
            json!({
                "capability": "00000000-0000-0000-0000-000000000000",
                "command": "unlink-1",
                "parentId": DocId::new([1; 32]).to_string(),
                "childId": DocId::new([2; 32]).to_string(),
            })
        );
        assert_eq!(
            serde_json::from_value::<RemoveChild>(value).unwrap(),
            request
        );
    }

    #[test]
    fn a_removal_without_a_command_is_refused() {
        let error = serde_json::from_value::<RemoveChild>(json!({
            "capability": "00000000-0000-0000-0000-000000000000",
            "parentId": DocId::new([1; 32]).to_string(),
            "childId": DocId::new([2; 32]).to_string(),
        }))
        .unwrap_err();
        assert!(error.to_string().contains("command"), "{error}");
    }
}
