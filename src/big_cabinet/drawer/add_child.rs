//! AddChild: delegate the parent's groups onto the child's groups, then publish the child entry.

use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use big_cabinet_core::drawer::DrawerChildEntry;
use std::collections::BTreeSet;

/// The level a principal admitted to a drawer at `access` gets on its metadata group: the same
/// level, except that `Relay` takes `Read` there — enumerating the canonical roster needs it,
/// and metadata is not the ciphertext Relay exists to withhold (ADR 014 "Child entries").
pub(super) fn meta_access(access: Access) -> Access {
    access.max(Access::Read)
}

impl Core {
    /// The level `member` holds on `group` in Keyhive, `None` when it holds nothing.
    pub(super) async fn group_level(
        &self,
        member: &BigKeyhiveGroup,
        group: &BigKeyhiveGroup,
    ) -> Option<KeyhiveAccess> {
        self.big_repo
            .keyhive()
            .agent_access_on(&member.id().into(), group.id().into())
            .await
    }

    /// The two delegations a child entry at `access` stands on — `(member, group, level)`: the
    /// parent's docs group onto the child's docs group, and the parent's meta group onto the
    /// child's meta group.
    pub(super) async fn child_delegations(
        &self,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<[(BigKeyhiveGroup, BigKeyhiveGroup, KeyhiveAccess); 2], DrawersError> {
        let (parent_meta, parent_docs) = self.groups(parent).await?;
        let (child_meta, child_docs) = self.groups(child).await?;
        Ok([
            (parent_docs, child_docs, keyhive_access(access)),
            (parent_meta, child_meta, keyhive_access(meta_access(access))),
        ])
    }

    /// Publish the entry `child` at `access` on `parent` unless the roster already lists it so.
    async fn publish_child(
        &self,
        context: &RequestContext,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<Option<WriteReceipt>, DrawersError> {
        if self.state.child_entry(parent, child).await? == Some(access) {
            return Ok(None);
        }
        let entry = DrawerChildEntry::new(access);
        let receipt = self
            .write_roster(
                context,
                parent,
                None,
                BTreeMap::from([(
                    child_entry_key(child),
                    serde_json::to_value(entry).expect("a child entry serializes"),
                )]),
                BTreeSet::new(),
            )
            .await?;
        self.state.put_child(parent, child, access).await?;
        Ok(Some(receipt))
    }

    /// Make `child` an entry of `parent` at `access`, idempotently: only what the Keyhive
    /// delegations and the roster do not already say is done, so a retry or a boot replay
    /// finishes an interrupted attempt. Widening delegates before it publishes, narrowing
    /// publishes before it re-delegates (ADR 014), so an interruption leaves the entry at or
    /// below the authority behind it. Keyhive revokes a member whole, so narrowing is a removal
    /// and a re-add at the wanted level. The receipt is the roster write's, or the parent's
    /// current heads when nothing had to change.
    pub(crate) async fn link(
        &self,
        context: &RequestContext,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<WriteReceipt, DrawersError> {
        let delegations = self.child_delegations(parent, child, access).await?;
        for (member, group, wanted) in &delegations {
            if self
                .group_level(member, group)
                .await
                .is_none_or(|current| current < *wanted)
            {
                self.big_repo
                    .add_member_to_group(member.clone(), group, *wanted)
                    .await
                    .map_err(keyhive_error)?;
            }
        }
        let written = self.publish_child(context, parent, child, access).await?;
        for (member, group, wanted) in &delegations {
            if self
                .group_level(member, group)
                .await
                .is_some_and(|current| current > *wanted)
            {
                self.big_repo
                    .remove_member_from_group(member.clone(), group)
                    .await
                    .map_err(keyhive_error)?;
                self.big_repo
                    .add_member_to_group(member.clone(), group, *wanted)
                    .await
                    .map_err(keyhive_error)?;
            }
        }
        match written {
            Some(receipt) => Ok(receipt),
            None => self.roster_receipt(context, parent).await,
        }
    }

    pub(crate) async fn resume_add_child(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let Op::AddChild {
            parent_id,
            child_id,
            access,
        } = &row.op
        else {
            unreachable!(
                "resume_add_child is dispatched on an add-child row: {:?}",
                row.op
            )
        };
        let receipt = self.link(context, parent_id, child_id, *access).await?;
        self.ledger.finish(&row.command, &receipt).await
    }
}

impl DrawerService {
    async fn add_child(
        &self,
        context: &RequestContext,
        params: AddChild,
    ) -> Result<WriteReceipt, DrawersError> {
        let core = &self.core;
        if params.parent_id == params.child_id {
            return Err(DrawersError::SelfChild);
        }
        let caller = core.invocation(context, params.authorization).await?;
        let actor = &caller.signer;
        core.authorize(context, &caller.authority, &params.parent_id, Access::Write)
            .await?;
        // The entry confers `access` on the child through the parent, so the actor must hold it
        // on the child itself; a Relay entry still takes Read, which is what enumerating the
        // child's roster needs.
        core.authorize(
            context,
            &caller.authority,
            &params.child_id,
            params.access.max(Access::Read),
        )
        .await?;
        let digest = digest(&(
            "addChild",
            actor.actor_id,
            &params.parent_id,
            &params.child_id,
            params.access,
        ));
        let op = Op::AddChild {
            parent_id: params.parent_id.clone(),
            child_id: params.child_id.clone(),
            access: params.access,
        };
        core.ledger
            .run(&params.command, digest, actor.actor_id, op, |_| {
                core.link(context, &params.parent_id, &params.child_id, params.access)
            })
            .await
    }
}

impl Handler<AddChild> for DrawerService {
    async fn call(
        &self,
        request: Envelope<AddChild>,
    ) -> Result<ResponseEnvelope<WriteReceipt>, DrawersError> {
        Ok(self
            .add_child(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    utils_rs::table_tests! {
        add_child_wire_shape,
        (access, expected),
        {
            let request = AddChild {
                authorization: Authorization::identity(CapId::default()),
                command: CommandId::try_from("link-1".to_owned()).unwrap(),
                parent_id: DocId::new([1; 32]),
                child_id: DocId::new([2; 32]),
                access,
            };
            let value = serde_json::to_value(&request).unwrap();
            assert_eq!(
                value,
                json!({
                    "authorization": {
                        "authority": "00000000-0000-0000-0000-000000000000",
                        "signer": "00000000-0000-0000-0000-000000000000",
                    },
                    "command": "link-1",
                    "parentId": DocId::new([1; 32]).to_string(),
                    "childId": DocId::new([2; 32]).to_string(),
                    "access": expected,
                })
            );
            assert_eq!(serde_json::from_value::<AddChild>(value).unwrap(), request);
        }
    }

    add_child_wire_shape! {
        relay: (Access::Relay, "Relay"),
        read: (Access::Read, "Read"),
        write: (Access::Write, "Write"),
        admin: (Access::Admin, "Admin"),
    }

    #[test]
    fn an_unknown_access_level_is_refused() {
        let error = serde_json::from_value::<AddChild>(json!({
            "authorization": {
                "authority": "00000000-0000-0000-0000-000000000000",
                "signer": "00000000-0000-0000-0000-000000000000",
            },
            "command": "link-1",
            "parentId": DocId::new([1; 32]).to_string(),
            "childId": DocId::new([2; 32]).to_string(),
            "access": "Pull",
        }))
        .unwrap_err();
        assert!(error.to_string().contains("Pull"), "{error}");
    }

    utils_rs::table_tests! {
        metadata_takes_read_for_relay,
        (access, meta),
        {
            assert_eq!(meta_access(access), meta);
        }
    }

    metadata_takes_read_for_relay! {
        relay: (Access::Relay, Access::Read),
        read: (Access::Read, Access::Read),
        write: (Access::Write, Access::Write),
        admin: (Access::Admin, Access::Admin),
    }
}
