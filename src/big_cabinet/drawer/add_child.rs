//! AddChild: delegate the parent's groups onto the child's groups, then publish the child entry.

use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use big_cabinet_core::drawer::DrawerChildEntry;
use std::collections::BTreeSet;

/// The level a principal admitted to a drawer at `access` gets on its metadata group: the same
/// level, except that `Relay` takes `Read` there — enumerating the canonical roster needs it,
/// and metadata is not the ciphertext Relay exists to withhold (ADR 019 "Child entries").
pub(super) fn meta_access(access: Access) -> Access {
    access.max(Access::Read)
}

/// Whether `access` reaches the sensitive metadata root's audience at all: Read and up do,
/// Relay does not — the sensitive group's membership is withheld from a Relay principal so
/// plaintext inventories named there reach only Read+. `None` means "must hold nothing".
pub(super) fn sensitive_access(access: Access) -> Option<Access> {
    (access >= Access::Read).then_some(access)
}

/// One Keyhive delegation a child entry stands on. A single-group child takes the parent's
/// groups onto its own groups; a self-doc child has no groups, so it takes the parent's docs
/// group onto its doc itself at exactly `access` — the same grant shape as a `docEntry`, with
/// no meta-group delegation and no `meta_access` widening (ADR 026 §6). `access: None` is a
/// delegation the entry wants the principal to hold *nothing* of — the sensitive group under
/// a Relay link — so phase one never raises it and phase three revokes a leftover hold.
pub(super) enum ChildDelegation {
    Groups {
        member: BigKeyhiveGroup,
        group: BigKeyhiveGroup,
        access: Option<Access>,
    },
    Doc {
        docs_group: BigKeyhiveGroup,
        doc: DocId,
        access: Option<Access>,
    },
}

impl ChildDelegation {
    /// The level this delegation wants on its group, `None` when it wants the principal to hold
    /// nothing there.
    pub(super) fn access(&self) -> Option<Access> {
        match self {
            Self::Groups { access, .. } | Self::Doc { access, .. } => *access,
        }
    }

    pub(super) async fn current(&self, core: &Core) -> Result<Option<Access>, DrawersError> {
        match self {
            Self::Groups { member, group, .. } => {
                Ok(core
                    .group_level(member, group)
                    .await
                    .map(|level| match level {
                        KeyhiveAccess::Relay => Access::Relay,
                        KeyhiveAccess::Read => Access::Read,
                        KeyhiveAccess::Edit => Access::Write,
                        KeyhiveAccess::Admin => Access::Admin,
                    }))
            }
            Self::Doc {
                docs_group, doc, ..
            } => core.docs_group_level(docs_group, doc).await,
        }
    }

    pub(super) async fn grant(&self, core: &Core) -> Result<(), DrawersError> {
        let access = self
            .access()
            .expect("grant is only called for a delegation that wants a level");
        match self {
            Self::Groups { member, group, .. } => core
                .big_repo
                .add_member_to_group(member.clone(), group, keyhive_access(access))
                .await
                .map_err(keyhive_error),
            Self::Doc {
                docs_group, doc, ..
            } => core
                .big_repo
                .grant_doc_access(doc.clone(), docs_group.clone(), keyhive_access(access))
                .await
                .map_err(keyhive_error),
        }
    }

    pub(super) async fn revoke(&self, core: &Core) -> Result<(), DrawersError> {
        match self {
            Self::Groups { member, group, .. } => core
                .big_repo
                .remove_member_from_group(member.clone(), group)
                .await
                .map_err(keyhive_error),
            Self::Doc {
                docs_group, doc, ..
            } => core
                .big_repo
                .revoke_doc_access(doc.clone(), docs_group.clone())
                .await
                .map_err(keyhive_error),
        }
    }
}

impl Core {
    /// The level `member` holds on `group` through its own delegation, `None` when it has none.
    /// Access reached through other groups does not count: with drawer 2 holding 1 and 1
    /// holding 0, 2 reaches 0's groups through 1, but there is no 2→0 delegation for a link to
    /// skip or an unlink to revoke.
    pub(super) async fn group_level(
        &self,
        member: &BigKeyhiveGroup,
        group: &BigKeyhiveGroup,
    ) -> Option<KeyhiveAccess> {
        self.big_repo
            .keyhive()
            .direct_access_on(&member.id().into(), group.id().into())
            .await
    }

    /// The delegations a child entry at `access` stands on. A self-doc *parent* is refused
    /// here through `groups` — it has no docs group to delegate — while a self-doc *child*
    /// under a single-group parent takes only the doc delegation (ADR 026 §6). A grouped
    /// child also takes the parent's sensitive group onto the child's sensitive group at the
    /// level Read-and-up confers; a Relay entry leaves it wanting nothing (`sensitive_access`).
    pub(super) async fn child_delegations(
        &self,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<Vec<ChildDelegation>, DrawersError> {
        let (parent_meta, parent_docs, parent_sensitive) = self.groups(parent).await?;
        let child_record = self
            .ensure_drawer(&RequestContext::root(), child)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(child.clone()))?;
        match child_record.layout_kind() {
            Some(LayoutKind::SelfDoc) => Ok(vec![ChildDelegation::Doc {
                docs_group: parent_docs,
                doc: child.clone(),
                access: Some(access),
            }]),
            Some(LayoutKind::SingleGroup) => {
                let (child_meta, child_docs, child_sensitive) = self.groups(child).await?;
                Ok(vec![
                    ChildDelegation::Groups {
                        member: parent_docs,
                        group: child_docs,
                        access: Some(access),
                    },
                    ChildDelegation::Groups {
                        member: parent_meta,
                        group: child_meta,
                        access: Some(meta_access(access)),
                    },
                    // `None` under a Relay link: the sensitive group's membership is withheld
                    // from a Relay principal, so the entry wants the child's sensitive group
                    // clear of the parent's; a leftover hold from a wider link is revoked in
                    // phase three.
                    ChildDelegation::Groups {
                        member: parent_sensitive,
                        group: child_sensitive,
                        access: sensitive_access(access),
                    },
                ])
            }
            None => Err(DrawersError::ForeignLayout(
                child_record
                    .descriptor
                    .layout
                    .tag()
                    .unwrap_or("unknown")
                    .to_owned(),
            )),
        }
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
    /// finishes an interrupted attempt. Phase one widens only delegations the entry wants
    /// (`Some(level)`) and never raises a hold above `level`; phase three publishes; phase four
    /// narrows every delegation that is now too strong, including one the entry wants gone
    /// entirely (`None` ⇒ revoke and do not re-grant). Keyhive revokes a member whole, so
    /// narrowing is a removal and a re-add at the wanted level. The receipt is the roster
    /// write's, or the parent's current heads when nothing had to change.
    pub(crate) async fn link(
        &self,
        context: &RequestContext,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<WriteReceipt, DrawersError> {
        let delegations = self.child_delegations(parent, child, access).await?;
        for delegation in &delegations {
            if let Some(wanted) = delegation.access() {
                let current = delegation.current(self).await?;
                if current.is_none_or(|current| current < wanted) {
                    if current.is_some() {
                        delegation.revoke(self).await?;
                    }
                    delegation.grant(self).await?;
                }
            }
        }
        let written = self.publish_child(context, parent, child, access).await?;
        for delegation in &delegations {
            let current = delegation.current(self).await?;
            let too_strong = match (delegation.access(), current) {
                (Some(wanted), Some(current)) => current > wanted,
                (None, Some(_)) => true,
                (_, None) => false,
            };
            if too_strong {
                delegation.revoke(self).await?;
                if delegation.access().is_some() {
                    delegation.grant(self).await?;
                }
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
        let caller = core.invocation(context, params.capability).await?;
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
                capability: CapId::default(),
                command: CommandId::try_from("link-1".to_owned()).unwrap(),
                parent_id: DocId::new([1; 32]),
                child_id: DocId::new([2; 32]),
                access,
            };
            let value = serde_json::to_value(&request).unwrap();
            assert_eq!(
                value,
                json!({
                    "capability": "00000000-0000-0000-0000-000000000000",
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
            "capability": "00000000-0000-0000-0000-000000000000",
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

    utils_rs::table_tests! {
        sensitive_metadata_reaches_only_read_and_up,
        (access, sensitive),
        {
            assert_eq!(sensitive_access(access), sensitive);
        }
    }

    sensitive_metadata_reaches_only_read_and_up! {
        relay: (Access::Relay, None),
        read: (Access::Read, Some(Access::Read)),
        write: (Access::Write, Some(Access::Write)),
        admin: (Access::Admin, Some(Access::Admin)),
    }
}
