//! InvitePrincipal / RevokePrincipal: Keyhive membership on a drawer's groups — how a drawer is
//! shared with another node or agent, as opposed to a local actor (`GrantDrawer`).

use super::add_child::meta_access;
use super::ledger::Op;
use super::*;
use crate::doc::pipeline::digest;
use big_repo::BigKeyhiveAuthority;
use big_sync_core::PeerKey;

impl Core {
    /// The Keyhive authority a request names, or why this node cannot name it. The 32 bytes are
    /// caller input, so a key off the curve is refused here: Keyhive's group lookup asserts it.
    async fn principal(&self, principal: &Principal) -> Result<BigKeyhiveAuthority, DrawersError> {
        match principal {
            Principal::Peer { peer_id: peer } => self
                .big_repo
                .keyhive_agent_for_peer(PeerKey::new(peer.0))
                .await
                .map_err(keyhive_error)?
                .map(Into::into)
                .ok_or_else(|| DrawersError::Keyhive(format!("unknown peer {peer}"))),
            Principal::Group { group_id: group } => {
                ed25519_dalek::VerifyingKey::from_bytes(&group.0)
                    .map_err(|_| keyhive_error(format!("{group} is not a valid Ed25519 point")))?;
                self.big_repo
                    .get_group_by_id(group.0)
                    .await
                    .map(Into::into)
                    .ok_or_else(|| {
                        DrawersError::Keyhive(format!("group {group} is not in keyhive"))
                    })
            }
        }
    }
}

/// A request digest as a command-safe suffix.
fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl Core {
    /// Both memberships an invite establishes: the docs group at the level, the meta group at
    /// the level that lets the principal read the canonical roster. Idempotent in Keyhive.
    pub(crate) async fn invite_principal(
        &self,
        _context: &RequestContext,
        drawer_id: &DrawerId,
        principal: &Principal,
        access: Access,
    ) -> Result<(), DrawersError> {
        let (meta, docs) = self.groups(drawer_id).await?;
        let principal = self.principal(principal).await?;
        self.big_repo
            .add_member_to_group(principal.clone(), &docs, keyhive_access(access))
            .await
            .map_err(keyhive_error)?;
        #[cfg(test)]
        self.crash_at(super::CrashPoint::DocsMembershipAdded)?;
        self.big_repo
            .add_member_to_group(principal, &meta, keyhive_access(meta_access(access)))
            .await
            .map_err(keyhive_error)
    }

    pub(crate) async fn revoke_principal_membership(
        &self,
        _context: &RequestContext,
        drawer_id: &DrawerId,
        principal: &Principal,
    ) -> Result<(), DrawersError> {
        let (meta, docs) = self.groups(drawer_id).await?;
        let principal = self.principal(principal).await?;
        self.big_repo
            .remove_member_from_group(principal.clone(), &docs)
            .await
            .map_err(keyhive_error)?;
        self.big_repo
            .remove_member_from_group(principal, &meta)
            .await
            .map_err(keyhive_error)
    }

    /// Resume an interrupted membership change from its row.
    pub(crate) async fn resume_membership(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        tracing::warn!(op = ?row.op, "resume_membership");
        match &row.op {
            Op::Invite {
                drawer_id,
                principal,
                access,
            } => {
                self.invite_principal(context, drawer_id, principal, *access)
                    .await?;
            }
            Op::Revoke {
                drawer_id,
                principal,
            } => {
                self.revoke_principal_membership(context, drawer_id, principal)
                    .await?;
            }
            other => unreachable!("resume_membership is dispatched on a membership row: {other:?}"),
        }
        self.ledger.finish(&row.command, &()).await
    }
}

impl DrawerService {
    /// Admit `principal` to the drawer's groups at `access` (metadata at Read for Relay, as for a
    /// child entry). Takes Admin on the drawer: membership of the groups is what reaches other
    /// nodes, where this service's ledger grants mean nothing.
    async fn invite(
        &self,
        context: &RequestContext,
        params: InvitePrincipal,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        let actor = &caller.signer;
        core.authorize(context, &caller.authority, &params.drawer_id, Access::Admin)
            .await?;
        // Two Keyhive memberships, one drawer fact: under the ledger so a crash between them
        // resumes rather than leaving the principal in the docs group and out of the meta.
        let digest = digest(&(
            "invitePrincipal",
            actor.actor_id,
            &params.drawer_id,
            &params.principal,
            params.access,
        ));
        let command = CommandId::try_from(format!("invitePrincipal/{}", hex_digest(&digest)))
            .expect("a derived command id is within bounds");
        let op = Op::Invite {
            drawer_id: params.drawer_id.clone(),
            principal: params.principal.clone(),
            access: params.access,
        };
        core.ledger
            .run(&command, digest, actor.actor_id, op, |_| {
                core.invite_principal(context, &params.drawer_id, &params.principal, params.access)
            })
            .await?;
        Ok(())
    }

    async fn revoke_principal(
        &self,
        context: &RequestContext,
        params: RevokePrincipal,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        let actor = &caller.signer;
        core.authorize(context, &caller.authority, &params.drawer_id, Access::Admin)
            .await?;
        let digest = digest(&(
            "revokePrincipal",
            actor.actor_id,
            &params.drawer_id,
            &params.principal,
        ));
        let command = CommandId::try_from(format!("revokePrincipal/{}", hex_digest(&digest)))
            .expect("a derived command id is within bounds");
        let op = Op::Revoke {
            drawer_id: params.drawer_id.clone(),
            principal: params.principal.clone(),
        };
        core.ledger
            .run(&command, digest, actor.actor_id, op, |_| {
                core.revoke_principal_membership(context, &params.drawer_id, &params.principal)
            })
            .await?;
        Ok(())
    }
}

impl Handler<InvitePrincipal> for DrawerService {
    async fn call(
        &self,
        request: Envelope<InvitePrincipal>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self.invite(&request.context, request.params).await?.into())
    }
}

impl Handler<RevokePrincipal> for DrawerService {
    async fn call(
        &self,
        request: Envelope<RevokePrincipal>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self
            .revoke_principal(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    utils_rs::table_tests! {
        principal_wire_shape,
        (principal, expected),
        {
            let value = serde_json::to_value(&principal).unwrap();
            assert_eq!(value, expected);
            assert_eq!(serde_json::from_value::<Principal>(value).unwrap(), principal);
        }
    }

    principal_wire_shape! {
        peer: (
            Principal::Peer { peer_id: GroupKey([5; 32]) },
            json!({ "kind": "peer", "peerId": GroupKey([5; 32]).to_string() })
        ),
        group: (
            Principal::Group { group_id: GroupKey([6; 32]) },
            json!({ "kind": "group", "groupId": GroupKey([6; 32]).to_string() })
        ),
    }

    #[test]
    fn invite_wire_shape() {
        let request = InvitePrincipal {
            capability: CapId::default(),
            drawer_id: DocId::new([1; 32]),
            principal: Principal::Peer {
                peer_id: GroupKey([5; 32]),
            },
            access: Access::Relay,
        };
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value,
            json!({
                "capability": "00000000-0000-0000-0000-000000000000",
                "drawerId": DocId::new([1; 32]).to_string(),
                "principal": { "kind": "peer", "peerId": GroupKey([5; 32]).to_string() },
                "access": "Relay",
            })
        );
        assert_eq!(
            serde_json::from_value::<InvitePrincipal>(value).unwrap(),
            request
        );
    }

    #[test]
    fn revoke_wire_shape() {
        let request = RevokePrincipal {
            capability: CapId::default(),
            drawer_id: DocId::new([1; 32]),
            principal: Principal::Group {
                group_id: GroupKey([6; 32]),
            },
        };
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value,
            json!({
                "capability": "00000000-0000-0000-0000-000000000000",
                "drawerId": DocId::new([1; 32]).to_string(),
                "principal": { "kind": "group", "groupId": GroupKey([6; 32]).to_string() },
            })
        );
        assert_eq!(
            serde_json::from_value::<RevokePrincipal>(value).unwrap(),
            request
        );
    }

    #[test]
    fn a_principal_of_an_unknown_kind_is_refused() {
        let error = serde_json::from_value::<Principal>(json!({
            "kind": "document",
            "document": GroupKey([7; 32]).to_string(),
        }))
        .unwrap_err();
        assert!(error.to_string().contains("document"), "{error}");
        let error = serde_json::from_value::<Principal>(json!({
            "kind": "peer",
            "peerId": utils_rs::hash::encode_base58_multibase([1u8; 16]),
        }))
        .unwrap_err();
        assert!(error.to_string().contains("not a group key"), "{error}");
    }
}
