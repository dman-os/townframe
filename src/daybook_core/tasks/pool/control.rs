//! Bounded current transport control, independent of retained source provenance.

use super::*;
use crate::tasks::control::{MAX_HEADS, POOL_DOMAIN as DOMAIN, SignedCurrentControl};

/// A current Edit author's exact transport declaration, not a historical writer
/// proof or retirement acknowledgement. Native Relay admission is still required.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PoolRotationControl(SignedCurrentControl);

// Borrow current fields only: encoding control must neither copy nor transmit
// the descriptor's potentially long predecessor/key-provenance history.
#[derive(Serialize)]
struct CurrentDescriptor<'a> {
    protocol: &'static str,
    rotation_generation: u64,
    pool_id: &'a str,
    authority_group: &'a [u8; 32],
    active_task_part: &'a PartKey,
    register_scope: &'a [u8],
    register_incarnation: &'a [u8; 32],
    allowed_key_refs: [&'a Url; 1],
    predecessor_parts: &'a [PartKey],
    historical_key_authorities: std::collections::BTreeMap<String, [u8; 32]>,
    publication_key: &'a PinnedJwk,
    archive_part: Option<&'a PartKey>,
    router_slot: &'a ObjKey,
    router_heartbeat_topic: &'a [u8; 32],
    allowed_rpc_transports: [&'static str; 1],
    retention_class: &'static str,
    routing_defaults: &'static str,
}

impl PoolRotationControl {
    pub(crate) fn statement(
        snapshot: &PoolDescriptorSnapshot,
    ) -> Result<Vec<u8>, CoordinationError> {
        let descriptor = &snapshot.descriptor;
        if snapshot.heads.is_empty()
            || snapshot.heads.len() > MAX_HEADS
            || descriptor.publication_key.heads.0.is_empty()
            || descriptor.publication_key.heads.0.len() > MAX_HEADS
        {
            return Err(CoordinationError::Invalid(
                "current control heads exceed budget",
            ));
        }
        descriptor
            .validate_storage(&snapshot.reference.document)
            .map_err(|_| CoordinationError::Invalid("invalid current descriptor"))?;
        if descriptor.authority_group != snapshot.reference.authority_group
            || descriptor.rotation_generation != snapshot.reference.rotation_generation
        {
            return Err(CoordinationError::Invalid(
                "current control reference mismatch",
            ));
        }
        let current = CurrentDescriptor {
            protocol: TASK_POOL_DESCRIPTOR_PROTOCOL,
            rotation_generation: descriptor.rotation_generation,
            pool_id: descriptor.pool_id.as_str(),
            authority_group: &descriptor.authority_group,
            active_task_part: &descriptor.active_task_part,
            register_scope: &descriptor.register_scope,
            register_incarnation: &descriptor.register_incarnation,
            allowed_key_refs: [&descriptor.publication_key.key_ref],
            predecessor_parts: &[],
            historical_key_authorities: Default::default(),
            publication_key: &descriptor.publication_key,
            archive_part: descriptor.archive_part.as_ref(),
            router_slot: &descriptor.router_slot,
            router_heartbeat_topic: descriptor.router_heartbeat_topic.as_bytes(),
            allowed_rpc_transports: ["irpc-iroh"],
            retention_class: match descriptor.retention_class {
                RetentionClass::UntilAuthoritativeRemoval => "until-authoritative-removal",
            },
            routing_defaults: match descriptor.routing_defaults {
                RoutingDefaults::SharedElection => "shared-election",
            },
        };
        SignedCurrentControl::statement(
            DOMAIN,
            &snapshot.reference.document,
            &snapshot.reference.authority_group,
            (
                &descriptor.register_scope,
                &descriptor.register_incarnation,
                &descriptor.publication_key.key_ref,
            ),
            &(&snapshot.heads, current),
        )
    }

    pub(crate) async fn sign(
        repo: &big_repo::BigRepo,
        statement: Vec<u8>,
        peer: &big_sync_core::PeerKey,
        authority: &big_repo::CoordinationAuthority,
    ) -> Result<Self, CoordinationError> {
        SignedCurrentControl::sign(DOMAIN, repo, statement, peer, authority)
            .await
            .map(Self)
    }

    /// Authenticate current control without decrypting its authority document.
    /// The expected pool/group are the receiver's explicit sponsorship, not hints.
    pub async fn admit(
        &self,
        repo: &big_repo::BigRepo,
        pool: &TaskPoolId,
        group: [u8; 32],
    ) -> Res<PoolDescriptorSnapshot> {
        self.admit_for_peer(repo, pool, group, None).await
    }

    pub(crate) async fn admit_for_peer(
        &self,
        repo: &big_repo::BigRepo,
        pool: &TaskPoolId,
        group: [u8; 32],
        peer: Option<&big_sync_core::PeerKey>,
    ) -> Res<PoolDescriptorSnapshot> {
        let (document, source, (heads, wire)): (
            _,
            _,
            (Vec<automerge::ChangeHash>, DescriptorWire),
        ) = self.0.decode(DOMAIN, group)?;
        let descriptor = TaskPoolDescriptor::from_wire(wire)?;
        ensure!(
            source.scope == descriptor.register_scope
                && source.incarnation == descriptor.register_incarnation
                && source.key_ref == descriptor.publication_key.key_ref,
            "current control source binding mismatch"
        );
        ensure!(
            descriptor.pool_id == *pool
                && descriptor.authority_group == group
                && !heads.is_empty()
                && heads.len() <= MAX_HEADS
                && descriptor.publication_key.heads.0.len() <= MAX_HEADS
                && descriptor.predecessor_parts.is_empty()
                && descriptor.historical_key_authorities.is_empty()
                && descriptor.allowed_key_refs.len() == 1,
            "control is not bounded current state for the sponsored pool"
        );
        descriptor.validate_storage(&document)?;
        self.0.admit(repo, document.clone(), group, peer).await?;
        Ok(PoolDescriptorSnapshot {
            reference: PoolReference {
                rotation_generation: descriptor.rotation_generation,
                document,
                authority_group: group,
            },
            heads,
            descriptor,
        })
    }
}
