//! Current slot transport only; execution policy and source history stay off wire.
use super::*;
use crate::tasks::control::{MAX_HEADS, PROCESSOR_DOMAIN as DOMAIN, SignedCurrentControl};
use big_sync_core::PeerKey;

pub(crate) struct ProcessorTransportSnapshot {
    pub reference: ProcessorDomainReference,
    pub heads: Vec<automerge::ChangeHash>,
    pub processor_full_id: String,
    pub register_binding: RegisterBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessorRotationControl(SignedCurrentControl);

impl ProcessorRotationControl {
    pub(crate) fn statement(
        snapshot: &ProcessorTransportSnapshot,
    ) -> Result<Vec<u8>, CoordinationError> {
        let binding = &snapshot.register_binding;
        if snapshot.heads.is_empty()
            || snapshot.heads.len() > MAX_HEADS
            || binding.publication_key.heads.0.is_empty()
            || binding.publication_key.heads.0.len() > MAX_HEADS
        {
            return Err(CoordinationError::Invalid(
                "current control heads exceed budget",
            ));
        }
        // Borrow the current fields: predecessor keys and private configuration
        // are neither cloned into this declaration nor transmitted to a Relay.
        SignedCurrentControl::statement(
            DOMAIN,
            &snapshot.reference.document,
            &snapshot.reference.authority_group,
            (
                &binding.scope,
                &binding.incarnation,
                &binding.publication_key.key_ref,
            ),
            &(
                binding.generation,
                &snapshot.processor_full_id,
                &snapshot.heads,
                &binding.publication_key,
            ),
        )
    }

    pub(crate) async fn sign(
        repo: &big_repo::BigRepo,
        statement: Vec<u8>,
        peer: &PeerKey,
        authority: &big_repo::CoordinationAuthority,
    ) -> Result<Self, CoordinationError> {
        SignedCurrentControl::sign(DOMAIN, repo, statement, peer, authority)
            .await
            .map(Self)
    }

    pub(crate) async fn admit(
        &self,
        repo: &big_repo::BigRepo,
        processor: &str,
        group: [u8; 32],
        peer: Option<&PeerKey>,
    ) -> Res<Arc<ProcessorTransportSnapshot>> {
        let (document, source, (generation, identity, heads, publication_key)): (
            _,
            _,
            (u64, String, Vec<automerge::ChangeHash>, PinnedJwk),
        ) = self.0.decode(DOMAIN, group)?;
        eyre::ensure!(
            identity == processor
                && processor.rsplit_once('/').is_some()
                && !heads.is_empty()
                && heads.len() <= MAX_HEADS
                && publication_key.heads.0.len() <= MAX_HEADS,
            "control is not bounded current state for the sponsored processor"
        );
        let incarnation = register_identity(processor);
        eyre::ensure!(
            source.scope == incarnation
                && source.incarnation == incarnation
                && source.key_ref == publication_key.key_ref,
            "current control source binding mismatch"
        );
        let allowed_key_refs = BTreeSet::from([publication_key.key_ref.clone()]);
        RegisterBinding::validate_keys(
            &document,
            &incarnation,
            &allowed_key_refs,
            &publication_key,
            &Default::default(),
        )?;
        let authority = self.0.admit(repo, document.clone(), group, peer).await?;
        Ok(Arc::new(ProcessorTransportSnapshot {
            reference: ProcessorDomainReference {
                document,
                authority_group: group,
            },
            heads,
            processor_full_id: identity,
            register_binding: RegisterBinding {
                generation,
                predecessor_parts: Default::default(),
                historical_key_authorities: Default::default(),
                scope: incarnation.to_vec(),
                part: processor_part(processor, generation),
                authority,
                allowed_key_refs,
                publication_key,
                incarnation,
            },
        }))
    }
}
