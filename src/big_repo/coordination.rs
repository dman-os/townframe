//! Checked native document/group authority and exact-transcript signing.

use crate::interlude::*;
use crate::{BigRepo, DocumentId};
use keyhive_core::{access::Access, principal::identifier::Identifier};
use keyhive_crypto::{signer::memory::MemorySigner, verifiable::Verifiable};
use std::collections::BTreeSet;

/// A locally checked authority snapshot. Generation is freshness evidence, not an epoch clock.
#[derive(Debug, Clone)]
pub struct CoordinationAuthority {
    document: DocumentId,
    group: [u8; 32],
    edit_agents: BTreeSet<[u8; 32]>,
    effective_agents: std::collections::BTreeMap<[u8; 32], Access>,
    local_access: Option<Access>,
    generation: u64,
}

impl CoordinationAuthority {
    /// The native admission reader's idle bound, only for a retained Pending
    /// authority obligation. Membership application has no complete notification
    /// guarantee; callers must not acknowledge that obligation before resampling.
    pub const PENDING_RECHECK_DELAY: std::time::Duration =
        crate::runtime2::keyhive_admission::IDLE_POLL;

    pub fn document(&self) -> &DocumentId {
        &self.document
    }
    pub fn group(&self) -> &[u8; 32] {
        &self.group
    }
    pub fn edit_agents(&self) -> &BTreeSet<[u8; 32]> {
        &self.edit_agents
    }
    /// Current document/group intersection, including Relay and Read audiences.
    /// Like the writer set, this is a sampled view, not a continuing grant.
    pub fn effective_agents(&self) -> &std::collections::BTreeMap<[u8; 32], Access> {
        &self.effective_agents
    }
    /// Sampled group/document intersection, for diagnostics only. It is not an
    /// ongoing grant: consumers must perform checked admission at their use point.
    pub fn local_access(&self) -> Option<Access> {
        self.local_access
    }
}

/// Borrowed private-key capability, available only inside a checked synchronous callback.
/// Neither the private key nor an owned signer can escape the callback.
pub struct CoordinationSigner<'a> {
    signer: &'a MemorySigner,
}

impl ed25519_dalek::Signer<ed25519_dalek::Signature> for CoordinationSigner<'_> {
    fn try_sign(
        &self,
        bytes: &[u8],
    ) -> Result<ed25519_dalek::Signature, ed25519_dalek::SignatureError> {
        ed25519_dalek::Signer::try_sign(self.signer, bytes)
    }
}

impl Verifiable for CoordinationSigner<'_> {
    fn verifying_key(&self) -> ed25519_dalek::VerifyingKey {
        self.signer.verifying_key()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoordinationError {
    #[error("coordination authority is not available yet")]
    Pending,
    #[error("current local authority does not permit this coordination operation")]
    Unauthorized,
    #[error("invalid coordination input: {0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

fn identifier(bytes: &[u8; 32]) -> Result<Identifier, CoordinationError> {
    ed25519_dalek::VerifyingKey::from_bytes(bytes)
        .map(Identifier::from)
        .map_err(|_| CoordinationError::Invalid("invalid authority identifier"))
}

impl BigRepo {
    /// Validate the real group binding and effective writers, bracketing every graph read.
    pub async fn coordination_authority(
        &self,
        document: DocumentId,
        group: [u8; 32],
    ) -> Result<CoordinationAuthority, CoordinationError> {
        let bytes = document
            .to_bytes32()
            .map_err(|_| CoordinationError::Invalid("invalid document width"))?;
        let subject = identifier(&bytes)?;
        let group_id = identifier(&group)?;
        let keyhive = self.keyhive.clone_keyhive();
        let generation = keyhive.state_generation();
        let doc = keyhive
            .get_document(subject.into())
            .await
            .ok_or(CoordinationError::Pending)?;
        if keyhive.get_group(group_id.into()).await.is_none() {
            return Err(CoordinationError::Pending);
        }
        let members = self.keyhive.agents_for_membered(subject).await;
        let group_members = self.keyhive.agents_for_membered(group_id).await;
        let _guard = doc.lock().await;
        // Membership stores increment under the mutated node's lock before rebuilt
        // members become visible. A walk racing that mutation cannot retain its generation.
        if keyhive.state_generation() != generation {
            return Err(CoordinationError::Pending);
        }
        // Group binding and membership do not depend on document payload keys.
        if !members.contains_key(&group_id) {
            return Err(CoordinationError::Unauthorized);
        }
        let local = Identifier::from(self.keyhive.coordination_signer().verifying_key());
        let local_access = members
            .get(&local)
            .zip(group_members.get(&local))
            .map(|(document_access, group_access)| (*document_access).min(*group_access));
        let effective_agents: std::collections::BTreeMap<_, _> = members
            .into_iter()
            .filter_map(|(agent, document_access)| {
                group_members
                    .get(&agent)
                    .map(|group_access| (agent.to_bytes(), document_access.min(*group_access)))
            })
            .collect();
        let edit_agents = effective_agents
            .iter()
            .filter(|(_, access)| **access >= Access::Edit)
            .map(|(agent, _)| *agent)
            .collect();
        Ok(CoordinationAuthority {
            document,
            group,
            edit_agents,
            effective_agents,
            local_access,
            generation,
        })
    }

    /// Final local admission; later SQL commit supplies durability, not a second authority check.
    pub async fn admit_coordination(
        &self,
        authority: &CoordinationAuthority,
    ) -> Result<CoordinationAuthority, CoordinationError> {
        self.admit_coordination_access(authority, Access::Edit)
            .await
    }

    /// Prove current local Read on both the named group and its authority document.
    /// The returned view proves permission at this admission point, not for later cached use.
    pub async fn admit_coordination_read(
        &self,
        authority: &CoordinationAuthority,
    ) -> Result<CoordinationAuthority, CoordinationError> {
        self.admit_coordination_access(authority, Access::Read)
            .await
    }

    /// Admit requested native access on both the configured group and document.
    pub async fn admit_coordination_access(
        &self,
        authority: &CoordinationAuthority,
        required: Access,
    ) -> Result<CoordinationAuthority, CoordinationError> {
        let current = self
            .coordination_authority(authority.document.clone(), authority.group)
            .await?;
        if current.generation != authority.generation {
            return Err(CoordinationError::Pending);
        }
        if !current
            .local_access
            .is_some_and(|access| access >= required)
        {
            return Err(CoordinationError::Unauthorized);
        }
        let keyhive = self.keyhive.clone_keyhive();
        let bytes = current
            .document
            .to_bytes32()
            .map_err(|_| CoordinationError::Invalid("invalid document width"))?;
        let doc = keyhive
            .get_document(identifier(&bytes)?.into())
            .await
            .ok_or(CoordinationError::Pending)?;
        let _guard = doc.lock().await;
        if keyhive.state_generation() != current.generation {
            return Err(CoordinationError::Pending);
        }
        Ok(current)
    }

    pub async fn with_coordination_signer<T>(
        &self,
        authority: &CoordinationAuthority,
        finish: impl FnOnce(&CoordinationSigner<'_>) -> Result<T, CoordinationError>,
    ) -> Result<T, CoordinationError> {
        let current = self.admit_coordination(authority).await?;
        let keyhive = self.keyhive.clone_keyhive();
        let bytes = current
            .document
            .to_bytes32()
            .map_err(|_| CoordinationError::Invalid("invalid document width"))?;
        let doc = keyhive
            .get_document(identifier(&bytes)?.into())
            .await
            .ok_or(CoordinationError::Pending)?;
        let _guard = doc.lock().await;
        if keyhive.state_generation() != current.generation {
            return Err(CoordinationError::Pending);
        }
        finish(&CoordinationSigner {
            signer: self.keyhive.coordination_signer(),
        })
    }
}
