use super::ActorMetadata;
use super::Attribution;
use super::AttributionError;
use super::Bytes;
use super::CLAIM_ID_DOMAIN;
use super::Cid;
use super::ClaimOperation;
use super::Dmeta;
use super::DmetaError;
use super::FacetClaim;
use super::FacetSignature;
use super::MultibaseKey;
use super::NativeValue;
use super::SignatureBytes;
use super::Uuid7;
use super::cid_of;
use super::encode_native;
use crate::FacetKey;
use ed25519_dalek::Signature;
use ed25519_dalek::VerifyingKey;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedFacetClaim {
    pub actor_public_key: MultibaseKey,
    pub signature: SignatureBytes,
    pub claim: FacetClaim,
}

impl SignedFacetClaim {
    pub fn from_signature(
        claim: FacetClaim,
        actor_public_key: MultibaseKey,
        signature: SignatureBytes,
    ) -> Result<Self, DmetaError> {
        let signed = Self {
            actor_public_key,
            signature,
            claim,
        };
        signed.verify()?;
        Ok(signed)
    }

    pub fn verify(&self) -> Result<(), DmetaError> {
        let key = VerifyingKey::from_bytes(self.actor_public_key.as_bytes())
            .map_err(|_| DmetaError::InvalidPublicKey)?;
        key.verify_strict(
            &self.claim.signing_bytes()?,
            &Signature::from_bytes(self.signature.as_bytes()),
        )
        .map_err(|_| DmetaError::InvalidSignature)
    }

    pub fn signature_id(&self) -> Result<Cid, DmetaError> {
        Ok(signature_id(&self.actor_public_key, &self.signature))
    }
}

pub fn signature_id(actor_public_key: &MultibaseKey, signature: &SignatureBytes) -> Cid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CLAIM_ID_DOMAIN);
    hasher.update(actor_public_key.as_bytes());
    hasher.update(signature.as_bytes());
    Cid::from_digest(*hasher.finalize().as_bytes())
}

impl SignedFacetClaim {
    pub fn value_evidence(&self, current: FacetValue<'_>) -> Evidence {
        match (&self.claim.operation, &self.claim.value_cid, current) {
            (_, _, FacetValue::Missing) => Evidence::Missing,
            (ClaimOperation::Remove, None, FacetValue::Removed) => Evidence::Present,
            (ClaimOperation::Set, Some(cid), FacetValue::Present(value))
                if cid_of(&encode_native(value)) == *cid =>
            {
                Evidence::Present
            }
            _ => Evidence::Invalid,
        }
    }
}

impl Dmeta {
    pub fn verify_facet(
        &self,
        key: &FacetKey,
        current: FacetValue<'_>,
        reconstructed: &BTreeMap<SignatureBytes, FacetClaim>,
        issuers: &BTreeMap<Uuid7, MultibaseKey>,
    ) -> VerificationState {
        let Some(metadata) = self.facets.get(key) else {
            return VerificationState::Uncovered;
        };
        facet_verification(metadata.signatures.iter().map(|entry| {
            let Some(actor) = self.actors.get(&entry.actor_uuid) else {
                return VerificationState::Rejected;
            };
            let Some(certificate) = actor.delegation.as_ref() else {
                return VerificationState::Rejected;
            };
            let signed = match entry.authenticate(actor, reconstructed.get(&entry.signature)) {
                Ok(signed) => signed,
                Err(DmetaError::MissingMessage) => return VerificationState::Pending,
                Err(_) => return VerificationState::Rejected,
            };
            // A compaction witness names its origin itself; every other entry names it through the
            // binding of its `docUuid`.
            if signed.claim.facet_key != *key
                || self.facet_uuids.get(&signed.claim.chosen_uuid) != Some(key)
                || (entry.inherited.is_none()
                    && self.branch_uuids.get(&entry.doc_uuid)
                        != Some(&signed.claim.origin_document))
            {
                return VerificationState::Rejected;
            }
            let attribution = match issuers.get(&entry.actor_uuid) {
                Some(issuer) => {
                    match Attribution::verify(certificate, issuer, entry.actor_uuid, actor) {
                        Ok(_) => Evidence::Present,
                        Err(AttributionError::Unsupported(_) | AttributionError::ResourceLimit) => {
                            Evidence::Missing
                        }
                        Err(_) => Evidence::Invalid,
                    }
                }
                None => Evidence::Missing,
            };
            fold_checks(SignatureChecks {
                message: Evidence::Present,
                attribution,
                value: signed.value_evidence(current),
            })
        }))
    }
}

impl FacetSignature {
    pub fn authenticate(
        &self,
        actor: &ActorMetadata,
        reconstructed: Option<&FacetClaim>,
    ) -> Result<SignedFacetClaim, DmetaError> {
        if self.message.is_some() && self.inherited.is_some() {
            return Err(DmetaError::WrongContext);
        }
        let carried = self
            .message
            .as_ref()
            .or_else(|| self.inherited.as_ref().map(|witness| &witness.message));
        let claim = match (carried, reconstructed) {
            (Some(bytes), _) => FacetClaim::from_signing_bytes(bytes.as_slice())?,
            (None, Some(claim)) => claim.clone(),
            (None, None) => return Err(DmetaError::MissingMessage),
        };
        if claim.actor_uuid != self.actor_uuid {
            return Err(DmetaError::WrongContext);
        }
        if let Some(expected) = reconstructed
            && expected.signing_bytes()? != claim.signing_bytes()?
        {
            return Err(DmetaError::WrongContext);
        }
        if let Some(witness) = &self.inherited {
            if witness.origin_doc != claim.origin_document {
                return Err(DmetaError::WrongContext);
            }
            if let Some(value) = &witness.value
                && (claim.operation != ClaimOperation::Set
                    || claim.value_cid.as_ref() != Some(&cid_of(value.as_slice())))
            {
                return Err(DmetaError::WrongContext);
            }
        }
        SignedFacetClaim::from_signature(claim, actor.pubkey.clone(), self.signature.clone())
    }

    pub fn carry_message(&mut self, claim: &FacetClaim) -> Result<(), DmetaError> {
        if self.actor_uuid != claim.actor_uuid || self.inherited.is_some() {
            return Err(DmetaError::WrongContext);
        }
        self.message = Some(Bytes::from(claim.signing_bytes()?));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FacetValue<'a> {
    Missing,
    Removed,
    Present(&'a NativeValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationState {
    Accepted,
    Pending,
    Rejected,
    Uncovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    Present,
    Missing,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignatureChecks {
    pub message: Evidence,
    pub attribution: Evidence,
    pub value: Evidence,
}

pub fn fold_checks(checks: SignatureChecks) -> VerificationState {
    if checks.message == Evidence::Invalid || checks.attribution == Evidence::Invalid {
        return VerificationState::Rejected;
    }
    if [checks.message, checks.attribution, checks.value].contains(&Evidence::Missing) {
        return VerificationState::Pending;
    }
    if checks.value == Evidence::Invalid {
        VerificationState::Uncovered
    } else {
        VerificationState::Accepted
    }
}

pub fn facet_verification(
    states: impl IntoIterator<Item = VerificationState>,
) -> VerificationState {
    let mut pending = false;
    let mut rejected = false;
    for state in states {
        match state {
            VerificationState::Accepted => return VerificationState::Accepted,
            VerificationState::Pending => pending = true,
            VerificationState::Rejected => rejected = true,
            VerificationState::Uncovered => {}
        }
    }
    if pending {
        VerificationState::Pending
    } else if rejected {
        VerificationState::Rejected
    } else {
        VerificationState::Uncovered
    }
}
