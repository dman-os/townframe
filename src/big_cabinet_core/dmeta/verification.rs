use super::{
    CLAIM_DOMAIN, CLAIM_ID_DOMAIN, ClaimOperation, DmetaError, FacetClaim, FacetSignature,
    cbor_value_cid, cid_bytes, value_cid,
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedFacetClaim {
    pub actor_public_key: Vec<u8>,
    pub signature: Vec<u8>,
    pub claim: FacetClaim,
}

impl SignedFacetClaim {
    /// Assemble a claim from the actor public identity and signature returned by the
    /// secrets-only authority boundary. The core never receives or exports a private key.
    pub fn from_signature(
        claim: FacetClaim,
        actor_public_key: Vec<u8>,
        signature: Vec<u8>,
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
        let key: [u8; 32] = self
            .actor_public_key
            .as_slice()
            .try_into()
            .map_err(|_| DmetaError::InvalidPublicKey)?;
        let signature: [u8; 64] = self
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| DmetaError::InvalidSignature)?;
        let key = VerifyingKey::from_bytes(&key).map_err(|_| DmetaError::InvalidPublicKey)?;
        key.verify(
            &self.claim.signing_bytes()?,
            &Signature::from_bytes(&signature),
        )
        .map_err(|_| DmetaError::InvalidSignature)
    }

    /// Stable claim identity, independent of the containing Automerge change.
    pub fn signature_id(&self) -> Result<Vec<u8>, DmetaError> {
        let mut bytes = CLAIM_ID_DOMAIN.to_vec();
        bytes.extend(&self.actor_public_key);
        bytes.extend(self.claim.signing_bytes()?);
        bytes.extend(&self.signature);
        Ok(cid_bytes(&bytes))
    }
}

/// Verification is intentionally three-valued: absent evidence is not a forgery.
impl FacetSignature {
    /// Verify a carried branch witness without pretending that a copied merged value was signed.
    /// Missing witness material is retryable (`Pending`); malformed or cryptographically invalid
    /// material is `ProvenForgery`.
    /// Verify an inherited witness against the source context reconstructed by the
    /// repository.  The destination branch is intentionally absent from this context:
    /// a valid inherited claim names its origin document, not the branch carrying it.
    pub fn verify_inherited_for(
        &self,
        expected_claim: &FacetClaim,
        copied_value: Option<&Value>,
    ) -> VerificationState {
        let Some(message) = &self.inherited_message else {
            return VerificationState::Pending;
        };
        let Ok(claim) = FacetClaim::from_signing_bytes(message) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(expected_message) = expected_claim.signing_bytes() else {
            return VerificationState::ProvenForgery;
        };
        if self.origin_doc.as_ref() != Some(&expected_claim.origin_document)
            || claim.signing_bytes().ok().as_deref() != Some(expected_message.as_slice())
        {
            return VerificationState::ProvenForgery;
        }
        self.verify_inherited_claim(&claim, copied_value)
    }

    fn verify_inherited_claim(
        &self,
        claim: &FacetClaim,
        copied_value: Option<&Value>,
    ) -> VerificationState {
        let Some(message) = &self.inherited_message else {
            return VerificationState::Pending;
        };
        let expected_cid = match (
            &claim.operation,
            &claim.value_cid,
            &self.inherited_value,
            copied_value,
        ) {
            (ClaimOperation::Remove, None, Some(_), _) => return VerificationState::ProvenForgery,
            (ClaimOperation::Remove, None, None, _) => None,
            (ClaimOperation::Set, Some(cid), Some(value), _) => match value_cid(value) {
                Ok(actual) if actual == *cid => Some(cid.clone()),
                Ok(_) => return VerificationState::ProvenForgery,
                Err(_) => return VerificationState::ProvenForgery,
            },
            (ClaimOperation::Set, Some(cid), None, Some(value)) => match value_cid(value) {
                Ok(actual) if actual == *cid => Some(cid.clone()),
                Ok(_) => return VerificationState::ProvenForgery,
                Err(_) => return VerificationState::ProvenForgery,
            },
            (ClaimOperation::Set, Some(_), None, None) => return VerificationState::Pending,
            _ => return VerificationState::ProvenForgery,
        };
        self.verify_inherited_bytes(message, expected_cid)
    }

    fn verify_inherited_bytes(
        &self,
        message: &[u8],
        expected_cid: Option<Vec<u8>>,
    ) -> VerificationState {
        let Ok(key_bytes) = <[u8; 32]>::try_from(self.actor_public_key.as_slice()) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(signature_bytes) = <[u8; 64]>::try_from(self.signature.as_slice()) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(key) = VerifyingKey::from_bytes(&key_bytes) else {
            return VerificationState::ProvenForgery;
        };
        if !message.starts_with(CLAIM_DOMAIN)
            || key
                .verify(message, &Signature::from_bytes(&signature_bytes))
                .is_err()
        {
            return VerificationState::ProvenForgery;
        }
        match cbor_value_cid(message) {
            Some(signed_cid) if signed_cid == expected_cid => VerificationState::Accepted,
            Some(_) => VerificationState::ProvenForgery,
            None => VerificationState::ProvenForgery,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationState {
    Accepted,
    Pending,
    ProvenForgery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    Present,
    Missing,
    Invalid,
}

pub fn verification_state(evidence: Evidence) -> VerificationState {
    match evidence {
        Evidence::Present => VerificationState::Accepted,
        Evidence::Missing => VerificationState::Pending,
        Evidence::Invalid => VerificationState::ProvenForgery,
    }
}

/// Whole-document closure for v1: any proven bad evidence closes ordinary reads and writes, while
/// pending evidence remains retryable.
pub fn document_verification(
    states: impl IntoIterator<Item = VerificationState>,
) -> VerificationState {
    let mut pending = false;
    for state in states {
        match state {
            VerificationState::ProvenForgery => return VerificationState::ProvenForgery,
            VerificationState::Pending => pending = true,
            VerificationState::Accepted => {}
        }
    }
    if pending {
        VerificationState::Pending
    } else {
        VerificationState::Accepted
    }
}
