use super::{
    Bytes, CLAIM_DOMAIN, CLAIM_ID_DOMAIN, Cid, ClaimOperation, DmetaError, FacetClaim,
    FacetSignature, InheritedWitness, MultibaseKey, MultibaseSignature, cid_of,
};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedFacetClaim {
    pub actor_public_key: MultibaseKey,
    pub signature: MultibaseSignature,
    pub claim: FacetClaim,
}

impl SignedFacetClaim {
    /// Assemble a claim from the actor public identity and signature returned by the secrets-only
    /// authority boundary. The core never receives or exports a private key.
    pub fn from_signature(
        claim: FacetClaim,
        actor_public_key: MultibaseKey,
        signature: MultibaseSignature,
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
        key.verify(
            &self.claim.signing_bytes()?,
            &Signature::from_bytes(self.signature.as_bytes()),
        )
        .map_err(|_| DmetaError::InvalidSignature)
    }

    /// Stable claim identity, independent of the containing Automerge change.
    pub fn signature_id(&self) -> Result<Cid, DmetaError> {
        let mut bytes = CLAIM_ID_DOMAIN.to_vec();
        bytes.extend(self.actor_public_key.as_bytes());
        bytes.extend(self.claim.signing_bytes()?);
        bytes.extend(self.signature.as_bytes());
        Ok(cid_of(&bytes))
    }
}

/// Verification is intentionally three-valued: absent evidence is not a forgery.
impl FacetSignature {
    /// Verify a signature whose message was reconstructed by the repository from the change that
    /// introduced it.
    ///
    /// `value` is the canonical native bytes of the facet value at that change, or `None` when the
    /// repository could not recover them. A set claim whose value cannot be recovered is pending,
    /// not forged: missing evidence is retried, it does not close the document.
    pub fn verify_reconstructed(
        &self,
        expected: &FacetClaim,
        value: Option<&[u8]>,
    ) -> VerificationState {
        let Ok(expected_message) = expected.signing_bytes() else {
            return VerificationState::ProvenForgery;
        };
        if !self.authenticates(&expected_message) {
            return VerificationState::ProvenForgery;
        }
        self.value_agrees(expected, value, None)
    }

    /// Verify a signature a branch carried over from its origin.
    ///
    /// The destination branch is absent from this check on purpose: a valid inherited claim names
    /// its origin document, not the branch carrying it. `copied_value` is the canonical native bytes
    /// of the value the branch actually holds.
    pub fn verify_inherited(
        &self,
        expected: &FacetClaim,
        copied_value: Option<&[u8]>,
    ) -> VerificationState {
        let Some(InheritedWitness {
            origin_doc,
            message,
            value,
        }) = &self.inherited
        else {
            return VerificationState::Pending;
        };
        if message.as_slice().is_empty() {
            return VerificationState::Pending;
        }
        let Ok(carried) = FacetClaim::from_signing_bytes(message.as_slice()) else {
            return VerificationState::ProvenForgery;
        };
        let Ok(expected_message) = expected.signing_bytes() else {
            return VerificationState::ProvenForgery;
        };
        if Some(origin_doc) != Some(&expected.origin_document)
            || carried.signing_bytes().ok().as_deref() != Some(expected_message.as_slice())
        {
            return VerificationState::ProvenForgery;
        }
        if !self.authenticates(message.as_slice()) {
            return VerificationState::ProvenForgery;
        }
        self.value_agrees(&carried, copied_value, value.as_ref())
    }

    fn authenticates(&self, message: &[u8]) -> bool {
        if !message.starts_with(CLAIM_DOMAIN) {
            return false;
        }
        let Ok(key) = VerifyingKey::from_bytes(self.actor_public_key.as_bytes()) else {
            return false;
        };
        key.verify(message, &Signature::from_bytes(self.signature.as_bytes()))
            .is_ok()
    }

    /// Whether the value the claim covers is the value in hand.
    ///
    /// A witness value, when one was retained, is what the signature covers; the copied value is
    /// only a fallback for the case where the branch copied exactly the signed bytes. A removal
    /// covers no value, so any retained value is a contradiction.
    fn value_agrees(
        &self,
        claim: &FacetClaim,
        copied_value: Option<&[u8]>,
        witness_value: Option<&Bytes>,
    ) -> VerificationState {
        let expected_cid = match (
            &claim.operation,
            &claim.value_cid,
            witness_value,
            copied_value,
        ) {
            (ClaimOperation::Remove, None, Some(_), _) => return VerificationState::ProvenForgery,
            (ClaimOperation::Remove, None, None, _) => None,
            (ClaimOperation::Set, Some(cid), Some(witness), _) => {
                if cid_of(witness.as_slice()) == *cid {
                    Some(cid.clone())
                } else {
                    return VerificationState::ProvenForgery;
                }
            }
            (ClaimOperation::Set, Some(cid), None, Some(copied)) => {
                if cid_of(copied) == *cid {
                    Some(cid.clone())
                } else {
                    return VerificationState::ProvenForgery;
                }
            }
            (ClaimOperation::Set, Some(_), None, None) => return VerificationState::Pending,
            _ => return VerificationState::ProvenForgery,
        };
        // The claim's own CID is what the signature covers: the message check above already
        // authenticated these bytes, so there is nothing to re-decode.
        if claim.value_cid == expected_cid {
            VerificationState::Accepted
        } else {
            VerificationState::ProvenForgery
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

/// One of the four independent checks in ADR 015 §6.
///
/// `message` is the actor signature over the reconstructed or carried claim. `delegation` is the
/// node-signed attribution of that actor key at the introduction frontier, not in today's dmeta.
/// `value` is the value CID against the copied value or the witness's retained value. `authority`
/// is the node signer of the containing change against document write authority. A repository
/// supplies the evidence it actually has; this type does not invent a missing check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignatureChecks {
    pub message: Evidence,
    pub delegation: Evidence,
    pub value: Evidence,
    pub authority: Evidence,
}

/// Fold the four checks and the predecessor verdicts into the one verdict a read caches.
///
/// Any failed check is a forgery. Any missing check is pending. A signature is accepted only when
/// every check passes and every predecessor is itself accepted. A pending or forged predecessor is
/// not accepted evidence, so a successor that names one cannot be accepted: descendants of rejected
/// history cannot launder its content into the accepted view.
pub fn fold_checks(
    checks: SignatureChecks,
    predecessors: impl IntoIterator<Item = VerificationState>,
) -> VerificationState {
    let mut pending = false;
    for evidence in [
        checks.message,
        checks.delegation,
        checks.value,
        checks.authority,
    ] {
        match evidence {
            Evidence::Invalid => return VerificationState::ProvenForgery,
            Evidence::Missing => pending = true,
            Evidence::Present => {}
        }
    }
    for predecessor in predecessors {
        match predecessor {
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
