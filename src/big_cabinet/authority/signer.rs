//! The only place an actor signature is produced.
//!
//! Signing is bound to an actor's own capability: the claim's actor UUID must
//! be the actor the capability belongs to, and the key that signs is that
//! actor's sealed key, opened for this one signature and checked against the
//! registered public identity.

use super::store::ActorRow;
use super::{AuthorityError, actor};
use big_cabinet_core::dmeta::{FacetClaim, SignedFacetClaim};

pub(super) fn sign_claim(
    wrapping: &[u8; 32],
    row: &ActorRow,
    claim: FacetClaim,
) -> Result<SignedFacetClaim, AuthorityError> {
    if claim.actor_uuid != row.uuid {
        return Err(AuthorityError::Unauthorized);
    }
    // Canonicalize the claim before opening the key: a malformed mutation
    // context must not reach the signing boundary.
    let message = claim.signing_bytes()?;
    let key = actor::unseal(
        wrapping,
        &row.uuid.to_string(),
        &row.sealed,
        &row.public_key,
    )?;
    let signature = actor::signature(&key, &message);
    Ok(SignedFacetClaim::from_signature(
        claim,
        actor::key_text(&row.public_key)?,
        signature,
    )?)
}
