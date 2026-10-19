use super::{AuthorityError, CapId};
use big_cabinet_core::dmeta::{FacetClaim, MultibaseKey, SignatureBytes, SignedFacetClaim};
use sqlx::{Sqlite, Transaction};

pub(super) async fn sign_as_in(
    repo: &super::AuthorityRepo,
    transaction: &mut Transaction<'_, Sqlite>,
    cap_id: &CapId,
    claim: &FacetClaim,
) -> Result<SignedFacetClaim, AuthorityError> {
    super::cap::verify_chain(transaction, cap_id).await?;
    let Some(root_key) = super::cap::effective_root_actor(transaction, cap_id).await? else {
        return Err(AuthorityError::Unauthorized);
    };
    let Some(actor) =
        super::store::AuthorityStateStore::actor_by_key_tx(transaction, &root_key).await?
    else {
        return Err(AuthorityError::ActorSignerMissing);
    };
    let Some(uuid_text) = actor.uuid.clone() else {
        return Err(AuthorityError::ActorSignerMissing);
    };
    let uuid = uuid_text
        .parse::<uuid::Uuid>()
        .map_err(|_| AuthorityError::StateCorrupt("actor uuid is not a uuid".into()))?;
    let actor_uuid = big_cabinet_core::dmeta::Uuid7::try_from(uuid)?;
    if claim.actor_uuid != actor_uuid {
        return Err(AuthorityError::Unauthorized);
    }
    claim.signing_bytes()?;
    let signer = super::actor::open_actor_signer(&repo.store, &actor.key_name, &root_key).await?;
    let signature = signer.sign(&claim.signing_bytes()?).await?;
    SignedFacetClaim::from_signature(
        claim.clone(),
        MultibaseKey::try_from(root_key.as_slice())?,
        SignatureBytes::try_from(signature.as_slice())?,
    )
    .map_err(Into::into)
}
