//! SignFacetClaim: sign a dmeta facet claim as the presenting actor.

use super::operation;
use super::*;

/// Sign a dmeta v2 facet claim as the actor `actor` is the capability of. Needs
/// the actor's own capability with `use`, and the claim's `actorUuid` must be
/// that actor. Which drawer authorizes the claim's document is DocsRepo's
/// check, made before this call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignFacetClaim {
    pub actor: CapId,
    pub claim: FacetClaim,
}

operation!(SignFacetClaim => SignedFacetClaim, procedure, "signFacetClaim", "SignedFacetClaim");

impl AuthorityService {
    async fn sign(&self, request: SignFacetClaim) -> Result<SignedFacetClaim, AuthorityError> {
        let mut transaction = self.read().await?;
        let chain = cap::walk(&mut transaction, &request.actor).await?;
        let uuid = actor::recognize(&mut transaction, &chain, Rights::USE).await?;
        let row = Self::actor_row(&mut transaction, uuid).await?;
        drop(transaction);
        signer::sign_claim(&self.wrapping, &row, request.claim)
    }
}

impl Handler<SignFacetClaim> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<SignFacetClaim>,
    ) -> Result<ResponseEnvelope<SignedFacetClaim>, AuthorityServiceError> {
        Ok(self.sign(request.params).await?.into())
    }
}
