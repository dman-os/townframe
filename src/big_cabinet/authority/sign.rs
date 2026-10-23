//! SignFacetClaim: sign a dmeta facet claim as the presenting actor.

use super::*;

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
