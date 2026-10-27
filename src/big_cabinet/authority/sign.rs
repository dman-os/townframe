//! SignFacetClaim: sign a dmeta facet claim as the presenting actor.

use super::*;

impl AuthorityService {
    async fn sign(&self, request: SignFacetClaim) -> Result<SignedFacetClaim, AuthorityError> {
        let state = self.state.lock().await;
        let chain = cap::walk(&state, &request.actor)?;
        let uuid = actor::recognize(&state, &chain, Rights::USE)?;
        let transaction = self.read().await?;
        let row = Self::actor_row(&transaction, uuid).await?;
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
