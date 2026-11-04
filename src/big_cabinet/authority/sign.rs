//! SignDatumClaim: sign a dmeta datum claim as the presenting actor.

use super::*;

impl AuthorityService {
    async fn sign(&self, request: SignDatumClaim) -> Result<SignedDatumClaim, AuthorityError> {
        let uuid = self.resolve(request.actor).await?.actor(Rights::USE)?;
        let row = self.cached_actor_row(uuid).await?;
        signer::sign_claim(&self.wrapping, &row, request.claim)
    }
}

impl Handler<SignDatumClaim> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<SignDatumClaim>,
    ) -> Result<ResponseEnvelope<SignedDatumClaim>, AuthorityServiceError> {
        Ok(self.sign(request.params).await?.into())
    }
}
