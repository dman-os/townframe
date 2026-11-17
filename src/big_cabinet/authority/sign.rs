//! SignDatumClaim: sign a dmeta datum claim as the subject of a handle.

use super::*;

impl AuthorityService {
    /// Sign as the actor `request.via` is bound to. The handle must carry `USE` and name an
    /// actor, and the claim must be that actor's own: a handle never signs for anyone else.
    async fn sign(&self, request: SignDatumClaim) -> Result<SignedDatumClaim, AuthorityError> {
        let subject = self.resolve(request.via).await?.subject(Rights::USE)?;
        let row = self.cached_actor_row(subject).await?;
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
