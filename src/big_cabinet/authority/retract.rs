//! RetractCapability: retract a capability and everything derived from it.

use super::*;

impl AuthorityService {
    async fn retract(&self, request: RetractCapability) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        cap::walk(&mut transaction, &request.via).await?;
        let target = cap::walk(&mut transaction, &request.capability).await?;
        cap::may_retract(&target, &request.via)?;
        if request.capability == self.root_cap {
            // The operator root is the boot invariant; retracting it would leave
            // nothing that can open an actor or name an object.
            return Err(AuthorityError::Unauthorized);
        }
        store::retract_cap(&mut transaction, &request.capability).await?;
        transaction.commit().await?;
        self.publish_change();
        Ok(())
    }
}

impl Handler<RetractCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<RetractCapability>,
    ) -> Result<ResponseEnvelope<()>, AuthorityServiceError> {
        Ok(self.retract(request.params).await?.into())
    }
}
