//! RetractCapability: retract a capability and everything derived from it.

use super::*;

impl AuthorityService {
    async fn retract(&self, request: RetractCapability) -> Result<(), AuthorityError> {
        let mut state = self.state.lock().await;
        cap::walk(&state, &request.via)?;
        let target = cap::walk(&state, &request.capability)?;
        cap::may_retract(&target, &request.via)?;
        state.retract(request.capability);
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
