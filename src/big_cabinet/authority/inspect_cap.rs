//! InspectCapability: report what a live capability says.

use super::*;

impl AuthorityService {
    async fn inspect(&self, request: InspectCapability) -> Result<Capability, AuthorityError> {
        let state = self.state.lock().await;
        Ok(cap::walk(&state, &request.capability)?.cap.wire())
    }
}

impl Handler<InspectCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<InspectCapability>,
    ) -> Result<ResponseEnvelope<Capability>, AuthorityServiceError> {
        Ok(self.inspect(request.params).await?.into())
    }
}
