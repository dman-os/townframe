//! InspectCapability: report what a live capability says.

use super::*;

impl AuthorityService {
    async fn inspect(&self, request: InspectCapability) -> Result<Capability, AuthorityError> {
        let mut transaction = self.read().await?;
        Ok(cap::walk(&mut transaction, &request.capability)
            .await?
            .cap
            .wire())
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
