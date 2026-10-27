//! InspectCapability: report what a live capability says.

use super::*;

impl Handler<InspectCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<InspectCapability>,
    ) -> Result<ResponseEnvelope<Capability>, AuthorityServiceError> {
        Ok(self
            .resolve(request.params.capability)
            .await?
            .capability
            .into())
    }
}
