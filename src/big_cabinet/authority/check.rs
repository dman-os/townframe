//! CheckGrant: the standing rights a subject holds over an object.

use super::*;

impl AuthorityService {
    async fn check_grant(&self, request: CheckGrant) -> Result<Rights, AuthorityError> {
        let mut rights = self
            .check_grants(CheckGrants {
                via: request.via,
                subject_id: request.subject_id,
                objects: vec![request.object],
            })
            .await?;
        Ok(rights.pop().expect("one object produces one grant verdict"))
    }
}

impl Handler<CheckGrant> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<CheckGrant>,
    ) -> Result<ResponseEnvelope<Rights>, AuthorityServiceError> {
        Ok(self.check_grant(request.params).await?.into())
    }
}
