//! GrantAccess: record a grant in an issuing actor's namespace.

use super::store::GrantRow;
use super::*;

impl AuthorityService {
    async fn grant(&self, request: GrantAccess) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let (issuer, granted_via) = self
            .grant_scope(&mut transaction, &via, &request.object)
            .await?;
        via.bound(request.rights)?;
        if !store::actor_exists(&mut transaction, request.subject_id).await? {
            return Err(AuthorityError::ActorNotFound);
        }
        store::put_grant(
            &mut transaction,
            issuer,
            request.subject_id,
            &request.object,
            &GrantRow {
                via: granted_via,
                rights: request.rights,
            },
        )
        .await?;
        transaction.commit().await?;
        self.publish_change();
        Ok(())
    }
}

impl Handler<GrantAccess> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<GrantAccess>,
    ) -> Result<ResponseEnvelope<()>, AuthorityServiceError> {
        Ok(self.grant(request.params).await?.into())
    }
}
