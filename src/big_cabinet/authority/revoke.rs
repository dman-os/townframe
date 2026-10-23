//! RevokeAccess: withdraw grants in an issuing actor's namespace.

use super::*;

impl AuthorityService {
    async fn revoke(&self, request: RevokeAccess) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let (issuer, granted_via) = self
            .grant_scope(&mut transaction, &via, &request.object)
            .await?;
        match granted_via {
            None => {
                store::delete_grants(
                    &mut transaction,
                    issuer,
                    request.subject_id,
                    &request.object,
                    None,
                )
                .await?;
            }
            Some(_) => {
                let rows = store::grants(
                    &mut transaction,
                    issuer,
                    request.subject_id,
                    &request.object,
                )
                .await?;
                for granted in rows.iter().filter_map(|row| row.via.as_ref()) {
                    if cap::descends_from(&mut transaction, granted, &via.cap.id).await? {
                        store::delete_grants(
                            &mut transaction,
                            issuer,
                            request.subject_id,
                            &request.object,
                            Some(granted),
                        )
                        .await?;
                    }
                }
            }
        }
        transaction.commit().await?;
        self.publish_change();
        Ok(())
    }
}

impl Handler<RevokeAccess> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<RevokeAccess>,
    ) -> Result<ResponseEnvelope<()>, AuthorityServiceError> {
        Ok(self.revoke(request.params).await?.into())
    }
}
