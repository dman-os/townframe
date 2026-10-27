use super::*;

impl AuthorityService {
    async fn resolve_authority(
        &self,
        request: ResolveAuthority,
    ) -> Result<ResolvedAuthority, AuthorityError> {
        let resolved = self.resolve(request.capability).await?;
        resolved.require(Rights::USE)?;
        let actor = match resolved.actor {
            Some(actor_id) => {
                let transaction = self.read().await?;
                let row = Self::actor_row(&transaction, actor_id).await?;
                Some(actor::public_actor(
                    &self.node_public_key(),
                    &row,
                    request.capability,
                )?)
            }
            None => None,
        };
        Ok(ResolvedAuthority {
            capability: resolved.capability,
            actor,
        })
    }
}

impl Handler<ResolveAuthority> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<ResolveAuthority>,
    ) -> Result<ResponseEnvelope<ResolvedAuthority>, AuthorityServiceError> {
        Ok(self.resolve_authority(request.params).await?.into())
    }
}
