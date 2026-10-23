use super::*;

impl AuthorityService {
    async fn resolve_authority(
        &self,
        request: ResolveAuthority,
    ) -> Result<ResolvedAuthority, AuthorityError> {
        let mut transaction = self.read().await?;
        let chain = cap::walk(&mut transaction, &request.capability).await?;
        chain.require(Rights::USE)?;
        let actor = match store::associated_actor(&mut transaction, chain.cap.id).await? {
            Some(actor_id) => {
                let row = Self::actor_row(&mut transaction, actor_id).await?;
                Some(actor::public_actor(
                    &self.node_public_key(),
                    &row,
                    request.capability,
                )?)
            }
            None => None,
        };
        Ok(ResolvedAuthority {
            capability: chain.cap.wire(),
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
