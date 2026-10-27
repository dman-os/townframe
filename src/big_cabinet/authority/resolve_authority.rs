use super::*;

impl AuthorityService {
    async fn resolve_authority(
        &self,
        request: ResolveAuthority,
    ) -> Result<ResolvedAuthority, AuthorityError> {
        let state = self.state.lock().await;
        let chain = cap::walk(&state, &request.capability)?;
        chain.require(Rights::USE)?;
        let actor = match state.actors.get(&chain.cap.id) {
            Some(binding) => {
                let actor_id = binding.actor_id;
                let mut transaction = self.read().await?;
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
