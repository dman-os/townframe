//! ResolveActor: identify the actor a capability presents.

use super::*;

impl AuthorityService {
    async fn resolve_actor(&self, request: ResolveActor) -> Result<Actor, AuthorityError> {
        let state = self.state.lock().await;
        let chain = cap::walk(&state, &request.capability)?;
        let uuid = actor::recognize(&state, &chain, Rights::USE)?;
        let mut transaction = self.read().await?;
        let row = Self::actor_row(&mut transaction, uuid).await?;
        actor::public_actor(&self.node_public_key(), &row, request.capability)
    }
}

impl Handler<ResolveActor> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<ResolveActor>,
    ) -> Result<ResponseEnvelope<Actor>, AuthorityServiceError> {
        Ok(self.resolve_actor(request.params).await?.into())
    }
}
