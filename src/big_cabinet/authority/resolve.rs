//! ResolveActor: identify the actor a capability presents.

use super::*;

impl AuthorityService {
    async fn resolve_actor(&self, request: ResolveActor) -> Result<Actor, AuthorityError> {
        let mut transaction = self.read().await?;
        let chain = cap::walk(&mut transaction, &request.capability).await?;
        let uuid = actor::recognize(&mut transaction, &chain, Rights::USE).await?;
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
