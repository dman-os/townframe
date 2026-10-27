//! ResolveActor: identify the actor a capability presents.

use super::*;

impl AuthorityService {
    async fn resolve_actor(&self, request: ResolveActor) -> Result<Actor, AuthorityError> {
        let uuid = self.resolve(request.capability).await?.actor(Rights::USE)?;
        // Registry rows are never deleted and a binding is only published after its row
        // commits, so the row is there for any actor the ledger recognized.
        let transaction = self.read().await?;
        let row = Self::actor_row(&transaction, uuid).await?;
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
