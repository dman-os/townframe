//! ResolveActor: the public identity of an actor, by id.

use super::*;

impl AuthorityService {
    async fn resolve_actor(&self, request: ResolveActor) -> Result<Actor, AuthorityError> {
        // An actor's id, URL, public key, certificate and parent are public information, so
        // this is not gated by a capability: knowing an id does not confer authority.
        let row = self.cached_actor_row(request.actor_id).await?;
        actor::public_actor(&self.node_public_key(), &row)
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
