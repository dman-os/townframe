//! ResolveActor: identify the actor a capability presents.

use super::operation;
use super::*;

/// Which actor is this actor capability? Needs `use`: presenting an actor's
/// capability is how a caller identifies as it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveActor {
    pub capability: CapId,
}

operation!(ResolveActor => Actor, query, "resolveActor", "Actor");

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
