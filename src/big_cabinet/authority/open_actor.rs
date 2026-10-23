//! OpenActor: open (or create) the child actor at a path and return its identity and capability.

use super::store::ActorRow;
use super::*;

impl AuthorityService {
    async fn open_actor(&self, request: OpenActor) -> Result<Actor, AuthorityError> {
        let mut transaction = self.write().await?;
        let parent = cap::walk(&mut transaction, &request.parent).await?;
        let parent_actor = actor::recognize(&mut transaction, &parent, Rights::MINT_CHILD).await?;
        parent.bound(request.rights)?;
        let parent_row = Self::actor_row(&mut transaction, parent_actor).await?;
        actor::check_child_path(&parent_row.path, &request.path)?;

        let row = match store::actor_at(&mut transaction, &request.path).await? {
            Some(row) => {
                assert_eq!(row.parent, Some(parent_actor), "a path names its parent");
                row
            }
            None => {
                let uuid = Uuid7::new();
                let key = actor::generate_key();
                let public_key = key.verifying_key().to_bytes();
                let url = actor::actor_url(&self.node_public_key(), &request.path)?;
                let certificate =
                    actor::issue_certificate(&self.node_key, uuid, &public_key, &url)?;
                let row = ActorRow {
                    actor_id: uuid,
                    path: request.path.clone(),
                    parent: Some(parent_actor),
                    public_key,
                    sealed: actor::seal(&self.wrapping, &uuid.to_string(), &key)?,
                    certificate: certificate.as_slice().to_vec(),
                };
                store::insert_actor(&mut transaction, &row).await?;
                row
            }
        };
        let capability = actor::issue(
            &mut transaction,
            &parent,
            self.actors_issuer,
            &row,
            request.rights,
        )
        .await?;
        transaction.commit().await?;
        actor::public_actor(&self.node_public_key(), &row, capability)
    }
}

impl Handler<OpenActor> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<OpenActor>,
    ) -> Result<ResponseEnvelope<Actor>, AuthorityServiceError> {
        Ok(self.open_actor(request.params).await?.into())
    }
}
