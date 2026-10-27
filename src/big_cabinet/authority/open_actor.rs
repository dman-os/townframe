//! OpenActor: open (or create) the child actor at a path and return its identity and capability.

use super::store::ActorRow;
use super::*;

impl AuthorityService {
    async fn open_actor(&self, request: OpenActor) -> Result<Actor, AuthorityError> {
        let mut state = self.state.lock().await;
        let parent = cap::walk(&state, &request.parent)?;
        let parent_actor = actor::recognize(&state, &parent, Rights::MINT_CHILD)?;
        let namespace = cap::walk(&state, &self.actors_namespace.capability)?;
        namespace.bound(request.rights)?;
        parent.bound(request.rights)?;
        let issued = state.child(
            format!("is actor {}", request.path),
            request.rights,
            self.actors_namespace.namespace_id,
            cap::Kind::Object,
            [self.actors_namespace.capability, request.parent],
        )?;
        let mut transaction = self.write().await?;
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
        let key = (row.actor_id, request.parent, request.rights.bits_i64());
        let existing = state.actor_caps.get(&key).copied();
        let capability = existing.unwrap_or(issued.id);
        let actor = actor::public_actor(&self.node_public_key(), &row, capability)?;
        transaction.commit().await?;
        if existing.is_none() {
            state.insert(issued);
            assert!(
                state
                    .actors
                    .insert(
                        capability,
                        cap::ActorBinding {
                            actor_id: row.actor_id,
                            reuse_key: Some(key),
                        },
                    )
                    .is_none()
            );
            assert!(state.actor_caps.insert(key, capability).is_none());
        }
        Ok(actor)
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
