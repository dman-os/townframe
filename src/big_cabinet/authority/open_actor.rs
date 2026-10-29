//! OpenActor: open (or create) the child actor at a path and return its identity and capability.
//!
//! The ledger never waits on storage, so opening is three steps: the ledger authorizes the
//! request and stages the capability, the registry row is found or written in one write
//! transaction, and the ledger then publishes the staged capability. A parent or namespace
//! retracted between the first and last step fails the open at the last step; the row it
//! may have written is the same row an open that succeeded and was then retracted leaves,
//! since registry rows outlive the in-memory capabilities that name them.

use super::store::ActorRow;
use super::*;

impl cap::State {
    pub(super) fn admit_actor(
        &self,
        parent: CapId,
        rights: Rights,
        object: String,
        namespace: Namespace,
    ) -> Result<(Uuid7, cap::CapRow), AuthorityError> {
        let parent_chain = cap::walk(self, &parent)?;
        let parent_actor = actor::recognize(self, &parent_chain, Rights::MINT_CHILD)?;
        let namespace_chain = cap::walk(self, &namespace.capability)?;
        namespace_chain.bound(rights)?;
        parent_chain.bound(rights)?;
        let issued = self.child(
            object,
            rights,
            namespace.namespace_id,
            cap::Kind::Object,
            [namespace.capability, parent],
        )?;
        Ok((parent_actor, issued))
    }

    /// Publish `issued` as `actor_id`'s capability under `parent`, unless that grant was
    /// already issued. Capabilities never change while live, so the admission still holds
    /// exactly when every capability `issued` depends on is still live.
    pub(super) fn bind_actor(
        &mut self,
        issued: cap::CapRow,
        actor_id: Uuid7,
        parent: CapId,
    ) -> Result<CapId, AuthorityError> {
        for dependency in &issued.dependencies {
            cap::walk(self, dependency)?;
        }
        let key = (actor_id, parent, issued.rights.bits_i64());
        if let Some(capability) = self.actor_caps.get(&key) {
            return Ok(*capability);
        }
        let capability = issued.id;
        self.insert(issued);
        assert!(
            self.actors
                .insert(
                    capability,
                    cap::ActorBinding {
                        actor_id,
                        reuse_key: Some(key),
                    },
                )
                .is_none()
        );
        assert!(self.actor_caps.insert(key, capability).is_none());
        Ok(capability)
    }
}

impl AuthorityService {
    async fn open_actor(&self, request: OpenActor) -> Result<Actor, AuthorityError> {
        let (parent_actor, issued) = cap::ask(&self.ledger, |reply| cap::Command::AdmitActor {
            parent: request.parent,
            rights: request.rights,
            object: format!("is actor {}", request.path),
            namespace: self.actors_namespace,
            reply,
        })
        .await?;
        let transaction = self.write().await?;
        let parent_row = Self::actor_row(&transaction, parent_actor).await?;
        actor::check_child_path(&parent_row.path, &request.path)?;

        let row = match store::actor_at(&transaction, &request.path).await? {
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
                    path: request.path,
                    parent: Some(parent_actor),
                    public_key,
                    sealed: actor::seal(&self.wrapping, &uuid.to_string(), &key)?,
                    certificate: certificate.as_slice().to_vec(),
                };
                store::insert_actor(&transaction, &row).await?;
                row
            }
        };
        transaction.commit().await?;
        let capability = cap::ask(&self.ledger, |reply| cap::Command::BindActor {
            issued,
            actor_id: row.actor_id,
            parent: request.parent,
            reply,
        })
        .await?;
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
