//! The authority service: durable state, boot, and one handler per protocol
//! operation.
//!
//! The public surface is [`AuthorityProtocol`]; every method here is reached
//! through a `Client<AuthorityProtocol>`, so the same authorization path runs for
//! an in-process caller, another process on the node, and an HTTP/XRPC caller.
//!
//! Writes run in one `BEGIN IMMEDIATE` transaction on the writer pool, so each
//! is checked and applied against the state it read. Reads — inspection,
//! resolution, grant checks, and signing — run on the reader pool beside them
//! and never wait for a writer.

use super::protocol::{
    Actor, AttenuateCapability, AuthorityProtocol, AuthorityServiceError, CapId, Capability,
    CheckGrant, GrantAccess, InspectCapability, MintCapability, OpenActor, ResolveActor,
    RetractCapability, RevokeAccess, Rights, SignFacetClaim,
};
use super::store::{self, ActorRow, CapRow, GrantRow, NodeRow};
use super::{
    ActorPublicKey, AuthorityError, NODE_SERVICE, OPERATOR_OBJECT, WRAPPING_KEY, actor, cap, signer,
};
use big_cabinet_core::dmeta::{MultibaseKey, SignedFacetClaim, Uuid7};
use ed25519_dalek::SigningKey;
use secrets_rs::SecretStore;
use service_utils_rs::{Client, Envelope, Handler, Host, Operation, ResponseEnvelope, StopToken};
use sqlx::{Sqlite, Transaction};
use sqlx_utils_rs::SqlCtx;
use std::sync::Arc;
use zeroize::Zeroizing;

/// The sealed-key owner label of the node's own identity.
const NODE_KEY_OWNER: &str = "node";

pub struct AuthorityService {
    sql: SqlCtx,
    wrapping: Zeroizing<[u8; 32]>,
    node_key: SigningKey,
    operator: Uuid7,
    root_cap: CapId,
    actors_issuer: Uuid7,
}

impl AuthorityService {
    /// Open the durable state, creating the node identity, the operator, and
    /// its root capability on first boot. The service has no background tasks;
    /// the token exists so callers compose authority shutdown with the
    /// components that depend on it.
    pub async fn boot(
        secrets: &SecretStore,
        sql: SqlCtx,
    ) -> Result<(Arc<Self>, StopToken), AuthorityError> {
        let (stop, _spawner) = StopToken::new();
        store::migrate(&sql.write_pool).await?;
        // The keyring read happens under the write reservation so two first
        // boots cannot each create a wrapping key and a node.
        let mut transaction = sql.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let existing = store::node(&mut transaction).await?;
        let wrapping = Zeroizing::new(match &existing {
            // Sealed keys exist, so only the key that sealed them will do: a
            // fresh one would turn every actor into an unopenable row.
            Some(_) => secrets
                .get_secret(NODE_SERVICE, WRAPPING_KEY)
                .await?
                .ok_or_else(|| {
                    AuthorityError::StateCorrupt(
                        "the keyring no longer holds the key the actor keys are sealed with".into(),
                    )
                })?,
            None => {
                secrets
                    .get_or_create_secret(NODE_SERVICE, WRAPPING_KEY, 32)
                    .await?
            }
        });
        let wrapping = Zeroizing::new(<[u8; 32]>::try_from(wrapping.as_slice()).map_err(|_| {
            AuthorityError::StateCorrupt("the wrapping key is not 32 bytes".into())
        })?);
        let node = match existing {
            Some(node) => node,
            None => Self::first_boot(&mut transaction, &wrapping).await?,
        };
        let node_key = actor::unseal(&wrapping, NODE_KEY_OWNER, &node.sealed, &node.public_key)?;
        transaction.commit().await?;
        Ok((
            Arc::new(Self {
                sql,
                wrapping,
                node_key,
                operator: node.operator,
                root_cap: node.root_cap,
                actors_issuer: node.actors_issuer,
            }),
            stop,
        ))
    }

    /// The node identity, the operator actor at the empty path, and the root
    /// capability, written together so a later boot finds all or none of them.
    async fn first_boot(
        transaction: &mut Transaction<'static, Sqlite>,
        wrapping: &[u8; 32],
    ) -> Result<NodeRow, AuthorityError> {
        let node_key = actor::generate_key();
        let node_public_key = node_key.verifying_key().to_bytes();
        let operator = Uuid7::new();
        let operator_key = actor::generate_key();
        let operator_public_key = operator_key.verifying_key().to_bytes();
        let url = actor::actor_url(&node_public_key, "")?;
        let certificate =
            actor::issue_certificate(&node_key, operator, &operator_public_key, &url)?;
        store::insert_actor(
            transaction,
            &ActorRow {
                uuid: operator,
                path: String::new(),
                parent: None,
                public_key: operator_public_key,
                sealed: actor::seal(wrapping, &operator.to_string(), &operator_key)?,
                certificate: certificate.as_slice().to_vec(),
            },
        )
        .await?;
        let actors_issuer = Uuid7::new();
        let root = CapRow {
            id: CapId::new_random(),
            object: OPERATOR_OBJECT.to_owned(),
            rights: Rights::all(),
            parent: None,
            issuer: actors_issuer,
            retracted: false,
        };
        store::insert_cap(transaction, &root).await?;
        store::associate_actor(transaction, root.id, operator).await?;
        let node = NodeRow {
            public_key: node_public_key,
            sealed: actor::seal(wrapping, NODE_KEY_OWNER, &node_key)?,
            operator,
            root_cap: root.id,
            actors_issuer,
        };
        store::insert_node(transaction, &node).await?;
        Ok(node)
    }

    pub fn root_cap(&self) -> CapId {
        self.root_cap
    }

    pub fn operator_uuid(&self) -> Uuid7 {
        self.operator
    }

    fn node_public_key(&self) -> ActorPublicKey {
        self.node_key.verifying_key().to_bytes()
    }

    /// The issuer a verifier must expect on this node's attribution certificates.
    pub fn node_issuer(&self) -> MultibaseKey {
        actor::key_text(&self.node_public_key()).expect("an Ed25519 key is a valid multibase key")
    }

    async fn write(&self) -> Result<Transaction<'static, Sqlite>, AuthorityError> {
        Ok(self.sql.write_pool.begin_with("BEGIN IMMEDIATE").await?)
    }

    /// A read snapshot: every query in it sees one committed state.
    async fn read(&self) -> Result<Transaction<'static, Sqlite>, AuthorityError> {
        Ok(self.sql.read_pool.begin().await?)
    }

    async fn actor_row(
        transaction: &mut Transaction<'static, Sqlite>,
        uuid: Uuid7,
    ) -> Result<ActorRow, AuthorityError> {
        Ok(store::actor(transaction, uuid)
            .await?
            .expect("an actor capability references a registered actor"))
    }

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
                    uuid,
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

    async fn resolve_actor(&self, request: ResolveActor) -> Result<Actor, AuthorityError> {
        let mut transaction = self.read().await?;
        let chain = cap::walk(&mut transaction, &request.capability).await?;
        let uuid = actor::recognize(&mut transaction, &chain, Rights::USE).await?;
        let row = Self::actor_row(&mut transaction, uuid).await?;
        actor::public_actor(&self.node_public_key(), &row, request.capability)
    }

    async fn mint(&self, request: MintCapability) -> Result<Capability, AuthorityError> {
        let mut transaction = self.write().await?;
        let issuer = cap::walk(&mut transaction, &request.issuer).await?;
        let namespace = actor::recognize(&mut transaction, &issuer, Rights::NARROW).await?;
        let child = issuer.child(request.object, request.rights, namespace)?;
        store::insert_cap(&mut transaction, &child).await?;
        transaction.commit().await?;
        Ok(child.wire())
    }

    async fn attenuate(&self, request: AttenuateCapability) -> Result<Capability, AuthorityError> {
        let mut transaction = self.write().await?;
        let held = cap::walk(&mut transaction, &request.capability).await?;
        held.require(Rights::NARROW)?;
        let child = held.child(held.cap.object.clone(), request.rights, held.cap.issuer)?;
        store::insert_cap(&mut transaction, &child).await?;
        transaction.commit().await?;
        Ok(child.wire())
    }

    async fn inspect(&self, request: InspectCapability) -> Result<Capability, AuthorityError> {
        let mut transaction = self.read().await?;
        Ok(cap::walk(&mut transaction, &request.capability)
            .await?
            .cap
            .wire())
    }

    async fn retract(&self, request: RetractCapability) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        cap::walk(&mut transaction, &request.via).await?;
        let target = cap::walk(&mut transaction, &request.capability).await?;
        cap::may_retract(&target, &request.via)?;
        if request.capability == self.root_cap {
            // The operator root is the boot invariant; retracting it would leave
            // nothing that can open an actor or name an object.
            return Err(AuthorityError::Unauthorized);
        }
        store::retract_cap(&mut transaction, &request.capability).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn grant_scope(
        &self,
        conn: &mut sqlx::SqliteConnection,
        via: &cap::Chain,
        object: &str,
    ) -> Result<(Uuid7, Option<CapId>), AuthorityError> {
        via.require(Rights::DELEGATE)?;
        if let Some(issuer) = store::associated_actor(conn, via.cap.id).await? {
            return Ok((issuer, None));
        }
        if via.cap.issuer == self.actors_issuer {
            return Err(AuthorityError::UnrecognizedActor);
        }
        if object != via.cap.object {
            return Err(AuthorityError::Escalation);
        }
        Ok((via.cap.issuer, Some(via.cap.id)))
    }

    async fn grant(&self, request: GrantAccess) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let (issuer, granted_via) = self
            .grant_scope(&mut transaction, &via, &request.object)
            .await?;
        via.bound(request.rights)?;
        if !store::actor_exists(&mut transaction, request.subject).await? {
            return Err(AuthorityError::ActorNotFound);
        }
        store::put_grant(
            &mut transaction,
            issuer,
            request.subject,
            &request.object,
            &GrantRow {
                via: granted_via,
                rights: request.rights,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn revoke(&self, request: RevokeAccess) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let (issuer, granted_via) = self
            .grant_scope(&mut transaction, &via, &request.object)
            .await?;
        match granted_via {
            None => {
                store::delete_grants(
                    &mut transaction,
                    issuer,
                    request.subject,
                    &request.object,
                    None,
                )
                .await?;
            }
            Some(_) => {
                let rows =
                    store::grants(&mut transaction, issuer, request.subject, &request.object)
                        .await?;
                for granted in rows.iter().filter_map(|row| row.via.as_ref()) {
                    if cap::descends_from(&mut transaction, granted, &via.cap.id).await? {
                        store::delete_grants(
                            &mut transaction,
                            issuer,
                            request.subject,
                            &request.object,
                            Some(granted),
                        )
                        .await?;
                    }
                }
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn check_grant(&self, request: CheckGrant) -> Result<Rights, AuthorityError> {
        let mut transaction = self.read().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let issuer = actor::recognize(&mut transaction, &via, Rights::USE).await?;
        let mut held = Rights::empty();
        for row in store::grants(&mut transaction, issuer, request.subject, &request.object).await?
        {
            let standing = match row.via {
                None => true,
                Some(granted) => match cap::walk(&mut transaction, &granted).await {
                    Ok(_) => true,
                    // Retracting the capability a grant was written through
                    // withdraws the grant.
                    Err(AuthorityError::InvalidCapability) => false,
                    Err(error) => return Err(error),
                },
            };
            if standing {
                held = held | row.rights;
            }
        }
        Ok(held)
    }

    async fn sign(&self, request: SignFacetClaim) -> Result<SignedFacetClaim, AuthorityError> {
        let mut transaction = self.read().await?;
        let chain = cap::walk(&mut transaction, &request.actor).await?;
        let uuid = actor::recognize(&mut transaction, &chain, Rights::USE).await?;
        let row = Self::actor_row(&mut transaction, uuid).await?;
        drop(transaction);
        signer::sign_claim(&self.wrapping, &row, request.claim)
    }
}

macro_rules! handlers {
    ($($request:ident => $method:ident),* $(,)?) => {
        $(
            impl Handler<$request> for AuthorityService {
                async fn call(
                    &self,
                    request: Envelope<$request>,
                ) -> Result<ResponseEnvelope<<$request as Operation>::Output>, AuthorityServiceError>
                {
                    Ok(self.$method(request.params).await?.into())
                }
            }
        )*
    };
}

handlers! {
    OpenActor => open_actor,
    ResolveActor => resolve_actor,
    MintCapability => mint,
    AttenuateCapability => attenuate,
    InspectCapability => inspect,
    RetractCapability => retract,
    GrantAccess => grant,
    RevokeAccess => revoke,
    CheckGrant => check_grant,
    SignFacetClaim => sign,
}

/// The layered handler the host serves. Naming it lets a consumer write
/// `Arc<Host<AuthorityHandler>>` and `Client::<AuthorityProtocol>::direct(host)`.
pub type AuthorityHandler = Arc<AuthorityService>;

/// A booted authority: the service, the host that fronts it, the client its
/// consumers use, and the operator root capability the embedder hands out.
pub struct Authority {
    pub service: Arc<AuthorityService>,
    pub host: Arc<Host<AuthorityHandler>>,
    pub client: Client<AuthorityProtocol>,
    pub root: CapId,
    /// The expected issuer for this node's attribution certificates.
    pub node_issuer: MultibaseKey,
}

/// Boot the authority service on `sql` and host it. The returned token stops
/// the host (joining in-flight calls) before the service.
pub async fn boot(
    secrets: &SecretStore,
    sql: SqlCtx,
) -> Result<(Authority, StopToken), AuthorityError> {
    let (service, service_stop) = AuthorityService::boot(secrets, sql).await?;
    let (host, host_stop) = Host::new(Arc::clone(&service) as AuthorityHandler);
    let client = Client::direct(Arc::clone(&host));
    let authority = Authority {
        root: service.root_cap(),
        node_issuer: service.node_issuer(),
        service,
        host,
        client,
    };
    Ok((authority, host_stop.then(service_stop)))
}
