//! Actor identities, capabilities, and signing for Cabinet, exposed as a
//! `service_utils_rs` service.

mod actor;
mod attenuate;
mod cap;
mod inspect_cap;
mod mint;
mod observe;
mod open_actor;
mod resolve;
mod resolve_authority;
mod retract;
mod sign;
mod signer;
mod store;

#[cfg(test)]
mod tests;

use self::store::ActorRow;
use self::store::NodeRow;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AttenuateCapability;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::CapabilityStatus;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OPERATOR_OBJECT;
use big_cabinet_core::authority::ObserveCapability;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::ResolveActor;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::ResolvedAuthority;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::authority::SignFacetClaim;
use big_cabinet_core::dmeta::AttributionError;
use big_cabinet_core::dmeta::DmetaError;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::SignedFacetClaim;
use big_cabinet_core::dmeta::Uuid7;
use ed25519_dalek::SigningKey;
use secrets_rs::SecretStore;
use secrets_rs::SecretsError;
use service_utils_rs::Client;
use service_utils_rs::Envelope;
use service_utils_rs::Handler;
use service_utils_rs::Host;
use service_utils_rs::ResponseEnvelope;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use sqlite_rs::QueryExecutor;
use sqlite_rs::ReadTx;
use sqlite_rs::Tx;
use std::sync::Arc;
use thiserror::Error;
use zeroize::Zeroizing;

/// An Ed25519 public key in raw bytes. The wire spelling of an identity is
/// dmeta's `MultibaseKey` text; this alias is the internal storage form.
pub type ActorPublicKey = [u8; 32];

/// The keyring service holding the wrapping key, and the label every sealed
/// key is bound to.
const NODE_SERVICE: &str = "big_cabinet.authority";
const WRAPPING_KEY: &str = "wrapping";

pub(crate) enum DomainAuthority<S> {
    Identity(Uuid7),
    Scoped(S),
}

/// Every way an authority operation or boot fails. Operations report the wire
/// form, [`AuthorityServiceError`]; boot reports this one, whose sources are
/// kept for the embedder.
#[derive(Debug, Error)]
pub enum AuthorityError {
    #[error("capability is unknown, retracted, or descends from a retracted capability")]
    InvalidCapability,
    #[error("capability lacks the right this operation needs")]
    Unauthorized,
    #[error("the request asks for more than the presented capability holds")]
    Escalation,
    #[error("AuthorityActors did not associate this capability with an actor")]
    UnrecognizedActor,
    #[error("actor path is not its parent's path plus one segment")]
    InvalidActorPath,
    #[error("actor is not registered")]
    ActorNotFound,
    #[error("persisted authority state is corrupt: {0}")]
    StateCorrupt(String),
    #[error("authority storage failed: {0}")]
    Storage(#[from] sqlite_rs::SqlError),
    #[error("secret store failed: {0}")]
    Secrets(#[from] SecretsError),
    #[error("facet claim is invalid: {0}")]
    Claim(#[from] DmetaError),
    #[error("attribution certificate is invalid: {0}")]
    Attribution(#[from] AttributionError),
}

impl From<AuthorityError> for AuthorityServiceError {
    fn from(error: AuthorityError) -> Self {
        match error {
            AuthorityError::InvalidCapability => Self::InvalidCapability,
            AuthorityError::Unauthorized => Self::Unauthorized,
            AuthorityError::Escalation => Self::Escalation,
            AuthorityError::UnrecognizedActor => Self::UnrecognizedActor,
            AuthorityError::InvalidActorPath => Self::InvalidActorPath,
            AuthorityError::ActorNotFound => Self::ActorNotFound,
            AuthorityError::StateCorrupt(message) => Self::StateCorrupt(message),
            AuthorityError::Storage(error) => Self::Storage(error.to_string()),
            // A sealed key that does not open is the same fault as a corrupt row.
            AuthorityError::Secrets(error) => Self::StateCorrupt(error.to_string()),
            AuthorityError::Claim(error) => Self::Claim(error.to_string()),
            AuthorityError::Attribution(error) => Self::Claim(error.to_string()),
        }
    }
}

/// The sealed-key owner label of the node's own identity.
const NODE_KEY_OWNER: &str = "node";

pub struct AuthorityService {
    sql: Database,
    wrapping: Zeroizing<[u8; 32]>,
    node_key: SigningKey,
    operator: Uuid7,
    root_cap: CapId,
    actors_namespace: Namespace,
    /// The ledger task owning every live capability; see [`cap::Command`].
    ledger: tokio::sync::mpsc::Sender<cap::Command>,
    /// Bumped by the ledger after every retraction, so observers know to re-check.
    changes: tokio::sync::watch::Sender<u64>,
}

impl AuthorityService {
    /// How many capabilities are live. Retraction deletes rows, so this is the number of
    /// handles anyone can still present.
    #[cfg(test)]
    pub(crate) async fn live_capabilities(&self) -> usize {
        self.peek(|state| state.caps.len()).await
    }

    /// Run `peek` on the ledger's state between two commands.
    #[cfg(test)]
    async fn peek<T: Send + 'static>(
        &self,
        peek: impl FnOnce(&cap::State) -> T + Send + 'static,
    ) -> T {
        let (reply, answer) = tokio::sync::oneshot::channel();
        self.ledger
            .send(cap::Command::Peek(Box::new(move |state| {
                reply
                    .send(peek(state))
                    .unwrap_or_else(|_| panic!("{}", utils_rs::expect_tags::ERROR_CHANNEL));
            })))
            .await
            .expect(utils_rs::expect_tags::ERROR_ACTOR);
        answer.await.expect(utils_rs::expect_tags::ERROR_CHANNEL)
    }

    /// Stall the ledger once it reaches this command, until the returned sender fires or
    /// drops. Commands sent meanwhile queue behind it in send order.
    #[cfg(test)]
    async fn hold(&self) -> tokio::sync::oneshot::Sender<()> {
        let (release, held) = tokio::sync::oneshot::channel();
        self.ledger
            .send(cap::Command::Hold(held))
            .await
            .expect(utils_rs::expect_tags::ERROR_ACTOR);
        release
    }

    pub async fn boot(
        secrets: &SecretStore,
        sql: Database,
    ) -> Result<(Arc<Self>, StopToken), AuthorityError> {
        let (stop, tasks) = StopToken::new();
        store::migrate(&sql).await?;
        // The keyring read happens under the write reservation so two first
        // boots cannot each create a wrapping key and a node.
        let transaction = sql.begin_write().await?;
        let existing = store::node(&transaction).await?;
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
            None => Self::first_boot(&transaction, &wrapping).await?,
        };
        let node_key = actor::unseal(&wrapping, NODE_KEY_OWNER, &node.sealed, &node.public_key)?;
        transaction.commit().await?;
        let mut state = cap::State::default();
        let actors_namespace = state.open_namespace(node.actors_issuer);
        let root = state.child(
            OPERATOR_OBJECT.to_owned(),
            Rights::all(),
            node.actors_issuer,
            cap::Kind::Object,
            [actors_namespace.capability],
        )?;
        let root_cap = root.id;
        state.insert(root);
        assert!(
            state
                .actors
                .insert(
                    root_cap,
                    cap::ActorBinding {
                        actor_id: node.operator,
                        reuse_key: None,
                    },
                )
                .is_none()
        );
        let changes = tokio::sync::watch::channel(0).0;
        let ledger = cap::spawn(state, changes.clone(), &tasks);
        Ok((
            Arc::new(Self {
                sql,
                wrapping,
                node_key,
                operator: node.operator,
                root_cap,
                actors_namespace,
                ledger,
                changes,
            }),
            stop,
        ))
    }

    async fn first_boot(transaction: &Tx, wrapping: &[u8; 32]) -> Result<NodeRow, AuthorityError> {
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
                actor_id: operator,
                path: String::new(),
                parent: None,
                public_key: operator_public_key,
                sealed: actor::seal(wrapping, &operator.to_string(), &operator_key)?,
                certificate: certificate.as_slice().to_vec(),
            },
        )
        .await?;
        let actors_issuer = Uuid7::new();
        let node = NodeRow {
            public_key: node_public_key,
            sealed: actor::seal(wrapping, NODE_KEY_OWNER, &node_key)?,
            operator,
            actors_issuer,
        };
        store::insert_node(transaction, &node).await?;
        Ok(node)
    }

    pub async fn open_namespace(&self, namespace_id: Uuid7) -> Result<Namespace, AuthorityError> {
        Ok(cap::ask(&self.ledger, |reply| {
            cap::Command::OpenNamespace(namespace_id, reply)
        })
        .await)
    }

    /// What the live capability `capability` says, and the actor it names, if any.
    async fn resolve(&self, capability: CapId) -> Result<cap::Resolved, AuthorityError> {
        cap::ask(&self.ledger, |reply| {
            cap::Command::Resolve(capability, reply)
        })
        .await
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

    async fn write(&self) -> Result<Tx, AuthorityError> {
        Ok(self.sql.begin_write().await?)
    }

    /// A read snapshot: every query in it sees one committed state.
    async fn read(&self) -> Result<ReadTx, AuthorityError> {
        Ok(self.sql.begin_read().await?)
    }

    async fn actor_row(
        transaction: &impl QueryExecutor,
        uuid: Uuid7,
    ) -> Result<ActorRow, AuthorityError> {
        Ok(store::actor(transaction, uuid)
            .await?
            .expect("an actor capability references a registered actor"))
    }
}

service_utils_rs::bind_service_protocol! {
    pub struct AuthorityBinding for AuthorityProtocol => AuthorityMessage {
        unary {
            OpenActor(OpenActor),
            ResolveActor(ResolveActor),
            ResolveAuthority(ResolveAuthority),
            MintCapability(MintCapability),
            AttenuateCapability(AttenuateCapability),
            InspectCapability(InspectCapability),
            RetractCapability(RetractCapability),
            SignFacetClaim(SignFacetClaim)
        }
        stream { ObserveCapability(ObserveCapability) }
    }
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
    sql: Database,
) -> Result<(Authority, StopToken), AuthorityError> {
    let (service, service_stop) = AuthorityService::boot(secrets, sql).await?;
    let (host, host_stop) = Host::new(Arc::clone(&service) as AuthorityHandler);
    let client = Client::direct(AuthorityBinding::new(Arc::clone(&host)));
    let authority = Authority {
        root: service.root_cap(),
        node_issuer: service.node_issuer(),
        service,
        host,
        client,
    };
    Ok((authority, host_stop.then(service_stop)))
}
