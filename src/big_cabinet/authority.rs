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

use self::store::{ActorRow, NodeRow};
use big_cabinet_core::dmeta::{
    AttributionError, DmetaError, MultibaseKey, SignedFacetClaim, Uuid7,
};
use ed25519_dalek::SigningKey;
use secrets_rs::{SecretStore, SecretsError};
use service_utils_rs::{Client, Envelope, Handler, Host, ResponseEnvelope, StopToken};
use sqlx::{Sqlite, Transaction};
use sqlx_utils_rs::SqlCtx;
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

pub use big_cabinet_core::authority::*;

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
    Storage(#[from] sqlx::Error),
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
    sql: SqlCtx,
    wrapping: Zeroizing<[u8; 32]>,
    node_key: SigningKey,
    operator: Uuid7,
    root_cap: CapId,
    actors_namespace: Namespace,
    state: Arc<tokio::sync::Mutex<cap::State>>,
    changes: tokio::sync::watch::Sender<u64>,
}

impl AuthorityService {
    /// How many capabilities are live. Retraction deletes rows, so this is the number of
    /// handles anyone can still present.
    #[cfg(test)]
    pub(crate) async fn live_capabilities(&self) -> usize {
        self.state.lock().await.caps.len()
    }

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
        Ok((
            Arc::new(Self {
                sql,
                wrapping,
                node_key,
                operator: node.operator,
                root_cap,
                actors_namespace,
                state: Arc::new(tokio::sync::Mutex::new(state)),
                changes: tokio::sync::watch::channel(0).0,
            }),
            stop,
        ))
    }

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
        Ok(self.state.lock().await.open_namespace(namespace_id))
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
    sql: SqlCtx,
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
