//! Actor identities, capabilities, grants, and signing for Cabinet, exposed as a
//! `service_utils_rs` service.
//!
//! The public surface is [`AuthorityProtocol`] plus the [`boot`] function that
//! hosts it. Four parts stay independent inside the service so each can be lifted
//! out later without untangling the others (ADR 017): a capability table, an
//! actor registry, a grant ledger namespaced by issuing actor, and a signer that
//! signs as the actor whose own capability is presented. Actors name objects and
//! decide containment; nothing here interprets an object string. Each protocol
//! operation lives in its own file with its wire type and implementation.
//!
//! The public surface is [`AuthorityProtocol`]; every method is reached through
//! a `Client<AuthorityProtocol>`, so the same authorization path runs for an
//! in-process caller, another process on the node, and an HTTP/XRPC caller.
//! Writes run in one `BEGIN IMMEDIATE` transaction on the writer pool, so each
//! is checked and applied against the state it read. Reads — inspection,
//! resolution, grant checks, and signing — run on the reader pool beside them
//! and never wait for a writer.

mod actor;
mod attenuate;
mod cap;
mod check;
mod grant;
mod inspect_cap;
mod mint;
mod open_actor;
mod resolve;
mod retract;
mod revoke;
mod sign;
mod signer;
mod store;

#[cfg(test)]
mod tests;

pub use self::attenuate::AttenuateCapability;
pub use self::check::CheckGrant;
pub use self::grant::GrantAccess;
pub use self::inspect_cap::InspectCapability;
pub use self::mint::MintCapability;
pub use self::open_actor::OpenActor;
pub use self::resolve::ResolveActor;
pub use self::retract::RetractCapability;
pub use self::revoke::RevokeAccess;
pub use self::sign::SignFacetClaim;

use self::store::{ActorRow, CapRow, NodeRow};
use big_cabinet_core::dmeta::{
    AttributionError, Bytes, DmetaError, FacetClaim, MultibaseKey, SignedFacetClaim, Uuid7,
};
use ed25519_dalek::SigningKey;
use secrets_rs::{SecretStore, SecretsError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use service_utils_rs::{
    Client, ContextError, Envelope, Handler, Host, Operation, ResponseEnvelope, StopToken,
    XrpcOperation, service_protocol,
};
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

/// The object string of the operator's root capability.
pub const OPERATOR_OBJECT: &str = "is operator";

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapId(uuid::Uuid);

impl CapId {
    pub(super) fn new_random() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub(super) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    pub(super) fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl std::fmt::Debug for CapId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CapId([redacted])")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct Rights(u32);

impl Rights {
    pub const USE: Self = Self(1);
    pub const NARROW: Self = Self(2);
    pub const MINT_CHILD: Self = Self(4);
    pub const DELEGATE: Self = Self(8);

    const NAMES: [(Self, &'static str); 4] = [
        (Self::USE, "use"),
        (Self::NARROW, "narrow"),
        (Self::MINT_CHILD, "mintChild"),
        (Self::DELEGATE, "delegate"),
    ];

    pub const fn all() -> Self {
        Self(Self::USE.0 | Self::NARROW.0 | Self::MINT_CHILD.0 | Self::DELEGATE.0)
    }

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn is_subset_of(self, other: Self) -> bool {
        other.contains(self)
    }

    fn named(name: &str) -> Option<Self> {
        Self::NAMES
            .iter()
            .find(|(_, right)| *right == name)
            .map(|(right, _)| *right)
    }

    pub(super) fn bits_i64(self) -> i64 {
        i64::from(self.0)
    }

    /// Rights read back from storage. Bits outside the vocabulary are a corrupt
    /// row, not a right this build forgot.
    pub(super) fn from_bits(bits: i64) -> Option<Self> {
        u32::try_from(bits)
            .ok()
            .filter(|bits| bits & !Self::all().0 == 0)
            .map(Self)
    }
}

impl std::ops::BitOr for Rights {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::fmt::Display for Rights {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (right, name) in Self::NAMES {
            if self.contains(right) {
                if !first {
                    formatter.write_str("+")?;
                }
                formatter.write_str(name)?;
                first = false;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for Rights {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl Serialize for Rights {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let names: Vec<&str> = Self::NAMES
            .iter()
            .filter(|(right, _)| self.contains(*right))
            .map(|(_, name)| *name)
            .collect();
        let mut seq = serializer.serialize_seq(Some(names.len()))?;
        for name in names {
            seq.serialize_element(name)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Rights {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let names = Vec::<String>::deserialize(deserializer)?;
        let mut rights = Self::default();
        for name in names {
            let right = Self::named(&name).ok_or_else(|| {
                serde::de::Error::custom(format!("unknown capability right {name}"))
            })?;
            rights = rights | right;
        }
        Ok(rights)
    }
}

/// One capability as the service reports it. `object` is opaque here: only the
/// actor that named it knows what it means.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capability {
    pub id: CapId,
    pub object: String,
    pub rights: Rights,
    pub issuer: Uuid7,
}

/// An actor binding, carrying exactly what dmeta's `ActorMetadata` needs plus
/// the actor capability that was presented for it or opened with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    pub uuid: Uuid7,
    pub url: String,
    pub public_key: MultibaseKey,
    /// Node-signed attribution certificate (ADR 015 §5) for this binding.
    pub certificate: Bytes,
    /// The actor that opened this one. The operator has none.
    pub parent_actor: Option<Uuid7>,
    pub capability: CapId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum AuthorityServiceError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
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
    #[error("authority state is corrupt: {0}")]
    StateCorrupt(String),
    #[error("authority storage failed: {0}")]
    Storage(String),
    #[error("claim is invalid or could not be signed: {0}")]
    Claim(String),
}

macro_rules! operation {
    ($request:ident => $output:ty, $kind:ident, $nsid:literal, $schema:literal) => {
        impl Operation for $request {
            type Output = $output;
            type Error = AuthorityServiceError;
            const XRPC: XrpcOperation =
                XrpcOperation::$kind(concat!("org.example.daybook.authority.", $nsid))
                    .with_schemas($schema, "AuthorityServiceError");
        }
    };
}

pub(crate) use operation;

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
}

service_protocol! {
    pub enum AuthorityProtocol => AuthorityMessage {
        unary {
            OpenActor(OpenActor),
            ResolveActor(ResolveActor),
            MintCapability(MintCapability),
            AttenuateCapability(AttenuateCapability),
            InspectCapability(InspectCapability),
            RetractCapability(RetractCapability),
            GrantAccess(GrantAccess),
            RevokeAccess(RevokeAccess),
            CheckGrant(CheckGrant),
            SignFacetClaim(SignFacetClaim)
        }
        stream {}
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
