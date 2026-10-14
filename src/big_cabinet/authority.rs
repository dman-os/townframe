//! Actor capabilities for Cabinet operations.
//!
//! Capability tokens authorize coarse Cabinet operations for an actor. They do
//! not replace BigRepo/Keyhive's node-level authority decision. Current
//! authorization state is owned by a SQLite store so independent repo handles
//! and processes observe issuance and revocation without stale in-memory state.

use std::{path::Path, sync::Arc};

use big_cabinet_core::dmeta::{FacetClaim, SignedFacetClaim};
use ed25519_dalek::{Signature as DalekSignature, Verifier, VerifyingKey};
use rand::RngCore;
use secrets_rs::{SecretStore, SecretsError, SigningKeyHandle};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, SqlitePool, Transaction, sqlite::SqlitePoolOptions};
use thiserror::Error;

pub type ActorPublicKey = [u8; 32];
pub type DrawerId = [u8; 32];
pub type TokenId = [u8; 16];
pub type Signature = Vec<u8>;

const TOKEN_VERSION: u8 = 1;
const NODE_SERVICE: &str = "big_cabinet.authority";
const NODE_KEY: &str = "node";
const OPERATOR_KEY: &str = "operator";

#[cfg(test)]
mod provisioning_test_faults {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    static FAIL_ACTOR_PUBLICATION: AtomicBool = AtomicBool::new(false);
    static FORCE_ACTOR_COMMIT_UNKNOWN: AtomicBool = AtomicBool::new(false);
    static FAIL_ROOT_INSTALL: AtomicBool = AtomicBool::new(false);
    static FAIL_ACTOR_INTENT_BIND: AtomicBool = AtomicBool::new(false);
    static FAIL_ACTOR_INTENT_REMOVAL: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_BARRIER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_REACHED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_RELEASED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_PRE_DELETE_NOTIFY: Notify = Notify::const_new();
    static CLEANUP_PRE_RELEASE_NOTIFY: Notify = Notify::const_new();
    static SERIALIZED_BEGIN_PROBE_ACTIVE: AtomicBool = AtomicBool::new(false);
    static SERIALIZED_BEGIN_PROBE_REACHED: AtomicBool = AtomicBool::new(false);
    static SERIALIZED_BEGIN_PROBE_NOTIFY: Notify = Notify::const_new();
    static CLEANUP_DELETE_BARRIER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static CLEANUP_DELETE_REACHED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_DELETE_RELEASED: AtomicBool = AtomicBool::new(false);
    static CLEANUP_DELETE_NOTIFY: Notify = Notify::const_new();
    static CLEANUP_RELEASE_NOTIFY: Notify = Notify::const_new();
    static ACTOR_PUBLICATION_BARRIER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static ACTOR_PUBLICATION_REACHED: AtomicBool = AtomicBool::new(false);
    static ACTOR_PUBLICATION_RELEASED: AtomicBool = AtomicBool::new(false);
    static ACTOR_PUBLICATION_NOTIFY: Notify = Notify::const_new();
    static ACTOR_RELEASE_NOTIFY: Notify = Notify::const_new();

    pub fn fail_actor_publication() {
        FAIL_ACTOR_PUBLICATION.store(true, Ordering::Release);
    }

    pub fn fail_root_install() {
        FAIL_ROOT_INSTALL.store(true, Ordering::Release);
    }

    pub fn fail_actor_intent_binding() {
        FAIL_ACTOR_INTENT_BIND.store(true, Ordering::Release);
    }

    pub fn take_actor_intent_binding_failure() -> bool {
        FAIL_ACTOR_INTENT_BIND.swap(false, Ordering::AcqRel)
    }

    pub fn take_root_install_failure() -> bool {
        FAIL_ROOT_INSTALL.swap(false, Ordering::AcqRel)
    }

    pub fn take_actor_publication_failure() -> bool {
        FAIL_ACTOR_PUBLICATION.swap(false, Ordering::AcqRel)
    }

    pub fn force_actor_commit_unknown() {
        FORCE_ACTOR_COMMIT_UNKNOWN.store(true, Ordering::Release);
    }

    pub fn take_actor_commit_unknown() -> bool {
        FORCE_ACTOR_COMMIT_UNKNOWN.swap(false, Ordering::AcqRel)
    }

    pub fn fail_actor_intent_removal() {
        FAIL_ACTOR_INTENT_REMOVAL.store(true, Ordering::Release);
    }

    pub fn take_actor_intent_removal_failure() -> bool {
        FAIL_ACTOR_INTENT_REMOVAL.swap(false, Ordering::AcqRel)
    }

    pub fn arm_cleanup_pre_delete_barrier() {
        CLEANUP_PRE_DELETE_BARRIER_ACTIVE.store(true, Ordering::Release);
        CLEANUP_PRE_DELETE_REACHED.store(false, Ordering::Release);
        CLEANUP_PRE_DELETE_RELEASED.store(false, Ordering::Release);
    }

    pub async fn before_cleanup_delete() {
        if !CLEANUP_PRE_DELETE_BARRIER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        CLEANUP_PRE_DELETE_REACHED.store(true, Ordering::Release);
        CLEANUP_PRE_DELETE_NOTIFY.notify_waiters();
        while !CLEANUP_PRE_DELETE_RELEASED.load(Ordering::Acquire) {
            CLEANUP_PRE_RELEASE_NOTIFY.notified().await;
        }
    }

    pub async fn wait_cleanup_pre_delete_barrier() {
        while !CLEANUP_PRE_DELETE_REACHED.load(Ordering::Acquire) {
            CLEANUP_PRE_DELETE_NOTIFY.notified().await;
        }
    }

    pub fn release_cleanup_pre_delete_barrier() {
        CLEANUP_PRE_DELETE_BARRIER_ACTIVE.store(false, Ordering::Release);
        CLEANUP_PRE_DELETE_RELEASED.store(true, Ordering::Release);
        CLEANUP_PRE_RELEASE_NOTIFY.notify_waiters();
    }

    pub fn arm_serialized_begin_probe() {
        SERIALIZED_BEGIN_PROBE_ACTIVE.store(true, Ordering::Release);
        SERIALIZED_BEGIN_PROBE_REACHED.store(false, Ordering::Release);
    }

    pub async fn wait_serialized_begin_probe() {
        while !SERIALIZED_BEGIN_PROBE_REACHED.load(Ordering::Acquire) {
            SERIALIZED_BEGIN_PROBE_NOTIFY.notified().await;
        }
    }

    pub fn notify_serialized_begin_probe() {
        if SERIALIZED_BEGIN_PROBE_ACTIVE.swap(false, Ordering::AcqRel) {
            SERIALIZED_BEGIN_PROBE_REACHED.store(true, Ordering::Release);
            SERIALIZED_BEGIN_PROBE_NOTIFY.notify_waiters();
        }
    }

    pub fn arm_cleanup_delete_barrier() {
        CLEANUP_DELETE_BARRIER_ACTIVE.store(true, Ordering::Release);
        CLEANUP_DELETE_REACHED.store(false, Ordering::Release);
        CLEANUP_DELETE_RELEASED.store(false, Ordering::Release);
    }

    pub async fn wait_cleanup_delete_barrier() {
        while !CLEANUP_DELETE_REACHED.load(Ordering::Acquire) {
            CLEANUP_DELETE_NOTIFY.notified().await;
        }
    }

    pub async fn after_cleanup_delete() {
        if !CLEANUP_DELETE_BARRIER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        CLEANUP_DELETE_REACHED.store(true, Ordering::Release);
        CLEANUP_DELETE_NOTIFY.notify_waiters();
        while !CLEANUP_DELETE_RELEASED.load(Ordering::Acquire) {
            CLEANUP_RELEASE_NOTIFY.notified().await;
        }
    }

    pub fn release_cleanup_delete_barrier() {
        CLEANUP_DELETE_BARRIER_ACTIVE.store(false, Ordering::Release);
        CLEANUP_DELETE_RELEASED.store(true, Ordering::Release);
        CLEANUP_RELEASE_NOTIFY.notify_waiters();
    }

    pub fn arm_actor_publication_barrier() {
        ACTOR_PUBLICATION_BARRIER_ACTIVE.store(true, Ordering::Release);
        ACTOR_PUBLICATION_REACHED.store(false, Ordering::Release);
        ACTOR_PUBLICATION_RELEASED.store(false, Ordering::Release);
    }

    pub async fn wait_actor_publication_barrier() {
        while !ACTOR_PUBLICATION_REACHED.load(Ordering::Acquire) {
            ACTOR_PUBLICATION_NOTIFY.notified().await;
        }
    }

    pub async fn before_actor_publication() {
        if !ACTOR_PUBLICATION_BARRIER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        ACTOR_PUBLICATION_REACHED.store(true, Ordering::Release);
        ACTOR_PUBLICATION_NOTIFY.notify_waiters();
        while !ACTOR_PUBLICATION_RELEASED.load(Ordering::Acquire) {
            ACTOR_RELEASE_NOTIFY.notified().await;
        }
    }

    pub fn release_actor_publication_barrier() {
        ACTOR_PUBLICATION_BARRIER_ACTIVE.store(false, Ordering::Release);
        ACTOR_PUBLICATION_RELEASED.store(true, Ordering::Release);
        ACTOR_RELEASE_NOTIFY.notify_waiters();
    }
}

/// The coarse, drawer-oriented grants understood by Cabinet.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Grant {
    ReadDrawer(DrawerId),
    WriteDrawer(DrawerId),
    ManageDrawer(DrawerId),
    CreateDocument(DrawerId),
    RemoveDocumentFromDrawer(DrawerId),
    AddChildDrawer(DrawerId),
    Delegate {
        drawer: DrawerId,
        grants: Vec<Grant>,
    },
}

impl Grant {
    fn drawer(&self) -> DrawerId {
        match self {
            Self::ReadDrawer(drawer)
            | Self::WriteDrawer(drawer)
            | Self::ManageDrawer(drawer)
            | Self::CreateDocument(drawer)
            | Self::RemoveDocumentFromDrawer(drawer)
            | Self::AddChildDrawer(drawer) => *drawer,
            Self::Delegate { drawer, .. } => *drawer,
        }
    }
}

/// A facet mutation presented at the actor-signing boundary.
///
/// `claim` is the actual Core logical claim: its six fields are canonicalized by
/// `FacetClaim::signing_bytes`. `operation` is the current capability context;
/// it is checked against the live authority registry before signing, but is not
/// part of the actor signature. The claim itself supplies the authenticated
/// mutation operation and target (document, facet key, UUID, value, and
/// predecessors), so there is no second caller-selected target to drift from.
/// Actor attribution proves the historical facet claim, not a current capability
/// or a node delegation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorClaim {
    pub operation: Grant,
    pub claim: FacetClaim,
}

impl ActorClaim {
    fn validate(&self) -> Result<(), AuthorityError> {
        if !matches!(self.operation, Grant::WriteDrawer(_)) {
            return Err(AuthorityError::Unauthorized);
        }
        // Validate the actual Core mutation, including Set/Remove and its CID,
        // before opening the secret handle. This prevents the authority boundary
        // from signing a malformed or ambiguous mutation context.
        self.claim.signing_bytes()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct UnsignedToken {
    version: u8,
    token_id: TokenId,
    subject: ActorPublicKey,
    grants: Vec<Grant>,
    parent: Option<TokenId>,
    issuer: ActorPublicKey,
    root: bool,
}

/// A node-signed, serializable capability that can cross an address-space
/// boundary. Its signature covers every field except `signature` itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilityToken {
    pub version: u8,
    pub token_id: TokenId,
    pub subject: ActorPublicKey,
    pub grants: Vec<Grant>,
    pub parent: Option<TokenId>,
    pub issuer: ActorPublicKey,
    /// A signed root marker. AuthorityRepo additionally checks the persisted
    /// root token ID before permitting current root operations.
    pub root: bool,
    pub signature: Signature,
}

impl CapabilityToken {
    fn unsigned(&self) -> UnsignedToken {
        UnsignedToken {
            version: self.version,
            token_id: self.token_id,
            subject: self.subject,
            grants: self.grants.clone(),
            parent: self.parent,
            issuer: self.issuer,
            root: self.root,
        }
    }

    fn payload(&self) -> Result<Vec<u8>, AuthorityError> {
        serde_json::to_vec(&self.unsigned()).map_err(AuthorityError::Encoding)
    }

    /// Verify the portable node signature without consulting local revocation.
    /// This is the historical cryptographic check used for signed claims.
    pub fn verify(&self, node_public_key: &ActorPublicKey) -> Result<(), AuthorityError> {
        if self.version != TOKEN_VERSION || self.issuer != *node_public_key {
            return Err(AuthorityError::InvalidToken);
        }
        let key =
            VerifyingKey::from_bytes(node_public_key).map_err(|_| AuthorityError::InvalidToken)?;
        verify_signature(&key, &self.payload()?, &self.signature)
    }
}

#[derive(Debug, Error)]
pub enum AuthorityError {
    #[error("authority token is invalid, unknown, revoked, or has been tampered with")]
    InvalidToken,
    #[error("capability does not authorize this operation")]
    Unauthorized,
    #[error("a delegated grant is not an attenuation of its parent")]
    Escalation,
    #[error("non-operator actors may only delegate through an explicit drawer delegation")]
    LateralDelegation,
    #[error("only the operator root capability may perform this operation")]
    OperatorOnly,
    #[error("the operator and node identities are reserved and cannot be delegated")]
    ReservedIdentity,
    #[error("an actor cannot delegate to itself")]
    SelfDelegation,
    #[error("actor has no signing key registered with AuthorityRepo")]
    ActorSignerMissing,
    #[error("persisted authority state is corrupt: {0}")]
    StateCorrupt(String),
    #[error("authority storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("authority storage filesystem operation failed: {0}")]
    StorageIo(#[from] std::io::Error),
    #[error("secret-store signing failed: {0}")]
    Secrets(#[from] SecretsError),
    #[error("authority commit outcome is unknown; reconcile before retrying: {0}")]
    CommitUnknown(String),
    #[error("actor provisioning requires reconciliation for {key_name}: {reason}")]
    ProvisioningUnresolved { key_name: String, reason: String },
    #[error("capability encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("facet claim is invalid: {0}")]
    FacetClaim(#[from] big_cabinet_core::dmeta::DmetaError),
}

#[derive(Clone)]
struct AuthorityStateStore {
    pool: SqlitePool,
}

struct ActorProvisionIntent {
    intent_id: [u8; 16],
    key_name: String,
    public_key: Option<ActorPublicKey>,
    preexisting: bool,
    cleanup_pending: bool,
}

impl AuthorityStateStore {
    async fn boot(path: impl AsRef<Path>) -> Result<Self, AuthorityError> {
        let path = path.as_ref();
        if path != Path::new(":memory:")
            && let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_lock (
                 lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1)
               , lock_nonce INTEGER NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query("INSERT OR IGNORE INTO authority_lock (lock_id, lock_nonce) VALUES (1, 0)")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_root (
                 root_id INTEGER PRIMARY KEY CHECK (root_id = 1)
               , token_json BLOB NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_actors (
                 public_key BLOB PRIMARY KEY
               , key_name TEXT NOT NULL UNIQUE
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_actor_intents (
                 intent_id BLOB PRIMARY KEY
               , key_name TEXT NOT NULL UNIQUE
               , public_key BLOB
               , preexisting INTEGER NOT NULL CHECK (preexisting IN (0, 1))
               , cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_pending IN (0, 1))
             )",
        )
        .execute(&pool)
        .await?;
        // Schema migration itself participates in the same SQLite write
        // reservation as authority operations. Two old-schema owners can
        // therefore not both observe a missing column and race ALTER TABLE.
        let mut schema_transaction = pool.begin().await?;
        sqlx::query("UPDATE authority_lock SET lock_nonce = lock_nonce + 1 WHERE lock_id = 1")
            .execute(&mut *schema_transaction)
            .await?;
        let actor_intent_columns = sqlx::query("PRAGMA table_info(authority_actor_intents)")
            .fetch_all(&mut *schema_transaction)
            .await?;
        if !actor_intent_columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .is_ok_and(|name| name == "cleanup_pending")
        }) {
            sqlx::query(
                "ALTER TABLE authority_actor_intents
                 ADD COLUMN cleanup_pending INTEGER NOT NULL DEFAULT 0
                    CHECK (cleanup_pending IN (0, 1))",
            )
            .execute(&mut *schema_transaction)
            .await?;
        }
        if let Err(error) = schema_transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "authority intent schema migration commit outcome is unknown: {error}"
            )));
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_boot_intents (
                 key_name TEXT PRIMARY KEY
               , public_key BLOB
               , preexisting INTEGER NOT NULL CHECK (preexisting IN (0, 1))
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_tokens (
                 token_id BLOB PRIMARY KEY
               , subject BLOB NOT NULL
               , parent_id BLOB
               , root INTEGER NOT NULL
               , token_json BLOB NOT NULL
               , payload_digest BLOB NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        // Subject uniqueness is an active-policy rule because a revoked actor
        // may be reissued a fresh capability. The serialized delegate
        // transaction enforces that rule without an index that cannot see the
        // separate revocation table.
        sqlx::query("DROP INDEX IF EXISTS authority_tokens_subject")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS authority_revocations (
                 token_id BLOB PRIMARY KEY
             )",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    async fn begin_serialized(&self) -> Result<Transaction<'_, Sqlite>, AuthorityError> {
        #[cfg(test)]
        provisioning_test_faults::notify_serialized_begin_probe();
        let mut transaction = self.pool.begin().await?;
        // The first write takes SQLite's database-wide write reservation. Every
        // current issuance/signing/revocation operation holds it until commit,
        // establishing one cross-process linearization point.
        sqlx::query("UPDATE authority_lock SET lock_nonce = lock_nonce + 1 WHERE lock_id = 1")
            .execute(&mut *transaction)
            .await?;
        Ok(transaction)
    }

    async fn root_tx(
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Option<CapabilityToken>, AuthorityError> {
        let row = sqlx::query("SELECT token_json FROM authority_root WHERE root_id = 1")
            .fetch_optional(&mut **transaction)
            .await?;
        row.map(|row| {
            let bytes: Vec<u8> = row.try_get("token_json")?;
            serde_json::from_slice(&bytes)
                .map_err(|error| AuthorityError::StateCorrupt(error.to_string()))
        })
        .transpose()
    }

    async fn install_root_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        candidate: &CapabilityToken,
    ) -> Result<CapabilityToken, AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_root_install_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected root publication failure".into(),
            )));
        }
        let bytes = serde_json::to_vec(candidate)?;
        sqlx::query(
            "INSERT OR IGNORE INTO authority_root (root_id, token_json)
             VALUES (1, ?1)",
        )
        .bind(bytes)
        .execute(&mut **transaction)
        .await?;
        let row = sqlx::query("SELECT token_json FROM authority_root WHERE root_id = 1")
            .fetch_one(&mut **transaction)
            .await?;
        let bytes: Vec<u8> = row.try_get("token_json")?;
        serde_json::from_slice(&bytes)
            .map_err(|error| AuthorityError::StateCorrupt(error.to_string()))
    }

    async fn actor_key_name_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        public_key: &ActorPublicKey,
    ) -> Result<Option<String>, AuthorityError> {
        let row = sqlx::query("SELECT key_name FROM authority_actors WHERE public_key = ?1")
            .bind(public_key.as_slice())
            .fetch_optional(&mut **transaction)
            .await?;
        row.map(|row| row.try_get("key_name"))
            .transpose()
            .map_err(Into::into)
    }

    async fn create_boot_intents(
        &self,
        node_preexisting: bool,
        operator_preexisting: bool,
    ) -> Result<(), AuthorityError> {
        let mut transaction = self.begin_serialized().await?;
        for (key_name, preexisting) in [
            (NODE_KEY, node_preexisting),
            (OPERATOR_KEY, operator_preexisting),
        ] {
            sqlx::query(
                "INSERT OR IGNORE INTO authority_boot_intents
                 (key_name, public_key, preexisting) VALUES (?1, NULL, ?2)",
            )
            .bind(key_name)
            .bind(i64::from(preexisting))
            .execute(&mut *transaction)
            .await?;
        }
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "boot provisioning intent commit outcome is unknown: {error}"
            )));
        }
        Ok(())
    }

    async fn insert_boot_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
        preexisting: bool,
    ) -> Result<(), AuthorityError> {
        sqlx::query(
            "INSERT OR IGNORE INTO authority_boot_intents
             (key_name, public_key, preexisting) VALUES (?1, NULL, ?2)",
        )
        .bind(key_name)
        .bind(i64::from(preexisting))
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn boot_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
    ) -> Result<Option<(Option<ActorPublicKey>, bool)>, AuthorityError> {
        let row = sqlx::query(
            "SELECT public_key, preexisting FROM authority_boot_intents
              WHERE key_name = ?1",
        )
        .bind(key_name)
        .fetch_optional(&mut **transaction)
        .await?;
        row.map(|row| {
            let public_key = row
                .try_get::<Option<Vec<u8>>, _>("public_key")?
                .map(fixed_bytes)
                .transpose()?;
            let preexisting: i64 = row.try_get("preexisting")?;
            if preexisting != 0 && preexisting != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "boot provisioning intent has invalid preexisting flag".into(),
                ));
            }
            Ok((public_key, preexisting != 0))
        })
        .transpose()
    }

    async fn reconcile_boot_intents(
        &self,
        store: &Arc<SecretStore>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), AuthorityError> {
        for key_name in [NODE_KEY, OPERATOR_KEY] {
            let Some((expected, _preexisting)) =
                Self::boot_intent_tx(transaction, key_name).await?
            else {
                continue;
            };
            let signer =
                match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, key_name).await {
                    Ok(signer) => signer,
                    Err(SecretsError::Missing(_)) => {
                        // Absence is not proof that the creator stopped. Retain
                        // the durable intent for a live or future owner.
                        continue;
                    }
                    Err(error) => {
                        return Err(AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.into(),
                            reason: format!("boot key read outcome is unknown: {error}"),
                        });
                    }
                };
            let public_key = signer.public_key().await.map_err(|error| {
                AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.into(),
                    reason: format!("boot public-key read outcome is unknown: {error}"),
                }
            })?;
            if expected.is_some_and(|expected| expected != public_key) {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.into(),
                    reason: "boot key disagrees with durable intent".into(),
                });
            }
            // A fixed identity is canonical and may be adopted by the next
            // serialized boot after exact public-key reading; no key is deleted.
            sqlx::query("DELETE FROM authority_boot_intents WHERE key_name = ?1")
                .bind(key_name)
                .execute(&mut **transaction)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.into(),
                    reason: format!("boot intent reconciliation is unresolved: {error}"),
                })?;
        }
        Ok(())
    }

    async fn bind_boot_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
        public_key: &ActorPublicKey,
    ) -> Result<(), AuthorityError> {
        let affected = sqlx::query(
            "UPDATE authority_boot_intents SET public_key = ?1
              WHERE key_name = ?2 AND public_key IS NULL",
        )
        .bind(public_key.as_slice())
        .bind(key_name)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(AuthorityError::StateCorrupt(
                "boot intent disappeared or was rebound".into(),
            ));
        }
        Ok(())
    }

    async fn create_actor_intent(
        &self,
        key_name: &str,
        preexisting: bool,
    ) -> Result<[u8; 16], AuthorityError> {
        let mut intent_id = [0_u8; 16];
        rand::rng().fill_bytes(&mut intent_id);
        let mut transaction = self.begin_serialized().await?;
        sqlx::query(
            "INSERT INTO authority_actor_intents
             (intent_id, key_name, public_key, preexisting, cleanup_pending)
             VALUES (?1, ?2, NULL, ?3, 0)",
        )
        .bind(intent_id.as_slice())
        .bind(key_name)
        .bind(i64::from(preexisting))
        .execute(&mut *transaction)
        .await?;
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "actor provisioning intent commit outcome is unknown for {key_name}: {error}"
            )));
        }
        Ok(intent_id)
    }

    async fn actor_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent_id: &[u8; 16],
        key_name: &str,
    ) -> Result<Option<ActorProvisionIntent>, AuthorityError> {
        let row = sqlx::query(
            "SELECT intent_id, key_name, public_key, preexisting, cleanup_pending
               FROM authority_actor_intents
              WHERE intent_id = ?1 AND key_name = ?2",
        )
        .bind(intent_id.as_slice())
        .bind(key_name)
        .fetch_optional(&mut **transaction)
        .await?;
        row.map(|row| {
            let public_key = row
                .try_get::<Option<Vec<u8>>, _>("public_key")?
                .map(fixed_bytes)
                .transpose()?;
            let preexisting: i64 = row.try_get("preexisting")?;
            if preexisting != 0 && preexisting != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "actor provisioning intent has invalid preexisting flag".into(),
                ));
            }
            let cleanup_pending: i64 = row.try_get("cleanup_pending")?;
            if cleanup_pending != 0 && cleanup_pending != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "actor provisioning intent has invalid cleanup phase".into(),
                ));
            }
            Ok(ActorProvisionIntent {
                intent_id: fixed_bytes(row.try_get("intent_id")?)?,
                key_name: row.try_get("key_name")?,
                public_key,
                preexisting: preexisting != 0,
                cleanup_pending: cleanup_pending != 0,
            })
        })
        .transpose()
    }

    async fn actor_intents_tx(
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<ActorProvisionIntent>, AuthorityError> {
        let rows = sqlx::query(
            "SELECT intent_id, key_name, public_key, preexisting, cleanup_pending
               FROM authority_actor_intents ORDER BY key_name",
        )
        .fetch_all(&mut **transaction)
        .await?;
        rows.into_iter()
            .map(|row| {
                let public_key = row
                    .try_get::<Option<Vec<u8>>, _>("public_key")?
                    .map(fixed_bytes)
                    .transpose()?;
                let preexisting: i64 = row.try_get("preexisting")?;
                if preexisting != 0 && preexisting != 1 {
                    return Err(AuthorityError::StateCorrupt(
                        "actor provisioning intent has invalid preexisting flag".into(),
                    ));
                }
                let cleanup_pending: i64 = row.try_get("cleanup_pending")?;
                if cleanup_pending != 0 && cleanup_pending != 1 {
                    return Err(AuthorityError::StateCorrupt(
                        "actor provisioning intent has invalid cleanup phase".into(),
                    ));
                }
                Ok(ActorProvisionIntent {
                    intent_id: fixed_bytes(row.try_get("intent_id")?)?,
                    key_name: row.try_get("key_name")?,
                    public_key,
                    preexisting: preexisting != 0,
                    cleanup_pending: cleanup_pending != 0,
                })
            })
            .collect()
    }

    async fn set_actor_intent_public_key_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
        public_key: &ActorPublicKey,
    ) -> Result<(), AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_actor_intent_binding_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected actor intent binding failure".into(),
            )));
        }
        let affected = sqlx::query(
            "UPDATE authority_actor_intents SET public_key = ?1
              WHERE intent_id = ?2 AND key_name = ?3 AND public_key IS NULL",
        )
        .bind(public_key.as_slice())
        .bind(intent.intent_id.as_slice())
        .bind(&intent.key_name)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(AuthorityError::StateCorrupt(
                "actor provisioning intent changed before public-key binding".into(),
            ));
        }
        Ok(())
    }

    async fn mark_actor_cleanup_pending_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
        public_key: &ActorPublicKey,
    ) -> Result<(), AuthorityError> {
        let affected = sqlx::query(
            "UPDATE authority_actor_intents
                SET public_key = ?1, cleanup_pending = 1
              WHERE intent_id = ?2 AND key_name = ?3
                AND cleanup_pending = 0
                AND (public_key IS NULL OR public_key = ?1)",
        )
        .bind(public_key.as_slice())
        .bind(intent.intent_id.as_slice())
        .bind(&intent.key_name)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(AuthorityError::StateCorrupt(
                "actor intent could not enter durable cleanup phase".into(),
            ));
        }
        Ok(())
    }

    async fn insert_actor_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        public_key: &ActorPublicKey,
        key_name: &str,
    ) -> Result<(), AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_actor_publication_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected actor publication failure".into(),
            )));
        }
        sqlx::query(
            "INSERT INTO authority_actors (public_key, key_name)
             VALUES (?1, ?2)",
        )
        .bind(public_key.as_slice())
        .bind(key_name)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn actor_binding_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        key_name: &str,
    ) -> Result<Option<ActorPublicKey>, AuthorityError> {
        let row = sqlx::query("SELECT public_key FROM authority_actors WHERE key_name = ?1")
            .bind(key_name)
            .fetch_optional(&mut **transaction)
            .await?;
        row.map(|row| fixed_bytes(row.try_get("public_key")?))
            .transpose()
    }

    async fn remove_actor_intent_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
    ) -> Result<(), AuthorityError> {
        #[cfg(test)]
        if provisioning_test_faults::take_actor_intent_removal_failure() {
            return Err(AuthorityError::Storage(sqlx::Error::Protocol(
                "injected actor intent removal failure".into(),
            )));
        }
        sqlx::query(
            "DELETE FROM authority_actor_intents
              WHERE intent_id = ?1 AND key_name = ?2",
        )
        .bind(intent.intent_id.as_slice())
        .bind(&intent.key_name)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn insert_token_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        token: &CapabilityToken,
    ) -> Result<(), AuthorityError> {
        let token_json = serde_json::to_vec(token)?;
        let payload_digest = token_digest(token)?;
        sqlx::query(
            "INSERT OR IGNORE INTO authority_tokens
             (token_id, subject, parent_id, root, token_json, payload_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(token.token_id.as_slice())
        .bind(token.subject.as_slice())
        .bind(token.parent.map(|parent| parent.to_vec()))
        .bind(i64::from(token.root))
        .bind(&token_json)
        .bind(payload_digest.as_slice())
        .execute(&mut **transaction)
        .await?;
        let Some(row) = sqlx::query(
            "SELECT subject, parent_id, root, token_json, payload_digest
             FROM authority_tokens WHERE token_id = ?1",
        )
        .bind(token.token_id.as_slice())
        .fetch_optional(&mut **transaction)
        .await?
        else {
            return Err(AuthorityError::StateCorrupt(
                "authority token insert disappeared".into(),
            ));
        };
        let subject = fixed_bytes::<32>(row.try_get("subject")?)?;
        let parent = optional_fixed_bytes::<16>(row.try_get("parent_id")?)?;
        let root: i64 = row.try_get("root")?;
        let stored_json: Vec<u8> = row.try_get("token_json")?;
        let stored_digest: Vec<u8> = row.try_get("payload_digest")?;
        if subject != token.subject
            || parent != token.parent
            || (root != 0) != token.root
            || stored_json != token_json
            || stored_digest != payload_digest
        {
            return Err(AuthorityError::StateCorrupt(
                "authority token ID collision or immutable issuance mismatch".into(),
            ));
        }
        Ok(())
    }

    async fn token_record_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        token_id: &TokenId,
    ) -> Result<Option<(ActorPublicKey, Option<TokenId>, bool)>, AuthorityError> {
        let row = sqlx::query(
            "SELECT subject, parent_id, root FROM authority_tokens
             WHERE token_id = ?1",
        )
        .bind(token_id.as_slice())
        .fetch_optional(&mut **transaction)
        .await?;
        row.map(|row| {
            let subject = fixed_bytes::<32>(row.try_get("subject")?)?;
            let parent = optional_fixed_bytes::<16>(row.try_get("parent_id")?)?;
            let root: i64 = row.try_get("root")?;
            Ok((subject, parent, root != 0))
        })
        .transpose()
    }

    async fn token_is_exact(&self, token: &CapabilityToken) -> Result<bool, AuthorityError> {
        let row = sqlx::query(
            "SELECT token_json, payload_digest FROM authority_tokens WHERE token_id = ?1",
        )
        .bind(token.token_id.as_slice())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let stored_json: Vec<u8> = row.try_get("token_json")?;
        let stored_digest: Vec<u8> = row.try_get("payload_digest")?;
        Ok(stored_json == serde_json::to_vec(token)?
            && stored_digest.as_slice() == token_digest(token)?.as_slice())
    }

    async fn is_revoked(&self, token_id: &TokenId) -> Result<bool, AuthorityError> {
        Ok(
            sqlx::query("SELECT 1 FROM authority_revocations WHERE token_id = ?1")
                .bind(token_id.as_slice())
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }
}

/// The Cabinet actor/capability boundary.
///
/// The SQLite state path is the authority owner boundary. Every current
/// verification queries that store rather than trusting a process-local cache;
/// private signing keys remain named handles inside `secrets_rs`.
#[derive(Clone)]
pub struct AuthorityRepo {
    store: Arc<SecretStore>,
    state: AuthorityStateStore,
    node_signer: SigningKeyHandle,
    node_public_key: ActorPublicKey,
    operator_public_key: ActorPublicKey,
    root_token_id: TokenId,
}

impl AuthorityRepo {
    async fn reconcile_cleanup_intent(
        store: &Arc<SecretStore>,
        transaction: &mut Transaction<'_, Sqlite>,
        intent: &ActorProvisionIntent,
    ) -> Result<(), AuthorityError> {
        let Some(expected) = intent.public_key else {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: "cleanup phase has no exact public-key binding".into(),
            });
        };
        if let Some(binding) = AuthorityStateStore::actor_binding_tx(transaction, &intent.key_name)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: format!("cleanup registry recheck is unresolved: {error}"),
            })?
        {
            if binding != expected {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: "cleanup phase found a conflicting registry public key".into(),
                });
            }
            AuthorityStateStore::remove_actor_intent_tx(transaction, intent)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("cleanup intent removal is unresolved: {error}"),
                })?;
            return Ok(());
        }
        let signer =
            match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, &intent.key_name).await {
                Ok(signer) => signer,
                Err(SecretsError::Missing(_)) => {
                    AuthorityStateStore::remove_actor_intent_tx(transaction, intent)
                        .await
                        .map_err(|error| AuthorityError::ProvisioningUnresolved {
                            key_name: intent.key_name.clone(),
                            reason: format!("cleanup intent removal is unresolved: {error}"),
                        })?;
                    return Ok(());
                }
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name.clone(),
                        reason: format!("cleanup key read outcome is unknown: {error}"),
                    });
                }
            };
        let actual =
            signer
                .public_key()
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("cleanup public-key read outcome is unknown: {error}"),
                })?;
        if actual != expected {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: "cleanup keyring public key disagrees with exact intent".into(),
            });
        }
        #[cfg(test)]
        provisioning_test_faults::before_cleanup_delete().await;
        store
            .delete_secret(NODE_SERVICE, &intent.key_name)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: format!("cleanup deletion outcome is unresolved: {error}"),
            })?;
        #[cfg(test)]
        provisioning_test_faults::after_cleanup_delete().await;
        AuthorityStateStore::remove_actor_intent_tx(transaction, intent)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: intent.key_name.clone(),
                reason: format!("cleanup intent removal is unresolved: {error}"),
            })
    }

    async fn reconcile_actor_intents(
        store: &Arc<SecretStore>,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), AuthorityError> {
        for intent in AuthorityStateStore::actor_intents_tx(transaction).await? {
            if intent.cleanup_pending {
                Self::reconcile_cleanup_intent(store, transaction, &intent).await?;
                continue;
            }
            let binding =
                AuthorityStateStore::actor_binding_tx(transaction, &intent.key_name).await?;
            if let Some(binding) = binding {
                if intent
                    .public_key
                    .is_some_and(|expected| expected != binding)
                {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name,
                        reason: "registry public key disagrees with provisioning intent".into(),
                    });
                }
                let signer =
                    match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, &intent.key_name)
                        .await
                    {
                        Ok(signer) => signer,
                        Err(error) => {
                            return Err(AuthorityError::ProvisioningUnresolved {
                                key_name: intent.key_name,
                                reason: format!("registered signer cannot be opened: {error}"),
                            });
                        }
                    };
                let actual = signer.public_key().await.map_err(|error| {
                    AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name.clone(),
                        reason: format!(
                            "registered signer public-key read outcome is unknown: {error}"
                        ),
                    }
                })?;
                if actual != binding {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name,
                        reason: "registered signer public key changed".into(),
                    });
                }
                AuthorityStateStore::remove_actor_intent_tx(transaction, &intent)
                    .await
                    .map_err(|error| AuthorityError::ProvisioningUnresolved {
                        key_name: intent.key_name.clone(),
                        reason: format!("actor intent removal is unresolved: {error}"),
                    })?;
                continue;
            }
            let signer =
                match SigningKeyHandle::open(Arc::clone(store), NODE_SERVICE, &intent.key_name)
                    .await
                {
                    Ok(signer) => signer,
                    Err(SecretsError::Missing(_)) => continue,
                    Err(error) => {
                        return Err(AuthorityError::ProvisioningUnresolved {
                            key_name: intent.key_name,
                            reason: format!("key ownership/read outcome is unknown: {error}"),
                        });
                    }
                };
            let public_key = signer.public_key().await.map_err(|error| {
                AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("public-key read outcome is unknown: {error}"),
                }
            })?;
            if intent.preexisting {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name,
                    reason: "pre-existing key has no exact actor registry binding".into(),
                });
            }
            if intent
                .public_key
                .is_some_and(|expected| expected != public_key)
            {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name,
                    reason: "keyring public key disagrees with provisioning intent".into(),
                });
            }
            if intent.public_key.is_none() {
                AuthorityStateStore::set_actor_intent_public_key_tx(
                    transaction,
                    &intent,
                    &public_key,
                )
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("recovered actor intent binding is unresolved: {error}"),
                })?;
            }
            AuthorityStateStore::insert_actor_tx(transaction, &public_key, &intent.key_name)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("actor registry publication is unresolved: {error}"),
                })?;
            AuthorityStateStore::remove_actor_intent_tx(transaction, &intent)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: intent.key_name.clone(),
                    reason: format!("actor intent removal is unresolved: {error}"),
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn reconcile_pending(&self) -> Result<(), AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        Self::reconcile_actor_intents(&self.store, &mut transaction).await?;
        if let Err(error) = transaction.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "pending actor reconciliation commit outcome is unknown: {error}"
            )));
        }
        Ok(())
    }

    /// Boot against a durable authority state path and return the stable
    /// operator root capability. Multiple handles/processes may open the same
    /// path; current authorization is read from the shared SQLite owner.
    pub async fn boot(
        store: Arc<SecretStore>,
        state_path: impl AsRef<Path>,
    ) -> Result<(Self, CapabilityToken), AuthorityError> {
        let state = AuthorityStateStore::boot(state_path).await?;
        // First reconcile only durable work from earlier owners. This lock is
        // committed before preparing this boot's own intents, so a creator that
        // loses the prepare/reacquisition race must revalidate before key use.
        let mut reconciliation = state.begin_serialized().await?;
        Self::reconcile_actor_intents(&store, &mut reconciliation).await?;
        state
            .reconcile_boot_intents(&store, &mut reconciliation)
            .await?;
        if let Err(error) = reconciliation.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "provisioning reconciliation commit outcome is unknown: {error}"
            )));
        }

        let node_preexisting = store.get_secret(NODE_SERVICE, NODE_KEY).await?.is_some();
        let operator_preexisting = store
            .get_secret(NODE_SERVICE, OPERATOR_KEY)
            .await?
            .is_some();
        state
            .create_boot_intents(node_preexisting, operator_preexisting)
            .await?;

        let mut bootstrap = state.begin_serialized().await?;
        let (node_expected, node_preexisting) = if let Some(intent) =
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, NODE_KEY).await?
        {
            intent
        } else {
            let preexisting = store.get_secret(NODE_SERVICE, NODE_KEY).await?.is_some();
            AuthorityStateStore::insert_boot_intent_tx(&mut bootstrap, NODE_KEY, preexisting)
                .await?;
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, NODE_KEY)
                .await?
                .expect("boot intent inserted under the write reservation")
        };
        let (operator_expected, operator_preexisting) = if let Some(intent) =
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, OPERATOR_KEY).await?
        {
            intent
        } else {
            let preexisting = store
                .get_secret(NODE_SERVICE, OPERATOR_KEY)
                .await?
                .is_some();
            AuthorityStateStore::insert_boot_intent_tx(&mut bootstrap, OPERATOR_KEY, preexisting)
                .await?;
            AuthorityStateStore::boot_intent_tx(&mut bootstrap, OPERATOR_KEY)
                .await?
                .expect("boot intent inserted under the write reservation")
        };
        if node_preexisting && store.get_secret(NODE_SERVICE, NODE_KEY).await?.is_none() {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: "pre-existing node key disappeared across intent reacquisition".into(),
            });
        }
        if operator_preexisting
            && store
                .get_secret(NODE_SERVICE, OPERATOR_KEY)
                .await?
                .is_none()
        {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: "pre-existing operator key disappeared across intent reacquisition".into(),
            });
        }
        let node_signer =
            match SigningKeyHandle::get_or_create(Arc::clone(&store), NODE_SERVICE, NODE_KEY).await
            {
                Ok(signer) => signer,
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: NODE_KEY.into(),
                        reason: format!("node key creation/read outcome is unknown: {error}"),
                    });
                }
            };
        let node_public_key = node_signer.public_key().await.map_err(|error| {
            AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: format!("node public-key read outcome is unknown: {error}"),
            }
        })?;
        if node_expected.is_some_and(|expected| expected != node_public_key) {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: "node key disagrees with durable boot intent".into(),
            });
        }
        if node_expected.is_none() {
            AuthorityStateStore::bind_boot_intent_tx(&mut bootstrap, NODE_KEY, &node_public_key)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: NODE_KEY.into(),
                    reason: format!("node boot intent binding is unresolved: {error}"),
                })?;
        }
        let node_signer = node_signer.bind_public_key(node_public_key);
        let operator_signer =
            match SigningKeyHandle::get_or_create(Arc::clone(&store), NODE_SERVICE, OPERATOR_KEY)
                .await
            {
                Ok(signer) => signer,
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name: OPERATOR_KEY.into(),
                        reason: format!("operator key creation/read outcome is unknown: {error}"),
                    });
                }
            };
        let operator_public_key = operator_signer.public_key().await.map_err(|error| {
            AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: format!("operator public-key read outcome is unknown: {error}"),
            }
        })?;
        if operator_expected.is_some_and(|expected| expected != operator_public_key) {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: "operator key disagrees with durable boot intent".into(),
            });
        }
        if operator_expected.is_none() {
            AuthorityStateStore::bind_boot_intent_tx(
                &mut bootstrap,
                OPERATOR_KEY,
                &operator_public_key,
            )
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: OPERATOR_KEY.into(),
                reason: format!("operator boot intent binding is unresolved: {error}"),
            })?;
        }
        let root = if let Some(root) =
            AuthorityStateStore::root_tx(&mut bootstrap)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: NODE_KEY.into(),
                    reason: format!("root read outcome is unresolved: {error}"),
                })? {
            validate_root(&root, &node_public_key, &operator_public_key)?;
            root
        } else {
            let candidate = sign_token(
                &node_signer,
                node_public_key,
                operator_public_key,
                root_grants(),
                None,
                true,
            )
            .await?;
            let root = AuthorityStateStore::install_root_tx(&mut bootstrap, &candidate)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: NODE_KEY.into(),
                    reason: format!("root publication is unresolved: {error}"),
                })?;
            validate_root(&root, &node_public_key, &operator_public_key)?;
            root
        };
        if AuthorityStateStore::token_record_tx(&mut bootstrap, &root.token_id)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: format!("root token read outcome is unresolved: {error}"),
            })?
            .is_none()
        {
            state
                .insert_token_tx(&mut bootstrap, &root)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: NODE_KEY.into(),
                    reason: format!("root token publication is unresolved: {error}"),
                })?;
        }
        sqlx::query("DELETE FROM authority_boot_intents WHERE key_name IN (?1, ?2)")
            .bind(NODE_KEY)
            .bind(OPERATOR_KEY)
            .execute(&mut *bootstrap)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: NODE_KEY.into(),
                reason: format!("final boot intent cleanup is unresolved: {error}"),
            })?;
        if let Err(error) = bootstrap.commit().await {
            return Err(AuthorityError::CommitUnknown(format!(
                "boot initialization for root {}: {error}",
                hex(&root.token_id)
            )));
        }
        let repo = Self {
            store,
            state,
            node_signer,
            node_public_key,
            operator_public_key,
            root_token_id: root.token_id,
        };
        Ok((repo, root))
    }

    pub fn node_public_key(&self) -> ActorPublicKey {
        self.node_public_key
    }

    pub fn operator_public_key(&self) -> ActorPublicKey {
        self.operator_public_key
    }

    /// Create and durably register an actor key. The private key remains in
    /// `secrets_rs`; only its public identity crosses this API.
    ///
    /// The caller owns cancellation. A durable provisioning intent is published
    /// before key creation, and unfinished intents are reconciled by the next
    /// serialized owner without deleting another live creator's key.
    pub async fn create_actor(&self) -> Result<ActorPublicKey, AuthorityError> {
        let mut name_bytes = [0_u8; 16];
        rand::rng().fill_bytes(&mut name_bytes);
        let key_name = format!("actor-{}", hex(&name_bytes));
        let preexisting = self
            .store
            .get_secret(NODE_SERVICE, &key_name)
            .await?
            .is_some();
        let intent_id = self
            .state
            .create_actor_intent(&key_name, preexisting)
            .await?;
        let mut transaction = match self.state.begin_serialized().await {
            Ok(transaction) => transaction,
            Err(error) => {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: format!("could not reacquire intent reservation: {error}"),
                });
            }
        };
        let Some(intent) =
            AuthorityStateStore::actor_intent_tx(&mut transaction, &intent_id, &key_name).await?
        else {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "provisioning intent was reconciled before creator reacquired it".into(),
            });
        };
        if intent.preexisting {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "random actor key name was already occupied before intent publication"
                    .into(),
            });
        }
        if self
            .store
            .get_secret(NODE_SERVICE, &key_name)
            .await?
            .is_some()
        {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "random actor key appeared before creator reacquired intent".into(),
            });
        }
        let signer =
            match SigningKeyHandle::get_or_create(Arc::clone(&self.store), NODE_SERVICE, &key_name)
                .await
            {
                Ok(signer) => signer,
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: format!("key creation/read outcome is unknown: {error}"),
                    });
                }
            };
        let public_key = match signer.public_key().await {
            Ok(public_key) => public_key,
            Err(error) => {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: format!("public-key read outcome is unknown: {error}"),
                });
            }
        };
        if public_key == self.node_public_key || public_key == self.operator_public_key {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "provisioned key has a reserved authority identity".into(),
            });
        }
        if intent
            .public_key
            .is_some_and(|expected| expected != public_key)
        {
            return Err(AuthorityError::ProvisioningUnresolved {
                key_name,
                reason: "keyring public key disagrees with durable intent".into(),
            });
        }
        if intent.public_key.is_none() {
            AuthorityStateStore::set_actor_intent_public_key_tx(
                &mut transaction,
                &intent,
                &public_key,
            )
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: key_name.clone(),
                reason: format!("actor intent binding is unresolved: {error}"),
            })?;
        }
        #[cfg(test)]
        provisioning_test_faults::before_actor_publication().await;
        if let Some(binding) =
            AuthorityStateStore::actor_binding_tx(&mut transaction, &key_name).await?
        {
            if binding != public_key {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "existing registry binding has a different public key".into(),
                });
            }
        } else if let Err(publication_error) =
            AuthorityStateStore::insert_actor_tx(&mut transaction, &public_key, &key_name).await
        {
            // First make the compensation phase durable. The external delete
            // must never precede this commit: cancellation or shutdown after
            // deletion can then be recovered from the pending intent.
            let owned_by_registry =
                AuthorityStateStore::actor_key_name_tx(&mut transaction, &public_key)
                    .await
                    .map_err(|error| AuthorityError::ProvisioningUnresolved {
                        key_name: key_name.clone(),
                        reason: format!("actor publication recheck is unresolved: {error}"),
                    })?;
            if owned_by_registry.is_some() {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: format!("actor publication is unresolved: {publication_error}"),
                });
            }
            AuthorityStateStore::mark_actor_cleanup_pending_tx(
                &mut transaction,
                &intent,
                &public_key,
            )
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: key_name.clone(),
                reason: format!("could not durably enter cleanup phase: {error}"),
            })?;
            if let Err(commit_error) = transaction.commit().await {
                return Err(AuthorityError::CommitUnknown(format!(
                    "actor cleanup phase commit outcome is unknown for {key_name}: {commit_error}"
                )));
            }
            // Reacquire and keep the shared write reservation through the
            // exact recheck, external delete, and intent removal. A second
            // owner can therefore not publish or clean the same intent while
            // this owner is between phase commit and finish.
            let mut finish = self.state.begin_serialized().await.map_err(|error| {
                AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("cleanup reacquisition is unresolved: {error}"),
                }
            })?;
            let current =
                AuthorityStateStore::actor_intent_tx(&mut finish, &intent.intent_id, &key_name)
                    .await
                    .map_err(|error| AuthorityError::ProvisioningUnresolved {
                        key_name: key_name.clone(),
                        reason: format!("cleanup intent recheck is unresolved: {error}"),
                    })?;
            let Some(current) = current else {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "cleanup phase was already finalized by another owner".into(),
                });
            };
            if !current.cleanup_pending || current.public_key != Some(public_key) {
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "cleanup intent binding changed before deletion".into(),
                });
            }
            if let Some(binding) = AuthorityStateStore::actor_binding_tx(&mut finish, &key_name)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("cleanup registry recheck is unresolved: {error}"),
                })?
            {
                if binding == public_key {
                    AuthorityStateStore::remove_actor_intent_tx(&mut finish, &current)
                        .await
                        .map_err(|error| AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.clone(),
                            reason: format!("cleanup intent removal is unresolved: {error}"),
                        })?;
                    if let Err(error) = finish.commit().await {
                        return Err(AuthorityError::CommitUnknown(format!(
                            "cleanup publication recheck commit is unknown for {key_name}: {error}"
                        )));
                    }
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: "publication completed before cleanup owner recheck".into(),
                    });
                }
                return Err(AuthorityError::ProvisioningUnresolved {
                    key_name,
                    reason: "cleanup found a conflicting registry public key".into(),
                });
            }
            match SigningKeyHandle::open(Arc::clone(&self.store), NODE_SERVICE, &key_name).await {
                Ok(signer) => {
                    let actual = signer.public_key().await.map_err(|error| {
                        AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.clone(),
                            reason: format!("cleanup public-key read outcome is unknown: {error}"),
                        }
                    })?;
                    if actual != public_key {
                        return Err(AuthorityError::ProvisioningUnresolved {
                            key_name,
                            reason: "cleanup keyring public key disagrees with intent".into(),
                        });
                    }
                }
                Err(SecretsError::Missing(_)) => {
                    AuthorityStateStore::remove_actor_intent_tx(&mut finish, &current)
                        .await
                        .map_err(|error| AuthorityError::ProvisioningUnresolved {
                            key_name: key_name.clone(),
                            reason: format!("cleanup intent removal is unresolved: {error}"),
                        })?;
                    if let Err(error) = finish.commit().await {
                        return Err(AuthorityError::CommitUnknown(format!(
                            "cleanup missing-key finish commit is unknown for {key_name}: {error}"
                        )));
                    }
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: "cleanup was already completed by another owner".into(),
                    });
                }
                Err(error) => {
                    return Err(AuthorityError::ProvisioningUnresolved {
                        key_name,
                        reason: format!("cleanup key read outcome is unknown: {error}"),
                    });
                }
            }
            #[cfg(test)]
            provisioning_test_faults::before_cleanup_delete().await;
            self.store
                .delete_secret(NODE_SERVICE, &key_name)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("exact-key cleanup failed: {error}"),
                })?;
            #[cfg(test)]
            provisioning_test_faults::after_cleanup_delete().await;
            AuthorityStateStore::remove_actor_intent_tx(&mut finish, &current)
                .await
                .map_err(|error| AuthorityError::ProvisioningUnresolved {
                    key_name: key_name.clone(),
                    reason: format!("cleanup intent removal is unresolved: {error}"),
                })?;
            if let Err(error) = finish.commit().await {
                return Err(AuthorityError::CommitUnknown(format!(
                    "actor cleanup finish commit outcome is unknown for {key_name}: {error}"
                )));
            }
            return Err(publication_error);
        }
        AuthorityStateStore::remove_actor_intent_tx(&mut transaction, &intent)
            .await
            .map_err(|error| AuthorityError::ProvisioningUnresolved {
                key_name: key_name.clone(),
                reason: format!("actor intent removal is unresolved: {error}"),
            })?;
        let commit = transaction.commit().await;
        #[cfg(test)]
        if provisioning_test_faults::take_actor_commit_unknown() {
            return match commit {
                Ok(()) => Err(AuthorityError::CommitUnknown(format!(
                    "injected actor {key_name} publication commit outcome is unknown"
                ))),
                Err(error) => Err(AuthorityError::CommitUnknown(format!(
                    "actor {key_name} publication commit outcome is unknown: {error}"
                ))),
            };
        }
        match commit {
            Ok(()) => Ok(public_key),
            Err(error) => Err(AuthorityError::CommitUnknown(format!(
                "actor {key_name} publication commit outcome is unknown: {error}"
            ))),
        }
    }

    /// Verify a token for a new operation, including current revocation and
    /// its persisted ancestor chain. This never reloads the whole state.
    pub async fn verify(&self, token: &CapabilityToken) -> Result<(), AuthorityError> {
        let mut transaction = self.state.pool.begin().await?;
        let result = self.verify_in(&mut transaction, token).await;
        transaction.rollback().await?;
        result
    }

    async fn verify_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        token: &CapabilityToken,
    ) -> Result<(), AuthorityError> {
        token.verify(&self.node_public_key)?;
        let rows = sqlx::query(
            "WITH RECURSIVE chain(depth, token_id, path, cycle) AS (
                 SELECT 0, token_id, ',' || hex(token_id) || ',', 0
                   FROM authority_tokens WHERE token_id = ?1
                 UNION ALL
                 SELECT chain.depth + 1
                      , authority_tokens.parent_id
                      , chain.path || hex(authority_tokens.parent_id) || ','
                      , CASE WHEN instr(chain.path, ',' || hex(authority_tokens.parent_id) || ',') > 0
                             THEN 1 ELSE 0 END
                   FROM chain
                   JOIN authority_tokens ON authority_tokens.token_id = chain.token_id
                  WHERE authority_tokens.parent_id IS NOT NULL
                    AND chain.cycle = 0
             )
             SELECT chain.depth, chain.token_id, chain.cycle
                  , authority_tokens.subject
                  , authority_tokens.parent_id
                  , authority_tokens.root
                  , authority_revocations.token_id AS revoked
                  , authority_tokens.token_json
                  , authority_tokens.payload_digest
               FROM chain
               JOIN authority_tokens ON authority_tokens.token_id = chain.token_id
               LEFT JOIN authority_revocations
                 ON authority_revocations.token_id = chain.token_id
              ORDER BY chain.depth",
        )
        .bind(token.token_id.as_slice())
        .fetch_all(&mut **transaction)
        .await?;
        if rows.is_empty() {
            return Err(AuthorityError::InvalidToken);
        }
        let supplied_json = serde_json::to_vec(token)?;
        let supplied_digest = token_digest(token)?;
        let mut ids = Vec::with_capacity(rows.len());
        let mut parents = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let id = fixed_bytes::<16>(row.try_get("token_id")?)?;
            let cycle: i64 = row.try_get("cycle")?;
            if cycle != 0 || row.try_get::<Option<Vec<u8>>, _>("revoked")?.is_some() {
                return Err(AuthorityError::InvalidToken);
            }
            let sql_subject = fixed_bytes::<32>(row.try_get("subject")?)?;
            let sql_parent = optional_fixed_bytes::<16>(row.try_get("parent_id")?)?;
            let sql_root: i64 = row.try_get("root")?;
            if sql_root != 0 && sql_root != 1 {
                return Err(AuthorityError::StateCorrupt(
                    "authority token root flag is not boolean".into(),
                ));
            }
            let stored_json: Vec<u8> = row.try_get("token_json")?;
            let stored_digest: Vec<u8> = row.try_get("payload_digest")?;
            let persisted: CapabilityToken =
                serde_json::from_slice(&stored_json).map_err(|error| {
                    AuthorityError::StateCorrupt(format!(
                        "persisted authority token is invalid: {error}"
                    ))
                })?;
            // SQL parent/subject/root columns are denormalized indexes, not
            // authority. Authenticate the immutable token JSON for every row
            // before trusting those columns to describe the chain.
            if persisted.token_id != id
                || persisted.subject != sql_subject
                || persisted.parent != sql_parent
                || persisted.root != (sql_root != 0)
                || stored_digest.as_slice() != token_digest(&persisted)?.as_slice()
                || persisted.verify(&self.node_public_key).is_err()
            {
                return Err(AuthorityError::StateCorrupt(
                    "persisted authority token disagrees with authenticated issuance".into(),
                ));
            }
            if index == 0
                && (id != token.token_id
                    || stored_json != supplied_json
                    || stored_digest != supplied_digest)
            {
                return Err(AuthorityError::InvalidToken);
            }
            ids.push(id);
            parents.push(sql_parent);
        }
        for (index, parent) in parents.iter().enumerate() {
            if let Some(parent) = *parent {
                if ids.get(index + 1).copied() != Some(parent) {
                    return Err(AuthorityError::InvalidToken);
                }
            } else if index + 1 != ids.len() {
                return Err(AuthorityError::InvalidToken);
            }
        }
        Ok(())
    }

    /// Issue an explicitly downward capability to a registered actor.
    pub async fn delegate(
        &self,
        parent: &CapabilityToken,
        subject: ActorPublicKey,
        grants: Vec<Grant>,
    ) -> Result<CapabilityToken, AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        self.verify_in(&mut transaction, parent).await?;
        if subject == self.node_public_key || subject == self.operator_public_key {
            return Err(AuthorityError::ReservedIdentity);
        }
        if subject == parent.subject {
            return Err(AuthorityError::SelfDelegation);
        }
        let Some(key_name) =
            AuthorityStateStore::actor_key_name_tx(&mut transaction, &subject).await?
        else {
            return Err(AuthorityError::ActorSignerMissing);
        };
        let operator = self.is_operator_root(parent);
        if !operator
            && sqlx::query(
                "SELECT 1
                   FROM authority_tokens
                   LEFT JOIN authority_revocations
                     ON authority_revocations.token_id = authority_tokens.token_id
                  WHERE authority_tokens.subject = ?1
                    AND authority_revocations.token_id IS NULL
                  LIMIT 1",
            )
            .bind(subject.as_slice())
            .fetch_optional(&mut *transaction)
            .await?
            .is_some()
        {
            return Err(AuthorityError::LateralDelegation);
        }
        if !operator {
            for grant in &grants {
                if !parent.grants.iter().any(|owned| owned == grant) {
                    return Err(AuthorityError::Escalation);
                }
                let drawer = grant.drawer();
                let delegated = parent.grants.iter().any(|owned| {
                    matches!(owned, Grant::Delegate { drawer: owned_drawer, grants } if *owned_drawer == drawer && grants.iter().any(|allowed| allowed == grant))
                });
                if !delegated {
                    return Err(AuthorityError::LateralDelegation);
                }
            }
        }
        let signer =
            SigningKeyHandle::open(Arc::clone(&self.store), NODE_SERVICE, &key_name).await?;
        if signer.public_key().await? != subject {
            return Err(AuthorityError::StateCorrupt(
                "actor registry signer changed identity".into(),
            ));
        }
        let token = sign_token(
            &self.node_signer,
            self.node_public_key,
            subject,
            grants,
            Some(parent.token_id),
            false,
        )
        .await?;
        self.state.insert_token_tx(&mut transaction, &token).await?;
        match transaction.commit().await {
            Ok(()) => Ok(token),
            Err(error) => match self.state.token_is_exact(&token).await {
                Ok(true) => Ok(token),
                Ok(false) => Err(AuthorityError::Storage(error)),
                Err(recheck) => Err(AuthorityError::CommitUnknown(format!(
                    "token {} issuance commit failed ({error}); registry recheck failed: {recheck}",
                    hex(&token.token_id)
                ))),
            },
        }
    }

    /// Check whether a current capability authorizes one Cabinet operation.
    /// All current operation callers must use this SQLite-backed boundary.
    pub async fn authorize(
        &self,
        token: &CapabilityToken,
        operation: &Grant,
    ) -> Result<(), AuthorityError> {
        // Authorization is a coherent read snapshot; it must not update the
        // global write-lock row. Mutating/signing operations use the serialized
        // boundary separately.
        let mut transaction = self.state.pool.begin().await?;
        let result = self.authorize_in(&mut transaction, token, operation).await;
        transaction.rollback().await?;
        result
    }

    async fn authorize_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        token: &CapabilityToken,
        operation: &Grant,
    ) -> Result<(), AuthorityError> {
        self.verify_in(transaction, token).await?;
        if self.is_operator_root(token) || token.grants.iter().any(|grant| grant == operation) {
            Ok(())
        } else {
            Err(AuthorityError::Unauthorized)
        }
    }

    /// Revoke one capability and all capabilities descended from it. This is
    /// local capability invalidation; it does not rewrite historical claims or
    /// mutate drawer/document/Keyhive state.
    pub async fn revoke(
        &self,
        operator: &CapabilityToken,
        token_id: TokenId,
    ) -> Result<(), AuthorityError> {
        let mut transaction = self.state.begin_serialized().await?;
        self.require_operator_root_in(&mut transaction, operator)
            .await?;
        if token_id == self.root_token_id {
            return Err(AuthorityError::Unauthorized);
        }
        if sqlx::query("SELECT 1 FROM authority_tokens WHERE token_id = ?1")
            .bind(token_id.as_slice())
            .fetch_optional(&mut *transaction)
            .await?
            .is_none()
        {
            return Err(AuthorityError::InvalidToken);
        }
        sqlx::query("INSERT OR IGNORE INTO authority_revocations (token_id) VALUES (?1)")
            .bind(token_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        match transaction.commit().await {
            Ok(()) => Ok(()),
            Err(error) => match self.state.is_revoked(&token_id).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(AuthorityError::Storage(error)),
                Err(recheck) => Err(AuthorityError::CommitUnknown(format!(
                    "revocation {} commit failed ({error}); state recheck failed: {recheck}",
                    hex(&token_id)
                ))),
            },
        }
    }

    /// Sign an actor-authored Core facet claim after checking current
    /// capability state and the registered subject key. Only the exact Core
    /// domain-prefixed deterministic-CBOR claim bytes are signed; capability
    /// and node provenance remain separate artifacts.
    pub async fn sign_actor_claim(
        &self,
        token: &CapabilityToken,
        claim: &ActorClaim,
    ) -> Result<SignedFacetClaim, AuthorityError> {
        claim.validate()?;
        let mut transaction = self.state.begin_serialized().await?;
        self.authorize_in(&mut transaction, token, &claim.operation)
            .await?;
        let Some(key_name) =
            AuthorityStateStore::actor_key_name_tx(&mut transaction, &token.subject).await?
        else {
            return Err(AuthorityError::ActorSignerMissing);
        };
        let signer = SigningKeyHandle::open(Arc::clone(&self.store), NODE_SERVICE, &key_name)
            .await?
            .bind_public_key(token.subject);
        let message = claim.claim.signing_bytes()?;
        let signature = signer.sign(&message).await?.to_vec();
        let signed = SignedFacetClaim::from_signature(
            claim.claim.clone(),
            token.subject.to_vec(),
            signature,
        )?;
        match transaction.commit().await {
            Ok(()) => Ok(signed),
            Err(error) => Err(AuthorityError::CommitUnknown(format!(
                "actor claim signing transaction outcome is unknown: {error}"
            ))),
        }
    }

    /// Verify the historical actor signature without consulting current capability
    /// state. This preserves legitimate claims after capability revocation.
    /// The current subject binding is still checked: the signed Core claim must
    /// identify exactly the subject authenticated by the node-signed token.
    pub fn verify_actor_claim(
        &self,
        token: &CapabilityToken,
        claim: &ActorClaim,
        signed: &SignedFacetClaim,
    ) -> Result<(), AuthorityError> {
        claim.validate()?;
        token.verify(&self.node_public_key)?;
        if signed.actor_public_key.as_slice() != token.subject.as_slice()
            || signed.claim != claim.claim
        {
            return Err(AuthorityError::InvalidToken);
        }
        signed.verify().map_err(|_| AuthorityError::InvalidToken)
    }

    fn is_operator_root(&self, token: &CapabilityToken) -> bool {
        token.subject == self.operator_public_key
            && token.parent.is_none()
            && token.root
            && token.token_id == self.root_token_id
    }

    async fn require_operator_root_in(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        token: &CapabilityToken,
    ) -> Result<(), AuthorityError> {
        self.verify_in(transaction, token).await?;
        if self.is_operator_root(token) {
            Ok(())
        } else {
            Err(AuthorityError::OperatorOnly)
        }
    }
}

async fn sign_token(
    signer: &SigningKeyHandle,
    issuer: ActorPublicKey,
    subject: ActorPublicKey,
    grants: Vec<Grant>,
    parent: Option<TokenId>,
    root: bool,
) -> Result<CapabilityToken, AuthorityError> {
    let mut token_id = [0_u8; 16];
    rand::rng().fill_bytes(&mut token_id);
    let mut token = CapabilityToken {
        version: TOKEN_VERSION,
        token_id,
        subject,
        grants,
        parent,
        issuer,
        root,
        signature: Vec::new(),
    };
    token.signature = signer.sign(&token.payload()?).await?.to_vec();
    Ok(token)
}

fn token_digest(token: &CapabilityToken) -> Result<[u8; 32], AuthorityError> {
    Ok(*blake3::hash(&token.payload()?).as_bytes())
}

fn validate_root(
    root: &CapabilityToken,
    node_public_key: &ActorPublicKey,
    operator_public_key: &ActorPublicKey,
) -> Result<(), AuthorityError> {
    root.verify(node_public_key)?;
    if !root.root
        || root.subject != *operator_public_key
        || root.parent.is_some()
        || root.issuer != *node_public_key
    {
        return Err(AuthorityError::StateCorrupt(
            "persisted authority root has invalid provenance".into(),
        ));
    }
    Ok(())
}

fn root_grants() -> Vec<Grant> {
    vec![
        Grant::ReadDrawer([0; 32]),
        Grant::WriteDrawer([0; 32]),
        Grant::ManageDrawer([0; 32]),
        Grant::CreateDocument([0; 32]),
        Grant::RemoveDocumentFromDrawer([0; 32]),
        Grant::AddChildDrawer([0; 32]),
        Grant::Delegate {
            drawer: [0; 32],
            grants: Vec::new(),
        },
    ]
}

fn verify_signature(
    key: &VerifyingKey,
    message: &[u8],
    signature: &Signature,
) -> Result<(), AuthorityError> {
    let signature: [u8; 64] = signature
        .as_slice()
        .try_into()
        .map_err(|_| AuthorityError::InvalidToken)?;
    key.verify(message, &DalekSignature::from_bytes(&signature))
        .map_err(|_| AuthorityError::InvalidToken)
}

fn fixed_bytes<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N], AuthorityError> {
    bytes.try_into().map_err(|_| {
        AuthorityError::StateCorrupt(format!("authority key has wrong width; expected {N} bytes"))
    })
}

fn optional_fixed_bytes<const N: usize>(
    bytes: Option<Vec<u8>>,
) -> Result<Option<[u8; N]>, AuthorityError> {
    bytes.map(fixed_bytes).transpose()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, sync::Arc};
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    fn facet_claim(seed: u128) -> FacetClaim {
        FacetClaim {
            origin_document: big_cabinet_core::DocumentId::new(b"mutation-document"),
            facet_key: big_cabinet_core::FacetKey::from_text("org.example.note", "main")
                .expect("facet key"),
            chosen_uuid: uuid::Uuid::from_u128(seed),
            operation: big_cabinet_core::dmeta::ClaimOperation::Set,
            value_cid: Some({
                let mut cid = vec![0x01, 0x55, 0x1e, 0x20];
                cid.extend([seed as u8; 32]);
                cid
            }),
            predecessors: Vec::new(),
        }
    }

    async fn store_and_path() -> (Arc<SecretStore>, TempDir, PathBuf) {
        let keyring = keyring_core::mock::Store::new().expect("mock keyring")
            as Arc<keyring_core::CredentialStore>;
        let store = Arc::new(
            SecretStore::boot_with_store(keyring)
                .await
                .expect("mock keyring"),
        );
        let directory = tempfile::tempdir().expect("authority state directory");
        let path = directory.path().join("authority.sqlite");
        (store, directory, path)
    }

    #[tokio::test]
    async fn stable_exported_root_authorizes_across_reopen() {
        let (store, _directory, path) = store_and_path().await;
        let (_repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let (reopened, reopened_root) = AuthorityRepo::boot(store, &path)
            .await
            .expect("authority reopen");
        assert_eq!(root, reopened_root);
        assert_eq!(root.token_id, reopened_root.token_id);
        reopened
            .authorize(&root, &Grant::ReadDrawer([99; 32]))
            .await
            .expect("current root authorization");
        let race_path = path.with_file_name("authority-race.sqlite");
        let (left, right) = tokio::join!(
            AuthorityRepo::boot(Arc::clone(&reopened.store), &race_path),
            AuthorityRepo::boot(Arc::clone(&reopened.store), &race_path),
        );
        let (_, left_root) = left.expect("left concurrent boot");
        let (_, right_root) = right.expect("right concurrent boot");
        assert_eq!(left_root, right_root);
    }

    #[tokio::test]
    async fn parallel_boots_share_one_initialized_identity_and_root() {
        let (store, _directory, path) = store_and_path().await;
        let barrier = Arc::new(Barrier::new(8));
        let mut boots = Vec::new();
        for _ in 0..8 {
            let barrier = Arc::clone(&barrier);
            let store = Arc::clone(&store);
            let path = path.clone();
            boots.push(tokio::spawn(async move {
                barrier.wait().await;
                AuthorityRepo::boot(store, path).await
            }));
        }
        let mut roots = Vec::new();
        for boot in boots {
            let (_repo, root) = boot
                .await
                .expect("parallel boot task")
                .expect("parallel boot");
            roots.push(root);
        }
        assert!(roots.windows(2).all(|roots| roots[0] == roots[1]));
    }

    #[tokio::test]
    async fn authorize_uses_a_read_snapshot_without_advancing_write_lock() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let before: i64 = sqlx::query("SELECT lock_nonce FROM authority_lock WHERE lock_id = 1")
            .fetch_one(&repo.state.pool)
            .await
            .expect("lock row")
            .try_get("lock_nonce")
            .expect("lock nonce");
        repo.authorize(&root, &Grant::ReadDrawer([99; 32]))
            .await
            .expect("current authorization");
        let after: i64 = sqlx::query("SELECT lock_nonce FROM authority_lock WHERE lock_id = 1")
            .fetch_one(&repo.state.pool)
            .await
            .expect("lock row")
            .try_get("lock_nonce")
            .expect("lock nonce");
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn root_provenance_and_reserved_targets_are_enforced() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let actor = repo.create_actor().await.expect("actor");
        assert!(matches!(
            repo.delegate(&root, repo.operator_public_key(), vec![])
                .await,
            Err(AuthorityError::ReservedIdentity)
        ));
        let child = repo
            .delegate(&root, actor, vec![Grant::ReadDrawer([99; 32])])
            .await
            .expect("child");
        assert!(matches!(
            repo.authorize(&child, &Grant::ReadDrawer([7; 32])).await,
            Err(AuthorityError::Unauthorized)
        ));
        assert!(matches!(
            repo.delegate(&child, repo.operator_public_key(), vec![])
                .await,
            Err(AuthorityError::ReservedIdentity)
        ));
    }

    #[tokio::test]
    async fn token_round_trip_and_context_tampering_are_refused() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let bytes = serde_json::to_vec(&root).expect("serialize token");
        let received: CapabilityToken = serde_json::from_slice(&bytes).expect("deserialize token");
        received
            .verify(&repo.node_public_key())
            .expect("portable signature");
        let mut tampered = received.clone();
        tampered.subject = [8; 32];
        assert!(matches!(
            repo.verify(&tampered).await,
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered = received;
        tampered.signature[0] ^= 1;
        assert!(matches!(
            repo.verify(&tampered).await,
            Err(AuthorityError::InvalidToken)
        ));
    }

    #[tokio::test]
    async fn delegation_is_downward_and_rejects_escalation_lateral_and_self() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let actor_a = repo.create_actor().await.expect("actor a");
        let actor_b = repo.create_actor().await.expect("actor b");
        let drawer = [7; 32];
        let read = Grant::ReadDrawer(drawer);
        let delegation = Grant::Delegate {
            drawer,
            grants: vec![read.clone()],
        };
        let parent = repo
            .delegate(&root, actor_a, vec![read.clone(), delegation])
            .await
            .expect("parent capability");
        let child = repo
            .delegate(&parent, actor_b, vec![read.clone()])
            .await
            .expect("attenuated child");
        repo.authorize(&child, &read)
            .await
            .expect("read remains allowed");
        assert!(matches!(
            repo.delegate(&parent, actor_b, vec![Grant::WriteDrawer(drawer)])
                .await,
            Err(AuthorityError::LateralDelegation)
        ));
        assert!(matches!(
            repo.delegate(&parent, actor_a, vec![read]).await,
            Err(AuthorityError::SelfDelegation)
        ));
    }

    #[tokio::test]
    async fn actor_claim_context_is_bound_and_historical_verification_survives_revoke() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let actor = repo.create_actor().await.expect("actor");
        let operation = Grant::WriteDrawer([3; 32]);
        let token = repo
            .delegate(&root, actor, vec![operation.clone()])
            .await
            .expect("capability");
        let claim = ActorClaim {
            operation: operation.clone(),
            claim: facet_claim(1),
        };
        let signed = repo
            .sign_actor_claim(&token, &claim)
            .await
            .expect("actor signature");
        repo.verify_actor_claim(&token, &claim, &signed)
            .expect("claim context");
        let message = claim.claim.signing_bytes().expect("canonical claim bytes");
        assert!(message.starts_with(b"daybook.facet-claim.v1\0"));
        let signature_bytes: [u8; 64] = signed
            .signature
            .as_slice()
            .try_into()
            .expect("signature width");
        VerifyingKey::from_bytes(&actor)
            .expect("actor public key")
            .verify(&message, &DalekSignature::from_bytes(&signature_bytes))
            .expect("independent Core verification");
        let mut tampered_claim = claim.clone();
        tampered_claim.claim.origin_document = big_cabinet_core::DocumentId::new(b"other-document");
        assert!(matches!(
            repo.verify_actor_claim(&token, &tampered_claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered_claim = claim.clone();
        tampered_claim.claim.facet_key =
            big_cabinet_core::FacetKey::from_text("org.example.other", "main").expect("facet key");
        assert!(matches!(
            repo.verify_actor_claim(&token, &tampered_claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered_claim = claim.clone();
        tampered_claim.claim.chosen_uuid = uuid::Uuid::from_u128(2);
        assert!(matches!(
            repo.verify_actor_claim(&token, &tampered_claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered_claim = claim.clone();
        tampered_claim.claim.operation = big_cabinet_core::dmeta::ClaimOperation::Remove;
        tampered_claim.claim.value_cid = None;
        assert!(matches!(
            repo.verify_actor_claim(&token, &tampered_claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered_claim = claim.clone();
        tampered_claim.claim.value_cid = Some({
            let mut cid = vec![0x01, 0x55, 0x1e, 0x20];
            cid.extend([0xaa; 32]);
            cid
        });
        assert!(matches!(
            repo.verify_actor_claim(&token, &tampered_claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered_claim = claim.clone();
        tampered_claim.claim.predecessors = vec![{
            let mut cid = vec![0x01, 0x55, 0x1e, 0x20];
            cid.extend([0xbb; 32]);
            cid
        }];
        assert!(matches!(
            repo.verify_actor_claim(&token, &tampered_claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        let wrong_operation = ActorClaim {
            operation: Grant::ManageDrawer([3; 32]),
            claim: claim.claim.clone(),
        };
        assert!(matches!(
            repo.sign_actor_claim(&token, &wrong_operation).await,
            Err(AuthorityError::Unauthorized)
        ));
        let wrong_drawer = ActorClaim {
            operation: Grant::WriteDrawer([4; 32]),
            claim: claim.claim.clone(),
        };
        assert!(matches!(
            repo.sign_actor_claim(&token, &wrong_drawer).await,
            Err(AuthorityError::Unauthorized)
        ));
        let mut wrong_subject = signed.clone();
        wrong_subject.actor_public_key = vec![8; 32];
        assert!(matches!(
            repo.verify_actor_claim(&token, &claim, &wrong_subject),
            Err(AuthorityError::InvalidToken)
        ));
        let mut tampered_signature = signed.clone();
        tampered_signature.signature[0] ^= 1;
        assert!(matches!(
            repo.verify_actor_claim(&token, &claim, &tampered_signature),
            Err(AuthorityError::InvalidToken)
        ));

        let other_actor = repo.create_actor().await.expect("other actor");
        let other_token = repo
            .delegate(&root, other_actor, vec![operation.clone()])
            .await
            .expect("other capability");
        assert!(matches!(
            repo.verify_actor_claim(&other_token, &claim, &signed),
            Err(AuthorityError::InvalidToken)
        ));
        repo.revoke(&root, token.token_id).await.expect("revoke");
        assert!(matches!(
            repo.authorize(&token, &claim.operation).await,
            Err(AuthorityError::InvalidToken)
        ));
        repo.verify_actor_claim(&token, &claim, &signed)
            .expect("historical claim remains verifiable");
        let (reopened, new_root) = AuthorityRepo::boot(store, &path)
            .await
            .expect("reopen authority");
        assert!(matches!(
            reopened.authorize(&token, &claim.operation).await,
            Err(AuthorityError::InvalidToken)
        ));
        reopened
            .delegate(&new_root, actor, vec![claim.operation])
            .await
            .expect("actor registry survives reboot");
    }

    #[tokio::test]
    async fn revoking_parent_invalidates_descendants_and_fresh_handles_see_updates() {
        let (store, _directory, path) = store_and_path().await;
        let (first, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let (second, second_root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("second authority boot");
        let actor_a = first.create_actor().await.expect("actor a");
        let actor_b = first.create_actor().await.expect("actor b");
        let drawer = [12; 32];
        let read = Grant::ReadDrawer(drawer);
        let parent = first
            .delegate(
                &root,
                actor_a,
                vec![
                    read.clone(),
                    Grant::Delegate {
                        drawer,
                        grants: vec![read.clone()],
                    },
                ],
            )
            .await
            .expect("parent capability");
        let child = second
            .delegate(&second_root, actor_b, vec![read.clone()])
            .await
            .expect("independent child");
        first
            .authorize(&child, &read)
            .await
            .expect("first handle sees second issuance");
        second
            .revoke(&second_root, child.token_id)
            .await
            .expect("revoke");
        assert!(matches!(
            first.authorize(&child, &read).await,
            Err(AuthorityError::InvalidToken)
        ));
        let replacement = first
            .delegate(&parent, actor_b, vec![read.clone()])
            .await
            .expect("revoked actor capability may be reissued");
        first
            .authorize(&replacement, &read)
            .await
            .expect("replacement capability is current");
    }

    #[tokio::test]
    async fn corrupted_denormalized_token_state_fails_closed() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let actor = repo.create_actor().await.expect("actor");
        let operation = Grant::ReadDrawer([61; 32]);
        let token = repo
            .delegate(&root, actor, vec![operation.clone()])
            .await
            .expect("token");
        sqlx::query("UPDATE authority_tokens SET subject = ?1 WHERE token_id = ?2")
            .bind([7_u8; 32].as_slice())
            .bind(token.token_id.as_slice())
            .execute(&repo.state.pool)
            .await
            .expect("corrupt subject index");
        assert!(matches!(
            repo.authorize(&token, &operation).await,
            Err(AuthorityError::StateCorrupt(_))
        ));

        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let actor = repo.create_actor().await.expect("actor");
        let token = repo
            .delegate(&root, actor, vec![operation.clone()])
            .await
            .expect("token");
        sqlx::query("UPDATE authority_tokens SET parent_id = ?1 WHERE token_id = ?2")
            .bind([3_u8; 16].as_slice())
            .bind(token.token_id.as_slice())
            .execute(&repo.state.pool)
            .await
            .expect("corrupt parent index");
        assert!(matches!(
            repo.verify(&token).await,
            Err(AuthorityError::StateCorrupt(_))
        ));

        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let actor = repo.create_actor().await.expect("actor");
        let token = repo
            .delegate(&root, actor, vec![operation])
            .await
            .expect("token");
        sqlx::query("UPDATE authority_tokens SET root = 2 WHERE token_id = ?1")
            .bind(token.token_id.as_slice())
            .execute(&repo.state.pool)
            .await
            .expect("corrupt root index");
        assert!(matches!(
            repo.verify(&token).await,
            Err(AuthorityError::StateCorrupt(_))
        ));

        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let parent_actor = repo.create_actor().await.expect("parent actor");
        let child_actor = repo.create_actor().await.expect("child actor");
        let drawer = [62; 32];
        let read = Grant::ReadDrawer(drawer);
        let parent = repo
            .delegate(
                &root,
                parent_actor,
                vec![
                    read.clone(),
                    Grant::Delegate {
                        drawer,
                        grants: vec![read.clone()],
                    },
                ],
            )
            .await
            .expect("parent token");
        let child = repo
            .delegate(&parent, child_actor, vec![read])
            .await
            .expect("child token");
        let mut persisted: serde_json::Value = serde_json::from_slice(
            &sqlx::query("SELECT token_json FROM authority_tokens WHERE token_id = ?1")
                .bind(parent.token_id.as_slice())
                .fetch_one(&repo.state.pool)
                .await
                .expect("parent row")
                .try_get::<Vec<u8>, _>("token_json")
                .expect("parent JSON"),
        )
        .expect("parent JSON value");
        persisted["subject"] = serde_json::Value::Array(
            [9_u8; 32]
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        );
        sqlx::query("UPDATE authority_tokens SET token_json = ?1 WHERE token_id = ?2")
            .bind(serde_json::to_vec(&persisted).expect("altered JSON"))
            .bind(parent.token_id.as_slice())
            .execute(&repo.state.pool)
            .await
            .expect("corrupt authenticated parent");
        assert!(matches!(
            repo.verify(&child).await,
            Err(AuthorityError::StateCorrupt(_))
        ));
    }

    #[tokio::test]
    async fn failed_operations_do_not_publish_authority_state() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        assert!(matches!(
            repo.delegate(&root, [44; 32], vec![Grant::ReadDrawer([1; 32])])
                .await,
            Err(AuthorityError::ActorSignerMissing)
        ));
        assert!(matches!(
            repo.revoke(&root, [55; 16]).await,
            Err(AuthorityError::InvalidToken)
        ));
        repo.authorize(&root, &Grant::ReadDrawer([77; 32]))
            .await
            .expect("failed mutations did not alter root state");
    }

    #[tokio::test]
    async fn concurrent_issuance_is_transactional() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let actor_a = repo.create_actor().await.expect("actor a");
        let actor_b = repo.create_actor().await.expect("actor b");
        let left = repo.delegate(&root, actor_a, vec![Grant::ReadDrawer([1; 32])]);
        let right = repo.delegate(&root, actor_b, vec![Grant::ReadDrawer([2; 32])]);
        let (left, right) = tokio::join!(left, right);
        let left = left.expect("left issuance");
        let right = right.expect("right issuance");
        repo.authorize(&left, &Grant::ReadDrawer([1; 32]))
            .await
            .expect("left current authorization");
        repo.authorize(&right, &Grant::ReadDrawer([2; 32]))
            .await
            .expect("right current authorization");
    }

    #[tokio::test]
    async fn nonoperator_same_subject_delegation_is_serialized() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(store, path)
            .await
            .expect("authority boot");
        let parent_actor = repo.create_actor().await.expect("parent actor");
        let target_actor = repo.create_actor().await.expect("target actor");
        let drawer = [42; 32];
        let read = Grant::ReadDrawer(drawer);
        let parent = repo
            .delegate(
                &root,
                parent_actor,
                vec![
                    read.clone(),
                    Grant::Delegate {
                        drawer,
                        grants: vec![read.clone()],
                    },
                ],
            )
            .await
            .expect("parent capability");
        let left = repo.delegate(&parent, target_actor, vec![read.clone()]);
        let right = repo.delegate(&parent, target_actor, vec![read]);
        let (left, right) = tokio::join!(left, right);
        assert_eq!(left.is_ok() as u8 + right.is_ok() as u8, 1);
        assert!(matches!(
            left.or(right),
            Ok(_) | Err(AuthorityError::LateralDelegation)
        ));
    }

    #[tokio::test]
    async fn signing_rechecks_registered_actor_and_node_public_keys() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let actor = repo.create_actor().await.expect("actor");
        let operation = Grant::WriteDrawer([11; 32]);
        let token = repo
            .delegate(&root, actor, vec![operation.clone()])
            .await
            .expect("capability");
        let key_name: String =
            sqlx::query("SELECT key_name FROM authority_actors WHERE public_key = ?1")
                .bind(actor.as_slice())
                .fetch_one(&repo.state.pool)
                .await
                .expect("actor registry")
                .try_get("key_name")
                .expect("key name");
        store
            .set_secret(NODE_SERVICE, &key_name, &[8; 32])
            .await
            .expect("replace actor key");
        let claim = ActorClaim {
            operation,
            claim: facet_claim(2),
        };
        assert!(matches!(
            repo.sign_actor_claim(&token, &claim).await,
            Err(AuthorityError::Secrets(SecretsError::Corrupt(_)))
        ));
    }

    #[tokio::test]
    async fn provisioning_public_key_read_failure_leaves_recoverable_intent() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        secrets_rs::test_reset_faults();
        // create_actor performs three service reads before the post-creation
        // public_key read: preflight, reacquisition, and get_or_create.
        secrets_rs::test_fail_get_after(NODE_SERVICE, 3);
        let error = repo
            .create_actor()
            .await
            .expect_err("injected public-key read");
        assert!(matches!(
            error,
            AuthorityError::ProvisioningUnresolved { .. }
        ));
        secrets_rs::test_reset_faults();

        let row = sqlx::query(
            "SELECT key_name, public_key FROM authority_actor_intents
              WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("durable actor intent");
        let key_name: String = row.try_get("key_name").expect("intent key name");
        let public_key: Option<Vec<u8>> = row.try_get("public_key").expect("intent public key");
        assert!(
            public_key.is_none(),
            "transaction rollback keeps intent unbound"
        );
        let seed = store
            .get_secret(NODE_SERVICE, &key_name)
            .await
            .expect("seed read")
            .expect("seed persisted before public-key fault");
        assert_eq!(seed.len(), 32);

        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("reopen reconciles actor intent");
        let registry =
            sqlx::query("SELECT public_key, key_name FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_one(&reopened.state.pool)
                .await
                .expect("recovered actor registry");
        let registered: Vec<u8> = registry.try_get("public_key").expect("registered key");
        assert_eq!(registered.len(), 32);
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1",)
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("intent lookup")
                .is_none()
        );
        assert_eq!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("recovered seed read")
                .expect("recovered seed"),
            seed
        );
    }

    #[tokio::test]
    async fn actor_publication_and_cleanup_faults_are_typed_and_durable() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        secrets_rs::test_reset_faults();
        provisioning_test_faults::fail_actor_publication();
        secrets_rs::test_fail_next_delete(NODE_SERVICE);
        let error = repo
            .create_actor()
            .await
            .expect_err("injected publication failure");
        assert!(matches!(
            error,
            AuthorityError::ProvisioningUnresolved { .. }
        ));
        secrets_rs::test_reset_faults();

        let row = sqlx::query(
            "SELECT key_name, public_key, cleanup_pending FROM authority_actor_intents
              WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("pending intent after failed cleanup");
        let key_name: String = row.try_get("key_name").expect("intent key name");
        assert!(
            row.try_get::<Option<Vec<u8>>, _>("public_key")
                .expect("intent key")
                .is_some()
        );
        assert_eq!(
            row.try_get::<i64, _>("cleanup_pending")
                .expect("cleanup phase"),
            1
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1",)
                .bind(&key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("registry lookup")
                .is_none()
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("seed lookup")
                .is_some()
        );

        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("reopen reconciles pending publication");
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("recovered registry lookup")
                .is_none()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1",)
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("recovered intent lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cancelled_creator_recovers_after_sqlite_reopen() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        provisioning_test_faults::arm_actor_publication_barrier();
        let task_repo = repo.clone();
        let task = tokio::spawn(async move { task_repo.create_actor().await });
        provisioning_test_faults::wait_actor_publication_barrier().await;
        task.abort();
        assert!(task.await.expect_err("creator task abort").is_cancelled());
        provisioning_test_faults::release_actor_publication_barrier();

        let row = sqlx::query(
            "SELECT key_name, public_key FROM authority_actor_intents
              WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("intent survives caller cancellation");
        let key_name: String = row.try_get("key_name").expect("intent key name");
        assert!(
            row.try_get::<Option<Vec<u8>>, _>("public_key")
                .expect("intent public key")
                .is_none()
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("cancelled seed lookup")
                .is_some()
        );

        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("independent boot recovers cancelled creator");
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("recovered actor lookup")
                .is_some()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1",)
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("recovered intent lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn boot_and_creator_owners_share_the_serialized_boundary() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        provisioning_test_faults::arm_actor_publication_barrier();
        let creator_repo = repo.clone();
        let creator = tokio::spawn(async move { creator_repo.create_actor().await });
        provisioning_test_faults::wait_actor_publication_barrier().await;
        let boot_store = Arc::clone(&store);
        let boot_path = path.clone();
        let boot = tokio::spawn(async move { AuthorityRepo::boot(boot_store, boot_path).await });
        provisioning_test_faults::release_actor_publication_barrier();
        let actor = creator
            .await
            .expect("creator owner task")
            .expect("creator owner");
        let (_reopened, root) = boot.await.expect("boot owner task").expect("boot owner");
        assert_ne!(actor, [0; 32]);
        assert!(root.root);
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE public_key = ?1")
                .bind(actor.as_slice())
                .fetch_optional(&repo.state.pool)
                .await
                .expect("serialized actor lookup")
                .is_some()
        );
    }

    #[tokio::test]
    async fn competing_boot_is_blocked_before_cleanup_delete_and_wins_after_release() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let (contender_repo, _contender_root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("contender pre-boot");
        provisioning_test_faults::fail_actor_publication();
        provisioning_test_faults::arm_cleanup_pre_delete_barrier();
        let creator_repo = repo.clone();
        let creator = tokio::spawn(async move { creator_repo.create_actor().await });
        provisioning_test_faults::wait_cleanup_pre_delete_barrier().await;
        let intent = sqlx::query(
            "SELECT key_name, cleanup_pending FROM authority_actor_intents
               WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("pre-delete cleanup intent");
        let key_name: String = intent.try_get("key_name").expect("intent key name");
        assert_eq!(
            intent.try_get::<i64, _>("cleanup_pending").expect("phase"),
            1
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("pre-delete seed")
                .is_some()
        );

        provisioning_test_faults::arm_serialized_begin_probe();
        let contender = tokio::spawn(async move { contender_repo.reconcile_pending().await });
        provisioning_test_faults::wait_serialized_begin_probe().await;
        provisioning_test_faults::release_cleanup_pre_delete_barrier();
        let creator_error = creator
            .await
            .expect("creator task")
            .expect_err("publication fault remains the creator outcome");
        assert!(matches!(creator_error, AuthorityError::Storage(_)));
        contender
            .await
            .expect("contender task")
            .expect("contender wins after creator finish");
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("post-cleanup seed")
                .is_none()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("post-cleanup intent")
                .is_none()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("post-cleanup registry")
                .is_none()
        );
    }

    #[tokio::test]
    async fn post_delete_cancellation_reconciles_durable_cleanup_phase() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let (contender_repo, _contender_root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("contender pre-boot");
        provisioning_test_faults::fail_actor_publication();
        provisioning_test_faults::arm_cleanup_delete_barrier();
        let task_repo = repo.clone();
        let task = tokio::spawn(async move { task_repo.create_actor().await });
        provisioning_test_faults::wait_cleanup_delete_barrier().await;
        provisioning_test_faults::arm_serialized_begin_probe();
        let contender = tokio::spawn(async move { contender_repo.reconcile_pending().await });
        provisioning_test_faults::wait_serialized_begin_probe().await;
        let row = sqlx::query(
            "SELECT key_name, public_key, cleanup_pending
               FROM authority_actor_intents WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("durable cleanup intent while contender is blocked");
        let key_name: String = row.try_get("key_name").expect("cleanup key name");
        let public_key: Vec<u8> = row.try_get("public_key").expect("cleanup public key");
        assert_eq!(public_key.len(), 32);
        assert_eq!(
            row.try_get::<i64, _>("cleanup_pending")
                .expect("cleanup phase"),
            1
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("deleted seed lookup")
                .is_none()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("published registry lookup")
                .is_none()
        );
        task.abort();
        assert!(
            task.await
                .expect_err("post-delete creator abort")
                .is_cancelled()
        );
        provisioning_test_faults::release_cleanup_delete_barrier();
        contender
            .await
            .expect("contender task")
            .expect("contender reconciliation after creator cancellation");

        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("contender cleanup intent lookup")
                .is_none()
        );
        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("reopen completes deleted cleanup phase");
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("cleanup intent removal lookup")
                .is_none()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("cleanup registry lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn post_delete_sql_removal_failure_reopens_to_recoverable_phase() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        provisioning_test_faults::fail_actor_publication();
        provisioning_test_faults::fail_actor_intent_removal();
        let error = repo
            .create_actor()
            .await
            .expect_err("cleanup SQL removal failure");
        assert!(matches!(
            error,
            AuthorityError::ProvisioningUnresolved { .. }
        ));
        let row = sqlx::query(
            "SELECT key_name, public_key, cleanup_pending
               FROM authority_actor_intents WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("cleanup phase survives removal failure");
        let key_name: String = row.try_get("key_name").expect("cleanup key name");
        assert_eq!(row.try_get::<i64, _>("cleanup_pending").expect("phase"), 1);
        assert_eq!(
            row.try_get::<Vec<u8>, _>("public_key")
                .expect("cleanup public key")
                .len(),
            32
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("deleted seed lookup")
                .is_none()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("registry lookup")
                .is_none()
        );

        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("reopen reconciles post-delete SQL failure");
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("reopened intent lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn foreign_preexisting_signer_is_never_deleted() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let key_name = "actor-foreign";
        store
            .set_secret(NODE_SERVICE, key_name, &[31; 32])
            .await
            .expect("foreign seed");
        sqlx::query(
            "INSERT INTO authority_actor_intents
             (intent_id, key_name, public_key, preexisting)
             VALUES (?1, ?2, NULL, 1)",
        )
        .bind([41_u8; 16].as_slice())
        .bind(key_name)
        .execute(&repo.state.pool)
        .await
        .expect("foreign intent");
        let error = match AuthorityRepo::boot(Arc::clone(&store), &path).await {
            Ok(_) => panic!("foreign preexisting signer is unresolved"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            AuthorityError::ProvisioningUnresolved { .. }
        ));
        assert!(
            store
                .get_secret(NODE_SERVICE, key_name)
                .await
                .expect("foreign seed lookup")
                .is_some()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1",)
                .bind(key_name)
                .fetch_optional(&repo.state.pool)
                .await
                .expect("foreign intent lookup")
                .is_some()
        );
    }

    #[tokio::test]
    async fn ambiguous_successful_publication_preserves_exact_seed() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        provisioning_test_faults::force_actor_commit_unknown();
        let error = repo
            .create_actor()
            .await
            .expect_err("injected ambiguous commit");
        assert!(matches!(error, AuthorityError::CommitUnknown(_)));
        let row = sqlx::query(
            "SELECT public_key, key_name FROM authority_actors ORDER BY key_name DESC LIMIT 1",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("published actor row after ambiguous commit");
        let public_key: Vec<u8> = row.try_get("public_key").expect("published public key");
        let key_name: String = row.try_get("key_name").expect("published key name");
        assert_eq!(public_key.len(), 32);
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("published seed lookup")
                .is_some()
        );
        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("reopen after ambiguous commit");
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("published row survives reopen")
                .is_some()
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("exact seed survives reopen")
                .is_some()
        );
    }

    #[tokio::test]
    async fn revocation_serializes_against_delegate_and_actor_signing() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        let parent_actor = repo.create_actor().await.expect("parent actor");
        let target_actor = repo.create_actor().await.expect("target actor");
        let drawer = [13; 32];
        let read = Grant::ReadDrawer(drawer);
        let parent = repo
            .delegate(
                &root,
                parent_actor,
                vec![
                    read.clone(),
                    Grant::Delegate {
                        drawer,
                        grants: vec![read.clone()],
                    },
                ],
            )
            .await
            .expect("parent capability");
        let delegate = repo.delegate(&parent, target_actor, vec![read.clone()]);
        let revoke = repo.revoke(&root, parent.token_id);
        let (child, revoked) = tokio::join!(delegate, revoke);
        revoked.expect("parent revoke");
        if let Ok(child) = child {
            assert!(matches!(
                repo.authorize(&child, &read).await,
                Err(AuthorityError::InvalidToken)
            ));
        }

        let signing_actor = repo.create_actor().await.expect("signing actor");
        let operation = Grant::WriteDrawer([14; 32]);
        let signing_token = repo
            .delegate(&root, signing_actor, vec![operation.clone()])
            .await
            .expect("signing capability");
        let claim = ActorClaim {
            operation: operation.clone(),
            claim: facet_claim(3),
        };
        let sign = repo.sign_actor_claim(&signing_token, &claim);
        let revoke = repo.revoke(&root, signing_token.token_id);
        let (signature, revoked) = tokio::join!(sign, revoke);
        revoked.expect("signing capability revoke");
        if let Ok(signed) = signature {
            repo.verify_actor_claim(&signing_token, &claim, &signed)
                .expect("historical signature");
            assert!(matches!(
                repo.authorize(&signing_token, &claim.operation).await,
                Err(AuthorityError::InvalidToken)
            ));
        }
    }

    #[tokio::test]
    async fn concurrent_old_schema_opens_serialize_cleanup_column_migration() {
        let (store, _directory, path) = store_and_path().await;
        let old_options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let old_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(old_options)
            .await
            .expect("old schema pool");
        sqlx::query(
            "CREATE TABLE authority_lock (
                 lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1)
               , lock_nonce INTEGER NOT NULL
             )",
        )
        .execute(&old_pool)
        .await
        .expect("old lock table");
        sqlx::query("INSERT INTO authority_lock (lock_id, lock_nonce) VALUES (1, 0)")
            .execute(&old_pool)
            .await
            .expect("old lock row");
        sqlx::query(
            "CREATE TABLE authority_actor_intents (
                 intent_id BLOB PRIMARY KEY
               , key_name TEXT NOT NULL UNIQUE
               , public_key BLOB
               , preexisting INTEGER NOT NULL CHECK (preexisting IN (0, 1))
             )",
        )
        .execute(&old_pool)
        .await
        .expect("old actor intent table");
        old_pool.close().await;

        let barrier = Arc::new(Barrier::new(2));
        let left_store = Arc::clone(&store);
        let left_path = path.clone();
        let left_barrier = Arc::clone(&barrier);
        let left = tokio::spawn(async move {
            left_barrier.wait().await;
            AuthorityRepo::boot(left_store, left_path).await
        });
        let right_store = Arc::clone(&store);
        let right_path = path.clone();
        let right_barrier = Arc::clone(&barrier);
        let right = tokio::spawn(async move {
            right_barrier.wait().await;
            AuthorityRepo::boot(right_store, right_path).await
        });
        let (_, left_root) = left.await.expect("left open").expect("left boot");
        let (right_repo, right_root) = right.await.expect("right open").expect("right boot");
        assert_eq!(left_root, right_root);
        let columns = sqlx::query("PRAGMA table_info(authority_actor_intents)")
            .fetch_all(&right_repo.state.pool)
            .await
            .expect("migrated schema columns");
        assert_eq!(
            columns
                .iter()
                .filter(|row| row
                    .try_get::<String, _>("name")
                    .is_ok_and(|name| name == "cleanup_pending"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn unbound_actor_intent_sql_failure_reopens_and_recovers() {
        let (store, _directory, path) = store_and_path().await;
        let (repo, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("authority boot");
        provisioning_test_faults::fail_actor_intent_binding();
        let error = repo
            .create_actor()
            .await
            .expect_err("injected intent binding SQL failure");
        assert!(matches!(
            error,
            AuthorityError::ProvisioningUnresolved { .. }
        ));
        let row = sqlx::query(
            "SELECT key_name, public_key FROM authority_actor_intents
               WHERE key_name LIKE 'actor-%'",
        )
        .fetch_one(&repo.state.pool)
        .await
        .expect("unbound actor intent");
        let key_name: String = row.try_get("key_name").expect("intent key name");
        assert!(
            row.try_get::<Option<Vec<u8>>, _>("public_key")
                .expect("intent public key")
                .is_none()
        );
        assert!(
            store
                .get_secret(NODE_SERVICE, &key_name)
                .await
                .expect("persisted actor seed")
                .is_some()
        );
        provisioning_test_faults::fail_actor_intent_binding();
        let recovery_error = match AuthorityRepo::boot(Arc::clone(&store), &path).await {
            Ok(_) => panic!("injected recovery intent binding failure must be unresolved"),
            Err(error) => error,
        };
        assert!(matches!(
            recovery_error,
            AuthorityError::ProvisioningUnresolved { key_name: failed_key, reason }
                if failed_key == key_name
                    && reason.contains("recovered actor intent binding is unresolved")
        ));
        let unbound_public_key: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT public_key FROM authority_actor_intents WHERE key_name = ?1",
        )
        .bind(&key_name)
        .fetch_one(&repo.state.pool)
        .await
        .expect("recovery failure preserves the unbound intent");
        assert!(unbound_public_key.is_none());
        let (reopened, _root) = AuthorityRepo::boot(Arc::clone(&store), &path)
            .await
            .expect("reopen recovers unbound intent");
        assert!(
            sqlx::query("SELECT 1 FROM authority_actors WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("recovered actor registry")
                .is_some()
        );
        assert!(
            sqlx::query("SELECT 1 FROM authority_actor_intents WHERE key_name = ?1")
                .bind(&key_name)
                .fetch_optional(&reopened.state.pool)
                .await
                .expect("recovered intent")
                .is_none()
        );
    }

    #[tokio::test]
    async fn root_sql_failure_is_typed_and_boot_intents_remain_recoverable() {
        let (store, _directory, path) = store_and_path().await;
        provisioning_test_faults::fail_root_install();
        let error = match AuthorityRepo::boot(Arc::clone(&store), &path).await {
            Ok(_) => panic!("injected root SQL failure must be unresolved"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            AuthorityError::ProvisioningUnresolved { .. }
        ));
        secrets_rs::test_reset_faults();
        let state = AuthorityStateStore::boot(&path)
            .await
            .expect("state reopen");
        let intent_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM authority_boot_intents WHERE key_name IN (?1, ?2)",
        )
        .bind(NODE_KEY)
        .bind(OPERATOR_KEY)
        .fetch_one(&state.pool)
        .await
        .expect("durable boot intents");
        assert_eq!(intent_count, 2);
        let (_repo, _root) = AuthorityRepo::boot(store, &path)
            .await
            .expect("boot intent recovery");
    }
}
