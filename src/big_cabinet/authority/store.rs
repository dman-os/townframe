//! The durable state boundary: the authority's SQLite tables and the queries
//! over them.
//!
//! Every function takes a connection so the caller decides the transaction: a
//! write runs inside one `BEGIN IMMEDIATE` transaction on the writer pool, which
//! is the single cross-process linearization point, and a read runs on the
//! reader pool beside it. Nothing here interprets an object string or decides
//! whether an operation is allowed.

use super::ActorPublicKey;
use super::AuthorityError;
use big_cabinet_core::dmeta::Uuid7;
use sqlite_rs::Database;
use sqlite_rs::Executor;
use sqlite_rs::QueryExecutor;

mod queries {
    include!("sqlite/gen.rs");
}

/// A signing seed encrypted under the node's wrapping key. The row identity is
/// the AEAD's associated data, so a sealed key moved to another row does not
/// open.
pub(super) struct SealedKey {
    pub(super) ciphertext: Vec<u8>,
    pub(super) nonce: [u8; 12],
}

/// The node's own identity and the operator it booted, created together once.
pub(super) struct NodeRow {
    pub(super) public_key: ActorPublicKey,
    pub(super) sealed: SealedKey,
    pub(super) operator: Uuid7,
    pub(super) actors_issuer: Uuid7,
}

pub(super) struct ActorRow {
    pub(super) actor_id: Uuid7,
    pub(super) path: String,
    pub(super) parent: Option<Uuid7>,
    pub(super) public_key: ActorPublicKey,
    pub(super) sealed: SealedKey,
    pub(super) certificate: Vec<u8>,
}

pub(super) async fn migrate(database: &Database) -> Result<(), AuthorityError> {
    let transaction = database.begin_write().await?;
    if queries::legacy_schema(&transaction).await? {
        return Err(AuthorityError::StateCorrupt(
            "legacy durable capability schema requires explicit migration; preserve actors, node keys, actors_issuer and issuer-authored grants before removing durable handles".into(),
        ));
    }
    if queries::central_grants(&transaction).await? {
        let populated: bool = transaction
            .query(include_str!("sqlite/central_grants_populated.sql"), ())
            .await?
            .one()?;
        if populated {
            return Err(AuthorityError::StateCorrupt(
                "central authority_grants rows require explicit domain-grant migration; preserve issuer, subject, object and rights in the owning domain before removing central grants".into(),
            ));
        }
    }
    transaction.rollback().await?;
    database
        .apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
        .await?;
    Ok(())
}

fn uuid(text: String) -> Result<Uuid7, AuthorityError> {
    let uuid: uuid::Uuid = text
        .parse()
        .map_err(|_| AuthorityError::StateCorrupt(format!("`{text}` is not a uuid")))?;
    Ok(Uuid7::try_from(uuid)?)
}

fn fixed<const N: usize>(bytes: Vec<u8>, what: &str) -> Result<[u8; N], AuthorityError> {
    bytes
        .try_into()
        .map_err(|_| AuthorityError::StateCorrupt(format!("{what} is not {N} bytes")))
}

pub(super) async fn node(conn: &impl QueryExecutor) -> Result<Option<NodeRow>, AuthorityError> {
    let Some(row) = queries::node(conn).await? else {
        return Ok(None);
    };
    Ok(Some(NodeRow {
        public_key: fixed(row.public_key, "the node key")?,
        sealed: SealedKey {
            ciphertext: row.sealed_key,
            nonce: fixed(row.key_nonce, "a key nonce")?,
        },
        operator: uuid(row.operator_uuid)?,
        actors_issuer: uuid(row.actors_issuer)?,
    }))
}

pub(super) async fn insert_node(
    conn: &impl Executor,
    node: &NodeRow,
) -> Result<(), AuthorityError> {
    queries::insert_node(
        conn,
        node.public_key.to_vec(),
        node.sealed.ciphertext.clone(),
        node.sealed.nonce.to_vec(),
        node.operator.to_string(),
        node.actors_issuer.to_string(),
    )
    .await?;
    Ok(())
}

fn actor_row(row: queries::ActorRow) -> Result<ActorRow, AuthorityError> {
    Ok(ActorRow {
        actor_id: uuid(row.uuid)?,
        path: row.path,
        parent: row.parent_uuid.map(uuid).transpose()?,
        public_key: fixed(row.public_key, "an actor key")?,
        sealed: SealedKey {
            ciphertext: row.sealed_key,
            nonce: fixed(row.key_nonce, "a key nonce")?,
        },
        certificate: row.certificate,
    })
}

pub(super) async fn actor(
    conn: &impl QueryExecutor,
    actor: Uuid7,
) -> Result<Option<ActorRow>, AuthorityError> {
    queries::actor(conn, actor.to_string())
        .await?
        .map(actor_row)
        .transpose()
}

pub(super) async fn actor_at(
    conn: &impl QueryExecutor,
    path: &str,
) -> Result<Option<ActorRow>, AuthorityError> {
    queries::actor_at(conn, path.to_owned())
        .await?
        .map(actor_row)
        .transpose()
}

pub(super) async fn insert_actor(
    conn: &impl Executor,
    actor: &ActorRow,
) -> Result<(), AuthorityError> {
    queries::insert_actor(
        conn,
        actor.actor_id.to_string(),
        actor.path.clone(),
        actor.parent.map(|parent| parent.to_string()),
        actor.public_key.to_vec(),
        actor.sealed.ciphertext.clone(),
        actor.sealed.nonce.to_vec(),
        actor.certificate.clone(),
    )
    .await?;
    Ok(())
}
