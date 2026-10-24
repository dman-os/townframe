//! The durable state boundary: the authority's SQLite tables and the queries
//! over them.
//!
//! Every function takes a connection so the caller decides the transaction: a
//! write runs inside one `BEGIN IMMEDIATE` transaction on the writer pool, which
//! is the single cross-process linearization point, and a read runs on the
//! reader pool beside it. Nothing here interprets an object string or decides
//! whether an operation is allowed.

use super::{ActorPublicKey, AuthorityError};
use big_cabinet_core::dmeta::Uuid7;
use sqlx::{Row, SqliteConnection, SqlitePool, sqlite::SqliteRow};

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

pub(super) async fn migrate(pool: &SqlitePool) -> Result<(), AuthorityError> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let legacy: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM sqlite_schema
              WHERE type = 'table'
                AND name IN ('authority_caps', 'authority_actor_issuances')
             UNION ALL
             SELECT 1 FROM pragma_table_info('authority_node') WHERE name = 'root_cap'
             UNION ALL
             SELECT 1 FROM pragma_table_info('authority_grants') WHERE name = 'granted_via'
         )",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if legacy {
        return Err(AuthorityError::StateCorrupt(
            "legacy durable capability schema requires explicit migration; preserve actors, node keys, actors_issuer and issuer-authored grants before removing durable handles".into(),
        ));
    }
    let central_grants: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
               FROM sqlite_schema
              WHERE type = 'table'
                AND name = 'authority_grants'
         )",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if central_grants {
        let populated: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM authority_grants)")
            .fetch_one(&mut *transaction)
            .await?;
        if populated {
            return Err(AuthorityError::StateCorrupt(
                "central authority_grants rows require explicit domain-grant migration; preserve issuer, subject, object and rights in the owning domain before removing central grants".into(),
            ));
        }
    }
    for statement in [
        "CREATE TABLE IF NOT EXISTS authority_actors (
             uuid TEXT PRIMARY KEY
           , path TEXT NOT NULL UNIQUE
           , parent_uuid TEXT REFERENCES authority_actors (uuid)
           , public_key BLOB NOT NULL UNIQUE
           , sealed_key BLOB NOT NULL
           , key_nonce BLOB NOT NULL
           , certificate BLOB NOT NULL
         )",
        "CREATE TABLE IF NOT EXISTS authority_node (
             node_id INTEGER PRIMARY KEY CHECK (node_id = 1)
           , public_key BLOB NOT NULL
           , sealed_key BLOB NOT NULL
           , key_nonce BLOB NOT NULL
           , operator_uuid TEXT NOT NULL REFERENCES authority_actors (uuid)
           , actors_issuer TEXT NOT NULL
         )",
    ] {
        sqlx::query(statement).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
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

fn sealed(row: &SqliteRow) -> Result<SealedKey, AuthorityError> {
    Ok(SealedKey {
        ciphertext: row.try_get("sealed_key")?,
        nonce: fixed(row.try_get("key_nonce")?, "a key nonce")?,
    })
}

pub(super) async fn node(conn: &mut SqliteConnection) -> Result<Option<NodeRow>, AuthorityError> {
    let Some(row) = sqlx::query(
        "SELECT public_key
              , sealed_key
              , key_nonce
              , operator_uuid
              , actors_issuer
           FROM authority_node
          WHERE node_id = 1",
    )
    .fetch_optional(conn)
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(NodeRow {
        public_key: fixed(row.try_get("public_key")?, "the node key")?,
        sealed: sealed(&row)?,
        operator: uuid(row.try_get("operator_uuid")?)?,
        actors_issuer: uuid(row.try_get("actors_issuer")?)?,
    }))
}

pub(super) async fn insert_node(
    conn: &mut SqliteConnection,
    node: &NodeRow,
) -> Result<(), AuthorityError> {
    sqlx::query(
        "INSERT INTO authority_node
              ( node_id
              , public_key
              , sealed_key
              , key_nonce
              , operator_uuid
              , actors_issuer
              )
         VALUES (1, ?1, ?2, ?3, ?4, ?5)",
    )
    .bind(node.public_key.as_slice())
    .bind(node.sealed.ciphertext.as_slice())
    .bind(node.sealed.nonce.as_slice())
    .bind(node.operator.to_string())
    .bind(node.actors_issuer.to_string())
    .execute(conn)
    .await?;
    Ok(())
}

fn actor_row(row: SqliteRow) -> Result<ActorRow, AuthorityError> {
    Ok(ActorRow {
        actor_id: uuid(row.try_get("uuid")?)?,
        path: row.try_get("path")?,
        parent: row
            .try_get::<Option<String>, _>("parent_uuid")?
            .map(uuid)
            .transpose()?,
        public_key: fixed(row.try_get("public_key")?, "an actor key")?,
        sealed: sealed(&row)?,
        certificate: row.try_get("certificate")?,
    })
}

/// One actor row selected by `$filter`, spelled as a literal so the query
/// stays a static string.
macro_rules! select_actor {
    ($filter:literal) => {
        concat!(
            "SELECT uuid
              , path
              , parent_uuid
              , public_key
              , sealed_key
              , key_nonce
              , certificate
           FROM authority_actors
          WHERE ",
            $filter
        )
    };
}

pub(super) async fn actor(
    conn: &mut SqliteConnection,
    actor: Uuid7,
) -> Result<Option<ActorRow>, AuthorityError> {
    sqlx::query(select_actor!("uuid = ?1"))
        .bind(actor.to_string())
        .fetch_optional(conn)
        .await?
        .map(actor_row)
        .transpose()
}

pub(super) async fn actor_at(
    conn: &mut SqliteConnection,
    path: &str,
) -> Result<Option<ActorRow>, AuthorityError> {
    sqlx::query(select_actor!("path = ?1"))
        .bind(path)
        .fetch_optional(conn)
        .await?
        .map(actor_row)
        .transpose()
}

pub(super) async fn insert_actor(
    conn: &mut SqliteConnection,
    actor: &ActorRow,
) -> Result<(), AuthorityError> {
    sqlx::query(
        "INSERT INTO authority_actors
              ( uuid
              , path
              , parent_uuid
              , public_key
              , sealed_key
              , key_nonce
              , certificate
              )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(actor.actor_id.to_string())
    .bind(&actor.path)
    .bind(actor.parent.map(|parent| parent.to_string()))
    .bind(actor.public_key.as_slice())
    .bind(actor.sealed.ciphertext.as_slice())
    .bind(actor.sealed.nonce.as_slice())
    .bind(actor.certificate.as_slice())
    .execute(conn)
    .await?;
    Ok(())
}
