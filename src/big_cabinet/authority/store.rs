//! The durable state boundary: the authority's SQLite tables and the queries
//! over them.
//!
//! Every function takes a connection so the caller decides the transaction: a
//! write runs inside one `BEGIN IMMEDIATE` transaction on the writer pool, which
//! is the single cross-process linearization point, and a read runs on the
//! reader pool beside it. Nothing here interprets an object string or decides
//! whether an operation is allowed.

use super::protocol::{CapId, Capability, Rights};
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
    pub(super) root_cap: CapId,
    pub(super) actors_issuer: Uuid7,
}

pub(super) struct ActorRow {
    pub(super) uuid: Uuid7,
    pub(super) path: String,
    pub(super) parent: Option<Uuid7>,
    pub(super) public_key: ActorPublicKey,
    pub(super) sealed: SealedKey,
    pub(super) certificate: Vec<u8>,
}

pub(super) struct CapRow {
    pub(super) id: CapId,
    pub(super) object: String,
    pub(super) rights: Rights,
    pub(super) parent: Option<CapId>,
    pub(super) issuer: Uuid7,
    pub(super) retracted: bool,
}

impl CapRow {
    pub(super) fn wire(&self) -> Capability {
        Capability {
            id: self.id,
            object: self.object.clone(),
            rights: self.rights,
            issuer: self.issuer,
        }
    }
}

pub(super) struct GrantRow {
    /// The object capability the grant was written through, or `None` when the
    /// issuer wrote it with its own actor capability.
    pub(super) via: Option<CapId>,
    pub(super) rights: Rights,
}

pub(super) async fn migrate(pool: &SqlitePool) -> Result<(), AuthorityError> {
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
        "CREATE TABLE IF NOT EXISTS authority_caps (
             cap_id BLOB PRIMARY KEY
           , object TEXT NOT NULL
           , rights INTEGER NOT NULL
           , parent_id BLOB REFERENCES authority_caps (cap_id)
           , issuer TEXT NOT NULL
           , retracted INTEGER NOT NULL DEFAULT 0 CHECK (retracted IN (0, 1))
         )",
        "CREATE TABLE IF NOT EXISTS authority_actor_issuances (
             cap_id BLOB PRIMARY KEY REFERENCES authority_caps (cap_id)
           , actor TEXT NOT NULL REFERENCES authority_actors (uuid)
         )",
        "CREATE INDEX IF NOT EXISTS authority_actor_issuances_by_actor
             ON authority_actor_issuances (actor)",
        "CREATE TABLE IF NOT EXISTS authority_node (
             node_id INTEGER PRIMARY KEY CHECK (node_id = 1)
           , public_key BLOB NOT NULL
           , sealed_key BLOB NOT NULL
           , key_nonce BLOB NOT NULL
           , operator_uuid TEXT NOT NULL REFERENCES authority_actors (uuid)
           , root_cap BLOB NOT NULL REFERENCES authority_caps (cap_id)
           , actors_issuer TEXT NOT NULL
         )",
        // A grant written by the issuer itself has no `granted_via`; the
        // expression index makes that one row per (issuer, subject, object) too.
        "CREATE TABLE IF NOT EXISTS authority_grants (
             issuer TEXT NOT NULL REFERENCES authority_actors (uuid)
           , subject TEXT NOT NULL REFERENCES authority_actors (uuid)
           , object TEXT NOT NULL
           , granted_via BLOB REFERENCES authority_caps (cap_id)
           , rights INTEGER NOT NULL
         )",
        "CREATE UNIQUE INDEX IF NOT EXISTS authority_grants_key
             ON authority_grants (issuer, subject, object, coalesce(granted_via, x''))",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
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

fn cap_id(bytes: Vec<u8>) -> Result<CapId, AuthorityError> {
    Ok(CapId::from_bytes(fixed(bytes, "a capability id")?))
}

fn rights(bits: i64) -> Result<Rights, AuthorityError> {
    Rights::from_bits(bits)
        .ok_or_else(|| AuthorityError::StateCorrupt(format!("rights bits {bits:#x}")))
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
              , root_cap
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
        root_cap: cap_id(row.try_get("root_cap")?)?,
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
              , root_cap
              , actors_issuer
              )
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(node.public_key.as_slice())
    .bind(node.sealed.ciphertext.as_slice())
    .bind(node.sealed.nonce.as_slice())
    .bind(node.operator.to_string())
    .bind(node.root_cap.as_bytes().as_slice())
    .bind(node.actors_issuer.to_string())
    .execute(conn)
    .await?;
    Ok(())
}

fn actor_row(row: SqliteRow) -> Result<ActorRow, AuthorityError> {
    Ok(ActorRow {
        uuid: uuid(row.try_get("uuid")?)?,
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

pub(super) async fn associate_actor(
    conn: &mut SqliteConnection,
    capability: CapId,
    actor: Uuid7,
) -> Result<(), AuthorityError> {
    sqlx::query("INSERT INTO authority_actor_issuances (cap_id, actor) VALUES (?1, ?2)")
        .bind(capability.as_bytes().as_slice())
        .bind(actor.to_string())
        .execute(conn)
        .await?;
    Ok(())
}

pub(super) async fn associated_actor(
    conn: &mut SqliteConnection,
    capability: CapId,
) -> Result<Option<Uuid7>, AuthorityError> {
    sqlx::query("SELECT actor FROM authority_actor_issuances WHERE cap_id = ?1")
        .bind(capability.as_bytes().as_slice())
        .fetch_optional(conn)
        .await?
        .map(|row| uuid(row.try_get("actor")?))
        .transpose()
}

pub(super) async fn actor_exists(
    conn: &mut SqliteConnection,
    actor: Uuid7,
) -> Result<bool, AuthorityError> {
    Ok(
        sqlx::query("SELECT 1 FROM authority_actors WHERE uuid = ?1")
            .bind(actor.to_string())
            .fetch_optional(conn)
            .await?
            .is_some(),
    )
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
    .bind(actor.uuid.to_string())
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

/// A capability and its ancestors, presented capability first, at most
/// `limit` rows. One recursive query rather than a round trip per link; rows are
/// returned as stored, retracted or not, and the caller decides what that
/// means. Parents are immutable and must exist when a row is inserted, so a
/// chain cannot cycle.
pub(super) async fn chain(
    conn: &mut SqliteConnection,
    cap: &CapId,
    limit: usize,
) -> Result<Vec<CapRow>, AuthorityError> {
    let rows = sqlx::query(
        "WITH RECURSIVE chain (depth, cap_id) AS (
             SELECT 0
                  , cap_id
               FROM authority_caps
              WHERE cap_id = ?1
              UNION ALL
             SELECT chain.depth + 1
                  , caps.parent_id
               FROM chain
               JOIN authority_caps AS caps
                 ON caps.cap_id = chain.cap_id
              WHERE caps.parent_id IS NOT NULL
                AND chain.depth + 1 < ?2
         )
         SELECT caps.cap_id
              , caps.object
              , caps.rights
              , caps.parent_id
              , caps.issuer
              , caps.retracted
           FROM chain
           JOIN authority_caps AS caps
             ON caps.cap_id = chain.cap_id
          ORDER BY chain.depth",
    )
    .bind(cap.as_bytes().as_slice())
    .bind(i64::try_from(limit).expect("chain limit fits i64"))
    .fetch_all(conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(CapRow {
                id: cap_id(row.try_get("cap_id")?)?,
                object: row.try_get("object")?,
                rights: rights(row.try_get("rights")?)?,
                parent: row
                    .try_get::<Option<Vec<u8>>, _>("parent_id")?
                    .map(cap_id)
                    .transpose()?,
                issuer: uuid(row.try_get("issuer")?)?,
                retracted: row.try_get::<i64, _>("retracted")? == 1,
            })
        })
        .collect()
}

pub(super) async fn insert_cap(
    conn: &mut SqliteConnection,
    cap: &CapRow,
) -> Result<(), AuthorityError> {
    sqlx::query(
        "INSERT INTO authority_caps
              ( cap_id
              , object
              , rights
              , parent_id
              , issuer
              , retracted
              )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(cap.id.as_bytes().as_slice())
    .bind(&cap.object)
    .bind(cap.rights.bits_i64())
    .bind(cap.parent.map(|parent| parent.as_bytes().to_vec()))
    .bind(cap.issuer.to_string())
    .bind(i64::from(cap.retracted))
    .execute(conn)
    .await?;
    Ok(())
}

/// An unretracted capability of `actor` with exactly this parent and these
/// rights. The caller has just walked `parent`, so such a row is live.
pub(super) async fn actor_cap(
    conn: &mut SqliteConnection,
    actor: Uuid7,
    parent: &CapId,
    rights: Rights,
) -> Result<Option<CapId>, AuthorityError> {
    sqlx::query(
        "SELECT caps.cap_id
           FROM authority_actor_issuances AS issued
           JOIN authority_caps AS caps ON caps.cap_id = issued.cap_id
          WHERE issued.actor = ?1
            AND caps.parent_id = ?2
            AND caps.rights = ?3
            AND caps.retracted = 0
          LIMIT 1",
    )
    .bind(actor.to_string())
    .bind(parent.as_bytes().as_slice())
    .bind(rights.bits_i64())
    .fetch_optional(conn)
    .await?
    .map(|row| cap_id(row.try_get("cap_id")?))
    .transpose()
}

pub(super) async fn retract_cap(
    conn: &mut SqliteConnection,
    cap: &CapId,
) -> Result<(), AuthorityError> {
    sqlx::query("UPDATE authority_caps SET retracted = 1 WHERE cap_id = ?1")
        .bind(cap.as_bytes().as_slice())
        .execute(conn)
        .await?;
    Ok(())
}

pub(super) async fn grants(
    conn: &mut SqliteConnection,
    issuer: Uuid7,
    subject: Uuid7,
    object: &str,
) -> Result<Vec<GrantRow>, AuthorityError> {
    let rows = sqlx::query(
        "SELECT granted_via
              , rights
           FROM authority_grants
          WHERE issuer = ?1
            AND subject = ?2
            AND object = ?3",
    )
    .bind(issuer.to_string())
    .bind(subject.to_string())
    .bind(object)
    .fetch_all(conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(GrantRow {
                via: row
                    .try_get::<Option<Vec<u8>>, _>("granted_via")?
                    .map(cap_id)
                    .transpose()?,
                rights: rights(row.try_get("rights")?)?,
            })
        })
        .collect()
}

pub(super) async fn put_grant(
    conn: &mut SqliteConnection,
    issuer: Uuid7,
    subject: Uuid7,
    object: &str,
    grant: &GrantRow,
) -> Result<(), AuthorityError> {
    sqlx::query(
        "INSERT INTO authority_grants
              ( issuer
              , subject
              , object
              , granted_via
              , rights
              )
         VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (issuer, subject, object, coalesce(granted_via, x''))
             DO UPDATE SET rights = excluded.rights",
    )
    .bind(issuer.to_string())
    .bind(subject.to_string())
    .bind(object)
    .bind(grant.via.map(|via| via.as_bytes().to_vec()))
    .bind(grant.rights.bits_i64())
    .execute(conn)
    .await?;
    Ok(())
}

/// Delete the grants of `object` to `subject` in `issuer`'s namespace: every
/// one when `via` is `None`, otherwise only the one written through `via`.
pub(super) async fn delete_grants(
    conn: &mut SqliteConnection,
    issuer: Uuid7,
    subject: Uuid7,
    object: &str,
    via: Option<&CapId>,
) -> Result<(), AuthorityError> {
    sqlx::query(
        "DELETE FROM authority_grants
          WHERE issuer = ?1
            AND subject = ?2
            AND object = ?3
            AND (?4 IS NULL OR granted_via = ?4)",
    )
    .bind(issuer.to_string())
    .bind(subject.to_string())
    .bind(object)
    .bind(via.map(|via| via.as_bytes().to_vec()))
    .execute(conn)
    .await?;
    Ok(())
}
