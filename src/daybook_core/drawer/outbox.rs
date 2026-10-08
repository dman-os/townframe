//! The drawer's SQLite write-ahead outbox for document adds (ADR 003 §19).
//!
//! Every add (`add`/`batch_add`/`add_temporary`) first writes a row keyed by
//! the caller's idempotency key BEFORE `commit_id` runs — write-ahead is the
//! integrity requirement — and advances it through
//! `pending-add → committed-staged → done`. `done` is reached only when the
//! `docs.map` entry is durable (and the partitions re-derived), never when the
//! in-memory flow returns. Boot reconciliation (see `reconciliation.rs`)
//! replays surviving rows; pending rows are NEVER aged out — they are resolved
//! by the boot loop or the caller's retry.
//!
//! Retry contract: the same key with a `done` row returns the same `doc_id`
//! without re-executing; a key already in flight is refused rather than
//! resumed (two executions of the registration sequence must not race). After
//! the TTL purge a late retry re-executes as a fresh add — a new `doc_id` for
//! the same key — because the durable record that named the old one is gone.

use crate::interlude::*;

use super::types::DocEntry;
use daybook_types::doc::ChangeHashSet;

/// Done rows older than this are purged at boot (`done_at`, days not minutes).
const DONE_TTL: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);

/// The lifecycle state of one outbox row. Serialized as its SQL spelling;
/// `state` also doubles as the table's check constraint vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutboxState {
    /// Written right before `commit_id` runs; a row stopped here is a crash
    /// inside or before the commit.
    PendingAdd,
    /// `commit_id` succeeded: keyhive document + events + sedimentree exist
    /// locally. For the temporary flow this is where commit-point semantics
    /// live; the registration (grants + `docs.map` entry) may not have landed.
    CommittedStaged,
    /// The docs.map entry is durable and the partitions re-derived. Purgeable.
    Done,
}

impl OutboxState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::PendingAdd => "pending-add",
            Self::CommittedStaged => "committed-staged",
            Self::Done => "done",
        }
    }

    fn from_db(state: &str) -> Self {
        match state {
            "pending-add" => Self::PendingAdd,
            "committed-staged" => Self::CommittedStaged,
            "done" => Self::Done,
            other => panic!("unknown outbox state in the drawer db: {other}"),
        }
    }
}

/// One write-ahead row of the add outbox (see the module docs).
#[derive(Debug, Clone)]
pub(crate) struct OutboxRow {
    pub idempotency_key: String,
    pub branch_doc_id: DocumentId,
    pub entry: DocEntry,
    pub staged_branch_heads: ChangeHashSet,
    pub state: OutboxState,
    /// Whether the row is a temporary (staging) add: an `add_temporary`
    /// receipt whose registration is decided by its durable `cipherBlob`
    /// claim (ADR 003 §19) — unclaimed, it stays unregistered, unlike an
    /// ordinary add whose outbox row IS the durable intent to register.
    pub temporary: bool,
}

/// The sqlx row shape of `drawer_add_outbox`'s columns.
type OutboxDbRow = (
    String,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    String,
    bool,
    i64,
    Option<i64>,
);

// ─── row encoding ──────────────────────────────────────────────────────────
//
// Both stored metadata types are `Reconcile/Hydrate` (automerge-native, like
// the drawer doc itself), so the rows serialize through the same mechanism
// and no serde derives are invented for them. The plaintext-SQL decision is
// the interim default; it is revisitable by changing exactly these helpers.

fn encode_entry(entry: &DocEntry) -> Res<Vec<u8>> {
    let mut doc = automerge::AutoCommit::new();
    autosurgeon::reconcile(&mut doc, entry)?;
    Ok(doc.save_nocompress())
}

fn decode_entry(bytes: &[u8]) -> Res<DocEntry> {
    let doc = automerge::Automerge::load(bytes)?;
    autosurgeon::hydrate(&doc).map_err(|err| ferr!("outbox entry decode failed: {err:?}"))
}

// Heads serialize through `am_utils_rs`'s string-vector codecs — the same
// spelling `add_branch_to_partitions_if_needed` puts in a part payload —
// because a bare seq does not reconcile at autosurgeon's top level.
fn encode_heads(heads: &ChangeHashSet) -> Res<Vec<u8>> {
    Ok(serde_json::to_vec(&am_utils_rs::serialize_commit_heads(
        heads.0.as_ref(),
    ))?)
}

fn decode_heads(bytes: &[u8]) -> Res<ChangeHashSet> {
    let heads: Vec<String> = serde_json::from_slice(bytes)?;
    Ok(ChangeHashSet(am_utils_rs::parse_commit_heads(&heads)?))
}

// ─── schema ────────────────────────────────────────────────────────────────

/// `drawer_add_outbox` rows are the write-ahead reconciliation record for
/// document adds: durably inserted before `commit_id`, advanced to `done` only
/// when the `docs.map` entry is durable. Boot reconciliation replays rows that
/// are not `done`; the boot purge deletes strictly `done` rows past the TTL, so
/// a late retry after a purge re-executes as a fresh add under the same key.
pub(crate) async fn ensure_outbox_schema(sql: &SqlCtx) -> Res<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS drawer_add_outbox (
            idempotency_key TEXT NOT NULL PRIMARY KEY,
            branch_doc_id BLOB NOT NULL,
            entry BLOB NOT NULL,
            staged_branch_heads BLOB NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('pending-add', 'committed-staged', 'done')),
            temporary INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            done_at INTEGER NULL
        ) STRICT
        "#,
    )
    .execute(&sql.write_pool)
    .await?;
    Ok(())
}

pub(crate) async fn get_row(sql: &SqlCtx, idempotency_key: &str) -> Res<Option<OutboxRow>> {
    let row = sqlx::query_as::<_, OutboxDbRow>(
        "SELECT idempotency_key, branch_doc_id, entry, staged_branch_heads, state, temporary, \
              created_at, done_at FROM drawer_add_outbox WHERE idempotency_key = ?1",
    )
    .bind(idempotency_key)
    .fetch_optional(&sql.write_pool)
    .await?;
    row.map(row_from_db).transpose()
}

pub(crate) async fn get_row_for_branch(
    sql: &SqlCtx,
    branch_doc_id: &DocumentId,
) -> Res<Option<OutboxRow>> {
    let row = sqlx::query_as::<_, OutboxDbRow>(
        "SELECT idempotency_key, branch_doc_id, entry, staged_branch_heads, state, temporary, \
              created_at, done_at FROM drawer_add_outbox WHERE branch_doc_id = ?1",
    )
    .bind(branch_doc_id.as_bytes())
    .fetch_optional(&sql.write_pool)
    .await?;
    row.map(row_from_db).transpose()
}

/// Every row that boot reconciliation still owes work for (`state != 'done'`).
pub(crate) async fn list_pending_rows(sql: &SqlCtx) -> Res<Vec<OutboxRow>> {
    let rows = sqlx::query_as::<_, OutboxDbRow>(
        "SELECT idempotency_key, branch_doc_id, entry, staged_branch_heads, state, temporary, \
              created_at, done_at FROM drawer_add_outbox WHERE state != 'done' ORDER BY created_at",
    )
    .fetch_all(&sql.write_pool)
    .await?;
    rows.into_iter().map(row_from_db).collect()
}

fn row_from_db(
    (key, branch_doc_id, entry, heads, state, temporary, _created_at, _done_at): OutboxDbRow,
) -> Res<OutboxRow> {
    Ok(OutboxRow {
        idempotency_key: key,
        branch_doc_id: DocumentId::new(
            TryInto::<[u8; 32]>::try_into(branch_doc_id)
                .map_err(|err| ferr!("outbox branch doc id is not 32 bytes: {err:?}"))?,
        ),
        entry: decode_entry(&entry)?,
        staged_branch_heads: decode_heads(&heads)?,
        state: OutboxState::from_db(&state),
        temporary,
    })
}

// ─── row writes ────────────────────────────────────────────────────────────

/// The write-ahead step of an add: the row must be durable before `commit_id`
/// runs. A key that is somehow already present errors — the caller deduped or
/// raced against an in-flight key, and replaying a key twice must not produce
/// a second row.
pub(crate) async fn insert_pending(
    sql: &SqlCtx,
    idempotency_key: &str,
    branch_doc_id: &DocumentId,
    entry: &DocEntry,
    staged_branch_heads: &ChangeHashSet,
    temporary: bool,
) -> Res<()> {
    sqlx::query(
        r#"
        INSERT INTO drawer_add_outbox (
            idempotency_key, branch_doc_id, entry, staged_branch_heads, state, temporary, created_at
        ) VALUES (?1, ?2, ?3, ?4, 'pending-add', ?5, ?6)
        "#,
    )
    .bind(idempotency_key)
    .bind(branch_doc_id.as_bytes())
    .bind(encode_entry(entry)?)
    .bind(encode_heads(staged_branch_heads)?)
    .bind(temporary)
    .bind(jiff::Timestamp::now().as_microsecond())
    .execute(&sql.write_pool)
    .await
    .map_err(|err| {
        ferr!(
            "outbox insert failed (is idempotency key '{idempotency_key}' already in flight?): {err}"
        )
    })?;
    Ok(())
}

/// `commit_id` succeeded; the keyhive authority and sedimentree exist locally.
pub(crate) async fn mark_committed_staged(sql: &SqlCtx, idempotency_key: &str) -> Res<()> {
    set_state(sql, idempotency_key, OutboxState::CommittedStaged).await
}

/// The docs.map entry is durable (and the re-derivable residuals done).
pub(crate) async fn mark_done(sql: &SqlCtx, idempotency_key: &str) -> Res<()> {
    set_state(sql, idempotency_key, OutboxState::Done).await
}

async fn set_state(sql: &SqlCtx, idempotency_key: &str, state: OutboxState) -> Res<()> {
    let done_at = (state == OutboxState::Done).then(|| jiff::Timestamp::now().as_microsecond());
    let updated = sqlx::query(
        "UPDATE drawer_add_outbox SET state = ?1, done_at = ?2 WHERE idempotency_key = ?3",
    )
    .bind(state.as_str())
    .bind(done_at)
    .bind(idempotency_key)
    .execute(&sql.write_pool)
    .await?
    .rows_affected();
    eyre::ensure!(
        updated == 1,
        "outbox row '{idempotency_key}' vanished mid-flight"
    );
    Ok(())
}

/// Terminal removal for a discarded (or abandoned-resolved) add; used by the
/// boot reconciliation's abandon branch and `discard_temporary`.
pub(crate) async fn delete_row(sql: &SqlCtx, idempotency_key: &str) -> Res<()> {
    sqlx::query("DELETE FROM drawer_add_outbox WHERE idempotency_key = ?1")
        .bind(idempotency_key)
        .execute(&sql.write_pool)
        .await?;
    Ok(())
}

/// Boot purge: strictly `done` rows past the TTL. Pending rows are never aged
/// out — the boot loop or the caller's retry resolves them.
pub(crate) async fn purge_done_rows(sql: &SqlCtx) -> Res<u64> {
    let cutoff = (jiff::Timestamp::now().as_microsecond() - as_micros(DONE_TTL)).max(0);
    let purged = sqlx::query(
        "DELETE FROM drawer_add_outbox WHERE state = 'done' AND done_at IS NOT NULL AND done_at < ?1",
    )
    .bind(cutoff)
    .execute(&sql.write_pool)
    .await?
    .rows_affected();
    Ok(purged)
}

fn as_micros(ttl: std::time::Duration) -> i64 {
    i64::try_from(ttl.as_micros()).expect("days-scale TTL fits in i64 micros")
}

#[cfg(test)]
mod row_encoding_tests {
    use super::*;
    use crate::drawer::types::StoredBranchRef;
    use crate::stores::VersionTag;

    /// The outbox row encoding round-trips: an entry and its branch heads come
    /// back field-identical — the record a replay commits must be the one the
    /// caller staged.
    #[tokio::test]
    async fn outbox_row_metadata_round_trips_through_the_encoding() -> Res<()> {
        let entry = DocEntry {
            branches: [(
                "main".to_string(),
                StoredBranchRef {
                    branch_doc_id: DocumentId::new([0_u8; 32]),
                },
            )]
            .into(),
            branches_deleted: HashMap::new(),
            vtag: VersionTag::mint(automerge::ActorId::from([7_u8; 16])),
            previous_version_heads: Some(ChangeHashSet(Arc::from([automerge::ChangeHash(
                [0_u8; 32],
            )]))),
        };
        let back = decode_entry(&encode_entry(&entry)?)?;
        assert_eq!(
            back.branches.get("main").map(|r| r.branch_doc_id.clone()),
            Some(DocumentId::new([0_u8; 32]))
        );
        assert_eq!(back.vtag.version, entry.vtag.version);
        assert!(
            back.previous_version_heads
                .is_some_and(|heads| !heads.0.is_empty())
        );

        let heads = ChangeHashSet(Arc::from([automerge::ChangeHash([0_u8; 32])]));
        let heads_back = decode_heads(&encode_heads(&heads)?)?;
        assert_eq!(heads_back.0.len(), 1);
        assert_eq!(heads_back.0[0], heads.0[0]);
        eyre::Ok(())
    }
}
