//! The command ledger: idempotency and crash recovery for creates, writes, branches and merges.
//!
//! A row is inserted **before** the first durable side effect of its command and advanced
//! `pending → committed → done`; `done` is written only once everything the receipt asserts is
//! durable. A completed command replays its stored receipt; one that is not `done` is refused as
//! in flight until the boot pass resolves it. Each row stores a digest of the request it was
//! written for, so a command id reused for a different request is refused rather than answered
//! with an unrelated receipt.
//!
//! The boot pass resolves what a crash left behind using facts that already live elsewhere, never
//! a second copy of them:
//! - a create's BigRepo reservation and Keyhive document say which side of `commit_id` the crash
//!   fell on;
//! - a write's Automerge change carries the command id as its commit message, so whether it was
//!   committed is read off the document itself;
//! - a branch is a create whose initial content is the origin's changes, so its document says the
//!   same as a create's, and its origin backlink is a write that carries the command id;
//! - a merge adds no change of its own, so it happened exactly when the target holds the source
//!   heads the row recorded.

use super::{CommandId, DocsError};
use automerge::ChangeHash;
use big_cabinet_core::dmeta::{DocId, Uuid7};
use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx_utils_rs::SqlCtx;

/// How long a completed command keeps answering replays. A retry after this re-executes.
pub(super) const DONE_TTL_MICROS: i64 = 7 * 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Create,
    Write,
    Branch,
    Merge,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Write => "write",
            Self::Branch => "branch",
            Self::Merge => "merge",
        }
    }
}

/// What a branch or a merge needs to be settled after a crash, beyond the document the row names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(super) enum Detail {
    Branch {
        origin: DocId,
        path: String,
        stealth: bool,
    },
    /// The row's document is the target; `heads` are the source heads merged into it.
    Merge {
        source: DocId,
        heads: Vec<ChangeHash>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum State {
    Pending,
    Committed,
    Done,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Committed => "committed",
            Self::Done => "done",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Row {
    pub command: CommandId,
    pub kind: Kind,
    pub digest: [u8; 32],
    pub actor: Uuid7,
    pub document: Option<DocId>,
    pub placement: Option<String>,
    pub detail: Option<Detail>,
    pub state: State,
    pub heads: Vec<ChangeHash>,
}

pub(super) struct Commands {
    sql: SqlCtx,
}

fn storage(error: sqlx::Error) -> DocsError {
    DocsError::Repository(format!("command ledger: {error}"))
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_micros() as i64
}

impl Commands {
    pub(super) async fn open(sql: SqlCtx) -> Result<Self, DocsError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_cabinet_commands (
                 command_id TEXT PRIMARY KEY
               , kind TEXT NOT NULL CHECK (kind IN ('create', 'write', 'branch', 'merge'))
               , request_digest BLOB NOT NULL
               , actor_uuid TEXT NOT NULL
               , doc_id TEXT
               , placement TEXT
               , detail_json TEXT
               , state TEXT NOT NULL CHECK (state IN ('pending', 'committed', 'done'))
               , heads_json TEXT NOT NULL
               , created_at INTEGER NOT NULL
               , done_at INTEGER
             ) STRICT",
        )
        .execute(&sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(Self { sql })
    }

    pub(super) async fn get(&self, command: &CommandId) -> Result<Option<Row>, DocsError> {
        let row = sqlx::query(
            "SELECT command_id
                  , kind
                  , request_digest
                  , actor_uuid
                  , doc_id
                  , placement
                  , detail_json
                  , state
                  , heads_json
               FROM big_cabinet_commands
              WHERE command_id = ?",
        )
        .bind(command.as_str())
        .fetch_optional(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(row.map(decode))
    }

    /// Insert a pending row. A row that already exists means a concurrent or earlier command with
    /// the same id, which is in flight from this command's point of view.
    #[allow(clippy::too_many_arguments)] // one per column of the row it inserts
    pub(super) async fn begin(
        &self,
        command: &CommandId,
        kind: Kind,
        digest: [u8; 32],
        actor: Uuid7,
        document: Option<&DocId>,
        placement: Option<&str>,
        detail: Option<&Detail>,
    ) -> Result<(), DocsError> {
        let inserted = sqlx::query(
            "INSERT INTO big_cabinet_commands
                 ( command_id
                 , kind
                 , request_digest
                 , actor_uuid
                 , doc_id
                 , placement
                 , detail_json
                 , state
                 , heads_json
                 , created_at
                 )
             VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', '[]', ?)
             ON CONFLICT (command_id) DO NOTHING",
        )
        .bind(command.as_str())
        .bind(kind.as_str())
        .bind(digest.as_slice())
        .bind(actor.to_string())
        .bind(document.map(ToString::to_string))
        .bind(placement)
        .bind(detail.map(|detail| serde_json::to_string(detail).expect("a detail serializes")))
        .bind(now_micros())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        if inserted.rows_affected() == 0 {
            return Err(DocsError::CommandInFlight {
                command: command.clone(),
            });
        }
        Ok(())
    }

    pub(super) async fn set_document(
        &self,
        command: &CommandId,
        document: &DocId,
    ) -> Result<(), DocsError> {
        let updated = sqlx::query(
            "UPDATE big_cabinet_commands
                SET doc_id = ?
              WHERE command_id = ?
                AND state = 'pending'
                AND doc_id IS NULL",
        )
        .bind(document.to_string())
        .bind(command.as_str())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        assert_eq!(updated.rows_affected(), 1, "a pending creating row is ours");
        Ok(())
    }

    pub(super) async fn advance(
        &self,
        command: &CommandId,
        to: State,
        heads: &[ChangeHash],
    ) -> Result<(), DocsError> {
        let from: &[&str] = match to {
            State::Pending => unreachable!("rows start pending"),
            State::Committed => &["pending"],
            State::Done => &["pending", "committed"],
        };
        let updated = sqlx::query(
            "UPDATE big_cabinet_commands
                SET state = ?
                  , heads_json = ?
                  , done_at = CASE WHEN ? = 'done' THEN ? ELSE done_at END
              WHERE command_id = ?
                AND state IN (?, ?)",
        )
        .bind(to.as_str())
        .bind(serde_json::to_string(heads).expect("heads serialize"))
        .bind(to.as_str())
        .bind(now_micros())
        .bind(command.as_str())
        .bind(from[0])
        .bind(from[from.len() - 1])
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        assert_eq!(
            updated.rows_affected(),
            1,
            "command {command} advances from {from:?} to {to:?}"
        );
        Ok(())
    }

    pub(super) async fn discard(&self, command: &CommandId) -> Result<(), DocsError> {
        sqlx::query(
            "DELETE FROM big_cabinet_commands
              WHERE command_id = ?
                AND state != 'done'",
        )
        .bind(command.as_str())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    pub(super) async fn unresolved(&self) -> Result<Vec<Row>, DocsError> {
        let rows = sqlx::query(
            "SELECT command_id
                  , kind
                  , request_digest
                  , actor_uuid
                  , doc_id
                  , placement
                  , detail_json
                  , state
                  , heads_json
               FROM big_cabinet_commands
              WHERE state != 'done'
              ORDER BY created_at",
        )
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows.into_iter().map(decode).collect())
    }

    pub(super) async fn purge_done(&self) -> Result<(), DocsError> {
        sqlx::query(
            "DELETE FROM big_cabinet_commands
              WHERE state = 'done'
                AND done_at < ?",
        )
        .bind(now_micros() - DONE_TTL_MICROS)
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(())
    }
}

fn decode(row: sqlx::sqlite::SqliteRow) -> Row {
    let text = |column: &str| -> String { row.get(column) };
    let digest: Vec<u8> = row.get("request_digest");
    Row {
        command: CommandId::try_from(text("command_id")).expect("stored command ids are valid"),
        kind: match text("kind").as_str() {
            "create" => Kind::Create,
            "write" => Kind::Write,
            "branch" => Kind::Branch,
            "merge" => Kind::Merge,
            other => unreachable!("kind is checked by the table: {other}"),
        },
        digest: digest.try_into().expect("a request digest is 32 bytes"),
        actor: text("actor_uuid")
            .parse::<uuid::Uuid>()
            .expect("stored actor UUIDs parse")
            .try_into()
            .expect("stored actor UUIDs are v7"),
        document: row
            .get::<Option<String>, _>("doc_id")
            .map(|document| document.parse().expect("stored document ids parse")),
        placement: row.get("placement"),
        detail: row
            .get::<Option<String>, _>("detail_json")
            .map(|detail| serde_json::from_str(&detail).expect("stored details parse")),
        state: match text("state").as_str() {
            "pending" => State::Pending,
            "committed" => State::Committed,
            "done" => State::Done,
            other => unreachable!("state is checked by the table: {other}"),
        },
        heads: serde_json::from_str(&text("heads_json")).expect("stored heads parse"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(label: &str) -> CommandId {
        CommandId::try_from(label.to_string()).expect("a valid command id")
    }

    async fn ledger() -> Commands {
        Commands::open(SqlCtx::memory().await.expect("sqlite"))
            .await
            .expect("the ledger opens")
    }

    async fn begin_pending(commands: &Commands, id: &CommandId) -> Row {
        let row = Row {
            command: id.clone(),
            kind: Kind::Create,
            digest: [7; 32],
            actor: Uuid7::new(),
            document: None,
            placement: Some("drawer:a".to_string()),
            detail: None,
            state: State::Pending,
            heads: Vec::new(),
        };
        commands
            .begin(
                id,
                row.kind,
                row.digest,
                row.actor,
                row.document.as_ref(),
                row.placement.as_deref(),
                row.detail.as_ref(),
            )
            .await
            .expect("begin");
        row
    }

    fn assert_rows(actual: impl IntoIterator<Item = Row>, expected: impl IntoIterator<Item = Row>) {
        let fields = |row: Row| {
            (
                row.command,
                row.kind,
                row.digest,
                row.actor,
                row.document,
                row.placement,
                row.detail,
                row.state,
                row.heads,
            )
        };
        assert_eq!(
            actual.into_iter().map(fields).collect::<Vec<_>>(),
            expected.into_iter().map(fields).collect::<Vec<_>>(),
        );
    }

    const COMMITTED_HEADS: &[ChangeHash] = &[ChangeHash([1; 32]), ChangeHash([2; 32])];
    const DONE_HEADS: &[ChangeHash] = &[ChangeHash([3; 32])];

    #[derive(Debug, Clone, Copy)]
    enum Op {
        SetDoc,
        Advance(State),
        Discard,
    }

    utils_rs::table_tests! {
        begin_cases tokio,
        (kind, document, placement, detail),
        {
            let commands = ledger().await;
            let id = command("cmd-begin");
            assert_rows(commands.get(&id).await.expect("get missing"), []);
            let row = Row {
                command: id.clone(), kind, digest: [7; 32], actor: Uuid7::new(),
                document, placement: placement.map(str::to_string), detail,
                state: State::Pending, heads: Vec::new(),
            };
            commands
                .begin(&id, row.kind, row.digest, row.actor, row.document.as_ref(), row.placement.as_deref(), row.detail.as_ref())
                .await
                .expect("begin");
            let reopened = Commands::open(commands.sql.clone()).await.expect("reopen");
            assert_rows(reopened.get(&id).await.expect("get"), [row]);
        },
    }

    begin_cases! {
        create_before_reservation: (Kind::Create, None, Some("drawer:a"), None),
        write_with_document: (Kind::Write, Some(DocId::new([9; 32])), None::<&str>, None),
        branch_before_reservation: (
            Kind::Branch,
            None,
            Some("drawer:a"),
            Some(Detail::Branch { origin: DocId::new([8; 32]), path: "/draft".into(), stealth: false }),
        ),
        merge_with_source_heads: (
            Kind::Merge,
            Some(DocId::new([9; 32])),
            None::<&str>,
            Some(Detail::Merge { source: DocId::new([8; 32]), heads: COMMITTED_HEADS.to_vec() }),
        ),
    }

    utils_rs::table_tests! {
        begin_conflicts tokio,
        (state, digest),
        {
            let commands = ledger().await;
            let id = command("cmd-conflict");
            let mut row = begin_pending(&commands, &id).await;
            if state != State::Pending {
                commands.advance(&id, state, COMMITTED_HEADS).await.expect("advance");
                row.state = state;
                row.heads = COMMITTED_HEADS.to_vec();
            }
            assert_eq!(
                commands.begin(&id, Kind::Write, digest, Uuid7::new(), Some(&DocId::new([9; 32])), Some("drawer:b"), None).await,
                Err(DocsError::CommandInFlight { command: id.clone() }),
            );
            assert_rows(commands.get(&id).await.expect("get"), [row]);
        },
    }

    begin_conflicts! {
        pending_same_request_digest: (State::Pending, [7; 32]),
        pending_different_request_digest: (State::Pending, [8; 32]),
        committed_same_request_digest: (State::Committed, [7; 32]),
        committed_different_request_digest: (State::Committed, [8; 32]),
        done_same_request_digest: (State::Done, [7; 32]),
        done_different_request_digest: (State::Done, [8; 32]),
    }

    utils_rs::table_tests! {
        transition_cases tokio,
        (ops, expected),
        {
            let ops: &[Op] = ops;
            let expected: Option<(State, Option<DocId>, &[ChangeHash])> = expected;
            let commands = ledger().await;
            let id = command("cmd-transition");
            let initial = begin_pending(&commands, &id).await;
            for op in ops {
                match op {
                    Op::SetDoc => commands
                        .set_document(&id, &DocId::new([3; 32]))
                        .await
                        .expect("set_document"),
                    Op::Advance(state) => {
                        let heads = if *state == State::Done { DONE_HEADS } else { COMMITTED_HEADS };
                        commands.advance(&id, *state, heads).await.expect("advance");
                    }
                    Op::Discard => commands.discard(&id).await.expect("discard"),
                }
            }
            let done_at: Option<Option<i64>> = sqlx::query_scalar(
                "SELECT done_at
                   FROM big_cabinet_commands
                  WHERE command_id = ?",
            )
            .bind(id.as_str())
            .fetch_optional(&commands.sql.read_pool)
            .await
            .expect("completion timestamp");
            assert_eq!(done_at.map(|timestamp| timestamp.is_some()), expected.as_ref().map(|(state, _, _)| *state == State::Done));
            assert_rows(
                commands.get(&id).await.expect("get"),
                expected.map(|(state, document, heads)| Row { state, document, heads: heads.to_vec(), ..initial }),
            );
        },
        enable_tracing: false,
    }

    transition_cases! {
        straight_to_done: (&[Op::Advance(State::Done)], Some((State::Done, None, DONE_HEADS))),
        commit_then_done_replaces_heads: (&[Op::Advance(State::Committed), Op::Advance(State::Done)], Some((State::Done, None, DONE_HEADS))),
        set_document_then_commit: (&[Op::SetDoc, Op::Advance(State::Committed)], Some((State::Committed, Some(DocId::new([3; 32])), COMMITTED_HEADS))),
        discard_pending: (&[Op::Discard], None),
        discard_committed: (&[Op::Advance(State::Committed), Op::Discard], None),
        discard_missing_is_idempotent: (&[Op::Discard, Op::Discard], None),
        done_survives_discard: (&[Op::Advance(State::Done), Op::Discard], Some((State::Done, None, DONE_HEADS))),
        advance_to_pending [should_panic(expected = "rows start pending")]: (&[Op::Advance(State::Pending)], None),
        recommit_after_commit [should_panic(expected = "advances from")]: (&[Op::Advance(State::Committed), Op::Advance(State::Committed)], None),
        commit_after_done [should_panic(expected = "advances from")]: (&[Op::Advance(State::Done), Op::Advance(State::Committed)], None),
        done_after_done [should_panic(expected = "advances from")]: (&[Op::Advance(State::Done), Op::Advance(State::Done)], None),
        advance_after_discard [should_panic(expected = "advances from")]: (&[Op::Discard, Op::Advance(State::Committed)], None),
        set_document_twice [should_panic(expected = "a pending creating row is ours")]: (&[Op::SetDoc, Op::SetDoc], None),
        set_document_after_commit [should_panic(expected = "a pending creating row is ours")]: (&[Op::Advance(State::Committed), Op::SetDoc], None),
        set_document_after_done [should_panic(expected = "a pending creating row is ours")]: (&[Op::Advance(State::Done), Op::SetDoc], None),
    }

    utils_rs::table_tests! {
        unresolved_cases tokio,
        (states, expected_order),
        {
            let states: &[(State, i64)] = states;
            let expected_order: &[usize] = expected_order;
            let commands = ledger().await;
            let mut rows = Vec::new();
            for (index, &(state, created_at)) in states.iter().enumerate() {
                let mut row = begin_pending(&commands, &command(&format!("cmd-{index}"))).await;
                let document = DocId::new([index as u8; 32]);
                commands.set_document(&row.command, &document).await.expect("set_document");
                row.document = Some(document);
                if state != State::Pending {
                    commands.advance(&row.command, state, COMMITTED_HEADS).await.expect("advance");
                    row.state = state;
                    row.heads = COMMITTED_HEADS.to_vec();
                }
                sqlx::query(
                    "UPDATE big_cabinet_commands
                        SET created_at = ?
                      WHERE command_id = ?",
                )
                .bind(created_at)
                .bind(row.command.as_str())
                .execute(&commands.sql.write_pool)
                .await
                .expect("set creation order");
                rows.push(row);
            }
            assert_rows(
                commands.unresolved().await.expect("unresolved"),
                expected_order.iter().map(|&index| rows[index].clone()),
            );
        },
    }

    unresolved_cases! {
        empty_ledger: (&[], &[]),
        all_done: (&[(State::Done, 1), (State::Done, 0)], &[]),
        unresolved_in_creation_order: (&[(State::Committed, 2), (State::Pending, 1)], &[1, 0]),
        mixed_states_exclude_done_and_preserve_order: (&[(State::Pending, 30), (State::Done, 10), (State::Committed, 20), (State::Pending, 0)], &[3, 2, 0]),
    }

    utils_rs::table_tests! {
        purge_done_cases tokio,
        (state, age, survives),
        {
            let age: Option<i64> = age;
            let commands = ledger().await;
            let id = command("cmd-purge");
            let mut row = begin_pending(&commands, &id).await;
            if state != State::Pending {
                commands.advance(&id, state, DONE_HEADS).await.expect("advance");
                row.state = state;
                row.heads = DONE_HEADS.to_vec();
            }
            let now = now_micros();
            sqlx::query(
                "UPDATE big_cabinet_commands
                    SET created_at = ?
                      , done_at = ?
                  WHERE command_id = ?",
            )
            .bind(now - 2 * DONE_TTL_MICROS)
            .bind(age.map(|age| now - age))
            .bind(id.as_str())
            .execute(&commands.sql.write_pool)
            .await
            .expect("backdate");
            commands.purge_done().await.expect("purge");
            assert_rows(commands.get(&id).await.expect("get"), survives.then_some(row));
            if !survives {
                let replacement = begin_pending(&commands, &id).await;
                assert_rows(commands.get(&id).await.expect("get reused id"), [replacement]);
            }
        },
    }

    purge_done_cases! {
        fresh_done_ignores_old_creation_time: (State::Done, Some(0), true),
        recent_done_survives: (State::Done, Some(DONE_TTL_MICROS - 60_000_000), true),
        expired_done_is_purged_and_id_reusable: (State::Done, Some(DONE_TTL_MICROS + 1), false),
        pending_without_done_at_survives: (State::Pending, None, true),
        committed_without_done_at_survives: (State::Committed, None, true),
        pending_with_old_done_at_survives: (State::Pending, Some(DONE_TTL_MICROS + 1), true),
        committed_with_old_done_at_survives: (State::Committed, Some(DONE_TTL_MICROS + 1), true),
    }
}
