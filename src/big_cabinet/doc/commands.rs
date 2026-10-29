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

use super::sqlite;
use automerge::ChangeHash;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocsError;
use serde::Deserialize;
use serde::Serialize;
use sqlite_rs::Database;
use sqlite_rs::SqlError;

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
    Create {
        owner_id: Option<Uuid7>,
    },
    Branch {
        origin: DocId,
        path: String,
        stealth: bool,
        owner_id: Option<Uuid7>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    sql: Database,
}

fn storage(error: SqlError) -> DocsError {
    DocsError::Repository(format!("command ledger: {error}"))
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_micros() as i64
}

impl Commands {
    pub(super) async fn open(sql: Database) -> Result<Self, DocsError> {
        sql.apply_migrations(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
            .await
            .map_err(storage)?;
        Ok(Self { sql })
    }

    pub(super) async fn get(&self, command: &CommandId) -> Result<Option<Row>, DocsError> {
        let row = sqlite::get_command(&self.sql, command.as_str().to_owned())
            .await
            .map_err(storage)?;
        Ok(row.map(decode))
    }

    /// Insert a pending row. A row that already exists means a concurrent or earlier command with
    /// the same id, which is in flight from this command's point of view.
    #[expect(
        clippy::too_many_arguments,
        reason = "one per column of the row it inserts"
    )]
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
        let inserted = sqlite::begin_command(
            &self.sql,
            command.as_str().to_owned(),
            kind.as_str().to_owned(),
            digest.to_vec(),
            actor.to_string(),
            document.map(ToString::to_string),
            placement.map(str::to_owned),
            detail.map(|detail| serde_json::to_string(detail).expect("a detail serializes")),
            now_micros(),
        )
        .await
        .map_err(storage)?;
        if inserted.rows_affected == 0 {
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
        let updated = sqlite::set_command_document(
            &self.sql,
            document.to_string(),
            command.as_str().to_owned(),
        )
        .await
        .map_err(storage)?;
        assert_eq!(updated.rows_affected, 1, "a pending creating row is ours");
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
        let updated = sqlite::advance_command(
            &self.sql,
            to.as_str().to_owned(),
            serde_json::to_string(heads).expect("heads serialize"),
            now_micros(),
            command.as_str().to_owned(),
            from[0].to_owned(),
            from[from.len() - 1].to_owned(),
        )
        .await
        .map_err(storage)?;
        assert_eq!(
            updated.rows_affected, 1,
            "command {command} advances from {from:?} to {to:?}"
        );
        Ok(())
    }

    pub(super) async fn discard(&self, command: &CommandId) -> Result<(), DocsError> {
        sqlite::discard_command(&self.sql, command.as_str().to_owned())
            .await
            .map_err(storage)?;
        Ok(())
    }

    pub(super) async fn unresolved(&self) -> Result<Vec<Row>, DocsError> {
        let rows = sqlite::unresolved_commands(&self.sql)
            .await
            .map_err(storage)?;
        Ok(rows.into_iter().map(decode).collect())
    }

    pub(super) async fn purge_done(&self) -> Result<(), DocsError> {
        sqlite::purge_done_commands(&self.sql, now_micros() - DONE_TTL_MICROS)
            .await
            .map_err(storage)?;
        Ok(())
    }
}

fn decode(row: sqlite::GetCommandRow) -> Row {
    Row {
        command: CommandId::try_from(row.command_id).expect("stored command ids are valid"),
        kind: match row.kind.as_str() {
            "create" => Kind::Create,
            "write" => Kind::Write,
            "branch" => Kind::Branch,
            "merge" => Kind::Merge,
            other => unreachable!("kind is checked by the table: {other}"),
        },
        digest: row
            .request_digest
            .try_into()
            .expect("a request digest is 32 bytes"),
        actor: row
            .actor_uuid
            .parse::<uuid::Uuid>()
            .expect("stored actor UUIDs parse")
            .try_into()
            .expect("stored actor UUIDs are v7"),
        document: row
            .doc_id
            .map(|document| document.parse().expect("stored document ids parse")),
        placement: row.placement,
        detail: row
            .detail_json
            .map(|detail| serde_json::from_str(&detail).expect("stored details parse")),
        state: match row.state.as_str() {
            "pending" => State::Pending,
            "committed" => State::Committed,
            "done" => State::Done,
            other => unreachable!("state is checked by the table: {other}"),
        },
        heads: serde_json::from_str(&row.heads_json).expect("stored heads parse"),
    }
}

#[cfg(test)]
mod tests;
