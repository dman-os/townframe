//! The op ledger: idempotency and crash recovery for the operations that touch both a roster and
//! Keyhive.
//!
//! A row is inserted before the first durable side effect and finished once everything the
//! receipt asserts is durable. Every op is idempotent by observed state — it grants, publishes,
//! removes or revokes only what is not already so — so an interrupted op is finished by running
//! it again, from whatever a crash left: boot runs every unfinished row, and a retry of the same
//! command resumes the row instead of being refused. What is refused is a command that is
//! executing *right now* in this process (the in-flight set) and a command id reused for a
//! different request (each row stores a digest of the request it was written for). A refusal
//! that left no side effect — denied, unknown drawer, not an entry — drops the row so the id is
//! free again; a failure that may have left one — transport, Keyhive, storage — keeps it.

use super::{DrawerId, DrawersError};
use crate::doc::CommandId;
use big_cabinet_core::dmeta::{DocId, Uuid7};
use big_cabinet_core::drawer::{DrawerAccess as Access, DrawerGroups};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row as _;
use sqlx_utils_rs::SqlCtx;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Mutex;

/// How long a completed command keeps answering replays. A retry after this re-executes.
const DONE_TTL_MICROS: i64 = 7 * 24 * 60 * 60 * 1_000_000;

/// What an interrupted operation needs to be finished, beyond the step it reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum Op {
    AddDoc {
        drawer_id: DrawerId,
        doc_id: DocId,
        access: Access,
    },
    RemoveDoc {
        drawer_id: DrawerId,
        doc_id: DocId,
    },
    AddChild {
        parent_id: DrawerId,
        child_id: DrawerId,
        access: Access,
    },
    RemoveChild {
        parent_id: DrawerId,
        child_id: DrawerId,
    },
    /// A single-group drawer. `groups` is recorded once the two groups exist, so a resume reuses
    /// them; the metadata doc is the DocsRepo command of the same id, whose own ledger says
    /// whether it was created.
    Create {
        groups: Option<DrawerGroups>,
    },
    /// A self-doc drawer declaration on an existing doc.
    CreateSelfDoc {
        doc_id: DocId,
    },
    /// A principal invited to a drawer's groups. Both memberships are re-asserted by a resume;
    /// adding a member that is already there is a no-op in Keyhive.
    Invite {
        drawer_id: DrawerId,
        principal: super::Principal,
        access: Access,
    },
    /// A principal removed from a drawer's groups.
    Revoke {
        drawer_id: DrawerId,
        principal: super::Principal,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Begun,
    Done,
}

impl Step {
    fn parse(value: &str) -> Self {
        match value {
            "begun" => Self::Begun,
            "done" => Self::Done,
            other => unreachable!("the step column is constrained: {other}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Row {
    pub(crate) command: CommandId,
    pub(crate) digest: [u8; 32],
    pub(crate) actor: Uuid7,
    pub(crate) op: Op,
    pub(crate) step: Step,
    pub(crate) receipt: Option<Value>,
}

pub(crate) struct Ledger {
    sql: SqlCtx,
    /// Commands executing in this process right now: the one thing a retry must not race.
    running: Mutex<BTreeSet<CommandId>>,
}

/// Removes the command from the in-flight set however the run ends.
struct Running<'a> {
    ledger: &'a Ledger,
    command: &'a CommandId,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.ledger
            .running
            .lock()
            .expect("the in-flight set is never poisoned")
            .remove(self.command);
    }
}

fn storage(error: sqlx::Error) -> DrawersError {
    DrawersError::Storage(format!("op ledger: {error}"))
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_micros() as i64
}

fn decode(row: sqlx::sqlite::SqliteRow) -> Row {
    Row {
        command: CommandId::try_from(row.get::<String, _>("command_id"))
            .expect("a stored command id was valid when stored"),
        digest: row
            .get::<Vec<u8>, _>("request_digest")
            .try_into()
            .expect("a stored digest is 32 bytes"),
        actor: Uuid7::try_from(
            row.get::<String, _>("actor_uuid")
                .parse::<uuid::Uuid>()
                .expect("a stored actor uuid parses"),
        )
        .expect("a stored actor uuid is a v7 uuid"),
        op: serde_json::from_str(&row.get::<String, _>("op_json"))
            .expect("a stored op was written by this service"),
        step: Step::parse(&row.get::<String, _>("step")),
        receipt: row
            .get::<Option<String>, _>("receipt_json")
            .map(|json| serde_json::from_str(&json).expect("a stored receipt is JSON")),
    }
}

impl Ledger {
    pub(crate) async fn open(sql: SqlCtx) -> Result<Self, DrawersError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS big_cabinet_drawer_ops (
                 command_id TEXT PRIMARY KEY
               , request_digest BLOB NOT NULL
               , actor_uuid TEXT NOT NULL
               , op_json TEXT NOT NULL
               , step TEXT NOT NULL CHECK (step IN ('begun', 'done'))
               , receipt_json TEXT
               , created_at INTEGER NOT NULL
               , done_at INTEGER
             ) STRICT",
        )
        .execute(&sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(Self {
            sql,
            running: Mutex::new(BTreeSet::new()),
        })
    }

    /// Run `command` once: answer a finished row's receipt, resume an unfinished one, or begin a
    /// new one around `op`. `body` receives the row's op — the one recorded, which for a resumed
    /// create names the groups already made — and returns the receipt a replay will answer with.
    pub(crate) async fn run<T, F, Fut>(
        &self,
        command: &CommandId,
        digest: [u8; 32],
        actor: Uuid7,
        op: Op,
        body: F,
    ) -> Result<T, DrawersError>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(Op) -> Fut,
        Fut: Future<Output = Result<T, DrawersError>>,
    {
        let in_flight = command.clone();
        if !self
            .running
            .lock()
            .expect("the in-flight set is never poisoned")
            .insert(in_flight)
        {
            return Err(DrawersError::CommandInFlight {
                command: command.clone(),
            });
        }
        let _running = Running {
            ledger: self,
            command,
        };
        let op = match self.get(command).await? {
            Some((row, _)) if row.digest != digest => {
                return Err(DrawersError::CommandReused {
                    command: command.clone(),
                });
            }
            Some((row, done_at)) if row.step == Step::Done => {
                if done_at.is_some_and(|at| now_micros() - at > DONE_TTL_MICROS) {
                    self.forget(command).await?;
                    self.begin(command, digest, actor, &op).await?;
                    op
                } else {
                    return Ok(serde_json::from_value(
                        row.receipt.expect("a done row stores its receipt"),
                    )
                    .expect("a stored receipt has the op's receipt shape"));
                }
            }
            Some((row, _)) => row.op,
            None => {
                self.begin(command, digest, actor, &op).await?;
                op
            }
        };
        match body(op).await {
            Ok(receipt) => {
                self.finish(command, &receipt).await?;
                Ok(receipt)
            }
            Err(error) if error.keeps_ledger_row() => Err(error),
            Err(error) => {
                self.forget(command).await?;
                Err(error)
            }
        }
    }

    async fn get(&self, command: &CommandId) -> Result<Option<(Row, Option<i64>)>, DrawersError> {
        let row = sqlx::query(
            "SELECT command_id
                  , request_digest
                  , actor_uuid
                  , op_json
                  , step
                  , receipt_json
                  , done_at
               FROM big_cabinet_drawer_ops
              WHERE command_id = ?",
        )
        .bind(command.as_str())
        .fetch_optional(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| {
            let done_at = row.get::<Option<i64>, _>("done_at");
            (decode(row), done_at)
        }))
    }

    async fn begin(
        &self,
        command: &CommandId,
        digest: [u8; 32],
        actor: Uuid7,
        op: &Op,
    ) -> Result<(), DrawersError> {
        let inserted = sqlx::query(
            "INSERT INTO big_cabinet_drawer_ops
                 ( command_id
                 , request_digest
                 , actor_uuid
                 , op_json
                 , step
                 , created_at
                 )
             VALUES (?, ?, ?, ?, 'begun', ?)
             ON CONFLICT (command_id) DO NOTHING",
        )
        .bind(command.as_str())
        .bind(digest.as_slice())
        .bind(actor.to_string())
        .bind(serde_json::to_string(op).expect("an op serializes"))
        .bind(now_micros())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        assert_eq!(
            inserted.rows_affected(),
            1,
            "a command is begun under the in-flight set, after its row was found absent"
        );
        Ok(())
    }

    /// Update what the row knows about its op: a create records its groups once they exist.
    pub(crate) async fn record(&self, command: &CommandId, op: &Op) -> Result<(), DrawersError> {
        sqlx::query(
            "UPDATE big_cabinet_drawer_ops
                SET op_json = ?
              WHERE command_id = ?",
        )
        .bind(serde_json::to_string(op).expect("an op serializes"))
        .bind(command.as_str())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// Finish a row with the receipt a replay will answer with.
    pub(crate) async fn finish<T: Serialize>(
        &self,
        command: &CommandId,
        receipt: &T,
    ) -> Result<(), DrawersError> {
        sqlx::query(
            "UPDATE big_cabinet_drawer_ops
                SET step = 'done'
                  , receipt_json = ?
                  , done_at = ?
              WHERE command_id = ?",
        )
        .bind(serde_json::to_string(receipt).expect("a receipt serializes"))
        .bind(now_micros())
        .bind(command.as_str())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    pub(crate) async fn forget(&self, command: &CommandId) -> Result<(), DrawersError> {
        sqlx::query(
            "DELETE FROM big_cabinet_drawer_ops
              WHERE command_id = ?",
        )
        .bind(command.as_str())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// Every row a crash left short of `done`, oldest first.
    pub(crate) async fn unfinished(&self) -> Result<Vec<Row>, DrawersError> {
        let rows = sqlx::query(
            "SELECT command_id
                  , request_digest
                  , actor_uuid
                  , op_json
                  , step
                  , receipt_json
               FROM big_cabinet_drawer_ops
              WHERE step != 'done'
              ORDER BY created_at",
        )
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows.into_iter().map(decode).collect())
    }

    /// Plant an unfinished row as a crash would leave it.
    #[cfg(test)]
    pub(crate) async fn plant(
        &self,
        command: &CommandId,
        digest: [u8; 32],
        actor: Uuid7,
        op: &Op,
    ) -> Result<(), DrawersError> {
        self.begin(command, digest, actor, op).await
    }
}
