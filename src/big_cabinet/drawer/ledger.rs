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

use super::sqlite;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::DrawerGroups;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawersError;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlite_rs::Database;
use sqlite_rs::SqlError;
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
        owner_id: Option<Uuid7>,
    },
    /// A self-doc drawer declaration on an existing doc.
    CreateSelfDoc {
        doc_id: DocId,
        owner_id: Option<Uuid7>,
    },
    /// A principal invited to a drawer's groups. Both memberships are re-asserted by a resume;
    /// adding a member that is already there is a no-op in Keyhive.
    Invite {
        drawer_id: DrawerId,
        principal: big_cabinet_core::drawer::protocol::Principal,
        access: Access,
    },
    /// A principal removed from a drawer's groups.
    Revoke {
        drawer_id: DrawerId,
        principal: big_cabinet_core::drawer::protocol::Principal,
    },
}

/// The boundaries inside a roster operation where a crash leaves one half done.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrashPoint {
    /// An entry's Keyhive grant landed; the roster never said so.
    DocGranted,
    /// An entry's roster write landed; the projection row never followed.
    RosterWritten,
    /// A removal's roster write landed; the Keyhive grant was never revoked.
    RosterRemoved,
    /// An invite's docs membership landed; the meta membership never did.
    DocsMembershipAdded,
}

trait LedgerFailure {
    fn keeps_ledger_row(&self) -> bool;
}

impl LedgerFailure for DrawersError {
    /// Whether a failed op may have left a side effect behind, so its ledger row stays for a
    /// retry or the next boot to finish; a refusal left nothing and frees its command id.
    fn keeps_ledger_row(&self) -> bool {
        match self {
            Self::Context(_)
            | Self::AuthorityUnavailable(_)
            | Self::DocsUnavailable(_)
            | Self::Keyhive(_)
            | Self::Storage(_)
            | Self::RosterUnreadable(_) => true,
            Self::Docs(error) => matches!(
                error,
                DocsError::Context(_)
                    | DocsError::AuthorityUnavailable(_)
                    | DocsError::Access(_)
                    | DocsError::Repository(_)
                    | DocsError::Pending { .. }
                    | DocsError::CommandInFlight { .. }
            ),
            Self::Denied
            | Self::Authority(_)
            | Self::UnknownDrawer(_)
            | Self::ForeignLayout(_)
            | Self::SelfDoc(_)
            | Self::AlreadyDrawer(_)
            | Self::SelfChild
            | Self::NotAnEntry(_)
            | Self::CommandInFlight { .. }
            | Self::CommandReused { .. } => false,
        }
    }
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
    sql: Database,
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

fn storage(error: SqlError) -> DrawersError {
    DrawersError::Storage(format!("op ledger: {error}"))
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_micros() as i64
}

fn decode(row: sqlite::GetOpRow) -> Row {
    Row {
        command: CommandId::try_from(row.command_id)
            .expect("a stored command id was valid when stored"),
        digest: row
            .request_digest
            .try_into()
            .expect("a stored digest is 32 bytes"),
        actor: Uuid7::try_from(
            row.actor_uuid
                .parse::<uuid::Uuid>()
                .expect("a stored actor uuid parses"),
        )
        .expect("a stored actor uuid is a v7 uuid"),
        op: serde_json::from_str(&row.op_json).expect("a stored op was written by this service"),
        step: Step::parse(&row.step),
        receipt: row
            .receipt_json
            .map(|json| serde_json::from_str(&json).expect("a stored receipt is JSON")),
    }
}

impl Ledger {
    pub(crate) async fn open(sql: Database) -> Result<Self, DrawersError> {
        sql.apply_migrations(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
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
        let row = sqlite::get_op(&self.sql, command.as_str().to_owned())
            .await
            .map_err(storage)?;
        Ok(row.map(|row| {
            let done_at = row.done_at;
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
        let inserted = sqlite::begin_op(
            &self.sql,
            command.as_str().to_owned(),
            digest.to_vec(),
            actor.to_string(),
            serde_json::to_string(op).expect("an op serializes"),
            now_micros(),
        )
        .await
        .map_err(storage)?;
        assert_eq!(
            inserted.rows_affected, 1,
            "a command is begun under the in-flight set, after its row was found absent"
        );
        Ok(())
    }

    /// Update what the row knows about its op: a create records its groups once they exist.
    pub(crate) async fn record(&self, command: &CommandId, op: &Op) -> Result<(), DrawersError> {
        sqlite::record_op(
            &self.sql,
            serde_json::to_string(op).expect("an op serializes"),
            command.as_str().to_owned(),
        )
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
        sqlite::finish_op(
            &self.sql,
            serde_json::to_string(receipt).expect("a receipt serializes"),
            now_micros(),
            command.as_str().to_owned(),
        )
        .await
        .map_err(storage)?;
        Ok(())
    }

    pub(crate) async fn forget(&self, command: &CommandId) -> Result<(), DrawersError> {
        sqlite::forget_op(&self.sql, command.as_str().to_owned())
            .await
            .map_err(storage)?;
        Ok(())
    }

    /// Every row a crash left short of `done`, oldest first.
    pub(crate) async fn unfinished(&self) -> Result<Vec<Row>, DrawersError> {
        let rows = sqlite::unfinished_ops(&self.sql).await.map_err(storage)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::drawer::GroupKey;
    use sqlite_rs::SqliteRuntime;

    #[tokio::test]
    async fn ledger_reopens_resumes_replays_and_expires_receipts() {
        let runtime = SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).unwrap();
        let ledger = Ledger::open(sql.clone()).await.unwrap();
        let command = CommandId::try_from("create ' drawer".to_owned()).unwrap();
        let actor = Uuid7::new();
        let op = Op::Create {
            groups: None,
            owner_id: Some(actor),
        };
        let recorded = Op::Create {
            groups: Some(DrawerGroups {
                meta_group: GroupKey([1; 32]),
                docs_group: GroupKey([2; 32]),
            }),
            owner_id: Some(actor),
        };
        let failure = DrawersError::Storage("interrupted".to_owned());
        let result = ledger
            .run::<Value, _, _>(&command, [3; 32], actor, op.clone(), |_| async {
                ledger.record(&command, &recorded).await.unwrap();
                Err(failure.clone())
            })
            .await;
        assert_eq!(result, Err(failure));
        drop(ledger);
        let ledger = Ledger::open(sql.clone()).await.unwrap();
        let mut expected = Row {
            command: command.clone(),
            digest: [3; 32],
            actor,
            op: recorded.clone(),
            step: Step::Begun,
            receipt: None,
        };
        assert_eq!(ledger.unfinished().await.unwrap(), vec![expected.clone()]);
        let receipt = serde_json::json!({"recovered": true});
        assert_eq!(
            ledger
                .run(&command, [3; 32], actor, op.clone(), |resumed| {
                    assert_eq!(resumed, recorded);
                    async { Ok(receipt.clone()) }
                })
                .await
                .unwrap(),
            receipt
        );
        expected.step = Step::Done;
        expected.receipt = Some(receipt.clone());
        let (stored, done_at) = ledger.get(&command).await.unwrap().unwrap();
        assert_eq!(stored, expected);
        assert!(done_at.is_some());
        assert!(ledger.unfinished().await.unwrap().is_empty());
        assert_eq!(
            ledger
                .run::<Value, _, _>(&command, [3; 32], actor, op.clone(), |_| async {
                    panic!("a completed command must not execute again")
                })
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(
            ledger
                .run::<Value, _, _>(&command, [4; 32], actor, op.clone(), |_| async {
                    panic!("a reused command must not execute")
                })
                .await
                .unwrap_err(),
            DrawersError::CommandReused {
                command: command.clone()
            }
        );
        sql.execute(
            "UPDATE big_cabinet_drawer_ops SET done_at = :done_at WHERE command_id = :command_id",
            (now_micros() - DONE_TTL_MICROS - 1, command.as_str()),
        )
        .await
        .unwrap();
        let renewed = serde_json::json!({"renewed": true});
        assert_eq!(
            ledger
                .run(&command, [3; 32], actor, op.clone(), |resumed| {
                    assert_eq!(resumed, op);
                    async { Ok(renewed.clone()) }
                })
                .await
                .unwrap(),
            renewed
        );
        ledger.forget(&command).await.unwrap();
        assert_eq!(
            ledger
                .run::<Value, _, _>(&command, [3; 32], actor, op, |_| async {
                    Err(DrawersError::Denied)
                })
                .await
                .unwrap_err(),
            DrawersError::Denied
        );
        assert!(ledger.get(&command).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn cancelled_command_retains_recovery_row_but_releases_in_flight_guard() {
        let runtime = SqliteRuntime::new(Default::default());
        let ledger = Ledger::open(runtime.ephemeral(&[]).unwrap()).await.unwrap();
        let command = CommandId::try_from("cancelled".to_owned()).unwrap();
        let actor = Uuid7::new();
        let op = Op::RemoveDoc {
            drawer_id: DocId::new([1; 32]),
            doc_id: DocId::new([2; 32]),
        };
        let (started, ready) = tokio::sync::oneshot::channel();
        let mut running =
            Box::pin(
                ledger.run::<(), _, _>(&command, [5; 32], actor, op.clone(), |_| async {
                    started.send(()).unwrap();
                    std::future::pending().await
                }),
            );
        tokio::select! {
            result = &mut running => panic!("command completed before cancellation: {result:?}"),
            result = ready => result.unwrap(),
        }
        assert_eq!(
            ledger
                .run::<(), _, _>(&command, [5; 32], actor, op.clone(), |_| async {
                    panic!("an in-flight command cannot execute twice")
                })
                .await
                .unwrap_err(),
            DrawersError::CommandInFlight {
                command: command.clone()
            }
        );
        drop(running);
        assert_eq!(
            ledger.unfinished().await.unwrap(),
            vec![Row {
                command: command.clone(),
                digest: [5; 32],
                actor,
                op: op.clone(),
                step: Step::Begun,
                receipt: None,
            }]
        );
        ledger
            .run(&command, [5; 32], actor, op.clone(), |resumed| {
                assert_eq!(resumed, op);
                async { Ok(()) }
            })
            .await
            .unwrap();
        assert!(ledger.unfinished().await.unwrap().is_empty());
    }
}
