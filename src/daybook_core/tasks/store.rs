//! Task semantics over authenticated encrypted current registers.
//!
//! One signed writer lane carries the immutable declaration and that writer's
//! terminal fact. Projection rebuilds publisher evidence from verified originals;
//! callers cannot smuggle another writer's terminal signature through a payload.

use super::storage::RegisterStore;
use super::{
    NodePubkey, PoolTaskId, PublisherEvidence, SignedTerminalFact, TASK_TICKET_PAYLOAD_SCHEMA,
    TaskDeclaration, TaskPoolId, TaskTicket, TerminalFact,
};
use crate::interlude::*;
use big_sync_core::encrypted_register::{LaneState, MergeOutcome};

#[derive(Serialize, Deserialize)]
struct TaskLane {
    schema: u16,
    declaration: TaskDeclaration,
    terminal: Option<TerminalFact>,
    #[serde(default)]
    input_witness: Vec<u8>,
}

pub struct TaskStore {
    register: RegisterStore,
    pool: TaskPoolId,
    publication: tokio::sync::Mutex<()>,
}

impl TaskStore {
    /// Own the register so publication serialization cannot be split between
    /// wrappers sharing one host. Share the TaskStore itself for concurrent use.
    pub fn new(register: RegisterStore, pool: TaskPoolId) -> Self {
        Self {
            register,
            pool,
            publication: tokio::sync::Mutex::new(()),
        }
    }

    pub fn register(&self) -> &RegisterStore {
        &self.register
    }

    /// Current signed lanes, not a retained publication journal. Ciphertext-only
    /// relays use RegisterStore directly; this projection requires local Read.
    pub async fn ticket(&self, task: PoolTaskId) -> Res<Option<TaskTicket>> {
        let Some(snapshot) = self.register.current(&task.to_bytes32()).await? else {
            return Ok(None);
        };
        let mut result: Option<TaskTicket> = None;
        for lane in snapshot.lanes.values() {
            let LaneState::Current { representation } = lane else {
                eyre::bail!("task writer equivocated; ticket is not schedulable");
            };
            let original = self.register.open_original(representation).await?;
            let lane: TaskLane = serde_json::from_slice(&original.body)?;
            eyre::ensure!(
                lane.schema == TASK_TICKET_PAYLOAD_SCHEMA,
                "unsupported task ticket payload schema {}",
                lane.schema
            );
            eyre::ensure!(
                lane.declaration.task_id == task,
                "task declaration identity mismatch"
            );
            eyre::ensure!(
                lane.declaration.pool == self.pool,
                "task declaration pool mismatch"
            );
            eyre::ensure!(
                lane.declaration.validate_local().is_valid(),
                "invalid authenticated task declaration"
            );
            let writer = NodePubkey::new(representation.original.writer);
            let mut ticket = TaskTicket::new(
                lane.declaration,
                PublisherEvidence {
                    publisher: writer,
                    envelope: serde_json::to_vec(&representation.original)?,
                    input_witness: lane.input_witness,
                },
            );
            if let Some(fact) = lane.terminal {
                ticket.terminal.insert(
                    writer,
                    SignedTerminalFact {
                        writer,
                        writer_seq: representation.original.writer_seq,
                        fact,
                    },
                );
            }
            if let Some(current) = &mut result {
                current.merge(&ticket)?;
            } else {
                result = Some(ticket);
            }
        }
        Ok(result)
    }

    /// Independently equivalent publishers may assert the same declaration.
    /// A task id is never reused for different execution meaning.
    pub async fn submit(
        &self,
        declaration: TaskDeclaration,
        input_witness: Vec<u8>,
    ) -> Res<MergeOutcome> {
        let _publication = self.publication.lock().await;
        assert!(
            declaration.validate_local().is_valid(),
            "invalid local task declaration"
        );
        eyre::ensure!(
            declaration.pool == self.pool,
            "task declaration pool mismatch"
        );
        let writer = NodePubkey::new(self.register.local_writer().await?);
        let mut terminal = None;
        if let Some(current) = self.ticket(declaration.task_id).await? {
            eyre::ensure!(
                current.declaration.declaration == declaration,
                "task identity has another immutable declaration"
            );
            if current.declaration.publishers.iter().any(|evidence| {
                evidence.publisher == writer && evidence.input_witness == input_witness
            }) {
                return Ok(MergeOutcome::Unchanged);
            }
            terminal = current.terminal.get(&writer).map(|lane| lane.fact.clone());
        }
        let task = declaration.task_id;
        self.register
            .publish_local(
                &task.to_bytes32(),
                vec![],
                serde_json::to_vec(&TaskLane {
                    schema: TASK_TICKET_PAYLOAD_SCHEMA,
                    declaration,
                    terminal,
                    input_witness,
                })?,
            )
            .await
    }

    /// Called only after the owning domain durably incorporated execution's
    /// result. Publication authenticates the actual local writer, not an input id.
    pub async fn record_terminal(&self, task: PoolTaskId, fact: TerminalFact) -> Res<MergeOutcome> {
        let _publication = self.publication.lock().await;
        let current = self
            .ticket(task)
            .await?
            .ok_or_else(|| ferr!("task declaration unavailable"))?;
        let writer = NodePubkey::new(self.register.local_writer().await?);
        let input_witness = current
            .declaration
            .publishers
            .iter()
            .find(|evidence| evidence.publisher == writer)
            .or_else(|| current.declaration.publishers.first())
            .expect(ERROR_IMPOSSIBLE)
            .input_witness
            .clone();
        if let Some(previous) = current.terminal.get(&writer)
            && (previous.fact == fact || (previous.fact.is_success() && !fact.is_success()))
        {
            return Ok(MergeOutcome::Unchanged);
        }
        self.register
            .publish_local(
                &task.to_bytes32(),
                vec![],
                serde_json::to_vec(&TaskLane {
                    schema: TASK_TICKET_PAYLOAD_SCHEMA,
                    declaration: current.declaration.declaration,
                    terminal: Some(fact),
                    input_witness,
                })?,
            )
            .await
    }
}
