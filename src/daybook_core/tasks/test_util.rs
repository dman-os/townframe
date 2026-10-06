//! Shared fixtures for the `tasks` behavioural tests.
//!
//! These build real declarations and tickets rather than mocks: a test that
//! passes here exercises the same merge, classification, and handshake code a
//! driver would.

use super::model::{
    AllocationId, CapabilitySummary, CoordinationIds, CoordinationSlotKey, DomainCoordinationRef,
    DomainId, EffectPolicy, HandlerRef, NodeIncarnationId, NodePubkey, PoolClaimId, PoolTaskId,
    PoolTaskIdInputs, Preference, PublisherEvidence, ResultRetention, SignedTerminalFact,
    TaskDeclaration, TaskPoolId, TaskTicket, TerminalFact, WorkGeneration, derive_pool_task_id,
};
use super::model::{OpaqueReason, OpaqueResultRef, PoolAttemptId};

use crate::interlude::*;
pub trait TestClassification {
    fn classify(&mut self, ticket: &TaskTicket) -> super::TaskClassification;
}

/// Deterministically complete emitted asynchronous actions in pure-machine tests.
pub fn drive_worker<I: CoordinationIds>(
    machine: &mut super::PoolWorkerMachine<I>,
    event: super::WorkerEvent,
    domain: &mut impl TestClassification,
    out: &mut Vec<super::WorkerAction>,
) {
    let mut pending = Vec::new();
    machine.on_event(event, &mut pending);
    let mut pending = std::collections::VecDeque::from(pending);
    while let Some(action) = pending.pop_front() {
        if let super::WorkerAction::ClassifyTask { token, ticket } = action {
            let classification = domain.classify(&ticket);
            let mut next = Vec::new();
            machine.on_event(
                super::WorkerEvent::Classified {
                    token,
                    classification,
                },
                &mut next,
            );
            pending.extend(next);
        } else if let super::WorkerAction::StartDispatch { ref token, .. } = action {
            let token = token.clone();
            out.push(action);
            let mut next = Vec::new();
            machine.on_event(super::WorkerEvent::DispatchStarted { token }, &mut next);
            pending.extend(next);
        } else {
            out.push(action);
        }
    }
}

pub trait TestObsolescence {
    fn is_proven_obsolete(&self, declaration: &TaskDeclaration) -> bool;
}

pub fn drive_router<I: CoordinationIds>(
    machine: &mut super::RouterMachine<I>,
    event: super::RouterEvent,
    domain: &impl TestObsolescence,
    out: &mut Vec<super::RouterAction>,
) {
    let mut pending = Vec::new();
    machine.on_event(event, &mut pending);
    let mut pending = std::collections::VecDeque::from(pending);
    while let Some(action) = pending.pop_front() {
        if let super::RouterAction::CheckObsolescence { token, ticket } = action {
            let obsolete = domain.is_proven_obsolete(&ticket.declaration.declaration);
            let mut next = Vec::new();
            machine.on_event(
                super::RouterEvent::ObsolescenceChecked { token, obsolete },
                &mut next,
            );
            pending.extend(next);
        } else {
            out.push(action);
        }
    }
}

pub fn origin_ticket(declaration: TaskDeclaration) -> Box<TaskTicket> {
    let producer = declaration
        .producer
        .expect("origin ticket has a known source origin");
    Box::new(TaskTicket::new(
        declaration,
        PublisherEvidence {
            publisher: producer,
            envelope: vec![1],
            input_witness: Vec::new(),
        },
    ))
}
pub fn node(seed: u8) -> NodePubkey {
    NodePubkey::new([seed; 32])
}

pub fn pool() -> TaskPoolId {
    TaskPoolId::from_label("image-description")
}

pub fn domain() -> DomainId {
    DomainId::from_label("triage")
}

/// A deterministic id for the given generation label, exactly as a task domain
/// would derive it.
pub fn derived_task(generation: &str) -> PoolTaskId {
    derive_pool_task_id(PoolTaskIdInputs {
        domain: &domain(),
        pool: &pool(),
        slot: &CoordinationSlotKey::from_label("photo-7/describe-image"),
        generation: &WorkGeneration::from_label(generation),
    })
}

/// A canonical declaration for one generation.
pub fn declaration(generation: &str) -> TaskDeclaration {
    TaskDeclaration {
        task_id: derived_task(generation),
        pool: pool(),
        domain: domain(),
        producer: Some(node(1)),
        handler: HandlerRef::from_label("describe-image"),
        input: generation.as_bytes().to_vec(),
        capabilities: CapabilitySummary::from_labels(["gpu"]),
        coordination_ref: Some(DomainCoordinationRef::from_label("photo-7")),
        placement: Preference::PreferOrigin(node(1)),
        effect_policy: EffectPolicy::Idempotent,
        not_before_secs: None,
        // Fixed finite fixture deadline (2100-01-01), so independently constructed
        // declarations retain identical meaning without wall-clock sampling.
        not_after_secs: 4_102_444_800,
        result_retention: ResultRetention::ExternalSettlement(DomainCoordinationRef::from_label(
            "photo-7",
        )),
    }
}

/// Publisher evidence as a given node would publish it. `envelope` differs per
/// publisher so tests prove evidence is not confused with canonical meaning.
pub fn evidence(publisher: NodePubkey, envelope_seed: u8) -> PublisherEvidence {
    PublisherEvidence {
        publisher,
        envelope: vec![envelope_seed; 8],
        input_witness: Vec::new(),
    }
}

pub fn ticket(generation: &str) -> TaskTicket {
    TaskTicket::new(declaration(generation), evidence(node(1), 1))
}

pub fn success_fact(attempt_id: u128, result: &str) -> TerminalFact {
    TerminalFact::Succeeded {
        attempt_id: PoolAttemptId::new(attempt_id),
        result_ref: Some(OpaqueResultRef::from_label(result)),
    }
}

pub fn cancelled_fact(reason: &str) -> TerminalFact {
    TerminalFact::Cancelled {
        reason: OpaqueReason::from_label(reason),
    }
}

pub fn terminal_lane(
    writer: NodePubkey,
    writer_seq: u64,
    fact: TerminalFact,
) -> SignedTerminalFact {
    SignedTerminalFact {
        writer,
        writer_seq,
        fact,
    }
}

pub fn incarnation(seed: u8) -> NodeIncarnationId {
    NodeIncarnationId::new(u128::from(seed))
}

/// A deterministic [`CoordinationIds`] source: one counter per identity kind.
///
/// Test-only counters keep identities predictable across reducer scenarios.
#[derive(Default)]
pub(crate) struct SequentialIds {
    claims: u128,
    allocations: u128,
    attempts: u128,
}

impl CoordinationIds for SequentialIds {
    fn next_claim_id(&mut self) -> PoolClaimId {
        self.claims += 1;
        PoolClaimId::new(self.claims)
    }

    fn next_allocation_id(&mut self) -> AllocationId {
        self.allocations += 1;
        AllocationId::new(self.allocations)
    }

    fn next_attempt_id(&mut self) -> PoolAttemptId {
        self.attempts += 1;
        PoolAttemptId::new(self.attempts)
    }
}

/// A tiny [`CoordinationIds`] wrapper for tests that need to inspect minted ids.
pub fn ids() -> SequentialIds {
    SequentialIds::default()
}

#[derive(Debug)]
struct TaskTestAccept {
    repo: SharedBigRepo,
    endpoint: iroh::Endpoint,
    connections: Arc<tokio::sync::Mutex<Vec<big_repo::BigRepoConnection>>>,
}

impl iroh::protocol::ProtocolHandler for TaskTestAccept {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(
                connection,
                self.endpoint.clone(),
                /*end_signal_tx*/ None,
            )
            .await
            .map_err(|error| iroh::protocol::AcceptError::from_boxed(error.into()))?;
        self.connections.lock().await.push(connection);
        Ok(())
    }
}

pub(crate) struct TaskTestNode {
    // Fields drop in declaration order: facade copies go before the owner guard.
    pub(crate) repo: SharedBigRepo,
    pub(crate) endpoint: iroh::Endpoint,
    owners: TaskTestOwners,
}

struct TaskTestOwners {
    root: crate::test_support::sync_node::Owners,
    router: Option<iroh::protocol::Router>,
    endpoint: Option<iroh::Endpoint>,
    rpc: Option<big_repo::rpc::BigRepoRpcStopToken>,
    connections: Arc<tokio::sync::Mutex<Vec<big_repo::BigRepoConnection>>>,
    stopped: bool,
}

impl TaskTestOwners {
    async fn stop(&mut self) -> Res<()> {
        // Stop accept producers before draining the connections they install.
        if let Some(router) = self.router.take() {
            router.shutdown().await?;
            drop(self.endpoint.take());
        } else if let Some(endpoint) = self.endpoint.take() {
            endpoint.close().await;
        }
        for connection in self.connections.lock().await.drain(..) {
            if !connection.is_closed() {
                connection.stop().await?;
            }
        }
        if let Some(rpc) = self.rpc.take() {
            rpc.stop().await?;
        }
        self.root.stop(/*runtime_first*/ false).await?;
        self.stopped = true;
        Ok(())
    }
}

impl Drop for TaskTestOwners {
    fn drop(&mut self) {
        if !self.stopped {
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(self.stop()))
                .expect("native task fixture teardown failed");
        }
    }
}

impl TaskTestNode {
    pub(crate) async fn boot(identity_seed: u8) -> Res<Self> {
        let (repo, repo_stop) = big_repo::BigRepo::boot(big_repo::Config {
            node_identity_seed: [identity_seed; 32],
            storage: big_repo::StorageConfig::Memory,
            scope_key: Arc::from("pool-nested-read-test"),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await?;
        let mut owners = TaskTestOwners {
            root: crate::test_support::sync_node::Owners::before_context(repo_stop),
            router: None,
            endpoint: None,
            rpc: None,
            connections: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            stopped: false,
        };
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .secret_key(iroh::SecretKey::from_bytes(&[identity_seed; 32]))
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        owners.endpoint = Some(endpoint.clone());
        let connections = Arc::clone(&owners.connections);
        let (rpc, rpc_stop) = big_repo::rpc::spawn_repo_rpc(Arc::clone(&repo)).await?;
        owners.rpc = Some(rpc_stop);
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                crate::sync::SUBDUCTION_ALPN,
                TaskTestAccept {
                    repo: Arc::clone(&repo),
                    endpoint: endpoint.clone(),
                    connections: Arc::clone(&connections),
                },
            )
            .accept(big_repo::rpc::REPO_SYNC_ALPN, rpc.protocol_handler())
            .spawn();
        owners.router = Some(router);
        Ok(Self {
            repo,
            endpoint,
            owners,
        })
    }

    pub(crate) async fn stop(self) -> Res<()> {
        let Self {
            repo,
            endpoint,
            mut owners,
        } = self;
        drop((repo, endpoint));
        owners.stop().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_task_fixture_error_scope_closes_repository() -> Res<()> {
    use automerge::transaction::Transactable;
    let mut witness = None;
    let failed: Res<()> = async {
        let node = TaskTestNode::boot(199).await?;
        let mut seed = automerge::Automerge::new();
        let mut transaction = seed.transaction();
        transaction.put(automerge::ROOT, "version", 1)?;
        transaction.commit();
        let handle = node.repo.create_doc(seed).await?;
        let document = handle.document_id();
        assert!(matches!(
            node.repo.get_doc(&document).await?,
            big_repo::DocLookup::Ready(_)
        ));
        drop(handle);
        witness = Some((Arc::clone(&node.repo), document));
        eyre::bail!("scenario refused with live native accept and RPC producers");
    }
    .await;
    let failure = failed.expect_err("exercise native fixture error scope");
    let (repo, document) = witness.ok_or(failure)?;
    assert!(repo.get_doc(&document).await.is_err());
    Ok(())
}
