//! Live scheduling transport. Retained task evidence uses BigSync, not this RPC.

use super::{
    AllocationId, AttemptState, DeclineReason, OpaqueReason, PoolAttemptId, PoolTaskId,
    RouterClaim, SessionKey, TaskDeclaration, TaskPoolId, WorkerRegistration,
};
use big_repo::rpc::BigRepoRpcHandle;
use big_sync_core::PeerKey;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc::{channel, rpc_requests};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub const TASK_COORDINATION_ALPN: &[u8] = b"townframe/task-coordination/2";

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterExecutorRequest {
    pub pool: TaskPoolId,
    pub session: SessionKey,
    pub registration: WorkerRegistration,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RouterMessage {
    Registered {
        claim: RouterClaim,
    },
    Offer {
        allocation_id: AllocationId,
        task_id: PoolTaskId,
        declaration: Box<TaskDeclaration>,
    },
    Start {
        allocation_id: AllocationId,
        task_id: PoolTaskId,
    },
    Cancel {
        task_id: PoolTaskId,
        reason: OpaqueReason,
    },
    Rejected {
        reason: DeclineReason,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutorReportRequest {
    pub pool: TaskPoolId,
    pub session: SessionKey,
    pub report: ExecutorReport,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutorReport {
    Accepted {
        allocation_id: AllocationId,
        attempt_id: PoolAttemptId,
    },
    Declined {
        allocation_id: AllocationId,
        reason: DeclineReason,
    },
    AttemptChanged {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
        state: AttemptState,
    },
    ReadinessWakeup {
        task_id: PoolTaskId,
    },
    Closed,
}

#[rpc_requests(message = TaskCoordinationRpcMessage)]
#[derive(Debug, Serialize, Deserialize)]
pub enum TaskCoordinationRpc {
    #[rpc(tx = channel::mpsc::Sender<RouterMessage>)]
    RegisterExecutor(RegisterExecutorRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<(), String>>)]
    Report(ExecutorReportRequest),
}

pub struct AuthenticatedTaskRequest {
    pub peer: PeerKey,
    pub endpoint: iroh::EndpointId,
    pub request: TaskCoordinationRpcMessage,
}

/// Ingress carries the authenticated application identity separately from the
/// request. The pool driver must check current authority and session ownership
/// before feeding any message to the scheduling machine.
#[derive(Clone)]
pub struct TaskCoordinationProtocolHandler {
    identities: BigRepoRpcHandle,
    requests: mpsc::Sender<AuthenticatedTaskRequest>,
}

impl TaskCoordinationProtocolHandler {
    pub fn new(
        identities: BigRepoRpcHandle,
        requests: mpsc::Sender<AuthenticatedTaskRequest>,
    ) -> Self {
        Self {
            identities,
            requests,
        }
    }
}

impl std::fmt::Debug for TaskCoordinationProtocolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskCoordinationProtocolHandler")
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for TaskCoordinationProtocolHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            let request = match irpc_iroh::read_request::<TaskCoordinationRpc>(&connection).await {
                Ok(Some(request)) => request,
                Ok(None) => return Ok(()),
                Err(error) => {
                    tracing::warn!(?error, "task scheduling transport failed");
                    return Ok(());
                }
            };
            // Recheck each request: disconnecting the native BigRepo connection
            // removes its mapping even if this separate RPC connection survives.
            let Some(peer) = self.identities.peer_for_endpoint(connection.remote_id()) else {
                connection.close(0u32.into(), b"native application identity unavailable");
                return Ok(());
            };
            let session = match &request {
                TaskCoordinationRpcMessage::RegisterExecutor(message) => message.inner.session,
                TaskCoordinationRpcMessage::Report(message) => message.inner.session,
            };
            if session.node.to_bytes32().as_slice() != peer.as_bytes() {
                connection.close(0u32.into(), b"task session identity mismatch");
                return Ok(());
            }
            // Receiver closure is the owner's explicit shutdown signal.
            if self
                .requests
                .send(AuthenticatedTaskRequest {
                    peer,
                    endpoint: connection.remote_id(),
                    request,
                })
                .await
                .is_err()
            {
                connection.close(0u32.into(), b"task coordination stopped");
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests;
