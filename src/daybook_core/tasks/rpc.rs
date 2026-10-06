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

pub const TASK_COORDINATION_ALPN: &[u8] = b"townframe/task-coordination/6";

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

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrentPoolControlRequest {
    pub pool: TaskPoolId,
    /// Optional authenticated advertisement; never creates an unsolicited pool.
    pub offered: Option<super::pool::PoolRotationControl>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrentProcessorControlRequest {
    pub processor: String,
    pub offered: Option<crate::rt::triage::domain::ProcessorRotationControl>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterSourceControlRequest {
    pub register_scope: Vec<u8>,
    pub authority_document: big_repo::DocumentId,
    pub key_ref: url::Url,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlRejection {
    Pending,
    Unauthorized,
    Invalid(String),
}

impl From<big_repo::CoordinationError> for ControlRejection {
    fn from(error: big_repo::CoordinationError) -> Self {
        match error {
            big_repo::CoordinationError::Pending => Self::Pending,
            big_repo::CoordinationError::Unauthorized => Self::Unauthorized,
            big_repo::CoordinationError::Invalid(reason) => Self::Invalid(reason.into()),
            big_repo::CoordinationError::Other(error) => {
                panic!("native current control failed: {error:?}")
            }
        }
    }
}

#[rpc_requests(message = TaskCoordinationRpcMessage)]
#[derive(Debug, Serialize, Deserialize)]
pub enum TaskCoordinationRpc {
    #[rpc(tx = channel::mpsc::Sender<RouterMessage>)]
    RegisterExecutor(RegisterExecutorRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<(), String>>)]
    Report(ExecutorReportRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<super::pool::PoolRotationControl, ControlRejection>>)]
    CurrentPoolControl(CurrentPoolControlRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<crate::rt::triage::domain::ProcessorRotationControl, ControlRejection>>)]
    CurrentProcessorControl(CurrentProcessorControlRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<super::RegisterSourceControl, ControlRejection>>)]
    RegisterSourceControl(RegisterSourceControlRequest),
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
                TaskCoordinationRpcMessage::RegisterExecutor(message) => {
                    Some(message.inner.session)
                }
                TaskCoordinationRpcMessage::Report(message) => Some(message.inner.session),
                // A passive relay has no executor session. Its native peer and
                // current pool sponsorship are checked independently.
                TaskCoordinationRpcMessage::CurrentPoolControl(_)
                | TaskCoordinationRpcMessage::CurrentProcessorControl(_)
                | TaskCoordinationRpcMessage::RegisterSourceControl(_) => None,
            };
            if let Some(session) = session
                && session.node.to_bytes32().as_slice() != peer.as_bytes()
            {
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
