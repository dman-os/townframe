use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::broadcast;

mod mock;
pub use mock::{MockApplicationBackend, PendingRefresh};

/// A stable identifier for a named query known to an application feature.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct QueryKey(pub String);

impl QueryKey {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

/// An opaque position owned and interpreted by the service that produced it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceCursor(Value);

impl SourceCursor {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    pub fn into_value(self) -> Value {
        self.0
    }
}

/// A refresh request sent whenever a live query is mounted or preloaded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefreshRequest {
    pub id: u64,
    pub query: QueryKey,
}

/// A data-only row returned by a query refresh or carried by a query event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataRow {
    pub key: String,
    pub value: Value,
}

/// A query delta carried by a GUI-driving backend event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QueryDelta {
    Upsert(DataRow),
    Remove { key: String },
}

/// The data returned by a refresh. The cursor remains opaque to dayframe_app.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefreshResponse {
    pub request: RefreshRequest,
    pub cursor: Option<SourceCursor>,
    pub rows: Vec<DataRow>,
}

/// A command is a named user intent with data-only arguments.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub id: CommandId,
    pub name: String,
    pub arguments: Value,
}

/// Identifies a command independently from its eventual outcome and is reused for retries.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CommandId(pub u64);

/// Identifies the backend boundary's local submission result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub command: CommandId,
    pub accepted: bool,
}

/// A command result carried by the GUI-driving event stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CommandOutcome {
    Accepted,
    Rejected { message: String },
}

/// A backend event is data-only so an in-process and daemon implementation share a contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackendEvent {
    QuerySnapshot {
        query: QueryKey,
        cursor: Option<SourceCursor>,
        rows: Vec<DataRow>,
    },
    QueryDelta {
        query: QueryKey,
        cursor: Option<SourceCursor>,
        changes: Vec<QueryDelta>,
    },
    CommandCompleted {
        command: CommandId,
        outcome: CommandOutcome,
    },
}

/// The application-facing boundary selected by a platform composition root.
#[async_trait]
pub trait ApplicationBackend: Send + Sync {
    async fn refresh(&self, request: RefreshRequest) -> Result<RefreshResponse, BackendError>;

    async fn submit(&self, command: Command) -> Result<CommandReceipt, BackendError>;

    fn subscribe(&self) -> broadcast::Receiver<BackendEvent>;
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BackendError {
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("backend rejected request: {0}")]
    Rejected(String),
}
