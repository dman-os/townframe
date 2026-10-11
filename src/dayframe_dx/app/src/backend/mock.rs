use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::sync::{broadcast, oneshot};

use super::{
    ApplicationBackend, BackendError, BackendEvent, Command, CommandId, CommandOutcome,
    CommandReceipt, DataRow, QueryDelta, QueryKey, RefreshRequest, RefreshResponse, SourceCursor,
};

#[derive(Clone)]
pub struct MockApplicationBackend {
    state: Arc<Mutex<MockState>>,
    events: broadcast::Sender<BackendEvent>,
}

#[derive(Default)]
struct MockState {
    refreshes: Vec<RefreshRequest>,
    commands: Vec<Command>,
    refresh_controls: VecDeque<RefreshControl>,
    submit_results: VecDeque<Result<CommandReceipt, BackendError>>,
}

enum RefreshControl {
    Immediate(Result<RefreshResponse, BackendError>),
    Pending(oneshot::Receiver<Result<RefreshResponse, BackendError>>),
}

pub struct PendingRefresh {
    sender: oneshot::Sender<Result<RefreshResponse, BackendError>>,
}

impl PendingRefresh {
    pub fn complete(self, response: Result<RefreshResponse, BackendError>) {
        self.sender
            .send(response)
            .expect("pending mock refresh receiver dropped");
    }
}

impl Default for MockApplicationBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockApplicationBackend {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(32);
        Self {
            state: Arc::new(Mutex::new(MockState::default())),
            events,
        }
    }

    pub fn refreshes(&self) -> Vec<RefreshRequest> {
        self.state
            .lock()
            .expect("mock backend state lock poisoned")
            .refreshes
            .clone()
    }

    pub fn commands(&self) -> Vec<Command> {
        self.state
            .lock()
            .expect("mock backend state lock poisoned")
            .commands
            .clone()
    }

    pub fn queue_refresh_response(&self, response: Result<RefreshResponse, BackendError>) {
        self.state
            .lock()
            .expect("mock backend state lock poisoned")
            .refresh_controls
            .push_back(RefreshControl::Immediate(response));
    }

    pub fn queue_refresh_error(&self, error: BackendError) {
        self.queue_refresh_response(Err(error));
    }

    pub fn queue_pending_refresh(&self) -> PendingRefresh {
        let (sender, receiver) = oneshot::channel();
        self.state
            .lock()
            .expect("mock backend state lock poisoned")
            .refresh_controls
            .push_back(RefreshControl::Pending(receiver));
        PendingRefresh { sender }
    }

    pub fn queue_submit_response(&self, response: Result<CommandReceipt, BackendError>) {
        self.state
            .lock()
            .expect("mock backend state lock poisoned")
            .submit_results
            .push_back(response);
    }

    pub fn queue_submit_error(&self, error: BackendError) {
        self.queue_submit_response(Err(error));
    }

    pub fn emit_query_snapshot(
        &self,
        query: QueryKey,
        cursor: Option<SourceCursor>,
        rows: Vec<DataRow>,
    ) {
        let _ = self.events.send(BackendEvent::QuerySnapshot {
            query,
            cursor,
            rows,
        });
    }

    pub fn emit_query_delta(
        &self,
        query: QueryKey,
        cursor: Option<SourceCursor>,
        changes: Vec<QueryDelta>,
    ) {
        let _ = self.events.send(BackendEvent::QueryDelta {
            query,
            cursor,
            changes,
        });
    }

    pub fn emit_command_completed(&self, command: CommandId, outcome: CommandOutcome) {
        let _ = self
            .events
            .send(BackendEvent::CommandCompleted { command, outcome });
    }
}

#[async_trait]
impl ApplicationBackend for MockApplicationBackend {
    async fn refresh(&self, request: RefreshRequest) -> Result<RefreshResponse, BackendError> {
        let control = {
            let mut state = self.state.lock().expect("mock backend state lock poisoned");
            state.refreshes.push(request.clone());
            state.refresh_controls.pop_front()
        };

        match control {
            Some(RefreshControl::Immediate(response)) => response,
            Some(RefreshControl::Pending(receiver)) => {
                receiver.await.expect("pending mock refresh sender dropped")
            }
            None => Ok(RefreshResponse {
                request,
                cursor: None,
                rows: Vec::new(),
            }),
        }
    }

    async fn submit(&self, command: Command) -> Result<CommandReceipt, BackendError> {
        let command_id = command.id;
        let response = {
            let mut state = self.state.lock().expect("mock backend state lock poisoned");
            state.commands.push(command);
            state.submit_results.pop_front()
        };
        response.unwrap_or(Ok(CommandReceipt {
            command: command_id,
            accepted: true,
        }))
    }

    fn subscribe(&self) -> broadcast::Receiver<BackendEvent> {
        self.events.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ApplicationBackend;

    #[tokio::test]
    async fn records_refreshes_and_commands_without_resolving_authoritative_events() {
        let backend = MockApplicationBackend::new();
        let request = RefreshRequest {
            id: 1,
            query: QueryKey::new("settings.display_hints"),
        };
        let command = Command {
            id: CommandId(2),
            name: "settings.update_hint".to_owned(),
            arguments: serde_json::json!({"key": "title"}),
        };

        backend.refresh(request.clone()).await.unwrap();
        backend.submit(command.clone()).await.unwrap();

        assert_eq!(backend.refreshes(), vec![request]);
        assert_eq!(backend.commands(), vec![command]);
    }

    #[test]
    fn explicit_events_are_data_only_and_ordered_by_send() {
        let backend = MockApplicationBackend::new();
        let mut events = backend.subscribe();
        let query = QueryKey::new("settings.display_hints");
        let cursor = Some(SourceCursor::new(serde_json::json!({"head": "a"})));
        backend.emit_query_snapshot(
            query.clone(),
            cursor.clone(),
            vec![DataRow {
                key: "title".to_owned(),
                value: serde_json::json!("visible"),
            }],
        );
        backend.emit_query_delta(
            query,
            cursor,
            vec![QueryDelta::Remove {
                key: "title".to_owned(),
            }],
        );
        backend.emit_command_completed(CommandId(3), CommandOutcome::Accepted);

        assert!(matches!(
            events.try_recv().unwrap(),
            BackendEvent::QuerySnapshot { .. }
        ));
        assert!(matches!(
            events.try_recv().unwrap(),
            BackendEvent::QueryDelta { .. }
        ));
        assert!(matches!(
            events.try_recv().unwrap(),
            BackendEvent::CommandCompleted { .. }
        ));
    }

    #[tokio::test]
    async fn queued_failures_and_pending_refreshes_are_deterministic() {
        let backend = MockApplicationBackend::new();
        backend.queue_refresh_error(BackendError::Unavailable("offline".to_owned()));
        backend.queue_submit_error(BackendError::Rejected("invalid".to_owned()));

        let request = RefreshRequest {
            id: 1,
            query: QueryKey::new("settings.display_hints"),
        };
        assert_eq!(
            backend.refresh(request).await,
            Err(BackendError::Unavailable("offline".to_owned()))
        );
        assert_eq!(
            backend
                .submit(Command {
                    id: CommandId(2),
                    name: "settings.update_hint".to_owned(),
                    arguments: serde_json::Value::Null,
                })
                .await,
            Err(BackendError::Rejected("invalid".to_owned()))
        );

        let pending = backend.queue_pending_refresh();
        let request = RefreshRequest {
            id: 3,
            query: QueryKey::new("settings.display_hints"),
        };
        let response = RefreshResponse {
            request: request.clone(),
            cursor: Some(SourceCursor::new(serde_json::json!({"head": "b"}))),
            rows: vec![DataRow {
                key: "title".to_owned(),
                value: serde_json::json!("visible"),
            }],
        };
        let refresh = backend.refresh(request);
        pending.complete(Ok(response.clone()));
        assert_eq!(refresh.await.unwrap(), response);
    }
}
