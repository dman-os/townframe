use std::collections::HashMap;

use thiserror::Error;

use crate::backend::{
    ApplicationBackend, BackendError, DataRow, QueryKey, RefreshRequest, SourceCursor,
};

use super::{Collection, CollectionChange, CollectionError};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RefreshToken {
    pub query: QueryKey,
    pub request_id: u64,
}

#[derive(Default)]
pub struct RefreshRegistry {
    next_request_id: u64,
    in_flight: HashMap<QueryKey, RefreshToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RefreshDecision {
    Started(RefreshToken),
    AlreadyInFlight(RefreshToken),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RefreshOutcome {
    Applied {
        token: RefreshToken,
        cursor: Option<SourceCursor>,
        rows: usize,
    },
    AlreadyInFlight(RefreshToken),
}

#[derive(Default)]
pub struct RefreshCoordinator {
    registry: RefreshRegistry,
}

#[derive(Debug, Error, PartialEq)]
pub enum RefreshError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error(transparent)]
    Collection(#[from] CollectionError),
    #[error("refresh response did not match request {expected:?}: got {actual:?}")]
    MismatchedRequest {
        expected: RefreshRequest,
        actual: RefreshRequest,
    },
    #[error("refresh row {key:?} could not be decoded: {message}")]
    Decode { key: String, message: String },
}

impl RefreshRegistry {
    pub fn ensure(&mut self, query: QueryKey) -> RefreshDecision {
        if let Some(token) = self.in_flight.get(&query) {
            return RefreshDecision::AlreadyInFlight(token.clone());
        }

        self.next_request_id += 1;
        let token = RefreshToken {
            query: query.clone(),
            request_id: self.next_request_id,
        };
        self.in_flight.insert(query, token.clone());
        RefreshDecision::Started(token)
    }

    pub fn complete(&mut self, token: RefreshToken) -> bool {
        self.in_flight
            .get(&token.query)
            .is_some_and(|current| *current == token)
            && self.in_flight.remove(&token.query).is_some()
    }

    pub fn is_in_flight(&self, query: &QueryKey) -> bool {
        self.in_flight.contains_key(query)
    }
}

impl RefreshCoordinator {
    pub async fn ensure<B, K, V, F>(
        &mut self,
        backend: &B,
        request: RefreshRequest,
        collection: &mut Collection<K, V>,
        decode: F,
    ) -> Result<RefreshOutcome, RefreshError>
    where
        B: ApplicationBackend + ?Sized,
        K: Ord + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
        F: Fn(DataRow) -> Result<CollectionChange<K, V>, String>,
    {
        let token = match self.registry.ensure(request.query.clone()) {
            RefreshDecision::AlreadyInFlight(token) => {
                return Ok(RefreshOutcome::AlreadyInFlight(token));
            }
            RefreshDecision::Started(token) => token,
        };

        let result = async {
            let response = backend.refresh(request.clone()).await?;
            if response.request != request {
                return Err(RefreshError::MismatchedRequest {
                    expected: request,
                    actual: response.request,
                });
            }
            let cursor = response.cursor.clone();
            let mut changes = Vec::with_capacity(response.rows.len());
            for row in response.rows {
                let key = row.key.clone();
                changes.push(decode(row).map_err(|message| RefreshError::Decode { key, message })?);
            }
            let rows = changes.len();
            collection.replace_authoritative(changes)?;
            Ok(RefreshOutcome::Applied {
                token: token.clone(),
                cursor,
                rows,
            })
        }
        .await;
        self.registry.complete(token);
        result
    }

    pub fn is_in_flight(&self, query: &QueryKey) -> bool {
        self.registry.is_in_flight(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{DataRow, MockApplicationBackend};

    #[test]
    fn duplicate_refreshes_share_one_in_flight_token() {
        let mut registry = RefreshRegistry::default();
        let query = QueryKey::new("settings.display_hints");

        let first = match registry.ensure(query.clone()) {
            RefreshDecision::Started(token) => token,
            RefreshDecision::AlreadyInFlight(_) => {
                panic!("first refresh unexpectedly deduplicated")
            }
        };
        let second = match registry.ensure(query.clone()) {
            RefreshDecision::AlreadyInFlight(token) => token,
            RefreshDecision::Started(_) => panic!("second refresh was not deduplicated"),
        };

        assert_eq!(first, second);
        assert!(registry.is_in_flight(&query));
        assert!(registry.complete(first));
        assert!(!registry.is_in_flight(&query));
    }

    #[tokio::test]
    async fn refresh_applies_rows_and_completes_its_token() {
        let backend = MockApplicationBackend::new();
        let request = RefreshRequest {
            id: 1,
            query: QueryKey::new("settings.display_hints"),
        };
        let expected_cursor = Some(SourceCursor::new(serde_json::json!(123456789)));
        backend.queue_refresh_response(Ok(crate::backend::RefreshResponse {
            request: request.clone(),
            cursor: expected_cursor.clone(),
            rows: vec![DataRow {
                key: "title".to_owned(),
                value: serde_json::json!("visible"),
            }],
        }));
        let mut coordinator = RefreshCoordinator::default();
        let mut collection = Collection::new();

        let outcome = coordinator
            .ensure(&backend, request, &mut collection, |row| {
                Ok(CollectionChange::Upsert {
                    key: row.key,
                    value: row.value.as_str().unwrap().to_owned(),
                })
            })
            .await
            .unwrap();

        assert_eq!(
            collection.get(&"title".to_owned()),
            Some("visible".to_owned())
        );
        match outcome {
            RefreshOutcome::Applied {
                cursor, rows: 1, ..
            } => {
                assert_eq!(cursor, expected_cursor);
            }
            other => panic!("unexpected refresh outcome: {other:?}"),
        }
        assert!(!coordinator.is_in_flight(&QueryKey::new("settings.display_hints")));
    }
}
