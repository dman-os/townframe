//! SQLite facade joining the keyed-frontier read and write halves.

use super::PartFrontierKey;
use super::sqlite_read::{SqliteFrontierRow, SqlitePartSelector, part_query_rows};
use super::sqlite_write::SqliteFrontierWrite;
use crate::keyed_frontier::{SqliteReadError, SqliteReadSource, open_sqlite_reader};
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierReadLimits, FrontierRevision, KeyedFrontier, KeyedFrontierError,
    KeyedFrontierReader, KeyedFrontierResult,
};
use big_sync_core::rpc::{ObjAddedToPart, ObjChanged, PartEvent};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::{async_trait, serde_json};

const EVENT_ADDED: i64 = 0;
const EVENT_CHANGED: i64 = 1;
const EVENT_REMOVED: i64 = 2;

#[derive(Clone)]
pub(crate) struct SqlitePartFrontier {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
    scope_id: i64,
    changed: Arc<Notify>,
}

impl SqlitePartFrontier {
    pub(crate) fn new(
        read_pool: SqlitePool,
        write_pool: SqlitePool,
        scope_id: i64,
        changed: Arc<Notify>,
    ) -> Self {
        Self {
            read_pool,
            write_pool,
            scope_id,
            changed,
        }
    }
}

fn invariant(message: &'static str) -> SqliteReadError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

fn payload(row: &SqliteFrontierRow) -> Result<serde_json::Value, SqliteReadError> {
    let payload = row
        .payload_json
        .as_deref()
        .filter(|payload| !payload.is_empty())
        .ok_or_else(|| invariant("live keyed-frontier row has no payload"))?;
    serde_json::from_str(payload).map_err(|error| Box::new(error) as SqliteReadError)
}

impl SqliteReadSource for SqlitePartFrontier {
    type Selector = SqlitePartSelector;
    type Row = SqliteFrontierRow;
    type Key = PartFrontierKey;
    type Value = PartEvent;

    fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    fn changed(&self) -> &Notify {
        &self.changed
    }

    fn committed_revision(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<FrontierRevision, SqliteReadError>> + Send + '_>> {
        Box::pin(async move {
            let revision: i64 =
                sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                    .fetch_one(&self.read_pool)
                    .await?;
            Ok(u64::try_from(revision).expect("SQLite frontier revision is non-negative"))
        })
    }

    fn row_revision(&self, row: &SqliteFrontierRow) -> FrontierRevision {
        row.revision
    }

    fn fetch_rows<'a>(
        &'a self,
        selector: &'a SqlitePartSelector,
        after: FrontierRevision,
        through: FrontierRevision,
        exact_revision: Option<FrontierRevision>,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SqliteFrontierRow>, SqliteReadError>> + Send + 'a>>
    {
        Box::pin(part_query_rows(
            self,
            self.scope_id,
            selector,
            after,
            through,
            exact_revision,
            limit,
        ))
    }

    fn decode_row(
        &self,
        row: SqliteFrontierRow,
    ) -> Result<Option<FrontierEntry<Self::Key, Self::Value>>, SqliteReadError> {
        let part_id = match row.part_ref {
            0 if row.part_id.is_none() => None,
            0 => {
                return Err(invariant(
                    "sentinel frontier row unexpectedly has a part id",
                ));
            }
            _ => Some(
                row.part_id
                    .ok_or_else(|| invariant("part frontier row has no part id"))?,
            ),
        };
        let key = match part_id {
            Some(part_id) => PartFrontierKey::Part {
                obj_id: row.obj_id,
                part_id,
            },
            None => PartFrontierKey::Object(row.obj_id),
        };
        let value = match row.event_type {
            EVENT_REMOVED => None,
            EVENT_ADDED => {
                let part_id = part_id.ok_or_else(|| invariant("object key cannot be added"))?;
                Some(PartEvent::Added(ObjAddedToPart {
                    cursor: row.revision,
                    part_id,
                    obj_id: row.obj_id,
                    payload: payload(&row)?,
                }))
            }
            EVENT_CHANGED => {
                let part_ids = part_id.into_iter().collect();
                Some(PartEvent::Changed(ObjChanged {
                    cursor: row.revision,
                    part_ids,
                    obj_id: row.obj_id,
                    payload: payload(&row)?,
                }))
            }
            _ => return Err(invariant("unknown keyed-frontier event type")),
        };
        Ok(Some(FrontierEntry {
            key,
            revision: row.revision,
            value,
        }))
    }
}

#[async_trait]
impl KeyedFrontier<PartFrontierKey, PartEvent> for SqlitePartFrontier {
    type Selector = SqlitePartSelector;
    type Context<'a>
        = Transaction<'a, Sqlite>
    where
        Self: 'a;
    type Transaction<'a>
        = SqliteFrontierWrite<'a>
    where
        Self: 'a;

    async fn begin<'a>(&'a self) -> KeyedFrontierResult<Self::Transaction<'a>> {
        let transaction = self
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let changed = Arc::clone(&self.changed);
        Ok(SqliteFrontierWrite::new(
            transaction,
            self.scope_id,
            Arc::new(move |_: FrontierRevision| changed.notify_waiters()),
        ))
    }

    async fn begin_with_context<'a>(
        &'a self,
        transaction: Self::Context<'a>,
    ) -> KeyedFrontierResult<Self::Transaction<'a>> {
        let changed = Arc::clone(&self.changed);
        Ok(SqliteFrontierWrite::new(
            transaction,
            self.scope_id,
            Arc::new(move |_: FrontierRevision| changed.notify_waiters()),
        ))
    }

    async fn open(
        &self,
        selector: Self::Selector,
        limits: FrontierReadLimits,
    ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent> + '_>> {
        open_sqlite_reader(self.clone(), selector, limits).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyed_frontier::contract;
    use big_sync_core::keyed_frontier::FrontierRevision;
    use big_sync_core::rpc::{ObjChanged, PartEvent};
    use big_sync_core::{BuckId, ObjId, PartId};
    use sqlx_utils_rs::SqlCtx;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::Notify;

    struct SqliteContractHarness {
        _sql: SqlCtx,
        frontier: SqlitePartFrontier,
    }

    impl SqliteContractHarness {
        async fn new() -> Self {
            let sql = SqlCtx::memory()
                .await
                .expect("create sqlite contract database");
            crate::sqlite_core::SqliteCore::init_schema(&sql.write_pool, BuckId::MAX_LEVEL)
                .await
                .expect("initialize sqlite contract schema");
            let core = crate::sqlite_core::SqliteCore::new(
                sql.clone(),
                "keyed-frontier-contract",
                BuckId::MAX_LEVEL,
            )
            .await
            .expect("create sqlite contract core");
            let frontier = SqlitePartFrontier::new(
                sql.read_pool.clone(),
                sql.write_pool.clone(),
                core.scope_id,
                Arc::new(Notify::new()),
            );
            Self {
                _sql: sql,
                frontier,
            }
        }
    }

    impl contract::KeyedFrontierContractHarness for SqliteContractHarness {
        type Key = PartFrontierKey;
        type Value = PartEvent;
        type Frontier = SqlitePartFrontier;

        fn frontier(&self) -> &Self::Frontier {
            &self.frontier
        }

        fn key(&self, index: u64) -> PartFrontierKey {
            let obj_id = ObjId::new([index as u8; 32]);
            if index == 2 {
                PartFrontierKey::Part {
                    obj_id,
                    part_id: PartId::new([index as u8; 32]),
                }
            } else {
                PartFrontierKey::Object(obj_id)
            }
        }

        fn value(&self, index: u64) -> PartEvent {
            let object_index = (index / 10) as u8;
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: if object_index == 2 {
                    vec![PartId::new([object_index; 32])]
                } else {
                    Vec::new()
                },
                obj_id: ObjId::new([object_index; 32]),
                payload: serde_json::json!({ "value": index }),
            })
        }

        fn values_match(&self, expected: &PartEvent, actual: &PartEvent) -> bool {
            match (expected, actual) {
                (PartEvent::Changed(expected), PartEvent::Changed(actual)) => {
                    expected.part_ids == actual.part_ids
                        && expected.obj_id == actual.obj_id
                        && expected.payload == actual.payload
                }
                _ => expected == actual,
            }
        }

        fn all_selector(&self, after: FrontierRevision) -> SqlitePartSelector {
            let mut selector = SqlitePartSelector::default();
            for index in 1..=9 {
                match self.key(index) {
                    PartFrontierKey::Object(obj_id) => {
                        selector.objects.insert(obj_id, after);
                    }
                    PartFrontierKey::Part { part_id, .. } => {
                        selector.parts.insert(part_id, after);
                    }
                }
            }
            selector
        }

        fn keys_selector(
            &self,
            bounds: BTreeMap<PartFrontierKey, FrontierRevision>,
        ) -> SqlitePartSelector {
            let mut selector = SqlitePartSelector::default();
            for (key, bound) in bounds {
                match key {
                    PartFrontierKey::Object(obj_id) => {
                        selector.objects.insert(obj_id, bound);
                    }
                    PartFrontierKey::Part { part_id, .. } => {
                        selector.parts.insert(part_id, bound);
                    }
                }
            }
            selector
        }
    }

    #[tokio::test]
    async fn sqlite_part_frontier_contract() {
        contract::assert_keyed_frontier_contract(&SqliteContractHarness::new().await).await;
    }
}
