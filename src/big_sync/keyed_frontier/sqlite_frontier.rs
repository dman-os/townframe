//! SQLite facade joining the keyed-frontier read and write halves.

use super::PartFrontierKey;
use super::sqlite_read::{
    SqliteFrontierRow, SqlitePartSelector, SqliteReadError, SqliteReadSource, open_sqlite_reader,
};
use super::sqlite_write::SqliteFrontierWrite;
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierReadLimits, FrontierRevision, KeyedFrontier, KeyedFrontierError,
    KeyedFrontierReader, KeyedFrontierResult,
};
use big_sync_core::rpc::{ObjAddedToPart, ObjChanged, PartEvent};
use sqlx::{Sqlite, SqlitePool, Transaction};
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
    type Key = PartFrontierKey;
    type Value = PartEvent;

    fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    fn scope_id(&self) -> i64 {
        self.scope_id
    }

    fn changed(&self) -> &Notify {
        &self.changed
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
        open_sqlite_reader(self, selector, limits).await
    }
}
