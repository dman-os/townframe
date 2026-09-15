//! SQLite write adapter for the part-store-shaped keyed frontier.
//!
//! The caller transfers an already-open transaction into this adapter. All
//! staged keys receive one global cursor on commit; the transaction itself is
//! finalized before the notification callback is invoked.

use big_sync_core::keyed_frontier::{
    FrontierRevision, KeyedFrontierError, KeyedFrontierResult, KeyedFrontierTransaction,
    TransactionIsolation,
};
use big_sync_core::rpc::PartEvent;
use big_sync_core::{ObjId, PartId};
use sqlx::{Row, Sqlite, Transaction};
use std::collections::BTreeMap;
use std::sync::Arc;
use utils_rs::prelude::{async_trait, serde_json};

use super::PartFrontierKey;

const EVENT_ADDED: i64 = 0;
const EVENT_CHANGED: i64 = 1;
const EVENT_REMOVED: i64 = 2;

type Notify = Arc<dyn Fn(FrontierRevision) + Send + Sync>;

fn invalid_event(message: &'static str) -> KeyedFrontierError {
    KeyedFrontierError::Backend(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    )))
}

pub(crate) struct SqliteFrontierWrite<'a> {
    transaction: Option<Transaction<'a, Sqlite>>,
    scope_id: i64,
    staged: BTreeMap<PartFrontierKey, Option<PartEvent>>,
    reserved_revision: Option<FrontierRevision>,
    notify: Notify,
}

impl<'a> SqliteFrontierWrite<'a> {
    pub(crate) fn new(transaction: Transaction<'a, Sqlite>, scope_id: i64, notify: Notify) -> Self {
        Self {
            transaction: Some(transaction),
            scope_id,
            staged: BTreeMap::new(),
            reserved_revision: None,
            notify,
        }
    }

    fn transaction_mut(&mut self) -> &mut Transaction<'a, Sqlite> {
        self.transaction
            .as_mut()
            .expect("sqlite frontier transaction present")
    }

    async fn reserve_revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        let value: i64 = sqlx::query_scalar(
            "UPDATE big_sync_meta SET value = value + 1 WHERE key = 'global_cursor' RETURNING value",
        )
        .fetch_one(&mut **self.transaction_mut())
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        u64::try_from(value).map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        if let Some(revision) = self.reserved_revision {
            return Ok(revision);
        }
        let revision = self.reserve_revision().await?;
        self.reserved_revision = Some(revision);
        Ok(revision)
    }

    async fn current_revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        let value: i64 =
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&mut **self.transaction_mut())
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        u64::try_from(value).map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn obj_ref(&mut self, obj_id: ObjId) -> KeyedFrontierResult<i64> {
        sqlx::query("INSERT OR IGNORE INTO big_sync_objs(scope_id, obj_id) VALUES (?, ?)")
            .bind(self.scope_id)
            .bind(obj_id.0.into_bytes().to_vec())
            .execute(&mut **self.transaction_mut())
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        sqlx::query_scalar("SELECT obj_ref FROM big_sync_objs WHERE scope_id = ? AND obj_id = ?")
            .bind(self.scope_id)
            .bind(obj_id.0.into_bytes().to_vec())
            .fetch_one(&mut **self.transaction_mut())
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn part_ref(&mut self, part_id: PartId) -> KeyedFrontierResult<i64> {
        sqlx::query("INSERT OR IGNORE INTO big_sync_parts(scope_id, part_id) VALUES (?, ?)")
            .bind(self.scope_id)
            .bind(part_id.0.into_bytes().to_vec())
            .execute(&mut **self.transaction_mut())
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        sqlx::query_scalar("SELECT part_ref FROM big_sync_parts WHERE scope_id = ? AND part_id = ?")
            .bind(self.scope_id)
            .bind(part_id.0.into_bytes().to_vec())
            .fetch_one(&mut **self.transaction_mut())
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn load_current(
        &mut self,
        key: &PartFrontierKey,
    ) -> KeyedFrontierResult<Option<PartEvent>> {
        let (obj_id, maybe_part_ref) = match key {
            PartFrontierKey::Object(obj_id) => (*obj_id, 0),
            PartFrontierKey::Part { obj_id, part_id } => {
                let part_ref = sqlx::query_scalar(
                    "SELECT part_ref
                       FROM big_sync_parts
                      WHERE scope_id = ?
                        AND part_id = ?",
                )
                .bind(self.scope_id)
                .bind(part_id.0.into_bytes().to_vec())
                .fetch_optional(&mut **self.transaction_mut())
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
                let Some(part_ref) = part_ref else {
                    return Ok(None);
                };
                (*obj_id, part_ref)
            }
        };
        let row = sqlx::query(
            "SELECT m.event_type
                 , m.txid
                 , o.payload_json
              FROM big_sync_members m
              JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
             WHERE m.scope_id = ?
               AND m.obj_ref = (SELECT obj_ref FROM big_sync_objs WHERE scope_id = ? AND obj_id = ?)
               AND m.maybe_part_ref = ?",
        )
        .bind(self.scope_id)
        .bind(self.scope_id)
        .bind(obj_id.0.into_bytes().to_vec())
        .bind(maybe_part_ref)
        .fetch_optional(&mut **self.transaction_mut())
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let event_type: i64 = row
            .try_get("event_type")
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let cursor: i64 = row
            .try_get("txid")
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        if event_type == EVENT_REMOVED {
            return Ok(None);
        }
        let payload_json: Option<String> = row
            .try_get("payload_json")
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let Some(payload_json) = payload_json.filter(|payload| !payload.is_empty()) else {
            return Err(KeyedFrontierError::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "live keyed-frontier row has no payload",
            ))));
        };
        let payload = serde_json::from_str(&payload_json)
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let cursor =
            u64::try_from(cursor).map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let event = match key {
            PartFrontierKey::Object(_) if event_type == EVENT_CHANGED => {
                PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor,
                    part_ids: Vec::new(),
                    obj_id,
                    payload,
                })
            }
            PartFrontierKey::Part { part_id, .. } if event_type == EVENT_ADDED => {
                PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                    cursor,
                    part_id: *part_id,
                    obj_id,
                    payload,
                })
            }
            PartFrontierKey::Part { part_id, .. } if event_type == EVENT_CHANGED => {
                PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor,
                    part_ids: vec![*part_id],
                    obj_id,
                    payload,
                })
            }
            _ if event_type == EVENT_REMOVED => return Ok(None),
            _ => return Err(invalid_event("invalid keyed-frontier event type for key")),
        };
        Ok(Some(event))
    }

    async fn apply(
        &mut self,
        revision: FrontierRevision,
        key: PartFrontierKey,
        mutation: Option<PartEvent>,
    ) -> KeyedFrontierResult<()> {
        let (obj_id, part_id) = match key {
            PartFrontierKey::Object(obj_id) => (obj_id, None),
            PartFrontierKey::Part { obj_id, part_id } => (obj_id, Some(part_id)),
        };
        let obj_ref = self.obj_ref(obj_id).await?;
        let (event_type, payload) = match (key, mutation) {
            (PartFrontierKey::Object(obj_id), Some(PartEvent::Changed(changed)))
                if changed.obj_id == obj_id && changed.part_ids.is_empty() =>
            {
                (EVENT_CHANGED, Some(changed.payload))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Added(added)))
                if added.obj_id == obj_id && added.part_id == part_id =>
            {
                (EVENT_ADDED, Some(added.payload))
            }
            (PartFrontierKey::Part { obj_id, part_id }, Some(PartEvent::Changed(changed)))
                if changed.obj_id == obj_id
                    && changed.part_ids.len() == 1
                    && changed.part_ids[0] == part_id =>
            {
                (EVENT_CHANGED, Some(changed.payload))
            }
            (_, None) => (EVENT_REMOVED, None),
            _ => return Err(invalid_event("invalid keyed-frontier event for key")),
        };
        if let Some(payload) = payload {
            let payload_json = serde_json::to_string(&payload)
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
            sqlx::query(
                "UPDATE big_sync_objs SET payload_json = ? WHERE scope_id = ? AND obj_ref = ?",
            )
            .bind(payload_json)
            .bind(self.scope_id)
            .bind(obj_ref)
            .execute(&mut **self.transaction_mut())
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        }
        let maybe_part_ref = match part_id {
            Some(part_id) => self.part_ref(part_id).await?,
            None => 0,
        };
        sqlx::query(
            "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET event_type = excluded.event_type, txid = excluded.txid",
        )
        .bind(self.scope_id)
        .bind(obj_ref)
        .bind(maybe_part_ref)
        .bind(event_type)
        .bind(i64::try_from(revision).expect("frontier revision fits sqlite integer"))
        .execute(&mut **self.transaction_mut())
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        if maybe_part_ref != 0 {
            sqlx::query("UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?) WHERE scope_id = ? AND part_ref = ?")
                .bind(i64::try_from(revision).expect("frontier revision fits sqlite integer"))
                .bind(self.scope_id)
                .bind(maybe_part_ref)
                .execute(&mut **self.transaction_mut())
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        }
        Ok(())
    }
}

#[async_trait]
impl<'a> KeyedFrontierTransaction<PartFrontierKey, PartEvent> for SqliteFrontierWrite<'a> {
    type Context = Transaction<'a, Sqlite>;

    fn isolation(&self) -> TransactionIsolation {
        TransactionIsolation::Serializable
    }

    fn context_mut(&mut self) -> &mut Self::Context {
        self.transaction_mut()
    }

    async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        SqliteFrontierWrite::revision(self).await
    }

    async fn get(&mut self, key: &PartFrontierKey) -> KeyedFrontierResult<Option<PartEvent>> {
        if let Some(value) = self.staged.get(key) {
            return Ok(value.clone());
        }
        self.load_current(key).await
    }

    async fn put(&mut self, key: PartFrontierKey, value: PartEvent) -> KeyedFrontierResult<()> {
        self.staged.insert(key, Some(value));
        Ok(())
    }

    async fn delete(&mut self, key: PartFrontierKey) -> KeyedFrontierResult<()> {
        self.staged.insert(key, None);
        Ok(())
    }

    async fn commit(mut self) -> KeyedFrontierResult<FrontierRevision> {
        let had_mutations = !self.staged.is_empty();
        let had_reserved_revision = self.reserved_revision.is_some();
        let revision = if had_mutations || had_reserved_revision {
            self.revision().await?
        } else {
            self.current_revision().await?
        };
        let staged = std::mem::take(&mut self.staged);
        for (key, mutation) in staged {
            self.apply(revision, key, mutation).await?;
        }
        self.transaction
            .take()
            .expect("sqlite frontier transaction present")
            .commit()
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        if had_mutations || had_reserved_revision {
            (self.notify)(revision);
        }
        Ok(revision)
    }

    async fn rollback(mut self) -> KeyedFrontierResult<()> {
        self.transaction
            .take()
            .expect("sqlite frontier transaction present")
            .rollback()
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }
}
