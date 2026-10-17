//! SQLite write adapter for the part-store-shaped keyed frontier.
//!
//! The caller transfers an already-open transaction into this adapter. All
//! staged keys receive one global cursor on commit; the transaction itself is
//! finalized before the notification callback is invoked.

use crate::sqlite::queries;
use big_sync_core::keyed_frontier::{
    FrontierRevision, KeyedFrontierError, KeyedFrontierResult, KeyedFrontierTransaction,
    TransactionIsolation,
};
use big_sync_core::rpc::PartEvent;
use big_sync_core::{ObjKey, PartKey};
use sqlite_rs::Tx;
use std::collections::BTreeMap;
use std::sync::Arc;
use utils_rs::prelude::{async_trait, serde_json};

use super::PartFrontierKey;

const EVENT_CHANGED: i64 = 1;
const EVENT_REMOVED: i64 = 2;

type Notify = Arc<dyn Fn(FrontierRevision) + Send + Sync>;

fn invalid_event(message: &'static str) -> KeyedFrontierError {
    KeyedFrontierError::Backend(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    )))
}

pub(crate) struct SqliteFrontierWrite {
    transaction: Option<Tx>,
    scope_id: i64,
    staged: BTreeMap<PartFrontierKey, Option<PartEvent>>,
    reserved_revision: Option<FrontierRevision>,
    notify: Notify,
}

impl SqliteFrontierWrite {
    pub(crate) fn new(transaction: Tx, scope_id: i64, notify: Notify) -> Self {
        Self {
            transaction: Some(transaction),
            scope_id,
            staged: BTreeMap::new(),
            reserved_revision: None,
            notify,
        }
    }

    fn transaction_mut(&mut self) -> &mut Tx {
        self.transaction
            .as_mut()
            .expect("sqlite frontier transaction present")
    }

    async fn reserve_revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        let value = queries::next_id(self.transaction_mut(), "global_cursor".to_owned())
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
        let value = queries::global_cursor(self.transaction_mut())
            .await
            .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        u64::try_from(value).map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn obj_ref(&mut self, obj_id: ObjKey) -> KeyedFrontierResult<i64> {
        let buck_index = i64::from(big_sync_core::BuckId::deepest_from_obj_key(&obj_id).index());
        let scope_id = self.scope_id;
        queries::ensure_obj_ref(
            self.transaction_mut(),
            scope_id,
            obj_id.as_bytes().to_vec(),
            buck_index,
        )
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn part_ref(&mut self, part_id: PartKey) -> KeyedFrontierResult<i64> {
        let scope_id = self.scope_id;
        queries::ensure_part_ref(
            self.transaction_mut(),
            scope_id,
            part_id.as_bytes().to_vec(),
        )
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))
    }

    async fn load_current(
        &mut self,
        key: &PartFrontierKey,
    ) -> KeyedFrontierResult<Option<PartEvent>> {
        let scope_id = self.scope_id;
        let (obj_id, maybe_part_ref) = match key {
            PartFrontierKey::Object(obj_id) => (obj_id.clone(), 0),
            PartFrontierKey::Part { obj_id, part_id } => {
                let part_ref = queries::find_part_ref(
                    self.transaction_mut(),
                    scope_id,
                    part_id.as_bytes().to_vec(),
                )
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
                let Some(part_ref) = part_ref else {
                    return Ok(None);
                };
                (obj_id.clone(), part_ref)
            }
        };
        let row = queries::frontier_member(
            self.transaction_mut(),
            scope_id,
            obj_id.as_bytes().to_vec(),
            maybe_part_ref,
        )
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        let Some(queries::FrontierMemberRow {
            event_type,
            txid: cursor,
            payload_json,
        }) = row
        else {
            return Ok(None);
        };
        if event_type == EVENT_REMOVED {
            return Ok(None);
        }
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
            PartFrontierKey::Part { part_id, .. } if event_type == EVENT_CHANGED => {
                PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor,
                    part_ids: vec![part_id.clone()],
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
        let (obj_id, part_id) = match &key {
            PartFrontierKey::Object(obj_id) => (obj_id.clone(), None),
            PartFrontierKey::Part { obj_id, part_id } => (obj_id.clone(), Some(part_id.clone())),
        };
        let obj_ref = self.obj_ref(obj_id).await?;
        let (event_type, payload) = match (key, mutation) {
            (PartFrontierKey::Object(obj_id), Some(PartEvent::Changed(changed)))
                if changed.obj_id == obj_id && changed.part_ids.is_empty() =>
            {
                (EVENT_CHANGED, Some(changed.payload))
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
        let scope_id = self.scope_id;
        if let Some(payload) = payload {
            let payload_json = serde_json::to_string(&payload)
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
            queries::set_obj_payload(self.transaction_mut(), payload_json, obj_ref)
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        }
        let maybe_part_ref = match part_id {
            Some(part_id) => self.part_ref(part_id).await?,
            None => 0,
        };
        // `added_at` is the cursor at which this row most recently became present: set on
        // insert and on the tombstone-to-present transition (a re-add), and preserved by a
        // present-to-present touch. A row that stays a tombstone keeps whatever stamp it had,
        // because the stamp is what tells a reader the removal is its business.
        let revision = i64::try_from(revision).expect("frontier revision fits sqlite integer");
        queries::upsert_member_event(
            self.transaction_mut(),
            scope_id,
            obj_ref,
            maybe_part_ref,
            event_type,
            revision,
            EVENT_REMOVED,
        )
        .await
        .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        if maybe_part_ref != 0 {
            queries::bump_part_cursor(self.transaction_mut(), revision, scope_id, maybe_part_ref)
                .await
                .map_err(|error| KeyedFrontierError::Backend(Box::new(error)))?;
        }
        Ok(())
    }
}

#[async_trait]
impl KeyedFrontierTransaction<PartFrontierKey, PartEvent> for SqliteFrontierWrite {
    type Context = Tx;

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
