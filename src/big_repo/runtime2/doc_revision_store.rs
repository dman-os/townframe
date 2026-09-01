//! An inert view of materialized Automerge frontier revisions from the local part log.
//!
//! This adapter exposes physical branch heads and group membership from the
//! frontier worker. It is not a second projection, revision log, or driver.
use crate::interlude::*;
use crate::runtime2::automerge_obj_to_doc_id;
use big_sync::{HostPartStore, LocalPartRevisionReader};
use big_sync_core::part_store::ObjPayload;
use big_sync_core::revisioned_store::{
    RevisionRead, RevisionReadLimits, RevisionedStore, RevisionedStoreReader,
};
use big_sync_core::rpc::{
    ObjAddedToPart, ObjChanged, ObjRemovedFromPart, SubEvent, SubPartsRequest, SubscriptionTarget,
};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomergeFrontierEvent {
    Added {
        doc_id: crate::DocumentId,
        heads: Arc<[automerge::ChangeHash]>,
        route: PartId,
        revision: u64,
    },
    Changed {
        doc_id: crate::DocumentId,
        heads: Arc<[automerge::ChangeHash]>,
        routes: Vec<PartId>,
        revision: u64,
    },
    Removed {
        doc_id: crate::DocumentId,
        route: PartId,
        revision: u64,
    },
}

/// One source-selection target for materialized Automerge frontier data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutomergeFrontierTarget {
    Part { part_id: PartId },
    Object { obj_id: ObjId },
}

/// Selects the authorized physical part/object stream used for Automerge
/// frontier reads. The replay cursor is supplied separately to `open`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutomergeFrontierSelector {
    // Foreign objects are excluded by the explicit object/part subscription;
    // the dedicated frontier scope makes ObjId -> DocumentId bijective.
    pub targets: Vec<AutomergeFrontierTarget>,
}

/// Inert source of materialized physical branch heads and group membership.
///
/// This is an adapter over the Automerge frontier worker, not a projection,
/// durable log, or background driver.
pub struct AutomergeFrontierRevisionStore {
    store: Arc<dyn HostPartStore>,
}

impl AutomergeFrontierRevisionStore {
    pub fn new(store: Arc<dyn HostPartStore>) -> Self {
        Self { store }
    }
}

pub struct Reader<'a> {
    inner: Box<dyn LocalPartRevisionReader>,
    _store: &'a AutomergeFrontierRevisionStore,
}

#[async_trait::async_trait]
impl RevisionedStore for AutomergeFrontierRevisionStore {
    type Revision = u64;
    type Entry = AutomergeFrontierEvent;
    type Selector = AutomergeFrontierSelector;
    type Error = eyre::Report;
    type Reader<'a> = Reader<'a>;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.store.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: u64,
        limits: RevisionReadLimits,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        let reqs = SubPartsRequest {
            lower_bound: after,
            targets: selector
                .targets
                .into_iter()
                .map(|target| match target {
                    AutomergeFrontierTarget::Part { part_id } => SubscriptionTarget::Part {
                        part_id,
                        cursor: after,
                    },
                    AutomergeFrontierTarget::Object { obj_id } => {
                        SubscriptionTarget::Object { obj_id }
                    }
                })
                .collect(),
        };
        let inner = self
            .store
            .open_local_revision_reader(reqs, limits)
            .await
            .wrap_err("opening physical document revisions")??;
        Ok(Reader {
            inner,
            _store: self,
        })
    }
}

fn doc_and_heads(
    obj_id: ObjId,
    payload: &ObjPayload,
    revision: u64,
) -> Res<(crate::DocumentId, Arc<[automerge::ChangeHash]>)> {
    let heads = payload
        .get("heads")
        .and_then(Value::as_array)
        .ok_or_else(|| ferr!("malformed physical frontier payload"))?;
    let names = heads
        .iter()
        .map(|head| {
            head.as_str()
                .ok_or_else(|| ferr!("malformed physical frontier head"))
        })
        .collect::<Res<Vec<_>>>()?;
    Ok((
        automerge_obj_to_doc_id(obj_id),
        am_utils_rs::parse_commit_heads(&names)
            .wrap_err_with(|| format!("invalid frontier payload at revision {revision}"))?,
    ))
}

#[async_trait::async_trait]
impl RevisionedStoreReader<u64, AutomergeFrontierEvent, eyre::Report> for Reader<'_> {
    async fn next(&mut self) -> Result<RevisionRead<u64, AutomergeFrontierEvent>, eyre::Report> {
        let read = self
            .inner
            .next()
            .await
            .wrap_err("reading physical document revisions")?;
        match read {
            RevisionRead::ReplayComplete { through } => {
                Ok(RevisionRead::ReplayComplete { through })
            }
            RevisionRead::Entries { revision, entries } => {
                let mut out = Vec::new();
                for event in entries {
                    match event {
                        SubEvent::Added(ObjAddedToPart {
                            cursor,
                            part_id,
                            obj_id,
                            payload,
                        }) => {
                            let (doc_id, heads) = doc_and_heads(obj_id, &payload, revision)
                                .wrap_err("invalid physical document revision")?;
                            out.push(AutomergeFrontierEvent::Added {
                                doc_id,
                                heads,
                                route: part_id,
                                revision: cursor,
                            });
                        }
                        SubEvent::Changed(ObjChanged {
                            cursor,
                            part_ids,
                            obj_id,
                            payload,
                        }) => {
                            let (doc_id, heads) = doc_and_heads(obj_id, &payload, revision)
                                .wrap_err("invalid physical document revision")?;
                            out.push(AutomergeFrontierEvent::Changed {
                                doc_id,
                                heads,
                                routes: part_ids,
                                revision: cursor,
                            });
                        }
                        SubEvent::Removed(ObjRemovedFromPart {
                            cursor,
                            part_id,
                            obj_id,
                        }) => out.push(AutomergeFrontierEvent::Removed {
                            doc_id: automerge_obj_to_doc_id(obj_id),
                            route: part_id,
                            revision: cursor,
                        }),
                        SubEvent::ReplayComplete => {
                            unreachable!("part reader emits replay boundary separately")
                        }
                    }
                }
                Ok(RevisionRead::Entries {
                    revision,
                    entries: out,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Scripted(VecDeque<big_sync_core::revisioned_store::RevisionRead<u64, SubEvent>>);
    #[async_trait::async_trait]
    impl LocalPartRevisionReader for Scripted {
        async fn next(
            &mut self,
        ) -> Res<big_sync_core::revisioned_store::RevisionRead<u64, SubEvent>> {
            Ok(self.0.pop_front().expect("script exhausted"))
        }
    }

    fn payload(byte: u8) -> ObjPayload {
        let heads = am_utils_rs::serialize_commit_heads(&[automerge::ChangeHash([byte; 32])]);
        serde_json::json!({ "heads": heads })
    }

    #[tokio::test]
    async fn reader_maps_atomic_events_and_replay_boundary() -> Res<()> {
        let obj = ObjId::new([9; 32]);
        let p1 = PartId::new([1; 32]);
        let p2 = PartId::new([2; 32]);
        let reads = VecDeque::from([
            RevisionRead::Entries {
                revision: 3,
                entries: vec![
                    SubEvent::Added(ObjAddedToPart {
                        cursor: 3,
                        part_id: p1,
                        obj_id: obj,
                        payload: payload(1),
                    }),
                    SubEvent::Changed(ObjChanged {
                        cursor: 3,
                        part_ids: vec![p1, p2],
                        obj_id: obj,
                        payload: payload(2),
                    }),
                    SubEvent::Removed(ObjRemovedFromPart {
                        cursor: 3,
                        part_id: p1,
                        obj_id: obj,
                    }),
                ],
            },
            RevisionRead::Entries {
                revision: 4,
                entries: vec![],
            },
            RevisionRead::ReplayComplete { through: 4 },
            RevisionRead::Entries {
                revision: 5,
                entries: vec![],
            },
        ]);
        let store = AutomergeFrontierRevisionStore {
            store: Arc::new(big_sync::MemoryPartStore::default()),
        };
        let mut reader = Reader {
            inner: Box::new(Scripted(reads)),
            _store: &store,
        };
        let first = reader.next().await?;
        assert!(
            matches!(first, RevisionRead::Entries { revision: 3, entries } if entries.len() == 3)
        );
        assert!(
            matches!(reader.next().await?, RevisionRead::Entries { revision: 4, entries } if entries.is_empty())
        );
        assert_eq!(
            reader.next().await?,
            RevisionRead::ReplayComplete { through: 4 }
        );
        assert!(
            matches!(reader.next().await?, RevisionRead::Entries { revision: 5, entries } if entries.is_empty())
        );
        Ok(())
    }

    #[test]
    fn malformed_heads_are_errors() {
        let err = doc_and_heads(ObjId::new([7; 32]), &serde_json::json!({"heads": [3]}), 1);
        assert!(err.is_err());
        let err = doc_and_heads(ObjId::new([7; 32]), &serde_json::json!({}), 1);
        assert!(err.is_err());
    }
}
