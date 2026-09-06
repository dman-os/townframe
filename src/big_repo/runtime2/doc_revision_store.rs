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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomergeFrontierEvent {
    Added {
        doc_id: crate::DocumentId,
        heads: Arc<[automerge::ChangeHash]>,
        causal_epoch: Option<[u8; 32]>,
        route: PartId,
        revision: u64,
    },
    Changed {
        doc_id: crate::DocumentId,
        heads: Arc<[automerge::ChangeHash]>,
        causal_epoch: Option<[u8; 32]>,
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
    /// Read every part and object in the local store, including newly-created parts.
    All,
    Part {
        part_id: PartId,
    },
    Object {
        obj_id: ObjId,
    },
}

/// Selects the authorized physical part/object stream used for Automerge
/// frontier reads. `All` explicitly selects the trusted local all-parts-and-
/// objects reader. The replay cursor is supplied separately to `open`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutomergeFrontierSelector {
    // The dedicated frontier scope makes ObjId -> DocumentId bijective;
    // object/part targets additionally restrict reads for per-document users.
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

pub struct Reader {
    inner: Box<dyn LocalPartRevisionReader>,
    store: Arc<dyn HostPartStore>,
    pending_read: Option<RevisionRead<u64, SubEvent>>,
}

#[async_trait::async_trait]
impl RevisionedStore for AutomergeFrontierRevisionStore {
    type Revision = u64;
    type Entry = AutomergeFrontierEvent;
    type Selector = AutomergeFrontierSelector;
    type Error = eyre::Report;
    type Reader<'a> = Reader;

    async fn latest_revision(&self) -> Result<Self::Revision, Self::Error> {
        self.store.latest_revision().await
    }

    async fn open<'a>(
        &'a self,
        selector: Self::Selector,
        after: u64,
    ) -> Result<Self::Reader<'a>, Self::Error> {
        let inner = if selector
            .targets
            .iter()
            .any(|target| matches!(target, AutomergeFrontierTarget::All))
        {
            self.store
                .open_local_revision_reader_all(after)
                .await
                .wrap_err("opening all physical document revisions")??
        } else {
            let reqs = SubPartsRequest {
                lower_bound: after,
                targets: selector
                    .targets
                    .into_iter()
                    .map(|target| match target {
                        AutomergeFrontierTarget::All => {
                            unreachable!("all target handled before building a subscription")
                        }
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
            self.store
                .open_local_revision_reader(reqs)
                .await
                .wrap_err("opening physical document revisions")??
        };
        Ok(Reader {
            inner,
            store: Arc::clone(&self.store),
            pending_read: None,
        })
    }
}

type FrontierDocState = (
    crate::DocumentId,
    Arc<[automerge::ChangeHash]>,
    Option<[u8; 32]>,
);

fn doc_and_heads(obj_id: ObjId, payload: &ObjPayload, revision: u64) -> Res<FrontierDocState> {
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
    let causal_epoch = match payload.get("causal_epoch") {
        None | Some(Value::Null) => None,
        Some(epoch) => Some(
            serde_json::from_value(epoch.clone()).wrap_err("invalid physical frontier epoch")?,
        ),
    };
    Ok((
        automerge_obj_to_doc_id(obj_id),
        am_utils_rs::parse_commit_heads(&names)
            .wrap_err_with(|| format!("invalid frontier payload at revision {revision}"))?,
        causal_epoch,
    ))
}

#[async_trait::async_trait]
impl RevisionedStoreReader<u64, AutomergeFrontierEvent, eyre::Report> for Reader {
    async fn next(
        &mut self,
        limits: RevisionReadLimits,
    ) -> Result<RevisionRead<u64, AutomergeFrontierEvent>, eyre::Report> {
        if self.pending_read.is_none() {
            self.pending_read = Some(
                self.inner
                    .next(limits)
                    .await
                    .wrap_err("reading physical document revisions")?,
            );
        }
        match self
            .pending_read
            .as_ref()
            .expect("pending physical revision read initialized")
        {
            RevisionRead::ReplayComplete { through } => {
                let through = *through;
                self.pending_read = None;
                Ok(RevisionRead::ReplayComplete { through })
            }
            RevisionRead::Entries { revision, entries } => {
                let revision = *revision;
                let entries = entries.clone();
                let mut out = Vec::new();
                for event in entries {
                    match event {
                        SubEvent::Added(ObjAddedToPart {
                            cursor,
                            part_id,
                            obj_id,
                            payload,
                        }) => {
                            let (doc_id, heads, causal_epoch) =
                                doc_and_heads(obj_id, &payload, revision)
                                    .wrap_err("invalid physical document revision")?;
                            out.push(AutomergeFrontierEvent::Added {
                                doc_id,
                                heads,
                                causal_epoch,
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
                            // Object subscriptions also observe part-membership
                            // removals. PartRevisionReader represents those as a
                            // payload-less Changed event; it is route churn, not
                            // an Automerge frontier payload. Do not feed it to
                            // the frontier decoder.
                            if payload.is_null() && part_ids.is_empty() {
                                continue;
                            }
                            let (doc_id, heads, causal_epoch) =
                                doc_and_heads(obj_id, &payload, revision)
                                    .wrap_err("invalid physical document revision")?;
                            out.push(AutomergeFrontierEvent::Changed {
                                doc_id,
                                heads,
                                causal_epoch,
                                routes: part_ids,
                                revision: cursor,
                            });
                        }
                        SubEvent::Removed(ObjRemovedFromPart {
                            cursor,
                            part_id,
                            obj_id,
                        }) => {
                            // A frontier object may be routed through multiple parts. A
                            // removal from one part is only a document removal once the
                            // object has no remaining frontier routes.
                            if !self
                                .store
                                .obj_parts(obj_id)
                                .await
                                .wrap_err("reading remaining frontier routes")?
                                .is_empty()
                            {
                                continue;
                            }
                            out.push(AutomergeFrontierEvent::Removed {
                                doc_id: automerge_obj_to_doc_id(obj_id),
                                route: part_id,
                                revision: cursor,
                            });
                        }
                        SubEvent::ReplayComplete => {
                            unreachable!("part reader emits replay boundary separately")
                        }
                    }
                }
                self.pending_read = None;
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
            _limits: big_sync_core::revisioned_store::RevisionReadLimits,
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
            store: Arc::clone(&store.store),
            pending_read: None,
        };
        let first = reader.next(RevisionReadLimits::default()).await?;
        assert!(
            matches!(first, RevisionRead::Entries { revision: 3, entries } if entries.len() == 3)
        );
        assert!(
            matches!(reader.next(RevisionReadLimits::default()).await?, RevisionRead::Entries { revision: 4, entries } if entries.is_empty())
        );
        assert_eq!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::ReplayComplete { through: 4 }
        );
        assert!(
            matches!(reader.next(RevisionReadLimits::default()).await?, RevisionRead::Entries { revision: 5, entries } if entries.is_empty())
        );
        Ok(())
    }

    #[tokio::test]
    async fn reader_skips_route_removal_with_remaining_frontier_route() -> Res<()> {
        let obj = ObjId::new([9; 32]);
        let p1 = PartId::new([1; 32]);
        let p2 = PartId::new([2; 32]);
        let store = Arc::new(big_sync::MemoryPartStore::default());
        store.add_obj_to_parts(obj, vec![p1, p2]).await?;
        store.remove_obj_from_part(obj, p1).await?;

        let reads = VecDeque::from([RevisionRead::Entries {
            revision: 7,
            entries: vec![SubEvent::Removed(ObjRemovedFromPart {
                cursor: 7,
                part_id: p1,
                obj_id: obj,
            })],
        }]);
        let mut reader = Reader {
            inner: Box::new(Scripted(reads)),
            store,
            pending_read: None,
        };

        assert!(matches!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries { revision: 7, entries } if entries.is_empty()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reader_ignores_object_subscription_route_removal_churn() -> Res<()> {
        let obj = ObjId::new([9; 32]);
        let reads = VecDeque::from([RevisionRead::Entries {
            revision: 7,
            entries: vec![SubEvent::Changed(ObjChanged {
                cursor: 7,
                part_ids: Vec::new(),
                obj_id: obj,
                payload: serde_json::Value::Null,
            })],
        }]);
        let store = AutomergeFrontierRevisionStore {
            store: Arc::new(big_sync::MemoryPartStore::default()),
        };
        let mut reader = Reader {
            inner: Box::new(Scripted(reads)),
            store: Arc::clone(&store.store),
            pending_read: None,
        };
        assert!(matches!(
            reader.next(RevisionReadLimits::default()).await?,
            RevisionRead::Entries { revision: 7, entries } if entries.is_empty()
        ));
        Ok(())
    }

    #[test]
    fn frontier_payload_preserves_causal_epoch() {
        let heads = am_utils_rs::serialize_commit_heads(&[automerge::ChangeHash([1; 32])]);
        let epoch = [7; 32];
        let (_, parsed_heads, parsed_epoch) = doc_and_heads(
            ObjId::new([7; 32]),
            &serde_json::json!({ "heads": heads, "causal_epoch": epoch }),
            1,
        )
        .expect("frontier payload must decode");
        assert_eq!(parsed_heads.len(), 1);
        assert_eq!(parsed_epoch, Some(epoch));
    }

    #[test]
    fn frontier_payload_accepts_null_causal_epoch() {
        let heads = am_utils_rs::serialize_commit_heads(&[automerge::ChangeHash([1; 32])]);
        let (_, _, parsed_epoch) = doc_and_heads(
            ObjId::new([7; 32]),
            &serde_json::json!({ "heads": heads, "causal_epoch": null }),
            1,
        )
        .expect("frontier payload with null epoch must decode");
        assert_eq!(parsed_epoch, None);
    }

    #[test]
    fn malformed_heads_are_errors() {
        let err = doc_and_heads(ObjId::new([7; 32]), &serde_json::json!({"heads": [3]}), 1);
        assert!(err.is_err());
        let err = doc_and_heads(ObjId::new([7; 32]), &serde_json::json!({}), 1);
        assert!(err.is_err());
    }

    #[test]
    fn malformed_causal_epoch_is_error() {
        let heads = am_utils_rs::serialize_commit_heads(&[automerge::ChangeHash([1; 32])]);
        let err = doc_and_heads(
            ObjId::new([7; 32]),
            &serde_json::json!({ "heads": heads, "causal_epoch": [1, 2] }),
            1,
        );
        assert!(err.is_err());
    }
}
