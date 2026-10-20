//! The MDK's typed document-mirror authoring surface (ADR 015 §5, §4). This adds ergonomics —
//! typed decoding, named bindings, observable values, and a receipt vocabulary that does not
//! require callers to navigate wire enums — without adding authority: the underlying
//! `documents::DocumentAccess` resource is still the only capability, and filtering still
//! happens host-side before data reaches this mirror.
use crate::documents;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::{Rc, Weak},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FacetState<T = String> {
    Loading,
    Absent,
    Present(T),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MirrorStatus {
    Live,
    Lagged,
    Released,
    Revoked,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MirrorSnapshot {
    pub revision: u64,
    pub facets: BTreeMap<(String, String), FacetState>,
    pub pending: Vec<u64>,
    pub refreshing: bool,
    pub status: MirrorStatus,
}

impl MirrorSnapshot {
    pub fn value<T: serde::de::DeserializeOwned>(
        &self,
        tag: &str,
        key_id: &str,
    ) -> Result<FacetState<T>, documents::DocumentError> {
        if matches!(self.status, MirrorStatus::Released | MirrorStatus::Revoked) {
            return Err(documents::DocumentError::Revoked);
        }
        match self.facets.get(&(tag.to_owned(), key_id.to_owned())) {
            Some(FacetState::Present(raw)) => Ok(FacetState::Present(
                serde_json::from_str(raw)
                    .expect("configuration facet violates its declared value shape"),
            )),
            Some(FacetState::Loading) => Ok(FacetState::Loading),
            Some(FacetState::Absent) => Ok(FacetState::Absent),
            None => Err(documents::DocumentError::Denied),
        }
    }
}

#[derive(Clone, Default)]
pub struct DocumentMirror {
    state: Rc<RefCell<MirrorState>>,
}

#[derive(Default)]
struct MirrorState {
    watches: BTreeMap<u64, Weak<WatchState>>,
    operations: BTreeMap<u64, MirrorOperation>,
}

impl DocumentMirror {
    pub fn bind(&self, binding: documents::DocumentBinding) -> ConfigDocument {
        ConfigDocument {
            binding,
            mirror: self.clone(),
        }
    }

    fn watch(&self, id: u64) -> Option<MirrorWatch> {
        self.state
            .borrow()
            .watches
            .get(&id)
            .and_then(Weak::upgrade)
            .map(|state| MirrorWatch { state })
    }

    fn retain(
        &self,
        remote: Option<documents::DocumentWatch>,
        route: documents::FacetRoute,
        snapshot: documents::DocumentSnapshot,
    ) -> MirrorWatch {
        let id = snapshot.watch_id;
        let watch = MirrorWatch {
            state: Rc::new(WatchState {
                id,
                snapshot: RefCell::new(mirror_snapshot(snapshot, &route)),
                route,
                remote: RefCell::new(remote),
                mirror: Rc::downgrade(&self.state),
            }),
        };
        if watch.current().status != MirrorStatus::Revoked {
            assert!(
                self.state
                    .borrow_mut()
                    .watches
                    .insert(id, Rc::downgrade(&watch.state))
                    .is_none(),
                "document watch identity reused"
            );
        } else {
            watch.detach();
        }
        watch
    }

    fn track(
        &self,
        remote: Option<documents::Operation>,
        receipt: documents::OperationReceipt,
    ) -> MirrorOperation {
        let id = receipt.id;
        let pending = matches!(receipt.status, documents::OperationStatus::LocallyApplied);
        let operation = MirrorOperation {
            state: Rc::new(OperationState {
                receipt: RefCell::new(receipt),
                remote: RefCell::new(remote),
            }),
        };
        if pending {
            assert!(
                self.state
                    .borrow_mut()
                    .operations
                    .insert(id, operation.clone())
                    .is_none(),
                "document operation identity reused"
            );
        } else {
            operation.state.remote.borrow_mut().take();
        }
        operation
    }

    pub fn changed(&self, snapshot: documents::DocumentSnapshot) -> bool {
        self.watch(snapshot.watch_id)
            .is_some_and(|watch| watch.apply(snapshot, Delivery::Change))
    }

    pub fn lagged(&self, watch_id: u64) -> Option<MirrorWatch> {
        let watch = self.watch(watch_id)?;
        watch.state.snapshot.borrow_mut().status = MirrorStatus::Lagged;
        Some(watch)
    }

    pub fn completed(&self, receipt: documents::OperationReceipt) -> bool {
        let operation = self.state.borrow().operations.get(&receipt.id).cloned();
        let Some(operation) = operation else {
            return false;
        };
        if receipt.revision <= operation.state.receipt.borrow().revision {
            return false;
        }
        let terminal = !matches!(receipt.status, documents::OperationStatus::LocallyApplied);
        let id = receipt.id;
        *operation.state.receipt.borrow_mut() = receipt;
        if terminal {
            self.state.borrow_mut().operations.remove(&id);
            operation.state.remote.borrow_mut().take();
        }
        true
    }
}

/// One named, already-authorized document binding handed to a micro at activation. The host
/// resolved the owner-config document and granted exactly this facet access; the binding is
/// authority, not a lookup result.
pub struct ConfigDocument {
    binding: documents::DocumentBinding,
    mirror: DocumentMirror,
}

impl ConfigDocument {
    pub fn name(&self) -> &str {
        &self.binding.name
    }

    pub fn watch(&self, tag: &str, key_id: &str) -> Result<MirrorWatch, documents::DocumentError> {
        let watch = self.binding.access.watch(&documents::WatchSelection {
            documents: vec![self.binding.document.clone()],
            branch: self.binding.branch.clone(),
            facets: documents::FacetSelection::Keys(vec![documents::FacetKey {
                tag: tag.into(),
                key_id: key_id.into(),
            }]),
        })?;
        let snapshot = watch.current()?;
        Ok(self.mirror.retain(
            Some(watch),
            documents::FacetRoute {
                document: self.binding.document.clone(),
                branch: self.binding.branch.clone(),
                key: documents::FacetKey {
                    tag: tag.into(),
                    key_id: key_id.into(),
                },
            },
            snapshot,
        ))
    }

    pub fn write<T: serde::Serialize>(
        &self,
        tag: &str,
        key_id: &str,
        value: &T,
    ) -> Result<MirrorOperation, documents::DocumentError> {
        let operation = self.binding.access.mutate(
            &documents::FacetRoute {
                document: self.binding.document.clone(),
                branch: self.binding.branch.clone(),
                key: documents::FacetKey {
                    tag: tag.into(),
                    key_id: key_id.into(),
                },
            },
            &documents::FacetMutation::Update(
                serde_json::to_string(value).expect("facet value must serialize to JSON"),
            ),
        )?;
        let receipt = operation.current()?;
        Ok(self.mirror.track(Some(operation), receipt))
    }
}

#[derive(Clone)]
pub struct MirrorOperation {
    state: Rc<OperationState>,
}

struct OperationState {
    receipt: RefCell<documents::OperationReceipt>,
    remote: RefCell<Option<documents::Operation>>,
}

impl MirrorOperation {
    pub fn current(&self) -> documents::OperationReceipt {
        self.state.receipt.borrow().clone()
    }
}

#[derive(Clone)]
pub struct MirrorWatch {
    state: Rc<WatchState>,
}

struct WatchState {
    id: u64,
    route: documents::FacetRoute,
    snapshot: RefCell<MirrorSnapshot>,
    remote: RefCell<Option<documents::DocumentWatch>>,
    mirror: Weak<RefCell<MirrorState>>,
}

impl Drop for WatchState {
    fn drop(&mut self) {
        if let Some(mirror) = self.mirror.upgrade() {
            mirror.borrow_mut().watches.remove(&self.id);
        }
    }
}

#[derive(Clone, Copy)]
enum Delivery {
    Change,
    Reset,
}

impl MirrorWatch {
    pub fn current(&self) -> MirrorSnapshot {
        self.state.snapshot.borrow().clone()
    }

    pub fn release(&self) {
        if self.current().status != MirrorStatus::Revoked {
            self.withdraw(MirrorStatus::Released);
        }
    }

    fn detach(&self) {
        if let Some(mirror) = self.state.mirror.upgrade() {
            mirror.borrow_mut().watches.remove(&self.state.id);
        }
        self.state.remote.borrow_mut().take();
    }

    fn withdraw(&self, status: MirrorStatus) {
        {
            let mut snapshot = self.state.snapshot.borrow_mut();
            snapshot.status = status;
            snapshot.facets.clear();
            snapshot.pending.clear();
            snapshot.refreshing = false;
        }
        self.detach();
    }

    fn apply(&self, incoming: documents::DocumentSnapshot, delivery: Delivery) -> bool {
        let revoked;
        let changed;
        {
            let mut current = self.state.snapshot.borrow_mut();
            if matches!(
                current.status,
                MirrorStatus::Released | MirrorStatus::Revoked
            ) || incoming.revision < current.revision
                || (matches!(delivery, Delivery::Change) && incoming.revision == current.revision)
            {
                return false;
            }
            assert_eq!(
                incoming.watch_id, self.state.id,
                "wrong document watch reset"
            );
            let mut next = mirror_snapshot(incoming, &self.state.route);
            revoked = next.status == MirrorStatus::Revoked;
            if !revoked
                && current.status == MirrorStatus::Lagged
                && matches!(delivery, Delivery::Change)
            {
                next.status = MirrorStatus::Lagged;
            }
            changed = *current != next;
            *current = next;
        }
        if revoked {
            self.detach();
        }
        changed
    }

    fn resync_with(
        &self,
        read: impl FnOnce() -> Result<documents::DocumentSnapshot, documents::DocumentError>,
    ) -> Result<bool, documents::DocumentError> {
        if matches!(
            self.state.snapshot.borrow().status,
            MirrorStatus::Released | MirrorStatus::Revoked
        ) {
            return Err(documents::DocumentError::Revoked);
        }
        let incoming = match read() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                match &error {
                    documents::DocumentError::Denied | documents::DocumentError::Revoked => {
                        self.withdraw(MirrorStatus::Revoked);
                    }
                    documents::DocumentError::SubscriptionLagged => {
                        self.state.snapshot.borrow_mut().status = MirrorStatus::Lagged;
                    }
                    documents::DocumentError::InvalidValue(_) => {}
                }
                return Err(error);
            }
        };
        if incoming.revision < self.state.snapshot.borrow().revision {
            self.state.snapshot.borrow_mut().status = MirrorStatus::Lagged;
            return Err(documents::DocumentError::SubscriptionLagged);
        }
        Ok(self.apply(incoming, Delivery::Reset))
    }

    pub fn resync(&self) -> Result<bool, documents::DocumentError> {
        self.resync_with(|| {
            self.state
                .remote
                .borrow()
                .as_ref()
                .expect("live document watch must retain its host resource")
                .current()
        })
    }

    pub fn refresh(&self) -> Result<(), documents::DocumentError> {
        self.resync_with(|| {
            let remote = self.state.remote.borrow();
            let remote = remote
                .as_ref()
                .expect("live document watch must retain its host resource");
            remote.refresh()?;
            remote.current()
        })?;
        Ok(())
    }
}

fn mirror_snapshot(
    snapshot: documents::DocumentSnapshot,
    route: &documents::FacetRoute,
) -> MirrorSnapshot {
    let mut facets = BTreeMap::new();
    if !snapshot.revoked {
        let key = (route.key.tag.clone(), route.key.key_id.clone());
        facets.insert(key, FacetState::Loading);
        for entry in snapshot.facets {
            assert!(
                entry.route.document == route.document
                    && entry.route.branch == route.branch
                    && entry.route.key.tag == route.key.tag
                    && entry.route.key.key_id == route.key.key_id,
                "document snapshot escaped its granted selection"
            );
            let state = match entry.value {
                documents::FacetValue::Loading => FacetState::Loading,
                documents::FacetValue::Absent => FacetState::Absent,
                documents::FacetValue::Present(raw) => FacetState::Present(raw),
            };
            facets.insert((entry.route.key.tag, entry.route.key.key_id), state);
        }
    }
    MirrorSnapshot {
        revision: snapshot.revision,
        facets,
        pending: if snapshot.revoked {
            vec![]
        } else {
            snapshot.pending
        },
        refreshing: !snapshot.revoked && snapshot.refreshing,
        status: if snapshot.revoked {
            MirrorStatus::Revoked
        } else {
            MirrorStatus::Live
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn route() -> documents::FacetRoute {
        documents::FacetRoute {
            document: "config".into(),
            branch: "main".into(),
            key: documents::FacetKey {
                tag: "counter".into(),
                key_id: "count".into(),
            },
        }
    }

    fn snapshot(
        watch_id: u64,
        revision: u64,
        value: documents::FacetValue,
    ) -> documents::DocumentSnapshot {
        documents::DocumentSnapshot {
            watch_id,
            revision,
            facets: vec![documents::FacetEntry {
                route: route(),
                value,
            }],
            pending: vec![],
            refreshing: false,
            revoked: false,
        }
    }

    fn retain(mirror: &DocumentMirror, id: u64) -> MirrorWatch {
        mirror.retain(
            None,
            route(),
            snapshot(id, 1, documents::FacetValue::Loading),
        )
    }

    fn receipt(
        id: u64,
        revision: u64,
        status: documents::OperationStatus,
    ) -> documents::OperationReceipt {
        documents::OperationReceipt {
            id,
            revision,
            status,
        }
    }

    #[test]
    fn reads_are_local_and_preserve_availability_freshness_and_pending() {
        let mirror = DocumentMirror::default();
        let watch = retain(&mirror, 1);
        for _ in 0..10 {
            assert_eq!(
                watch.current().value::<u64>("counter", "count").unwrap(),
                FacetState::Loading
            );
        }
        assert!(mirror.changed(snapshot(1, 2, documents::FacetValue::Absent)));
        assert_eq!(
            watch.current().value::<u64>("counter", "count").unwrap(),
            FacetState::Absent
        );
        let mut incoming = snapshot(1, 3, documents::FacetValue::Present("5".into()));
        incoming.pending = vec![42];
        incoming.refreshing = true;
        assert!(mirror.changed(incoming));
        assert_eq!(
            watch.current(),
            MirrorSnapshot {
                revision: 3,
                facets: [(
                    ("counter".into(), "count".into()),
                    FacetState::Present("5".into())
                )]
                .into(),
                pending: vec![42],
                refreshing: true,
                status: MirrorStatus::Live,
            }
        );
        assert_eq!(
            watch.current().value::<u64>("counter", "count").unwrap(),
            FacetState::Present(5)
        );
        assert!(matches!(
            watch.current().value::<u64>("other", "count"),
            Err(documents::DocumentError::Denied)
        ));
    }

    #[test]
    fn routing_is_monotonic_and_isolated_between_watches() {
        let mirror = DocumentMirror::default();
        let first = retain(&mirror, 1);
        let second = retain(&mirror, 2);
        let second_initial = second.current();
        assert!(mirror.changed(snapshot(1, 3, documents::FacetValue::Present("8".into()))));
        let current = first.current();
        for (id, revision) in [(1, 1), (1, 3), (99, 9)] {
            assert!(!mirror.changed(snapshot(id, revision, documents::FacetValue::Absent)));
        }
        assert_eq!(first.current(), current);
        assert_eq!(second.current(), second_initial);
        assert!(mirror.lagged(99).is_none());
    }

    #[test]
    fn lag_requires_explicit_reset_and_never_fetches_during_reads() {
        let mirror = DocumentMirror::default();
        let watch = retain(&mirror, 1);
        mirror.changed(snapshot(1, 3, documents::FacetValue::Present("8".into())));
        let lagged = mirror.lagged(1).unwrap();
        for _ in 0..10 {
            assert_eq!(watch.current().status, MirrorStatus::Lagged);
            assert_eq!(
                watch.current().value::<u64>("counter", "count").unwrap(),
                FacetState::Present(8)
            );
        }
        assert!(mirror.changed(snapshot(1, 4, documents::FacetValue::Present("9".into()))));
        assert_eq!(watch.current().status, MirrorStatus::Lagged);
        let reads = Cell::new(0);
        assert!(matches!(
            lagged.resync_with(|| {
                reads.set(reads.get() + 1);
                Ok(snapshot(1, 3, documents::FacetValue::Absent))
            }),
            Err(documents::DocumentError::SubscriptionLagged)
        ));
        assert_eq!(watch.current().status, MirrorStatus::Lagged);
        assert!(
            lagged
                .resync_with(|| {
                    reads.set(reads.get() + 1);
                    Ok(snapshot(1, 4, documents::FacetValue::Present("9".into())))
                })
                .unwrap()
        );
        assert_eq!(reads.get(), 2);
        assert_eq!(watch.current().status, MirrorStatus::Live);
        assert_eq!(
            watch.current().value::<u64>("counter", "count").unwrap(),
            FacetState::Present(9)
        );
    }

    #[test]
    fn release_and_revocation_are_terminal_for_every_clone() {
        for terminal in [MirrorStatus::Released, MirrorStatus::Revoked] {
            let mirror = DocumentMirror::default();
            let watch = retain(&mirror, 1);
            let clone = watch.clone();
            mirror.changed(snapshot(1, 2, documents::FacetValue::Present("7".into())));
            match terminal {
                MirrorStatus::Released => watch.release(),
                MirrorStatus::Revoked => {
                    let mut revoked = snapshot(1, 3, documents::FacetValue::Present("99".into()));
                    revoked.revoked = true;
                    revoked.pending = vec![4];
                    revoked.refreshing = true;
                    assert!(mirror.changed(revoked));
                }
                MirrorStatus::Live | MirrorStatus::Lagged => unreachable!(),
            }
            assert_eq!(
                clone.current(),
                MirrorSnapshot {
                    revision: if terminal == MirrorStatus::Released {
                        2
                    } else {
                        3
                    },
                    facets: BTreeMap::new(),
                    pending: vec![],
                    refreshing: false,
                    status: terminal,
                }
            );
            assert!(!mirror.changed(snapshot(1, 9, documents::FacetValue::Present("99".into()))));
            assert!(mirror.lagged(1).is_none());
            assert!(matches!(
                clone.resync_with(|| panic!("released watch performed IO")),
                Err(documents::DocumentError::Revoked)
            ));
            assert!(matches!(
                clone.current().value::<u64>("counter", "count"),
                Err(documents::DocumentError::Revoked)
            ));
            clone.release();
            assert_eq!(clone.current().status, terminal);
        }
    }

    #[test]
    fn resync_authority_failure_withdraws_local_values() {
        let mirror = DocumentMirror::default();
        let watch = retain(&mirror, 1);
        mirror.changed(snapshot(1, 2, documents::FacetValue::Present("7".into())));
        assert!(matches!(
            watch.resync_with(|| Err(documents::DocumentError::Revoked)),
            Err(documents::DocumentError::Revoked)
        ));
        assert_eq!(watch.current().status, MirrorStatus::Revoked);
        assert!(watch.current().facets.is_empty());
        assert!(!mirror.changed(snapshot(1, 3, documents::FacetValue::Present("8".into()))));
    }

    #[test]
    fn last_watch_drop_removes_routing_but_not_pending_operations() {
        let mirror = DocumentMirror::default();
        let watch = retain(&mirror, 1);
        let clone = watch.clone();
        let operation = mirror.track(
            None,
            receipt(42, 0, documents::OperationStatus::LocallyApplied),
        );
        drop(watch);
        assert!(mirror.changed(snapshot(1, 2, documents::FacetValue::Present("1".into()))));
        drop(clone);
        assert!(mirror.state.borrow().watches.is_empty());
        assert!(!mirror.changed(snapshot(1, 3, documents::FacetValue::Absent)));
        assert!(!mirror.completed(receipt(42, 0, documents::OperationStatus::Accepted)));
        assert!(mirror.completed(receipt(42, 1, documents::OperationStatus::Accepted)));
        assert!(matches!(
            operation.current().status,
            documents::OperationStatus::Accepted
        ));
        assert_eq!(operation.current().revision, 1);
        assert!(mirror.state.borrow().operations.is_empty());
        assert!(!mirror.completed(receipt(42, 2, documents::OperationStatus::LocallyApplied)));
        assert!(matches!(
            operation.current().status,
            documents::OperationStatus::Accepted
        ));
    }

    #[test]
    fn rollback_details_survive_explicit_watch_release() {
        let mirror = DocumentMirror::default();
        let watch = retain(&mirror, 1);
        let operation = mirror.track(
            None,
            receipt(42, 0, documents::OperationStatus::LocallyApplied),
        );
        watch.release();
        assert!(mirror.completed(receipt(
            42,
            1,
            documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(
                documents::TransportFailure {
                    message: "disconnected".into(),
                    outcome_unknown: true,
                },
            )),
        )));
        let current = operation.current();
        assert_eq!((current.id, current.revision), (42, 1));
        let documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(failure)) =
            current.status
        else {
            panic!("transport receipt lost its failure details");
        };
        assert_eq!(
            (failure.message.as_str(), failure.outcome_unknown),
            ("disconnected", true)
        );
        assert_eq!(watch.current().status, MirrorStatus::Released);
        assert!(mirror.state.borrow().operations.is_empty());
    }

    #[test]
    fn dropping_ui_operation_handles_does_not_discard_completion_tracking() {
        let mirror = DocumentMirror::default();
        drop(mirror.track(
            None,
            receipt(42, 0, documents::OperationStatus::LocallyApplied),
        ));
        assert_eq!(mirror.state.borrow().operations.len(), 1);
        assert!(mirror.completed(receipt(
            42,
            1,
            documents::OperationStatus::RolledBack(documents::OperationFailure::Transport(
                documents::TransportFailure {
                    message: "disconnected".into(),
                    outcome_unknown: true,
                }
            ),)
        )));
        assert!(mirror.state.borrow().operations.is_empty());
    }
}
