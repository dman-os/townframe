//! The MDK's typed document-mirror authoring surface (ADR 015 §5, §4). This adds ergonomics —
//! typed decoding, named bindings, observable values, and a receipt vocabulary that does not
//! require callers to navigate wire enums — without adding authority: the underlying
//! `documents::DocumentAccess` resource is still the only capability, and filtering still
//! happens host-side before data reaches this mirror.
//!
//! Reads never perform IO: a watch answers from its retained snapshot, and the host's
//! `document-events` callbacks (routed through [`DocumentMirror::changed`] and friends) are the
//! only thing that moves it forward. Every such move runs the mirror's one change notifier, so a
//! presentation layer can observe "the mirror changed" without each watch knowing about it.
use crate::documents;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::rc::Weak;

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
    notifier: Option<Rc<dyn Fn()>>,
}

impl MirrorState {
    fn notify(&self) {
        if let Some(notifier) = &self.notifier {
            notifier();
        }
    }
}

impl DocumentMirror {
    /// Install the one observer this mirror runs after every state change it applies —
    /// delivered snapshots, lag marks, completions, resyncs, releases. The guest runtime points
    /// it at its presentation so hooks re-render; a mirror without one is silent.
    pub fn set_notifier(&self, notifier: impl Fn() + 'static) {
        assert!(
            self.state
                .borrow_mut()
                .notifier
                .replace(Rc::new(notifier))
                .is_none(),
            "document mirror notifier installed twice"
        );
    }

    fn notify(&self) {
        self.state.borrow().notify();
    }

    pub fn bind(&self, binding: documents::DocumentBinding) -> ConfigDocument {
        ConfigDocument {
            binding: Rc::new(binding),
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
        let changed = self
            .watch(snapshot.watch_id)
            .is_some_and(|watch| watch.apply(snapshot, Delivery::Change));
        if changed {
            self.notify();
        }
        changed
    }

    pub fn lagged(&self, watch_id: u64) -> Option<MirrorWatch> {
        let watch = self.watch(watch_id)?;
        watch.state.snapshot.borrow_mut().status = MirrorStatus::Lagged;
        self.notify();
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
        self.notify();
        true
    }
}

/// One named, already-authorized document binding handed to a micro at activation. The host
/// resolved the owner-config document and granted exactly this facet access; the binding is
/// authority, not a lookup result. Clones share the binding's `document-access` resource.
#[derive(Clone)]
pub struct ConfigDocument {
    binding: Rc<documents::DocumentBinding>,
    mirror: DocumentMirror,
}

impl ConfigDocument {
    pub fn name(&self) -> &str {
        &self.binding.name
    }

    /// A typed handle on one facet of this document: a retained watch plus the write route, the
    /// guest-side twin of the native `DocumentRef::facet`. Fails only if the host refuses the
    /// watch, i.e. the manifest did not grant this facet.
    pub fn facet<T>(&self, tag: &str, key_id: &str) -> Result<Facet<T>, documents::DocumentError> {
        let watch = self.watch(tag, key_id)?;
        Ok(Facet {
            document: self.clone(),
            key: documents::FacetKey {
                tag: tag.into(),
                key_id: key_id.into(),
            },
            watch,
            _value: PhantomData,
        })
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

/// A typed handle on one granted facet: reads answer from the retained mirror, writes go
/// through the binding's `document-access`. Clones share one watch, so equality is watch
/// identity — what a Dioxus prop needs to know.
pub struct Facet<T> {
    document: ConfigDocument,
    key: documents::FacetKey,
    watch: MirrorWatch,
    _value: PhantomData<fn() -> T>,
}

impl<T> Clone for Facet<T> {
    fn clone(&self) -> Self {
        Self {
            document: self.document.clone(),
            key: self.key.clone(),
            watch: self.watch.clone(),
            _value: PhantomData,
        }
    }
}

impl<T> PartialEq for Facet<T> {
    fn eq(&self, other: &Self) -> bool {
        self.watch == other.watch
    }
}

impl<T> Facet<T> {
    pub fn watch(&self) -> &MirrorWatch {
        &self.watch
    }

    pub fn current(&self) -> MirrorSnapshot {
        self.watch.current()
    }

    /// Re-read the host's current snapshot into the mirror (after a write, or when lagged).
    pub fn resync(&self) -> Result<bool, documents::DocumentError> {
        self.watch.resync()
    }

    /// Ask the host to refresh the document from its source, then re-read it.
    pub fn refresh(&self) -> Result<(), documents::DocumentError> {
        self.watch.refresh()
    }

    /// The facet's typed value in `snapshot`. A released or revoked watch reads as
    /// [`FacetState::Loading`]: the value is gone, and the snapshot's status says why.
    pub fn state(&self, snapshot: &MirrorSnapshot) -> FacetState<T>
    where
        T: serde::de::DeserializeOwned,
    {
        match snapshot.status {
            MirrorStatus::Released | MirrorStatus::Revoked => FacetState::Loading,
            MirrorStatus::Live | MirrorStatus::Lagged => snapshot
                .value(&self.key.tag, &self.key.key_id)
                .expect("facet watch must contain its own granted facet"),
        }
    }

    pub fn write(&self, value: &T) -> Result<MirrorOperation, documents::DocumentError>
    where
        T: serde::Serialize,
    {
        self.document.write(&self.key.tag, &self.key.key_id, value)
    }
}

#[derive(Clone)]
pub struct MirrorOperation {
    state: Rc<OperationState>,
}

impl PartialEq for MirrorOperation {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
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

impl PartialEq for MirrorWatch {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
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
        // Only a live watch has anything to withdraw; releasing a terminal watch is a silent
        // no-op so the notifier contract stays exactly-once.
        if matches!(
            self.current().status,
            MirrorStatus::Live | MirrorStatus::Lagged
        ) {
            self.withdraw(MirrorStatus::Released);
            self.notify();
        }
    }

    fn notify(&self) {
        if let Some(mirror) = self.state.mirror.upgrade() {
            mirror.borrow().notify();
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

    /// Pull a fresh snapshot through `read` and apply it as a reset. Every outcome is reflected
    /// in the watch — a newer snapshot replaces the mirror, an older one marks it lagged, an
    /// authority failure withdraws it — so the notifier runs whichever way it went.
    fn resync_with(
        &self,
        read: impl FnOnce() -> Result<documents::DocumentSnapshot, documents::DocumentError>,
    ) -> Result<bool, documents::DocumentError> {
        let result = self.reset_from(read);
        self.notify();
        result
    }

    fn reset_from(
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
mod tests;
