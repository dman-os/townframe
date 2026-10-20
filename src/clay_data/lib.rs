//! The document data layer (ADR 015 §4). Three layers with different runtime shapes:
//! [`DocumentService`] is the shared host service (sparse cache, refresh deduplication,
//! retention, optimistic transactions, permissioned delivery); [`DocumentAccess`] is a
//! context's authorized mirror entry point; `Facet<T>` is the typed authoring surface.
//! Contribution collections (Settings sections, root mounts) are a deliberately separate
//! mechanism in `clay_host`: they share the reactive-read shape but have registration and
//! disposal semantics instead of optimistic persistence or source cursors.
//!
//! Source cursors and cache policy remain host-owned. Native wrappers use these data values
//! and handles; the WIT resources in `clay_wit` map the same watch/selection/mutation
//! protocol without transporting callbacks or renderer state.
mod collection;

use clay_host::{AccessError, Host, Instance};
use collection::{Collection, CollectionChange};
use daybook_types::manifest::RoutineFacetAccess;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::watch;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FacetRoute {
    pub document: String,
    pub branch: String,
    pub tag: String,
    pub key_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FacetSelection {
    Tags(BTreeSet<String>),
    Keys(BTreeSet<(String, String)>),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchSelection {
    pub documents: BTreeSet<String>,
    pub branch: String,
    pub facets: FacetSelection,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FacetValue {
    /// Not yet hydrated locally. Distinct from `Absent` so an uncached facet is never mistaken
    /// for a nonexistent one (transcript:322).
    Loading,
    /// Known absent after a successful read.
    Absent,
    Present(Value),
}
/// Freshness evidence for a facet value (ADR 014 §4). A pending refresh never hides an existing
/// local value; `Refreshing` is evidence, not a loading state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Freshness {
    /// Served from the retained local projection with no refresh in flight.
    Cached,
    /// A targeted refresh for this watch's current selection is in flight.
    Refreshing,
    /// The projection matches the last authoritative read.
    Current,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentSnapshot {
    pub revision: u64,
    pub facets: BTreeMap<FacetRoute, FacetValue>,
    pub pending: BTreeSet<u64>,
    pub freshness: Freshness,
    pub revoked: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FacetMutation {
    Write(Value),
    Create(Value),
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum OperationError {
    #[error("operation rejected: {message}")]
    Rejected { message: String },
    #[error("transport failure (outcome unknown: {outcome_unknown}): {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationStatus {
    LocallyApplied,
    Accepted,
    RolledBack(OperationError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationReceipt {
    pub id: u64,
    pub revision: u64,
    pub status: OperationStatus,
}

#[derive(Clone)]
pub struct Operation {
    service: DocumentService,
    updates: watch::Receiver<OperationReceipt>,
}

impl std::fmt::Debug for Operation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("Operation")
            .field(&self.current())
            .finish()
    }
}

impl Operation {
    pub fn id(&self) -> u64 {
        self.updates.borrow().id
    }

    pub fn current(&self) -> OperationReceipt {
        let _state = self.service.inner.lock().unwrap();
        self.updates.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<OperationReceipt> {
        self.updates.clone()
    }

    pub async fn next(
        &self,
        after_revision: u64,
    ) -> Result<Option<OperationReceipt>, DocumentError> {
        let mut updates = self.subscribe();
        loop {
            let receipt = {
                let _state = self.service.inner.lock().unwrap();
                updates.borrow_and_update().clone()
            };
            if after_revision > receipt.revision {
                return Err(DocumentError::SubscriptionLagged);
            }
            if receipt.revision > after_revision {
                return Ok(Some(receipt));
            }
            if receipt.status != OperationStatus::LocallyApplied {
                return Ok(None);
            }
            updates
                .changed()
                .await
                .expect("pending operation lost its completion owner");
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DocumentError {
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error("subscription continuation is invalid; reread the current snapshot")]
    SubscriptionLagged,
}

/// Resolved, host-approved document grants. Owner configuration resolution precedes issuance;
/// normal document predicate evaluation will likewise precede issuance in the core adapter.
pub struct DocumentGrant {
    pub document: String,
    pub branch: String,
    pub facets: Vec<RoutineFacetAccess>,
}
struct Authority {
    documents: BTreeMap<(String, String), Vec<RoutineFacetAccess>>,
    revoked: AtomicBool,
}
#[derive(Clone)]
pub struct DocumentAccess {
    service: DocumentService,
    instance: Instance,
    authority: Arc<Authority>,
}
#[derive(Clone)]
pub struct DocumentWatch {
    lease: Arc<WatchLease>,
}
struct WatchLease {
    service: DocumentService,
    id: u64,
    access: DocumentAccess,
}

#[derive(Clone)]
pub struct DocumentService {
    inner: Arc<Mutex<State>>,
}
struct State {
    next: u64,
    clock: u64,
    capacity: usize,
    source: BTreeMap<FacetRoute, Value>,
    // Mock FacetSet lookup: selected document/tag routes, including removal tombstones.
    membership: BTreeMap<(String, String, String), BTreeSet<String>>,
    config_owners: BTreeMap<String, (String, String)>,
    cache: BTreeMap<FacetRoute, CacheEntry>,
    projection: Collection<FacetRoute, Value>,
    watches: BTreeMap<u64, WatchRecord>,
    pending: BTreeMap<u64, PendingMutation>,
    reads: Vec<FacetRoute>,
    hold_refresh: bool,
    requests: BTreeMap<FacetRoute, BTreeMap<u64, u64>>,
}
struct CacheEntry {
    touched: u64,
}
struct WatchRecord {
    authority: Arc<Authority>,
    selection: WatchSelection,
    generation: u64,
    minimum_revision: u64,
    updates: watch::Sender<DocumentSnapshot>,
}
struct PendingMutation {
    route: FacetRoute,
    mutation: FacetMutation,
    updates: watch::Sender<OperationReceipt>,
}

/// Privileged mock controls are deliberately absent from MicroContext and DocumentAccess.
#[derive(Clone)]
pub struct MockDocuments {
    service: DocumentService,
}

impl DocumentService {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                next: 0,
                clock: 0,
                capacity,
                source: BTreeMap::new(),
                membership: BTreeMap::new(),
                config_owners: BTreeMap::new(),
                cache: BTreeMap::new(),
                projection: Collection::new(),
                watches: BTreeMap::new(),
                pending: BTreeMap::new(),
                reads: Vec::new(),
                hold_refresh: false,
                requests: BTreeMap::new(),
            })),
        }
    }
}
impl DocumentService {
    /// Uses the manifest's owner-config semantics. The composition root supplies already-approved
    /// ACLs; dependency/tag validation remains the loader's responsibility, not a second ACL model.
    pub fn grant_config(
        &self,
        host: &Host,
        instance: &Instance,
        invoking_plug: &str,
        acl: Vec<RoutineFacetAccess>,
    ) -> Result<BTreeMap<String, DocumentRef>, AccessError> {
        host.check(instance)?;
        let mut owners: BTreeMap<String, Vec<RoutineFacetAccess>> = BTreeMap::new();
        for access in acl {
            let owner = access
                .owner_plug_id
                .as_deref()
                .unwrap_or(invoking_plug)
                .to_owned();
            owners.entry(owner).or_default().push(access);
        }
        let mut result = BTreeMap::new();
        for (owner, facets) in owners {
            let (document, branch) = self
                .inner
                .lock()
                .unwrap()
                .config_owners
                .get(&owner)
                .cloned()
                .expect("approved config owner has no document binding");
            let access = self.grant_documents(
                host,
                instance,
                vec![DocumentGrant {
                    document: document.clone(),
                    branch: branch.clone(),
                    facets,
                }],
            )?;
            result.insert(owner, access.document(document, branch)?);
        }
        Ok(result)
    }

    pub fn mock_documents(&self) -> MockDocuments {
        MockDocuments {
            service: self.clone(),
        }
    }
    pub fn grant_documents(
        &self,
        host: &Host,
        instance: &Instance,
        grants: Vec<DocumentGrant>,
    ) -> Result<DocumentAccess, AccessError> {
        host.check(instance)?;
        let mut documents: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for grant in grants {
            documents
                .entry((grant.document, grant.branch))
                .or_default()
                .extend(grant.facets);
        }
        let authority = Arc::new(Authority {
            documents,
            revoked: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&self.inner);
        let cleanup = authority.clone();
        host.own(
            instance,
            Box::new(move || {
                cleanup.revoked.store(true, Ordering::Release);
                if let Some(service) = weak.upgrade() {
                    let mut state = service.lock().unwrap();
                    let ids = state
                        .watches
                        .iter()
                        .filter(|(_, record)| Arc::ptr_eq(&record.authority, &cleanup))
                        .map(|(id, _)| *id)
                        .collect::<Vec<_>>();
                    for id in ids {
                        state.release_watch(id, /*revoked*/ true);
                    }
                }
            }),
        )?;
        Ok(DocumentAccess {
            service: self.clone(),
            instance: instance.clone(),
            authority,
        })
    }
}
impl DocumentAccess {
    fn check(&self) -> Result<(), AccessError> {
        self.instance.check()?;
        if self.authority.revoked.load(Ordering::Acquire) {
            Err(AccessError::Revoked)
        } else {
            Ok(())
        }
    }

    pub fn watch(&self, selection: WatchSelection) -> Result<DocumentWatch, AccessError> {
        self.check()?;
        self.authority.validate(&selection)?;
        let mut state = self.service.inner.lock().unwrap();
        self.check()?;
        state.next += 1;
        let id = state.next;
        let (updates, _) = watch::channel(DocumentSnapshot {
            revision: 0,
            facets: BTreeMap::new(),
            pending: BTreeSet::new(),
            freshness: Freshness::Cached,
            revoked: false,
        });
        assert!(
            state
                .watches
                .insert(
                    id,
                    WatchRecord {
                        authority: self.authority.clone(),
                        selection,
                        generation: 0,
                        minimum_revision: 0,
                        updates
                    }
                )
                .is_none()
        );
        state.refresh_watch(id);
        Ok(DocumentWatch {
            lease: Arc::new(WatchLease {
                service: self.service.clone(),
                id,
                access: self.clone(),
            }),
        })
    }

    pub fn mutate(
        &self,
        route: FacetRoute,
        mutation: FacetMutation,
    ) -> Result<Operation, AccessError> {
        self.check()?;
        let right = match &mutation {
            FacetMutation::Write(_) => Right::Write,
            FacetMutation::Create(_) => Right::Create,
            FacetMutation::Delete => Right::Delete,
        };
        if !self.authority.allows(&route, right) {
            return Err(AccessError::Denied);
        }
        let mut state = self.service.inner.lock().unwrap();
        self.check()?;
        // Seed this route's base before overlaying a write. Never hydrate unrelated facets.
        if !state.cache.contains_key(&route) {
            state.hydrate(&route);
        }
        state.next += 1;
        let id = state.next;
        let change = match &mutation {
            FacetMutation::Write(value) | FacetMutation::Create(value) => {
                CollectionChange::Upsert {
                    key: route.clone(),
                    value: value.clone(),
                }
            }
            FacetMutation::Delete => CollectionChange::Remove { key: route.clone() },
        };
        state.projection.apply_optimistic(id, change);
        state.note_route(&route);
        let (updates, receipt) = watch::channel(OperationReceipt {
            id,
            revision: 0,
            status: OperationStatus::LocallyApplied,
        });
        assert!(
            state
                .pending
                .insert(
                    id,
                    PendingMutation {
                        route,
                        mutation,
                        updates
                    }
                )
                .is_none()
        );
        state.publish();
        Ok(Operation {
            service: self.service.clone(),
            updates: receipt,
        })
    }
}

/// A document binding can be delivered through another granted service; knowing an ID alone
/// does not construct it. This convenience uses the same multi-document watch protocol.
#[derive(Clone)]
pub struct DocumentRef {
    access: DocumentAccess,
    document: String,
    branch: String,
}
#[derive(Clone)]
pub struct Facet<T> {
    pub watch: DocumentWatch,
    access: DocumentAccess,
    route: FacetRoute,
    marker: std::marker::PhantomData<T>,
}
impl<T> PartialEq for Facet<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.watch.lease, &other.watch.lease)
    }
}
impl DocumentAccess {
    pub fn document(&self, document: String, branch: String) -> Result<DocumentRef, AccessError> {
        self.check()?;
        if !self
            .authority
            .documents
            .contains_key(&(document.clone(), branch.clone()))
        {
            return Err(AccessError::Denied);
        }
        Ok(DocumentRef {
            access: self.clone(),
            document,
            branch,
        })
    }
}
impl DocumentRef {
    pub fn document_id(&self) -> &str {
        &self.document
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn access(&self) -> DocumentAccess {
        self.access.clone()
    }

    pub fn mutate(
        &self,
        tag: String,
        key_id: String,
        mutation: FacetMutation,
    ) -> Result<Operation, AccessError> {
        self.access.mutate(
            FacetRoute {
                document: self.document.clone(),
                branch: self.branch.clone(),
                tag,
                key_id,
            },
            mutation,
        )
    }
    pub fn facet<T: serde::de::DeserializeOwned + serde::Serialize>(
        &self,
        tag: String,
        key_id: String,
    ) -> Result<Facet<T>, AccessError> {
        let route = FacetRoute {
            document: self.document.clone(),
            branch: self.branch.clone(),
            tag,
            key_id,
        };
        let watch = self.access.watch(WatchSelection {
            documents: [route.document.clone()].into(),
            branch: route.branch.clone(),
            facets: FacetSelection::Keys([(route.tag.clone(), route.key_id.clone())].into()),
        })?;
        Ok(Facet {
            watch,
            access: self.access.clone(),
            route,
            marker: std::marker::PhantomData,
        })
    }
}
impl<T: serde::de::DeserializeOwned + serde::Serialize> Facet<T> {
    pub fn value(&self, snapshot: &DocumentSnapshot) -> Option<T> {
        match snapshot.facets.get(&self.route) {
            Some(FacetValue::Present(value)) => Some(
                serde_json::from_value(value.clone())
                    .expect("configuration facet violates its declared value shape"),
            ),
            Some(FacetValue::Loading | FacetValue::Absent) | None => None,
        }
    }
    pub fn write(&self, value: T) -> Result<Operation, AccessError> {
        self.access.mutate(
            self.route.clone(),
            FacetMutation::Write(
                serde_json::to_value(value).expect("facet value must serialize to JSON"),
            ),
        )
    }
}
impl DocumentWatch {
    pub fn current(&self) -> Result<DocumentSnapshot, AccessError> {
        self.lease.access.check()?;
        let state = self.lease.service.inner.lock().unwrap();
        self.lease.access.check()?;
        let record = state
            .watches
            .get(&self.lease.id)
            .ok_or(AccessError::Revoked)?;
        Ok(record.updates.borrow().clone())
    }
    pub async fn next(&self, after_revision: u64) -> Result<DocumentSnapshot, DocumentError> {
        let mut updates = self.subscribe()?;
        loop {
            {
                let state = self.lease.service.inner.lock().unwrap();
                self.lease.access.check()?;
                let record = state
                    .watches
                    .get(&self.lease.id)
                    .ok_or(AccessError::Revoked)?;
                let snapshot = record.updates.borrow();
                if after_revision < record.minimum_revision || after_revision > snapshot.revision {
                    return Err(DocumentError::SubscriptionLagged);
                }
                if snapshot.revision > after_revision {
                    return Ok(snapshot.clone());
                }
            }
            updates.changed().await.map_err(|_| AccessError::Revoked)?;
        }
    }

    pub fn subscribe(&self) -> Result<watch::Receiver<DocumentSnapshot>, AccessError> {
        self.lease.access.check()?;
        let state = self.lease.service.inner.lock().unwrap();
        self.lease.access.check()?;
        Ok(state
            .watches
            .get(&self.lease.id)
            .ok_or(AccessError::Revoked)?
            .updates
            .subscribe())
    }
    /// Targeted read of one authorized route through this watch's mirror: hydrates exactly that
    /// route (never a whole-map clone) and returns the mirror's current view of it.
    pub fn read(&self, route: &FacetRoute) -> Result<FacetValue, AccessError> {
        self.lease.access.check()?;
        let mut state = self.lease.service.inner.lock().unwrap();
        self.lease.access.check()?;
        let record = state
            .watches
            .get(&self.lease.id)
            .ok_or(AccessError::Revoked)?;
        if !record.contains(route) {
            return Err(AccessError::Denied);
        }
        if !state.cache.contains_key(route) {
            state.hydrate(route);
            state.publish();
        }
        Ok(state
            .watches
            .get(&self.lease.id)
            .and_then(|record| record.updates.borrow().facets.get(route).cloned())
            .unwrap_or(FacetValue::Absent))
    }
    /// Request a targeted refresh of this watch's retained selection. The returned snapshot is
    /// the post-request view; a pending refresh shows up as `Freshness::Refreshing`, never as a
    /// missing value (ADR 014 §4).
    pub fn refresh(&self) -> Result<DocumentSnapshot, AccessError> {
        self.lease.access.check()?;
        let mut state = self.lease.service.inner.lock().unwrap();
        self.lease.access.check()?;
        if !state.watches.contains_key(&self.lease.id) {
            return Err(AccessError::Revoked);
        }
        state.refresh_watch(self.lease.id);
        Ok(state
            .watches
            .get(&self.lease.id)
            .ok_or(AccessError::Revoked)?
            .updates
            .borrow()
            .clone())
    }
    pub fn select(&self, selection: WatchSelection) -> Result<(), AccessError> {
        self.lease.access.check()?;
        self.lease.access.authority.validate(&selection)?;
        let mut state = self.lease.service.inner.lock().unwrap();
        self.lease.access.check()?;
        let record = state
            .watches
            .get_mut(&self.lease.id)
            .ok_or(AccessError::Revoked)?;
        record.generation += 1;
        record.minimum_revision = record.updates.borrow().revision + 1;
        record.selection = selection;
        state.refresh_watch(self.lease.id);
        state.evict();
        Ok(())
    }
    pub fn close(&self) {
        self.lease
            .service
            .inner
            .lock()
            .unwrap()
            .release_watch(self.lease.id, /*revoked*/ false);
    }
}

impl Drop for WatchLease {
    fn drop(&mut self) {
        self.service
            .inner
            .lock()
            .unwrap()
            .release_watch(self.id, /*revoked*/ false);
    }
}

#[derive(Clone, Copy)]
enum Right {
    Read,
    Write,
    Create,
    Delete,
}
impl Authority {
    fn allows(&self, route: &FacetRoute, right: Right) -> bool {
        !self.revoked.load(Ordering::Acquire)
            && self
                .documents
                .get(&(route.document.clone(), route.branch.clone()))
                .is_some_and(|acl| {
                    acl.iter().any(|access| {
                        access.tag.0 == route.tag
                            && access
                                .key_id
                                .as_ref()
                                .is_none_or(|key| key == &route.key_id)
                            && match right {
                                Right::Read => access.read,
                                Right::Write => access.write,
                                Right::Create => access.create,
                                Right::Delete => access.delete,
                            }
                    })
                })
    }
    fn validate(&self, selection: &WatchSelection) -> Result<(), AccessError> {
        for document in &selection.documents {
            let acl = self
                .documents
                .get(&(document.clone(), selection.branch.clone()))
                .ok_or(AccessError::Denied)?;
            match &selection.facets {
                FacetSelection::Tags(tags) => {
                    for tag in tags {
                        if !acl.iter().any(|access| access.read && access.tag.0 == *tag) {
                            return Err(AccessError::Denied);
                        }
                    }
                }
                FacetSelection::Keys(keys) => {
                    for (tag, key_id) in keys {
                        let route = FacetRoute {
                            document: document.clone(),
                            branch: selection.branch.clone(),
                            tag: tag.clone(),
                            key_id: key_id.clone(),
                        };
                        if !self.allows(&route, Right::Read) {
                            return Err(AccessError::Denied);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
impl WatchRecord {
    fn contains(&self, route: &FacetRoute) -> bool {
        self.selection.documents.contains(&route.document)
            && self.selection.branch == route.branch
            && match &self.selection.facets {
                FacetSelection::Tags(tags) => tags.contains(&route.tag),
                FacetSelection::Keys(keys) => {
                    keys.contains(&(route.tag.clone(), route.key_id.clone()))
                }
            }
            && self.authority.allows(route, Right::Read)
    }
}

impl State {
    fn routes_for(&self, record: &WatchRecord) -> BTreeSet<FacetRoute> {
        let mut routes = BTreeSet::new();
        for document in &record.selection.documents {
            let keys = match &record.selection.facets {
                FacetSelection::Keys(keys) => keys.clone(),
                FacetSelection::Tags(tags) => {
                    let mut keys = BTreeSet::new();
                    for tag in tags {
                        if let Some(ids) = self.membership.get(&(
                            document.clone(),
                            record.selection.branch.clone(),
                            tag.clone(),
                        )) {
                            keys.extend(ids.iter().map(|id| (tag.clone(), id.clone())));
                        }
                        // An explicitly authorized key can be observed even before it exists.
                        for acl in &record.authority.documents
                            [&(document.clone(), record.selection.branch.clone())]
                        {
                            if acl.read
                                && acl.tag.0 == *tag
                                && let Some(id) = &acl.key_id
                            {
                                keys.insert((tag.clone(), id.clone()));
                            }
                        }
                    }
                    keys
                }
            };
            for (tag, key_id) in keys {
                let route = FacetRoute {
                    document: document.clone(),
                    branch: record.selection.branch.clone(),
                    tag,
                    key_id,
                };
                if record.contains(&route) {
                    routes.insert(route);
                }
            }
        }
        routes
    }

    fn note_route(&mut self, route: &FacetRoute) {
        self.membership
            .entry((
                route.document.clone(),
                route.branch.clone(),
                route.tag.clone(),
            ))
            .or_default()
            .insert(route.key_id.clone());
    }

    fn hydrate(&mut self, route: &FacetRoute) {
        self.clock += 1;
        self.reads.push(route.clone());
        let value = self.source.get(route).cloned();
        let change = value.map_or_else(
            || CollectionChange::Remove { key: route.clone() },
            |value| CollectionChange::Upsert {
                key: route.clone(),
                value,
            },
        );
        self.projection.apply_authoritative(change);
        self.cache.insert(
            route.clone(),
            CacheEntry {
                touched: self.clock,
            },
        );
    }

    fn refresh_watch(&mut self, id: u64) {
        let record = &self.watches[&id];
        let generation = record.generation;
        let routes = self.routes_for(record);
        for route in routes {
            if self.hold_refresh {
                self.requests
                    .entry(route)
                    .or_default()
                    .insert(id, generation);
            } else {
                self.hydrate(&route);
            }
        }
        self.publish();
    }

    fn snapshot(&self, id: u64, record: &WatchRecord) -> DocumentSnapshot {
        let mut facets = BTreeMap::new();
        let mut pending = BTreeSet::new();
        for route in self.routes_for(record) {
            let value = if self.cache.contains_key(&route) {
                self.projection
                    .get(&route)
                    .map_or(FacetValue::Absent, FacetValue::Present)
            } else {
                FacetValue::Loading
            };
            for (id, mutation) in &self.pending {
                if mutation.route == route {
                    pending.insert(*id);
                }
            }
            facets.insert(route, value);
        }
        DocumentSnapshot {
            revision: record.updates.borrow().revision,
            facets,
            pending,
            freshness: if self
                .requests
                .values()
                .any(|waiters| waiters.get(&id) == Some(&record.generation))
            {
                Freshness::Refreshing
            } else {
                Freshness::Current
            },
            revoked: false,
        }
    }

    fn publish(&mut self) {
        // Each mirror advances only for its own permitted changes, not for unrelated source work.
        for (id, record) in &self.watches {
            let mut snapshot = self.snapshot(*id, record);
            if snapshot != *record.updates.borrow() || snapshot.revision < record.minimum_revision {
                snapshot.revision += 1;
                record.updates.send_replace(snapshot);
            }
        }
        self.evict();
    }

    fn release_watch(&mut self, id: u64, revoked: bool) {
        if let Some(record) = self.watches.remove(&id) {
            let revision = record.updates.borrow().revision + 1;
            record.updates.send_replace(DocumentSnapshot {
                revision,
                facets: BTreeMap::new(),
                pending: BTreeSet::new(),
                freshness: Freshness::Cached,
                revoked,
            });
        }
        self.requests.retain(|_, waiters| {
            waiters.remove(&id);
            !waiters.is_empty()
        });
        self.evict();
    }

    fn complete(&mut self, id: u64, outcome: Result<(), OperationError>) {
        let pending = self.pending.remove(&id).expect("operation is not pending");
        let status = match outcome {
            Ok(()) => {
                self.projection.accept(id);
                match pending.mutation {
                    FacetMutation::Write(value) | FacetMutation::Create(value) => {
                        self.source.insert(pending.route, value);
                    }
                    FacetMutation::Delete => {
                        self.source.remove(&pending.route);
                    }
                }
                OperationStatus::Accepted
            }
            Err(error) => {
                self.projection.rollback(id);
                OperationStatus::RolledBack(error)
            }
        };
        self.publish();
        pending.updates.send_replace(OperationReceipt {
            id,
            revision: 1,
            status,
        });
    }

    fn evict(&mut self) {
        let pinned = self
            .watches
            .values()
            .flat_map(|record| self.routes_for(record))
            .chain(self.pending.values().map(|pending| pending.route.clone()))
            .collect::<BTreeSet<_>>();
        let mut unretained = self
            .cache
            .iter()
            .filter(|(route, _)| !pinned.contains(*route))
            .map(|(route, entry)| (entry.touched, route.clone()))
            .collect::<Vec<_>>();
        unretained.sort();
        let excess = unretained.len().saturating_sub(self.capacity);
        for (_, route) in unretained.into_iter().take(excess) {
            self.cache.remove(&route);
            self.projection
                .apply_authoritative(CollectionChange::Remove { key: route });
        }
    }
}
impl MockDocuments {
    pub fn bind_config(&self, owner: String, document: String, branch: String) {
        assert!(
            self.service
                .inner
                .lock()
                .unwrap()
                .config_owners
                .insert(owner, (document, branch))
                .is_none(),
            "config owner already bound"
        );
    }
    pub fn seed(&self, route: FacetRoute, value: Value) {
        let mut state = self.service.inner.lock().unwrap();
        state.note_route(&route);
        state.source.entry(route).or_insert(value);
    }
    pub fn complete(&self, id: u64, outcome: Result<(), OperationError>) {
        self.service.inner.lock().unwrap().complete(id, outcome);
    }

    pub fn settle(&self, accept: bool) {
        let mut state = self.service.inner.lock().unwrap();
        let ids = state.pending.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let outcome = if accept {
                Ok(())
            } else {
                Err(OperationError::Rejected {
                    message: "mock source rejected operation".into(),
                })
            };
            state.complete(id, outcome);
        }
    }
    pub fn external_change(&self, route: FacetRoute, value: Option<Value>) {
        let mut state = self.service.inner.lock().unwrap();
        state.note_route(&route);
        if let Some(value) = value {
            state.source.insert(route.clone(), value);
        } else {
            state.source.remove(&route);
        }
        if state.watches.values().any(|record| record.contains(&route))
            || state.pending.values().any(|pending| pending.route == route)
        {
            state.hydrate(&route);
        }
        // An unretained cached value is invalidated rather than silently served as current later.
        else {
            state.cache.remove(&route);
            state
                .projection
                .apply_authoritative(CollectionChange::Remove { key: route });
        }
        state.publish();
    }
    pub fn hold_refreshes(&self, hold: bool) {
        self.service.inner.lock().unwrap().hold_refresh = hold;
    }
    pub fn complete_refreshes(&self) {
        let mut state = self.service.inner.lock().unwrap();
        let requests = std::mem::take(&mut state.requests);
        for (route, waiters) in requests {
            if waiters.iter().any(|(id, generation)| {
                state.watches.get(id).is_some_and(|record| {
                    record.generation == *generation && record.contains(&route)
                })
            }) {
                state.hydrate(&route);
            }
        }
        state.publish();
    }
    pub fn reads(&self) -> Vec<FacetRoute> {
        self.service.inner.lock().unwrap().reads.clone()
    }
    pub fn cached_routes(&self) -> BTreeSet<FacetRoute> {
        self.service
            .inner
            .lock()
            .unwrap()
            .cache
            .keys()
            .cloned()
            .collect()
    }
    pub fn pending_refreshes(&self) -> usize {
        self.service.inner.lock().unwrap().requests.len()
    }
}
