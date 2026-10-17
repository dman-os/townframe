//! Narrow document projection boundary. Source cursors and cache policy remain host-owned.
//! Native wrappers use these data values and handles; future WIT resources can map the same
//! watch/selection/mutation protocol without transporting callbacks or renderer state.
use crate::data::{Collection, CollectionChange, TransactionId, TransactionStatus};
use crate::host::{AccessError, Host, Instance};
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
    Loading,
    Absent,
    Present(Value),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentSnapshot {
    pub revision: u64,
    pub facets: BTreeMap<FacetRoute, FacetValue>,
    pub pending: BTreeSet<u64>,
    pub last_transaction: Option<(u64, TransactionStatus)>,
    pub refreshing: bool,
    pub revoked: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FacetMutation {
    Write(Value),
    Create(Value),
    Delete,
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
pub(crate) struct DocumentService {
    inner: Arc<Mutex<State>>,
}
struct State {
    next: u64,
    revision: u64,
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
    receipt: Option<(u64, TransactionStatus)>,
}
struct WatchRecord {
    authority: Arc<Authority>,
    selection: WatchSelection,
    generation: u64,
    updates: watch::Sender<DocumentSnapshot>,
}
struct PendingMutation {
    route: FacetRoute,
}

/// Privileged mock controls are deliberately absent from MicroContext and DocumentAccess.
#[derive(Clone)]
pub struct MockDocuments {
    service: DocumentService,
}

impl DocumentService {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                next: 0,
                revision: 0,
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
impl Host {
    /// Uses the manifest's owner-config semantics. The composition root supplies already-approved
    /// ACLs; dependency/tag validation remains the loader's responsibility, not a second ACL model.
    pub fn grant_config(
        &self,
        instance: &Instance,
        invoking_plug: &str,
        acl: Vec<RoutineFacetAccess>,
    ) -> Result<BTreeMap<String, DocumentRef>, AccessError> {
        self.check(instance)?;
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
                .documents
                .inner
                .lock()
                .unwrap()
                .config_owners
                .get(&owner)
                .cloned()
                .expect("approved config owner has no document binding");
            let access = self.grant_documents(
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
            service: self.documents.clone(),
        }
    }
    pub fn grant_documents(
        &self,
        instance: &Instance,
        grants: Vec<DocumentGrant>,
    ) -> Result<DocumentAccess, AccessError> {
        self.check(instance)?;
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
        let weak = Arc::downgrade(&self.documents.inner);
        let cleanup = authority.clone();
        self.own(
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
            service: self.documents.clone(),
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
        if self.authority.revoked.load(Ordering::Acquire) {
            return Err(AccessError::Revoked);
        }
        state.next += 1;
        let id = state.next;
        let (updates, _) = watch::channel(DocumentSnapshot {
            revision: state.revision,
            facets: BTreeMap::new(),
            pending: BTreeSet::new(),
            last_transaction: None,
            refreshing: false,
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

    pub fn mutate(&self, route: FacetRoute, mutation: FacetMutation) -> Result<u64, AccessError> {
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
        if self.authority.revoked.load(Ordering::Acquire) {
            return Err(AccessError::Revoked);
        }
        // Seed this route's base before overlaying a write. Never hydrate unrelated facets.
        if !state.cache.contains_key(&route) {
            state.hydrate(&route);
        }
        let value = match mutation {
            FacetMutation::Write(value) | FacetMutation::Create(value) => Some(value),
            FacetMutation::Delete => None,
        };
        state.next += 1;
        let id = state.next;
        let change = value.map_or_else(
            || CollectionChange::Remove { key: route.clone() },
            |value| CollectionChange::Upsert {
                key: route.clone(),
                value,
            },
        );
        state
            .projection
            .apply_optimistic(TransactionId(id), change)
            .unwrap();
        state.note_route(&route);
        state.cache.get_mut(&route).unwrap().receipt =
            Some((id, TransactionStatus::LocallyApplied));
        assert!(
            state
                .pending
                .insert(id, PendingMutation { route })
                .is_none()
        );
        state.publish();
        Ok(id)
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
    pub fn mutate(
        &self,
        tag: String,
        key_id: String,
        mutation: FacetMutation,
    ) -> Result<u64, AccessError> {
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
    pub fn write(&self, value: T) -> Result<u64, AccessError> {
        self.access.mutate(
            self.route.clone(),
            FacetMutation::Write(serde_json::to_value(value).unwrap()),
        )
    }
}
impl DocumentWatch {
    pub fn current(&self) -> Result<DocumentSnapshot, AccessError> {
        self.lease.access.check()?;
        let state = self.lease.service.inner.lock().unwrap();
        let record = state
            .watches
            .get(&self.lease.id)
            .ok_or(AccessError::Revoked)?;
        Ok(record.updates.borrow().clone())
    }
    pub fn subscribe(&self) -> Result<watch::Receiver<DocumentSnapshot>, AccessError> {
        self.lease.access.check()?;
        let state = self.lease.service.inner.lock().unwrap();
        Ok(state
            .watches
            .get(&self.lease.id)
            .ok_or(AccessError::Revoked)?
            .updates
            .subscribe())
    }
    pub fn refresh(&self) -> Result<(), AccessError> {
        self.lease.access.check()?;
        let mut state = self.lease.service.inner.lock().unwrap();
        if !state.watches.contains_key(&self.lease.id) {
            return Err(AccessError::Revoked);
        }
        state.refresh_watch(self.lease.id);
        Ok(())
    }
    pub fn select(&self, selection: WatchSelection) -> Result<(), AccessError> {
        self.lease.access.check()?;
        self.lease.access.authority.validate(&selection)?;
        let mut state = self.lease.service.inner.lock().unwrap();
        let record = state
            .watches
            .get_mut(&self.lease.id)
            .ok_or(AccessError::Revoked)?;
        record.generation += 1;
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
        self.projection.apply_authoritative(change).unwrap();
        self.cache.insert(
            route.clone(),
            CacheEntry {
                touched: self.clock,
                receipt: self.cache.get(route).and_then(|entry| entry.receipt),
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
        let mut last_transaction = None;
        for route in self.routes_for(record) {
            if let Some(receipt) = self.cache.get(&route).and_then(|entry| entry.receipt)
                && last_transaction
                    .is_none_or(|previous: (u64, TransactionStatus)| previous.0 <= receipt.0)
            {
                last_transaction = Some(receipt);
            }
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
            last_transaction,
            refreshing: self
                .requests
                .values()
                .any(|waiters| waiters.get(&id) == Some(&record.generation)),
            revoked: false,
        }
    }

    fn publish(&mut self) {
        // Each mirror advances only for its own permitted changes, not for unrelated source work.
        for (id, record) in &self.watches {
            let mut snapshot = self.snapshot(*id, record);
            if snapshot != *record.updates.borrow() {
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
                refreshing: false,
                last_transaction: None,
                revoked,
            });
        }
        self.requests.retain(|_, waiters| {
            waiters.remove(&id);
            !waiters.is_empty()
        });
        self.evict();
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
                .apply_authoritative(CollectionChange::Remove { key: route })
                .unwrap();
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
    pub fn settle(&self, accept: bool) {
        let mut state = self.service.inner.lock().unwrap();
        let pending = std::mem::take(&mut state.pending);
        for (id, mutation) in pending {
            if accept {
                state.projection.accept(TransactionId(id)).unwrap();
                if let Some(value) = state.projection.get(&mutation.route) {
                    state.source.insert(mutation.route.clone(), value);
                } else {
                    state.source.remove(&mutation.route);
                }
            } else {
                state.projection.rollback(TransactionId(id)).unwrap();
            }
            state.cache.get_mut(&mutation.route).unwrap().receipt = Some((
                id,
                if accept {
                    TransactionStatus::Accepted
                } else {
                    TransactionStatus::RolledBack
                },
            ));
            state
                .projection
                .forget_completed(TransactionId(id))
                .unwrap();
        }
        state.publish();
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
                .apply_authoritative(CollectionChange::Remove { key: route })
                .unwrap();
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
