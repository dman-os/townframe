//! Merge policies (ADR 017 §9): each policy's recomputation on scope close, install and
//! uninstall, always inside one projection transaction, plus a model check of every
//! policy against a recompute-from-scratch oracle.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use clay_store::MemoryStore;
use proptest::prelude::*;

use super::*;
use crate::projection::ProjectionLease;
use crate::types::Change;
use crate::types::Cursor;
use crate::types::Page;
use crate::types::ReadLimits;

/// `(scope, local key)` under `Union`, `(0, local key)` under every other policy.
type Key = (u64, u32);
type Coll = RegistrationCollection<u32, String, Key>;
type Scope = RegistrationScope<u32, String, Key>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Union,
    Group,
    Priority,
    Lww,
    Reject,
}

const KINDS: [Kind; 5] = [
    Kind::Union,
    Kind::Group,
    Kind::Priority,
    Kind::Lww,
    Kind::Reject,
];

fn materializer(kind: Kind) -> Materializer<u32, String, Key> {
    match kind {
        Kind::Union => Materializer::Union {
            key: |scope, key| (scope.get(), *key),
        },
        Kind::Group => Materializer::Group {
            key: |key| (0, *key),
            merge: |contributions| {
                contributions
                    .iter()
                    .map(|contribution| contribution.value.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            },
        },
        Kind::Priority => Materializer::Priority {
            key: |key| (0, *key),
            installation_rank: |scope_key| ScopeRank(100 + u64::from(scope_key[0])),
        },
        Kind::Lww => Materializer::HostOrderedLww {
            key: |key| (0, *key),
        },
        Kind::Reject => Materializer::RejectCollision {
            key: |key| (0, *key),
        },
    }
}

fn volatile(kind: Kind) -> Coll {
    RegistrationCollection::with_materializer(
        (),
        std::num::NonZeroUsize::new(256).unwrap(),
        [1, 2],
        materializer(kind),
    )
}

fn codec() -> DurableCodec<u32, String> {
    DurableCodec {
        encode_key: |key| key.to_be_bytes().to_vec(),
        decode_key: |bytes| u32::from_be_bytes(bytes.try_into().expect("u32 key")),
        encode_value: |value| value.as_bytes().to_vec(),
        decode_value: |bytes| String::from_utf8(bytes.to_vec()).expect("utf8 value"),
    }
}

async fn durable(kind: Kind, store: &Arc<MemoryStore>) -> Coll {
    Coll::with_durable_source(
        Arc::clone(store),
        b"merged".to_vec(),
        codec(),
        (),
        std::num::NonZeroUsize::new(256).unwrap(),
        [1, 2],
        materializer(kind),
    )
    .await
    .unwrap()
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let Poll::Ready(value) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("expected a ready future");
    };
    value
}

/// A scope under `kind`; `rank` is used only by `Priority`.
fn open(collection: &Coll, kind: Kind, rank: u64) -> Scope {
    match kind {
        Kind::Priority => collection.create_ranked_scope(ScopeRank(rank)),
        _ => collection.create_scope(),
    }
}

fn put(scope: &Scope, key: u32, value: &str) {
    let writer = scope.writer().unwrap();
    let based_on = writer.version(&key);
    writer
        .put(key, based_on, value.to_owned())
        .unwrap()
        .dispatch();
}

fn rows(entries: &[(u32, &str)]) -> BTreeMap<u32, String> {
    entries
        .iter()
        .map(|(key, value)| (*key, (*value).to_owned()))
        .collect()
}

/// A reader that applies every transaction to a plain map, to observe exactly what a
/// consumer of the projection sees, one transaction at a time.
struct Follower {
    lease: ProjectionLease<Key, String>,
    through: Cursor,
    view: BTreeMap<Key, String>,
}

impl Follower {
    fn new(collection: &Coll) -> Self {
        let mut lease = collection.projection().acquire((), None).unwrap();
        let page = ready(lease.read(None, ReadLimits::default())).unwrap();
        assert!(page.bootstrap_complete);
        let mut follower = Self {
            lease,
            through: page.through.clone(),
            view: BTreeMap::new(),
        };
        follower.apply(page);
        follower
    }

    fn apply(&mut self, page: Page<Key, String>) {
        for transaction in page.transactions {
            let mut seen = BTreeMap::new();
            for change in transaction.changes {
                assert!(
                    seen.insert(*change.key(), ()).is_none(),
                    "one transaction changed {:?} twice",
                    change.key()
                );
                match change {
                    Change::Upsert { key, value, .. } => {
                        self.view.insert(key, value);
                    }
                    Change::Remove { key, .. } => {
                        assert!(self.view.remove(&key).is_some(), "removed an absent row");
                    }
                }
            }
        }
    }

    /// Read what has been published since the last call; returns the transaction count.
    fn advance(&mut self) -> usize {
        let polled = {
            let mut read = std::pin::pin!(
                self.lease
                    .read(Some(self.through.clone()), ReadLimits::default())
            );
            read.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        };
        let Poll::Ready(page) = polled else {
            return 0;
        };
        let page = page.unwrap();
        self.through = page.through.clone();
        let count = page.transactions.len();
        self.apply(page);
        count
    }

    fn values(&self) -> Vec<(u32, &str)> {
        self.view
            .iter()
            .map(|((_, key), value)| (*key, value.as_str()))
            .collect()
    }
}

#[test]
fn union_keeps_every_scopes_row_and_close_removes_only_its_own() {
    let collection = volatile(Kind::Union);
    let (a, b) = (
        open(&collection, Kind::Union, 0),
        open(&collection, Kind::Union, 0),
    );
    put(&a, 1, "a");
    put(&b, 1, "b");
    let mut follower = Follower::new(&collection);
    assert_eq!(
        follower.view,
        BTreeMap::from([
            ((a.id().get(), 1), "a".to_owned()),
            ((b.id().get(), 1), "b".to_owned())
        ])
    );
    a.close().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.view.len(), 1);
    assert_eq!(follower.view[&(b.id().get(), 1)], "b");
}

#[test]
fn group_values_every_contribution_in_deterministic_scope_order() {
    let collection = volatile(Kind::Group);
    let (a, b, c) = (
        open(&collection, Kind::Group, 0),
        open(&collection, Kind::Group, 0),
        open(&collection, Kind::Group, 0),
    );
    // Write out of mint order: the group order is the scope order, not the write order.
    put(&c, 1, "c");
    put(&a, 1, "a");
    put(&b, 1, "b");
    put(&b, 2, "only-b");
    let mut follower = Follower::new(&collection);
    assert_eq!(follower.values(), [(1, "a,b,c"), (2, "only-b")]);
    b.close().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "a,c")]);
    // Updating a contribution re-renders the group in place.
    put(&a, 1, "a2");
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "a2,c")]);
    c.writer().unwrap().clear().unwrap().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "a2")]);
    // A fresh reader bootstraps the same view.
    assert_eq!(Follower::new(&collection).view, follower.view);
}

#[test]
fn priority_reveals_the_next_ranked_contribution_when_the_winner_leaves() {
    let collection = volatile(Kind::Priority);
    let low = open(&collection, Kind::Priority, 1);
    let high = open(&collection, Kind::Priority, 9);
    let mid = open(&collection, Kind::Priority, 5);
    put(&high, 1, "high");
    put(&low, 1, "low");
    put(&mid, 1, "mid");
    put(&low, 2, "low-only");
    let mut follower = Follower::new(&collection);
    assert_eq!(follower.values(), [(1, "high"), (2, "low-only")]);
    // A lower-ranked write changes nothing visible, and publishes nothing.
    put(&mid, 1, "mid2");
    assert_eq!(follower.advance(), 0);
    // Removing the winning row reveals the next rank in the same transaction.
    let writer = high.writer().unwrap();
    let version = writer.version(&1);
    writer.remove(1, version).unwrap().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "mid2"), (2, "low-only")]);
    mid.close().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "low"), (2, "low-only")]);
    low.close().dispatch();
    assert_eq!(follower.advance(), 1);
    assert!(follower.view.is_empty());
}

#[test]
fn priority_ranks_are_explicit_and_unique() {
    let collection = volatile(Kind::Priority);
    // A programming error panics under the collection lock, which poisons that
    // collection: every case below uses a fresh one.
    let held = collection.create_ranked_scope(ScopeRank(3));
    assert!(std::panic::catch_unwind(|| collection.create_ranked_scope(ScopeRank(3))).is_err());
    let unranked = volatile(Kind::Priority);
    assert!(std::panic::catch_unwind(|| unranked.create_scope()).is_err());
    let unranked = volatile(Kind::Priority);
    assert!(std::panic::catch_unwind(|| unranked.create_staged_scope()).is_err());
    // The rank is free again once its scope closes.
    let collection = volatile(Kind::Priority);
    drop(collection.create_ranked_scope(ScopeRank(3)));
    collection.create_ranked_scope(ScopeRank(3));
    // Dropping a scope of the poisoned collection would panic again.
    std::mem::forget(held);
    // Other policies take no ranks.
    let union = volatile(Kind::Union);
    assert!(std::panic::catch_unwind(|| union.create_ranked_scope(ScopeRank(1))).is_err());
}

#[test]
fn host_ordered_lww_falls_back_to_the_previous_write_when_the_latest_scope_closes() {
    let collection = volatile(Kind::Lww);
    let (a, b, c) = (
        open(&collection, Kind::Lww, 0),
        open(&collection, Kind::Lww, 0),
        open(&collection, Kind::Lww, 0),
    );
    put(&a, 1, "a1");
    put(&b, 1, "b1");
    put(&c, 1, "c1");
    let mut follower = Follower::new(&collection);
    assert_eq!(follower.values(), [(1, "c1")]);
    // Recency is write order, not scope order: a's rewrite now wins.
    put(&a, 1, "a2");
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "a2")]);
    a.close().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "c1")]);
    c.close().dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "b1")]);
}

#[test]
fn reject_collision_refuses_a_held_key_until_its_holder_closes() {
    let collection = volatile(Kind::Reject);
    let (a, b) = (
        open(&collection, Kind::Reject, 0),
        open(&collection, Kind::Reject, 0),
    );
    let mut follower = Follower::new(&collection);
    put(&a, 1, "a");
    assert_eq!(follower.advance(), 1);
    let writer_b = b.writer().unwrap();
    assert_eq!(
        writer_b
            .put(1, None, "b".into())
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Collision { holder: a.id() })
    );
    // The refusal changed nothing, and distinct keys and own rewrites are fine.
    assert_eq!(follower.advance(), 0);
    assert_eq!(writer_b.row_count(), 0);
    writer_b.put(2, None, "b2".into()).unwrap().dispatch();
    put(&a, 1, "a-again");
    assert_eq!(follower.advance(), 2);
    a.close().dispatch();
    assert_eq!(follower.advance(), 1);
    put(&b, 1, "b");
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "b"), (2, "b2")]);
}

#[test]
fn reject_collision_counts_staged_rows_as_held() {
    let collection = volatile(Kind::Reject);
    let staged = collection.create_staged_scope();
    put(&staged, 1, "staged");
    let live = collection.create_scope();
    assert_eq!(
        live.writer()
            .unwrap()
            .put(1, None, "live".into())
            .map(RegistrationDispatch::dispatch),
        Err(MutationError::Collision {
            holder: staged.id()
        })
    );
    let mut follower = Follower::new(&collection);
    assert!(follower.view.is_empty());
    collection.commit_scopes(&[&staged]).dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "staged")]);
}

#[test]
fn staged_contributions_stay_out_of_merged_views_until_commit() {
    let collection = volatile(Kind::Group);
    let live = collection.create_scope();
    let staged = collection.create_staged_scope();
    put(&live, 1, "live");
    put(&staged, 1, "staged");
    let mut follower = Follower::new(&collection);
    assert_eq!(follower.values(), [(1, "live")]);
    collection.commit_scopes(&[&staged]).dispatch();
    assert_eq!(follower.advance(), 1);
    assert_eq!(follower.values(), [(1, "live,staged")]);
}

#[test]
fn durable_install_and_uninstall_recompute_and_reopen_reproduces_the_view() {
    for kind in [Kind::Group, Kind::Priority, Kind::Lww] {
        let store = Arc::new(MemoryStore::default());
        let collection = ready(durable(kind, &store));
        let live = open(&collection, kind, 1);
        put(&live, 1, "live");
        let mut follower = Follower::new(&collection);
        // Installed out of key order: Group order is by installation key, then incarnations.
        ready(collection.install(b"b", rows(&[(1, "b1"), (2, "b2")])))
            .unwrap()
            .dispatch();
        assert_eq!(follower.advance(), 1, "{kind:?}");
        ready(collection.install(b"a", rows(&[(1, "a1")])))
            .unwrap()
            .dispatch();
        // Under Priority "b" already outranks "a" on key 1: nothing visible changed.
        let visible = usize::from(kind != Kind::Priority);
        assert_eq!(follower.advance(), visible, "{kind:?}");
        let expected_one = match kind {
            Kind::Group => "a1,b1,live",
            // Installation ranks (100 + key byte) beat the incarnation's rank 1; "b" > "a".
            Kind::Priority => "b1",
            // "a" was installed last.
            _ => "a1",
        };
        assert_eq!(
            follower.values(),
            [(1, expected_one), (2, "b2")],
            "{kind:?}"
        );
        // Reinstalling replaces a's rows: key 1 leaves, key 3 arrives, one transaction.
        ready(collection.install(b"a", rows(&[(3, "a3")])))
            .unwrap()
            .dispatch();
        assert_eq!(follower.advance(), 1, "{kind:?}");
        let expected_one = match kind {
            Kind::Group => "b1,live",
            _ => "b1",
        };
        assert_eq!(
            follower.values(),
            [(1, expected_one), (2, "b2"), (3, "a3")],
            "{kind:?}"
        );
        ready(collection.uninstall(b"b")).unwrap().dispatch();
        assert_eq!(follower.advance(), 1, "{kind:?}");
        assert_eq!(follower.values(), [(1, "live"), (3, "a3")], "{kind:?}");
        drop(live);
        drop(follower);
        // Reopening reproduces the materialized view of what is stored, whatever
        // scope ids and versions the reload assigned.
        let reopened = ready(durable(kind, &store));
        assert_eq!(Follower::new(&reopened).values(), [(3, "a3")], "{kind:?}");
    }
}

#[test]
fn durable_reopen_restores_priority_and_group_order_from_stable_keys() {
    let store = Arc::new(MemoryStore::default());
    let collection = ready(durable(Kind::Group, &store));
    ready(collection.install(b"z", rows(&[(1, "z")])))
        .unwrap()
        .dispatch();
    ready(collection.install(b"m", rows(&[(1, "m")])))
        .unwrap()
        .dispatch();
    let before = Follower::new(&collection).view;
    assert_eq!(before[&(0, 1)], "m,z");
    drop(collection);
    assert_eq!(
        Follower::new(&ready(durable(Kind::Group, &store))).view,
        before
    );

    let store = Arc::new(MemoryStore::default());
    let collection = ready(durable(Kind::Priority, &store));
    ready(collection.install(b"z", rows(&[(1, "z")])))
        .unwrap()
        .dispatch();
    ready(collection.install(b"m", rows(&[(1, "m")])))
        .unwrap()
        .dispatch();
    let before = Follower::new(&collection).view;
    assert_eq!(before[&(0, 1)], "z");
    drop(collection);
    assert_eq!(
        Follower::new(&ready(durable(Kind::Priority, &store))).view,
        before
    );
}

#[test]
fn durable_install_collision_is_refused_before_the_store_changes() {
    let store = Arc::new(MemoryStore::default());
    let collection = ready(durable(Kind::Reject, &store));
    ready(collection.install(b"a", rows(&[(1, "a1")])))
        .unwrap()
        .dispatch();
    let live = collection.create_scope();
    put(&live, 2, "live");
    let mut follower = Follower::new(&collection);
    assert!(matches!(
        ready(collection.install(b"b", rows(&[(3, "b3"), (1, "b1")]))),
        Err(InstallError::Collision { .. })
    ));
    assert!(matches!(
        ready(collection.install(b"b", rows(&[(2, "b2")]))),
        Err(InstallError::Collision { holder }) if holder == live.id()
    ));
    assert_eq!(follower.advance(), 0);
    // Reinstalling a scope over its own keys is not a collision.
    ready(collection.install(b"a", rows(&[(1, "a1b")])))
        .unwrap()
        .dispatch();
    assert_eq!(follower.advance(), 1);
    drop(live);
    assert_eq!(
        Follower::new(&ready(durable(Kind::Reject, &store))).values(),
        [(1, "a1b")]
    );
}

/// An incarnation slot or an installation, as the oracle names scopes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Ident {
    /// The stable installation key byte.
    Install(u8),
    Slot(usize),
}

/// One oracle scope: its rows carry the host order (`seq`) in which they were accepted.
struct ModelScope {
    ident: Ident,
    /// Mint order of an incarnation scope; unused by installations.
    opened: u64,
    rank: u64,
    rows: BTreeMap<u32, (u64, String)>,
}

#[derive(Default)]
struct Model {
    scopes: Vec<ModelScope>,
    seq: u64,
    opened: u64,
}

impl Model {
    fn scope(&mut self, ident: Ident) -> Option<&mut ModelScope> {
        self.scopes.iter_mut().find(|scope| scope.ident == ident)
    }

    fn holder(&self, key: u32, except: Ident) -> Option<Ident> {
        self.scopes
            .iter()
            .find(|scope| scope.ident != except && scope.rows.contains_key(&key))
            .map(|scope| scope.ident)
    }

    /// The consumer-facing view recomputed from nothing but the contributions.
    fn expected(&self, kind: Kind, id_of: &dyn Fn(Ident) -> u64) -> BTreeMap<Key, String> {
        let keys: std::collections::BTreeSet<u32> = self
            .scopes
            .iter()
            .flat_map(|scope| scope.rows.keys().copied())
            .collect();
        let mut view = BTreeMap::new();
        for key in keys {
            let mut contributions: Vec<(&ModelScope, &(u64, String))> = self
                .scopes
                .iter()
                .filter_map(|scope| scope.rows.get(&key).map(|row| (scope, row)))
                .collect();
            match kind {
                Kind::Union => {
                    for (scope, (_, value)) in contributions {
                        view.insert((id_of(scope.ident), key), value.clone());
                    }
                }
                Kind::Group => {
                    // Installations by key, then incarnations by mint order.
                    contributions.sort_by_key(|(scope, _)| match scope.ident {
                        Ident::Install(byte) => (0, u64::from(byte)),
                        Ident::Slot(_) => (1, scope.opened),
                    });
                    let joined: Vec<&str> = contributions
                        .iter()
                        .map(|(_, row)| row.1.as_str())
                        .collect();
                    view.insert((0, key), joined.join(","));
                }
                Kind::Priority => {
                    let (_, (_, value)) = contributions
                        .iter()
                        .max_by_key(|(scope, _)| scope.rank)
                        .unwrap();
                    view.insert((0, key), value.clone());
                }
                Kind::Lww => {
                    let (_, (_, value)) = contributions
                        .iter()
                        .max_by_key(|(_, (seq, _))| *seq)
                        .unwrap();
                    view.insert((0, key), value.clone());
                }
                Kind::Reject => {
                    assert_eq!(contributions.len(), 1, "model holds a collision");
                    view.insert((0, key), contributions[0].1.1.clone());
                }
            }
        }
        view
    }
}

#[derive(Clone, Debug)]
enum Op {
    Open(usize),
    Close(usize),
    Put(usize, u32, u8),
    Remove(usize, u32),
    Clear(usize),
    Install(u8, Vec<(u32, u8)>),
    Uninstall(u8),
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    let slot = 0..3usize;
    let key = 0..4u32;
    prop::collection::vec(
        prop_oneof![
            slot.clone().prop_map(Op::Open),
            slot.clone().prop_map(Op::Close),
            (slot.clone(), key.clone(), 0..4u8).prop_map(|(s, k, v)| Op::Put(s, k, v)),
            (slot.clone(), key.clone()).prop_map(|(s, k)| Op::Remove(s, k)),
            slot.prop_map(Op::Clear),
            (0..2u8, prop::collection::vec((key.clone(), 0..4u8), 0..4))
                .prop_map(|(i, rows)| Op::Install(i, rows)),
            (0..2u8).prop_map(Op::Uninstall),
        ],
        0..40,
    )
}

fn run_model(kind: Kind, ops: &[Op]) -> Result<(), TestCaseError> {
    let store = Arc::new(MemoryStore::default());
    let collection = ready(durable(kind, &store));
    let mut follower = Follower::new(&collection);
    let mut model = Model::default();
    let mut slots: [Option<Scope>; 3] = [None, None, None];
    // Installation keys are one byte, `b'a' + i`, whose Priority rank is `100 + byte`.
    let install_key = |i: u8| vec![b'a' + i];
    let id_of = |collection: &Coll, ident: Ident, slots: &[Option<Scope>; 3]| match ident {
        Ident::Slot(slot) => slots[slot].as_ref().unwrap().id().get(),
        Ident::Install(byte) => collection.lock().installations[&vec![b'a' + byte]].get(),
    };
    for op in ops {
        match op {
            Op::Open(slot) if slots[*slot].is_none() => {
                // Slot ranks 1..=3 stay below installation ranks and are unique.
                slots[*slot] = Some(open(&collection, kind, 1 + *slot as u64));
                model.opened += 1;
                let opened = model.opened;
                model.scopes.push(ModelScope {
                    ident: Ident::Slot(*slot),
                    opened,
                    rank: 1 + *slot as u64,
                    rows: BTreeMap::new(),
                });
            }
            Op::Close(slot) => {
                if let Some(scope) = slots[*slot].take() {
                    scope.close().dispatch();
                    model
                        .scopes
                        .retain(|scope| scope.ident != Ident::Slot(*slot));
                }
            }
            Op::Put(slot, key, value) => {
                let Some(scope) = &slots[*slot] else { continue };
                let writer = scope.writer().unwrap();
                let based_on = writer.version(key);
                let result = writer.put(*key, based_on, format!("v{value}"));
                let held = model.holder(*key, Ident::Slot(*slot));
                if kind == Kind::Reject && held.is_some() {
                    let refused = matches!(result, Err(MutationError::Collision { .. }));
                    prop_assert!(refused);
                } else {
                    result.unwrap().dispatch();
                    model.seq += 1;
                    let seq = model.seq;
                    model
                        .scope(Ident::Slot(*slot))
                        .unwrap()
                        .rows
                        .insert(*key, (seq, format!("v{value}")));
                }
            }
            Op::Remove(slot, key) => {
                let Some(scope) = &slots[*slot] else { continue };
                let writer = scope.writer().unwrap();
                let based_on = writer.version(key);
                writer.remove(*key, based_on).unwrap().dispatch();
                model.scope(Ident::Slot(*slot)).unwrap().rows.remove(key);
            }
            Op::Clear(slot) => {
                let Some(scope) = &slots[*slot] else { continue };
                scope.writer().unwrap().clear().unwrap().dispatch();
                model.scope(Ident::Slot(*slot)).unwrap().rows.clear();
            }
            Op::Install(i, entries) => {
                let installed: BTreeMap<u32, String> = entries
                    .iter()
                    .map(|(key, value)| (*key, format!("i{value}")))
                    .collect();
                let ident = Ident::Install(*i);
                let collides = kind == Kind::Reject
                    && installed
                        .keys()
                        .any(|key| model.holder(*key, ident).is_some());
                let result = ready(collection.install(&install_key(*i), installed.clone()));
                if collides {
                    let refused = matches!(result, Err(InstallError::Collision { .. }));
                    prop_assert!(refused);
                } else {
                    result.unwrap().dispatch();
                    let mut fresh = BTreeMap::new();
                    for (key, value) in installed {
                        model.seq += 1;
                        fresh.insert(key, (model.seq, value));
                    }
                    match model.scope(ident) {
                        Some(scope) => scope.rows = fresh,
                        None => model.scopes.push(ModelScope {
                            ident,
                            opened: 0,
                            rank: 100 + u64::from(b'a' + *i),
                            rows: fresh,
                        }),
                    }
                }
            }
            Op::Uninstall(i) => {
                ready(collection.uninstall(&install_key(*i)))
                    .unwrap()
                    .dispatch();
                model
                    .scopes
                    .retain(|scope| scope.ident != Ident::Install(*i));
            }
            Op::Open(_) => continue,
        }
        let transactions = follower.advance();
        prop_assert!(
            transactions <= 1,
            "{kind:?}: {transactions} transactions for {op:?}"
        );
        let expected = model.expected(kind, &|ident| id_of(&collection, ident, &slots));
        prop_assert_eq!(&follower.view, &expected, "{:?} after {:?}", kind, op);
        // A reader that joins now bootstraps the same view.
        prop_assert_eq!(&Follower::new(&collection).view, &expected);
    }
    // Reopening drops incarnations; the stored installations materialize identically.
    drop(follower);
    slots = [None, None, None];
    let _ = &slots;
    drop(collection);
    let reopened = ready(durable(kind, &store));
    model
        .scopes
        .retain(|scope| matches!(scope.ident, Ident::Install(_)));
    // Row versions are reassigned in store scan order: installation key, then row key.
    model.scopes.sort_by_key(|scope| scope.ident);
    model.seq = 0;
    for scope in &mut model.scopes {
        for (seq, _) in scope.rows.values_mut() {
            model.seq += 1;
            *seq = model.seq;
        }
    }
    let expected = model.expected(kind, &|ident| id_of(&reopened, ident, &[None, None, None]));
    prop_assert_eq!(Follower::new(&reopened).view, expected);
    Ok(())
}

proptest! {
    #[test]
    fn every_policy_projects_the_recomputed_model_after_every_step(ops in ops()) {
        for kind in KINDS {
            run_model(kind, &ops)?;
        }
    }
}
