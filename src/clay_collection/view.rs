use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::ledger::Ledger;
use crate::ledger::OverlayPolicy;
use crate::replica::Replica;
use crate::types::BaseDelta;
use crate::types::Batch;
use crate::types::Coverage;
use crate::types::DemandId;
use crate::types::DemandToken;
use crate::types::OperationId;
use crate::types::OverlayDelta;
use crate::types::ScopeId;
use crate::types::WriteOutcome;

static NEXT_OPERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_DEMAND: AtomicU64 = AtomicU64::new(1);
static NEXT_LISTENER: AtomicU64 = AtomicU64::new(1);

/// Process-wide so two views feeding one hop cannot mint colliding ids in-process. A
/// cross-address-space chain still needs the host to namespace ids per guest; that belongs
/// to the WIT layer, not here.
fn mint_operation() -> OperationId {
    OperationId(NEXT_OPERATION.fetch_add(1, AtomicOrdering::Relaxed))
}

fn mint_demand() -> DemandId {
    DemandId(NEXT_DEMAND.fetch_add(1, AtomicOrdering::Relaxed))
}

/// The consumer→upstream half of the waist. Nothing here names a collection: the far end
/// may be another view, a host-held store, or a node adapter.
pub trait Upstream<K, V> {
    /// Register the relay that carries this hop's batches downstream. Called once, before
    /// any demand, so the subscription exists before the first row arrives and there is no
    /// read-then-subscribe gap. An upstream that delivers over its own transport instead of
    /// through a relay has nothing to register here, but must still close that gap itself.
    fn attach(&self, downstream: &Downstream<K, V>);
    fn demand(&self, token: DemandToken, id: DemandId);
    fn release(&self, id: DemandId);
    fn write(&self, id: OperationId, key: K, value: Option<V>, writer: ScopeId);
}

/// A marker-free handle on a downstream hop's state. Relays register against the state,
/// which does not depend on whether that hop may write, so an authority can feed a
/// read-only reader and a writable guest alike.
pub struct Downstream<K, V> {
    state: Weak<RefCell<State<K, V>>>,
}

impl<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static> Downstream<K, V> {
    pub fn apply(&self, batch: Batch<K, V>) {
        if let Some(state) = self.state.upgrade() {
            dispatch(&state, batch, false);
        }
    }

    pub fn close(&self) {
        if let Some(state) = self.state.upgrade() {
            close(&state);
        }
    }
}

impl<K, V> Clone for Downstream<K, V> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// Marker for a hop that may submit writes: an authority, or a guest relaying its own
/// optimistic rows upstream.
pub struct Writable;
/// Marker for a hop that may only read, demand, and release. A host-collection reader has
/// no contributor capability, so its write path does not exist rather than failing at
/// runtime — the type carries the fact the capability check already established.
pub struct ReadOnly;

/// Where a hop's rows come from. This is explicit rather than inferred from the absence of
/// an upstream: a consumer that was never wired to one must not silently act as an
/// authority, because that makes it answer its own demand with nothing and treat its own
/// writes as terminal.
enum UpstreamSlot<K, V> {
    Authority,
    Consumer(Rc<dyn Upstream<K, V>>),
}

impl<K, V> Clone for UpstreamSlot<K, V> {
    fn clone(&self) -> Self {
        match self {
            Self::Authority => Self::Authority,
            Self::Consumer(upstream) => Self::Consumer(Rc::clone(upstream)),
        }
    }
}

type Filter<V> = Box<dyn Fn(&V) -> bool>;
type OrderBy<V> = Box<dyn Fn(&V, &V) -> Ordering>;
type Listener<K, V> = Rc<dyn Fn(&Event<K, V>)>;
type Relay<K, V> = Weak<RefCell<State<K, V>>>;

#[derive(Clone, Debug, PartialEq)]
pub struct RowChange<K, V> {
    pub key: K,
    pub before: Option<V>,
    pub after: Option<V>,
}

/// One visible transition of a hop.
///
/// `forward` is the layer-correct delta set — base, overlays and evidence kept apart —
/// for downstream views; `changes` is the flattened result for UI subscribers. Both
/// describe the same transition, because a batch is applied whole: splitting base from
/// overlay would let a downstream render a frame where a pending write vanished and then
/// reappeared.
#[derive(Clone, Debug)]
pub struct Event<K, V> {
    pub changes: Vec<RowChange<K, V>>,
    pub forward: Batch<K, V>,
    pub reset: bool,
    pub closed: bool,
}

impl<K, V> Event<K, V> {
    fn empty() -> Self {
        Self {
            changes: Vec::new(),
            forward: Batch::new(0),
            reset: false,
            closed: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.forward.is_empty() && !self.reset && !self.closed
    }
}

pub struct Query<V> {
    pub filter: Option<Filter<V>>,
    pub order_by: Option<OrderBy<V>>,
    pub limit: Option<usize>,
    pub demand: Option<DemandToken>,
}

impl<V> Default for Query<V> {
    fn default() -> Self {
        Self {
            filter: None,
            order_by: None,
            limit: None,
            demand: None,
        }
    }
}

impl<V> Query<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filter(mut self, f: impl Fn(&V) -> bool + 'static) -> Self {
        self.filter = Some(Box::new(f));
        self
    }

    pub fn order_by(mut self, f: impl Fn(&V, &V) -> Ordering + 'static) -> Self {
        self.order_by = Some(Box::new(f));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn demand(mut self, token: DemandToken) -> Self {
        self.demand = Some(token);
        self
    }
}

struct Notification<K, V> {
    event: Rc<Event<K, V>>,
    listeners: Vec<(u64, Listener<K, V>)>,
    relays: Vec<(u64, Relay<K, V>)>,
}

struct State<K, V> {
    replica: Replica<K, V>,
    ledger: Ledger<K, V>,
    upstream: UpstreamSlot<K, V>,
    covered: BTreeSet<DemandId>,
    demands: BTreeSet<DemandId>,
    query_demands: HashMap<DemandToken, (u64, DemandId)>,
    closed: bool,
    notifying: bool,
    notifications: VecDeque<Notification<K, V>>,
    listeners: Vec<(u64, Listener<K, V>)>,
    relays: Vec<(u64, Relay<K, V>)>,
}

impl<K, V> Drop for State<K, V> {
    fn drop(&mut self) {
        if let UpstreamSlot::Consumer(upstream) = &self.upstream {
            for id in std::mem::take(&mut self.demands) {
                upstream.release(id);
            }
        }
    }
}

impl<K: Ord + Clone, V: Clone> State<K, V> {
    fn visible(&self, key: &K) -> Option<V> {
        match self.ledger.pending(key) {
            Some(pending) => pending.cloned(),
            None => self.replica.get(key).map(|row| row.value.clone()),
        }
    }

    fn all_visible(&self) -> Vec<(K, V)> {
        let keys: BTreeSet<K> = self
            .replica
            .rows()
            .map(|(key, _)| key.clone())
            .chain(self.ledger.keys().cloned())
            .collect();
        keys.into_iter()
            .filter_map(|key| self.visible(&key).map(|value| (key, value)))
            .collect()
    }
}

/// One hop: a base replica, a segregated overlay ledger, and a local query over what a
/// reader actually sees.
///
/// A view is `Clone`; clones share the hop. Downstream relays hold the state weakly so a
/// chain of views cannot form an ownership cycle through its own listeners. The third
/// parameter is the write-capability marker: a [`Writable`] hop may submit rows, a
/// [`ReadOnly`] hop may only read, demand, and release.
pub struct View<K, V, W = Writable> {
    state: Rc<RefCell<State<K, V>>>,
    marker: PhantomData<W>,
}

impl<K: Ord + Clone + 'static, V: Clone + 'static, W> Clone for View<K, V, W> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            marker: PhantomData,
        }
    }
}

pub struct Subscription<K, V> {
    state: Weak<RefCell<State<K, V>>>,
    id: u64,
}

impl<K, V> Drop for Subscription<K, V> {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state
                .borrow_mut()
                .listeners
                .retain(|(id, _)| *id != self.id);
        }
    }
}

impl<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static> View<K, V, Writable> {
    /// The hop that owns its rows. Writes are terminal, demand is answered from what it
    /// already holds, and disposing a scope removes the base rows that scope owns.
    pub fn authority(policy: OverlayPolicy, incarnation: u64) -> Self {
        Self::with_slot(policy, incarnation, UpstreamSlot::Authority)
    }

    /// A writable hop fed by an upstream one: a guest that predicts its own rows and
    /// relays the intent upward, or a relay between two hops.
    pub fn consumer(
        policy: OverlayPolicy,
        incarnation: u64,
        upstream: Box<dyn Upstream<K, V>>,
    ) -> Self {
        Self::with_slot(
            policy,
            incarnation,
            UpstreamSlot::Consumer(Rc::from(upstream)),
        )
    }
}

impl<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static> View<K, V, ReadOnly> {
    /// A read-only hop fed by an upstream one: a consumer of a host-held collection, which
    /// holds no contributor capability. It can query, demand coverage, and release demand,
    /// and the absence of a write path is what keeps it from fabricating rows locally.
    pub fn reader(
        policy: OverlayPolicy,
        incarnation: u64,
        upstream: Box<dyn Upstream<K, V>>,
    ) -> (Self, Downstream<K, V>) {
        let view = Self::with_slot(
            policy,
            incarnation,
            UpstreamSlot::Consumer(Rc::from(upstream)),
        );
        let downstream = view.downstream();
        (view, downstream)
    }
}

impl<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static, W> View<K, V, W> {
    fn with_slot(policy: OverlayPolicy, incarnation: u64, upstream: UpstreamSlot<K, V>) -> Self {
        let view = Self {
            state: Rc::new(RefCell::new(State {
                replica: Replica::new(incarnation),
                ledger: Ledger::new(policy),
                upstream,
                covered: BTreeSet::new(),
                demands: BTreeSet::new(),
                query_demands: HashMap::new(),
                closed: false,
                notifying: false,
                notifications: VecDeque::new(),
                listeners: Vec::new(),
                relays: Vec::new(),
            })),
            marker: PhantomData,
        };
        // Register the relay before any demand, so the subscription exists before the first
        // row arrives. A consumer's upstream is stored by `with_slot` already, which is what
        // makes the registration reachable from the far end's first publish.
        let slot = view.state.borrow().upstream.clone();
        if let UpstreamSlot::Consumer(upstream) = slot {
            upstream.attach(&view.downstream());
        }
        view
    }

    /// The handle this hop's upstream registers as a relay.
    fn downstream(&self) -> Downstream<K, V> {
        Downstream {
            state: Rc::downgrade(&self.state),
        }
    }

    pub fn policy(&self) -> OverlayPolicy {
        self.state.borrow().ledger.policy()
    }

    pub fn incarnation(&self) -> u64 {
        self.state.borrow().replica.incarnation()
    }

    pub fn is_closed(&self) -> bool {
        self.state.borrow().closed
    }

    pub fn close(&self) {
        close(&self.state);
    }

    pub fn base_len(&self) -> usize {
        self.state.borrow().replica.len()
    }

    pub fn overlay_len(&self) -> usize {
        self.state.borrow().ledger.len()
    }

    pub fn is_covered(&self, id: DemandId) -> bool {
        self.state.borrow().covered.contains(&id)
    }

    pub fn visible(&self, key: &K) -> Option<V> {
        self.state.borrow().visible(key)
    }

    pub fn subscribe(&self, listener: impl Fn(&Event<K, V>) + 'static) -> Subscription<K, V> {
        let id = NEXT_LISTENER.fetch_add(1, AtomicOrdering::Relaxed);
        self.state
            .borrow_mut()
            .listeners
            .push((id, Rc::new(listener)));
        Subscription {
            state: Rc::downgrade(&self.state),
            id,
        }
    }

    pub fn query(&self, query: &Query<V>) -> Vec<(K, V)> {
        let mut rows = self.state.borrow().all_visible();
        if let Some(filter) = &query.filter {
            rows.retain(|(_, value)| filter(value));
        }
        if let Some(order_by) = &query.order_by {
            rows.sort_by(|left, right| order_by(&left.1, &right.1));
        }
        if let Some(limit) = query.limit {
            rows.truncate(limit);
        }
        if let Some(token) = &query.demand {
            let request = {
                let mut state = self.state.borrow_mut();
                let incarnation = state.replica.incarnation();
                if state.closed
                    || state
                        .query_demands
                        .get(token)
                        .is_some_and(|(epoch, _)| *epoch == incarnation)
                {
                    None
                } else {
                    let id = mint_demand();
                    let previous = state.query_demands.insert(token.clone(), (incarnation, id));
                    Some((id, previous))
                }
            };
            if let Some((id, previous)) = request {
                if let Some((_, previous)) = previous {
                    self.release(previous);
                }
                self.request(token.clone(), id);
            }
        }
        rows
    }

    pub fn query_coverage(&self, query: &Query<V>) -> Coverage {
        let state = self.state.borrow();
        let request = query
            .demand
            .as_ref()
            .and_then(|token| state.query_demands.get(token));
        match request {
            Some((epoch, id)) if *epoch == state.replica.incarnation() && !state.closed => {
                if state.covered.contains(id) {
                    Coverage::Complete
                } else {
                    Coverage::Partial
                }
            }
            _ => Coverage::Unknown,
        }
    }

    pub fn release_query(&self, query: &Query<V>) {
        let request = query
            .demand
            .as_ref()
            .and_then(|token| self.state.borrow_mut().query_demands.remove(token));
        if let Some((_, id)) = request {
            self.release(id);
        }
    }

    /// Issue demand upstream. The id is minted here and travels unchanged, so coverage
    /// reported by the far end resolves this view's own lease. An authority answers with
    /// everything it holds: a registry is complete by construction, so the token's content
    /// does not narrow the answer — a computed authority would have to interpret it.
    ///
    /// A consumer never answers its own demand. Coverage arrives in a batch from the far
    /// end, so an unwired or unanswered demand stays visibly uncovered instead of reporting
    /// a satisfied lease over an empty replica.
    pub fn demand(&self, token: DemandToken) -> DemandId {
        let id = mint_demand();
        self.request(token, id);
        id
    }

    fn request(&self, token: DemandToken, id: DemandId) {
        let upstream = {
            let mut state = self.state.borrow_mut();
            if state.closed {
                return;
            }
            assert!(state.demands.insert(id), "duplicate demand");
            state.upstream.clone()
        };
        match upstream {
            UpstreamSlot::Consumer(upstream) => upstream.demand(token, id),
            UpstreamSlot::Authority => self.bootstrap(id),
        }
    }

    /// Ending demand stops delivery upstream. It does not evict rows: retention belongs to
    /// the replica's own policy, so unmounting a view cannot pull data that another view is
    /// about to ask for.
    pub fn release(&self, id: DemandId) {
        let upstream = {
            let mut state = self.state.borrow_mut();
            if !state.demands.remove(&id) {
                return;
            }
            state.covered.remove(&id);
            state.query_demands.retain(|_, (_, request)| *request != id);
            state.upstream.clone()
        };
        if let UpstreamSlot::Consumer(upstream) = upstream {
            upstream.release(id);
        }
    }

    fn bootstrap(&self, id: DemandId) {
        let state = self.state.borrow();
        let incarnation = state.replica.incarnation();
        let base = state
            .replica
            .rows()
            .map(|(key, row)| BaseDelta::Upsert {
                key: key.clone(),
                version: row.version,
                value: row.value.clone(),
            })
            .collect();
        drop(state);
        dispatch(
            &self.state,
            Batch {
                incarnation,
                base,
                overlays: Vec::new(),
                settled: Vec::new(),
                covered: vec![id],
            },
            false,
        );
    }
}

impl<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static> View<K, V, Writable> {
    /// The upstream→consumer half of the waist: fold one batch into this hop and publish
    /// the resulting transition to listeners and downstream relays.
    pub fn apply(&self, batch: Batch<K, V>) -> Event<K, V> {
        dispatch(&self.state, batch, false)
    }

    /// Submit a whole-row write. The origin always records its own prediction, regardless of
    /// policy — policy only decides whether the other hops in the chain remember it too —
    /// and the batch is applied before the intent goes upstream, so an authority that
    /// answers synchronously still finds the overlay parked when its echo lands.
    pub fn write(&self, key: K, value: Option<V>, writer: ScopeId) -> OperationId {
        assert!(!self.is_closed(), "write on a closed view");
        let id = mint_operation();
        let upstream = self.state.borrow().upstream.clone();
        match upstream {
            UpstreamSlot::Consumer(upstream) => {
                self.apply_origin(Batch {
                    incarnation: self.incarnation(),
                    base: Vec::new(),
                    overlays: vec![OverlayDelta::Put {
                        id,
                        key: key.clone(),
                        value: value.clone(),
                        writer,
                    }],
                    settled: Vec::new(),
                    covered: Vec::new(),
                });
                upstream.write(id, key, value, writer);
            }
            UpstreamSlot::Authority => self.authoritative_write(id, key, value, writer),
        }
        id
    }

    /// Withdraw everything one scope wrote, plus every base row it contributed when this
    /// view is the authority. One batch, so a teardown is a single visible transition.
    ///
    /// A consumer never removes base rows: it did not author them, and dropping them
    /// locally would show the collection empty until the authority happened to speak
    /// again. Only overlays the scope itself predicted are withdrawn there.
    pub fn dispose_scope(&self, scope: ScopeId) -> Event<K, V> {
        let mut state = self.state.borrow_mut();
        let withdrawn = state.ledger.ids_for_scope(scope);
        let removed = match &state.upstream {
            UpstreamSlot::Authority => state.replica.plan_remove_owner(scope),
            UpstreamSlot::Consumer(_) => Vec::new(),
        };
        let incarnation = state.replica.incarnation();
        drop(state);
        self.apply(Batch {
            incarnation,
            base: removed,
            overlays: withdrawn
                .into_iter()
                .map(|id| OverlayDelta::Clear { id })
                .collect(),
            settled: Vec::new(),
            covered: Vec::new(),
        })
    }

    /// The origin's own write: recorded here no matter what the policy says about hops
    /// that merely pass the write through.
    fn apply_origin(&self, batch: Batch<K, V>) -> Event<K, V> {
        dispatch(&self.state, batch, true)
    }

    /// Terminal write: this view is the authority, so there is nothing to wait for — the
    /// row and the evidence that the origin's pending overlay may retire travel together.
    fn authoritative_write(&self, id: OperationId, key: K, value: Option<V>, writer: ScopeId) {
        let mut state = self.state.borrow_mut();
        let incarnation = state.replica.incarnation();
        let (base, overlays, version) = match value {
            Some(value) => {
                let delta = state.replica.plan_put(&key, Some(writer), value);
                let version = match &delta {
                    BaseDelta::Upsert { version, .. } => *version,
                    BaseDelta::Remove { .. } => unreachable!("plan_put returns an upsert"),
                };
                (vec![delta], Vec::new(), version)
            }
            None => {
                let delta = state.replica.plan_remove(&key);
                let version = match &delta {
                    BaseDelta::Remove { version, .. } => *version,
                    BaseDelta::Upsert { .. } => unreachable!("plan_remove returns a removal"),
                };
                (vec![delta], vec![OverlayDelta::Clear { id }], version)
            }
        };
        drop(state);
        self.apply(Batch {
            incarnation,
            base,
            overlays,
            settled: vec![(id, WriteOutcome::Durable { version })],
            covered: Vec::new(),
        });
    }
}

fn close<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static>(
    state: &Rc<RefCell<State<K, V>>>,
) {
    let (event, listeners, relays, upstream, demands) = {
        let mut state = state.borrow_mut();
        if state.closed {
            return;
        }
        state.closed = true;
        let incarnation = state.replica.incarnation();
        let changes = state
            .all_visible()
            .into_iter()
            .map(|(key, value)| RowChange {
                key,
                before: Some(value),
                after: None,
            })
            .collect();
        state.replica.reset(incarnation);
        state.ledger.clear();
        state.covered.clear();
        state.query_demands.clear();
        (
            Event {
                changes,
                forward: Batch::new(incarnation),
                reset: false,
                closed: true,
            },
            std::mem::take(&mut state.listeners),
            std::mem::take(&mut state.relays),
            state.upstream.clone(),
            std::mem::take(&mut state.demands),
        )
    };
    if let UpstreamSlot::Consumer(upstream) = upstream {
        for id in demands {
            upstream.release(id);
        }
    }
    notify(
        state,
        Notification {
            event: Rc::new(event),
            listeners,
            relays,
        },
    );
}

fn notify<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static>(
    state: &Rc<RefCell<State<K, V>>>,
    notification: Notification<K, V>,
) {
    {
        let mut state = state.borrow_mut();
        state.notifications.push_back(notification);
        if state.notifying {
            return;
        }
        state.notifying = true;
    }
    loop {
        let notification = {
            let mut state = state.borrow_mut();
            let Some(notification) = state.notifications.pop_front() else {
                state.notifying = false;
                return;
            };
            notification
        };
        let event = notification.event;
        if event.closed {
            for (_, relay) in notification.relays {
                if let Some(downstream) = relay.upgrade() {
                    close(&downstream);
                }
            }
            for (_, listener) in notification.listeners {
                listener(&event);
            }
            continue;
        }
        if !event.changes.is_empty() || event.reset || !event.forward.covered.is_empty() {
            for (_, listener) in notification.listeners {
                listener(&event);
            }
        }
        if event.reset || !event.forward.is_empty() {
            for (_, relay) in notification.relays {
                if let Some(downstream) = relay.upgrade() {
                    dispatch(&downstream, event.forward.clone(), false);
                }
            }
        }
    }
}

/// Apply one batch and publish it, releasing the borrow before any subscriber runs so a
/// relay can apply into its own state without re-entering this one.
///
/// Listeners see flattened row changes; relays see the layer-kept delta set. The two are
/// gated independently: a batch that only settles or covers demand still has to travel
/// downstream even though no row visibly moved.
fn dispatch<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static>(
    state: &Rc<RefCell<State<K, V>>>,
    batch: Batch<K, V>,
    origin: bool,
) -> Event<K, V> {
    let notification = {
        let mut state = state.borrow_mut();
        let event = apply(&mut state, batch, origin);
        if event.is_empty() {
            return event;
        }
        state.relays.retain(|(_, relay)| relay.strong_count() > 0);
        Notification {
            event: Rc::new(event),
            listeners: state
                .listeners
                .iter()
                .map(|(id, listener)| (*id, Rc::clone(listener)))
                .collect(),
            relays: state.relays.clone(),
        }
    };
    let event = Rc::clone(&notification.event);
    notify(state, notification);
    Rc::unwrap_or_clone(event)
}

fn apply<K: Ord + Clone, V: Clone + PartialEq>(
    state: &mut State<K, V>,
    batch: Batch<K, V>,
    origin: bool,
) -> Event<K, V> {
    if state.closed || batch.incarnation < state.replica.incarnation() {
        return Event::empty();
    }

    let reset = batch.incarnation > state.replica.incarnation();
    let mut candidates: BTreeSet<K> = if reset {
        state
            .replica
            .rows()
            .map(|(key, _)| key.clone())
            .chain(state.ledger.keys().cloned())
            .collect()
    } else {
        BTreeSet::new()
    };
    candidates.extend(batch.base.iter().map(|delta| match delta {
        BaseDelta::Upsert { key, .. } => key.clone(),
        BaseDelta::Remove { key, .. } => key.clone(),
    }));
    candidates.extend(batch.overlays.iter().filter_map(|delta| match delta {
        OverlayDelta::Put { key, .. } => Some(key.clone()),
        OverlayDelta::Clear { id } => state.ledger.get(*id).map(|overlay| overlay.key.clone()),
    }));
    for (id, _) in &batch.settled {
        if let Some(overlay) = state.ledger.get(*id) {
            candidates.insert(overlay.key.clone());
        }
    }

    let before: Vec<(K, Option<V>)> = candidates
        .iter()
        .map(|key| (key.clone(), state.visible(key)))
        .collect();

    if batch.incarnation > state.replica.incarnation() {
        state.replica.reset(batch.incarnation);
        state.ledger.clear();
        state.covered.clear();
    }
    let incarnation = state.replica.incarnation();

    let mut forward = Batch::new(incarnation);
    forward.base = batch
        .base
        .iter()
        .filter(|delta| match delta {
            BaseDelta::Upsert { version, .. } | BaseDelta::Remove { version, .. } => {
                version.incarnation == incarnation
            }
        })
        .cloned()
        .collect();
    state.replica.apply(incarnation, &forward.base);

    let record_puts = origin || state.ledger.policy() == OverlayPolicy::Replicated;
    forward.overlays = state.ledger.apply_incoming(&batch.overlays, record_puts);
    forward.settled = batch.settled;
    for (id, outcome) in &forward.settled {
        state.ledger.settle(*id, outcome.clone());
    }
    state
        .ledger
        .retire_satisfied(&candidates, |key| state.replica.version(key));

    forward.covered = batch
        .covered
        .into_iter()
        .filter(|id| state.demands.contains(id) && state.covered.insert(*id))
        .collect();

    if reset {
        for (epoch, id) in state.query_demands.values_mut() {
            if state.covered.contains(id) {
                *epoch = incarnation;
            }
        }
    }

    let changes = before
        .into_iter()
        .filter_map(|(key, was)| {
            let now = state.visible(&key);
            (was != now).then_some(RowChange {
                key,
                before: was,
                after: now,
            })
        })
        .collect();

    Event {
        changes,
        forward,
        reset,
        closed: false,
    }
}

/// A writable hop is itself a valid upstream: this is what makes a chain of hops rather
/// than a two-party client/server. A read-only hop cannot serve as one, because it has no
/// write path to accept the intent it would have to relay.
impl<K: Ord + Clone + 'static, V: Clone + PartialEq + 'static> Upstream<K, V>
    for View<K, V, Writable>
{
    fn attach(&self, downstream: &Downstream<K, V>) {
        if self.is_closed() {
            downstream.close();
            return;
        }
        let id = NEXT_LISTENER.fetch_add(1, AtomicOrdering::Relaxed);
        self.state
            .borrow_mut()
            .relays
            .push((id, downstream.state.clone()));
    }

    fn demand(&self, token: DemandToken, id: DemandId) {
        self.request(token, id);
    }

    fn release(&self, id: DemandId) {
        View::release(self, id);
    }

    fn write(&self, id: OperationId, key: K, value: Option<V>, writer: ScopeId) {
        assert!(!self.is_closed(), "write on a closed upstream");
        let upstream = self.state.borrow().upstream.clone();
        match upstream {
            UpstreamSlot::Consumer(upstream) => {
                // Record the pass-through intent as one transition: the batch carries the
                // Put into this hop's own ledger (gated by policy) and out to its relays
                // in the same apply, so siblings see the pending row and this hop's own
                // listeners see it appear rather than finding it already there. A `Local`
                // hop skips the apply entirely — it forwards the intent upward and shows
                // nothing until the authority echoes the committed row.
                if self.policy() == OverlayPolicy::Replicated {
                    let mut batch = Batch::new(self.incarnation());
                    batch.overlays = vec![OverlayDelta::Put {
                        id,
                        key: key.clone(),
                        value: value.clone(),
                        writer,
                    }];
                    self.apply(batch);
                }
                upstream.write(id, key, value, writer);
            }
            UpstreamSlot::Authority => self.authoritative_write(id, key, value, writer),
        }
    }
}
