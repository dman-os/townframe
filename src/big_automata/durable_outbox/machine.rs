//! The outbox machine: the in-memory index of every live want, and the decisions of what to
//! fetch from whom and when. Sans-IO: rows, connections and call outcomes go in, commands
//! come out of [`Outbox::flush`]. The laws are in the [parent module](super).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::interlude::*;
use crate::kit::scheduler::Instant;
use crate::kit::scheduler::TaskId;
use crate::kit::scheduler::keyed::KeyedScheduler;

use super::Version;
use super::Want;

/// A fetch's id: one per start, never reused.
pub type CallId = TaskId;

#[derive(Debug, Clone)]
pub struct Config {
    /// `C`: fetches running or stopping at once.
    pub max_fetches: usize,
    /// `M`: fetches running or stopping plus landings in flight, at least `max_fetches`.
    pub max_in_flight: usize,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    /// How much waiting one priority level is worth (law 7). `Duration::MAX` makes priority
    /// strict.
    pub aging: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_fetches: 4,
            max_in_flight: 16,
            min_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(60),
            aging: Duration::from_secs(60),
        }
    }
}

/// One fetch to run: the object's content, at the version and identity recorded, from one
/// provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub obj: ObjKey,
    pub version: Version,
    pub identity: ByteKey,
    pub provider: PeerKey,
}

/// What the backend's fetch left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fetched {
    /// The content is durable. The want row is still there: the driver lands it.
    Durable,
    /// The content is durable and the backend, writing it in the want table's own
    /// transaction, already landed the want (law 12); `deleted` is what that landing answered.
    Landed { deleted: bool },
}

/// A fetch's terminal event. Every call reports exactly once; a cancelled call may report its
/// real result if it raced the cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    Fetched(Fetched),
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Fetch {
        call: CallId,
        job: Job,
    },
    /// Abort the call. It still reports (as `Cancelled`, or its real result), and counts
    /// against `C` and `M` until it does.
    Cancel {
        call: CallId,
    },
    /// Run the landing of law 12 to its end: never cancelled.
    Land {
        obj: ObjKey,
        version: Version,
    },
}

/// What a want is doing, for the laws' checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Eligible to start, or blocked on a slot.
    Ready,
    /// Waiting out a failure backoff.
    Backoff,
    /// No connected provider has not been tried.
    Parked,
    /// An earlier fetch of the object is still ending.
    Draining,
    Fetching(CallId),
    Landing,
    /// Landed without deleting: waiting for the stream to show what replaced the row.
    Stale,
}

#[derive(Debug)]
enum State {
    Idle,
    Fetching { task: CallId, provider: PeerKey },
    Landing(Version),
    Stale,
}

#[derive(Debug)]
struct Entry {
    want: Want,
    state: State,
    /// Providers failed since the last backoff.
    tried: BTreeSet<PeerKey>,
    /// Backoffs armed since the want's version last changed.
    rounds: u32,
    backoff_until: Option<Instant>,
    /// Where the want sits in `ready`.
    ready_key: Option<i128>,
}

#[derive(Debug)]
pub struct Outbox {
    config: Config,
    index: BTreeMap<ObjKey, Entry>,
    /// Every peer advertising a want, to its objects: a provider connecting or leaving finds
    /// the wants it touches without a scan.
    advertised_by: BTreeMap<PeerKey, BTreeSet<ObjKey>>,
    connected: BTreeSet<PeerKey>,
    /// The wants that may start now, best first.
    ready: BTreeSet<(i128, ObjKey)>,
    timers: BTreeSet<(Instant, ObjKey)>,
    /// One task per object, which is the object's fetch: running, or stopping.
    fetches: KeyedScheduler<ObjKey, Job>,
    /// Cancelled fetches the driver has yet to report.
    stopping: BTreeMap<CallId, ObjKey>,
    landing: BTreeSet<(ObjKey, Version)>,
    /// Lands decided by an input, in order, ahead of the next flush's commands.
    pending: Vec<Command>,
}

impl Outbox {
    pub fn new(config: Config) -> Self {
        assert!(config.max_fetches >= 1, "at least one fetch");
        assert!(
            config.max_in_flight >= config.max_fetches,
            "M must allow C fetches"
        );
        Self {
            config,
            index: default(),
            advertised_by: default(),
            connected: default(),
            ready: default(),
            timers: default(),
            fetches: default(),
            stopping: default(),
            landing: default(),
            pending: default(),
        }
    }

    // ── inputs ──────────────────────────────────────────────────────────────────────────────

    /// Rows the stream delivered, `None` for a tombstone, in order.
    pub fn rows(&mut self, rows: impl IntoIterator<Item = (ObjKey, Option<Want>)>) {
        for (obj, row) in rows {
            match row {
                None => self.forget(&obj),
                Some(want) => self.observe(obj, want),
            }
        }
    }

    pub fn peer_connected(&mut self, peer: PeerKey) {
        if self.connected.insert(peer.clone()) {
            self.refresh_advertised(&peer);
        }
    }

    /// The fetches running against the peer are cancelled and their wants start again against
    /// another provider; the cancel is not a failure (law 9).
    pub fn peer_disconnected(&mut self, peer: &PeerKey) {
        if !self.connected.remove(peer) {
            return;
        }
        for obj in self.advertised_by.get(peer).cloned().unwrap_or_default() {
            let against_peer = matches!(
                self.index.get(&obj).map(|entry| &entry.state),
                Some(State::Fetching { provider, .. }) if provider == peer
            );
            if against_peer {
                self.cancel_fetch(&obj);
            }
            self.refresh(&obj);
        }
    }

    /// A fetch's terminal event, matched by call id (law 5).
    pub fn fetch_ended(&mut self, now: Instant, call: CallId, ended: Ended) {
        if let Some(obj) = self.stopping.remove(&call) {
            // A result the want gave up on, whatever it is, is stale: the key frees on the
            // driver's report alone.
            let replacement = self.fetches.stopped(now, call);
            assert!(replacement.is_none(), "{ERROR_IMPOSSIBLE}");
            self.refresh(&obj);
            return;
        }
        assert!(
            ended != Ended::Cancelled,
            "call {call} reported cancelled but was never cancelled"
        );
        let obj = self
            .fetches
            .key_of(call)
            .cloned()
            .unwrap_or_else(|| panic!("terminal event for unknown call {call}"));
        let (_, job) = self.fetches.complete(call).expect(ERROR_IMPOSSIBLE);
        let entry = self.index.get_mut(&obj).expect(ERROR_IMPOSSIBLE);
        assert!(
            matches!(entry.state, State::Fetching { task, .. } if task == call),
            "{ERROR_IMPOSSIBLE}"
        );
        entry.state = State::Idle;
        match ended {
            Ended::Fetched(Fetched::Durable) => {
                entry.state = State::Landing(job.version);
                self.landing.insert((obj.clone(), job.version));
                self.pending.push(Command::Land {
                    obj: obj.clone(),
                    version: job.version,
                });
            }
            Ended::Fetched(Fetched::Landed { deleted }) => {
                self.landing.insert((obj.clone(), job.version));
                entry.state = State::Landing(job.version);
                self.landed(&obj, job.version, deleted);
            }
            Ended::Failed => self.failed(now, &obj, job.provider),
            Ended::Cancelled => unreachable!("{ERROR_IMPOSSIBLE}"),
        }
        self.refresh(&obj);
    }

    /// A landing's answer: whether it deleted the row.
    pub fn landed(&mut self, obj: &ObjKey, version: Version, deleted: bool) {
        assert!(
            self.landing.remove(&(obj.clone(), version)),
            "landed answers a landing never issued"
        );
        let Some(entry) = self.index.get_mut(obj) else {
            return;
        };
        // a landing of an older version than the one now landing has nothing left to say
        if !matches!(entry.state, State::Landing(landing) if landing == version) {
            return;
        }
        if entry.want.version != version {
            // a newer row was already seen: it needs its own fetch
            entry.state = State::Idle;
        } else if deleted {
            self.remove(obj);
            return;
        } else {
            entry.state = State::Stale;
        }
        self.refresh(obj);
    }

    /// Fire the backoffs that are due, start what may start, and hand the driver its commands.
    pub fn flush(&mut self, now: Instant, out: &mut Vec<Command>) {
        while let Some((due, obj)) = self.timers.first().cloned() {
            if due > now {
                break;
            }
            self.timers.pop_first();
            self.index
                .get_mut(&obj)
                .expect(ERROR_IMPOSSIBLE)
                .backoff_until = None;
            self.refresh(&obj);
        }
        while self.has_room() {
            let Some((_, obj)) = self.ready.pop_first() else {
                break;
            };
            self.start(now, &obj);
        }
        out.append(&mut self.pending);
        out.extend(
            self.fetches
                .drain_stop_queue()
                .map(|call| Command::Cancel { call }),
        );
        out.extend(self.fetches.drain_spawn_queue().map(|task| Command::Fetch {
            call: task.id,
            job: task.seed,
        }));
        #[cfg(test)]
        self.check();
    }

    /// When the driver should flush next to fire a backoff.
    pub fn next_due(&self) -> Option<Instant> {
        self.timers.first().map(|(due, _)| *due)
    }

    // ── views ───────────────────────────────────────────────────────────────────────────────

    /// How many wants the index holds.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// The version the index holds for the object.
    pub fn version_of(&self, obj: &ObjKey) -> Option<Version> {
        self.index.get(obj).map(|entry| entry.want.version)
    }

    /// Fetches running or stopping: the slots `C` bounds.
    pub fn fetching(&self) -> usize {
        self.fetches.counts().live + self.fetches.stopping()
    }

    /// Landings in flight.
    pub fn landings(&self) -> usize {
        self.landing.len()
    }

    /// Failure rounds the want has been through: a poison signal (law 10).
    pub fn rounds_of(&self, obj: &ObjKey) -> Option<u32> {
        self.index.get(obj).map(|entry| entry.rounds)
    }

    pub fn phase_of(&self, obj: &ObjKey) -> Option<Phase> {
        let entry = self.index.get(obj)?;
        Some(match &entry.state {
            State::Fetching { task, .. } => Phase::Fetching(*task),
            State::Landing(_) => Phase::Landing,
            State::Stale => Phase::Stale,
            State::Idle if self.busy(obj) => Phase::Draining,
            State::Idle if entry.backoff_until.is_some() => Phase::Backoff,
            State::Idle if Self::pick(&self.connected, entry).is_none() => Phase::Parked,
            State::Idle => Phase::Ready,
        })
    }

    // ── decisions ───────────────────────────────────────────────────────────────────────────

    /// A tombstone, or a withdrawal seen late: the want is gone, and so is its fetch (law 11).
    fn forget(&mut self, obj: &ObjKey) {
        if self.index.contains_key(obj) {
            self.cancel_fetch(obj);
            self.remove(obj);
        }
    }

    fn observe(&mut self, obj: ObjKey, want: Want) {
        let Some(entry) = self.index.get_mut(&obj) else {
            for peer in &want.providers {
                self.advertised_by
                    .entry(peer.clone())
                    .or_default()
                    .insert(obj.clone());
            }
            self.index.insert(
                obj.clone(),
                Entry {
                    want,
                    state: State::Idle,
                    tried: default(),
                    rounds: 0,
                    backoff_until: None,
                    ready_key: None,
                },
            );
            self.refresh(&obj);
            return;
        };
        let superseded = entry.want.version != want.version;
        let old_providers = std::mem::replace(&mut entry.want, want).providers;
        let new_providers = entry.want.providers.clone();
        if superseded {
            // a new identity is a new want: its failures and backoff belong to the old one
            entry.tried.clear();
            entry.rounds = 0;
            if let Some(until) = entry.backoff_until.take() {
                self.timers.remove(&(until, obj.clone()));
            }
            match entry.state {
                State::Fetching { .. } => self.cancel_fetch(&obj),
                State::Stale => self.entry_mut(&obj).state = State::Idle,
                // a landing of the old version ends by itself and finds a newer row
                State::Idle | State::Landing(_) => {}
            }
        }
        self.reindex_providers(&obj, &old_providers, &new_providers);
        self.refresh(&obj);
    }

    /// A failed fetch from `provider`: the next provider at once, else a backoff (law 10).
    fn failed(&mut self, now: Instant, obj: &ObjKey, provider: PeerKey) {
        tracing::warn!(%obj, %provider, "outbox fetch failed");
        let entry = self.index.get_mut(obj).expect(ERROR_IMPOSSIBLE);
        entry.tried.insert(provider);
        if Self::pick(&self.connected, entry).is_some() {
            return;
        }
        entry.rounds += 1;
        entry.tried.clear();
        let until = now + Self::backoff(&self.config, entry.rounds);
        entry.backoff_until = Some(until);
        self.timers.insert((until, obj.clone()));
    }

    /// `min * 2^(rounds - 1)`, capped.
    fn backoff(config: &Config, rounds: u32) -> Duration {
        config
            .min_backoff
            .saturating_mul(1u32 << (rounds - 1).min(20))
            .min(config.max_backoff)
    }

    fn start(&mut self, now: Instant, obj: &ObjKey) {
        let entry = self.index.get_mut(obj).expect(ERROR_IMPOSSIBLE);
        entry.ready_key = None;
        let provider = Self::pick(&self.connected, entry)
            .expect("a ready want has a provider")
            .clone();
        let job = Job {
            obj: obj.clone(),
            version: entry.want.version,
            identity: entry.want.identity.clone(),
            provider: provider.clone(),
        };
        let task = self
            .fetches
            .replace(now, obj.clone(), job)
            .expect("a ready want's key is idle");
        entry.state = State::Fetching { task, provider };
    }

    /// Cancel the want's fetch, if it has one, and leave the want idle. The object's key stays
    /// busy until the driver reports the call ended (law 5).
    fn cancel_fetch(&mut self, obj: &ObjKey) {
        let entry = self.index.get_mut(obj).expect(ERROR_IMPOSSIBLE);
        if !matches!(entry.state, State::Fetching { .. }) {
            return;
        }
        entry.state = State::Idle;
        let task = self.fetches.cancel(obj).expect(ERROR_IMPOSSIBLE);
        // every call is drained at the flush that issues it, so a cancelled one always runs on
        assert_eq!(self.fetches.stopping_task(obj), Some(task));
        self.stopping.insert(task, obj.clone());
    }

    fn has_room(&self) -> bool {
        let fetching = self.fetching();
        fetching < self.config.max_fetches
            && fetching + self.landing.len() < self.config.max_in_flight
    }

    /// The provider the next fetch goes to: the first connected advertiser not tried since
    /// the last backoff.
    fn pick<'e>(connected: &BTreeSet<PeerKey>, entry: &'e Entry) -> Option<&'e PeerKey> {
        entry
            .want
            .providers
            .iter()
            .find(|peer| connected.contains(*peer) && !entry.tried.contains(*peer))
    }

    /// Whether the object's key is held by a running or stopping fetch.
    fn busy(&self, obj: &ObjKey) -> bool {
        self.fetches.active_task(obj).is_some() || self.fetches.stopping_task(obj).is_some()
    }

    /// The order of law 7, smallest first: `queued_at` less the aging a priority level earns.
    fn order(&self, want: &Want) -> i128 {
        let aging = i128::try_from(self.config.aging.as_millis()).unwrap_or(i128::MAX / 512);
        i128::from(want.queued_at) - i128::from(want.priority) * aging
    }

    /// Recompute whether the want may start, and where it sits in `ready`.
    fn refresh(&mut self, obj: &ObjKey) {
        let Some(entry) = self.index.get(obj) else {
            return;
        };
        let eligible = matches!(entry.state, State::Idle)
            && entry.backoff_until.is_none()
            && !self.busy(obj)
            && Self::pick(&self.connected, entry).is_some();
        let key = eligible.then(|| self.order(&entry.want));
        let entry = self.index.get_mut(obj).expect(ERROR_IMPOSSIBLE);
        if entry.ready_key == key {
            return;
        }
        if let Some(old) = std::mem::replace(&mut entry.ready_key, key) {
            self.ready.remove(&(old, obj.clone()));
        }
        if let Some(key) = key {
            self.ready.insert((key, obj.clone()));
        }
    }

    fn refresh_advertised(&mut self, peer: &PeerKey) {
        for obj in self.advertised_by.get(peer).cloned().unwrap_or_default() {
            self.refresh(&obj);
        }
    }

    fn reindex_providers(
        &mut self,
        obj: &ObjKey,
        old: &BTreeSet<PeerKey>,
        new: &BTreeSet<PeerKey>,
    ) {
        for gone in old.difference(new) {
            self.unadvertise(gone, obj);
        }
        for added in new.difference(old) {
            self.advertised_by
                .entry(added.clone())
                .or_default()
                .insert(obj.clone());
        }
    }

    fn unadvertise(&mut self, peer: &PeerKey, obj: &ObjKey) {
        let objs = self.advertised_by.get_mut(peer).expect(ERROR_IMPOSSIBLE);
        objs.remove(obj);
        if objs.is_empty() {
            self.advertised_by.remove(peer);
        }
    }

    /// Drop the want from every index. Its fetch, if any, is already cancelled.
    fn remove(&mut self, obj: &ObjKey) {
        let entry = self.index.remove(obj).expect(ERROR_IMPOSSIBLE);
        debug_assert!(!matches!(entry.state, State::Fetching { .. }));
        if let Some(key) = entry.ready_key {
            self.ready.remove(&(key, obj.clone()));
        }
        if let Some(until) = entry.backoff_until {
            self.timers.remove(&(until, obj.clone()));
        }
        for peer in &entry.want.providers {
            self.unadvertise(peer, obj);
        }
    }

    fn entry_mut(&mut self, obj: &ObjKey) -> &mut Entry {
        self.index.get_mut(obj).expect(ERROR_IMPOSSIBLE)
    }

    /// The indexes recomputed from the wants: what law 4 says they are. Test builds assert it
    /// at the end of every flush.
    #[cfg(test)]
    fn check(&self) {
        let mut ready = BTreeSet::new();
        let mut advertised = BTreeMap::<PeerKey, BTreeSet<ObjKey>>::new();
        let mut timers = BTreeSet::new();
        let mut fetching = 0;
        for (obj, entry) in &self.index {
            for peer in &entry.want.providers {
                advertised
                    .entry(peer.clone())
                    .or_default()
                    .insert(obj.clone());
            }
            if let Some(until) = entry.backoff_until {
                timers.insert((until, obj.clone()));
            }
            let eligible = matches!(entry.state, State::Idle)
                && entry.backoff_until.is_none()
                && !self.busy(obj)
                && Self::pick(&self.connected, entry).is_some();
            if eligible {
                ready.insert((self.order(&entry.want), obj.clone()));
            }
            if let State::Fetching { task, .. } = entry.state {
                fetching += 1;
                assert_eq!(self.fetches.active_task(obj), Some(task));
            }
        }
        assert_eq!(self.ready, ready, "the ready set");
        assert_eq!(self.advertised_by, advertised, "the provider index");
        assert_eq!(self.timers, timers, "the backoff timers");
        assert_eq!(
            self.fetching(),
            fetching + self.stopping.len(),
            "slots are running plus stopping fetches"
        );
        assert!(self.fetching() <= self.config.max_fetches);
        assert!(self.fetching() + self.landing.len() <= self.config.max_in_flight);
    }
}
