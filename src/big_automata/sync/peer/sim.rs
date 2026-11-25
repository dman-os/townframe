//! The whole-machine simulation of [`PeerSync`]: the machine, a fake backend over a local
//! store, a peer whose part store is the ground truth, and a network between them, in one
//! deterministic test. The laws are in the [parent module](crate::sync); the unit-level law
//! tests are in `tests/`.
//!
//! # What is modelled
//!
//! - **The peer** ([`remote`]): a memory part store with the replay server and responder
//!   answering sessions and rounds, and summaries, bucket lists and leaves read from the store
//!   for our principal. A twin oracle snapshots the membership we may read after every commit,
//!   keyed by rev.
//! - **The local side** ([`local`]): a memory part store behind a fake backend. `record`
//!   applies a job's adds as pending memberships and its removals, and answers `Done` (which
//!   also copies the payload) or `Fetch` by a seeded draw; a fetch copies the payload after a
//!   seeded number of progress steps, some very long, may fail, and honours `Cancel`, though a
//!   cancelled call may also race to completion. The cursors the machine persists go to a map
//!   that survives crashes. The driver's own work is here too: a bucket listing is filtered
//!   against the local part and relisted past pages that filter to nothing; leaves are filtered
//!   against the local objects.
//! - **The network** ([`net`]): every outward call and its answer is a message. Ops deliver
//!   any message (reordering, delay), drop one (the machine hears `Reply::Lost`), duplicate
//!   one, or tamper with a page (an event naming no target, an unrequested target, a row
//!   outside its target's window, an object-target removal). The server forgets its sessions
//!   on a sweep or a restart; a sweep and a restart lose what is in flight, as the replay
//!   simulation does (a message outliving the TTL revives a forgotten session, which replay
//!   law 5 documents).
//! - **Store generations**: the peer may lose its data (a new store and generation, empty), go
//!   back to a backup taken earlier (`Snapshot`, then `RestoreMinted`, which mints), or, in
//!   the scenarios, go back without a mint (`RestoreUnminted`). Each is a peer restart, so the
//!   client reconnects with its persisted cursors. A pull-only client keeps the members of the
//!   old log that the new one has no row for ("ghosts"), so the checks compare the local
//!   members with the peer's as a superset that holds nothing beyond the ghosts
//!   ([`remote::Remote::holds`]).
//! - **Time** is a virtual clock; the machine's backoffs and deadlines run on it.
//! - **Crashes** keep the local store and the persisted cursors, and start a fresh machine
//!   with a fresh session; the embedder re-adds its wanted routes with the persisted cursors.
//!   Each call the backend had in flight either committed before the crash or never started.
//!
//! # What is not modelled
//!
//! - Bucket walks run at working level 0 only (one root bucket), because the working level
//!   rises with the part's size and the universe is small. Walks are reached by making the
//!   peer's summaries claim a large dirty count ([`Op::Bias`]).
//! - Content is compared for object routes only; membership is compared for parts. A fetch
//!   copies the peer's payload as of its landing, as fetching the bytes would.
//! - The peer's own access control for fetched bytes; the transport's framing.
//!
//! # Checks
//!
//! After every step ([`checks`]): the machine's own law 13 recomputation (test builds assert it
//! at the end of every flush); the `Synced`/`Unsynced` edges alternate and agree with the
//! machine; in-flight backend calls are within the window, the fetch cap and one record and
//! one fetch per object; at most one round per lane, within the window; no `PersistCursor`
//! for a part while it is walking; persisted cursors never decrease and never pass the
//! peer's head; every persisted cursor is crash-safe (the local store holds whatever the peer
//! has at or below it). A tampered page changes nothing in the machine.
//!
//! At every `Synced`: the local members of the part equal the peer's members we may read as of
//! the cursor the part is at.
//!
//! After a quiet drain (reliable network, no writes, the backend completes): every wanted part
//! we may read is `Synced` at the peer's head, the local members equal the peer's, routed
//! objects have the peer's content, and nothing is in flight. "Quiet" is decided against the
//! peer's head and the machine's state now, never a flag an earlier message set. Every case
//! then crashes, restarts, and drains again.
//!
//! `SIM_TRACE=1` prints every command the machine issues and every answer it gets.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::executor::block_on;
use proptest::prelude::*;

use super::*;

use crate::fingerprint::FingerprintSeed;
use crate::part_store::Generation;
use crate::part_store::laws;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::replay::server::SESSION_TTL;
use crate::sync::bucket::FilteredBuckets;
use crate::sync::bucket::filter_buckets;
use crate::sync::bucket::filter_objects;

mod checks;
#[cfg(test)]
mod determinism;
mod driver;
mod local;
mod net;
mod ops;
mod remote;
#[cfg(test)]
mod scenarios;

use local::Flight;
use local::Local;
use local::Step;
use net::*;
use ops::MAX_BATCH;
use ops::Op;
#[cfg(test)]
use ops::run;
use remote::Remote;

/// The machine's configuration and the size of the universe a case draws from.
#[derive(Clone)]
struct Shape {
    pub parts: u8,
    pub objs: u8,
    /// Objects `0..obj_routes` may have object routes.
    pub obj_routes: u8,
    /// The most parts the peer's summary call takes.
    pub summary_cap: usize,
    pub config: Config,
}

impl Shape {
    /// About 3 parts, 4 objects, 2 object routes; a window of 3, so it binds constantly. The
    /// window must exceed twice `live_limit`: while a walk runs the rounds get half the room,
    /// and a held `Live` round takes `live_limit` of it first, so with less the `Bulk` lane
    /// is never served (a walk of a part the peer stopped serving retries for ever).
    fn small() -> Self {
        Self {
            parts: 3,
            objs: 4,
            obj_routes: 2,
            summary_cap: 2,
            config: Config {
                lifecycle: lifecycle::Config {
                    max_parts: 3,
                    min_backoff: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(4),
                },
                jobs: object_jobs::Config {
                    window: 3,
                    max_tracked: 5,
                    fetch_cap: 1,
                    min_backoff: Duration::from_secs(2),
                    max_backoff: Duration::from_secs(5),
                },
                lanes: lanes::Config {
                    hold_ms: 300,
                    live_limit: 1,
                    bulk_batch: 2,
                    bulk_linger: Duration::from_secs(1),
                    min_backoff: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(4),
                },
                strat: AllowedStrat::CursorAndBucket,
                min_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(4),
            },
        }
    }
}

struct Sim {
    shape: Shape,
    sync: PeerSync,
    remote: Remote,
    local: Local,
    net: Vec<Msg>,
    now: Instant,
    wanted_parts: BTreeSet<PartKey>,
    wanted_objs: BTreeSet<ObjKey>,
    /// The edges the driver saw, as the fact they leave.
    mirror: BTreeMap<PartKey, bool>,
    /// Parts whose walk may have read state newer than their cursor.
    walked: BTreeSet<PartKey>,
    /// The stage each listing call is at.
    relist: BTreeMap<CallId, BuckId>,
    session: u128,
    seeds: u64,
    /// Every round the machine sent: its lane and row limit.
    pub rounds: Vec<(Lane, u32)>,
    /// Every restart the machine reported (`PartLifecycle` law 10).
    pub restarts: Vec<Restart>,
    /// Parts restarted since their last persist: that persist may be below the stored cursor,
    /// even in the same generation.
    rewound: BTreeSet<PartKey>,
}

/// A part started over from cursor 0, as `PeerCommand::Restarted` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Restart {
    part: PartKey,
    held: PeerCursor,
    peer_generation: Generation,
    peer_latest: Rev,
}

impl Sim {
    fn new(shape: Shape, seed: u64) -> Self {
        let parts: Vec<PartKey> = (0..shape.parts).map(part).collect();
        Self {
            sync: PeerSync::new(shape.config.clone(), Uuid::from_u128(1)),
            remote: Remote::new(
                &parts,
                &(0..shape.objs).map(obj).collect::<Vec<_>>(),
                shape.summary_cap,
            ),
            local: Local::new(shape.objs, seed),
            shape,
            net: vec![],
            now: Instant::now(),
            wanted_parts: default(),
            wanted_objs: default(),
            mirror: default(),
            walked: default(),
            relist: default(),
            session: 1,
            seeds: 0,
            rounds: vec![],
            restarts: vec![],
            rewound: default(),
        }
    }

    /// An input of the machine, and the flush the driver owes after it.
    fn input(&mut self, input: impl FnOnce(&mut PeerSync)) {
        input(&mut self.sync);
        self.flush();
    }

    fn flush(&mut self) {
        let mut out = vec![];
        self.sync.flush(self.now, &mut out);
        self.process(out);
        self.check_step();
    }

    fn send(&mut self, call: CallId, req: Req) {
        self.net.push(Msg {
            call,
            body: Body::Req(req),
            hostile: None,
        });
    }

    /// The driver's side of the machine's commands.
    fn process(&mut self, out: Vec<PeerCommand>) {
        if std::env::var("SIM_TRACE").is_ok() {
            for cmd in &out {
                eprintln!("TRACE {cmd:?}");
            }
        }
        let mut synced = vec![];
        for cmd in out {
            match cmd {
                PeerCommand::Summary { call, parts } => self.send(call, Req::Summary(parts)),
                PeerCommand::Session { call, update } => self.send(call, Req::Session(update)),
                PeerCommand::Round { call, lane, page } => {
                    self.rounds.push((lane, page.limit));
                    self.send(call, Req::Round(page));
                }
                PeerCommand::ListBuckets {
                    call,
                    part_key,
                    offset,
                    since,
                    working_level,
                } => {
                    self.relist.insert(call, offset);
                    let req = Req::List {
                        part: part_key,
                        offset,
                        since,
                        level: working_level,
                    };
                    self.send(call, req);
                }
                PeerCommand::LeafBuckets {
                    call,
                    part_key,
                    since,
                    buckets,
                } => {
                    self.seeds += 1;
                    let seed = FingerprintSeed::new(self.seeds, !self.seeds);
                    let req = Req::Leaf {
                        part: part_key,
                        since,
                        buckets,
                        seed,
                    };
                    self.send(call, req);
                }
                PeerCommand::Record { call, job } => self.local.issue_record(call, job),
                PeerCommand::Fetch { call, job } => self.local.issue_fetch(call, job),
                // only backend calls are cancelled while the machine lives; a disconnect
                // is checked on its own
                PeerCommand::Cancel { call } => {
                    if let Some(flight) = self.local.calls.get_mut(&call) {
                        flight.cancel();
                    }
                }
                PeerCommand::PersistCursor { part, cursor } => {
                    // the restart's write of cursor 0 (`PartLifecycle` laws 7 and 10) is the
                    // one persist a walking part may make: 0 skips nothing
                    assert!(
                        cursor.rev == 0 || !self.sync.walking(&part),
                        "law 9: {part} persisted at {cursor:?} while it is walking"
                    );
                    // the table keeps the last write; the machine only lowers a cursor within
                    // a generation by restarting the part
                    if let Some(held) = self.local.persisted.get(&part)
                        && held.generation == cursor.generation
                        && !self.rewound.remove(&part)
                    {
                        assert!(
                            cursor.rev >= held.rev,
                            "{part} persisted at {cursor:?} after {held:?}"
                        );
                    }
                    self.rewound.remove(&part);
                    self.local.persisted.insert(part, cursor);
                }
                PeerCommand::Restarted {
                    part,
                    held,
                    peer_generation,
                    peer_latest,
                } => {
                    assert!(
                        held.generation != peer_generation || peer_latest < held.rev,
                        "{part} restarted though {held:?} is a position in the peer's log \
                         (generation {peer_generation}, latest {peer_latest})"
                    );
                    assert_eq!(
                        peer_generation,
                        self.remote.generation(),
                        "{part} restarted on a generation that is not the peer's"
                    );
                    self.rewound.insert(part.clone());
                    self.restarts.push(Restart {
                        part,
                        held,
                        peer_generation,
                        peer_latest,
                    });
                }
                PeerCommand::Synced { part } => {
                    assert!(
                        !self.mirror.get(&part).copied().unwrap_or(false),
                        "{part} reported Synced twice"
                    );
                    self.mirror.insert(part.clone(), true);
                    synced.push(part);
                }
                PeerCommand::Unsynced { part } => {
                    assert_eq!(
                        self.mirror.insert(part.clone(), false),
                        Some(true),
                        "{part} reported Unsynced without being Synced"
                    );
                }
            }
        }
        // judged after the whole flush: the machine's state is the one the edges describe
        for part in synced {
            self.check_synced(&part);
        }
    }

    // # The backend

    fn progress_call(&mut self, call: CallId, finish: bool) {
        let job_obj = self.local.calls[&call].job().obj.clone();
        let payload = self.remote.payload(&job_obj);
        match self.local.step(call, payload, finish) {
            Step::Running => {}
            Step::Record(outcome) => self.input(|s| s.record_ended(call, outcome)),
            Step::Fetch(outcome) => self.input(|s| s.fetch_ended(call, outcome)),
        }
    }
}
