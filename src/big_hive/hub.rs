//! The Hub machine: the one place that decides which documents are alive, who hears about what
//! changed, which documents catch up and when, and which sessions to which peers exist. It
//! composes the other machines and routes between them; it does not re-implement them. It
//! replaces the policy half of `big_repo`'s runtime2 hub (`hub.rs`), and none of its plumbing.
//!
//! # What it is, and what it is not
//!
//! Like every machine in this crate the Hub is sans-IO: no awaits, no clock reads, no IO, no
//! channels, no locks and no randomness. Every input is a method on [`Hub`]; everything the Hub
//! wants done leaves as a [`Command`] from the next [`Hub::flush`], which is also where the clock
//! is read. Its driver executes the commands and reports what happened through the inputs.
//!
//! The Hub does not hold the [Document machines](crate::document). It decides that one starts or
//! stops ([`Command::StartDocument`], [`Command::StopDocument`]) and tells it what it needs to
//! know ([`Command::ToDocument`]); the driver runs each Document in a loop of its own. It does
//! hold two machines of `big_automata`, whole: [`Interest`](big_automata::sessions::interest) and
//! [`Sessions`](big_automata::sessions::session), and wires them (see "Sessions and routes").
//! The Write coordinator is not inside it either: a plan names the documents it needs, and the
//! caller asks the Hub for them ([`Hub::demand`]) before it submits the plan.
//!
//! # The state machines
//!
//! The enums are private; their names are the vocabulary of the laws.
//!
//! **Document lifetime** (`Run`, per document): `Absent` or `Running { generation }`. A start
//! gives the machine a generation (handles carry it, so a handle of an earlier machine is
//! recognised) and a fresh automerge actor. A stop is a request ([`Command::StopDocument`]): the
//! Hub asks once the Document has reported itself evictable and nothing wants it, and the driver
//! grants it only if the machine is evictable *when the request reaches it*, because a status
//! the Hub holds can be out of date (the coordinator and the handles reach a Document without
//! the Hub). The document is `Stopping` until the answer; a refusal leaves it running, and a
//! document wanted meanwhile starts again, as a new machine, once the old one is gone. What keeps a document running is **demand**: the handles
//! the caller holds ([`Hub::demand`]), the interest leases ([`Hub::interest_acquired`]), and a
//! catch-up task. The Document keeps its own counts too, and the Hub does not look inside them:
//! it only stops a document whose demand is zero and which says it is evictable (and says so
//! after its idle TTL, which is the Document's own clock).
//!
//! **Catch-up** (`Pursuit`, per document): `None`, `Queued`, `Running` or `Parked`. A readable
//! document whose tree revision is ahead of its frontier revision is *stale*, and the Hub owes
//! it a Document that materializes and publishes. It is `Queued` until fewer than
//! [`Config::catch_up`] documents are being caught up, then `Running` as a task of a
//! [`KeyedScheduler`] (one task per document, so a second feed row only raises the target), and
//! the task ends when the Document reports it has nothing left to catch up. A document whose
//! content has no key yet reports itself parked: it holds no slot, and the epoch move that
//! unblocks it ([`Hub::epoch_moved`]) queues it again. A document that stops being readable
//! leaves catch-up altogether.
//!
//! **The durable cursor.** Every feed row ([`Hub::feed`]) joins a [`WatermarkMachine`] job at its
//! revision; the job settles when the document is not stale, when it is caught up, or when it
//! parks. The rows of one transaction share a revision and arrive one by one, so the cursor
//! also waits for [`Hub::feed_complete`], which the driver calls once every row at or below a
//! revision has been delivered. The cursor only advances past settled jobs and leaves as [`Command::SaveCursor`], so a
//! restart resumes from it: the driver feeds the rows after the cursor, and the documents that are
//! stale at startup (a parked document's job is settled, and the cursor moved on), by a query.
//!
//! **Keyhive rounds** (`Round`, per peer): `Idle`, `Active { call }`, `Deferred { call }`. A
//! round is the Keyhive boundary's business; the Hub owns the policy around it. At most one
//! round per peer is in flight. Wants that arrive during a round are coalesced into one
//! follow-up. A round that completed with an admission watermark ahead of the Hub's
//! `admitted_head` is deferred until the head catches up, so a caller told "reconciled" is
//! never told so before the admissions are projected. A peer that disconnects cancels its round.
//!
//! **Lifecycle** (`Life`): `Open`, `Draining`, `Drained`. See "Draining".
//!
//! # Sessions and routes
//!
//! The Hub owns one `Interest` machine and one `Sessions` machine and nothing else about
//! sessions. The caller states which `(peer, principal)` sessions may serve a document
//! ([`Hub::standing`], computed from Keyline) and when a remote refuses a document on a session
//! ([`Hub::refused`]). The Hub forwards interest leases, wires `NeedSession` and `ReleaseSession`
//! into `Sessions`, `Sessions`'s losses and closes back into `Interest`, and emits what the layer
//! above must do: [`Command::Dial`], [`Command::Close`], [`Command::AddRoute`] and
//! [`Command::RemoveRoute`]. The Hub adds no policy of its own to the routes, so every route law
//! of those machines holds through it.
//!
//! # Routing
//!
//! A change-feed row reaches a running Document as a [`Notice`]; a document that is not running
//! hears nothing, and reads the tree when it starts. A keyhive epoch move reaches a running
//! Document the same way, and starts nothing: the only way an epoch move starts a Document is
//! through catch-up, for a document it has just made readable and that is stale. Between a
//! Document and the Hub there is no ordering except FIFO from the Hub to one Document.
//!
//! # Windows
//!
//! Each machine bounds what it has outstanding toward each other actor, so each mailbox has a
//! known size. The Hub's windows: toward one Document, at most [`Config::window`] notices are
//! unconsumed (a notice is one message that carries the latest tree revision, the latest
//! readability and the net interest change, so a burst coalesces into the next one instead of
//! queueing), plus the one reply to a request for an actor; toward the Keyhive boundary, one
//! round per peer; toward the driver for dialing, `Sessions`'s caps; and catch-up runs at most
//! [`Config::catch_up`] documents at once. A Document reports how many notices it has consumed
//! ([`Status::consumed`]), which is how the Hub learns that room opened and that a status
//! describes the Document *after* every notice it sent.
//!
//! # Draining
//!
//! [`Hub::drain`] ends new work: demand and interest are refused ([`Draining`]), queued catch-up
//! is dropped, and nothing new starts. The Hub then waits for what is in flight: rounds are
//! cancelled (and report), sessions are closed and none is dialed again, dials in flight finish, catch-up tasks end, and every Document stops, as soon as
//! it is evictable and nothing wants it. [`Command::Drained`] is sent exactly once, at the first
//! flush at which nothing is in flight, and [`Hub::drained`] says so. This replaces runtime2's
//! `cmd_closed` and its test-only quiescence probes.
//!
//! # Laws
//!
//! 1. **One Document per id.** A document has at most one running machine, and a start is of a
//!    generation no earlier machine had.
//! 2. **Stop only when evictable.** The Hub asks for a stop only after the Document reported
//!    itself evictable, with no demand and no catch-up, and nothing starts for the document
//!    before the answer; the driver grants it only to a machine that is evictable at that
//!    moment, so nothing in flight is lost.
//! 3. **Stale readable documents are served.** A document the Hub was told is readable and
//!    stale is running, queued or parked; one that is parked has said it lacks keys.
//! 4. **Parked documents wake on the unblocking epoch.** The epoch move of a parked document
//!    queues it again, and nothing else does except new content.
//! 5. **No start on keyhive alone.** An epoch move never starts a Document that is not stale.
//! 6. **Catch-up is bounded.** At most [`Config::catch_up`] documents are running a catch-up
//!    task.
//! 7. **The cursor never passes unsettled work.** The saved cursor is monotone and every feed
//!    row at or below it has been settled.
//! 8. **Windows hold.** No Document has more than [`Config::window`] unconsumed notices (and one
//!    actor reply), and a coalesced notice loses nothing.
//! 9. **One round per peer, resolved once.** A waiter is resolved exactly once, and a round
//!    that completed ahead of the admitted head resolves nothing until the head catches up.
//! 10. **Draining ends exactly when nothing is in flight.**
//! 11. **No route without standing.** A route exists only on an open session whose peer and
//!     principal the caller named eligible for the document (the laws of `Interest`, through
//!     the wiring).
//! 12. **A fresh actor.** No two starts, and no two actor requests, get the same actor, across
//!     restarts of the Hub too (the driver supplies a nonce per process).
//! 13. **A Document a coordinator plan holds or is owed a prepare by is never stopped.** Callers
//!     are served only by a machine that runs and is not being asked to stop ([`Hub::serving`]),
//!     and a plan keeps its documents demanded until its reply, so the Hub does not ask for a
//!     stop; a machine that is granted a stop is evictable, which a held plan, a queued prepare
//!     or an unanswered call each prevent.
//!
//! # Open questions and hooks
//!
//! - The round's admission protocol (`admitted_seq`, `admitted_head`) is modelled on runtime2's;
//!   the Keyhive boundary lane may change it. Only [`Hub::round_done`] and
//!   [`Hub::admission_advanced`] would.
//! - Interest to the Document: the Document keeps an interest count of its own (it hydrates for
//!   an interest); the Hub forwards the lease as a notice and counts it as demand.

mod peers;
mod rounds;
#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::time::Duration;

use automerge::ActorId;
use big_automata::kit::Instant;
use big_automata::kit::scheduler::TaskId;
use big_automata::kit::scheduler::keyed::KeyedScheduler;
use big_automata::kit::watermark::WatermarkMachine;
use big_automata::rev::Rev;
use big_automata::sessions::interest::Interest;
use big_automata::sessions::session;
use big_automata::sessions::session::Sessions;

use crate::coordinator::DocId;
use crate::document::CallId;

/// A caller waiting for a keyhive round, named by the caller.
pub type Waiter = u64;

#[derive(Debug, Clone)]
pub struct Config {
    /// The most documents caught up at once.
    pub catch_up: usize,
    /// The most notices one Document may have unconsumed.
    pub window: usize,
    /// How long a document stays routed after its last interest lease.
    pub interest_grace: Duration,
    pub sessions: session::Config,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            catch_up: 8,
            window: 4,
            interest_grace: Duration::from_secs(30),
            sessions: session::Config {
                grace: Duration::from_secs(60),
                per_peer: 2,
                total: 32,
                backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(60),
            },
        }
    }
}

/// What the driver knows when it builds the Hub.
#[derive(Debug, Clone, Copy)]
pub struct Boot {
    /// The saved cursor of the change feed: every row at or below it was settled.
    pub cursor: Rev,
    /// Random bytes, fresh for this process. Actors are minted from it, so a restarted Hub does
    /// not repeat one (the machine takes no randomness).
    pub nonce: [u8; 16],
}

/// A row of the change feed: a document's tree is now at `rev`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedRow {
    pub doc: DocId,
    pub rev: Rev,
    /// The revision the document's published frontier reflects, read with the row.
    pub frontier_rev: Rev,
    /// Whether this node can read the document, read with the row.
    pub readable: bool,
}

/// The keyhive epoch for a document moved. The Keyhive boundary computes the documents an event
/// affects and reads where each stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochMove {
    pub doc: DocId,
    pub readable: bool,
    pub tree_rev: Rev,
    pub frontier_rev: Rev,
}

/// What a Document says about itself after a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// How many notices (and actor replies) the machine has consumed.
    pub consumed: u64,
    /// A readable document with remote content or a frontier still to bring up to date.
    pub catching_up: bool,
    /// Content with no key yet: nothing more to do until the epoch moves.
    pub parked: bool,
    /// [`Document::evictable`](crate::document::Document::evictable).
    pub evictable: bool,
}

/// What a Document tells the Hub, besides its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Report {
    Status(Status),
    /// [`Command::Evictable`](crate::document::Command::Evictable): idle for its TTL.
    Evictable,
    /// [`Command::NewActor`](crate::document::Command::NewActor).
    NeedsActor {
        call: CallId,
    },
    /// The answer to [`Command::StopDocument`]: the machine was evictable and is gone.
    Stopped,
    /// The answer to [`Command::StopDocument`]: the machine was busy, and runs on. A status
    /// follows.
    StopRefused,
}

/// What the Hub tells a running Document: everything that changed since the last notice, as one
/// message, so a burst never queues more than the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Notice {
    /// The newest tree revision ([`Document::tree_changed`](crate::document::Document::tree_changed)).
    pub tree: Option<Rev>,
    /// The latest readability ([`Document::epoch_moved`](crate::document::Document::epoch_moved)).
    pub readable: Option<bool>,
    /// Interest leases acquired and released (acquired first).
    pub acquired: u32,
    pub released: u32,
}

impl Notice {
    fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToDoc {
    Notice(Notice),
    /// The answer to a [`Report::NeedsActor`].
    ActorAssigned {
        call: CallId,
        actor: ActorId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command<P, L> {
    /// Start the document's machine under `generation` and `actor`. The driver builds the
    /// Document's `Start` from storage when it executes this: reading storage then, after every
    /// feed row the Hub has already seen and before every notice it will send, is what keeps a
    /// machine from missing a row.
    StartDocument {
        doc: DocId,
        generation: u64,
        actor: ActorId,
    },
    /// Stop the machine if it is evictable *now* and its mailbox is empty; answer
    /// [`Report::Stopped`] or [`Report::StopRefused`]. Nothing is started for the document
    /// before the answer.
    StopDocument {
        doc: DocId,
        generation: u64,
    },
    ToDocument {
        doc: DocId,
        generation: u64,
        input: ToDoc,
    },
    /// Persist the feed cursor.
    SaveCursor {
        rev: Rev,
    },
    Dial {
        call: CallId,
        peer: P,
        principal: L,
    },
    Close {
        peer: P,
        principal: L,
    },
    AddRoute {
        doc: DocId,
        peer: P,
        principal: L,
    },
    RemoveRoute {
        doc: DocId,
        peer: P,
        principal: L,
    },
    /// Run a keyhive round with the peer; the answer is [`Hub::round_done`].
    StartRound {
        call: CallId,
        peer: P,
    },
    /// Abort a round. Its outcome is still reported.
    CancelRound {
        call: CallId,
    },
    /// A caller's wait for a round ended: reconciled (`ok`) or not.
    RoundResolved {
        waiter: Waiter,
        ok: bool,
    },
    /// Nothing is in flight any more. Sent once.
    Drained,
}

/// Where a document's catch-up stands, for the laws.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Catch {
    Queued,
    Running,
    Parked,
}

/// What the Hub holds about one document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DocView {
    /// The machine that runs, or is being asked to stop.
    pub generation: Option<u64>,
    /// The machine that may be handed work: running and not being asked to stop.
    pub serving: Option<u64>,
    pub demand: usize,
    pub catch: Option<Catch>,
    /// Notices sent and not yet consumed.
    pub in_flight: u64,
    /// The revision of the document's unsettled mark on the feed cursor.
    pub unsettled: Option<Rev>,
}

/// The Hub is draining: no new work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Draining;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Life {
    Open,
    Draining,
    Drained,
}

#[derive(Debug)]
enum Run {
    Absent,
    Running(Box<Running>),
    /// The Hub asked the driver to stop the machine, and has not heard whether it did. The driver
    /// stops it only if it is evictable *when the request reaches it* (a status the Hub holds
    /// may be out of date: the coordinator and the handles reach a Document without the Hub).
    Stopping(Box<Stopping>),
}

#[derive(Debug)]
struct Stopping {
    running: Running,
}

impl Run {
    /// The machine, running or being asked to stop.
    fn machine(&self) -> Option<&Running> {
        match self {
            Run::Absent => None,
            Run::Running(running) => Some(running),
            Run::Stopping(stopping) => Some(&stopping.running),
        }
    }

    fn machine_mut(&mut self) -> Option<&mut Running> {
        match self {
            Run::Absent => None,
            Run::Running(running) => Some(running),
            Run::Stopping(stopping) => Some(&mut stopping.running),
        }
    }
}

#[derive(Debug)]
struct Running {
    generation: u64,
    /// Messages sent to the machine, and the latest status it reported.
    sent: u64,
    /// How many the machine has consumed, as its latest status said.
    consumed: u64,
    /// The latest status, if it can still be trusted (none right after a refused stop).
    status: Option<Status>,
    ttl_reported: bool,
    /// What the window held back.
    held: Notice,
}

impl Running {
    fn in_flight(&self) -> u64 {
        self.sent - self.consumed
    }

    /// The status describes the machine after everything sent to it.
    fn settled(&self) -> Option<Status> {
        self.status
            .filter(|status| status.consumed == self.sent && self.held.is_empty())
    }
}

#[derive(Debug)]
enum Pursuit {
    None,
    Queued { seq: u64, rev: Rev },
    Running { task: TaskId, rev: Rev },
    Parked,
}

#[derive(Debug)]
struct Doc {
    run: Run,
    demand: usize,
    /// Interest leases held, which a restarted machine is told of.
    interest: u32,
    pursuit: Pursuit,
    /// The revision of this document's unsettled mark on the feed cursor.
    job: Option<Rev>,
}

impl Doc {
    fn new() -> Self {
        Self {
            run: Run::Absent,
            demand: 0,
            interest: 0,
            pursuit: Pursuit::None,
            job: None,
        }
    }
}

/// Bugs planted on purpose, to show the simulation catches them (see `tests`). Only tests plant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plant {
    /// A document is stopped as soon as the TTL says so, whatever wants it (law 2).
    StopBusy,
    /// A second machine is started for a document that is running (law 1).
    DoubleStart,
    /// An epoch move starts the document's machine (law 5).
    StartOnEpoch,
    /// An epoch move does not queue a parked document (law 4).
    NoWake,
    /// A feed row for a document that is not running queues no catch-up (law 3).
    DropStale,
    /// Catch-up starts without regard to the limit (law 6).
    LimitIgnored,
    /// The cursor moves past a row whose job is not settled (law 7).
    CursorAhead,
    /// Notices are sent without regard to the window (law 8).
    WindowIgnored,
    /// A cancelled round's waiters are never resolved (law 9).
    LoseWaiters,
    /// The Hub says it is drained while a round is in flight (law 10).
    DrainEarly,
    /// A revoke of standing is not passed on (law 11).
    IgnoreRevoke,
    /// Every start gets the same actor (law 12).
    SameActor,
    /// A machine being asked to stop is said to serve callers (law 13).
    ServeWhileStopping,
}

pub struct Hub<P, L> {
    config: Config,
    nonce: [u8; 16],
    next_actor: u64,
    next_generation: u64,
    next_call: CallId,
    life: Life,
    docs: BTreeMap<DocId, Doc>,
    /// Catch-up waiting for a slot, oldest first.
    queue: BTreeMap<u64, DocId>,
    next_seq: u64,
    /// The running and parked catch-up tasks, one per document. The seed is the target revision.
    catch_up: KeyedScheduler<DocId, Rev>,
    /// The feed cursor. A document's mark is keyed by it; a *gate* (key `None`) at a revision
    /// stays open until the driver says every row at that revision has been delivered, because
    /// the rows of one transaction share a revision and arrive one by one.
    marks: WatermarkMachine<Option<DocId>, (), Rev>,
    gates: BTreeSet<Rev>,
    /// The revision through which every feed row has been delivered.
    through: Rev,
    /// The cursor the driver was last told to save, and the one it may be told next.
    saved: Rev,
    cursor_due: Option<Rev>,
    interest: Interest<DocId, (P, L)>,
    sessions: Sessions<P, L>,
    peers: BTreeMap<P, rounds::PeerRound>,
    /// The dials out, by call, so an outcome names its session.
    dials: BTreeMap<CallId, (P, L)>,
    admitted_head: u64,
    /// Rounds the Hub cancelled, whose outcomes are stale.
    cancelled: BTreeSet<CallId>,
    pending: Vec<Command<P, L>>,
    #[cfg(test)]
    plant: Option<Plant>,
}

impl<P, L> Hub<P, L>
where
    P: Ord + Clone + Debug,
    L: Ord + Clone + Debug,
{
    pub fn new(config: Config, boot: Boot) -> Self {
        assert!(config.catch_up >= 1 && config.window >= 1);
        Self {
            interest: Interest::new(config.interest_grace),
            sessions: Sessions::new(config.sessions),
            config,
            nonce: boot.nonce,
            next_actor: 0,
            next_generation: 0,
            next_call: 0,
            life: Life::Open,
            docs: BTreeMap::new(),
            queue: BTreeMap::new(),
            next_seq: 0,
            catch_up: KeyedScheduler::default(),
            marks: WatermarkMachine::default(),
            gates: BTreeSet::new(),
            through: boot.cursor,
            saved: boot.cursor,
            cursor_due: None,
            peers: BTreeMap::new(),
            dials: BTreeMap::new(),
            admitted_head: 0,
            cancelled: BTreeSet::new(),
            pending: Vec::new(),
            #[cfg(test)]
            plant: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn plant(&mut self, plant: Plant) {
        self.plant = Some(plant);
    }

    #[cfg(test)]
    fn planted(&self, plant: Plant) -> bool {
        self.plant == Some(plant)
    }

    #[cfg(not(test))]
    fn planted(&self, _: Plant) -> bool {
        false
    }

    // ── what wants a document ───────────────────────────────────────────────────────────────

    /// Somebody wants the document's machine (a handle, a plan). It is started by the next
    /// flush's commands if it was not running, or once the machine that is stopping is gone; the
    /// driver serves the caller from whichever machine runs for the document.
    pub fn demand(&mut self, doc: DocId) -> Result<(), Draining> {
        if self.life != Life::Open {
            return Err(Draining);
        }
        self.docs.entry(doc).or_insert_with(Doc::new).demand += 1;
        self.ensure_running(doc);
        Ok(())
    }

    /// A [`Hub::demand`] ended.
    pub fn undemand(&mut self, doc: DocId) {
        self.docs_mut(doc).demand -= 1;
        self.maybe_stop(doc);
        self.gc(doc);
    }

    /// Somebody holds interest in the document: it stays alive, and routes to the sessions that
    /// may serve it.
    pub fn interest_acquired(&mut self, doc: DocId) -> Result<(), Draining> {
        self.demand(doc)?;
        self.interest.lease_acquired(doc);
        self.docs_mut(doc).interest += 1;
        self.notice(doc, |notice| notice.acquired += 1);
        Ok(())
    }

    pub fn interest_released(&mut self, doc: DocId) {
        self.interest.lease_dropped(&doc);
        self.docs_mut(doc).interest -= 1;
        self.notice(doc, |notice| notice.released += 1);
        self.undemand(doc);
    }

    // ── what a Document says ────────────────────────────────────────────────────────────────

    /// A Document machine reported. A report from a machine that is not the current one of the
    /// document (it was stopped) changes nothing.
    pub fn reported(&mut self, doc: DocId, generation: u64, report: Report) {
        if let Report::Stopped | Report::StopRefused = report {
            self.stop_answered(doc, generation, report == Report::Stopped);
            return;
        }
        let Some(run) = self.running_mut(doc, generation) else {
            return;
        };
        match report {
            Report::Status(status) => {
                assert!(
                    status.consumed <= run.sent && status.consumed >= run.consumed,
                    "a status counts what was sent"
                );
                run.consumed = status.consumed;
                run.status = Some(status);
                if !status.evictable {
                    run.ttl_reported = false;
                }
                self.release_held(doc);
                self.check_pursuit(doc);
            }
            Report::Evictable => run.ttl_reported = true,
            Report::NeedsActor { call } => {
                let actor = self.mint_actor();
                self.send(doc, ToDoc::ActorAssigned { call, actor });
            }
            Report::Stopped | Report::StopRefused => unreachable!("answered above"),
        }
        self.maybe_stop(doc);
    }

    // ── the change feed and the keyhive epoch ───────────────────────────────────────────────

    /// A row of the change feed, in revision order.
    pub fn feed(&mut self, row: FeedRow) {
        self.notice(row.doc, |notice| {
            notice.tree = Some(notice.tree.map_or(row.rev, |rev| rev.max(row.rev)));
        });
        if self.life != Life::Open {
            return;
        }
        let known = self.docs.get(&row.doc).and_then(|doc| doc.job);
        if known.is_some_and(|job| job >= row.rev) {
            return;
        }
        // A row at or below the completed revision is one the startup query found: the cursor is
        // already past it, so it needs a pursuit and no mark.
        let marked = row.rev > self.through;
        if marked {
            if self.gates.insert(row.rev) {
                self.marks.add_job(None, row.rev, [()]);
            }
            // the newer row supersedes the older one's mark: catching up to the latest covers it
            let released = self.marks.release_stream(&Some(row.doc), &(), &row.rev);
            self.advance(released);
            self.marks.add_job(Some(row.doc), row.rev, [()]);
            if self.planted(Plant::CursorAhead) {
                self.advance(Some(row.rev));
            }
            self.docs.entry(row.doc).or_insert_with(Doc::new).job = Some(row.rev);
        } else {
            self.docs.entry(row.doc).or_insert_with(Doc::new);
        }
        let stale = row.readable && row.frontier_rev < row.rev;
        if stale
            && !(self.planted(Plant::DropStale)
                && self
                    .docs
                    .get(&row.doc)
                    .is_none_or(|doc| matches!(doc.run, Run::Absent)))
        {
            self.want_catch_up(row.doc, row.rev);
        } else {
            if !row.readable {
                self.leave_catch_up(row.doc);
            }
            self.settle_job(row.doc);
        }
        self.gc(row.doc);
    }

    /// The keyhive epoch for a document moved.
    pub fn epoch_moved(&mut self, moved: EpochMove) {
        // The move carries the tree revision it was read at: the Document is told it too, or the
        // Hub could count a catch-up done that the Document does not know it owes.
        self.notice(moved.doc, |notice| {
            notice.readable = Some(moved.readable);
            notice.tree = Some(
                notice
                    .tree
                    .map_or(moved.tree_rev, |rev| rev.max(moved.tree_rev)),
            );
        });
        if self.planted(Plant::StartOnEpoch) && self.life == Life::Open {
            self.docs.entry(moved.doc).or_insert_with(Doc::new);
            self.ensure_running(moved.doc);
        }
        if !moved.readable {
            self.leave_catch_up(moved.doc);
            self.settle_job(moved.doc);
        } else if self.life == Life::Open {
            let stale = moved.frontier_rev < moved.tree_rev;
            let parked = self
                .docs
                .get(&moved.doc)
                .is_some_and(|doc| matches!(doc.pursuit, Pursuit::Parked));
            if stale && !(parked && self.planted(Plant::NoWake)) {
                self.docs.entry(moved.doc).or_insert_with(Doc::new);
                self.want_catch_up(moved.doc, moved.tree_rev);
            } else if !stale {
                self.leave_catch_up(moved.doc);
            }
        }
        self.maybe_stop(moved.doc);
        self.gc(moved.doc);
    }

    /// Every feed row at or below `through` has been delivered (a transaction's rows share a
    /// revision, so only the driver knows the revision is complete). The cursor never passes
    /// a revision before this.
    pub fn feed_complete(&mut self, through: Rev) {
        if through <= self.through {
            // (the startup query's rows complete below the cursor, and a repeat says nothing new)
            return;
        }
        self.through = through;
        let done: Vec<Rev> = self.gates.range(..=through).copied().collect();
        for rev in done {
            self.gates.remove(&rev);
            for (_, cursor) in self.marks.settle_on_cursor(&None, &rev) {
                self.advance(cursor);
            }
        }
    }

    // ── draining ────────────────────────────────────────────────────────────────────────────

    /// Stop starting work. Queued catch-up is dropped (its marks stay unsettled, so a restart
    /// finds the documents again), and rounds are cancelled.
    pub fn drain(&mut self) {
        if self.life != Life::Open {
            return;
        }
        self.life = Life::Draining;
        let queued: Vec<_> = std::mem::take(&mut self.queue).into_values().collect();
        for doc in queued {
            self.docs_mut(doc).pursuit = Pursuit::None;
            self.gc(doc);
        }
        self.cancel_rounds();
        // no peer is connected as far as sessions go: the open ones are closed, and nothing is
        // dialed again whatever the interest says
        let peers: Vec<P> = self.peers.keys().cloned().collect();
        for peer in peers {
            self.sessions.peer_disconnected(&peer);
        }
        let running: Vec<_> = self.docs.keys().copied().collect();
        for doc in running {
            self.maybe_stop(doc);
        }
    }

    /// Whether nothing is in flight and the Hub is draining.
    pub fn drained(&self) -> bool {
        self.life == Life::Drained
    }

    // ── time and commands ───────────────────────────────────────────────────────────────────

    pub fn next_due(&self) -> Option<Instant> {
        [self.interest.next_due(), self.sessions.next_due()]
            .into_iter()
            .flatten()
            .min()
    }

    /// Start what the inputs since the last flush made possible, and take the commands.
    pub fn flush(&mut self, now: Instant, out: &mut Vec<Command<P, L>>) {
        self.start_catch_up(now);
        self.wire_sessions(now);
        if let Some(rev) = self.cursor_due.take()
            && rev > self.saved
        {
            self.saved = rev;
            self.pending.push(Command::SaveCursor { rev });
        }
        if self.life == Life::Draining && self.nothing_in_flight() {
            self.life = Life::Drained;
            self.pending.push(Command::Drained);
        }
        out.append(&mut self.pending);
        #[cfg(test)]
        self.check();
    }

    // ── views, for the laws ─────────────────────────────────────────────────────────────────

    /// What the Hub holds about a document, for the laws and for callers that must pick a machine.
    pub fn doc(&self, doc: DocId) -> DocView {
        let Some(entry) = self.docs.get(&doc) else {
            return DocView::default();
        };
        let machine = entry.run.machine();
        DocView {
            generation: machine.map(|running| running.generation),
            // a machine that runs and is not being asked to stop: the only one that may be handed
            // work (a handle, a plan's prepare), because a request sent to a stopping machine
            // may be lost with it (law 13)
            serving: match &entry.run {
                Run::Running(running) => Some(running.generation),
                Run::Stopping(stopping) if self.planted(Plant::ServeWhileStopping) => {
                    Some(stopping.running.generation)
                }
                Run::Stopping(_) | Run::Absent => None,
            },
            demand: entry.demand,
            catch: match entry.pursuit {
                Pursuit::None => None,
                Pursuit::Queued { .. } => Some(Catch::Queued),
                Pursuit::Running { .. } => Some(Catch::Running),
                Pursuit::Parked => Some(Catch::Parked),
            },
            in_flight: machine.map_or(0, Running::in_flight),
            // the revision of the document's unsettled mark on the feed cursor
            unsettled: entry.job,
        }
    }

    /// Documents whose catch-up runs now.
    pub fn catching_up(&self) -> usize {
        self.catch_up.counts().live
    }

    pub fn interest(&self) -> &Interest<DocId, (P, L)> {
        &self.interest
    }

    pub fn sessions(&self) -> &Sessions<P, L> {
        &self.sessions
    }

    pub fn saved_cursor(&self) -> Rev {
        self.saved
    }

    /// The machine's own invariants, asserted at the end of every flush in test builds.
    #[cfg(test)]
    fn check(&self) {
        let queued = self
            .docs
            .values()
            .filter(|doc| matches!(doc.pursuit, Pursuit::Queued { .. }))
            .count();
        assert_eq!(queued, self.queue.len(), "the queue and the pursuits agree");
        let running = self
            .docs
            .values()
            .filter(|doc| matches!(doc.pursuit, Pursuit::Running { .. }))
            .count();
        assert_eq!(
            running,
            self.catch_up.counts().live,
            "tasks and pursuits agree"
        );
        if self.plant.is_none() {
            assert!(running <= self.config.catch_up);
            for doc in self.docs.values() {
                if let Some(run) = doc.run.machine() {
                    assert!(run.in_flight() <= self.config.window as u64 + 1);
                }
            }
        }
    }

    // ── internals ───────────────────────────────────────────────────────────────────────────

    fn docs_mut(&mut self, doc: DocId) -> &mut Doc {
        self.docs.get_mut(&doc).expect("a document the Hub knows")
    }

    fn running_mut(&mut self, doc: DocId, generation: u64) -> Option<&mut Running> {
        match self.docs.get_mut(&doc)?.run.machine_mut() {
            Some(running) if running.generation == generation => Some(running),
            _ => None,
        }
    }

    fn mint_actor(&mut self) -> ActorId {
        self.next_actor += 1;
        let counter = if self.planted(Plant::SameActor) {
            0
        } else {
            self.next_actor
        };
        let mut bytes = self.nonce.to_vec();
        bytes.extend(counter.to_be_bytes());
        ActorId::from(bytes)
    }

    /// Start the document's machine if there is none. A machine that is being asked to stop is
    /// replaced when the driver says it is gone, if the document is still wanted then.
    fn ensure_running(&mut self, doc: DocId) {
        let double = self.planted(Plant::DoubleStart);
        match self.docs_mut(doc).run {
            Run::Running(_) if !double => return,
            Run::Stopping(_) => return,
            Run::Absent | Run::Running(_) => {}
        }
        self.next_generation += 1;
        let generation = self.next_generation;
        self.start(doc, generation);
    }

    fn start(&mut self, doc: DocId, generation: u64) {
        let actor = self.mint_actor();
        let entry = self.docs_mut(doc);
        entry.run = Run::Running(Box::new(Running {
            generation,
            sent: 0,
            consumed: 0,
            status: None,
            ttl_reported: false,
            held: Notice {
                acquired: entry.interest,
                ..Notice::default()
            },
        }));
        self.pending.push(Command::StartDocument {
            doc,
            generation,
            actor,
        });
        self.release_held(doc);
    }

    /// Tell the running machine something, through the window. A machine that is not running
    /// hears nothing: it reads the tree when it starts.
    fn notice(&mut self, doc: DocId, change: impl FnOnce(&mut Notice)) {
        let Some(running) = self
            .docs
            .get_mut(&doc)
            .and_then(|entry| entry.run.machine_mut())
        else {
            return;
        };
        change(&mut running.held);
        self.release_held(doc);
    }

    /// Send the held notice if the window has room.
    fn release_held(&mut self, doc: DocId) {
        let window = self.config.window as u64;
        let ignore = self.planted(Plant::WindowIgnored);
        let Some(Doc {
            run: Run::Running(running),
            ..
        }) = self.docs.get_mut(&doc)
        else {
            return;
        };
        if running.held.is_empty() || (running.in_flight() >= window && !ignore) {
            return;
        }
        let notice = std::mem::take(&mut running.held);
        self.send(doc, ToDoc::Notice(notice));
    }

    /// Send a message to the running machine, outside the window.
    fn send(&mut self, doc: DocId, input: ToDoc) {
        let Some(running) = self.docs_mut(doc).run.machine_mut() else {
            panic!("a message for a machine that is not running");
        };
        running.sent += 1;
        let generation = running.generation;
        self.pending.push(Command::ToDocument {
            doc,
            generation,
            input,
        });
    }

    /// Stop the machine if it reported itself evictable and nothing wants it.
    fn maybe_stop(&mut self, doc: DocId) {
        let draining = self.life != Life::Open;
        let busy = self.planted(Plant::StopBusy);
        let Some(entry) = self.docs.get(&doc) else {
            return;
        };
        let Run::Running(running) = &entry.run else {
            return;
        };
        let evictable = running.settled().is_some_and(|status| status.evictable)
            && (running.ttl_reported || draining);
        let wanted = entry.demand > 0 || matches!(entry.pursuit, Pursuit::Running { .. });
        if (evictable && !wanted) || (busy && running.ttl_reported) {
            let generation = running.generation;
            let Run::Running(running) = std::mem::replace(&mut self.docs_mut(doc).run, Run::Absent)
            else {
                unreachable!("matched above");
            };
            self.docs_mut(doc).run = Run::Stopping(Box::new(Stopping { running: *running }));
            self.pending.push(Command::StopDocument { doc, generation });
        }
    }

    /// The driver's answer to a stop request.
    fn stop_answered(&mut self, doc: DocId, generation: u64, stopped: bool) {
        let entry = self.docs_mut(doc);
        let Run::Stopping(stopping) = std::mem::replace(&mut entry.run, Run::Absent) else {
            panic!("an answer to a stop that was not asked");
        };
        assert_eq!(stopping.running.generation, generation);
        if stopped {
            // the machine is gone: what it was told since the request is moot, because its
            // successor reads the tree and is told of the leases when it starts
            let wanted = entry.demand > 0 || matches!(entry.pursuit, Pursuit::Running { .. });
            if wanted {
                self.next_generation += 1;
                let next = self.next_generation;
                self.start(doc, next);
            } else {
                self.gc(doc);
            }
        } else {
            let mut running = stopping.running;
            // what we know of it is out of date: a fresh status follows
            running.status = None;
            running.ttl_reported = false;
            entry.run = Run::Running(Box::new(running));
            self.release_held(doc);
        }
    }

    /// Forget a document nothing is known about.
    fn gc(&mut self, doc: DocId) {
        if let Some(entry) = self.docs.get(&doc)
            && matches!(entry.run, Run::Absent)
            && entry.demand == 0
            && matches!(entry.pursuit, Pursuit::None)
            && entry.job.is_none()
        {
            self.docs.remove(&doc);
        }
    }

    // ── catch-up ────────────────────────────────────────────────────────────────────────────

    /// The document is stale and readable at `rev`: it needs a machine.
    fn want_catch_up(&mut self, doc: DocId, rev: Rev) {
        let entry = self.docs_mut(doc);
        match &mut entry.pursuit {
            Pursuit::None => {
                self.next_seq += 1;
                let seq = self.next_seq;
                self.queue.insert(seq, doc);
                self.docs_mut(doc).pursuit = Pursuit::Queued { seq, rev };
            }
            Pursuit::Queued { rev: queued, .. } | Pursuit::Running { rev: queued, .. } => {
                *queued = (*queued).max(rev);
            }
            Pursuit::Parked => {
                // new content, or the epoch that unblocks it: it queues like any start, so the
                // limit holds
                assert!(self.catch_up.cancel(&doc).is_none());
                self.next_seq += 1;
                let seq = self.next_seq;
                self.queue.insert(seq, doc);
                self.docs_mut(doc).pursuit = Pursuit::Queued { seq, rev };
            }
        }
    }

    /// The document no longer needs catching up (it is not readable, or not stale).
    fn leave_catch_up(&mut self, doc: DocId) {
        let Some(entry) = self.docs.get_mut(&doc) else {
            return;
        };
        match std::mem::replace(&mut entry.pursuit, Pursuit::None) {
            Pursuit::None => {}
            Pursuit::Queued { seq, .. } => {
                self.queue.remove(&seq);
            }
            Pursuit::Running { task, .. } => {
                self.catch_up.complete(task).expect("a running task");
            }
            Pursuit::Parked => {
                assert!(self.catch_up.cancel(&doc).is_none());
            }
        }
    }

    /// Start queued catch-up while there is room.
    fn start_catch_up(&mut self, now: Instant) {
        let limit = if self.planted(Plant::LimitIgnored) {
            usize::MAX
        } else {
            self.config.catch_up
        };
        while self.life == Life::Open && self.catch_up.counts().live < limit {
            let Some((_, doc)) = self.queue.pop_first() else {
                break;
            };
            let Pursuit::Queued { rev, .. } = self.docs_mut(doc).pursuit else {
                unreachable!("a queued document is queued");
            };
            let task = self
                .catch_up
                .replace(now, doc, rev)
                .expect("a key with no task starts one");
            // the Hub is the driver of its own tasks: nothing else to hand them to
            self.catch_up.drain_spawn_queue().for_each(drop);
            self.docs_mut(doc).pursuit = Pursuit::Running { task, rev };
            self.ensure_running(doc);
            self.check_pursuit(doc);
        }
    }

    /// End the document's catch-up task if its machine says it has nothing left, and park the
    /// document if it lacks keys.
    fn check_pursuit(&mut self, doc: DocId) {
        let Some(entry) = self.docs.get(&doc) else {
            return;
        };
        let Pursuit::Running { task, rev } = entry.pursuit else {
            return;
        };
        let Run::Running(running) = &entry.run else {
            return;
        };
        let Some(status) = running.settled() else {
            return;
        };
        if status.catching_up {
            return;
        }
        self.catch_up.complete(task).expect("a running task");
        if status.parked {
            self.catch_up.park(doc, rev);
            self.docs_mut(doc).pursuit = Pursuit::Parked;
        } else {
            self.docs_mut(doc).pursuit = Pursuit::None;
        }
        // a parked document waits on keys, not on the feed: its mark settles, and the startup
        // query finds it again
        self.settle_job(doc);
        self.maybe_stop(doc);
        self.gc(doc);
    }

    // ── the cursor ──────────────────────────────────────────────────────────────────────────

    fn settle_job(&mut self, doc: DocId) {
        let Some(rev) = self.docs.get_mut(&doc).and_then(|entry| entry.job.take()) else {
            return;
        };
        for (_, cursor) in self.marks.settle_on_cursor(&Some(doc), &rev) {
            self.advance(cursor);
        }
        self.gc(doc);
    }

    fn advance(&mut self, cursor: Option<Rev>) {
        if let Some(cursor) = cursor {
            self.cursor_due = Some(self.cursor_due.map_or(cursor, |due| due.max(cursor)));
        }
    }

    // ── draining ────────────────────────────────────────────────────────────────────────────

    fn nothing_in_flight(&self) -> bool {
        let docs_quiet = self.docs.values().all(|doc| {
            matches!(doc.run, Run::Absent) && !matches!(doc.pursuit, Pursuit::Running { .. })
        });
        let early = self.planted(Plant::DrainEarly);
        docs_quiet && (early || self.rounds_quiet()) && self.sessions_quiet()
    }
}
