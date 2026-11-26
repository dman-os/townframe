//! The Keyhive boundary: the policy around the one keyhive instance, as a pure state machine.
//! Everything big_hive does with keyhive goes through here, so the rules about when keyhive may
//! be called live in one place and can be tested without keyhive, SQLite or a clock.
//!
//! # What it is, and what it is not
//!
//! Like the [Document machine](crate::document) and the [Write coordinator](crate::coordinator)
//! it is sans-IO: no awaits, no IO, no channels, no locks, no spawning, no randomness. Every input
//! is a method; everything it wants done leaves as a [`Command`] from the next
//! [`KeyhiveBoundary::flush`], which is also the only place the clock is read. A driver owns it,
//! runs the commands (keyhive calls, the transaction, Keyline reads) and reports each answer
//! back through the input that names it. Keyhive calls, transactions and store reads are
//! therefore abstract commands here, and the real keyhive is never needed to test the policy.
//!
//! The "keyhive lock" is not a lock in the machine. It is a rule about which commands may be
//! outstanding together, and the machine never issues two commands that break it. The driver
//! still takes its real lock ([`Command::AcquireLock`]) because other tasks read keyhive too.
//!
//! # The two kinds of work
//!
//! **The encryption path** (shared lock). A Document or the coordinator asks
//! [`KeyhiveBoundary::prepare_encrypt`]; the machine hydrates the document if it is not loaded,
//! issues [`Command::Encrypt`] and answers [`Command::Encrypted`]. Encrypting changes no memory
//! in keyhive. The asker commits the change with its other rows and then reports the result with
//! [`KeyhiveBoundary::settle_encrypt`]: `applied` (the commit landed, so keyhive applies the
//! preparation) or dropped. Encryptions for different documents run at the same time. A
//! preparation holds the shared lock from `Encrypt` until it is settled, and not only around
//! the apply. The reason is the exclusive path: a membership change that landed between the
//! preparation and the commit (a removal, say) would leave the prepared key stale, and applying
//! it afterwards would encrypt for a reader who is no longer one. So a locked operation waits
//! for every preparation to commit and settle, and new preparations wait behind it.
//!
//! **Decryption** (shared lock) is the same kind of keyhive call: [`KeyhiveBoundary::decrypt`]
//! hydrates the document if it is not loaded, issues [`Command::Decrypt`] and answers
//! [`Command::Decrypted`]. It holds the shared lock only while the call is out, because it
//! prepares nothing that a later commit settles.
//!
//! **The locked path** (exclusive lock), for membership changes and for keyhive events that
//! arrive from a peer. In this order:
//! 1. [`Command::AcquireLock`]: lock first, then the transaction. The reverse can deadlock
//!    against a task that holds the lock and waits to begin.
//! 2. [`Command::Begin`]: open the write transaction. Keyline inserts are routed into it.
//! 3. [`Command::Run`]: keyhive runs the operation, and what it reports (CGKA ops, prekey ops,
//!    pending-event changes) and the part-store membership rows of the affected documents are
//!    written into the same transaction. A membership change hydrates the affected documents
//!    first, so their CGKA ops are issued now and not at each document's next write. If our
//!    prekeys are below the floor the same command refills them.
//! 4. [`Command::Commit`], then [`Command::ReleaseLock`].
//! 5. On failure: [`Command::Rollback`] (which erases the Keyline rows), then
//!    [`Command::Reopen`] (drop the keyhive instance, open a new one, which holds nothing but our
//!    identity), then release. Keyhive changed its memory during the call, before the commit, so
//!    the instance cannot be trusted after a failure.
//!
//! Locked operations run one at a time in submission order. A queued one stops new shared work
//! from starting (so it cannot starve) and starts when the shared work in flight has settled.
//!
//! # Hydration and eviction
//!
//! A document is `NotLoaded`, `Loading` or `Loaded` (`Load`). Before any operation on a
//! document that is not loaded the machine issues [`Command::Hydrate`]. The hub says another
//! process moved a document's version row with [`KeyhiveBoundary::evict`]; the machine then
//! issues [`Command::Evict`] once nothing prepared refers to the document, or after a hydrate
//! that was already reading stale rows. A reopen forgets every document.
//!
//! # Telling peers
//!
//! After a keyhive event commits, the machine tells every **directly connected** session the
//! event may concern, and nobody else. It never gossips, and it never sends an event back to the
//! session it came from (the source is part of what the commit reports). A session is one
//! connection: the hub gives a reconnect a new [`SessionId`], so "once per connection" is "once
//! per session id". Which sessions may see an event is Keyline's answer, not the machine's: after
//! a debounce ([`Config::debounce`], at most [`Config::max_delay`] since the first event) it asks
//! [`Command::Eligible`] with the events a session has accumulated, and notifies only the subset
//! the answer grants, with [`Command::Notify`]. Asking at send time and not at commit time means
//! a session that lost standing in between is not told. An event a session was not eligible for
//! is not retried for it: a session that gains standing learns by the hub's keyhive round. An
//! event is reported once, by the commit that made it durable; the machine relies on that to
//! send it at most once.
//!
//! # Prekeys
//!
//! The driver reports how many of our prekeys are left ([`KeyhiveBoundary::prekeys_remaining`],
//! and every `ran` report). Below [`Config::prekey_floor`] the next locked operation's `Run`
//! carries `refill`, so the refill commits in that operation's transaction. If none is queued
//! the machine queues a refill-only operation. This replaces runtime2's janitor loop. A refused
//! operation that carried the refill is retried on a backoff deadline
//! ([`Config::refill_backoff_min`], doubling up to [`Config::refill_backoff_max`]), exposed
//! through [`KeyhiveBoundary::next_due`], so an idle node does not stay below the floor. Any
//! operation that runs before the deadline carries the refill too, and a successful refill
//! resets the backoff.
//!
//! # Errors
//!
//! Contention (`SQLITE_BUSY`) never reaches the machine: the SQLite driver waits it out and
//! panics if it persists. Any other store failure is [`Ended::StoreFailed`], and the machine
//! answers it with [`Command::Trap`]: after that it emits nothing and ignores every input, and
//! the hub tears the instance down. A keyhive refusal ([`Ended::Refused`]) is not a store
//! failure: an encryption it refuses is answered as failed, and a locked operation it refuses
//! is rolled back and the instance reopened.
//!
//! # Laws
//!
//! 1. **Nothing is applied unless its commit landed.** [`Command::ApplyEncrypt`] is issued only
//!    for a preparation settled as applied; a dropped one is only dropped.
//! 2. **The locked path is all or nothing, and a failure reopens.** A locked operation's rows
//!    land with its commit or not at all, and after a failure no keyhive call is issued before
//!    the instance has been reopened.
//! 3. **The lock is exclusive.** [`Command::AcquireLock`] is issued with no encryption in flight
//!    (prepared and unsettled counts), no decryption and no hydration in flight, and no keyhive
//!    call other than the operation's own is issued until [`Command::ReleaseLock`].
//! 4. **Notifications are targeted and once.** A [`Command::Notify`] goes only to a connected
//!    session, only with events the latest Keyline answer granted it, never with an event from
//!    that session, and no event goes to a session twice.
//! 5. **Hydrate before use.** Every [`Command::Encrypt`], [`Command::Decrypt`] and
//!    [`Command::ApplyEncrypt`] names a document that is loaded.
//! 6. **The prekey floor is restored.** A `Run` issued with the level below the floor carries
//!    `refill`, so the next locked operation restores the floor.
//! 7. **A trap stops everything.** No command follows [`Command::Trap`].
//! 8. **A refused refill is retried.** After a refused operation that carried the refill, the
//!    machine has a deadline ([`KeyhiveBoundary::next_due`]) at which it runs another, and the
//!    floor is restored once the driver lets it.
//!
//! # Where its neighbours come in
//!
//! The Document machine's `PrepareEncrypt` and `SettleEncrypt` commands and the coordinator's are
//! this machine's [`KeyhiveBoundary::prepare_encrypt`] and [`KeyhiveBoundary::settle_encrypt`];
//! [`Command::Encrypted`] is their `encrypted`. The Hub feeds [`KeyhiveBoundary::evict`] and the
//! sessions, and computes eligibility.
//!
//! **Change needed in the Document machine (not made here):** its `Decrypt { call, heads, rev }`
//! command goes to the driver directly today. It must go through [`KeyhiveBoundary::decrypt`]
//! instead, so that the decryption is hydrated for and excluded from locked operations like
//! every other keyhive call. The driver forwards the Document's `Decrypt` as the boundary's
//! request (the payload is whatever the driver reads to decrypt: the commits behind `heads` at
//! `rev`) and routes [`Command::Decrypted`] back to the Document's `decrypted`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::time::Duration;

use crate::coordinator::DocId;
use crate::document::CallId;
use crate::document::EncryptedChange;
use crate::document::Instant;
use crate::document::Outcome;
use crate::document::Ticket;

mod answers;
mod policy;

#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;

/// One connection to a peer. A reconnect is a new session.
pub type SessionId = u64;

/// A keyhive event, named by its hash.
pub type EventId = u64;

#[derive(Debug, Clone)]
pub struct Config {
    /// How long after the last new event a session is asked about its events.
    pub debounce: Duration,
    /// The longest a session's first event waits, however many keep arriving.
    pub max_delay: Duration,
    /// Below this many prekeys the next locked operation refills.
    pub prekey_floor: usize,
    /// The first wait before a refused refill is tried again, doubling up to the maximum.
    pub refill_backoff_min: Duration,
    pub refill_backoff_max: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            prekey_floor: 8,
            refill_backoff_min: Duration::from_secs(1),
            refill_backoff_max: Duration::from_secs(60),
        }
    }
}

/// Who asked for an encryption, echoed in the answer so the driver routes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asker {
    Document(DocId),
    Coordinator,
}

/// How a call to the driver ended. Which cases a call may report is in each input's docs;
/// anything else is a bug in the driver and panics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ended<T> {
    Done(T),
    /// Keyhive's own rules said no (an encryption without write access, a membership change that
    /// is not allowed). The store is fine.
    Refused,
    /// The store failed for a reason other than contention.
    StoreFailed,
    /// The call was cancelled and did nothing.
    Cancelled,
}

/// What an encryption's preparation made: the ciphertext (standing for every row the commit
/// carries for it: content key, rotation) and the keyhive events the commit makes durable, such
/// as a rotation's CGKA op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    pub ciphertext: Vec<u8>,
    pub events: Vec<EventId>,
}

/// What keyhive reported for a locked operation, written in the operation's transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The documents the operation hydrated.
    pub hydrated: Vec<DocId>,
    /// The events the operation makes durable, each with the session it came from.
    pub events: Vec<(EventId, Option<SessionId>)>,
    /// Our prekeys left after the operation, and after the refill if it asked for one.
    pub prekeys: usize,
}

/// A locked operation the caller asks for. The machine does not look inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockedOp {
    Membership(Vec<u8>),
    Ingest { from: SessionId, events: Vec<u8> },
}

/// A locked operation was refused by keyhive or failed to commit. Nothing changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused;

/// The step a trap happened in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Hydrate,
    Encrypt,
    Decrypt,
    Apply,
    Begin,
    Run,
    Commit,
    Rollback,
    Reopen,
    Eligible,
}

#[derive(Debug)]
pub enum Command {
    /// Load the document into the keyhive instance. Answer: [`KeyhiveBoundary::hydrated`].
    Hydrate { call: CallId, doc: DocId },
    /// Prepare an encryption. Its `call` is the preparation's token from here on. Answer:
    /// [`KeyhiveBoundary::encrypted`].
    Encrypt {
        call: CallId,
        doc: DocId,
        change: Vec<u8>,
    },
    /// Decrypt a payload for a document. Holds the shared lock while out. Answer:
    /// [`KeyhiveBoundary::decrypted`].
    Decrypt {
        call: CallId,
        doc: DocId,
        payload: Vec<u8>,
    },
    /// Apply a preparation whose commit landed. Answer: [`KeyhiveBoundary::applied`].
    ApplyEncrypt { call: CallId, token: u64 },
    /// Forget a preparation whose commit did not land. No answer.
    DropEncrypt { token: u64 },
    /// Drop the document from the keyhive instance. Synchronous and memory only; no answer.
    Evict { doc: DocId },
    /// Take the keyhive lock exclusively. Answer: [`KeyhiveBoundary::lock_acquired`].
    AcquireLock { call: CallId },
    /// Open the write transaction. Answer: [`KeyhiveBoundary::began`].
    Begin { call: CallId },
    /// Run the operation (`None` is a refill alone), write what keyhive reports into the
    /// transaction, and refill the prekeys if `refill`. Answer: [`KeyhiveBoundary::ran`].
    Run {
        call: CallId,
        op: Option<LockedOp>,
        refill: bool,
    },
    /// Commit the transaction. Answer: [`KeyhiveBoundary::committed`].
    Commit { call: CallId },
    /// Roll the transaction back. Answer: [`KeyhiveBoundary::rolled_back`].
    Rollback { call: CallId },
    /// Drop the keyhive instance and open a new one. Answer: [`KeyhiveBoundary::reopened`].
    Reopen { call: CallId },
    /// Release the exclusive lock. No answer.
    ReleaseLock,
    /// Which of `events` may the session's principal see, per Keyline? Answer:
    /// [`KeyhiveBoundary::eligible`].
    Eligible {
        call: CallId,
        session: SessionId,
        events: BTreeSet<EventId>,
    },
    /// Tell the session that these events committed. No answer.
    Notify {
        session: SessionId,
        events: Vec<EventId>,
    },
    /// Abort a call in flight. Its outcome is still reported.
    Cancel { call: CallId },
    /// The answer to a [`KeyhiveBoundary::prepare_encrypt`].
    Encrypted {
        to: Asker,
        call: CallId,
        outcome: Outcome<EncryptedChange>,
    },
    /// The answer to a [`KeyhiveBoundary::decrypt`].
    Decrypted {
        to: Asker,
        call: CallId,
        outcome: Outcome<Vec<u8>>,
    },
    /// The answer to a [`KeyhiveBoundary::locked`].
    Reply {
        ticket: Ticket,
        result: Result<(), Refused>,
    },
    /// The store failed: the hub tears the instance down. The last command there is.
    Trap { during: Step },
}

/// Bugs planted on purpose, to show the simulation catches them (see `sim`).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plant {
    /// A dropped preparation is applied all the same (law 1).
    ApplyDropped,
    /// Encryptions start while a locked operation runs (law 3).
    EncryptDuringLocked,
    /// A locked operation starts with encryptions in flight (law 3).
    LockWithEncryptionInFlight,
    /// An event is sent back to its source (law 4).
    EchoToSource,
    /// Sessions are told every event they were asked about, eligible or not (law 4).
    NotifyIneligible,
    /// A notified event stays pending and is sent again (law 4).
    NotifyTwice,
    /// A failure skips the reopen (law 2).
    SkipReopen,
    /// An encryption is issued on a document that is not loaded (law 5).
    SkipHydrate,
    /// A run below the prekey floor does not ask for a refill (law 6).
    SkipRefill,
    /// The machine keeps issuing commands after a trap (law 7).
    ContinueAfterTrap,
    /// A decryption is issued on a document that is not loaded (law 5).
    DecryptWithoutHydrate,
    /// A refused refill is never retried (law 8).
    NoRefillRetry,
}

/// What the machine knows of a document in the keyhive instance. Absent means not loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Load {
    Loading(CallId),
    Loaded,
}

enum TokenState {
    /// `Encrypt` is out for `asker`'s `call`.
    Encrypting { asker: Asker, call: CallId },
    /// Answered, waiting for the commit's verdict.
    Prepared { events: Vec<EventId> },
    /// `ApplyEncrypt` is out.
    Applying { call: CallId, events: Vec<EventId> },
}

struct Token {
    doc: DocId,
    state: TokenState,
}

/// What a held request wants done to its document.
enum Work {
    Encrypt(Vec<u8>),
    Decrypt(Vec<u8>),
}

struct Request {
    asker: Asker,
    call: CallId,
    doc: DocId,
    work: Work,
}

struct Queued {
    /// `None` for the refill the machine queued itself.
    ticket: Option<Ticket>,
    op: Option<LockedOp>,
}

enum Phase {
    Locking(CallId),
    Beginning(CallId),
    Running(CallId),
    Committing(CallId, Report),
    RollingBack(CallId),
    Reopening(CallId),
}

struct Active {
    ticket: Option<Ticket>,
    op: Option<LockedOp>,
    /// The run carried the prekey refill.
    refill: bool,
    phase: Phase,
}

#[derive(Default)]
struct Peer {
    /// Committed events the session has not been asked about.
    pending: BTreeSet<EventId>,
    /// New events arrived since the last flush, which stamps their time.
    dirty: bool,
    first: Option<Instant>,
    last: Option<Instant>,
    /// The [`Command::Eligible`] in flight and the events it asked about.
    query: Option<(CallId, BTreeSet<EventId>)>,
}

/// The machine.
pub struct KeyhiveBoundary {
    config: Config,
    next_call: CallId,
    trapped: bool,
    docs: BTreeMap<DocId, Load>,
    evict_wanted: BTreeSet<DocId>,
    tokens: BTreeMap<u64, Token>,
    held: VecDeque<Request>,
    /// Decryptions out, by their call: who asked, under which call, for which document.
    decrypting: BTreeMap<CallId, (Asker, CallId, DocId)>,
    queue: VecDeque<Queued>,
    active: Option<Active>,
    /// Our prekeys left, as last reported.
    prekeys: usize,
    refill_wanted: bool,
    /// Refused refills in a row, which set the backoff.
    refill_failures: u32,
    /// A refusal waits for the next flush to stamp its retry deadline.
    refill_stamp: bool,
    refill_due: Option<Instant>,
    peers: BTreeMap<SessionId, Peer>,
    /// Eligibility queries of sessions that closed: their answers are ignored.
    cancelled: BTreeSet<CallId>,
    pending: Vec<Command>,
    #[cfg(test)]
    plant: Option<Plant>,
}

impl KeyhiveBoundary {
    pub fn new(config: Config) -> Self {
        Self {
            prekeys: config.prekey_floor,
            config,
            next_call: 0,
            trapped: false,
            docs: BTreeMap::new(),
            evict_wanted: BTreeSet::new(),
            tokens: BTreeMap::new(),
            held: VecDeque::new(),
            decrypting: BTreeMap::new(),
            queue: VecDeque::new(),
            active: None,
            refill_wanted: false,
            refill_failures: 0,
            refill_stamp: false,
            refill_due: None,
            peers: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            pending: Vec::new(),
            #[cfg(test)]
            plant: None,
        }
    }

    // ── inputs: requests ────────────────────────────────────────────────────────────────────

    /// Encrypt `change` of `doc` for `asker`, whose `call` the [`Command::Encrypted`] echoes.
    pub fn prepare_encrypt(&mut self, asker: Asker, call: CallId, doc: DocId, change: Vec<u8>) {
        if self.trapped {
            return;
        }
        self.held.push_back(Request {
            asker,
            call,
            doc,
            work: Work::Encrypt(change),
        });
    }

    /// Decrypt `payload` of `doc` for `asker`, whose `call` the [`Command::Decrypted`] echoes.
    pub fn decrypt(&mut self, asker: Asker, call: CallId, doc: DocId, payload: Vec<u8>) {
        if self.trapped {
            return;
        }
        self.held.push_back(Request {
            asker,
            call,
            doc,
            work: Work::Decrypt(payload),
        });
    }

    /// The preparation `token` (from [`Command::Encrypted`]) was committed (`applied`) or will
    /// not be. A token is settled once, after its answer.
    pub fn settle_encrypt(&mut self, token: u64, applied: bool) {
        if self.trapped {
            return;
        }
        let state = self
            .tokens
            .get_mut(&token)
            .expect("settling a preparation that is held");
        let TokenState::Prepared { events } = &mut state.state else {
            panic!("a preparation is settled once, after its answer");
        };
        let events = std::mem::take(events);
        #[cfg(test)]
        let applied = applied || self.plant == Some(Plant::ApplyDropped);
        if applied {
            let call = self.fresh_call();
            let state = self.tokens.get_mut(&token).expect("held");
            state.state = TokenState::Applying { call, events };
            self.pending.push(Command::ApplyEncrypt { call, token });
        } else {
            self.tokens.remove(&token);
            self.pending.push(Command::DropEncrypt { token });
        }
    }

    /// Run a membership change or ingest keyhive events from a peer, under the exclusive lock.
    /// The answer is [`Command::Reply`].
    pub fn locked(&mut self, ticket: Ticket, op: LockedOp) {
        if self.trapped {
            return;
        }
        self.queue.push_back(Queued {
            ticket: Some(ticket),
            op: Some(op),
        });
    }

    /// The hub saw another process move the document's version row: whatever keyhive holds of it
    /// may be stale.
    pub fn evict(&mut self, doc: DocId) {
        if self.trapped {
            return;
        }
        if self.docs.contains_key(&doc) {
            self.evict_wanted.insert(doc);
        }
    }

    /// A peer connected. `session` is new.
    pub fn session_opened(&mut self, session: SessionId) {
        if self.trapped {
            return;
        }
        let old = self.peers.insert(session, Peer::default());
        assert!(old.is_none(), "a session id is never reused");
    }

    /// The connection closed. Nothing more is sent to it.
    pub fn session_closed(&mut self, session: SessionId) {
        if self.trapped {
            return;
        }
        let peer = self.peers.remove(&session).expect("a connected session");
        if let Some((call, _)) = peer.query {
            self.cancelled.insert(call);
            self.pending.push(Command::Cancel { call });
        }
    }

    /// How many of our prekeys are left, as the driver saw it change.
    pub fn prekeys_remaining(&mut self, left: usize) {
        if self.trapped {
            return;
        }
        self.prekeys = left;
        if left < self.config.prekey_floor {
            self.refill_wanted = true;
        } else {
            self.refill_restored();
        }
    }

    // ── flush ───────────────────────────────────────────────────────────────────────────────

    /// Take the commands: first what the inputs since the last flush decided, then what the
    /// state allows now.
    pub fn flush(&mut self, now: Instant, out: &mut Vec<Command>) {
        if !self.trapped || self.planted_continue() {
            self.advance(now);
        }
        out.append(&mut self.pending);
    }

    /// When time must pass for the machine to act: the next debounce expiring, or a refused
    /// refill's retry.
    pub fn next_due(&self) -> Option<Instant> {
        if self.trapped {
            return None;
        }
        self.peers
            .values()
            .filter_map(|peer| self.due(peer))
            .chain(self.refill_due)
            .min()
    }

    /// Whether nothing is queued, in flight or waiting.
    pub fn is_idle(&self) -> bool {
        self.held.is_empty()
            && self.queue.is_empty()
            && self.active.is_none()
            && self.tokens.is_empty()
            && self.decrypting.is_empty()
            && self.refill_due.is_none()
            && !self
                .docs
                .values()
                .any(|load| matches!(load, Load::Loading(_)))
            && self.cancelled.is_empty()
            && self
                .peers
                .values()
                .all(|peer| peer.pending.is_empty() && peer.query.is_none())
    }

    pub fn is_trapped(&self) -> bool {
        self.trapped
    }
}
