//! The Document machine: everything the hive does for one document that is open, wanted or
//! behind, as a pure state machine. It replaces the per-document actor of `big_repo`'s runtime
//! (`DocWorkerMsg`: `PutDoc`, `AcquireInterest`, `CommitDelta`, `ApplySyncSession` and so on)
//! and owns the one live [`automerge::Automerge`] of its document.
//!
//! # What it is, and what it is not
//!
//! The machine is sans-IO in the way of `big_automata`'s `PeerSync` and `Outbox`. It has no
//! awaits, no clock reads, no IO, no channels, no locks and no randomness. Every input is a
//! method on [`Document`]; everything the machine wants done leaves as a [`Command`] from the
//! next [`Document::flush`], which is also the only place the clock is read. A driver owns the
//! machine, executes the commands (storage, the Keyhive boundary, the change listeners), and
//! reports each outcome back through the input that names it. The driver, the Keyhive boundary,
//! the Write coordinator and the Hub are separate pieces that this machine only talks to through
//! commands and inputs, so none of them is needed to test it.
//!
//! One machine exists per document that has a handle, an interest or catch-up work. Which
//! documents have a machine is the Hub's business; so is the idle deadline's consequence: the
//! machine only *reports* that it may be evicted ([`Command::Evictable`]), and the Hub stops it.
//!
//! # The one idea: nothing is visible before it is committed
//!
//! Memory only ever holds committed state *as far as any reader can tell*. A write runs the
//! caller's closure in a transaction on the live document, commits that transaction and takes
//! the bytes of the change it made. From then until the write ends, the live document holds a
//! change that is not yet committed to SQLite, so the machine lets nobody look: reads wait,
//! remote content is not applied, the frontier is not published and the change feed is not
//! notified. The change is encrypted by the Keyhive boundary (which does not change memory),
//! committed to SQLite together with its other rows, and only when the commit reports success
//! does the write end: the document is released, held reads are served and the caller is
//! answered. If the write fails or is aborted instead, the machine throws the live document
//! away and hydrates a new one from committed state, then serves the held reads and later
//! writes from that. A reader therefore never observes a change before `committed`, a failed
//! write leaves nothing behind in memory or on disk, and a crash loses nothing that was ever
//! visible. The price of the design is that a failed write costs a hydration; failures are
//! rare, and a write never copies the document.
//!
//! Changes made by somebody else are written the same way. [`Document::ingest`] takes
//! automerge changes that an external author built on a document of their own and sends as
//! bytes. They are applied to the live document, persisted and released exactly like a local
//! write, with no caller closure: the same pipeline, the same exclusions, the same laws. The
//! multi-document shape builds on this. A caller that locks several documents and mutates
//! their live docs does so with a `BigDocHandle`-level lock on its own side; that lock is not
//! in this machine, which sees one write on one document at a time. The back end, which takes
//! `(document, change bytes)` for N documents from local handles or external authors, is a
//! [`Document::prepare`] or [`Document::ingest`] on each document and one commit by the Write
//! coordinator.
//!
//! # The state machines
//!
//! The machine is four small state machines whose transitions are each written in one place.
//! The enums are private; their names are the vocabulary of the laws.
//!
//! **Phase** (`Phase`): whether there is a live document.
//! - `Cold`: no live document. The machine hydrates when somebody wants it: a handle, an
//!   interest, a queued write or read, or a published frontier that is behind the tree.
//! - `Hydrating { call }`: a [`Command::Hydrate`] is out. Its answer is every change of the
//!   document that can be read now. If some content has no key yet, the answer says which
//!   ([`Materialized::blocked`]), and the machine does not guess: it parks.
//! - `Live`: the document is in memory, with `applied_rev`, the tree revision through which its
//!   content has been applied or found blocked, and the remote materialization state below.
//! - `Parked { missing }`: hydration found content with no key. Nothing is readable or
//!   writable until the keyhive epoch for this document moves ([`Document::epoch_moved`]),
//!   which sends the machine back to `Cold` to try again. Reads and writes wait while parked.
//!
//! **Writes** (`Writes`): the commit pipeline, at most one write at a time, which is what gives
//! per-document order without any further mechanism. Later writes wait in a FIFO queue.
//! - `Idle`.
//! - `Encrypting { call }`: the change is made (it is in the live document, which nobody may
//!   look at) and out for encryption.
//! - `Committing { call }`: the encrypted change, its frontier row and the epoch it was
//!   encrypted under are out as a [`Command::Commit`]. If the commit reports the epoch stale (a
//!   membership change landed since), nothing was written: the machine encrypts the same
//!   change again, which is the only part that depends on the epoch, and commits again, up to
//!   [`Config::max_stale_retries`] times, and then answers [`Rejected::Stale`].
//! - `HeldForPlan { plan }`: the document was prepared for a multi-document plan (see below)
//!   and waits for [`Document::apply`] or [`Document::abort`]. Nothing else is prepared or
//!   committed on the document until then.
//!
//! **Remote materialization** (`Remote`, inside `Live`): content other nodes committed. The
//! Hub reports the newest tree revision it knows ([`Document::tree_changed`]); the machine asks
//! the driver for the changes it lacks.
//! - `UpToDate`: `applied_rev == known_rev`.
//! - `Behind`: the tree is ahead of what has been applied.
//! - `Applying { call }`: a [`Command::Decrypt`] is out.
//! - `Blocked { refs }`: the last attempt found content with no key. The document stays live and
//!   usable; it tries again on the next epoch move or tree change.
//!
//! **Frontier publication** (`Frontier`): the published `(heads, rev)` of the document is a row
//! downstream consumers follow. It is `Current` when it reflects the live heads at
//! `applied_rev`, `Stale` when it does not, and `Publishing { call, rev, heads }` while a
//! [`Command::PublishFrontier`] is out. A local write publishes its frontier in its own commit
//! rather than in a second transaction. A document that is blocked is never published: a frontier
//! for half a document would stop the Hub from finding it stale once the keys arrive.
//!
//! These four are not fully independent. While a write is anywhere in the pipeline the live
//! document holds a change that is not committed, so three things wait for it: reads, the
//! publication of the frontier (its heads would include the change) and the notification of
//! listeners (likewise). Remote materialization does not wait. automerge is a CRDT: remote
//! changes may be applied to the live document while a local write is in flight, in either
//! order, with the same result. If the write then fails, the machine hydrates again from
//! committed state, which keeps every durable remote change and drops the local one. A write
//! The frontier's rows are written one at a time: a write does not start while a publication
//! is out, and the frontier is not published while a write is in flight. That ordering is the
//! machine's own. It cannot be left to the document's driver, because the commit of a
//! multi-document plan is run by the Write coordinator, outside it: a publication still in
//! flight could land after the plan's row and take the frontier back (the simulation shows this,
//! `planted_write_while_publishing_is_caught`).
//!
//! The frontier row of a local write is the one place a compare-and-set stays. The row
//! carries `if_prior_rev`, the revision the tree was at when the change was made, and the store
//! writes the row only if the tree is still there. Ordering alone would keep the rows in order,
//! but not truthful: remote content that landed in the tree and has not been applied here is
//! missing from the row's heads, yet the row would claim a revision that contains it. The
//! simulation shows this (`without_the_compare_and_set_a_frontier_row_claims_unapplied_content`).
//!
//! # Inputs
//!
//! From handles: [`Document::opened`], [`Document::handle_dropped`],
//! [`Document::interest_acquired`], [`Document::interest_released`], [`Document::write`] (the
//! closure builds a change against a transaction, as synchronous CPU work inside the step) and
//! [`Document::read`] (a closure over the live document, which only ever holds committed
//! state). A [`Handle`] carries the machine's generation, so a handle of an earlier machine is
//! recognised and refused.
//!
//! From the Hub: [`Document::tree_changed`] (the change feed) and [`Document::epoch_moved`]
//! (the keyhive epoch for this document moved, with whether the document is readable now).
//!
//! From the Write coordinator: [`Document::prepare`], [`Document::apply`] and
//! [`Document::abort`].
//!
//! Outcomes: [`Document::hydrated`], [`Document::decrypted`], [`Document::encrypted`],
//! [`Document::committed`] and [`Document::published`]. Each names the [`CallId`] of its
//! command, and an outcome for a call that is not in flight changes nothing. A call ends as
//! [`Outcome::Done`], [`Outcome::Failed`] or [`Outcome::Cancelled`].
//!
//! # Commands
//!
//! [`Command::Hydrate`] and [`Command::Decrypt`] go to the driver, which reads the committed
//! content and keys, decrypts it through the Keyhive boundary and answers with automerge
//! changes. [`Command::PrepareEncrypt`] goes to the Keyhive boundary. [`Command::Commit`]
//! carries a [`Plan`]: a plain value naming the rows by kind, never SQL. [`Command::SettleEncrypt`]
//! tells the Keyhive boundary whether a prepared encryption was applied or dropped.
//! [`Command::Prepared`] answers the Write coordinator. [`Command::Reply`] answers a handle.
//! [`Command::Notify`] tells change listeners that heads moved. [`Command::PublishFrontier`]
//! writes the frontier row. [`Command::Cancel`] asks the driver to abort a call, which still
//! reports. [`Command::Evictable`] reports an idle machine to the Hub.
//!
//! # Multi-document plans
//!
//! The Write coordinator sends [`Document::prepare`] to each document of a plan. The machine
//! makes the change on the live document, as for a write, and answers
//! [`Command::Prepared`] with the change bytes and the frontier row the plan must carry. It
//! then holds the document until [`Document::apply`] (the plan committed: apply the change)
//! or [`Document::abort`] (it did not). A single-document write is the same protocol run by
//! the machine itself.
//!
//! A plan holds every document it has prepared until all of them are, so it must not wait for
//! something that may never come. A parked document therefore refuses to prepare, whether the
//! plan reached it before or after it parked, and the coordinator aborts the plan. An unreadable
//! document refuses too ([`Rejected::Unreadable`]). A document that is merely hydrating or
//! busy answers when it can: that wait ends.
//!
//! # Laws
//!
//! 1. **Readers see only committed state.** No read is served, and no notification is sent,
//!    while the live document holds a change that is not committed. A failed or aborted write
//!    leaves nothing: the live document is rebuilt from committed state before anybody looks
//!    again. (A reader could otherwise see state a crash erases.)
//! 2. **Per-document order.** Writes apply in submission order, and each is built on a state
//!    that includes every earlier write. (One in flight at a time, FIFO.)
//! 3. **Read your writes.** A handle whose write returned sees that write: the reply is sent
//!    once the change is committed, and reads wait for the write that is in flight.
//! 4. **Plans hold the document.** While prepared for a plan, no other write is prepared or
//!    committed on the document; on `apply` or `abort` the held writes resume in order.
//!    (Otherwise a plan's commit could be built on a state the plan's other documents never
//!    saw.)
//! 5. **Frontier.** `frontier_rev <= applied_rev <= known_rev`. A local write publishes its
//!    frontier in its own commit; remote content publishes once applied. A readable document is
//!    never left with `frontier_rev < known_rev` and nothing in flight, parked or blocked.
//!    A frontier is never ahead of what was applied.
//! 6. **Parked means blocked.** A parked document lacks keys for some content, and leaves
//!    `Parked` on `epoch_moved` by trying again.
//! 7. **Eviction.** [`Document::evictable`] holds only with no handles, no interest, nothing in
//!    flight and nothing held, and no catch-up work left. Evicting loses nothing, because
//!    everything live is committed (no write is in flight to leave an uncommitted change).
//! 8. **Unreadable documents are never hydrated.** No `Hydrate` or `Decrypt` is issued while
//!    the machine has been told the document is unreadable.
//!
//! # How a local commit and remote content meet
//!
//! Tree revisions come from one sequence for the hive, and the feed reports a tree only at its
//! latest revision, so a local commit at revision `R` may have replaced the feed row of a remote
//! commit that landed just before it. The machine therefore advances `applied_rev` to `R` only
//! when [`Landed::prior_rev`], the revision the tree had inside the commit's transaction, equals
//! the `applied_rev` it had; otherwise it stays behind and fetches. The frontier row of a plan
//! carries `if_prior_rev` for the same reason: the store writes it only if the tree is still at
//! that revision, so it can never claim content the machine has not applied.
//!
//! # Failures
//!
//! A write that fails (encryption or commit) is answered to its caller with
//! [`Rejected::Failed`] and never retried here: nothing was applied, and the caller knows what
//! to do. A hydration, decryption or publication that fails is retried on a backoff ladder
//! ([`Config`]); the clock is read in `flush`, so a failure only marks the machine and the next
//! flush fixes the deadline. A cancelled call frees its slot and costs no backoff. A caller's
//! closure that fails is a refusal ([`Rejected::Op`]), not a crash: it is the caller's input.
//! A broken invariant (a plan applied that holds nothing, a read that goes back in time) panics.
//!
//! # Where the machine's neighbours come in
//!
//! The Hub starts and stops machines, forwards the feed and epoch moves, and owns the catch-up
//! scheduler that wakes documents whose frontier is stale. The Write coordinator drives plans.
//! The Keyhive boundary does hydration decryption and encryption, and the driver runs the SQL.
//! None of them is in this module. Their interfaces here are the commands and inputs above,
//! kept abstract so each lane can change its side without touching the machine's logic.
//!
//! # Open questions and hooks
//!
//! - Author filtering at read time (automerge 0.12's author APIs): the hook is the answer to
//!   `Hydrate` and `Decrypt`, which are changes the driver has already filtered.
//! - Write pipelining: one commit in flight per document. The `Writes` enum is where
//!   pipelining would change.
//! - Checkpoint coverage: not here; it can ride `epoch_moved` or a wake from the Hub.
//! - A bound on the held-write queue (refuse writes when it is full): not built.

mod inputs;
mod pump;
#[cfg(test)]
mod sim;
#[cfg(test)]
pub(crate) mod tests;

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::time::Duration;

use automerge::ActorId;
use automerge::Automerge;
use automerge::AutomergeError;
use automerge::Change;
use automerge::ChangeHash;
use automerge::transaction::Transaction;
use big_automata::rev::Rev;

pub use web_time::Instant;

/// The id of a command that expects an answer, fresh per command, never reused.
pub type CallId = u64;

/// The caller's name for a read or a write, echoed in its [`Command::Reply`]. Unrelated to
/// [`CallId`]: the machine never interprets it.
pub type Ticket = u64;

/// A multi-document plan, named by the Write coordinator.
pub type PlanId = u64;

/// A write's closure: builds a change in a transaction on the live document. Its output is
/// delivered once the change is committed.
pub type WriteOp<O> = Box<dyn FnOnce(&mut Transaction<'_>) -> Result<O, AutomergeError> + Send>;

/// A read's closure over the live document, which only ever holds committed state.
pub type ReadOp<O> = Box<dyn FnOnce(&Automerge) -> O + Send>;

#[derive(Debug, Clone)]
pub struct Config {
    /// How long an evictable machine waits before it reports [`Command::Evictable`].
    pub idle_ttl: Duration,
    /// The backoff ladder of calls that failed: hydrating, decrypting and publishing. A failed
    /// write is answered to its caller and never retried here.
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    /// How many times a write whose commit found its epoch stale encrypts again before it is
    /// answered with [`Rejected::Stale`].
    pub max_stale_retries: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            idle_ttl: Duration::from_secs(30),
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_stale_retries: 3,
        }
    }
}

/// What the Hub knows when it starts a machine.
#[derive(Debug, Clone)]
pub struct Start {
    /// Distinguishes this machine from every earlier one of the document; handles carry it.
    pub generation: u64,
    /// The automerge actor this machine's changes are made under. It must be fresh for every
    /// machine: a restarted machine reusing an actor whose last change it has not seen would
    /// repeat a sequence number. (The machine takes no randomness, so a simulation replays.)
    /// After a commit that failed or was left undecided the machine asks for another
    /// ([`Command::NewActor`]), for the same reason: the commit may have landed after all, and
    /// the next change must not collide with it.
    pub actor: ActorId,
    pub readable: bool,
    /// The tree's revision now.
    pub known_rev: Rev,
    /// The revision the document's published frontier reflects.
    pub frontier_rev: Rev,
}

/// A handle to the machine that issued it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Handle {
    pub generation: u64,
    pub id: u64,
}

/// A call's terminal event. Every call reports exactly once; a cancelled call may report its
/// real result if it raced the cancel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<T> {
    Done(T),
    Failed,
    Cancelled,
    /// A commit only: the transaction found that the epoch of document `doc` is not the one the
    /// plan was encrypted under, and wrote nothing.
    Stale {
        doc: u64,
    },
}

/// A document's key epoch: the version of its CGKA state that content is encrypted under. A
/// membership change moves it.
pub type Epoch = u64;

/// Content that has no key yet, named by what the driver knows it by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ContentRef(pub [u8; 32]);

/// What reading the document's content found: the automerge changes it could decrypt, the tree
/// revision it read them at, and the content it could not.
#[derive(Debug, Clone)]
pub struct Materialized {
    pub changes: Vec<Change>,
    pub rev: Rev,
    /// The revision the published frontier row reflects, read in the same snapshot. After a
    /// commit that was left undecided this is how the machine learns whether its frontier row
    /// was written.
    pub frontier_rev: Rev,
    pub blocked: Vec<ContentRef>,
}

/// A change after the Keyhive boundary encrypted it, not yet applied there. The machine does
/// not look inside; `token` names the preparation for [`Command::SettleEncrypt`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedChange {
    pub token: u64,
    pub ciphertext: Vec<u8>,
    /// The document's key epoch the content was encrypted under.
    pub epoch: Epoch,
}

/// What a commit's transaction reports for this document's tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Landed {
    /// The revision the commit's batch drew.
    pub rev: Rev,
    /// The revision the tree had inside the same transaction, before the batch.
    pub prior_rev: Rev,
}

/// The frontier row that goes with a change: the heads after it, written only if the tree's
/// revision before the batch is `if_prior_rev`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontierRow {
    pub heads: Vec<ChangeHash>,
    pub if_prior_rev: Rev,
}

/// The rows of one document's write, by kind. The driver turns them into the stores' rows (the
/// commit, the tree revision bump and pruning, the content key, any CGKA rows, the part store
/// and outbox rows) in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub change: EncryptedChange,
    pub frontier: Option<FrontierRow>,
    /// The commit is refused ([`Outcome::Stale`]) if the document's epoch is no longer this.
    /// Only the plan's own documents are checked.
    pub if_epoch: Option<Epoch>,
}

/// What the Write coordinator gets back from [`Document::prepare`].
#[derive(Debug, Clone, PartialEq)]
pub struct PlanPrepared<O> {
    /// The change's bytes, or `None` if nothing changed.
    pub change: Option<Vec<u8>>,
    pub frontier: Option<FrontierRow>,
    /// The closure's output; `None` for an ingest.
    pub out: Option<O>,
}

/// Why a request was refused. Nothing was changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejected {
    /// The handle is not open on this machine (it was dropped, or it is from an earlier one).
    StaleHandle,
    Unreadable,
    /// The document is parked on keys that have not arrived, which a plan cannot wait for while
    /// it holds other documents.
    Parked,
    /// The caller's closure failed.
    Op(String),
    /// Encrypting or committing failed.
    Failed,
    /// The document's key epoch kept moving between encryption and commit, past the retry limit.
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Local,
    Remote,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command<O> {
    /// Read every change of the document that can be read now.
    Hydrate {
        call: CallId,
    },
    /// Read the changes the document lacks, given its `heads`, at a tree revision of at least
    /// `rev`. The heads may name the change of a write in flight, which is not committed: the
    /// driver answers with what it can prove the document lacks.
    Decrypt {
        call: CallId,
        heads: Vec<ChangeHash>,
        rev: Rev,
    },
    PrepareEncrypt {
        call: CallId,
        change: Vec<u8>,
    },
    Commit {
        call: CallId,
        plan: Plan,
    },
    /// The preparation `token` was committed (`applied`) or will not be (dropped).
    SettleEncrypt {
        token: u64,
        applied: bool,
    },
    Prepared {
        plan: PlanId,
        result: Result<PlanPrepared<O>, Rejected>,
    },
    Reply {
        ticket: Ticket,
        result: Result<O, Rejected>,
    },
    /// The answer to [`Document::ingest`]: the changes are committed and applied, or were not.
    Ingested {
        ticket: Ticket,
        result: Result<(), Rejected>,
    },
    /// The live document's heads moved.
    Notify {
        origin: Origin,
        heads: Vec<ChangeHash>,
    },
    PublishFrontier {
        call: CallId,
        heads: Vec<ChangeHash>,
        rev: Rev,
    },
    /// Abort a call in flight. Its outcome is still reported.
    Cancel {
        call: CallId,
    },
    /// Ask the Hub for a fresh automerge actor, answered by [`Document::actor_assigned`]. The
    /// machine asks after a commit that failed or was left undecided, and waits for the answer
    /// before it hydrates again.
    NewActor {
        call: CallId,
    },
    /// The machine has been idle for the configured time and may be stopped.
    Evictable,
}

struct Live {
    doc: Automerge,
    applied_rev: Rev,
    remote: Remote,
}

enum Phase {
    Cold,
    Hydrating { call: CallId },
    Live(Box<Live>),
    Parked { missing: Vec<ContentRef> },
}

enum Remote {
    UpToDate,
    Behind,
    Applying { call: CallId },
    Blocked { refs: Vec<ContentRef> },
}

enum Frontier {
    Current,
    Stale,
    Publishing {
        call: CallId,
        rev: Rev,
        heads: Vec<ChangeHash>,
    },
}

/// What a write is: a caller's closure, or changes an external author built.
pub enum Work<O> {
    Op(WriteOp<O>),
    Changes(Vec<Change>),
}

impl<O> std::fmt::Debug for Work<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Work::Op(_) => write!(f, "Op(..)"),
            Work::Changes(changes) => write!(f, "Changes({})", changes.len()),
        }
    }
}

/// A change made on the live document, waiting for its commit.
struct Built<O> {
    /// The change's bytes, which an encryption that has to be repeated is made from again.
    bytes: Vec<u8>,
    /// The frontier row the commit carries, if the document was in a state to publish.
    frontier: Option<FrontierRow>,
    /// The closure's output; `None` for an ingest, which has no closure.
    out: Option<O>,
}

/// What a write did to the live document.
struct Drafted<O> {
    /// The bytes of the change to commit; `None` if nothing changed.
    bytes: Option<Vec<u8>>,
    frontier: Option<FrontierRow>,
    out: Option<O>,
}

enum Writes<O> {
    Idle,
    Encrypting {
        call: CallId,
        ticket: Ticket,
        built: Built<O>,
        /// Encryptions repeated so far because the commit found its epoch stale.
        attempts: u32,
    },
    Committing {
        call: CallId,
        ticket: Ticket,
        token: u64,
        built: Built<O>,
        attempts: u32,
    },
    /// Prepared for `plan`. The change is already in the live document.
    HeldForPlan {
        plan: PlanId,
        frontier: Option<FrontierRow>,
        /// Whether the work changed anything: a plan that did not has nothing to apply, and
        /// aborting it costs nothing.
        changed: bool,
    },
}

enum Queued<O> {
    Write { ticket: Ticket, work: Work<O> },
    Plan { plan: PlanId, work: Work<O> },
}

/// The idle deadline: armed while the machine is evictable.
enum IdleClock {
    Busy,
    Counting(Instant),
    Reported,
}

/// The backoff of failed hydrating, decrypting and publishing. A failure only marks `arm`;
/// the next flush reads the clock and fixes the deadline.
#[derive(Default)]
struct Retry {
    failures: u32,
    arm: bool,
    until: Option<Instant>,
}

/// Bugs planted on purpose, to show the simulation catches them (see `tests`).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plant {
    /// Reads are served while a write is in flight, and see its uncommitted change (law 1).
    ServeReadsDuringWrite,
    /// The queue is served last in, first out (law 2).
    LifoQueue,
    /// The frontier is published at the newest known revision (law 5).
    FrontierAhead,
    /// A prepared plan does not hold the document (law 4).
    PlanDoesNotHold,
    /// An unreadable document is hydrated (law 8).
    HydrateUnreadable,
    /// A local commit is taken to have applied everything below its revision, without the
    /// check against the revision the tree had before it (laws 1 and 5).
    TrustLandedRev,
    /// A commit found stale is applied as if it had landed (law 7).
    ApplyOnStale,
    /// The frontier is published while a write is in flight, so its heads include a change that
    /// is not committed (law 1).
    PublishDuringWrite,
    /// A remote change applied during a write notifies listeners of heads that include the
    /// write's uncommitted change (law 1).
    NotifyDuringWrite,
    /// The machine keeps its actor after a failed commit, so the next change reuses the failed
    /// one's sequence number (laws 1 and 2).
    ReuseActor,
    /// A commit's frontier row is taken to be written when the tree is at the machine's current
    /// applied revision, not at the revision the row expected: remote content applied during
    /// the write makes the two differ (law 5).
    FrontierByCurrentRev,
    /// A write starts while the frontier is being published, so a plan's commit can land before
    /// the publication and the row goes back (law 5).
    WriteWhilePublishing,
}

/// The machine. `O` is what a read or a write closure returns to its caller.
pub struct Document<O> {
    config: Config,
    generation: u64,
    actor: ActorId,
    readable: bool,
    handles: BTreeSet<u64>,
    next_handle: u64,
    interest: u32,
    idle: IdleClock,
    /// The newest tree revision reported.
    known_rev: Rev,
    /// The revision the published frontier reflects.
    frontier_rev: Rev,
    phase: Phase,
    frontier: Frontier,
    writes: Writes<O>,
    queue: VecDeque<Queued<O>>,
    reads: VecDeque<(Ticket, ReadOp<O>)>,
    next_call: CallId,
    /// Cancelled calls the driver has yet to report; their outcomes change nothing.
    stopping: BTreeSet<CallId>,
    /// Commands decided by an input, ahead of the next flush's.
    pending: Vec<Command<O>>,
    retry: Retry,
    /// A call out for a fresh actor: nothing is hydrated until it is answered.
    awaiting_actor: Option<CallId>,
    /// A failed write threw away the live document, and remote changes applied since the last
    /// notification may have gone with it: the next hydration notifies.
    renotify: bool,
    #[cfg(test)]
    plant: Option<Plant>,
}

impl<O> Document<O> {
    pub fn new(config: Config, start: Start) -> Self {
        assert!(
            start.frontier_rev <= start.known_rev,
            "a frontier cannot be ahead of the tree"
        );
        Self {
            config,
            generation: start.generation,
            actor: start.actor,
            readable: start.readable,
            handles: BTreeSet::new(),
            next_handle: 0,
            interest: 0,
            idle: IdleClock::Busy,
            known_rev: start.known_rev,
            frontier_rev: start.frontier_rev,
            phase: Phase::Cold,
            frontier: if start.frontier_rev == start.known_rev {
                Frontier::Current
            } else {
                Frontier::Stale
            },
            writes: Writes::Idle,
            queue: VecDeque::new(),
            reads: VecDeque::new(),
            next_call: 0,
            stopping: BTreeSet::new(),
            pending: Vec::new(),
            retry: Retry::default(),
            awaiting_actor: None,
            renotify: false,
            #[cfg(test)]
            plant: None,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn fresh_call(&mut self) -> CallId {
        self.next_call += 1;
        self.next_call
    }
}
