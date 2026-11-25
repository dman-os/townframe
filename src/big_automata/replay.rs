//! The replay session protocol: how a peer pulls a part store's log from another peer.
//!
//! This module owns the protocol's semantics. Everything that crosses the wire is in [`wire`];
//! the client and server are sans-IO state machines; the responder turns one page request into
//! one authorized read of the part store. The store, the transport and the scheduling policy
//! sit underneath or on top and may change. What is written here is the contract between two
//! nodes, and it has to keep holding across versions.
//!
//! # Nouns
//!
//! * **Session**: one target set the server holds for one client, named by a client-chosen
//!   [`wire::SessionId`]. A client keeps one session per peer. A server may forget a session at
//!   any time (TTL, restart); the client then opens a new one.
//! * **Target**: a [`wire::Route`] (a part, or a single object) held in a session under a
//!   client-chosen [`wire::TargetId`]. Ids are never reused within a session.
//! * **Generation**: the number an update carries. Updates are deltas, so the generation is
//!   what makes them idempotent and ordered.
//! * **Round**: one page request over some of the session's targets, each with the cursor the
//!   client resumes it from.
//! * **Verdict**: the per-target answer of a round, [`wire::Verdict::Events`] or
//!   [`wire::Verdict::Refused`].
//!
//! # Verbs
//!
//! [`wire::UpdateSession`] opens a session or changes its target set; [`wire::PageSession`]
//! runs one round; [`wire::CloseSession`] drops the session. Cancelling a round is the
//! transport's request cancellation: the protocol has no message for it.
//!
//! # Laws
//!
//! **Sessions and updates**
//!
//! 1. *Opening.* An update for a session the server does not hold applies only at generation
//!    0, and opens the session with the update's additions as its whole target set. Any other
//!    generation is answered with [`wire::SessionError::UnknownSession`].
//! 2. *Idempotence.* The server applies an update only if its generation is greater than the
//!    one it holds. Otherwise it applies nothing and answers with the generation it holds and
//!    the refusals that generation produced, so a client retrying an update whose answer was
//!    lost reads the same outcome it would have read the first time.
//! 3. *Delta order.* Removals apply before additions, so one update can drop a target and
//!    re-add the same route under a fresh id. Removing an id the session does not hold is a
//!    no-op, so a client may re-send removals whose acknowledgement it never saw.
//! 4. *Admission.* An addition is held only if its route is known to the server and readable by
//!    the client at update time. Every other addition is named in `refused` and not held. A
//!    refusal does not say whether the route is unknown or unreadable: a refusal that looked
//!    different would confirm that a part exists. The client keeps a refused route wanted and
//!    re-adds it later under a fresh id; only the client ever drops a route.
//! 5. *Forgetting.* [`wire::CloseSession`], a TTL sweep and a server restart all have the same
//!    effect: the session is gone, and the next update or round naming it is answered with
//!    `UnknownSession`. The client recovers by opening a new session (fresh id, generation 0)
//!    with its whole wanted set. Nothing about a forgotten session can corrupt the new one,
//!    because the ids differ.
//!
//!    *Revival.* A delayed copy of a session's opening update (generation 0) that arrives after
//!    the server forgot the session opens it again under the same id, and nothing tells the
//!    client. The revived session holds the opening's targets, not the client's current set:
//!    - a target the client added since is missing: the next round naming it is answered
//!      `UnknownTarget` (law 6), and the client recovers as from `UnknownSession`, with a new
//!      session;
//!    - a target the client removed since is held again, but the client never names it in a
//!      round, so it carries nothing, and it goes when the revived session expires.
//!
//!    So a revival costs the server a stale session until its TTL and the client one reset,
//!    and never delivers a row the client did not ask for. (The Quint model's `laws`
//!    configuration and the Rust simulation both reach this interleaving; see `replay/spec`.)
//!
//! **Rounds**
//!
//! 6. *Ids.* A round names targets by id with a cursor each. A round naming an id the session
//!    does not hold is answered with [`wire::SessionError::UnknownTarget`]. That is a client bug,
//!    except after a revival (law 5), when it is how the client learns its session is not the
//!    one it believes, and it opens a new one.
//! 7. *Current access, one snapshot.* Each target's verdict is decided from the client's access
//!    at the time of the round, in the same snapshot as the rows the round reads. A target the
//!    client may not read now is [`wire::Verdict::Refused`] and carries no rows, whatever the
//!    revision of the rows it has not read yet: access granted in the past is never a grant
//!    for rows the client did not take while it had it.
//! 8. *No skip.* For a target answered `Events { resume, .. }`, the round carried every row of
//!    the target's route in `(cursor, resume]`, collapsed to the latest row per key. Rows past
//!    `resume` are left for a later round. `resume` never goes below the round's cursor, so a
//!    client that resumes from it neither skips nor goes backwards.
//! 9. *At least once.* The server keeps no cursor. A round the client never received (lost,
//!    cancelled, failed) changes nothing on either side, and the client repeats it from the
//!    same cursor. Rows can therefore arrive more than once; they never arrive out of order
//!    within a target.
//! 10. *Drained.* `drained: true` means the target's read reached the newest revision of the
//!     round's snapshot. It is a hint for pacing, never "the replay is over".
//! 11. *Every removal after the cursor.* The server delivers every removal of a membership
//!     after the target's cursor, whatever rev the membership first went live at. The client's
//!     applied state can be ahead of its cursor: the persisted cursor only passes settled work,
//!     so a long fetch of one object holds it while a later membership is applied locally. If
//!     that membership is removed and the client restarts (or unwants and wants the part
//!     again) before applying the removal, a replay from the cursor would skip the add and the
//!     removal together unless the removal is sent, and the member would stay locally for good
//!     (found by the stack simulation). The price is removal rows for members the client may
//!     never have held. A client applies a round's answer for a target only while its cursor
//!     for that target is still the cursor the round was asked from; an answer to an older
//!     round, overtaken by a newer one, is dropped, so rows of a stale answer never sit next
//!     to a newer round's.
//! 12. *Object targets carry content.* An object target delivers the object's changes in the
//!     parts the client may read, not its membership removals; those travel on part targets.
//!     An object target is refused while the object is live in no part the client may read.
//! 13. *Hold.* A round whose targets have no rows may wait up to its `hold_ms` (capped by the
//!     server) for one to appear. Cancelling the request ends the wait. Waiting never changes
//!     what a round may carry.
//!     The wait is a [long-poll](crate::long_poll) on the routes the round names, so a commit on
//!     other routes neither wakes it nor costs it a read, and a server may linger a short while
//!     after the first commit that concerns it (up to a full page) to answer a burst as one
//!     page.
//!
//! **Limits**
//!
//! 14. The server caps targets per session, sessions per client, the page `limit` and
//!     `hold_ms`. A request over a cap on targets or sessions is refused whole with
//!     [`wire::SessionError::Limit`]; `limit` and `hold_ms` are silently capped.

pub mod client;
pub(crate) mod lanes;
pub mod responder;
pub mod server;
pub mod wire;

#[cfg(test)]
mod tests;
