//! Sessions and interest: which connections to peers exist, as which of our principals, and which
//! wanted objects are routed over them.
//!
//! Two sans-IO machines, generic over every identity they handle, so that every caller of
//! `big_automata` with objects to fetch over peers can use them: `big_hive` instantiates them
//! with document ids, and `big_blobs` with blob hashes. Neither names documents, keys, Keyline or
//! keyhive; what the caller knows about *standing* (which `(peer, principal)` pairs may serve an
//! object) is an input.
//!
//! - [`interest::Interest`] holds the leases on objects (and the 30 s grace after the last one)
//!   and decides on which open sessions each object is routed, and which sessions it needs.
//! - [`session::Sessions`] holds the sessions: dialing with backoff, caps per peer and in total,
//!   and closing after an idle grace. A peer's *default* session (our own principal) stays for as
//!   long as the peer is connected; any other session exists only while some object needs it.
//!
//! The owner of both (the Hub in `big_hive`) wires them: `Interest`'s
//! [`NeedSession`](interest::Command::NeedSession) and
//! [`ReleaseSession`](interest::Command::ReleaseSession) are `Sessions::need` and
//! `Sessions::release`; `Sessions`'s dial outcomes and losses become
//! `Interest::session_opened` and `Interest::session_closed`. A driver does the dialing and the
//! closing, and applies the route commands to whatever carries routes (a replay session's route
//! set, a blob provider list).
//!
//! Why this is a public module of its own and not part of `kit`: `kit` holds the building blocks
//! that know nothing about peers (the scheduler, the watermark), while these are policy machines
//! with their own laws, like `durable_outbox` and `replay`.

pub mod interest;
pub mod session;

#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;

/// The bugs the simulation must catch, planted into the machines on purpose.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plant {
    /// Interest routes on every open session, not only where standing allows.
    RouteEverywhere,
    /// Interest asks for on-demand sessions even when a default session can serve.
    EagerShared,
    /// Interest applies grants of standing but never revokes.
    IgnoreRevoke,
    /// Sessions never closes an idle session.
    NeverClose,
    /// Sessions admits past its caps.
    NoCaps,
}
