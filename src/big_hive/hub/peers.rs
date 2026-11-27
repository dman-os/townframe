//! The Hub's connections and routes: the wiring of `Interest` and `Sessions` (see the "Sessions
//! and routes" section of the [parent](super)).

use std::collections::BTreeMap;
use std::fmt::Debug;

use big_automata::kit::Instant;
use big_automata::sessions::interest::Command as Routed;
use big_automata::sessions::interest::Tier;
use big_automata::sessions::session::Command as Dialed;

use super::Command;
use super::Hub;
use super::Plant;
use super::rounds::PeerRound;
use crate::coordinator::DocId;
use crate::document::CallId;

impl<P, L> Hub<P, L>
where
    P: Ord + Clone + Debug,
    L: Ord + Clone + Debug,
{
    /// A peer connected. `default_principal` is the principal of the session that exists toward
    /// it whatever anyone needs (our own node principal).
    pub fn peer_connected(&mut self, peer: P, default_principal: L) {
        if self.life != super::Life::Open {
            // draining: no session is dialed, and no round started
            return;
        }
        self.sessions
            .peer_connected(peer.clone(), default_principal);
        let previous = self.peers.insert(peer, PeerRound::new());
        assert!(previous.is_none(), "a peer connected twice");
    }

    /// A peer's connection is gone, and its sessions and round with it.
    pub fn peer_disconnected(&mut self, peer: &P) {
        if self.life == super::Life::Open {
            self.sessions.peer_disconnected(peer);
        }
        self.drop_rounds(peer);
    }

    /// Which `(peer, principal)` sessions may serve the document: those whose peer and whose
    /// principal both stand at Relay or above on it, computed by the caller from Keyline. The
    /// full replacement; empty when standing is revoked. Ignored for a document nobody holds
    /// interest in, so the caller sends it after [`Hub::interest_acquired`].
    pub fn standing(&mut self, doc: DocId, eligible: BTreeMap<(P, L), Tier>) {
        if self.planted(Plant::IgnoreRevoke) && eligible.is_empty() {
            return;
        }
        self.interest.standing(&doc, eligible);
    }

    /// The remote refused the document on the session.
    pub fn refused(&mut self, doc: DocId, session: (P, L)) {
        self.interest.refused(&doc, &session);
    }

    /// The outcome of a [`Command::Dial`].
    pub fn dialed(&mut self, now: Instant, call: CallId, ok: bool) {
        let session = self
            .dials
            .remove(&call)
            .expect("an outcome for a dial that is not out");
        self.sessions.dialed(now, call, ok);
        if ok {
            self.interest.session_opened(session);
        }
    }

    /// An open session was lost, not closed by us.
    pub fn session_lost(&mut self, now: Instant, peer: P, principal: L) {
        self.interest
            .session_closed(&(peer.clone(), principal.clone()));
        self.sessions.closed(now, peer, principal);
    }

    pub(super) fn sessions_quiet(&self) -> bool {
        self.dials.is_empty()
    }

    /// Carry what `Interest` and `Sessions` say to each other and to the driver, until neither
    /// says anything more.
    pub(super) fn wire_sessions(&mut self, now: Instant) {
        loop {
            let mut quiet = true;
            let mut routed = vec![];
            self.interest.flush(now, &mut routed);
            for command in routed {
                quiet = false;
                match command {
                    Routed::AddRoute {
                        object,
                        session: (peer, principal),
                    } => self.pending.push(Command::AddRoute {
                        doc: object,
                        peer,
                        principal,
                    }),
                    Routed::RemoveRoute {
                        object,
                        session: (peer, principal),
                    } => self.pending.push(Command::RemoveRoute {
                        doc: object,
                        peer,
                        principal,
                    }),
                    Routed::NeedSession {
                        session: (peer, principal),
                    } => self.sessions.need(peer, principal),
                    Routed::ReleaseSession {
                        session: (peer, principal),
                    } => self.sessions.release(peer, principal),
                }
            }
            let mut dialed = vec![];
            self.sessions.flush(now, &mut dialed);
            for command in dialed {
                quiet = false;
                match command {
                    Dialed::Dial {
                        call,
                        peer,
                        principal,
                    } => {
                        self.dials.insert(call, (peer.clone(), principal.clone()));
                        self.pending.push(Command::Dial {
                            call,
                            peer,
                            principal,
                        });
                    }
                    Dialed::Close { peer, principal } => {
                        self.interest
                            .session_closed(&(peer.clone(), principal.clone()));
                        self.pending.push(Command::Close { peer, principal });
                    }
                }
            }
            if quiet {
                return;
            }
        }
    }
}
