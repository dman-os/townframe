//! The naive oracle of the Keyhive boundary: a monitor that watches every command the machine
//! issues and every answer the driver gives, and keeps the dumbest possible model of the world
//! (sets and maps, no cleverness) to check each law against. It shares no code with the machine.
//! A failure names the law in its message, which the planted-bug tests rely on.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::*;

/// The operation whose transaction is open.
struct Tx {
    ticket: Option<Ticket>,
    events: Vec<(EventId, Option<SessionId>)>,
    prekeys: usize,
}

pub(super) struct Oracle {
    floor: usize,
    level: usize,
    trapped: bool,
    /// The exclusive lock is held, from `AcquireLock` to `ReleaseLock`.
    exclusive: bool,
    tx_open: bool,
    tx: Option<Tx>,
    /// Keyhive's memory holds changes the database does not: from a run's answer to the commit
    /// or the reopen.
    dirty: bool,
    /// The operation's run ended well, so a commit may be issued.
    ran_ok: bool,
    loaded: BTreeSet<DocId>,
    hydrating: BTreeSet<DocId>,
    /// Preparations holding the shared lock, with their document.
    enc: BTreeMap<u64, DocId>,
    /// Decryptions out, with their document: they hold the shared lock while out.
    dec: BTreeMap<u64, DocId>,
    token_events: BTreeMap<u64, Vec<EventId>>,
    landed: BTreeSet<u64>,
    connected: BTreeSet<SessionId>,
    source: BTreeMap<EventId, Option<SessionId>>,
    granted: BTreeMap<SessionId, BTreeSet<EventId>>,
    delivered: BTreeSet<(SessionId, EventId)>,
    /// Sessions that were connected when an event committed and still owe an answer for it.
    owed: BTreeSet<(SessionId, EventId)>,
    committed_tickets: BTreeSet<Ticket>,
}

impl Oracle {
    pub fn new(floor: usize) -> Self {
        Self {
            floor,
            level: floor,
            trapped: false,
            exclusive: false,
            tx_open: false,
            tx: None,
            dirty: false,
            ran_ok: false,
            loaded: BTreeSet::new(),
            hydrating: BTreeSet::new(),
            enc: BTreeMap::new(),
            dec: BTreeMap::new(),
            token_events: BTreeMap::new(),
            landed: BTreeSet::new(),
            connected: BTreeSet::new(),
            source: BTreeMap::new(),
            granted: BTreeMap::new(),
            delivered: BTreeSet::new(),
            owed: BTreeSet::new(),
            committed_tickets: BTreeSet::new(),
        }
    }

    pub fn level(&self) -> usize {
        self.level
    }

    pub fn set_level(&mut self, level: usize) {
        self.level = level;
    }

    pub fn landed(&mut self, token: u64) {
        self.landed.insert(token);
    }

    pub fn session_opened(&mut self, session: SessionId) {
        self.connected.insert(session);
    }

    pub fn session_closed(&mut self, session: SessionId) {
        self.connected.remove(&session);
        self.owed.retain(|(s, _)| *s != session);
    }

    // ── commands ────────────────────────────────────────────────────────────────────────────

    /// Check a command the machine issued against the laws, and record what it means.
    pub fn on_command(&mut self, command: &Command) {
        assert!(!self.trapped, "{command:?} was issued after a trap (law 7)");
        // a keyhive call: not during a locked operation (law 3), never on an instance that
        // holds what the database does not (law 2)
        let keyhive_call = |this: &Self, what: &str| {
            assert!(
                !this.exclusive,
                "{what} runs while the lock is exclusive (law 3)"
            );
            assert!(
                !this.dirty,
                "{what} runs on an instance that was not reopened after a failure (law 2)"
            );
        };
        match command {
            Command::Hydrate { doc, .. } => {
                keyhive_call(self, "a hydration");
                assert!(!self.loaded.contains(doc), "a loaded document is hydrated");
                assert!(self.hydrating.insert(*doc), "one hydration per document");
            }
            Command::Encrypt { call, doc, .. } => {
                keyhive_call(self, "an encryption");
                assert!(
                    self.loaded.contains(doc),
                    "an encryption on document {doc} that was not hydrated (law 5)"
                );
                self.enc.insert(*call, *doc);
            }
            Command::Decrypt { call, doc, .. } => {
                keyhive_call(self, "a decryption");
                assert!(
                    self.loaded.contains(doc),
                    "a decryption on document {doc} that was not hydrated (law 5)"
                );
                self.dec.insert(*call, *doc);
            }
            Command::ApplyEncrypt { token, .. } => {
                keyhive_call(self, "an apply");
                assert!(
                    self.landed.contains(token),
                    "preparation {token} is applied though its commit did not land (law 1)"
                );
                let doc = self.enc.get(token).expect("a preparation held");
                assert!(
                    self.loaded.contains(doc),
                    "an apply on document {doc} that is not loaded (law 5)"
                );
            }
            Command::DropEncrypt { token } => {
                assert!(
                    self.enc.remove(token).is_some(),
                    "a preparation held is dropped"
                );
            }
            Command::Evict { doc } => {
                keyhive_call(self, "an eviction");
                assert!(
                    !self.enc.values().any(|d| d == doc) && !self.dec.values().any(|d| d == doc),
                    "a document with a preparation is evicted"
                );
                self.loaded.remove(doc);
            }
            Command::AcquireLock { .. } => {
                assert!(!self.exclusive, "a lock taken twice");
                assert!(
                    self.enc.is_empty() && self.dec.is_empty() && self.hydrating.is_empty(),
                    "the lock is taken exclusively with encryptions, decryptions or hydrations in flight (law 3)"
                );
                self.exclusive = true;
            }
            Command::Begin { .. } => {
                assert!(
                    self.exclusive,
                    "the transaction begins before the lock is held (law 3)"
                );
                self.tx_open = true;
            }
            Command::Run { refill, .. } => {
                assert!(
                    self.exclusive && self.tx_open,
                    "a run outside the transaction"
                );
                assert!(!self.dirty, "a run on a dirty instance (law 2)");
                assert!(
                    *refill || self.level >= self.floor,
                    "a run with {} prekeys left does not refill (law 6)",
                    self.level
                );
            }
            Command::Commit { .. } => {
                assert!(
                    self.tx_open && self.ran_ok,
                    "a commit of an operation that did not run (law 2)"
                );
            }
            Command::Rollback { .. } => assert!(self.tx_open, "a rollback with no transaction"),
            Command::Reopen { .. } => assert!(self.exclusive, "a reopen outside the lock"),
            Command::ReleaseLock => {
                assert!(self.exclusive, "a release without the lock");
                assert!(
                    !self.tx_open,
                    "the lock is released with the transaction open (law 2)"
                );
                assert!(
                    !self.dirty,
                    "the lock is released on an instance that was not reopened after a failure (law 2)"
                );
                self.exclusive = false;
            }
            Command::Eligible { session, .. } => {
                assert!(
                    self.connected.contains(session),
                    "a query for a closed session"
                );
            }
            Command::Notify { session, events } => {
                assert!(
                    self.connected.contains(session),
                    "session {session} is notified though it is not connected (law 4)"
                );
                let granted = self.granted.get(session).cloned().unwrap_or_default();
                for event in events {
                    assert!(
                        granted.contains(event),
                        "session {session} is told of event {event}, which Keyline did not grant it (law 4)"
                    );
                    assert!(
                        self.source[event] != Some(*session),
                        "event {event} is sent back to its source, session {session} (law 4)"
                    );
                    assert!(
                        self.delivered.insert((*session, *event)),
                        "event {event} reaches session {session} twice (law 4)"
                    );
                    self.owed.remove(&(*session, *event));
                }
            }
            Command::Reply { ticket, result } => {
                assert_eq!(
                    result.is_ok(),
                    self.committed_tickets.contains(ticket),
                    "operation {ticket} is answered {result:?} but its commit did not agree (law 2)"
                );
            }
            Command::Trap { .. } => self.trapped = true,
            Command::Cancel { .. } | Command::Encrypted { .. } | Command::Decrypted { .. } => {}
        }
    }

    // ── answers ─────────────────────────────────────────────────────────────────────────────

    pub fn hydrated(&mut self, doc: DocId, ended: &Ended<()>) {
        self.hydrating.remove(&doc);
        if *ended == Ended::Done(()) {
            self.loaded.insert(doc);
        }
    }

    pub fn encrypted(&mut self, call: u64, ended: &Ended<Prepared>) {
        match ended {
            Ended::Done(prepared) => {
                self.token_events.insert(call, prepared.events.clone());
            }
            Ended::Refused => {
                self.enc.remove(&call);
            }
            _ => {}
        }
    }

    pub fn decrypted(&mut self, call: u64, _ended: &Ended<Vec<u8>>) {
        self.dec.remove(&call);
    }

    pub fn applied(&mut self, token: u64, ended: &Ended<()>) {
        if *ended == Ended::Done(()) {
            self.enc.remove(&token);
            let events = self.token_events.remove(&token).unwrap_or_default();
            self.commit_events(events.into_iter().map(|e| (e, None)).collect());
        }
    }

    pub fn began(&mut self, _ended: &Ended<()>) {
        self.tx = Some(Tx {
            ticket: None,
            events: vec![],
            prekeys: self.level,
        });
        self.ran_ok = false;
    }

    pub fn ran(&mut self, ticket: Option<Ticket>, refill: bool, ended: &Ended<Report>) {
        match ended {
            Ended::Done(report) => {
                // keyhive's memory holds the operation now, and the database does not yet
                self.dirty = true;
                self.ran_ok = true;
                self.loaded.extend(report.hydrated.iter().copied());
                if refill {
                    assert!(
                        report.prekeys >= self.floor,
                        "the sim's driver restores the floor when it is asked to"
                    );
                }
                self.tx = Some(Tx {
                    ticket,
                    events: report.events.clone(),
                    prekeys: report.prekeys,
                });
            }
            Ended::Refused => {
                self.dirty = true;
            }
            _ => {}
        }
    }

    pub fn committed(&mut self, ended: &Ended<()>) {
        if *ended == Ended::Done(()) {
            let tx = self.tx.take().expect("a transaction");
            self.tx_open = false;
            self.dirty = false;
            self.level = tx.prekeys;
            if let Some(ticket) = tx.ticket {
                self.committed_tickets.insert(ticket);
            }
            self.commit_events(tx.events);
        }
    }

    pub fn rolled_back(&mut self, ended: &Ended<()>) {
        if *ended == Ended::Done(()) {
            self.tx = None;
            self.tx_open = false;
        }
    }

    pub fn reopened(&mut self, ended: &Ended<()>) {
        if *ended == Ended::Done(()) {
            self.loaded.clear();
            self.dirty = false;
        }
    }

    pub fn eligible(
        &mut self,
        session: SessionId,
        asked: &BTreeSet<EventId>,
        ended: &Ended<BTreeSet<EventId>>,
    ) {
        let Ended::Done(granted) = ended else {
            return;
        };
        if !self.connected.contains(&session) {
            return;
        }
        for event in asked.difference(granted) {
            self.owed.remove(&(session, *event));
        }
        self.granted.insert(session, granted.clone());
    }

    /// Events that became durable: each connected session but the source owes an answer.
    fn commit_events(&mut self, events: Vec<(EventId, Option<SessionId>)>) {
        for (event, source) in events {
            assert!(
                self.source.insert(event, source).is_none(),
                "the sim commits an event once"
            );
            for session in &self.connected {
                if Some(*session) != source {
                    self.owed.insert((*session, event));
                }
            }
        }
    }

    // ── the quiet state ─────────────────────────────────────────────────────────────────────

    pub fn check_quiet(
        &self,
        loaded: &BTreeSet<DocId>,
        tickets: Ticket,
        replies: &BTreeMap<Ticket, Result<(), Refused>>,
    ) {
        assert!(
            !self.exclusive && !self.tx_open && !self.dirty,
            "a drain ends outside a locked operation"
        );
        assert!(self.enc.is_empty() && self.dec.is_empty() && self.hydrating.is_empty());
        assert_eq!(
            replies.len() as u64,
            tickets,
            "every locked operation was answered"
        );
        assert!(
            self.owed.is_empty(),
            "committed events were neither sent nor filtered: {:?} (law 4)",
            self.owed
        );
        assert!(
            self.level >= self.floor,
            "the prekeys are below the floor after a drain, so a refused refill was not retried (law 8)"
        );
        assert_eq!(
            loaded, &self.loaded,
            "the machine's loaded documents are keyhive's"
        );
    }
}
