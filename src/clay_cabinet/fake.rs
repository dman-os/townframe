//! An in-memory cabinet that answers `pollDatums` the way the node does (ADR 025), for tests.
//!
//! It keeps a log of facts, each at its own revision: a datum written or removed, a document
//! entered into or removed from a drawer's roster. A poll replays the log after its cursor and
//! applies ADR 025 §7's stateless rules:
//! - a changed selected row of a matching document is one `datum` event;
//! - a fact the predicate depends on (a roster entry of a drawer it names, a datum of a tag it
//!   names) is a move-in, every selected row of the document, when the document matches now,
//!   and a `leave` when it doesn't.
//!
//! Matching is evaluated against current state, as the node may evaluate a page ahead of its
//! revision; consumers converge either way.
//!
//! Capabilities are granted per drawer. A poll must be anchored on a drawer the capability is
//! granted, or on a document such a drawer lists, as the node requires below node-wide
//! authority. `InDrawer` ignores `transitive` and `at_least`: the fake has no child drawers.

use crate::CabinetDocs;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::query::Anchor;
use big_cabinet_core::query::IndexFacts;
use big_cabinet_core::query::Predicate;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone, Debug)]
enum Fact {
    Datum { doc_id: DocId, key: DatumKey },
    Roster { drawer_id: DocId, doc_id: DocId },
}

#[derive(Default)]
struct State {
    revision: u64,
    log: Vec<(u64, Fact)>,
    /// Current rows, with the revision each was last written at.
    rows: BTreeMap<DocId, BTreeMap<DatumKey, u64>>,
    /// Removed rows, kept so a replay can report the removal.
    removed: BTreeMap<(DocId, DatumKey), u64>,
    rosters: BTreeMap<DocId, BTreeSet<DocId>>,
    grants: HashMap<CapId, Grant>,
    polls: HashMap<CapId, usize>,
}

#[derive(Clone, Debug)]
pub enum Grant {
    /// Node-wide: any predicate, anchored or not.
    Operator,
    /// Read on these drawers.
    Drawers(BTreeSet<DocId>),
}

/// The fake cabinet. Clone it to hand the same store to several authorities.
#[derive(Clone)]
pub struct MemoryCabinet {
    state: Arc<Mutex<State>>,
    changed: Arc<watch::Sender<u64>>,
}

impl Default for MemoryCabinet {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            changed: Arc::new(watch::channel(0).0),
        }
    }
}

impl MemoryCabinet {
    pub fn grant(&self, capability: CapId, grant: Grant) {
        self.lock().grants.insert(capability, grant);
    }

    pub fn write_datum(&self, doc_id: &DocId, key: &DatumKey) {
        self.record(
            Fact::Datum {
                doc_id: doc_id.clone(),
                key: key.clone(),
            },
            |state, revision| {
                state.removed.remove(&(doc_id.clone(), key.clone()));
                state
                    .rows
                    .entry(doc_id.clone())
                    .or_default()
                    .insert(key.clone(), revision);
            },
        );
    }

    pub fn remove_datum(&self, doc_id: &DocId, key: &DatumKey) {
        self.record(
            Fact::Datum {
                doc_id: doc_id.clone(),
                key: key.clone(),
            },
            |state, revision| {
                let rows = state
                    .rows
                    .get_mut(doc_id)
                    .expect("removing a datum of an unknown doc");
                rows.remove(key).expect("removing an absent datum");
                if rows.is_empty() {
                    state.rows.remove(doc_id);
                }
                state
                    .removed
                    .insert((doc_id.clone(), key.clone()), revision);
            },
        );
    }

    pub fn add_to_drawer(&self, drawer_id: &DocId, doc_id: &DocId) {
        self.record(
            Fact::Roster {
                drawer_id: drawer_id.clone(),
                doc_id: doc_id.clone(),
            },
            |state, _| {
                state
                    .rosters
                    .entry(drawer_id.clone())
                    .or_default()
                    .insert(doc_id.clone());
            },
        );
    }

    pub fn remove_from_drawer(&self, drawer_id: &DocId, doc_id: &DocId) {
        self.record(
            Fact::Roster {
                drawer_id: drawer_id.clone(),
                doc_id: doc_id.clone(),
            },
            |state, _| {
                assert!(
                    state
                        .rosters
                        .get_mut(drawer_id)
                        .is_some_and(|roster| roster.remove(doc_id))
                );
            },
        );
    }

    /// Withdraws `capability`'s grant. Held polls wake, as the node wakes them on a capability
    /// change, so a held poll does not wait out its hold to be refused.
    pub fn revoke(&self, capability: CapId) {
        assert!(self.lock().grants.remove(&capability).is_some());
        let revision = self.lock().revision;
        self.changed.send_replace(revision);
    }

    /// How many polls `capability` has made.
    pub fn polls(&self, capability: CapId) -> usize {
        self.lock().polls.get(&capability).copied().unwrap_or(0)
    }

    fn record(&self, fact: Fact, apply: impl FnOnce(&mut State, u64)) {
        let revision = {
            let mut state = self.lock();
            state.revision += 1;
            let revision = state.revision;
            apply(&mut state, revision);
            state.log.push((revision, fact));
            revision
        };
        self.changed.send_replace(revision);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("fake cabinet poisoned")
    }

    fn page(&self, request: &PollDatums) -> Result<DatumPage, DocsError> {
        let mut state = self.lock();
        *state.polls.entry(request.capability).or_default() += 1;
        authorize(&state, request)?;
        let limit = match request.limit {
            0 => usize::MAX,
            limit => limit as usize,
        };
        let mut events = Vec::new();
        let mut through = request.after;
        let start = state
            .log
            .partition_point(|(revision, _)| *revision <= request.after);
        let facts = &state.log[start..];
        for (revision, fact) in facts.iter().take(limit) {
            through = *revision;
            match fact {
                Fact::Datum { doc_id, key } => {
                    let names_tag = names_tag(&request.predicate, key);
                    if names_tag {
                        move_in_or_out(&state, request, *revision, doc_id, &mut events);
                    } else if request.datums.selects(key)
                        && request.predicate.matches(&*state, doc_id)
                    {
                        events.push(DatumEvent::Datum {
                            revision: *revision,
                            doc_id: doc_id.clone(),
                            datum_key: key.clone(),
                            datum: observed(&state, doc_id, key),
                        });
                    }
                }
                Fact::Roster { drawer_id, doc_id } => {
                    if names_drawer(&request.predicate, drawer_id) {
                        move_in_or_out(&state, request, *revision, doc_id, &mut events);
                    }
                }
            }
        }
        let caught_up = facts.len() <= limit;
        if caught_up {
            through = state.revision;
        }
        Ok(DatumPage {
            events,
            through,
            caught_up,
        })
    }
}

impl CabinetDocs for MemoryCabinet {
    fn poll_datums(&self, request: PollDatums) -> BoxFuture<'static, Result<DatumPage, DocsError>> {
        let cabinet = self.clone();
        Box::pin(async move {
            let mut changed = cabinet.changed.subscribe();
            let page = cabinet.page(&request)?;
            if !page.events.is_empty() || request.hold_ms == 0 {
                return Ok(page);
            }
            // Hold an empty page until something is recorded, then answer from the same cursor.
            let hold = Duration::from_millis(u64::from(request.hold_ms));
            match tokio::time::timeout(hold, changed.changed()).await {
                Ok(_) => cabinet.page(&request),
                Err(_) => Ok(page),
            }
        })
    }
}

fn authorize(state: &State, request: &PollDatums) -> Result<(), DocsError> {
    let drawers = match state.grants.get(&request.capability) {
        None => return Err(DocsError::Denied),
        Some(Grant::Operator) => return Ok(()),
        Some(Grant::Drawers(drawers)) => drawers,
    };
    let anchors = request.predicate.anchors().map_err(|_| DocsError::Denied)?;
    let admitted = !anchors.is_empty()
        && anchors.iter().all(|anchor| match anchor {
            Anchor::Drawer { drawer_id, .. } => drawers.contains(drawer_id),
            Anchor::Doc { doc_id } => drawers.iter().any(|drawer_id| {
                state
                    .rosters
                    .get(drawer_id)
                    .is_some_and(|roster| roster.contains(doc_id))
            }),
        });
    if admitted {
        Ok(())
    } else {
        Err(DocsError::Denied)
    }
}

fn move_in_or_out(
    state: &State,
    request: &PollDatums,
    revision: u64,
    doc_id: &DocId,
    events: &mut Vec<DatumEvent>,
) {
    if !request.predicate.matches(state, doc_id) {
        events.push(DatumEvent::Leave {
            revision,
            doc_id: doc_id.clone(),
        });
        return;
    }
    for key in state.rows.get(doc_id).into_iter().flat_map(BTreeMap::keys) {
        if request.datums.selects(key) {
            events.push(DatumEvent::Datum {
                revision,
                doc_id: doc_id.clone(),
                datum_key: key.clone(),
                datum: observed(state, doc_id, key),
            });
        }
    }
}

fn observed(state: &State, doc_id: &DocId, key: &DatumKey) -> ObservedDatum {
    match state.rows.get(doc_id).and_then(|rows| rows.get(key)) {
        Some(written) => ObservedDatum::Present {
            doc_heads: Vec::new(),
            version: DatumVersion {
                heads: Vec::new(),
                clock: *written,
            },
        },
        None => ObservedDatum::Removed {
            doc_heads: None,
            version: DatumVersion {
                heads: Vec::new(),
                clock: state.removed[&(doc_id.clone(), key.clone())],
            },
        },
    }
}

impl IndexFacts for State {
    fn has_key(&self, doc_id: &DocId, key: &DatumKey) -> bool {
        self.rows
            .get(doc_id)
            .is_some_and(|rows| rows.contains_key(key))
    }

    fn has_tag(&self, doc_id: &DocId, tag: &DatumTag) -> bool {
        self.rows
            .get(doc_id)
            .is_some_and(|rows| rows.keys().any(|key| key.tag() == tag))
    }

    /// The fake keeps flat rosters and no drawer nesting, so `transitive` changes nothing.
    fn listed_in(&self, drawer_id: &DocId, doc_id: &DocId, _transitive: bool) -> bool {
        self.rosters
            .get(drawer_id)
            .is_some_and(|roster| roster.contains(doc_id))
    }
}

fn names_tag(predicate: &Predicate, key: &DatumKey) -> bool {
    match predicate {
        Predicate::HasTag { tag } => key.tag() == tag,
        Predicate::HasKey { key: named } => named == key,
        Predicate::And { clauses } | Predicate::Or { clauses } => {
            clauses.iter().any(|clause| names_tag(clause, key))
        }
        Predicate::Not { inner } => names_tag(inner, key),
        Predicate::IsDoc { .. } | Predicate::InDrawer { .. } | Predicate::HasReferenceTo { .. } => {
            false
        }
    }
}

fn names_drawer(predicate: &Predicate, drawer: &DocId) -> bool {
    match predicate {
        Predicate::InDrawer { drawer_id, .. } => drawer_id == drawer,
        Predicate::And { clauses } | Predicate::Or { clauses } => {
            clauses.iter().any(|clause| names_drawer(clause, drawer))
        }
        Predicate::Not { inner } => names_drawer(inner, drawer),
        Predicate::HasTag { .. }
        | Predicate::HasKey { .. }
        | Predicate::IsDoc { .. }
        | Predicate::HasReferenceTo { .. } => false,
    }
}
