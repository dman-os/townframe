use super::*;
use proptest::collection::vec as prop_vec;
use proptest::prelude::*;
use std::sync::atomic::AtomicUsize;

const SLOTS: [&str; 3] = ["a", "b", "c"];

/// One facet slot as the model sees it.
///
/// `values` holds every value a read may return: one entry when the slot is determined, several
/// when concurrent signed writes to it merged. Which of those Automerge shows is its own tie-break
/// and not the model's to decide, so an ambiguous slot asserts membership rather than equality.
/// `heads` is always exact — the introductions of every signature covering the slot — which is what
/// keeps an ambiguous slot a precise version for `IfFacetsUnchanged`.
#[derive(Clone, Debug, Default, PartialEq)]
struct Slot {
    values: Vec<Option<Value>>,
    heads: Vec<ChangeHash>,
}

impl Slot {
    /// The model tracks introductions exactly but not Automerge op counts, so its
    /// versions carry `clock: 0` and actual clocks are zeroed before comparison.
    fn version(&self) -> FacetVersion {
        FacetVersion {
            heads: self.heads.clone(),
            clock: 0,
        }
    }

    fn exact(value: Option<Value>, heads: Vec<ChangeHash>) -> Self {
        Self {
            values: vec![value],
            heads,
        }
    }

    fn present(&self) -> bool {
        self.values.iter().any(Option::is_some)
    }

    fn ambiguous(&self) -> bool {
        self.values.len() > 1
    }

    /// The read state this slot stands for: exact when determined, the first candidate otherwise.
    fn read(&self) -> FacetRead {
        match self
            .values
            .first()
            .expect("a slot always has at least one candidate")
        {
            Some(value) => FacetRead::Accepted {
                value: value.clone(),
                version: self.version(),
            },
            None => FacetRead::Absent {
                version: self.version(),
            },
        }
    }

    /// Rewrite an actual read to the candidate it matched, so a whole-response equality still says
    /// something when a slot is ambiguous. A read that matches no candidate is a real mismatch.
    fn normalize(&self, actual: FacetRead, context: &str) -> FacetRead {
        let mut actual = actual;
        match &mut actual {
            FacetRead::Accepted { version, .. }
            | FacetRead::Absent { version }
            | FacetRead::Pending { version, .. }
            | FacetRead::Undecodable { version, .. } => version.clock = 0,
            FacetRead::Rejected { .. } => {}
        }
        if !self.ambiguous() {
            return actual;
        }
        let covered = match &actual {
            FacetRead::Accepted { value, version } => {
                version.heads == self.heads && self.values.contains(&Some(value.clone()))
            }
            FacetRead::Absent { version } => {
                version.heads == self.heads && self.values.contains(&None)
            }
            other => panic!("{context}: {other:?} is not a read of a merged slot"),
        };
        assert!(
            covered,
            "{context}: {actual:?} is none of the merge candidates {:?}",
            self.values
        );
        self.read()
    }
}

/// The slot a facet key names.
fn slot_of(facet: &FacetKey) -> usize {
    SLOTS
        .iter()
        .position(|name| key(name) == *facet)
        .expect("every returned facet is a modelled slot")
}

#[derive(Clone, Debug)]
struct Snapshot {
    heads: Vec<ChangeHash>,
    ancestors: BTreeSet<ChangeHash>,
    slots: [Slot; SLOTS.len()],
}

struct Model {
    document: DocId,
    checkpoints: Vec<Snapshot>,
    views: BTreeMap<Vec<ChangeHash>, Snapshot>,
}

impl Model {
    fn new(receipt: &WriteReceipt, initial: &BTreeMap<FacetKey, Value>) -> Self {
        assert_eq!(receipt.heads.len(), 1);
        let snapshot = Snapshot {
            heads: receipt.heads.clone(),
            ancestors: receipt.heads.iter().copied().collect(),
            slots: SLOTS.map(|name| {
                Slot::exact(
                    initial.get(&key(name)).cloned(),
                    if initial.contains_key(&key(name)) {
                        receipt.heads.clone()
                    } else {
                        Vec::new()
                    },
                )
            }),
        };
        Self {
            document: receipt.doc_id.clone(),
            checkpoints: vec![snapshot.clone()],
            views: BTreeMap::from([(snapshot.heads.clone(), snapshot)]),
        }
    }

    fn current(&self) -> &Snapshot {
        self.checkpoints.last().unwrap()
    }

    fn commit(
        &mut self,
        base: &Snapshot,
        changes: &BTreeMap<usize, Option<Value>>,
        receipt: &WriteReceipt,
        context: &str,
    ) {
        assert_eq!(receipt.doc_id, self.document, "{context}");
        let current = self.current();
        let introduced: Vec<_> = receipt
            .heads
            .iter()
            .copied()
            .filter(|head| !current.ancestors.contains(head))
            .collect();
        assert_eq!(introduced.len(), 1, "{context}: one new commit");
        let mut transaction = base.clone();
        transaction.heads = introduced.clone();
        transaction.ancestors.extend(&introduced);
        let mut merged = current.clone();
        merged.heads.retain(|head| !base.ancestors.contains(head));
        merged.heads.extend(&introduced);
        merged.heads.sort();
        merged.ancestors.extend(&introduced);
        assert_eq!(receipt.heads, merged.heads, "{context}: document frontier");
        for (&slot, value) in changes {
            let written = Slot::exact(value.clone(), introduced.clone());
            transaction.slots[slot] = written.clone();
            // A write replaces the signatures it observed at its base, so their introductions leave
            // the merged set even when a concurrent write also covered the slot.
            let mut heads = current.slots[slot].heads.clone();
            heads.extend(&written.heads);
            heads.retain(|head| !base.slots[slot].heads.contains(head));
            heads.sort();
            let values = if base.slots[slot] == current.slots[slot] {
                written.values
            } else {
                // Something else wrote this slot after the base, so this write is concurrent with
                // it: both signatures cover the merged slot, and which value Automerge shows is its
                // own tie-break. The candidates are deliberately an over-approximation — the model
                // asserts the read is one of them, never which.
                let mut values = current.slots[slot].values.clone();
                for candidate in written.values {
                    if !values.contains(&candidate) {
                        values.push(candidate);
                    }
                }
                values
            };
            merged.slots[slot] = Slot { values, heads };
        }
        if transaction.heads != merged.heads {
            assert!(
                self.views
                    .insert(transaction.heads.clone(), transaction)
                    .is_none(),
                "{context}: fresh transaction view"
            );
        }
        assert!(
            self.views
                .insert(merged.heads.clone(), merged.clone())
                .is_none(),
            "{context}: fresh merged view"
        );
        self.checkpoints.push(merged);
    }

    async fn check(&self, node: &Node, actor: &Actor, snapshot: &Snapshot, at: At, context: &str) {
        // Rewrite every facet of a response to the candidate its slot matched, so the comparisons
        // below stay whole-object equalities.
        let normalize = |read: FacetsRead| FacetsRead {
            heads: read.heads,
            facets: read
                .facets
                .into_iter()
                .map(|(facet, actual)| {
                    let actual = snapshot.slots[slot_of(&facet)].normalize(actual, context);
                    (facet, actual)
                })
                .collect(),
        };
        // Drop the slots whose presence a concurrent merge left undetermined. The selected read
        // below still pins their exact state, including the removal candidate.
        let determined = |facet: &FacetKey| !snapshot.slots[slot_of(facet)].ambiguous();
        let keep_reads = |read: FacetsRead| FacetsRead {
            heads: read.heads,
            facets: read
                .facets
                .into_iter()
                .filter(|(facet, _)| determined(facet))
                .collect(),
        };
        let keep_listed = |listing: FacetListing| FacetListing {
            heads: listing.heads,
            facets: listing
                .facets
                .into_iter()
                .filter(|(facet, _)| determined(facet))
                .map(|(facet, mut version)| {
                    version.clock = 0;
                    (facet, version)
                })
                .collect(),
        };

        let selected = FacetsRead {
            heads: snapshot.heads.clone(),
            facets: SLOTS
                .iter()
                .enumerate()
                .map(|(slot, name)| (key(name), snapshot.slots[slot].read()))
                .collect(),
        };
        for optimistic in [false, true] {
            let actual = op(node
                .client()
                .call(
                    RequestContext::root(),
                    ReadFacets {
                        capability: actor.capability,
                        doc_id: self.document.clone(),
                        at: at.clone(),
                        facets: Some(SLOTS.map(key).into_iter().collect()),
                        optimistic,
                    },
                )
                .await)
            .unwrap_or_else(|error| panic!("{context}: {error:?}"));
            assert_eq!(
                normalize(actual),
                selected,
                "{context}: selected, optimistic={optimistic}"
            );
        }
        let present: BTreeMap<_, _> = SLOTS
            .iter()
            .enumerate()
            .filter(|(slot, _)| snapshot.slots[*slot].present())
            .map(|(slot, name)| (key(name), snapshot.slots[slot].read()))
            .collect();
        let actual = node
            .read(actor, &self.document, at.clone(), false)
            .await
            .unwrap_or_else(|error| panic!("{context}: {error:?}"));
        assert_eq!(
            keep_reads(normalize(actual)),
            keep_reads(FacetsRead {
                heads: snapshot.heads.clone(),
                facets: present,
            }),
            "{context}: all present facets"
        );
        let listing = op(node
            .client()
            .call(
                RequestContext::root(),
                ListFacets {
                    capability: actor.capability,
                    doc_id: self.document.clone(),
                    at,
                },
            )
            .await)
        .unwrap_or_else(|error| panic!("{context}: {error:?}"));
        assert_eq!(
            keep_listed(listing),
            keep_listed(FacetListing {
                heads: snapshot.heads.clone(),
                facets: SLOTS
                    .iter()
                    .enumerate()
                    .filter(|(slot, _)| snapshot.slots[*slot].present())
                    .map(|(slot, name)| (key(name), snapshot.slots[slot].version()))
                    .collect(),
            }),
            "{context}: listing"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum Edit {
    Set(usize),
    SetInitial(usize),
    Remove(usize),
    SetAndRemove(usize, usize),
}

impl Edit {
    fn mask(self) -> u8 {
        match self {
            Self::Set(slot) | Self::SetInitial(slot) | Self::Remove(slot) => 1 << slot,
            Self::SetAndRemove(set, remove) => (1 << set) | (1 << remove),
        }
    }

    fn changes(self, text: &str) -> BTreeMap<usize, Option<Value>> {
        match self {
            Self::Set(slot) => BTreeMap::from([(slot, Some(note(text)))]),
            Self::SetInitial(slot) => BTreeMap::from([(slot, Some(note("initial")))]),
            Self::Remove(slot) => BTreeMap::from([(slot, None)]),
            Self::SetAndRemove(set, remove) => {
                BTreeMap::from([(set, Some(note(text))), (remove, None)])
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Current,
    Heads(usize),
    Document(usize),
    Facets(usize, u8),
}

impl Mode {
    /// Generated steps carry arbitrary seeds rather than indexes that are known to be in range, so
    /// an index resolves against the checkpoints that exist when the step runs. Every generated
    /// step therefore names a real base, and shrinking a seed only retargets the step — proptest
    /// keeps a shrunk case only if it still fails.
    fn resolved(self, model: &Model) -> Self {
        let at = |at: usize| at % model.checkpoints.len();
        match self {
            Self::Current => Self::Current,
            Self::Heads(index) => Self::Heads(at(index)),
            Self::Document(index) => Self::Document(at(index)),
            Self::Facets(index, mask) => Self::Facets(at(index), mask),
        }
    }

    fn request(self, model: &Model) -> WriteMode {
        match self.resolved(model) {
            Self::Current => WriteMode::AtCurrent,
            Self::Heads(at) => WriteMode::AtHeads {
                heads: model.checkpoints[at].heads.clone(),
            },
            Self::Document(at) => WriteMode::IfDocUnchanged {
                heads: model.checkpoints[at].heads.clone(),
            },
            Self::Facets(at, mask) => WriteMode::IfFacetsUnchanged {
                facets: SLOTS
                    .iter()
                    .enumerate()
                    .filter(|(slot, _)| mask & (1 << slot) != 0)
                    .map(|(slot, name)| (key(name), model.checkpoints[at].slots[slot].version()))
                    .collect(),
            },
        }
    }

    fn conflict(self, model: &Model) -> Option<DocsError> {
        let current = model.current();
        match self.resolved(model) {
            Self::Current | Self::Heads(_) => None,
            Self::Document(at) => {
                (current.heads != model.checkpoints[at].heads).then(|| DocsError::Conflict {
                    heads: current.heads.clone(),
                    facets: BTreeMap::new(),
                })
            }
            Self::Facets(at, mask) => {
                let slots: Vec<_> = (0..SLOTS.len())
                    .filter(|slot| mask & (1 << slot) != 0)
                    .collect();
                slots
                    .iter()
                    .any(|&slot| {
                        current.slots[slot].heads != model.checkpoints[at].slots[slot].heads
                    })
                    .then(|| DocsError::Conflict {
                        heads: current.heads.clone(),
                        facets: slots
                            .into_iter()
                            .map(|slot| (key(SLOTS[slot]), current.slots[slot].version()))
                            .collect(),
                    })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Author {
    Owner,
    Other,
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Write(Edit, Mode, Author),
    Read(usize),
    Replay(usize),
    Restart,
    OtherDenied,
    ShareRead,
    ShareWrite,
}

#[derive(Clone)]
enum Request {
    Create(CreateDoc),
    Write(WriteFacets),
}

impl Request {
    async fn call(&self, node: &Node) -> Result<WriteReceipt, DocsError> {
        match self {
            Self::Create(request) => op(node
                .client()
                .call(RequestContext::root(), request.clone())
                .await),
            Self::Write(request) => op(node
                .client()
                .call(RequestContext::root(), request.clone())
                .await),
        }
    }
}

async fn run(node: &mut Node, actors: &[Actor; 2], case: &str, initial_mask: u8, script: &[Op]) {
    tracing::info!(
        case,
        initial_mask,
        steps = script.len(),
        "model script started"
    );
    let [owner, other] = actors;
    let initial: BTreeMap<_, _> = SLOTS
        .iter()
        .enumerate()
        .filter(|(slot, _)| initial_mask & (1 << slot) != 0)
        .map(|(_, name)| (key(name), note("initial")))
        .collect();
    let create = Request::Create(CreateDoc {
        capability: owner.capability,
        command: format!("{case}/create").try_into().unwrap(),
        placement: PLACEMENT.into(),
        facets: initial.clone(),
    });
    let created = create.call(node).await.unwrap();
    let mut model = Model::new(&created, &initial);
    let mut commands = vec![(create, created)];
    // What `other` may do, so a generated step's expectation matches the grants actually made.
    let mut shared_read = false;
    let mut shared_write = false;
    model
        .check(node, owner, model.current(), At::Current, case)
        .await;
    for (step, operation) in script.iter().enumerate() {
        let context = format!("{case}, step {step} {operation:?}, script {script:?}");
        match *operation {
            Op::Write(edit, mode, author) => {
                let actor = match author {
                    Author::Owner => owner,
                    Author::Other => other,
                };
                let changes = edit.changes(&format!("{case}/{step}"));
                let mode = mode.resolved(&model);
                let context = format!("{context}, resolved base {mode:?}");
                let request = Request::Write(WriteFacets {
                    capability: actor.capability,
                    command: Some(format!("{case}/{step}").try_into().unwrap()),
                    doc_id: model.document.clone(),
                    mode: mode.request(&model),
                    set: changes
                        .iter()
                        .filter_map(|(&slot, value)| {
                            value
                                .as_ref()
                                .map(|value| (key(SLOTS[slot]), value.clone()))
                        })
                        .collect(),
                    remove: changes
                        .iter()
                        .filter(|(_, value)| value.is_none())
                        .map(|(&slot, _)| key(SLOTS[slot]))
                        .collect(),
                });
                let actual = request.call(node).await;
                if author == Author::Other && !shared_write {
                    assert_eq!(actual, Err(DocsError::Denied), "{context}");
                } else if let Some(conflict) = mode.conflict(&model) {
                    let actual = actual.map_err(|mut error| {
                        if let DocsError::Conflict { facets, .. } = &mut error {
                            for version in facets.values_mut() {
                                version.clock = 0;
                            }
                        }
                        error
                    });
                    assert_eq!(actual, Err(conflict), "{context}");
                } else {
                    let receipt = actual.unwrap_or_else(|error| panic!("{context}: {error:?}"));
                    let base = match mode.resolved(&model) {
                        Mode::Heads(at) => &model.checkpoints[at],
                        Mode::Current | Mode::Document(_) | Mode::Facets(_, _) => model.current(),
                    }
                    .clone();
                    model.commit(&base, &changes, &receipt, &context);
                    commands.push((request, receipt));
                }
            }
            Op::Read(seed) => {
                let snapshot = &model.checkpoints[seed % model.checkpoints.len()];
                model
                    .check(
                        node,
                        owner,
                        snapshot,
                        At::Heads(snapshot.heads.clone()),
                        &context,
                    )
                    .await;
            }
            Op::Replay(seed) => {
                let (request, receipt) = &commands[seed % commands.len()];
                assert_eq!(request.call(node).await, Ok(receipt.clone()), "{context}");
            }
            Op::Restart => node.restart().await.unwrap(),
            Op::OtherDenied => {
                // Meaningless once `other` holds a grant; the generator cannot know that yet.
                if shared_read || shared_write {
                    continue;
                }
                for at in [At::Current, At::Heads(model.checkpoints[0].heads.clone())] {
                    for optimistic in [false, true] {
                        assert_eq!(
                            node.read(other, &model.document, at.clone(), optimistic)
                                .await,
                            Err(DocsError::Denied),
                            "{context}"
                        );
                    }
                }
                for mode in [
                    Mode::Current,
                    Mode::Heads(0),
                    Mode::Document(0),
                    Mode::Facets(0, 7),
                ] {
                    assert_eq!(
                        node.write(
                            other,
                            &model.document,
                            mode.request(&model),
                            [(key(SLOTS[0]), note("denied"))],
                            [key(SLOTS[1])],
                        )
                        .await,
                        Err(DocsError::Denied),
                        "{context}"
                    );
                }
            }
            Op::ShareRead => {
                if shared_read {
                    continue;
                }
                node.grant(
                    other,
                    LedgerAccess::object(AccessRequest::Read(&model.document)),
                )
                .await;
                shared_read = true;
                model
                    .check(node, other, model.current(), At::Current, &context)
                    .await;
                // Read access alone must not let `other` write — but a generated script may have
                // granted write first, in which case there is nothing left to refuse.
                if shared_write {
                    continue;
                }
                assert_eq!(
                    node.write(
                        other,
                        &model.document,
                        WriteMode::AtCurrent,
                        [(key(SLOTS[0]), note("read-only"))],
                        [],
                    )
                    .await,
                    Err(DocsError::Denied),
                    "{context}"
                );
            }
            Op::ShareWrite => {
                if shared_write {
                    continue;
                }
                node.grant(
                    other,
                    LedgerAccess::object(AccessRequest::Write(&model.document)),
                )
                .await;
                shared_write = true;
            }
        }
        model
            .check(node, owner, model.current(), At::Current, &context)
            .await;
    }
    for snapshot in model.views.values() {
        model
            .check(
                node,
                owner,
                snapshot,
                At::Heads(snapshot.heads.clone()),
                case,
            )
            .await;
    }
    tracing::info!(case, "model script completed");
}

fn exhaustive(mode: Mode) -> Vec<Vec<Op>> {
    let edits = [
        Edit::Set(0),
        Edit::Set(1),
        Edit::Remove(0),
        Edit::Remove(1),
        Edit::SetAndRemove(0, 1),
        Edit::SetAndRemove(1, 2),
    ];
    edits
        .into_iter()
        .flat_map(|first| {
            edits.into_iter().map(move |second| {
                let second_mode = match mode {
                    Mode::Heads(_) => Mode::Heads(1),
                    Mode::Facets(_, _) => Mode::Facets(0, second.mask()),
                    Mode::Current | Mode::Document(_) => mode,
                };
                vec![
                    Op::Write(first, mode, Author::Owner),
                    Op::Write(second, second_mode, Author::Owner),
                    Op::Read(0),
                    Op::Replay(0),
                    Op::Replay(1),
                ]
            })
        })
        .collect()
}

utils_rs::table_tests! {
    conformance tokio,
    (scenario, initial, scripts),
    {
        let mut node = Node::boot().await;
        let actors = [node.writer("owner").await, node.writer("other").await];
        for (case, script) in scripts.iter().enumerate() {
            run(&mut node, &actors, &format!("{scenario}/{case}"), initial, script).await;
        }
        node.stop().await;
    },
    multi_thread: false,
}

conformance! {
    exhaustive_current_empty_long_test: ("current", 0, exhaustive(Mode::Current)),
    exhaustive_explicit_heads_long_test: ("heads", 3, exhaustive(Mode::Heads(0))),
    exhaustive_document_conditions_long_test: ("document", 1, exhaustive(Mode::Document(0))),
    exhaustive_facet_conditions_long_test: ("facets", 1, exhaustive(Mode::Facets(0, 7))),
    historical_disjoint_branches: ("branches", 1, vec![vec![
        Op::Write(Edit::Set(0), Mode::Current, Author::Owner),
        Op::Write(Edit::Set(1), Mode::Heads(0), Author::Owner),
        Op::Write(Edit::Remove(2), Mode::Heads(0), Author::Owner),
        Op::Read(1),
        Op::Read(2),
        Op::Write(Edit::SetAndRemove(2, 0), Mode::Document(3), Author::Owner),
        Op::Replay(1),
        Op::Replay(2),
        Op::Restart,
        Op::Replay(0),
        Op::Replay(3),
    ]]),
    tombstone_conditions_and_resurrection: ("tombstones", 0, vec![vec![
        Op::Write(Edit::Remove(0), Mode::Facets(0, 1), Author::Owner),
        Op::Write(Edit::SetAndRemove(0, 1), Mode::Facets(0, 3), Author::Owner),
        Op::Write(Edit::Set(0), Mode::Facets(1, 1), Author::Owner),
        Op::Write(Edit::Remove(0), Mode::Document(2), Author::Owner),
        Op::Write(Edit::Remove(0), Mode::Facets(3, 1), Author::Owner),
        Op::Write(Edit::Set(0), Mode::Facets(3, 1), Author::Owner),
        Op::Write(Edit::SetAndRemove(0, 1), Mode::Facets(4, 3), Author::Owner),
        Op::Read(1),
        Op::Restart,
        Op::Replay(1),
        Op::Replay(2),
    ]]),
    equal_values_still_advance_versions: ("equal-values", 1, vec![vec![
        Op::Write(Edit::SetInitial(0), Mode::Current, Author::Owner),
        Op::Write(Edit::Set(1), Mode::Facets(0, 1), Author::Owner),
        Op::Write(Edit::SetInitial(0), Mode::Facets(1, 1), Author::Owner),
        Op::Write(Edit::SetAndRemove(1, 0), Mode::Document(1), Author::Owner),
        Op::Write(Edit::Set(0), Mode::Current, Author::Owner),
        Op::Write(Edit::SetInitial(0), Mode::Current, Author::Owner),
        Op::Write(Edit::Remove(0), Mode::Facets(2, 1), Author::Owner),
        Op::Read(0),
        Op::Replay(1),
    ]]),
    actor_isolation_and_shared_authorship: ("actors", 3, vec![vec![
        Op::OtherDenied,
        Op::Write(Edit::Set(0), Mode::Current, Author::Owner),
        Op::OtherDenied,
        Op::ShareRead,
        Op::ShareWrite,
        Op::Write(Edit::SetAndRemove(1, 2), Mode::Facets(0, 6), Author::Other),
        Op::Write(Edit::Remove(0), Mode::Document(2), Author::Owner),
        Op::Write(Edit::Set(0), Mode::Facets(3, 1), Author::Other),
        Op::Read(0),
        Op::Restart,
        Op::Replay(2),
        Op::Replay(0),
    ]]),
    // Found by the generated scripts: granting write before read leaves nothing for the read-only
    // refusal to refuse, and a read grant must not be mistaken for a write grant either way.
    write_granted_before_read: ("share-order", 1, vec![vec![
        Op::OtherDenied,
        Op::ShareWrite,
        Op::ShareRead,
        Op::Write(Edit::Set(0), Mode::Current, Author::Other),
        Op::Read(0),
        Op::Restart,
        Op::Replay(1),
    ]]),
    // Two writes to one slot from different bases merge under both signatures: which value
    // Automerge shows is its tie-break, the facet heads are both introductions, and a later write
    // recovers an exactly determined slot.
    concurrent_writes_to_one_slot_merge_under_both_signatures: ("overlap", 1, vec![vec![
        Op::Write(Edit::Set(0), Mode::Current, Author::Owner),
        Op::Write(Edit::Set(0), Mode::Heads(0), Author::Owner),
        Op::Read(1),
        Op::Write(Edit::Remove(0), Mode::Heads(1), Author::Owner),
        Op::Read(2),
        Op::Write(Edit::Set(0), Mode::Current, Author::Owner),
        Op::Read(1),
        Op::Read(3),
    ]]),
    // Found by the generated scripts: two first writes of a slot from bases where it had no
    // metadata each create the slot's metadata map, and the losing map's signature is still
    // current. A later write replaces both, so neither may survive it as a facet head.
    concurrent_first_writes_leave_two_metadata_maps: ("first-writes", 0, vec![vec![
        Op::Write(Edit::Set(1), Mode::Current, Author::Owner),
        Op::Write(Edit::Set(1), Mode::Heads(0), Author::Owner),
        Op::Read(2),
        Op::Write(Edit::SetAndRemove(0, 1), Mode::Document(2), Author::Owner),
        Op::Read(3),
        Op::Restart,
        Op::Replay(2),
    ]]),
    // Found by the generated scripts: two concurrent writes from the same base that make the same
    // claim on a slot (here, both removing slot 1 as the same actor) produce identical signature
    // bytes in two distinct Automerge map entries. An unrelated write to another slot must retain
    // both entries rather than deduplicating them by JSON value and touching the slot unsigned.
    concurrent_identical_claims_keep_both_signature_entries: ("identical-claims", 2, vec![vec![
        Op::Write(Edit::Remove(1), Mode::Current, Author::Owner),
        Op::Write(Edit::SetAndRemove(0, 1), Mode::Heads(0), Author::Owner),
        Op::Write(Edit::SetInitial(2), Mode::Document(2), Author::Owner),
        Op::Read(3),
    ]]),
}

// Generated scripts: the same reference model, but proptest picks the operations, their modes,
// their targets and their authors, and shrinks a failing script to the shortest one that still
// fails. The deterministic tables above are the specification; this explores the interleavings they
// cannot enumerate. Steps carry seeds, not indexes — `Mode::resolved` maps a seed onto the
// checkpoints that exist when the step runs, so every generated step names a real base. A failure
// persists its seed to `src/proptest-regressions/`: proptest puts that directory beside the crate
// root, and this crate has no `src/` of its own.
fn any_edit() -> impl Strategy<Value = Edit> {
    (0..6usize, 0..SLOTS.len(), 0..SLOTS.len() - 1).prop_map(|(kind, slot, offset)| match kind {
        0 => Edit::Set(slot),
        1 => Edit::SetInitial(slot),
        2 => Edit::Remove(slot),
        // A facet cannot be both set and removed, so the second slot is always a different one.
        _ => Edit::SetAndRemove(slot, (slot + 1 + offset) % SLOTS.len()),
    })
}

fn any_mode() -> impl Strategy<Value = Mode> {
    prop_oneof![
        Just(Mode::Current),
        any::<usize>().prop_map(Mode::Heads),
        any::<usize>().prop_map(Mode::Document),
        (any::<usize>(), 1..=7u8).prop_map(|(at, mask)| Mode::Facets(at, mask)),
    ]
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (any_edit(), any_mode(), any_author()).prop_map(|(edit, mode, author)| {
            Op::Write(edit, mode, author)
        }),
        1 => any::<usize>().prop_map(Op::Read),
        1 => any::<usize>().prop_map(Op::Replay),
        1 => Just(Op::Restart),
        1 => Just(Op::OtherDenied),
        1 => Just(Op::ShareRead),
        1 => Just(Op::ShareWrite),
    ]
}

fn any_author() -> impl Strategy<Value = Author> {
    prop_oneof![Just(Author::Owner), Just(Author::Other)]
}

/// Command ids are `case/step`, so each case needs its own name — including every shrink attempt,
/// which re-runs the script against a node that already holds the earlier ids.
static CASES: AtomicUsize = AtomicUsize::new(0);

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    #[test]
    fn generated_scripts_match_the_reference_model_long_test(
        (initial, script) in (0..=7u8, prop_vec(any_op(), 2..=12)),
    ) {
        let case = CASES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a tokio runtime");
        runtime.block_on(async {
            let mut node = Node::boot().await;
            let actors = [node.writer("owner").await, node.writer("other").await];
            run(&mut node, &actors, &format!("generated/{case}"), initial, &script).await;
            node.stop().await;
        });
    }
}
