//! The naive world the machine is checked against: for each document, every committed change
//! with the revision it landed at and whether its key has arrived, the tree's revision, whether
//! the document is readable, and the published frontier row. No indexes and no caches: what a
//! reader may see is recomputed from the changes each time, by loading them into a fresh
//! automerge document.
//!
//! A change is `local` if it was written through a machine, `remote` if another node wrote it.
//! A remote change has a key or does not; a reader sees only changes with keys. One sequence of
//! revisions covers every document, as in the hive.

use std::collections::BTreeSet;

use automerge::ActorId;
use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ROOT;
use automerge::ReadDoc;
use automerge::transaction::Transactable;

use super::*;

pub(in crate::document) struct Stored {
    pub change: Change,
    pub rev: Rev,
    pub keyed: bool,
}

pub(in crate::document) struct DocModel {
    pub changes: Vec<Stored>,
    pub tree_rev: Rev,
    pub readable: bool,
    /// The published frontier row: its heads (sorted) and the revision they reflect.
    pub frontier: Option<(Vec<ChangeHash>, Rev)>,
    /// The remote node's replica, which its next change builds on.
    remote: Automerge,
    remote_writes: u64,
}

pub(in crate::document) struct World {
    pub docs: Vec<DocModel>,
    /// Whether a commit's frontier row is checked against the revision it expects. Off, it is
    /// written whatever the tree held (a store without the compare-and-set).
    pub cas: bool,
    rev: Rev,
}

fn sorted(mut heads: Vec<ChangeHash>) -> Vec<ChangeHash> {
    heads.sort();
    heads
}

/// The document a reader would load from `changes`.
fn load<'a>(changes: impl IntoIterator<Item = &'a Change>) -> Automerge {
    let mut doc = Automerge::new();
    doc.apply_changes(changes.into_iter().cloned().collect::<Vec<_>>())
        .expect("committed changes apply");
    doc
}

impl DocModel {
    /// Everything a reader may see once the changes at or below `rev` that have keys are in.
    fn visible_at(&self, rev: Rev) -> Automerge {
        load(
            self.changes
                .iter()
                .filter(|stored| stored.keyed && stored.rev <= rev)
                .map(|stored| &stored.change),
        )
    }

    pub fn keyless(&self) -> Vec<ContentRef> {
        self.changes
            .iter()
            .filter(|stored| !stored.keyed)
            .map(|stored| ContentRef(stored.change.hash().0))
            .collect()
    }
}

impl World {
    pub fn new(docs: usize) -> Self {
        Self {
            docs: (0..docs)
                .map(|at| DocModel {
                    changes: vec![],
                    tree_rev: 0,
                    readable: true,
                    frontier: None,
                    remote: Automerge::new().with_actor(ActorId::from(vec![0xb0, at as u8])),
                    remote_writes: 0,
                })
                .collect(),
            rev: 0,
            cas: true,
        }
    }

    /// The root keys of the document as a reader sees it at `rev`.
    pub fn keys_at(&self, doc: usize, rev: Rev) -> BTreeSet<String> {
        let visible = self.docs[doc].visible_at(rev);
        visible.keys(ROOT).collect()
    }

    /// A change an external author builds on a document of their own, with no history: it adds
    /// the key `e{n}`.
    pub fn author_change(&self, doc: usize, n: u64) -> Change {
        let mut actor = vec![0xc0, doc as u8];
        actor.extend(n.to_be_bytes());
        let mut author = Automerge::new().with_actor(ActorId::from(actor));
        let mut tx = author.transaction();
        tx.put(ROOT, format!("e{n}"), 0_i64)
            .expect("the author writes");
        tx.commit();
        author
            .get_last_local_change()
            .expect("the change just made")
    }

    /// The tag of the local write that made `change`: the one root key it adds to the committed
    /// document, `l{tag}`. An external author's change has none.
    pub fn new_tag(&self, doc: usize, change: &Change) -> Option<u64> {
        let all = || self.docs[doc].changes.iter().map(|stored| &stored.change);
        let before: BTreeSet<String> = load(all()).keys(ROOT).collect();
        let after: BTreeSet<String> = load(all().chain([change])).keys(ROOT).collect();
        let added: Vec<_> = after.difference(&before).collect();
        // (a change that is already committed, which a commit that was reported failed can
        // leave behind, adds none)
        assert!(added.len() <= 1, "a write adds one key");
        added
            .first()?
            .strip_prefix('l')
            .map(|tag| tag.parse().expect("a tag"))
    }

    pub fn visible(&self, doc: usize) -> BTreeSet<String> {
        self.keys_at(doc, Rev::MAX)
    }

    /// The heads of the document as a reader sees it at `rev`.
    pub fn heads_at(&self, doc: usize, rev: Rev) -> Vec<ChangeHash> {
        sorted(self.docs[doc].visible_at(rev).get_heads())
    }

    /// Every committed change of the document, keyed or not.
    pub fn committed_hashes(&self, doc: usize) -> BTreeSet<ChangeHash> {
        self.docs[doc]
            .changes
            .iter()
            .map(|stored| stored.change.hash())
            .collect()
    }

    pub fn tree_rev(&self, doc: usize) -> Rev {
        self.docs[doc].tree_rev
    }

    /// Another node commits a change to the document, with or without its key arriving.
    pub fn remote_write(&mut self, doc: usize, keyed: bool) -> Rev {
        let model = &mut self.docs[doc];
        model.remote_writes += 1;
        let mut tx = model.remote.transaction();
        tx.put(ROOT, format!("r{}", model.remote_writes), 0_i64)
            .expect("the remote writes");
        tx.commit();
        let change = model
            .remote
            .get_last_local_change()
            .expect("the change just made");
        self.rev += 1;
        model.changes.push(Stored {
            change,
            rev: self.rev,
            keyed,
        });
        model.tree_rev = self.rev;
        self.rev
    }

    /// The keys of every remote change of the document arrive.
    pub fn grant_keys(&mut self, doc: usize) {
        for stored in &mut self.docs[doc].changes {
            stored.keyed = true;
        }
    }

    /// One transaction commits the changes of several documents at one revision, each with the
    /// frontier row it carries (written only if the tree was at the revision the row expects).
    /// Answers what each document's commit reports.
    pub fn commit(&mut self, batch: Vec<(usize, Change, Option<FrontierRow>)>) -> Vec<Landed> {
        self.rev += 1;
        let rev = self.rev;
        let mut landed = vec![];
        let mut rows = vec![];
        for (doc, change, row) in batch {
            let model = &mut self.docs[doc];
            let prior_rev = model.tree_rev;
            assert!(
                !model
                    .changes
                    .iter()
                    .any(|stored| stored.change.actor_id() == change.actor_id()
                        && stored.change.seq() == change.seq()
                        && stored.change.hash() != change.hash()),
                "doc {doc}: a duplicate SeqNumber: two different changes by one actor with one sequence number"
            );
            model.changes.push(Stored {
                change,
                rev,
                keyed: true,
            });
            model.tree_rev = rev;
            landed.push(Landed { rev, prior_rev });
            rows.extend(
                row.filter(|row| !self.cas || row.if_prior_rev == prior_rev)
                    .map(|row| (doc, row)),
            );
        }
        for (doc, row) in rows {
            let wanted = self.heads_at(doc, rev);
            assert_eq!(
                sorted(row.heads),
                wanted,
                "doc {doc}: a frontier row written at rev {rev} holds the heads of the tree at {rev}"
            );
            self.write_frontier(doc, wanted, rev);
        }
        landed
    }

    fn write_frontier(&mut self, doc: usize, heads: Vec<ChangeHash>, rev: Rev) {
        assert!(
            rev >= self.frontier_rev(doc),
            "doc {doc}: the frontier went back from {} to {rev}",
            self.frontier_rev(doc)
        );
        self.docs[doc].frontier = Some((heads, rev));
    }

    /// The machine publishes a frontier row on its own.
    pub fn publish(&mut self, doc: usize, heads: Vec<ChangeHash>, rev: Rev) {
        assert!(
            rev <= self.docs[doc].tree_rev,
            "doc {doc}: a frontier at {rev} is ahead of the tree"
        );
        assert_eq!(
            sorted(heads),
            self.heads_at(doc, rev),
            "doc {doc}: a frontier published at {rev} holds the heads of the tree at {rev}"
        );
        let wanted = self.heads_at(doc, rev);
        self.write_frontier(doc, wanted, rev);
    }

    pub fn frontier_rev(&self, doc: usize) -> Rev {
        self.docs[doc].frontier.as_ref().map_or(0, |(_, rev)| *rev)
    }

    /// What reading the document finds: the changes with keys, the revision, and what has none.
    pub fn materialize(&self, doc: usize) -> Materialized {
        let model = &self.docs[doc];
        assert!(model.readable, "an unreadable document is never read");
        Materialized {
            changes: model
                .changes
                .iter()
                .filter(|stored| stored.keyed)
                .map(|stored| stored.change.clone())
                .collect(),
            rev: model.tree_rev,
            frontier_rev: self.frontier_rev(doc),
            blocked: model.keyless(),
        }
    }
}
