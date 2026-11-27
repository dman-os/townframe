//! The executor against a naive model of all four stores, after every step of generated plans.
//!
//! The model keeps, per document, what a plan can change: the tree's revision and commit heads,
//! the published frontier, the key epoch; and per hive the certificates, the content keys, the
//! group-part membership and the revision counter. It decides the verdict, every `Landed` and every
//! frontier outcome from the plan alone, with no SQL, and then each store is read back and held to
//! it. Steps that break a write (a fault at the n-th statement) must change nothing.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::executor::block_on;
use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;
use sqlite_rs::QueryExecutor;
use sqlite_rs::SqliteDb;

use super::Doc;
use super::Rig;
use super::certificate;
use super::cgka_chain;
use super::content_key;
use super::content_ref;
use super::doc_key;
use super::dump;
use super::extra_part;
use super::flaky::Break;
use super::flaky::Flaky;
use super::fresh_db;
use super::frontier_obj;
use super::group;
use super::head;
use super::plan;
use super::tree;
use crate::document::Landed;
use crate::executor::DocReport;
use crate::executor::Frontier;
use crate::executor::FrontierOutcome;
use crate::executor::Verdict;
use crate::sedimentree::suite::pooled;
use crate::sqlite::queries;

const DOCS: u8 = 3;

#[derive(Clone, Debug)]
struct DocStep {
    id: u8,
    commits: Vec<u8>,
    /// Heads to publish, and whether the row is computed against a revision the tree is not at.
    frontier: Option<(Vec<u8>, bool)>,
    /// Whether the expected key epoch is one the document is not at.
    stale_epoch: Option<bool>,
}

#[derive(Clone, Debug)]
struct Step {
    docs: Vec<DocStep>,
    certificates: Vec<u8>,
    content_keys: Vec<(u8, u8)>,
    /// Documents whose CGKA history the plan extends by one operation.
    rotate: Vec<u8>,
    /// Documents whose frontier object the plan also puts in the extra part.
    extra: Vec<u8>,
    /// Break the n-th write statement, if the plan has that many.
    fault_at: Option<usize>,
}

fn doc_step() -> impl Strategy<Value = DocStep> {
    (
        0..DOCS,
        vec(0u8..4, 0..3),
        option::of((vec(0u8..4, 1..3), any::<bool>())),
        option::of(any::<bool>()),
    )
        .prop_map(|(id, commits, frontier, stale_epoch)| DocStep {
            id,
            commits,
            frontier,
            stale_epoch,
        })
}

fn step() -> impl Strategy<Value = Step> {
    (
        vec(doc_step(), 0..4),
        vec(3u8..8, 0..3),
        vec((0..DOCS, 0u8..3), 0..3),
        vec(0..DOCS, 0..2),
        vec(0..DOCS, 0..2),
        option::of(0usize..16),
    )
        .prop_map(
            |(docs, certificates, content_keys, rotate, extra, fault_at)| Step {
                docs,
                certificates,
                content_keys,
                rotate,
                extra,
                fault_at,
            },
        )
}

/// What a step must answer.
#[derive(Debug, PartialEq)]
enum Expect {
    Stale(u64),
    Committed {
        tree_rev: Option<u64>,
        docs: Vec<DocReport>,
    },
}

#[derive(Clone, Default)]
struct Model {
    /// The newest revision any tree has.
    rev: u64,
    tree_rev: BTreeMap<u8, u64>,
    heads: BTreeMap<u8, BTreeSet<u8>>,
    frontier: BTreeMap<u8, (Vec<u8>, u64)>,
    epoch: BTreeMap<u8, u64>,
    certificates: BTreeSet<u8>,
    content_keys: BTreeSet<(u8, u8)>,
    extra: BTreeSet<u8>,
}

impl Model {
    fn prior(&self, doc: u8) -> u64 {
        self.tree_rev.get(&doc).copied().unwrap_or(0)
    }

    fn epoch_of(&self, doc: u8) -> u64 {
        self.epoch.get(&doc).copied().unwrap_or(0)
    }

    /// The documents of a step, each once, in first-named order.
    fn docs(step: &Step) -> Vec<&DocStep> {
        let mut seen = BTreeSet::new();
        step.docs.iter().filter(|doc| seen.insert(doc.id)).collect()
    }

    /// What the real plan for `step` is, given where the model's documents are.
    async fn plan(&self, step: &Step) -> crate::executor::CommitPlan<super::Ref, ()> {
        let docs = Self::docs(step)
            .into_iter()
            .map(|doc| {
                let mut out = Doc::new(doc.id).commits(&doc.commits);
                if let Some((heads, stale)) = &doc.frontier {
                    out = out.frontier(heads, self.prior(doc.id) + if *stale { 1000 } else { 0 });
                }
                if let Some(stale) = doc.stale_epoch {
                    out = out.if_epoch(self.epoch_of(doc.id) + u64::from(stale));
                }
                out
            })
            .collect();
        let mut plan = plan(docs).await;
        plan.certificates = step.certificates.iter().map(|n| certificate(*n)).collect();
        plan.keyhive.content_keys = step
            .content_keys
            .iter()
            .map(|(doc, content)| {
                (
                    doc_key(*doc),
                    content_ref(*content),
                    content_key(*doc, *content),
                )
            })
            .collect();
        let rotate: BTreeSet<_> = step.rotate.iter().copied().collect();
        plan.keyhive.cgka_ops = rotate
            .into_iter()
            .map(|doc| (doc_key(doc), cgka_chain(doc, self.epoch_of(doc) + 1)))
            .collect();
        let extra: BTreeSet<_> = step.extra.iter().copied().collect();
        plan.parts.add = extra
            .into_iter()
            .map(|doc| (frontier_obj(doc), extra_part()))
            .collect();
        plan
    }

    /// Decide the step from the plan alone; on a commit, the model moves.
    fn apply(&mut self, step: &Step) -> Expect {
        let docs = Self::docs(step);
        if let Some(stale) = docs.iter().find(|doc| doc.stale_epoch == Some(true)) {
            return Expect::Stale(u64::from(stale.id));
        }
        let mut batch = None;
        let mut reports = Vec::new();
        for doc in docs {
            let prior = self.prior(doc.id);
            let held = self.heads.entry(doc.id).or_default();
            let fresh = doc.commits.iter().any(|head| !held.contains(head));
            held.extend(doc.commits.iter().copied());
            let rev = if fresh {
                *batch.get_or_insert(self.rev + 1)
            } else {
                prior
            };
            if fresh {
                self.tree_rev.insert(doc.id, rev);
            }
            let frontier = match &doc.frontier {
                None => FrontierOutcome::Absent,
                Some((_, true)) => FrontierOutcome::Stale,
                Some((heads, false)) => {
                    self.frontier.insert(doc.id, (heads.clone(), rev));
                    FrontierOutcome::Written
                }
            };
            reports.push(DocReport {
                doc: u64::from(doc.id),
                landed: Landed {
                    rev,
                    prior_rev: prior,
                },
                frontier,
            });
        }
        self.rev = batch.unwrap_or(self.rev);
        self.certificates.extend(step.certificates.iter().copied());
        self.content_keys.extend(step.content_keys.iter().copied());
        for doc in step.rotate.iter().collect::<BTreeSet<_>>() {
            *self.epoch.entry(*doc).or_default() += 1;
        }
        self.extra.extend(step.extra.iter().copied());
        Expect::Committed {
            tree_rev: batch,
            docs: reports,
        }
    }
}

/// Every store, read back, against the model.
async fn check<D: SqliteDb>(rig: &Rig<D>, model: &Model, at: &str) {
    let feed = rig.feed().await;
    let wanted: BTreeMap<_, _> = model
        .tree_rev
        .iter()
        .map(|(doc, rev)| (tree(*doc), *rev))
        .collect();
    assert_eq!(feed, wanted, "{at}: the change feed");

    let snapshot = rig.db.begin_read().await.unwrap();
    for doc in 0..DOCS {
        let rows = queries::commit_ids(&snapshot, tree(doc).as_bytes().to_vec())
            .await
            .unwrap();
        let held: BTreeSet<Vec<u8>> = rows.into_iter().collect();
        let wanted: BTreeSet<Vec<u8>> = model
            .heads
            .get(&doc)
            .into_iter()
            .flatten()
            .filter(|_| model.tree_rev.contains_key(&doc))
            .map(|head| pooled(*head).as_bytes().to_vec())
            .collect();
        assert_eq!(held, wanted, "{at}: commits of document {doc}");

        assert_eq!(
            rig.epoch(doc).await,
            model.epoch_of(doc),
            "{at}: epoch of document {doc}"
        );
        for content in 0..3 {
            let key = rig
                .keyhive
                .session(&snapshot)
                .get_cached_content_key(doc_key(doc), &content_ref(content))
                .await
                .unwrap();
            let wanted = model
                .content_keys
                .contains(&(doc, content))
                .then(|| content_key(doc, content));
            assert_eq!(
                key.map(|key| key.as_slice().to_vec()),
                wanted.map(|key| key.as_slice().to_vec()),
                "{at}: content key {doc}/{content}"
            );
        }
    }
    let certificates: i64 = snapshot
        .query("SELECT count(*) FROM keyline_certificates", ())
        .await
        .unwrap()
        .one()
        .unwrap();
    assert_eq!(
        certificates,
        model.certificates.len() as i64,
        "{at}: certificates"
    );
    D::end_read(snapshot).await.unwrap();

    for doc in 0..DOCS {
        let wanted = model.frontier.get(&doc).map(|(heads, rev)| {
            let mut parts = BTreeSet::from([group()]);
            if model.extra.contains(&doc) {
                parts.insert(extra_part());
            }
            (
                Frontier {
                    heads: heads.iter().copied().map(head).collect(),
                    rev: *rev,
                },
                parts,
            )
        });
        assert_eq!(rig.frontier(doc).await, wanted, "{at}: frontier of {doc}");
    }
}

async fn run(steps: Vec<Step>) {
    let db = fresh_db().await;
    let rig = Rig::on(Flaky::new(db.clone())).await;
    let mut model = Model::default();
    for (index, step) in steps.iter().enumerate() {
        let at = format!("step {index} {step:?}");
        let plan = model.plan(step).await;
        let before = dump(&db).await;
        match step.fault_at {
            Some(write) => rig.db.control.arm(write, Break::Fail),
            None => rig.db.control.disarm(),
        }
        match rig.exec.execute(plan).await {
            Err(_) => {
                assert!(step.fault_at.is_some(), "{at}: a fault nobody injected");
                assert_eq!(dump(&db).await, before, "{at}: a fault left rows");
            }
            Ok(verdict) => {
                let mut next = model.clone();
                let expect = next.apply(step);
                match (verdict, expect) {
                    (Verdict::Stale { doc }, Expect::Stale(wanted)) => {
                        assert_eq!(doc, wanted, "{at}");
                        assert_eq!(dump(&db).await, before, "{at}: a stale plan left rows");
                    }
                    (Verdict::Committed(done), Expect::Committed { tree_rev, docs }) => {
                        assert_eq!(done.tree_rev, tree_rev, "{at}");
                        assert_eq!(done.docs, docs, "{at}");
                        model = next;
                    }
                    (verdict, expect) => panic!("{at}: answered {verdict:?}, wanted {expect:?}"),
                }
            }
        }
        rig.db.control.disarm();
        check(&rig, &model, &at).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn the_executor_agrees_with_the_model_after_every_step(steps in vec(step(), 1..8)) {
        block_on(run(steps));
    }
}
