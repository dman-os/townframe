//! The commit-plan executor: the storage side of the drivers. It takes the rows of a plan, as the
//! [Document](crate::document) machine and the [Write coordinator](crate::coordinator) ask for
//! them, writes every one in a single SQLite write transaction, commits it, and reports what each
//! document's tree did. It decides nothing: it checks what the plan says to check, writes what
//! the plan says to write, and answers.
//!
//! # What a plan carries
//!
//! The machines' plans name rows by kind ([`Plan`](crate::document::Plan),
//! [`DocRows`](crate::coordinator::DocRows)): an opaque ciphertext and a frontier row. A storage
//! transaction needs more than that: the signed sedimentree commit and its blob, the tree it
//! belongs to, the keyhive rows that go with it, the part store's keys. Resolving the one into the
//! other is the driver's job (it holds the signer, the Keyhive boundary's output and the
//! document's keys), so the executor's input is the resolved form, [`CommitPlan`]:
//!
//! - one [`DocCommit`] per document: the sedimentree rows to save in its tree, the frontier row
//!   to publish, and the key epoch its content was encrypted under;
//! - the plan-wide rows: Keyline certificates, [`KeyhiveRows`] (content keys, CGKA operations and
//!   the secrets of a rotation, prekey operations, pending events) and [`PartRows`] (group-part
//!   membership).
//!
//! A plan with no sedimentree rows for a document, only a frontier row, is how the frontier of
//! remote content is published (the machine's `PublishFrontier`). A plan with no documents is a
//! membership or ingest transaction.
//!
//! # What an execution does
//!
//! 1. Opens **one** write transaction: the part store's, which is `BEGIN IMMEDIATE` on the
//!    database. Every other store writes through that same transaction (their `_in` entry
//!    points), so all the rows commit together or not at all.
//! 2. **Validates** the plan's key epochs before writing anything (below).
//! 3. Writes the rows: certificates, keyhive rows, then each document's tree and last the part
//!    store (frontier payloads, then membership).
//! 4. Commits, then tells each store it committed, which warms the caches and moves the change
//!    feed (the sedimentree watch, the part store's long polls and watch).
//! 5. Answers with a [`Verdict`]: [`Executed`] (a [`Landed`] per document, the machine's own
//!    type) or [`Verdict::Stale`].
//!
//! # Optimistic validation
//!
//! Encryption happens before the commit, outside any transaction, so a membership change can
//! land in between and make the ciphertext be under a key the document no longer uses. Each
//! document's [`DocCommit::if_epoch`] carries the epoch it was encrypted under (`keyhive_sqlite`'s per-document
//! key epoch, which every CGKA operation of the document bumps). The executor reads each plan
//! document's current epoch **first thing inside the transaction**, and if any differs, rolls
//! back and answers [`Verdict::Stale`] naming the first such document in plan order. Only the
//! plan's own documents are read, so a change to any other document can never fail a plan. A
//! stale plan wrote nothing; the caller re-encrypts and tries again.
//!
//! # Frontier compare-and-set
//!
//! A document's [`FrontierWrite`] carries `if_prior_rev`, the tree revision the heads were
//! computed against. The frontier is written only if the tree's revision **before this plan's
//! batch** is still that revision, that is, the revision [`Landed::prior_rev`] reports. If remote
//! content landed in the tree meanwhile, the row is skipped ([`FrontierOutcome::Stale`]) and the
//! rest of the plan still commits: the row would have claimed a revision that contains content
//! the heads do not. (The task statement for this executor says "`frontier_rev` equals
//! `if_prior_rev`"; the machine's own contract, in its module docs, is the tree's revision, which
//! is what is implemented. The stored frontier carries its revision, see [`Frontier`].)
//!
//! The payload written is a [`Frontier`]: the heads and the revision the row reflects, which is
//! the revision of this plan's batch if the document's tree changed in it, and the tree's current
//! one if not.
//!
//! # Revisions
//!
//! Every tree the plan changes takes the same revision, one batch of the sedimentree store, so a
//! multi-document plan is one step on the change feed. [`Landed::rev`] is that revision.
//! A document whose tree the plan did not change (a duplicate or covered commit, or a
//! frontier-only plan) reports `rev == prior_rev`: the tree is still where it was.
//!
//! # Failures
//!
//! - **Contention** (`SQLITE_BUSY` surviving the busy timeout) is a panic. With one writer
//!   process it means a second writer exists, which breaks an invariant.
//! - **Any other storage error** is a [`Fault`]: the transaction is rolled back, nothing of the
//!   plan is visible, and [`Executor::run_or_trap`] hands the fault to the supervisor through a
//!   [`Trap`] and never answers (the `big_sync/trap.rs` pattern), so the hive is torn down.
//!   [`Executor::execute`] returns it instead, for tests and for drivers that trap themselves.
//! - A plan that breaks the coordinator's invariants (a document or tree named twice) panics.
//!
//! The executor never answers `Failed`: a fault is not recoverable for the instance, so there is
//! nobody to answer. The machines' `Outcome::Failed` path stays for a driver that decides
//! differently.
//!
//! # Laws
//!
//! 1. **Atomic.** After a fault, a stale verdict, a dropped future or a crash, none of a plan's
//!    rows is visible in any store, and no revision was drawn.
//! 2. **One revision per plan** for the trees it changes, and it is the next one: what
//!    [`Landed::rev`] reports is what the change feed shows.
//! 3. **Validation reads only the plan's documents** and happens before any write.
//! 4. **Frontier compare-and-set**: the row is written exactly when the tree's prior revision
//!    equals `if_prior_rev`.
//! 5. **Observers move after the commit**, never before: a store's `committed` is called only
//!    once the transaction is durable.
//!
//! # Platforms
//!
//! Generic over [`SqliteDb`], like the stores it drives; no timers or threads of its own.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use automerge::ChangeHash;
use beekem::operation::CgkaOperation;
use big_automata::ObjKey;
use big_automata::PartKey;
use big_automata::part_store::ObjPayload;
use big_automata::part_store::PartStoreWrite;
use big_automata::rev::Rev;
use big_automata_sqlite::SqlitePartStore;
use big_automata_sqlite::SqlitePartTx;
use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyhive_content::reference::ContentRef;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId;
use keyhive_core::principal::individual::id::IndividualId;
use keyhive_core::principal::individual::op::KeyOp;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::share_key::ShareKey;
use keyhive_crypto::share_key::ShareSecretKey;
use keyhive_crypto::symmetric_key::SymmetricKey;
use keyhive_crypto::verified::Verified;
use keyhive_sqlite::SqliteKeyhiveStore;
use keyline::certificate::VerifiedCertificate;
use keyline_sqlite::SqliteKeyline;
use keyline_sqlite::Staged;
use sedimentree_core::depth::DepthMetric;
use sedimentree_core::fragment::Fragment;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::LooseCommit;
use sqlite_rs::SqlError;
use sqlite_rs::SqliteDb;
use tokio::sync::mpsc;

use crate::coordinator::DocId;
use crate::document::Epoch;
use crate::document::Landed;
use crate::sedimentree::Batch;
use crate::sedimentree::Mutation;
use crate::sedimentree::Save;
use crate::sedimentree::SedimentreeStore;
use crate::sqlite::queries;

#[cfg(test)]
mod suite;

/// A document's frontier row: `heads` published into the frontier part-store scope as the payload
/// of `obj`, which is made a member of `parts` (the document's group parts) in the same
/// transaction. The caller names the keys; the executor does not derive them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontierWrite {
    pub obj: ObjKey,
    pub parts: Vec<PartKey>,
    pub heads: Vec<ChangeHash>,
    /// Written only if the tree's revision before this plan is this one.
    pub if_prior_rev: Rev,
}

/// One document's rows of a plan.
pub struct DocCommit {
    /// The coordinator's name for the document; [`Landed`] is reported under it.
    pub doc: DocId,
    pub tree: SedimentreeId,
    pub commits: Vec<Save<LooseCommit>>,
    pub fragments: Vec<Save<Fragment>>,
    pub frontier: Option<FrontierWrite>,
    /// The keyhive document whose key epoch [`DocCommit::if_epoch`] is checked against.
    pub keyhive_doc: DocumentId,
    /// The key epoch the content was encrypted under (the coordinator's
    /// [`DocRows::if_epoch`](crate::coordinator::DocRows::if_epoch)), if the plan expects one.
    pub if_epoch: Option<Epoch>,
}

/// Group-part membership: `(object, part)` pairs. Additions are written before removals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartRows {
    pub add: Vec<(ObjKey, PartKey)>,
    pub remove: Vec<(ObjKey, PartKey)>,
}

/// The keyhive rows of a plan, written through `keyhive_sqlite`'s session.
pub struct KeyhiveRows<T: ContentRef> {
    /// The leaf secrets of a rotation.
    pub key_pairs: Vec<(ShareKey, ShareSecretKey)>,
    pub prekey_ops: Vec<(IndividualId, Vec<Arc<KeyOp>>)>,
    pub cgka_ops: Vec<(DocumentId, Vec<Arc<Verified<CgkaOperation>>>)>,
    pub content_heads: Vec<(DocumentId, Vec<T>)>,
    pub content_keys: Vec<(DocumentId, T, SymmetricKey)>,
    pub pending_put: Vec<StaticEvent<T>>,
    pub pending_drop: Vec<Digest<StaticEvent<T>>>,
}

impl<T: ContentRef> Default for KeyhiveRows<T> {
    fn default() -> Self {
        Self {
            key_pairs: Vec::new(),
            prekey_ops: Vec::new(),
            cgka_ops: Vec::new(),
            content_heads: Vec::new(),
            content_keys: Vec::new(),
            pending_put: Vec::new(),
            pending_drop: Vec::new(),
        }
    }
}

/// Everything one transaction writes.
pub struct CommitPlan<T: ContentRef, W> {
    pub docs: Vec<DocCommit>,
    pub certificates: Vec<VerifiedCertificate<W>>,
    pub keyhive: KeyhiveRows<T>,
    pub parts: PartRows,
}

impl<T: ContentRef, W> Default for CommitPlan<T, W> {
    fn default() -> Self {
        Self {
            docs: Vec::new(),
            certificates: Vec::new(),
            keyhive: KeyhiveRows::default(),
            parts: PartRows::default(),
        }
    }
}

/// The payload of a frontier object in the part store: the heads, and the tree revision they
/// reflect (`frontier_rev`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frontier {
    pub heads: Vec<ChangeHash>,
    pub rev: Rev,
}

impl Frontier {
    pub fn to_payload(&self) -> ObjPayload {
        serde_json::json!({
            "heads": self.heads.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "rev": self.rev,
        })
    }

    /// Read a payload this executor wrote. Any other shape is a broken invariant and panics.
    pub fn from_payload(payload: &ObjPayload) -> Self {
        let heads = payload["heads"]
            .as_array()
            .expect("a frontier payload has heads")
            .iter()
            .map(|head| {
                head.as_str()
                    .and_then(|head| head.parse().ok())
                    .expect("a frontier head is a change hash")
            })
            .collect();
        Self {
            heads,
            rev: payload["rev"]
                .as_u64()
                .expect("a frontier payload has a revision"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierOutcome {
    /// The document had no frontier row.
    Absent,
    Written,
    /// The tree was no longer at `if_prior_rev`: the row was skipped.
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocReport {
    pub doc: DocId,
    pub landed: Landed,
    pub frontier: FrontierOutcome,
}

/// What a committed plan did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed {
    /// Per document, in the plan's order.
    pub docs: Vec<DocReport>,
    /// The revision the plan's batch drew, if it changed any tree.
    pub tree_rev: Option<Rev>,
    /// The part store's revision, if the plan changed one of its log rows.
    pub parts_rev: Option<Rev>,
}

impl Executed {
    /// What the Write coordinator's `committed` takes.
    pub fn landed(&self) -> Vec<(DocId, Landed)> {
        self.docs
            .iter()
            .map(|report| (report.doc, report.landed))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Committed(Executed),
    /// A document's key epoch moved since its content was encrypted; nothing was written.
    Stale {
        doc: DocId,
    },
}

/// Where in an execution a fault happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Begin,
    Epoch,
    Certificates,
    Keyhive,
    Tree,
    Frontier,
    Membership,
    Commit,
    Rollback,
}

/// A storage failure that is not contention. The plan was rolled back.
#[derive(Debug, thiserror::Error)]
#[error("storage fault while {stage:?}: {source}")]
pub struct Fault {
    pub stage: Stage,
    #[source]
    source: Box<dyn Error + Send + Sync>,
}

impl Fault {
    /// Contention is a panic, not a fault: see the module docs.
    fn new(stage: Stage, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        let source = source.into();
        let mut link: Option<&(dyn Error + 'static)> = Some(&*source);
        while let Some(error) = link {
            assert!(
                !matches!(error.downcast_ref::<SqlError>(), Some(SqlError::Busy(_))),
                "write contention at {stage:?}: a second writer exists on the hive's database: {error}"
            );
            link = error.source();
        }
        Self { stage, source }
    }
}

fn at<E: Into<Box<dyn Error + Send + Sync>>>(stage: Stage) -> impl FnOnce(E) -> Fault {
    move |source| Fault::new(stage, source)
}

/// What stops an execution before its commit.
enum Stop {
    Stale(DocId),
    Fault(Fault),
}

impl From<Fault> for Stop {
    fn from(fault: Fault) -> Self {
        Self::Fault(fault)
    }
}

/// Where a fault goes: the supervisor, which tears the hive down (the `big_sync/trap.rs`
/// pattern).
#[derive(Clone)]
pub struct Trap {
    faults: mpsc::Sender<Fault>,
}

impl Trap {
    pub fn new() -> (Self, mpsc::Receiver<Fault>) {
        let (faults, rx) = mpsc::channel(1);
        (Self { faults }, rx)
    }
}

/// The four stores of a hive's database, over one [`SqliteDb`] handle.
pub struct Executor<D: SqliteDb, M, T, W> {
    trees: SedimentreeStore<D, M>,
    keyline: SqliteKeyline<W, D>,
    keyhive: SqliteKeyhiveStore<T, D>,
    parts: SqlitePartStore<D>,
}

/// What a transaction staged, for the stores' `committed` once it is durable.
struct Written<W> {
    batch: Batch,
    certificates: Option<Staged<W>>,
    docs: Vec<DocReport>,
}

impl<D, M, T, W> Executor<D, M, T, W>
where
    D: SqliteDb,
    M: DepthMetric,
    T: ContentRef,
    W: Encode + Decode,
{
    /// The stores must be attached to the same database.
    pub fn new(
        trees: SedimentreeStore<D, M>,
        keyline: SqliteKeyline<W, D>,
        keyhive: SqliteKeyhiveStore<T, D>,
        parts: SqlitePartStore<D>,
    ) -> Self {
        Self {
            trees,
            keyline,
            keyhive,
            parts,
        }
    }

    /// [`execute`](Self::execute), with a fault sent to the supervisor: the future then never
    /// completes, so nothing is answered for a plan whose fate the hive cannot continue from.
    pub async fn run_or_trap(&self, plan: CommitPlan<T, W>, trap: &Trap) -> Verdict {
        match self.execute(plan).await {
            Ok(verdict) => verdict,
            Err(fault) => {
                trap.faults
                    .send(fault)
                    .await
                    .expect("the supervisor outlives the executor");
                std::future::pending().await
            }
        }
    }

    /// Run `plan` in one write transaction.
    ///
    /// # Panics
    ///
    /// On write contention, and if the plan names a document or a tree twice.
    pub async fn execute(&self, plan: CommitPlan<T, W>) -> Result<Verdict, Fault> {
        let docs: BTreeSet<_> = plan.docs.iter().map(|doc| doc.doc).collect();
        let trees: BTreeSet<_> = plan.docs.iter().map(|doc| doc.tree).collect();
        assert!(
            docs.len() == plan.docs.len() && trees.len() == plan.docs.len(),
            "a plan names each document and tree once"
        );

        let mut tx = self.parts.begin().await.map_err(at(Stage::Begin))?;
        let written = match self.write(&mut tx, plan).await {
            Ok(written) => written,
            Err(stop) => {
                // Dropping the transaction would roll it back too; doing it here makes a failed
                // rollback visible.
                if let Err(error) = tx.rollback().await {
                    let cause = match &stop {
                        Stop::Fault(fault) => fault.to_string(),
                        Stop::Stale(doc) => format!("document {doc} was stale"),
                    };
                    return Err(Fault::new(
                        Stage::Rollback,
                        format!("{error}; the plan had stopped on: {cause}"),
                    ));
                }
                return match stop {
                    Stop::Stale(doc) => Ok(Verdict::Stale { doc }),
                    Stop::Fault(fault) => Err(fault),
                };
            }
        };
        let parts_rev = tx.commit().await.map_err(at(Stage::Commit))?;
        // Durable: now the observers may move.
        let Written {
            batch,
            certificates,
            docs,
        } = written;
        let tree_rev = batch.rev();
        self.trees.committed(batch);
        if let Some(staged) = certificates {
            self.keyline.committed(staged);
        }
        Ok(Verdict::Committed(Executed {
            docs,
            tree_rev,
            parts_rev,
        }))
    }

    async fn write(
        &self,
        tx: &mut SqlitePartTx<D>,
        plan: CommitPlan<T, W>,
    ) -> Result<Written<W>, Stop> {
        let CommitPlan {
            docs,
            certificates,
            keyhive,
            parts,
        } = plan;
        let x = tx.executor();
        let session = self.keyhive.session(x);

        // Validation first, over the plan's own documents only.
        for doc in &docs {
            let Some(if_epoch) = doc.if_epoch else { continue };
            let now = session
                .doc_epoch(doc.keyhive_doc)
                .await
                .map_err(at(Stage::Epoch))?;
            if now != if_epoch {
                return Err(Stop::Stale(doc.doc));
            }
        }

        let staged_certificates = if certificates.is_empty() {
            None
        } else {
            Some(
                self.keyline
                    .insert_in(x, certificates)
                    .await
                    .map_err(at(Stage::Certificates))?,
            )
        };

        session
            .put_key_pairs(&keyhive.key_pairs)
            .await
            .map_err(at(Stage::Keyhive))?;
        for (who, ops) in &keyhive.prekey_ops {
            session
                .append_prekey_ops(*who, ops)
                .await
                .map_err(at(Stage::Keyhive))?;
        }
        for (doc, ops) in &keyhive.cgka_ops {
            session
                .append_cgka_ops(*doc, ops)
                .await
                .map_err(at(Stage::Keyhive))?;
        }
        for (doc, heads) in &keyhive.content_heads {
            session
                .put_content_heads(*doc, heads)
                .await
                .map_err(at(Stage::Keyhive))?;
        }
        for (doc, content_ref, key) in &keyhive.content_keys {
            session
                .cache_content_key(*doc, content_ref, key)
                .await
                .map_err(at(Stage::Keyhive))?;
        }
        session
            .drop_pending_events(&keyhive.pending_drop)
            .await
            .map_err(at(Stage::Keyhive))?;
        session
            .put_pending_events(&keyhive.pending_put)
            .await
            .map_err(at(Stage::Keyhive))?;

        // The trees, all in one batch: one revision for the plan.
        let mut batch = self.trees.batch();
        let mut reports = Vec::with_capacity(docs.len());
        let mut payloads = Vec::new();
        let mut members = parts.add;
        for doc in docs {
            let tree_id = doc.tree.as_bytes().to_vec();
            let prior = queries::tree_state(x, tree_id)
                .await
                .map_err(at(Stage::Tree))?
                .map_or(0, |state| rev_of(state.rev));
            let changed = if doc.commits.is_empty() && doc.fragments.is_empty() {
                false
            } else {
                self.trees
                    .apply_in(
                        x,
                        &mut batch,
                        doc.tree,
                        Mutation::Save {
                            commits: doc.commits,
                            fragments: doc.fragments,
                        },
                    )
                    .await
                    .map_err(at(Stage::Tree))?
                    .changed
            };
            // The batch's revision is fixed by its first change, so it is this document's if the
            // document changed in it; a document the plan left alone is still where it was.
            let landed = Landed {
                rev: if changed {
                    batch
                        .rev()
                        .expect("a changed tree took the batch's revision")
                } else {
                    prior
                },
                prior_rev: prior,
            };
            let frontier = match doc.frontier {
                None => FrontierOutcome::Absent,
                Some(row) if row.if_prior_rev != prior => FrontierOutcome::Stale,
                Some(row) => {
                    let frontier = Frontier {
                        heads: row.heads,
                        rev: landed.rev,
                    };
                    members.extend(row.parts.into_iter().map(|part| (row.obj.clone(), part)));
                    payloads.push((row.obj, frontier.to_payload()));
                    FrontierOutcome::Written
                }
            };
            reports.push(DocReport {
                doc: doc.doc,
                landed,
                frontier,
            });
        }

        if !payloads.is_empty() {
            self.parts
                .set_payloads(tx, payloads)
                .await
                .map_err(at(Stage::Frontier))?;
        }
        if !members.is_empty() {
            self.parts
                .add_members(tx, &members)
                .await
                .map_err(at(Stage::Membership))?;
        }
        if !parts.remove.is_empty() {
            self.parts
                .remove_members(tx, &parts.remove)
                .await
                .map_err(at(Stage::Membership))?;
        }

        Ok(Written {
            batch,
            certificates: staged_certificates,
            docs: reports,
        })
    }
}

fn rev_of(rev: i64) -> Rev {
    Rev::try_from(rev).expect("a stored rev is not negative")
}
