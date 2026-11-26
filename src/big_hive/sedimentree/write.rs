//! Mutations: what one transaction does to one tree.

use std::collections::BTreeSet;
use std::sync::Arc;

use sedimentree_core::blob::has_meta::HasBlobMeta;
use sedimentree_core::codec::decode::DecodeFields;
use sedimentree_core::codec::encode::Encode;
use sedimentree_core::codec::encode::EncodeFields;
use sedimentree_core::codec::schema::Schema;
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::depth::DepthMetric;
use sedimentree_core::fragment::Fragment;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::LooseCommit;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_core::sedimentree::Sedimentree;
use sedimentree_core::sedimentree::minimized::MinimizedSedimentree;
use sqlite_rs::Executor;
use sqlite_rs::Sql;
use sqlite_rs::SqliteDb;
use subduction_crypto::signed::Signed;
use subduction_crypto::verified_meta::VerifiedMeta;

use big_automata::rev::Rev;

use super::Error;
use super::SedimentreeStore;
use crate::sqlite::queries;

/// Bugs the tests plant on purpose, to see the differential catch each (see
/// `suite/differential.rs`). Nothing plants one outside a test.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Plant {
    /// A save leaves the rows it covers in place.
    KeepCovered,
    /// A save stores an item it just found covered.
    StoreCovered,
    /// A delete that removes rows does not move the `rev`.
    SilentDelete,
    /// A hit is served from the cache without checking its `rev` against the database.
    StaleCache,
}

#[cfg(test)]
thread_local! {
    /// The futures run on the test's own thread (`block_on`), so a plant is per test.
    pub(super) static PLANT: std::cell::Cell<Option<Plant>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(super) fn planted(plant: Plant) -> bool {
    PLANT.with(|slot| slot.get() == Some(plant))
}

#[cfg(not(test))]
pub(super) const fn planted(_: Plant) -> bool {
    false
}

/// Headroom under SQLite's default `SQLITE_MAX_VARIABLE_NUMBER` of 32766.
const MAX_PARAMS: usize = 30_000;
/// Variables one row of either item table binds.
const ROW_WIDTH: usize = 6;

/// An item to store, and what its writer knows about its ciphertext.
#[derive(Clone)]
pub struct Save<T: Schema + EncodeFields + DecodeFields> {
    pub verified: VerifiedMeta<T>,
    /// The hash of the PCS update the blob was encrypted under, or `None` for a plaintext item.
    /// Stored in its own column (see [`SedimentreeStore::commit_blobs_under`]); nothing decodes
    /// the blob to learn it.
    pub pcs_update_hash: Option<[u8; 32]>,
}

impl<T: Schema + EncodeFields + DecodeFields> From<VerifiedMeta<T>> for Save<T> {
    fn from(verified: VerifiedMeta<T>) -> Self {
        Self {
            verified,
            pcs_update_hash: None,
        }
    }
}

/// What a transaction does to one tree. Every variant registers or retires the tree and moves
/// its `rev` only if it changed something.
pub enum Mutation {
    /// Make the tree exist, empty if it was not.
    Register,
    /// Delete the tree: every row, and the registration. The tree id keeps a tombstone row.
    Retire,
    /// Store items, then minimize: what the tree already holds and what the new items cover is
    /// deleted, and an item that is covered on arrival is not stored. Registers the tree.
    Save {
        commits: Vec<Save<LooseCommit>>,
        fragments: Vec<Save<Fragment>>,
    },
    DeleteCommit(CommitId),
    DeleteCommits,
    DeleteFragment(CommitId),
    DeleteFragments,
}

/// One atomic revision of the hive under construction: every tree its mutations change gets the
/// same `rev` (the next one, taken at the first change), and the mutations join the caller's
/// transaction. Several batches may share a transaction; each is its own revision.
///
/// Hand it to [`SedimentreeStore::committed`] after the transaction commits. Drop it after a
/// rollback: nothing outside the transaction ever saw the change.
#[must_use = "pass it to `SedimentreeStore::committed` once the transaction commits, or drop it after a rollback"]
#[derive(Default)]
pub struct Batch {
    rev: Option<i64>,
    staged: Vec<Staged>,
}

impl Batch {
    /// The revision, once a mutation of the batch changed something.
    pub fn rev(&self) -> Option<Rev> {
        self.rev.map(rev_from_sql)
    }
}

/// What one mutation of a batch did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
    /// Whether the tree changed (and so took the batch's `rev`).
    pub changed: bool,
    /// How many rows a save inserted: the new items that were not covered on arrival.
    pub saved: usize,
}

pub(super) fn rev_from_sql(rev: i64) -> Rev {
    Rev::try_from(rev).expect("a stored rev is positive")
}

/// One changed tree of a batch, for the cache once the transaction commits.
pub(super) struct Staged {
    tree_id: SedimentreeId,
    /// The tree after a save, which the cache can carry forward.
    tree: Option<MinimizedSedimentree>,
    saved: usize,
    /// The `rev` the tree took, if it changed.
    rev: Option<i64>,
}

impl Staged {
    fn unchanged(tree_id: SedimentreeId) -> Self {
        Self {
            tree_id,
            tree: None,
            saved: 0,
            rev: None,
        }
    }

    fn changed(tree_id: SedimentreeId, rev: i64) -> Self {
        Self {
            tree_id,
            tree: None,
            saved: 0,
            rev: Some(rev),
        }
    }
}

/// The two item tables, which differ in a name and a column.
trait Kind: Schema + EncodeFields + DecodeFields + HasBlobMeta + Encode + Clone {
    const INSERT: &'static str;
    const DELETE: &'static str;
    /// The column of the head in the table's primary key.
    const HEAD_COLUMN: &'static str;
    fn head(&self) -> CommitId;
}

impl Kind for LooseCommit {
    const INSERT: &'static str = "INSERT INTO sedimentree_commits\n     ( tree_id\n     , commit_id\n     , digest\n     , pcs_update_hash\n     , signed\n     , blob\n     )\nVALUES ";
    const DELETE: &'static str = "DELETE FROM sedimentree_commits\n WHERE tree_id = ";
    const HEAD_COLUMN: &'static str = "commit_id";
    fn head(&self) -> CommitId {
        LooseCommit::head(self)
    }
}

impl Kind for Fragment {
    const INSERT: &'static str = "INSERT INTO sedimentree_fragments\n     ( tree_id\n     , fragment_head\n     , digest\n     , pcs_update_hash\n     , signed\n     , blob\n     )\nVALUES ";
    const DELETE: &'static str = "DELETE FROM sedimentree_fragments\n WHERE tree_id = ";
    const HEAD_COLUMN: &'static str = "fragment_head";
    fn head(&self) -> CommitId {
        Fragment::head(self)
    }
}

/// An item of the tree, by the key its row has.
struct Held<T> {
    head: CommitId,
    digest: [u8; 32],
    payload: T,
}

impl<T: Kind> Held<T> {
    fn of(payload: T) -> Self {
        Self {
            head: payload.head(),
            digest: Digest::hash(&payload).into_bytes(),
            payload,
        }
    }

    fn key(&self) -> (CommitId, [u8; 32]) {
        (self.head, self.digest)
    }
}

/// A new item with its row.
struct Fresh<T> {
    held: Held<T>,
    signed: Vec<u8>,
    blob: Vec<u8>,
    pcs_update_hash: Option<[u8; 32]>,
}

impl<T: Kind> Fresh<T> {
    fn of(save: Save<T>) -> Self {
        let (signed, payload, blob) = save.verified.into_full_parts();
        Self {
            held: Held::of(payload),
            signed: signed.as_bytes().to_vec(),
            blob: blob.into_contents(),
            pcs_update_hash: save.pcs_update_hash,
        }
    }
}

/// The rows of a tree, decoded.
#[derive(Default)]
pub(super) struct Stored {
    commits: Vec<Held<LooseCommit>>,
    fragments: Vec<Held<Fragment>>,
}

impl Stored {
    pub(super) async fn read(
        tx: &impl sqlite_rs::QueryExecutor,
        tree_id: &[u8],
    ) -> Result<Self, Error> {
        let mut stored = Self::default();
        for row in queries::tree_items(tx, tree_id.to_vec()).await? {
            let digest = digest_of(row.digest);
            if row.kind == 0 {
                stored.commits.push(Held {
                    head: commit_id_of(&row.head),
                    digest,
                    payload: Signed::<LooseCommit>::try_decode(&row.signed)?
                        .try_decode_trusted_payload()?,
                });
            } else {
                stored.fragments.push(Held {
                    head: commit_id_of(&row.head),
                    digest,
                    payload: Signed::<Fragment>::try_decode(&row.signed)?
                        .try_decode_trusted_payload()?,
                });
            }
        }
        Ok(stored)
    }

    /// The tree these rows are. Stored trees are minimal, so it needs no minimizing.
    pub(super) fn into_tree(self) -> MinimizedSedimentree {
        MinimizedSedimentree::already_minimal(Sedimentree::new(
            self.fragments
                .into_iter()
                .map(|held| held.payload)
                .collect(),
            self.commits.into_iter().map(|held| held.payload).collect(),
        ))
    }
}

pub(super) fn commit_id_of(bytes: &[u8]) -> CommitId {
    CommitId::new(bytes.try_into().expect("a stored head is 32 bytes (CHECK)"))
}

fn digest_of(bytes: Vec<u8>) -> [u8; 32] {
    bytes
        .try_into()
        .expect("a stored digest is 32 bytes (CHECK)")
}

/// What a save does to the rows of one table.
struct Plan<T> {
    /// Heads whose rows go: covered by the new minimization, or replaced by a lower digest.
    delete: Vec<CommitId>,
    insert: Vec<Fresh<T>>,
}

/// Compare the rows of a table with the minimized tree, and the new items with the rows.
fn plan<T: Kind>(
    stored: &[Held<T>],
    incoming: Vec<Fresh<T>>,
    kept: &BTreeSet<(CommitId, [u8; 32])>,
) -> Plan<T> {
    let delete = stored
        .iter()
        .filter(|held| !planted(Plant::KeepCovered) && !kept.contains(&held.key()))
        .map(|held| held.head)
        .collect();
    let present: BTreeSet<_> = stored.iter().map(Held::key).collect();
    let mut seen = BTreeSet::new();
    let insert = incoming
        .into_iter()
        .filter(|fresh| {
            let key = fresh.held.key();
            (kept.contains(&key) || planted(Plant::StoreCovered))
                && !present.contains(&key)
                && seen.insert(key)
        })
        .collect();
    Plan { delete, insert }
}

impl<T: Kind> Plan<T> {
    fn is_empty(&self) -> bool {
        self.delete.is_empty() && self.insert.is_empty()
    }

    /// Delete before insert: a head whose payload is replaced by a lower digest is in both.
    async fn execute(self, tx: &impl Executor, tree_id: &[u8]) -> Result<usize, Error> {
        for chunk in self.delete.chunks(MAX_PARAMS - 1) {
            let mut sql = Sql::new(T::DELETE);
            sql.bind(tree_id.to_vec());
            sql.push("\n   AND ");
            sql.push(T::HEAD_COLUMN);
            sql.push(" IN ");
            sql.bind_list(chunk.iter().map(|head| head.as_bytes().to_vec()));
            sql.execute(tx).await?;
        }
        let inserted = self.insert.len();
        let mut rows = self.insert.into_iter();
        loop {
            let chunk: Vec<_> = rows.by_ref().take(MAX_PARAMS / ROW_WIDTH).collect();
            if chunk.is_empty() {
                return Ok(inserted);
            }
            let mut sql = Sql::new(T::INSERT);
            for (index, fresh) in chunk.into_iter().enumerate() {
                if index != 0 {
                    sql.push(", ");
                }
                sqlite_rs::BindTuple::bind_tuple(
                    (
                        tree_id.to_vec(),
                        fresh.held.head.as_bytes().to_vec(),
                        fresh.held.digest.to_vec(),
                        fresh.pcs_update_hash.map(|hash| hash.to_vec()),
                        fresh.signed,
                        fresh.blob,
                    ),
                    &mut sql,
                );
            }
            sql.execute(tx).await?;
        }
    }
}

impl<D: SqliteDb, M: DepthMetric> SedimentreeStore<D, M> {
    /// A batch with nothing in it yet.
    pub fn batch(&self) -> Batch {
        Batch::default()
    }

    /// Do `mutation` to the tree `id` as part of `batch`, through `tx`, a write transaction the
    /// caller owns and may share with other stores of the same database: the change joins that
    /// transaction's commit or rollback.
    ///
    /// The rows are in the tables, and the tree has the batch's `rev`, for whoever reads through
    /// `tx`, and for everyone else only once `tx` commits. After a commit, pass the batch to
    /// [`committed`](Self::committed); after a rollback, drop it.
    ///
    /// # Errors
    ///
    /// On an error the transaction may hold part of the mutation: roll it back.
    pub async fn apply_in(
        &self,
        tx: &impl Executor,
        batch: &mut Batch,
        id: SedimentreeId,
        mutation: Mutation,
    ) -> Result<Applied, Error> {
        let staged = self.stage(tx, batch.rev, id, mutation).await?;
        let applied = Applied {
            changed: staged.rev.is_some(),
            saved: staged.saved,
        };
        if staged.rev.is_some() {
            batch.rev = staged.rev;
            batch.staged.push(staged);
        }
        Ok(applied)
    }

    async fn stage(
        &self,
        tx: &impl Executor,
        rev: Option<i64>,
        id: SedimentreeId,
        mutation: Mutation,
    ) -> Result<Staged, Error> {
        let tree_id = id.as_bytes().to_vec();
        match mutation {
            Mutation::Register => {
                let moved = queries::register_tree(tx, tree_id, rev).await?;
                Ok(moved.map_or_else(|| Staged::unchanged(id), |rev| Staged::changed(id, rev)))
            }
            Mutation::Retire => {
                let Some(rev) = queries::retire_tree(tx, rev, tree_id.clone()).await? else {
                    return Ok(Staged::unchanged(id));
                };
                queries::delete_commits(tx, tree_id.clone()).await?;
                queries::delete_fragments(tx, tree_id).await?;
                Ok(Staged::changed(id, rev))
            }
            Mutation::Save { commits, fragments } => {
                self.save_in(tx, rev, id, tree_id, commits, fragments).await
            }
            Mutation::DeleteCommit(head) => {
                let deleted =
                    queries::delete_commit(tx, tree_id.clone(), head.as_bytes().to_vec()).await?;
                Self::touched(tx, rev, id, tree_id, deleted.rows_affected).await
            }
            Mutation::DeleteCommits => {
                let deleted = queries::delete_commits(tx, tree_id.clone()).await?;
                Self::touched(tx, rev, id, tree_id, deleted.rows_affected).await
            }
            Mutation::DeleteFragment(head) => {
                let deleted =
                    queries::delete_fragment(tx, tree_id.clone(), head.as_bytes().to_vec()).await?;
                Self::touched(tx, rev, id, tree_id, deleted.rows_affected).await
            }
            Mutation::DeleteFragments => {
                let deleted = queries::delete_fragments(tx, tree_id.clone()).await?;
                Self::touched(tx, rev, id, tree_id, deleted.rows_affected).await
            }
        }
    }

    /// A delete moves the `rev` if it deleted a row. The rows of a tree are a subset of its
    /// minimal form after it, so no minimizing follows.
    async fn touched(
        tx: &impl Executor,
        rev: Option<i64>,
        id: SedimentreeId,
        tree_id: Vec<u8>,
        deleted: u64,
    ) -> Result<Staged, Error> {
        if deleted == 0 || planted(Plant::SilentDelete) {
            return Ok(Staged::unchanged(id));
        }
        Ok(Staged::changed(
            id,
            queries::touch_tree(tx, tree_id, rev).await?,
        ))
    }

    async fn save_in(
        &self,
        tx: &impl Executor,
        rev: Option<i64>,
        id: SedimentreeId,
        tree_id: Vec<u8>,
        commits: Vec<Save<LooseCommit>>,
        fragments: Vec<Save<Fragment>>,
    ) -> Result<Staged, Error> {
        let live = queries::tree_state(tx, tree_id.clone())
            .await?
            .is_some_and(|state| state.live);
        // A tombstoned or new tree has no rows (a retire deleted them), so there is nothing to
        // read.
        let stored = if live {
            Stored::read(tx, &tree_id).await?
        } else {
            Stored::default()
        };
        let commits: Vec<_> = commits.into_iter().map(Fresh::of).collect();
        let fragments: Vec<_> = fragments.into_iter().map(Fresh::of).collect();

        // The tree as it would be with the new items, minimized: what survives is what the rows
        // should be. `Sedimentree` breaks a tie between payloads of one head by lowest digest.
        let mut tree = Sedimentree::new(
            stored
                .fragments
                .iter()
                .map(|held| &held.payload)
                .chain(fragments.iter().map(|fresh| &fresh.held.payload))
                .cloned()
                .collect(),
            stored
                .commits
                .iter()
                .map(|held| &held.payload)
                .chain(commits.iter().map(|fresh| &fresh.held.payload))
                .cloned()
                .collect(),
        );
        tree.minimize_in_place(&self.shared.depth);
        let kept_commits = tree
            .loose_commits()
            .map(|commit| (commit.head(), Digest::hash(commit).into_bytes()))
            .collect();
        let kept_fragments = tree
            .fragments()
            .map(|fragment| (fragment.head(), Digest::hash(fragment).into_bytes()))
            .collect();

        let commit_plan = plan(&stored.commits, commits, &kept_commits);
        let fragment_plan = plan(&stored.fragments, fragments, &kept_fragments);
        if live && commit_plan.is_empty() && fragment_plan.is_empty() {
            return Ok(Staged::unchanged(id));
        }
        let rev = queries::touch_tree(tx, tree_id.clone(), rev).await?;
        let saved =
            commit_plan.execute(tx, &tree_id).await? + fragment_plan.execute(tx, &tree_id).await?;
        Ok(Staged {
            tree_id: id,
            tree: Some(MinimizedSedimentree::already_minimal(tree)),
            saved,
            rev: Some(rev),
        })
    }

    /// The commit half of [`apply_in`](Self::apply_in): bring the cache up to the transaction's
    /// result and move the [`RevSource`](big_automata::rev::RevSource) watch to the batch's rev.
    /// It is only a cache and a wakeup: a store that is never told still answers correctly, by
    /// rebuilding from the tables on the next read, and a stream that is not woken finds the
    /// change when it next reads `latest_rev` (which is how another process learns).
    pub fn committed(&self, batch: Batch) {
        let Some(rev) = batch.rev else {
            return;
        };
        for staged in batch.staged {
            let rev = staged.rev.expect("a staged tree changed");
            match staged.tree {
                Some(tree) => self
                    .shared
                    .cache
                    .insert(staged.tree_id, rev, Arc::new(tree)),
                None => self.shared.cache.evict(staged.tree_id),
            }
        }
        self.shared
            .committed
            .send_modify(|latest| *latest = (*latest).max(rev_from_sql(rev)));
    }

    /// Do `mutation` in a transaction of its own and commit it. Returns the number of rows a
    /// save inserted.
    pub async fn apply(&self, id: SedimentreeId, mutation: Mutation) -> Result<usize, Error> {
        let tx = self.db.begin_write().await?;
        let mut batch = self.batch();
        // An error drops `tx`, which rolls it back.
        let applied = self.apply_in(&tx, &mut batch, id, mutation).await?;
        D::commit(tx).await?;
        self.committed(batch);
        Ok(applied.saved)
    }
}
