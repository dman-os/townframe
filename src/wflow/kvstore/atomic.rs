//! SQLite backend for the namespace contract in
//! `wflow_core::kvstore::atomic` (conditional batch commits, coherent
//! snapshot reads, monotonic revision hints).
//!
//! Every namespace is one row in `wflow_atomic_namespaces` carrying its
//! revision, plus its byte-keyed rows in `wflow_atomic_entries`. A commit is
//! `UPDATE ... SET revision = revision + 1 WHERE revision = expected`; when no
//! row matches, the request is a `Conflict` and none of its mutations are
//! written. Reads run every statement inside one pinned WAL read snapshot, so
//! a batch is never half visible. Range scans compare BLOBs, which SQLite does
//! with `memcmp` and then length, the same order as `BTreeMap<Arc<[u8]>, _>`.
//!
//! One committer task per factory owns all writes. It takes the first queued
//! request, drains whatever else is already queued (up to
//! `MAX_GROUP_REQUESTS`, never waiting for more), and stages the whole group
//! in one `BEGIN IMMEDIATE` transaction, in queue order. Revision hints and
//! replies are published only after `COMMIT` returns. A statement failure
//! rolls the whole group back; a group of several requests is then retried one
//! request per transaction, so one poisoned request cannot fail its siblings
//! forever and every sibling is still acknowledged only after its own commit.
//! A failed `COMMIT` reports `Unknown` to every request of that group.
//!
//! Durability: connections run WAL with `synchronous=NORMAL`. An acknowledged
//! commit survives a process crash; a power loss can roll back the most recent
//! acknowledged commits (never half of one).
//!
//! Ownership: a namespace admits one writer per factory, and an advisory
//! `flock` on `<database>.wflow-<namespace>.lock` excludes writers of other
//! factories and processes. Each queued request holds that ownership until the
//! committer has applied it, so dropping a writer (or a commit future) cannot
//! let a new writer in while stale requests are still pending. Lock files are
//! left in place; deleting them would race a concurrent locker. Revision hints
//! are process-local to the factory: readers in another factory or process see
//! committed data but receive no notification.

use crate::interlude::*;

use sqlite_rs::BindTuple;
use sqlite_rs::Database;
use sqlite_rs::ReadTx;
use sqlite_rs::ResultSet;
use sqlite_rs::Sql;
use sqlite_rs::SqlValue;
use sqlite_rs::Tx;
use std::ops::Bound;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use wflow_core::kvstore::atomic::CommitError;
use wflow_core::kvstore::atomic::Entry;
use wflow_core::kvstore::atomic::KeyRange;
use wflow_core::kvstore::atomic::Mutation;
use wflow_core::kvstore::atomic::Read;
use wflow_core::kvstore::atomic::Store;
use wflow_core::kvstore::atomic::Writer;

mod sqlite {
    #[expect(
        dead_code,
        reason = "scans call the checked SQL directly and move blobs out of the rows; the typed row wrappers would copy every payload twice"
    )]
    pub mod queries {
        include!("sqlite/gen.rs");
    }
}
use sqlite::queries;

const MAX_GROUP_REQUESTS: usize = 64;
const REQUEST_QUEUE: usize = 256;
/// Rows per multi-row statement; three bound values per upserted row stays
/// far below SQLite's bound-parameter limit.
const MAX_BOUND_ROWS: usize = 256;

pub struct SqliteFactory {
    db: Database,
    namespaces: StdMutex<HashMap<String, Arc<Namespace>>>,
    stats: Arc<Stats>,
    requests: mpsc::Sender<CommitRequest>,
}

/// Physical transactions committed by the group committer, and the requests
/// those transactions carried.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CommitStats {
    pub transactions: u64,
    pub requests: u64,
}

#[derive(Default)]
struct Stats {
    transactions: AtomicU64,
    requests: AtomicU64,
}

struct Namespace {
    name: String,
    revisions: watch::Sender<u64>,
    writer_live: AtomicBool,
    ownership: Arc<Semaphore>,
}

impl SqliteFactory {
    pub async fn boot(db: Database) -> Res<Self> {
        db.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        let (requests, receiver) = mpsc::channel(REQUEST_QUEUE);
        let stats = Arc::new(Stats::default());
        tokio::spawn(
            Committer {
                db: db.clone(),
                stats: Arc::clone(&stats),
                requests: receiver,
            }
            .run(),
        );
        Ok(Self {
            db,
            namespaces: default(),
            stats,
            requests,
        })
    }

    pub fn commit_stats(&self) -> CommitStats {
        CommitStats {
            transactions: self.stats.transactions.load(Ordering::Acquire),
            requests: self.stats.requests.load(Ordering::Acquire),
        }
    }

    pub fn store(&self, namespace: &str) -> SqliteStore {
        SqliteStore {
            db: self.db.clone(),
            namespace: self.namespace(namespace),
        }
    }

    pub async fn writer(&self, namespace: &str) -> Res<SqliteWriter> {
        let namespace = self.namespace(namespace);
        if namespace.writer_live.swap(true, Ordering::AcqRel) {
            eyre::bail!("namespace {} already has a writer", namespace.name);
        }
        let live = LiveGuard(Arc::clone(&namespace));
        // Requests queued by a dropped writer hold this permit until the
        // committer has applied them.
        let permit = Arc::clone(&namespace.ownership)
            .acquire_owned()
            .await
            .expect("namespace ownership semaphore is never closed");
        let mut lock_path = self.db.path().as_os_str().to_owned();
        lock_path.push(format!(".wflow-{}.lock", namespace.name));
        let lock = tokio::task::spawn_blocking(move || lock_owner(PathBuf::from(lock_path)))
            .await
            .expect(ERROR_TOKIO)?;
        let namespace_id = queries::ensure_namespace(&self.db, namespace.name.clone()).await?;
        Ok(SqliteWriter {
            store: SqliteStore {
                db: self.db.clone(),
                namespace,
            },
            namespace_id,
            ownership: Arc::new(Ownership {
                _lock: lock,
                _permit: permit,
            }),
            requests: self.requests.clone(),
            _live: live,
        })
    }

    fn namespace(&self, name: &str) -> Arc<Namespace> {
        assert!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
            "namespace names are [A-Za-z0-9_-]+ so they can name lock files: {name:?}"
        );
        let mut namespaces = self.namespaces.lock().expect(ERROR_MUTEX);
        Arc::clone(namespaces.entry(name.to_owned()).or_insert_with(|| {
            Arc::new(Namespace {
                name: name.to_owned(),
                revisions: watch::channel(0).0,
                writer_live: AtomicBool::new(false),
                ownership: Arc::new(Semaphore::new(1)),
            })
        }))
    }
}

fn lock_owner(path: PathBuf) -> Res<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .wrap_err_with(|| format!("failed opening writer lock {path:?}"))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(ferr!(
            "writer lock {path:?} is held by another factory or process"
        )),
        Err(std::fs::TryLockError::Error(error)) => {
            Err(error).wrap_err_with(|| format!("failed locking {path:?}"))
        }
    }
}

/// Clears the in-process writer flag when the writer, or a cancelled
/// `writer()` call, goes away.
struct LiveGuard(Arc<Namespace>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        assert!(self.0.writer_live.swap(false, Ordering::AcqRel));
    }
}

/// Field order matters: the file lock is released before the permit, so a
/// waiting in-process writer never finds its own factory's lock still held.
struct Ownership {
    _lock: std::fs::File,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct SqliteStore {
    db: Database,
    namespace: Arc<Namespace>,
}

#[async_trait]
impl Store for SqliteStore {
    async fn read(&self, keys: &[Arc<[u8]>], range: KeyRange, limit: usize) -> Res<Read> {
        assert_ordered(&range);
        let snapshot = self.db.begin_read().await?;
        let state = queries::namespace_state(&snapshot, self.namespace.name.clone()).await?;
        let read = match state {
            None => Read {
                revision: 0,
                values: vec![None; keys.len()],
                entries: Vec::new(),
            },
            Some(state) => Read {
                revision: revision_from(state.revision),
                values: lookup(&snapshot, state.namespace_id, keys).await?,
                entries: scan(&snapshot, state.namespace_id, range, limit).await?,
            },
        };
        snapshot.rollback().await?;
        Ok(read)
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.namespace.revisions.subscribe()
    }
}

/// The same invariant `BTreeMap::range` enforces for the memory backend.
fn assert_ordered(range: &KeyRange) {
    if let (
        Bound::Included(start) | Bound::Excluded(start),
        Bound::Included(end) | Bound::Excluded(end),
    ) = range
    {
        assert!(start <= end, "range start is greater than range end");
        assert!(
            start != end || !matches!(range, (Bound::Excluded(_), Bound::Excluded(_))),
            "range start and end are equal and excluded"
        );
    }
}

async fn lookup(
    snapshot: &ReadTx,
    namespace_id: i64,
    keys: &[Arc<[u8]>],
) -> Res<Vec<Option<Arc<[u8]>>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut found = HashMap::with_capacity(keys.len());
    for chunk in keys.chunks(MAX_BOUND_ROWS) {
        let mut sql = Sql::new("SELECT key, value FROM wflow_atomic_entries WHERE namespace_id = ");
        sql.bind(namespace_id)
            .push(" AND key IN ")
            .bind_list(chunk.iter().map(AsRef::<[u8]>::as_ref));
        found.extend(
            decode_entries(sql.query(snapshot).await?).map(|entry| (entry.key, entry.value)),
        );
    }
    Ok(keys
        .iter()
        .map(|key| found.get(key.as_ref()).cloned())
        .collect())
}

/// `key > k` is `key >= k || 0x00` and `key <= k` is `key < k || 0x00`: no
/// byte string sorts strictly between `k` and `k || 0x00`.
async fn scan(
    snapshot: &ReadTx,
    namespace_id: i64,
    range: KeyRange,
    limit: usize,
) -> Res<Vec<Entry>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let lower = match range.0 {
        Bound::Unbounded => Vec::new(),
        Bound::Included(key) => key.to_vec(),
        Bound::Excluded(key) => successor(&key),
    };
    let upper = match range.1 {
        Bound::Unbounded => None,
        Bound::Included(key) => Some(successor(&key)),
        Bound::Excluded(key) => Some(key.to_vec()),
    };
    let rows = match upper {
        None => {
            snapshot
                .query(queries::SCAN_FROM_SQL, (namespace_id, lower, limit))
                .await?
        }
        Some(upper) => {
            snapshot
                .query(
                    queries::SCAN_BOUNDED_SQL,
                    (namespace_id, lower, upper, limit),
                )
                .await?
        }
    };
    Ok(decode_entries(rows).collect())
}

fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

/// Moves the blobs out of `(key, value)` rows instead of cloning them.
fn decode_entries(rows: ResultSet) -> impl Iterator<Item = Entry> {
    rows.rows.into_iter().map(|row| {
        let mut values = row.into_values().into_iter();
        let (Some(SqlValue::Blob(key)), Some(SqlValue::Blob(value)), None) =
            (values.next(), values.next(), values.next())
        else {
            panic!("STRICT wflow_atomic_entries rows are (key BLOB, value BLOB)");
        };
        Entry {
            key: key.into(),
            value: value.into(),
        }
    })
}

fn revision_from(revision: i64) -> u64 {
    u64::try_from(revision).expect("revision column is CHECKed nonnegative")
}

pub struct SqliteWriter {
    store: SqliteStore,
    namespace_id: i64,
    ownership: Arc<Ownership>,
    requests: mpsc::Sender<CommitRequest>,
    _live: LiveGuard,
}

#[async_trait]
impl Writer for SqliteWriter {
    fn reader(&self) -> Arc<dyn Store> {
        Arc::new(self.store.clone())
    }

    async fn commit(
        &mut self,
        expected_revision: u64,
        mutations: Vec<Mutation>,
    ) -> Result<u64, CommitError> {
        let (reply, outcome) = oneshot::channel();
        self.requests
            .send(CommitRequest {
                namespace: Arc::clone(&self.store.namespace),
                namespace_id: self.namespace_id,
                expected_revision,
                mutations,
                reply,
                _ownership: Arc::clone(&self.ownership),
            })
            .await
            .expect(ERROR_ACTOR);
        outcome.await.expect(ERROR_ACTOR)
    }
}

struct CommitRequest {
    namespace: Arc<Namespace>,
    namespace_id: i64,
    expected_revision: u64,
    mutations: Vec<Mutation>,
    reply: oneshot::Sender<Result<u64, CommitError>>,
    _ownership: Arc<Ownership>,
}

/// `Ok(revision)` for an applied request, `Err(actual_revision)` for a
/// revision conflict that wrote nothing.
type Staged = Result<u64, u64>;

enum GroupError {
    /// `BEGIN IMMEDIATE` failed; nothing was staged.
    Begin(eyre::Report),
    /// A staging statement failed and the transaction was rolled back.
    Statement(eyre::Report),
    /// `COMMIT` failed; the outcome is not known.
    Commit(eyre::Report),
}

struct Committer {
    db: Database,
    stats: Arc<Stats>,
    requests: mpsc::Receiver<CommitRequest>,
}

impl Committer {
    async fn run(mut self) {
        while let Some(first) = self.requests.recv().await {
            let mut group = vec![first];
            while group.len() < MAX_GROUP_REQUESTS
                && let Ok(next) = self.requests.try_recv()
            {
                group.push(next);
            }
            self.commit_group(group).await;
        }
    }

    async fn commit_group(&self, group: Vec<CommitRequest>) {
        match self.apply(&group).await {
            Ok(staged) => publish(group, staged),
            Err(GroupError::Statement(_)) if group.len() > 1 => {
                for request in group {
                    let single = vec![request];
                    match self.apply(&single).await {
                        Ok(staged) => publish(single, staged),
                        Err(error) => fail(single, &error),
                    }
                }
            }
            Err(error) => fail(group, &error),
        }
    }

    async fn apply(&self, group: &[CommitRequest]) -> Result<Vec<Staged>, GroupError> {
        let transaction = self
            .db
            .begin_write()
            .await
            .map_err(|error| GroupError::Begin(error.into()))?;
        let mut staged = Vec::with_capacity(group.len());
        for request in group {
            match stage(&transaction, request).await {
                Ok(outcome) => staged.push(outcome),
                Err(error) => {
                    // A failed ROLLBACK discards the connection, and closing a
                    // connection rolls its open transaction back, so the
                    // group is uncommitted either way.
                    return Err(GroupError::Statement(match transaction.rollback().await {
                        Ok(()) => error,
                        Err(rollback) => error.wrap_err(format!("rollback failed: {rollback}")),
                    }));
                }
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| GroupError::Commit(error.into()))?;
        self.stats.transactions.fetch_add(1, Ordering::AcqRel);
        self.stats
            .requests
            .fetch_add(group.len() as u64, Ordering::AcqRel);
        Ok(staged)
    }
}

async fn stage(transaction: &Tx, request: &CommitRequest) -> Res<Staged> {
    // A revision beyond i64 was never issued, so it cannot match.
    let advanced = match i64::try_from(request.expected_revision) {
        Ok(expected) => {
            queries::advance_revision(transaction, request.namespace_id, expected).await?
        }
        Err(_) => None,
    };
    let Some(revision) = advanced else {
        let actual = queries::namespace_revision(transaction, request.namespace_id).await?;
        return Ok(Err(revision_from(actual)));
    };
    // Mutations apply in vector order, so only the last one per key survives.
    let mut last = BTreeMap::new();
    for mutation in &request.mutations {
        last.insert(mutation.key.as_ref(), mutation.value.as_deref());
    }
    let (puts, deletes): (Vec<_>, Vec<_>) =
        last.into_iter().partition(|(_, value)| value.is_some());
    for chunk in puts.chunks(MAX_BOUND_ROWS) {
        let mut sql =
            Sql::new("INSERT INTO wflow_atomic_entries (namespace_id, key, value) VALUES ");
        for (index, (key, value)) in chunk.iter().enumerate() {
            if index != 0 {
                sql.push(", ");
            }
            (
                request.namespace_id,
                *key,
                value.expect("partitioned puts carry values"),
            )
                .bind_tuple(&mut sql);
        }
        sql.push(" ON CONFLICT (namespace_id, key) DO UPDATE SET value = excluded.value");
        sql.execute(transaction).await?;
    }
    for chunk in deletes.chunks(MAX_BOUND_ROWS) {
        let mut sql = Sql::new("DELETE FROM wflow_atomic_entries WHERE namespace_id = ");
        sql.bind(request.namespace_id)
            .push(" AND key IN ")
            .bind_list(chunk.iter().map(|(key, _)| *key));
        sql.execute(transaction).await?;
    }
    Ok(Ok(revision_from(revision)))
}

fn publish(group: Vec<CommitRequest>, staged: Vec<Staged>) {
    for (request, outcome) in group.into_iter().zip(staged) {
        let result = match outcome {
            Ok(revision) => {
                request.namespace.revisions.send_replace(revision);
                Ok(revision)
            }
            Err(actual_revision) => Err(CommitError::Conflict { actual_revision }),
        };
        reply(request, result);
    }
}

fn fail(group: Vec<CommitRequest>, error: &GroupError) {
    for request in group {
        let result = match error {
            GroupError::Begin(error) | GroupError::Statement(error) => {
                CommitError::NotCommitted(ferr!("sqlite commit rolled back: {error:#}"))
            }
            GroupError::Commit(error) => {
                CommitError::Unknown(ferr!("sqlite COMMIT failed: {error:#}"))
            }
        };
        reply(request, Err(result));
    }
}

fn reply(request: CommitRequest, result: Result<u64, CommitError>) {
    // The caller may have dropped its commit future after queueing. The
    // outcome above is already settled, the request's ownership is released
    // only now, and a reopened writer reconciles from storage.
    let _cancelled = request.reply.send(result);
}

#[cfg(test)]
#[path = "atomic_tests.rs"]
mod tests;
