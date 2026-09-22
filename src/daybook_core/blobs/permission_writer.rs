//! Blob-inventory permission writer: the consumer half of ADR 013.
//!
//! The derived blob-inventory partition part of an inventory document must
//! carry, as its access rows, exactly the readers of that document (ADR 013
//! decision A1/A2). The Keyhive access delta stream
//! (`big_repo::KeyhiveAccessRevisionStore`) reports *which subject changed*;
//! this machine maps that subject to the partition part it controls, writes the
//! subject's own closure with a full-replacement `set_part_members`, and acks
//! the entry only after that write is durable.
//!
//! Selection is [`AccessSubjectSet::All`] (ADR 013 §2 and open question 7): the
//! admission log has no per-subject key space to select on, so the reader emits
//! one entry per subject-bearing admitted event and this machine's subject →
//! part map decides which of them it owns. A subject that controls no derived
//! part here is a no-op that is still acked — it is neither a failure nor a
//! reason to stop the walker.
//!
//! Acking is memory-less, which ADR 013 open question 6 permits on the `All`
//! path only: the affected test is vacuous there, and the sink already holds
//! the last full replacement, so the walker's own cursor settlement is the
//! whole durable state. `set_part_members` is a full replacement and therefore
//! idempotent, which is also why a replayed entry needs no sparse memory row.
//!
//! Whoever serves the derived parts owns this machine: `IrohSyncRepo::boot` spawns
//! it with the repository's two inventory documents, beside the part stores that
//! serve them, and the boot seed runs inside the spawn before the reader opens,
//! per ADR 013 §8. An `Rt` does not: the clone path and any headless sync boot the
//! serving boundary without one, and a part whose rows were never written refuses
//! every peer — which reads to that peer as an unknown part, blocking its full
//! sync forever.

use crate::interlude::*;

use crate::blobs::blob_inventory_part_id_from_doc_id;
use crate::local_state::SqliteLocalStateRepo;
use crate::repos::RepoStopToken;
use big_repo::keyhive_core::access::Access;
use big_repo::keyhive_core::principal::identifier::Identifier;
use big_repo::{
    AccessSubject, AccessSubjectSet, BigKeyhiveHandle, DocumentId, KeyhiveAccessDelta,
    KeyhiveAccessRevisionStore, KeyhiveAccessSelector, SharedBigRepo, SharedPartStore,
};
use big_sync::DeltaWalkerStateRepo as _;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::{
    ConcurrentDelta, ConcurrentDeltaRead, ConcurrentDeltaWalker, DeltaAck,
};
use big_sync_core::delta_walker_state::DeltaWalkerStateTransaction as _;
use big_sync_core::revisioned_store::RevisionedStore as _;
use big_sync_core::tokio_keyed_scheduler::{TokioKeyedScheduler, TokioTaskCompletion};
use tokio_util::sync::CancellationToken;

/// Walker-state identity of the permission machine.
///
/// One namespace owns the machine's cursor and (with it) the retention reader
/// id, so the machine cannot acquire two retention rows and nothing else can
/// share this one (ADR 013 §9, open question 2).
pub(crate) const BLOB_INVENTORY_PERMISSION_STATE_ID: &str =
    "@daybook/core/blob-inventory-permissions";

/// The machine's keyed execution budget.
///
/// Keys are watched subjects, never admission `seq`, so this bounds how many
/// subjects can be mid-write rather than how much of the log is in flight: with
/// the two inventory documents of today there is at most one task per document
/// (ADR 013 open question 5 asks for a budget smaller than the pin worker's 64).
const PERMISSION_TASK_BUDGET: usize = 8;

/// Spawn the blob-inventory permission writer.
///
/// `inventory_documents` is the watch set — today the repository's two
/// inventory documents. Each one's derived partition part
/// ([`blob_inventory_part_id_from_doc_id`]) gets that document's own closure as
/// its access rows.
/// **Seeds first, unconditionally** (ADR 013 §8): before the machine opens its
/// reader, every derived part is written from the current Keyhive closure, so a
/// fresh store is correct immediately rather than only after the first event.
/// Seeding is idempotent (`set_part_members` is a full replacement), so a boot
/// that finds the rows already correct rewrites the same rows.
pub(crate) async fn spawn_blob_inventory_permission_writer(
    part_store: SharedPartStore,
    sqlite_local_state_repo: Arc<SqliteLocalStateRepo>,
    big_repo: SharedBigRepo,
    inventory_documents: Vec<DocumentId>,
    parent_cancel_token: CancellationToken,
) -> Res<RepoStopToken> {
    let parts = inventory_parts(&inventory_documents)?;
    // ADR 013 §8, first step: seed before the reader can open.
    seed_inventory_parts(&part_store, big_repo.keyhive(), &parts).await?;
    let sql = sqlite_local_state_repo
        .ensure_sqlite_ctx(BLOB_INVENTORY_PERMISSION_STATE_ID)
        .await?;
    let state = SqliteDeltaWalkerStateRepo::new(
        sql.read_pool.clone(),
        sql.write_pool.clone(),
        BLOB_INVENTORY_PERMISSION_STATE_ID,
        "keyhive-access",
    )
    .await?;
    let stream = KeyhiveAccessRevisionStore::new(&big_repo);

    let cancel_token = parent_cancel_token.child_token();
    let worker_cancel_token = cancel_token.clone();
    // One supervisor owns the machine: a panic takes the task down per the
    // task-panic-handler convention, so the future ends with `unwrap` rather
    // than a swallowed error.
    let worker_handle = tokio::spawn(async move {
        run_permission_machine(part_store, stream, state, parts, worker_cancel_token)
            .await
            .unwrap();
    });
    Ok(RepoStopToken {
        cancel_token,
        worker_handle: Some(worker_handle),
    })
}

/// The partition part each watched inventory document controls.
///
/// The subject is the document's Keyhive identity — the same id the stream
/// tags a `Document` subject with — and the part is the derived partition the
/// sync path and the boot seed ask about.
fn inventory_parts(documents: &[DocumentId]) -> Res<BTreeMap<AccessSubject, PartKey>> {
    documents
        .iter()
        .map(|doc_id| {
            let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32()?)
                .map_err(|_| ferr!("inventory document id is not an Ed25519 point: {doc_id}"))?;
            Ok((
                AccessSubject::Document(Identifier::from(verifying_key)),
                blob_inventory_part_id_from_doc_id(&doc_id.to_string()),
            ))
        })
        .collect()
}

/// Write every inventory document's current closure into its derived part.
///
/// The stream only reports subjects that *changed*, so a store that has never been
/// populated — a fresh install, or one whose wake-ups were pruned (ADR 013 §9) —
/// would learn nothing until the next event on that document. Seeding is therefore
/// unconditional and runs before the reader opens (§8), and it is idempotent because
/// `set_part_members` is a full replacement.
async fn seed_inventory_parts(
    part_store: &SharedPartStore,
    keyhive: &BigKeyhiveHandle,
    parts: &BTreeMap<AccessSubject, PartKey>,
) -> Res<()> {
    for (subject, part) in parts {
        // `inventory_parts` only ever names documents; a group subject would not
        // control a derived inventory partition part.
        let AccessSubject::Document(document_id) = subject else {
            continue;
        };
        let agents = keyhive.agents_for_membered(*document_id).await;
        part_store
            .set_part_members(part.clone(), peer_access_map(&agents))
            .await?;
        tracing::debug!(
            ?subject,
            ?part,
            members = agents.len(),
            "seeded a blob-inventory partition part from Keyhive"
        );
    }
    Ok(())
}

/// The type hop the consumer owns (ADR 013 §5): the closure is keyed by Keyhive
/// [`Identifier`] and the sink by [`PeerKey`].
fn peer_access_map(agents: &BTreeMap<Identifier, Access>) -> HashMap<PeerKey, Access> {
    agents
        .iter()
        .map(|(identifier, access)| (PeerKey::new(identifier.to_bytes()), *access))
        .collect()
}

/// One merged command for a watched subject: the newest closure read for it,
/// with the source cursor the write must cover.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PermissionTask {
    key: AccessSubject,
    cursor: u64,
    part: PartKey,
    /// The subject's resulting closure, verbatim, including `Relay` (ADR 013
    /// §4: relays retain blob partitions without reading private documents).
    agents: BTreeMap<Identifier, Access>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionTaskOutput {
    Written,
}

async fn run_permission_machine(
    part_store: SharedPartStore,
    stream: KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo>,
    state: SqliteDeltaWalkerStateRepo,
    parts: BTreeMap<AccessSubject, PartKey>,
    cancel_token: CancellationToken,
) -> Res<()> {
    let durable = state.progress().await?.upstream_revision;
    // ADR 013 §9: a consumer whose cursor sits below the archive floor can never
    // be woken for the gap — those wake-ups are tombstoned and pruned — so it
    // must not open a reader below the floor. The boot seed has already written
    // every part from live Keyhive state, which is what makes the gap skippable:
    // the rows are a function of current state, not of the events that produced
    // it. Advancing the walker's own progress to the floor is also what keeps
    // its documented contract (`ConcurrentDeltaWalker::open`: the caller's
    // `after` and `state.progress()` agree) and keeps the first readable entry
    // contiguous with the walker's durable prefix.
    let archived = stream.archived_through().await?;
    let resume = durable.max(archived);
    if resume > durable {
        let skipped = archived - durable;
        warn!(
            durable,
            archived,
            skipped,
            "keyhive access wake-ups were pruned below this consumer's cursor; \
             parts were seeded from live Keyhive state and the walker resumes at \
             the archive floor"
        );
        let mut tx = state.begin().await?;
        tx.advance_from(durable, resume).await?;
        tx.commit().await?;
    }
    // ADR 013 §9: the retention cursor is the correctness mechanism that keeps
    // this consumer's wake-ups from being pruned, so it is registered at the
    // durable progress *before* the reader opens (the order the group-part
    // worker registers its own reader in). It is monotone, so it may lag the
    // walker and must never lead it — which the advance above preserves, since
    // it moves the walker to the floor first and registers the floor after.
    stream.note_retention(&state, resume).await?;
    let reader = stream
        .open(
            KeyhiveAccessSelector {
                watch: AccessSubjectSet::All,
                memory: state.clone(),
            },
            resume,
        )
        .await?;
    let mut walker =
        ConcurrentDeltaWalker::open(reader, state.clone(), |entry: &KeyhiveAccessDelta| {
            entry.subject
        })
        .await?;
    let mut tasks = TokioKeyedScheduler::new(PERMISSION_TASK_BUDGET);
    // The newest unacked delta per watched subject.
    let mut pending: HashMap<AccessSubject, PermissionTask> = HashMap::new();
    loop {
        let available = PERMISSION_TASK_BUDGET.saturating_sub(tasks.active_count());
        let next_deadline = tasks.next_deadline();
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Ok(()),
            completion = tasks.next_completion() => {
                on_task_completion(
                    &stream,
                    &state,
                    &mut walker,
                    &mut pending,
                    completion?,
                )
                .await?;
            }
            read = async {
                if available == 0 {
                    std::future::pending().await
                } else {
                    walker
                        .next(
                            std::num::NonZeroUsize::new(available)
                                .expect("available is non-zero"),
                        )
                        .await
                }
            } => {
                match read? {
                    ConcurrentDeltaRead::ReplayComplete { .. } => {}
                    ConcurrentDeltaRead::Entries { entries, .. } => {
                        for delta in entries {
                            on_delta(
                                &part_store,
                                &mut walker,
                                &parts,
                                &mut tasks,
                                &mut pending,
                                delta,
                            )
                            .await?;
                        }
                    }
                }
            }
            _ = async {
                if let Some(deadline) = next_deadline {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                tasks.tick(std::time::Instant::now())?;
            }
        }
    }
}

/// Merge one arrival into the key's pending command, or settle it as a no-op.
///
/// A subject this machine owns no part for owes nothing, so it is acked right
/// here: no task is started and no write happens. Acking is what keeps the
/// walker's contiguous prefix moving past the (vast majority of) events that
/// name other documents.
async fn on_delta(
    part_store: &SharedPartStore,
    walker: &mut ConcurrentDeltaWalker<
        '_,
        KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo>,
        SqliteDeltaWalkerStateRepo,
        AccessSubject,
    >,
    parts: &BTreeMap<AccessSubject, PartKey>,
    tasks: &mut TokioKeyedScheduler<AccessSubject, PermissionTask, PermissionTaskOutput>,
    pending: &mut HashMap<AccessSubject, PermissionTask>,
    delta: ConcurrentDelta<AccessSubject, KeyhiveAccessDelta>,
) -> Res<()> {
    let Some(part) = parts.get(&delta.entry.subject) else {
        walker.ack(delta.key, delta.cursor).await?;
        return Ok(());
    };
    // Take the pending command out of the map and merge in place: no per-arrival
    // clone of the accumulated work. A stale-cursor arrival for a subject whose
    // command already covers a newer revision is the only drop — the closure is
    // read-time state, so the newest read is the whole truth and the older
    // cursor is superseded by the ack of the newer one.
    let task = match pending.remove(&delta.key) {
        Some(existing) if existing.cursor > delta.cursor => {
            pending.insert(existing.key, existing);
            return Ok(());
        }
        Some(mut existing) => {
            existing.agents = delta.entry.agents;
            existing.cursor = existing.cursor.max(delta.cursor);
            existing
        }
        None => PermissionTask {
            key: delta.key,
            cursor: delta.cursor,
            part: part.clone(),
            agents: delta.entry.agents,
        },
    };
    pending.insert(task.key, task.clone());
    let future = run_permission_task(Arc::clone(part_store), task.clone());
    tasks.replace(task.key, task, future)?;
    Ok(())
}

/// Replace one part's access rows with the subject's closure.
///
/// The type hop belongs to the consumer and never to the stream (ADR 013 §5):
/// the closure is keyed by Keyhive [`Identifier`] and the sink takes
/// [`PeerKey`]. `set_part_members` creates the part if it does not exist yet
/// and rewrites its rows in one transaction, so this is a full replacement and
/// idempotent for a replayed entry.
async fn run_permission_task(
    part_store: SharedPartStore,
    task: PermissionTask,
) -> Res<PermissionTaskOutput> {
    let agents = peer_access_map(&task.agents);
    part_store
        .set_part_members(task.part.clone(), agents)
        .await?;
    Ok(PermissionTaskOutput::Written)
}

/// Ack a task whose write is already durable, and advance retention over it.
///
/// The completion is the only place a cursor moves, and it moves only for a
/// task that reported `Written`: a store error leaves the entry unacked, the
/// walker re-drives it from the durable cursor, and the error surfaces here
/// rather than being swallowed.
async fn on_task_completion(
    stream: &KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo>,
    state: &SqliteDeltaWalkerStateRepo,
    walker: &mut ConcurrentDeltaWalker<
        '_,
        KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo>,
        SqliteDeltaWalkerStateRepo,
        AccessSubject,
    >,
    pending: &mut HashMap<AccessSubject, PermissionTask>,
    completion: TokioTaskCompletion<PermissionTask, PermissionTaskOutput>,
) -> Res<()> {
    let task = completion.command;
    match completion.result {
        Ok(PermissionTaskOutput::Written) => {
            let ack = walker.ack(task.key, task.cursor).await?;
            if let DeltaAck::Accepted {
                through: Some(through),
            } = ack
            {
                stream.note_retention(state, through).await?;
            }
            if pending
                .get(&task.key)
                .is_some_and(|existing| existing.cursor == task.cursor)
            {
                pending.remove(&task.key);
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::boot_repo;
    use automerge::transaction::Transactable as _;
    use big_sync::{ReadTarget, SqlitePartStore};
    use big_sync_core::BuckId;
    use sqlx::Row as _;
    use std::time::Duration;

    // The harness observes the machine through the machine's own state identity: progress
    // and the sparse memory rows are keyed by `(namespace, consumer_id)`, and a test-local
    // namespace would read a different sqlite file (`ensure_sqlite_ctx` opens one per id),
    // so `durable_revision` would report 0 no matter what the machine settled.

    /// A live BigRepo and a private blob part store, with no runtime attached
    /// beyond the repository's own workers.
    ///
    /// The store is private to the test rather than the repository's, so a test
    /// can fault-inject into it without touching the repository's own scope.
    struct Harness {
        _temp: tempfile::TempDir,
        repo: SharedBigRepo,
        part_store: SharedPartStore,
        store_sql: SqlCtx,
        /// The scope `store_sql`'s sink rows belong to. `SharedPartStore` is a
        /// scope-erased trait object, and the rows `part_members_in` reads are keyed by
        /// `(scope, part_id)`, so the scope is resolved here and carried alongside.
        store_scope_id: i64,
        local_state: Arc<SqliteLocalStateRepo>,
        state: SqliteDeltaWalkerStateRepo,
        teardown: Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
    }

    impl Harness {
        async fn new() -> Res<Self> {
            tokio::task::block_in_place(|| {
                utils_rs::testing::load_envs_once();
                utils_rs::testing::setup_tracing_once();
            });
            let temp = tempfile::tempdir()?;
            let (repo, _big_sync, teardown) = boot_repo().await?;
            let store_sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
            let store_scope: Arc<str> = Arc::from("daybook-blobs-test");
            let part_store: SharedPartStore = Arc::new(
                SqlitePartStore::new(
                    store_sql.clone(),
                    Arc::clone(&store_scope),
                    BuckId::MAX_LEVEL,
                )
                .await?,
            );
            // The store above ensured the scope; resolve its id rather than reading it
            // back through the trait object, which has no scope.
            let store_scope_id = big_sync::sqlite_core::SqliteCore::ensure_scope_id(
                &store_sql.write_pool,
                &store_scope,
            )
            .await?;
            let (local_state, local_state_stop) =
                SqliteLocalStateRepo::boot(temp.path().join("local_state")).await?;
            let sql = local_state
                .ensure_sqlite_ctx(BLOB_INVENTORY_PERMISSION_STATE_ID)
                .await?;
            let state = SqliteDeltaWalkerStateRepo::new(
                sql.read_pool.clone(),
                sql.write_pool.clone(),
                BLOB_INVENTORY_PERMISSION_STATE_ID,
                "keyhive-access",
            )
            .await?;
            let teardown_future = Box::new(move || {
                // Resolved before the async block so the block captures only `Send`
                // values: the boxed teardown is not `Send` and crossing an await with it
                // would stop the whole harness future from being `Send`.
                let repo_teardown = teardown();
                Box::pin(async move {
                    local_state_stop.stop().await?;
                    repo_teardown.await
                }) as futures::future::BoxFuture<'static, Res<()>>
            });
            Ok(Self {
                _temp: temp,
                repo,
                part_store,
                store_sql,
                store_scope_id,
                local_state,
                state,
                teardown: teardown_future,
            })
        }

        /// Create a document with content, so it is a Keyhive document with a
        /// closure and its events are admitted.
        async fn create_doc(&self) -> Res<DocumentId> {
            let mut content = automerge::Automerge::new();
            content
                .transact(|tx| {
                    tx.put(automerge::ROOT, "title", "inventory")?;
                    Ok::<_, automerge::AutomergeError>(())
                })
                .map_err(|err| ferr!("building document content failed: {err:?}"))?;
            let handle = self.repo.create_doc(content).await?;
            Ok(handle.document_id())
        }

        async fn spawn(&self, documents: Vec<DocumentId>) -> Res<RepoStopToken> {
            spawn_blob_inventory_permission_writer(
                Arc::clone(&self.part_store),
                Arc::clone(&self.local_state),
                Arc::clone(&self.repo),
                documents,
                CancellationToken::new(),
            )
            .await
        }

        async fn stop(self) -> Res<()> {
            let Harness {
                _temp: _,
                repo: _,
                part_store: _,
                store_sql: _,
                store_scope_id: _,
                local_state: _,
                state: _,
                teardown,
            } = self;
            teardown().await
        }

        /// The access rows of one part, exactly as the sink stores them.
        async fn part_members(&self, part: &PartKey) -> Res<BTreeMap<PeerKey, Access>> {
            part_members_in(&self.store_sql, self.store_scope_id, part).await
        }

        /// The machine's expectation for one watched document: that document's
        /// own closure, converted to the sink's key space.
        async fn expected(&self, doc: &DocumentId) -> Res<BTreeMap<PeerKey, Access>> {
            expected_members(self.repo.keyhive(), doc).await
        }

        /// Wait for a part's rows to reach `expected`, failing with the machine's
        /// own state rather than hanging if they never do.
        async fn wait_for_members(
            &self,
            part: &PartKey,
            expected: BTreeMap<PeerKey, Access>,
        ) -> Res<()> {
            if self.poll_members(part, &expected).await? {
                return Ok(());
            }
            Err(ferr!(
                "part {part} never reached its expected members; the walker is at revision {}",
                self.durable_revision().await
            ))
        }

        async fn poll_members(
            &self,
            part: &PartKey,
            expected: &BTreeMap<PeerKey, Access>,
        ) -> Res<bool> {
            Ok(tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if &self.part_members(part).await? == expected {
                        return Ok::<bool, eyre::Report>(true);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .is_ok())
        }

        /// The walker's durable cursor: what the machine has acked.
        async fn durable_revision(&self) -> u64 {
            self.state
                .progress()
                .await
                .expect("walker progress is readable")
                .upstream_revision
        }

        async fn wait_for_revision(&self, revision: u64) -> Res<()> {
            let settled = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if self.durable_revision().await >= revision {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await;
            if settled.is_err() {
                return Err(ferr!(
                    "the walker never settled revision {revision}; it is stuck at {} (head now {})",
                    self.durable_revision().await,
                    KeyhiveAccessRevisionStore::<SqliteDeltaWalkerStateRepo>::new(&self.repo)
                        .latest_revision()
                        .await
                        .unwrap_or(u64::MAX)
                ));
            }
            Ok(())
        }
    }

    /// Model maintenance having pruned this scope's admitted log: the bytes the
    /// admission reader joins against are gone and the archive floor is recorded
    /// at the old head, exactly as `prune_admitted_events` leaves the store once a
    /// reader floor lets it delete. The permission machine is the scope's only
    /// consumer here and it has never run, so nothing pins those rows against
    /// pruning.
    ///
    /// Returns the recorded floor.
    async fn prune_admitted_log(repo: &SharedBigRepo) -> Res<u64> {
        let sql = repo.sql_ctx();
        let head: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM big_repo_keyhive_admissions")
                .fetch_one(&sql.read_pool)
                .await?;
        assert!(head > 0, "the fixture needs admitted events to prune");
        sqlx::query("DELETE FROM big_repo_keyhive_event_log")
            .execute(&sql.write_pool)
            .await?;
        // The scope id is bound from the admissions table, so the fixture cannot
        // drift from the scope the store reads its floor out of.
        sqlx::query(
            "INSERT INTO big_repo_keyhive_archived_through(scope_id, seq) \
                 SELECT scope_id \
                      , MAX(seq) \
                   FROM big_repo_keyhive_admissions \
                  GROUP BY scope_id \
             ON CONFLICT(scope_id) DO UPDATE \
                     SET seq = MAX(seq, excluded.seq)",
        )
        .execute(&sql.write_pool)
        .await?;
        Ok(head as u64)
    }

    fn doc_identifier(doc_id: &DocumentId) -> Res<Identifier> {
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32()?)
            .map_err(|_| ferr!("document id is not an Ed25519 point: {doc_id}"))?;
        Ok(Identifier::from(verifying_key))
    }

    /// A principal that is a member of nothing, for the refusal control.
    fn stranger() -> PeerKey {
        PeerKey::new([0xEE; 32])
    }

    /// One document's own closure, in the sink's key space: the type hop the boot
    /// seed and the machine both make (ADR 013 §5).
    async fn expected_members(
        keyhive: &BigKeyhiveHandle,
        doc: &DocumentId,
    ) -> Res<BTreeMap<PeerKey, Access>> {
        Ok(keyhive
            .agents_for_membered(doc_identifier(doc)?)
            .await
            .into_iter()
            .map(|(identifier, access)| (PeerKey::new(identifier.to_bytes()), access))
            .collect())
    }

    /// The access rows of one part, exactly as the sink stores them in `sql`.
    ///
    /// A part's identity is the `(scope, part_id)` pair: `part_id` is unique only
    /// within a scope (`UNIQUE(scope_id, part_id)`), so a read that names only the
    /// part unions the rows of every scope in the database that holds the same part
    /// name. The scope id is the caller's to resolve — the ctx carries none.
    ///
    /// Parameterised by the sqlite ctx rather than the harness's private store, so a
    /// test that reads the booted repository's own store asks the same question the
    /// same way.
    async fn part_members_in(
        sql: &SqlCtx,
        scope_id: i64,
        part: &PartKey,
    ) -> Res<BTreeMap<PeerKey, Access>> {
        let rows = sqlx::query(
            "SELECT s.principal_id AS principal_id, s.access_level AS access_level
               FROM big_sync_syncable s
               JOIN big_sync_parts p ON p.part_ref = s.part_ref
              WHERE p.scope_id = ?1 AND p.part_id = ?2",
        )
        .bind(scope_id)
        .bind(big_sync::sqlite_core::SqliteCore::part_blob(part.clone()))
        .fetch_all(&sql.read_pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let principal: Vec<u8> = row.get("principal_id");
                let level: i64 = row.get("access_level");
                // A principal id is read back out of storage, so a width other than the 32
                // bytes every fixed-width consumer takes is reported rather than assumed.
                let principal =
                    PeerKey::new(<[u8; 32]>::try_from(principal.as_slice()).map_err(|_| {
                        eyre::eyre!(
                            "a principal id is {} bytes wide, expected 32",
                            principal.len()
                        )
                    })?);
                Ok((
                    principal,
                    big_sync::sqlite_core::decode_access(
                        u8::try_from(level).expect("an access level fits a byte"),
                    ),
                ))
            })
            .collect()
    }

    /// A part's rows belong to its `(scope, part_id)` pair, not to the part name:
    /// `part_id` is unique only within a scope (`UNIQUE(scope_id, part_id)`), so a read
    /// that names the part alone unions every scope in the database that holds the same
    /// name. Twenty scopes each holding `part` with twenty members is the shape the
    /// unscoped predicate answers with 400 rows; each scope must see its own twenty.
    #[tokio::test]
    async fn part_members_are_read_from_one_scope() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let part = PartKey::new([0x5E; 32]);
        let peer = |scope: u8, member: u8| {
            let mut bytes = [0u8; 32];
            bytes[0] = scope;
            bytes[1] = member;
            PeerKey::new(bytes)
        };

        let mut scopes = Vec::new();
        for scope in 0..20u8 {
            let scope_key: Arc<str> = Arc::from(format!("daybook-blobs-scope-{scope}"));
            let store: SharedPartStore = Arc::new(
                SqlitePartStore::new(sql.clone(), Arc::clone(&scope_key), BuckId::MAX_LEVEL)
                    .await?,
            );
            let scope_id =
                big_sync::sqlite_core::SqliteCore::ensure_scope_id(&sql.write_pool, &scope_key)
                    .await?;
            // Every scope holds the same part name; only the scope tells the members apart.
            let expected: BTreeMap<PeerKey, Access> = (0..20u8)
                .map(|member| (peer(scope, member), Access::Read))
                .collect();
            store
                .set_part_members(part.clone(), expected.clone().into_iter().collect())
                .await?;
            scopes.push((scope_id, expected));
        }

        for (scope_id, expected) in &scopes {
            assert_eq!(
                part_members_in(&sql, *scope_id, &part).await?,
                expected.clone(),
                "scope {scope_id} must see only its own members"
            );
        }
        Ok(())
    }

    /// ADR 013 §9: a consumer whose cursor sits below the archive floor can never be
    /// woken for the gap — that history is gone. The parts are a function of live
    /// Keyhive state rather than of the events that produced them, so the machine
    /// recovers by resuming at the floor with the boot seed holding the rows.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cursor_below_the_archive_floor_resumes_at_the_floor() -> Res<()> {
        let harness = Harness::new().await?;
        let inventory_doc = harness.create_doc().await?;
        let part = blob_inventory_part_id_from_doc_id(&inventory_doc.to_string());

        let floor = prune_admitted_log(&harness.repo).await?;
        assert_eq!(
            harness.durable_revision().await,
            0,
            "the consumer has never run, so its cursor is below the floor"
        );

        let watch = harness.spawn(vec![inventory_doc.clone()]).await?;
        // Without the recovery the walker has nothing to read and no way to
        // learn the gap existed, so it sits at 0 forever.
        harness.wait_for_revision(floor).await?;
        assert_eq!(
            harness.part_members(&part).await?,
            harness.expected(&inventory_doc).await?,
            "the boot seed, not the pruned wake-ups, holds the rows through recovery"
        );
        watch.stop().await?;
        harness.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_inventory_document_writes_its_own_closure_and_other_subjects_write_nothing()
    -> Res<()> {
        let harness = Harness::new().await?;
        let core_doc = harness.create_doc().await?;
        let docs_doc = harness.create_doc().await?;
        let other_doc = harness.create_doc().await?;
        // A grant only on the core inventory document: the two documents must
        // not cross-write, and the per-document rule is what makes that
        // observable at all (ADR 013 decision A2).
        let granted = harness.repo.create_group_with_parents(Vec::new()).await?;
        harness
            .repo
            .grant_doc_access(core_doc.clone(), granted.clone(), Access::Read)
            .await?;

        let stream = KeyhiveAccessRevisionStore::<SqliteDeltaWalkerStateRepo>::new(&harness.repo);
        let head = stream.latest_revision().await?;
        let watch = harness
            .spawn(vec![core_doc.clone(), docs_doc.clone()])
            .await?;

        let core_part = blob_inventory_part_id_from_doc_id(&core_doc.to_string());
        let docs_part = blob_inventory_part_id_from_doc_id(&docs_doc.to_string());
        // The typed spelling is what the boot seed and the sync path derive the
        // part from, so the machine's string-keyed spelling has to agree with it.
        assert_eq!(
            core_part,
            crate::blobs::blob_inventory_part_id(&core_doc),
            "the two part-id spellings must name the same part"
        );
        assert_ne!(core_part, docs_part, "one part per inventory document");

        harness
            .wait_for_members(&core_part, harness.expected(&core_doc).await?)
            .await?;
        harness
            .wait_for_members(&docs_part, harness.expected(&docs_doc).await?)
            .await?;

        let core_members = harness.part_members(&core_part).await?;
        let docs_members = harness.part_members(&docs_part).await?;
        let granted_peer = PeerKey::new(granted.id().to_bytes());
        assert!(
            core_members.contains_key(&granted_peer),
            "the core inventory document's part carries its document closure"
        );
        assert!(
            !docs_members.contains_key(&granted_peer),
            "a grant on one inventory document must not reach the other's part"
        );
        assert_eq!(core_members, harness.expected(&core_doc).await?);

        // The serving-path predicate the lane exists to clear: a member may
        // fetch, a stranger may not.
        assert!(
            !harness
                .part_store
                .read_denied(
                    ReadTarget::Part(core_part.clone()),
                    harness.repo.local_peer_id()
                )
                .await?,
            "a closure member may fetch the inventory part"
        );
        assert!(
            harness
                .part_store
                .read_denied(ReadTarget::Part(core_part.clone()), stranger())
                .await?,
            "a principal outside the closure must still be refused"
        );

        // A document this machine owns no part for is a no-op that is still
        // acked: the walker may not settle past an unacked entry, so reaching
        // the admission head observed before the wait is the ack.
        harness.wait_for_revision(head).await?;
        let other_part = blob_inventory_part_id_from_doc_id(&other_doc.to_string());
        assert_eq!(
            harness.part_members(&other_part).await?,
            BTreeMap::new(),
            "a subject that controls no derived part writes nothing"
        );

        watch.stop().await?;
        harness.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_smaller_closure_leaves_no_stale_member() -> Res<()> {
        let harness = Harness::new().await?;
        let inventory_doc = harness.create_doc().await?;
        let granted = harness.repo.create_group_with_parents(Vec::new()).await?;
        harness
            .repo
            .grant_doc_access(inventory_doc.clone(), granted.clone(), Access::Read)
            .await?;

        let watch = harness.spawn(vec![inventory_doc.clone()]).await?;
        let part = blob_inventory_part_id_from_doc_id(&inventory_doc.to_string());
        harness
            .wait_for_members(&part, harness.expected(&inventory_doc).await?)
            .await?;
        assert!(
            harness
                .part_members(&part)
                .await?
                .contains_key(&PeerKey::new(granted.id().to_bytes())),
            "the grant has to be written before the revocation can test replacement"
        );

        harness
            .repo
            .revoke_doc_access(inventory_doc.clone(), granted.clone())
            .await?;
        harness
            .wait_for_members(&part, harness.expected(&inventory_doc).await?)
            .await?;
        assert!(
            !harness
                .part_members(&part)
                .await?
                .contains_key(&PeerKey::new(granted.id().to_bytes())),
            "a full replacement must leave no member of the previous closure behind"
        );

        watch.stop().await?;
        harness.stop().await?;
        Ok(())
    }

    /// The boot seed is what makes a fresh store correct before any event: it reads
    /// the closure out of Keyhive directly, so it does not depend on the stream ever
    /// delivering the grant that produced it.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_boot_seed_writes_a_fresh_part_from_keyhive() -> Res<()> {
        let harness = Harness::new().await?;
        let inventory_doc = harness.create_doc().await?;
        let granted = harness.repo.create_group_with_parents(Vec::new()).await?;
        harness
            .repo
            .grant_doc_access(inventory_doc.clone(), granted.clone(), Access::Read)
            .await?;

        let part = blob_inventory_part_id_from_doc_id(&inventory_doc.to_string());
        let parts = BTreeMap::from([(
            AccessSubject::Document(doc_identifier(&inventory_doc)?),
            part.clone(),
        )]);
        // Nothing has written the derived part yet: the store is as fresh as an
        // install that has never run this machine.
        assert_eq!(
            harness.part_members(&part).await?,
            BTreeMap::new(),
            "no event has been processed yet, so only the seed can populate the part"
        );

        seed_inventory_parts(&harness.part_store, harness.repo.keyhive(), &parts).await?;

        let members = harness.part_members(&part).await?;
        assert_eq!(
            members,
            harness.expected(&inventory_doc).await?,
            "the seed writes exactly the document's closure"
        );
        assert!(
            members.contains_key(&PeerKey::new(granted.id().to_bytes())),
            "the granted group is in the closure the seed read"
        );

        harness.stop().await?;
        Ok(())
    }

    /// The user-facing proof for ADR 013 §8: the spawn that serves these parts has
    /// to leave each derived inventory partition readable by exactly the readers of
    /// its inventory document, before any event on that document arrives.
    ///
    /// The seed runs inside the spawn before the machine's reader opens, so the rows
    /// are already present when it returns: a spawn that never ran the seed leaves a
    /// part with no access rows, which refuses every peer (and reads to it as an
    /// unknown part, blocking its full sync forever — the failure the serving
    /// boundary's own tests, `cli_clone_and_wait_until_synced_smoke` and
    /// `long_test_iroh_clone_sync_batch_100_docs_with_blobs`, exercise end to end).
    /// The document ids and the part store come from the booted `RepoCtx`, never from
    /// a fixture the test picked.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_serving_spawn_seeds_the_inventory_parts_before_any_event() -> Res<()> {
        let test_cx = crate::test_support::test_cx("serving_inventory_part_permissions").await?;
        let rcx = Arc::clone(&test_cx.rt.rcx);
        // The blob part store is built on the repository's own sqlite ctx
        // (`open_blob_part_store(big_repo.sql_ctx())`), and the access rows live there.
        let store_sql = test_cx._acx.sql_ctx();
        // That ctx is shared by every scope in the database, so the blob store's own
        // scope has to be named: `open_blob_part_store` built it under `BLOB_SCOPE_KEY`.
        let store_scope_id = big_sync::sqlite_core::SqliteCore::ensure_scope_id(
            &store_sql.write_pool,
            &Arc::from(crate::repo::BLOB_SCOPE_KEY),
        )
        .await?;
        let local_peer = test_cx._acx.local_peer_id();
        // Exactly what `IrohSyncRepo::boot` spawns: the parts it serves plus the
        // store, state and repository they are derived from.
        let writer = spawn_blob_inventory_permission_writer(
            Arc::clone(&rcx.blob_part_store),
            Arc::clone(&rcx.sqlite_local_state_repo),
            Arc::clone(&rcx.big_repo),
            vec![
                rcx.core_inventory_doc_id.clone(),
                rcx.docs_inventory_doc_id.clone(),
            ],
            CancellationToken::new(),
        )
        .await?;

        for inventory_doc in [
            rcx.core_inventory_doc_id.clone(),
            rcx.docs_inventory_doc_id.clone(),
        ] {
            let part = crate::blobs::blob_inventory_part_id(&inventory_doc);
            let expected = expected_members(rcx.big_repo.keyhive(), &inventory_doc).await?;
            assert!(
                expected.contains_key(&local_peer),
                "the document's own admin is in its closure, or the admission control proves nothing"
            );

            assert_eq!(
                part_members_in(&store_sql, store_scope_id, &part).await?,
                expected,
                "a boot writes the inventory document's own closure into its derived part"
            );

            // The peer-facing predicate the lane exists to clear: a closure member may
            // read the part, a principal outside the closure may not.
            assert!(
                !rcx.blob_part_store
                    .read_denied(ReadTarget::Part(part.clone()), local_peer.clone())
                    .await?,
                "a reader of the inventory document may fetch its derived part"
            );
            assert!(
                rcx.blob_part_store
                    .read_denied(ReadTarget::Part(part), stranger())
                    .await?,
                "a principal outside the closure must still be refused"
            );
        }

        writer.stop().await?;
        test_cx.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_store_error_leaves_the_entry_unacked() -> Res<()> {
        let harness = Harness::new().await?;
        let inventory_doc = harness.create_doc().await?;
        // Break the sink's own storage: `set_part_members` cannot commit, so the
        // machine must not ack the entry it could not apply.
        sqlx::query("DROP TABLE big_sync_syncable")
            .execute(&harness.store_sql.write_pool)
            .await?;

        let stream = KeyhiveAccessRevisionStore::new(&harness.repo);
        let parts = inventory_parts(std::slice::from_ref(&inventory_doc))?;
        let error = run_permission_machine(
            Arc::clone(&harness.part_store),
            stream,
            harness.state.clone(),
            parts,
            CancellationToken::new(),
        )
        .await
        .expect_err("a sink that cannot commit must surface as a machine error");

        assert!(
            format!("{error:?}").contains("no such table"),
            "the failure has to be the injected store error, got: {error:?}"
        );
        assert_eq!(
            harness.durable_revision().await,
            0,
            "work the machine did not apply must stay unacked, so the walker re-drives it"
        );

        harness.stop().await?;
        Ok(())
    }
}
