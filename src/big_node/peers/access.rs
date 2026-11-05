//! The peers service's feed of keyhive access changes.
//!
//! Whether a peer is a member of a group this node wants from it is keyhive state, and it arrives
//! when the keyhive pull with that peer admits the grant. `big_repo` reports each admission as a
//! per-subject access delta over its durable admission log ([`KeyhiveAccessRevisionStore`]); this
//! task is the peers service's consumer of that stream, driven by a [`ConcurrentDeltaWalker`] over
//! walker state of its own, as `big_blobs`' permissions ingress does. The actor reads the deltas
//! and recomputes the parts of the peers they touch, so a grant is acted on when it lands rather
//! than when a timer next asks.

use super::*;
use big_repo::AccessSubjectSet;
use big_repo::KeyhiveAccessRevisionStore;
use big_repo::KeyhiveAccessSelector;
use big_sync::DeltaWalkerStateRepo;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaRead;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaWalker;
use big_sync_core::delta_walker_state::DeltaWalkerStateTransaction;
use big_sync_core::revisioned_store::RevisionedStore;
use std::num::NonZeroUsize;

/// Starts [`feed`] as one of the service's tasks and returns the deltas it reads.
pub(super) fn spawn(
    tasks: &service_utils_rs::Spawner,
    repo: big_repo::SharedBigRepo,
    scope_key: &str,
) -> mpsc::UnboundedReceiver<KeyhiveAccessDelta> {
    let (changes, deltas) = mpsc::unbounded_channel();
    let scope_key = scope_key.to_owned();
    let cancel = tasks.clone();
    tasks
        .spawn(async move {
            feed(repo, &scope_key, changes, cancel.cancelled())
                .await
                .unwrap()
        })
        .expect("a fresh stop token accepts tasks");
    deltas
}

/// Reads the repo's access stream into `changes` until `shutdown` resolves.
///
/// The walker state lives in the repo's database under `{scope_key}:big-node-peers-v1`, so repos
/// that share a database (distinct scope keys) keep distinct cursors and retention rows. A delta
/// is acked once handed to the actor: the actor's state is in memory, and a peer that connects
/// later computes its parts from live keyhive state, so nothing is owed for a delta the actor
/// never saw.
async fn feed(
    repo: big_repo::SharedBigRepo,
    scope_key: &str,
    changes: mpsc::UnboundedSender<KeyhiveAccessDelta>,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let state = SqliteDeltaWalkerStateRepo::new(
        repo.database(),
        format!("{scope_key}:big-node-peers-v1"),
        "keyhive-access",
    )
    .await?;
    let stream: KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo> =
        KeyhiveAccessRevisionStore::new(&repo);
    // A cursor below the log's pruned floor cannot be woken for the gap, so it resumes at the
    // floor (ADR 013 section 9). Nothing is rebuilt: parts are recomputed on connect.
    let durable = state.progress().await?.upstream_revision;
    let resume = durable.max(stream.archived_through().await?);
    if resume > durable {
        let mut tx = state.begin().await?;
        tx.advance_from(durable, resume).await?;
        tx.commit().await?;
    }
    stream.note_retention(&state).await?;
    let reader = stream
        .open(
            KeyhiveAccessSelector {
                watch: AccessSubjectSet::All,
                memory: state.clone(),
            },
            resume,
        )
        .await?;
    let mut walker: ConcurrentDeltaWalker<
        '_,
        KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo>,
        SqliteDeltaWalkerStateRepo,
        AccessSubject,
    > = ConcurrentDeltaWalker::open(reader, state, |entry: &KeyhiveAccessDelta| entry.subject)
        .await?;
    tokio::pin!(shutdown);
    loop {
        let read = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            read = walker.next(NonZeroUsize::new(32).expect("32 is not zero")) => read?,
        };
        if let ConcurrentDeltaRead::Entries { entries, .. } = read {
            for delta in entries {
                let (key, cursor) = (delta.key, delta.cursor);
                // The actor holds the receiver until it stops, and it stops with this task: a
                // closed channel is the service going down, not a failure.
                if changes.send(delta.entry).is_err() {
                    return Ok(());
                }
                walker.ack(key, cursor).await?;
            }
        }
    }
}
