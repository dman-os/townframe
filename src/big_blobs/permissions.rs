use crate::work_queue::WorkQueue;
use big_cabinet_core::DocId;
use big_repo::AccessSubject;
use big_repo::AccessSubjectSet;
use big_repo::KeyhiveAccessDelta;
use big_repo::KeyhiveAccessRevisionStore;
use big_repo::KeyhiveAccessSelector;
use big_repo::SharedBigRepo;
use big_repo::SharedPartStore;
use big_sync::DeltaWalkerStateRepo;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::PeerKey;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaRead;
use big_sync_core::concurrent_delta_walker::ConcurrentDeltaWalker;
use big_sync_core::delta_walker_state::DeltaWalkerStateTransaction;
use big_sync_core::revisioned_store::RevisionedStore;
use std::num::NonZeroUsize;
use utils_rs::prelude::Res;

pub(crate) async fn seed(
    repo: &SharedBigRepo,
    parts: &SharedPartStore,
    document: &DocId,
) -> Res<()> {
    let key = ed25519_dalek::VerifyingKey::from_bytes(&document.to_bytes32()?)?;
    let identifier = big_repo::keyhive_core::principal::identifier::Identifier::from(key);
    let agents = repo.keyhive().agents_for_membered(identifier).await;
    parts
        .set_part_members(
            crate::workers::inventory_part(document),
            agents
                .into_iter()
                .map(|(id, access)| (PeerKey::new(id.to_bytes()), access))
                .collect(),
        )
        .await?;
    Ok(())
}

/// Seed the plaintext part of `drawer` from live Keyhive state (ADR 026 §7): its
/// members are the agents holding access on the drawer's docs group.
pub(crate) async fn seed_drawer(
    repo: &SharedBigRepo,
    parts: &SharedPartStore,
    drawer: &DocId,
    docs_group: [u8; 32],
) -> Res<()> {
    let key = ed25519_dalek::VerifyingKey::from_bytes(&docs_group)?;
    let identifier = big_repo::keyhive_core::principal::identifier::Identifier::from(key);
    let agents = repo.keyhive().agents_for_membered(identifier).await;
    parts
        .set_part_members(
            crate::workers::plaintext_part(drawer),
            agents
                .into_iter()
                .map(|(id, access)| (PeerKey::new(id.to_bytes()), access))
                .collect(),
        )
        .await?;
    Ok(())
}

pub(crate) async fn run(
    repo: SharedBigRepo,
    queue: WorkQueue,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let state = SqliteDeltaWalkerStateRepo::new(
        repo.database(),
        "big-blobs-permissions-v1",
        "keyhive-access",
    )
    .await?;
    let stream: KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo> =
        KeyhiveAccessRevisionStore::new(&repo);
    let durable = state.progress().await?.upstream_revision;
    let resume = durable.max(stream.archived_through().await?);
    if resume > durable {
        queue.rearm().await?;
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
        let read: ConcurrentDeltaRead<AccessSubject, KeyhiveAccessDelta> = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            read = walker.next(NonZeroUsize::new(32).unwrap()) => read?,
        };
        if let ConcurrentDeltaRead::Entries { entries, .. } = read {
            for delta in entries {
                match delta.entry.subject {
                    AccessSubject::Document(id) => {
                        queue.enqueue(&DocId::new(id.to_bytes())).await?
                    }
                    AccessSubject::Group(_) => queue.rearm().await?,
                }
                walker.ack(delta.key, delta.cursor).await?;
            }
        }
    }
}
