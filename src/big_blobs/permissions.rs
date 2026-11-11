use crate::work_queue::WorkQueue;
use big_cabinet_core::DocId;
use big_repo::AccessSubject;
use big_repo::AccessSubjectSet;
use big_repo::KeyhiveAccessDelta;
use big_repo::KeyhiveAccessRevisionStore;
use big_repo::KeyhiveAccessSelector;
use big_repo::SharedBigRepo;
use big_repo::SharedPartStore;
use big_repo::keyhive_core::access::Access;
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
                .filter(|(_, access)| *access >= Access::Read)
                .map(|(id, access)| (PeerKey::new(id.to_bytes()), access))
                .collect(),
        )
        .await?;
    Ok(())
}

async fn seed_known(
    repo: &SharedBigRepo,
    parts: &SharedPartStore,
    sql: &sqlite_rs::Database,
) -> Res<()> {
    let inventories = crate::admission::sqlite::queries::known_inventories(
        sql,
        crate::workers::INVENTORY_KIND.to_owned(),
    )
    .await?;
    for inventory in inventories {
        seed(repo, parts, &inventory.parse()?).await?;
    }
    Ok(())
}

/// The walker state of the permissions ingress for the repo booted with `scope_key`.
///
/// The state lives in the repo's database, which repos with distinct scope keys may share (and
/// `big_node` nodes do). Each repo has an admission log of its own, so the cursor is namespaced
/// `{scope_key}:big-blobs-permissions-v1`: a shared cursor would let one repo's acks skip the
/// other's deltas, and register both under one retention row.
async fn walker_state(repo: &SharedBigRepo, scope_key: &str) -> Res<SqliteDeltaWalkerStateRepo> {
    SqliteDeltaWalkerStateRepo::new(
        repo.database(),
        format!("{scope_key}:big-blobs-permissions-v1"),
        "keyhive-access",
    )
    .await
    .map_err(Into::into)
}

pub(crate) async fn run(
    repo: SharedBigRepo,
    scope_key: &str,
    parts: SharedPartStore,
    sql: sqlite_rs::Database,
    queue: WorkQueue,
    mirrors: crate::admission::InventoryMirrors,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    seed_known(&repo, &parts, &sql).await?;
    mirrors.refresh_policy().await?;
    let state = walker_state(&repo, scope_key).await?;
    let stream: KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo> =
        KeyhiveAccessRevisionStore::new(&repo);
    let durable = state.progress().await?.upstream_revision;
    let resume = durable.max(stream.archived_through().await?);
    if resume > durable {
        queue.rearm().await?;
        mirrors.refresh_policy().await?;
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
                        let document = DocId::new(id.to_bytes());
                        let known = crate::admission::sqlite::queries::known_inventory(
                            &sql,
                            document.to_string(),
                            crate::workers::INVENTORY_KIND.to_owned(),
                        )
                        .await?;
                        if !known.is_empty() {
                            seed(&repo, &parts, &document).await?;
                            queue.rearm().await?;
                        }
                        queue.enqueue(&document).await?;
                    }
                    AccessSubject::Group(_) => {
                        seed_known(&repo, &parts, &sql).await?;
                        queue.rearm().await?;
                    }
                }
                mirrors.refresh_policy().await?;
                walker.ack(delta.key, delta.cursor).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use big_sync::ReadTarget;
    use std::sync::Arc;

    /// Repos that share a database (distinct scope keys, as `big_node` nodes may) each own an
    /// admission log, so each must own its permissions cursor. With one unscoped namespace the
    /// first repo's acks would stand as the second's progress, and the second's ingress would
    /// resume past deltas it never read.
    #[tokio::test]
    async fn repos_sharing_a_database_keep_distinct_permission_cursors() -> Res<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[])?;
        let mut repos = Vec::new();
        for (seed, scope_key) in [(91_u8, "first"), (92, "second")] {
            let (repo, stop) = big_repo::BigRepo::boot(big_repo::Config {
                node_identity_seed: [seed; 32],
                storage: big_repo::Storage {
                    sql: sql.clone(),
                    keyhive: big_repo::KeyhiveStorage::Memory,
                },
                scope_key: scope_key.into(),
                hidden_parts: Default::default(),
                automerge_frontier_group_scope: Default::default(),
                causal_checkpoint_group_scope: Default::default(),
                group_part_group_scope: Default::default(),
            })
            .await?;
            repos.push((scope_key, repo, stop));
        }
        let (first_scope, first, _) = &repos[0];
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "fixture", true)?;
        tx.commit();
        let handle = first.create_doc(doc).await?;
        let stream: KeyhiveAccessRevisionStore<SqliteDeltaWalkerStateRepo> =
            KeyhiveAccessRevisionStore::new(first);
        let admitted = stream.latest_revision().await?;
        assert!(admitted > 0, "creating a doc admits keyhive events");
        let first_state = walker_state(first, first_scope).await?;
        let mut tx = first_state.begin().await?;
        tx.advance_from(0, admitted).await?;
        tx.commit().await?;
        let (second_scope, second, _) = &repos[1];
        let second_state = walker_state(second, second_scope).await?;
        assert_eq!(first_state.progress().await?.upstream_revision, admitted);
        assert_eq!(
            second_state.progress().await?.upstream_revision,
            0,
            "the second repo's cursor moved with the first's acks"
        );
        drop((handle, first_state, second_state, stream));
        for (_, repo, stop) in repos {
            drop(repo);
            stop.stop().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn empty_inventory_seeding_requires_exact_read_standing() -> Res<()> {
        let runtime = Arc::new(sqlite_rs::SqliteRuntime::new(Default::default()));
        let mut nodes = Vec::new();
        for seed in [81, 82] {
            nodes.push(
                big_repo::BigRepo::boot(big_repo::Config {
                    node_identity_seed: [seed; 32],
                    storage: big_repo::Storage::memory(&runtime).unwrap(),
                    scope_key: "inventory-permissions".into(),
                    hidden_parts: Default::default(),
                    automerge_frontier_group_scope: Default::default(),
                    causal_checkpoint_group_scope: Default::default(),
                    group_part_group_scope: Default::default(),
                })
                .await?,
            );
        }
        let (reader, reader_stop) = nodes.pop().unwrap();
        let (owner, owner_stop) = nodes.pop().unwrap();
        let agent = owner
            .receive_keyhive_contact_card(&reader.local_keyhive_contact_card())
            .await?;
        let mut initial = automerge::Automerge::new();
        let mut tx = initial.transaction();
        tx.put(automerge::ROOT, "fixture", true)?;
        tx.commit();
        let first = owner.create_doc(initial.clone()).await?;
        let second = owner.create_doc(initial).await?;
        let first_id = first.document_id();
        let second_id = second.document_id();
        owner
            .grant_doc_access(first_id.clone(), agent.clone(), Access::Relay)
            .await?;
        owner
            .grant_doc_access(second_id.clone(), agent.clone(), Access::Read)
            .await?;
        let parts: SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
        let first_part = crate::workers::inventory_part(&first_id);
        let second_part = crate::workers::inventory_part(&second_id);
        let peer = reader.local_peer_id();
        seed(&owner, &parts, &first_id).await?;
        seed(&owner, &parts, &second_id).await?;
        assert!(
            parts
                .read_denied(ReadTarget::Part(first_part.clone()), peer.clone())
                .await?
        );
        assert!(
            !parts
                .read_denied(ReadTarget::Part(second_part.clone()), peer.clone())
                .await?
        );
        assert_eq!(parts.member_count(first_part.clone()).await?, 0);
        assert_eq!(parts.member_count(second_part.clone()).await?, 0);
        owner
            .revoke_doc_access(first_id.clone(), agent.clone())
            .await?;
        owner
            .grant_doc_access(first_id.clone(), agent.clone(), Access::Read)
            .await?;
        seed(&owner, &parts, &first_id).await?;
        assert!(
            !parts
                .read_denied(ReadTarget::Part(first_part.clone()), peer.clone())
                .await?
        );
        owner.revoke_doc_access(first_id.clone(), agent).await?;
        seed(&owner, &parts, &first_id).await?;
        assert!(
            parts
                .read_denied(ReadTarget::Part(first_part), peer.clone())
                .await?
        );
        assert!(
            !parts
                .read_denied(ReadTarget::Part(second_part), peer)
                .await?
        );
        drop(first);
        drop(second);
        reader_stop.stop().await?;
        owner_stop.stop().await?;
        Ok(())
    }
}
