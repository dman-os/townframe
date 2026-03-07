use super::*;

pub(super) struct DocSyncWorkerStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl DocSyncWorkerStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(2)).await?;
        Ok(())
    }
}

pub async fn spawn_doc_sync_worker(
    doc_id: DocumentId,
    acx: AmCtx,
    cancel_token: CancellationToken,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
) -> Res<DocSyncWorkerStopToken> {
    let stop_cancel_token = cancel_token.clone();
    let worker = DocSyncWorker {
        doc_id: doc_id.clone(),
        msg_tx,
        retry,
    };

    let fut = {
        async move {
            let Some(handle) = acx.repo().find(doc_id.clone()).await? else {
                worker.handle_missing_doc();
                return eyre::Ok(());
            };
            let (broker_handle, broker_stop_token) =
                acx.change_manager().add_doc(handle.clone()).await?;
            let mut heads_listener = broker_handle.get_head_listener().await?;
            let (peer_state, state_stream) = handle.peers();
            worker.handle_peer_state_update(peer_state);

            let mut idle_timeout = Box::pin(tokio::time::sleep(Duration::from_secs(120)));
            let mut state_stream = state_stream.boxed();
            let loop_res: Res<()> = loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => {
                        debug!("cancel token lit");
                        break eyre::Ok(());
                    }
                    val = heads_listener.change_rx().recv() => {
                        let Some(heads) = val else {
                            break Err(eyre::eyre!("DocChangeBroker was removed from repo, weird!"));
                        };
                        worker.handle_heads_update(heads);
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + Duration::from_secs(120));
                    }
                    val = state_stream.next() => {
                        let Some(diff) = val else {
                            break Err(eyre::eyre!("DocHandle was removed from repo, weird!"));
                        };
                        worker.handle_peer_state_update(diff);
                        idle_timeout
                            .as_mut()
                            .reset(tokio::time::Instant::now() + Duration::from_secs(120));
                    }
                    _ = &mut idle_timeout => {
                        worker.handle_timeout();
                        break eyre::Ok(());
                    }
                }
            };
            if let Ok(token) = Arc::try_unwrap(broker_stop_token) {
                token.stop().await?;
            }
            loop_res
        }
    };
    let join_handle = tokio::spawn(
        async move { fut.await.unwrap() }.instrument(tracing::info_span!("DocSyncWorker task")),
    );
    Ok(DocSyncWorkerStopToken {
        cancel_token: stop_cancel_token,
        join_handle,
    })
}

struct DocSyncWorker {
    doc_id: DocumentId,
    msg_tx: mpsc::UnboundedSender<Msg>,
    retry: RetryState,
}

impl DocSyncWorker {
    fn handle_heads_update(&self, heads: Arc<[automerge::ChangeHash]>) {
        self.msg_tx
            .send(Msg::DocHeadsUpdated {
                doc_id: self.doc_id.clone(),
                heads: ChangeHashSet(heads),
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }

    fn handle_peer_state_update(&self, diff: DocPeerStateView) {
        self.msg_tx
            .send(Msg::DocPeerStateViewUpdated {
                doc_id: self.doc_id.clone(),
                diff,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }

    fn handle_timeout(&self) {
        self.msg_tx
            .send(Msg::DocSyncRequestBackoff {
                doc_id: self.doc_id.clone(),
                delay: Duration::from_millis(500),
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }

    fn handle_missing_doc(&self) {
        self.msg_tx
            .send(Msg::DocSyncRequestBackoff {
                doc_id: self.doc_id.clone(),
                delay: Duration::from_millis(500),
                previous_attempt_no: self.retry.attempt_no,
                previous_backoff: self.retry.last_backoff,
                previous_attempt_at: self.retry.last_attempt_at,
            })
            .expect("FullSyncWorker went down without cleaning boot_doc_sync_worker");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::sync::{self, SyncDoc};
    use automerge::transaction::Transactable;
    async fn wait_for_peer_doc_state(
        handle: &samod::DocHandle,
        conn_id: ConnectionId,
        predicate: impl Fn(&samod::PeerDocState) -> bool,
    ) -> Res<samod::PeerDocState> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if tokio::time::Instant::now() > deadline {
                eyre::bail!("timed out waiting for peer doc state");
            }
            let (current, _state_stream) = handle.peers();
            if let Some(state) = current.get(&conn_id) {
                if predicate(state) {
                    return Ok(state.clone());
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn peer_doc_state_heads_are_frontiers_not_full_history() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let alice_peer_id = format!("alice-{}", Uuid::new_v4());
        let bob_peer_id = format!("bob-{}", Uuid::new_v4());
        let (alice_acx, alice_stop) = AmCtx::boot(
            am_utils_rs::Config {
                peer_id: alice_peer_id.clone(),
                storage: am_utils_rs::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let (bob_acx, bob_stop) = AmCtx::boot(
            am_utils_rs::Config {
                peer_id: bob_peer_id.clone(),
                storage: am_utils_rs::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        #[allow(deprecated)]
        fn repos(acx: &AmCtx) -> &samod::Repo {
            acx.repo()
        }

        let connected = crate::tincans::connect_repos(repos(&alice_acx), repos(&bob_acx));
        repos(&alice_acx)
            .when_connected(samod::PeerId::from(bob_peer_id.as_str()))
            .await?;
        let alice_on_bob = repos(&bob_acx)
            .when_connected(samod::PeerId::from(alice_peer_id.as_str()))
            .await?;

        let alice_handle = alice_acx.add_doc(automerge::Automerge::new()).await?;
        let bob_handle = bob_acx
            .find_doc(alice_handle.document_id())
            .await?
            .ok_or_eyre("bob could not find alice doc")?;

        let (initial_states, _state_stream) = bob_handle.peers();
        assert!(
            initial_states.contains_key(&alice_on_bob.id()),
            "expected bob to track peer state for alice"
        );

        let heads_at_1 = alice_handle.with_document(|doc| -> Res<Vec<automerge::ChangeHash>> {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "k1", "v1")?;
            tx.commit();
            Ok(doc.get_heads())
        })?;
        let state = wait_for_peer_doc_state(&bob_handle, alice_on_bob.id(), |state| {
            state.their_heads.as_ref() == Some(&heads_at_1)
        })
        .await?;
        assert_eq!(state.their_heads, Some(heads_at_1.clone()));

        let heads_at_2 = alice_handle.with_document(|doc| -> Res<Vec<automerge::ChangeHash>> {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "k2", "v2")?;
            tx.commit();
            Ok(doc.get_heads())
        })?;
        let state = wait_for_peer_doc_state(&bob_handle, alice_on_bob.id(), |state| {
            state.their_heads.as_ref() == Some(&heads_at_2)
                && state.shared_heads.as_ref() == Some(&heads_at_2)
        })
        .await?;
        assert_eq!(state.their_heads, Some(heads_at_2.clone()));
        assert_eq!(state.shared_heads, Some(heads_at_2.clone()));

        let heads_after_branch =
            alice_handle.with_document(|doc| -> Res<Vec<automerge::ChangeHash>> {
                let branch_base = heads_at_1.clone();
                let mut tx = doc.transaction_at(automerge::PatchLog::null(), &branch_base);
                tx.put(automerge::ROOT, "k_branch", "v_branch")?;
                tx.commit();
                Ok(doc.get_heads())
            })?;
        assert!(
            heads_after_branch.len() >= 2,
            "expected concurrent branch heads after transaction_at from old heads"
        );
        let state = wait_for_peer_doc_state(&bob_handle, alice_on_bob.id(), |state| {
            state.their_heads.as_ref() == Some(&heads_after_branch)
        })
        .await?;
        assert_eq!(state.their_heads, Some(heads_after_branch.clone()));

        let total_change_count = alice_handle.with_document(|doc| doc.get_changes(&[]).len());
        let their_head_count = state.their_heads.as_ref().map_or(0, Vec::len);
        assert!(
            their_head_count < total_change_count,
            "their_heads should be current frontier heads, not full change history"
        );

        connected.disconnect().await;
        alice_stop.stop().await?;
        bob_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn peer_doc_state_for_diverged_histories_matches_common_then_converged_heads() -> Res<()>
    {
        utils_rs::testing::setup_tracing_once();
        let alice_peer_id = format!("alice-diverge-{}", Uuid::new_v4());
        let bob_peer_id = format!("bob-diverge-{}", Uuid::new_v4());
        let (alice_acx, alice_stop) = AmCtx::boot(
            am_utils_rs::Config {
                peer_id: alice_peer_id.clone(),
                storage: am_utils_rs::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let (bob_acx, bob_stop) = AmCtx::boot(
            am_utils_rs::Config {
                peer_id: bob_peer_id.clone(),
                storage: am_utils_rs::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        #[allow(deprecated)]
        fn repos(acx: &AmCtx) -> &samod::Repo {
            acx.repo()
        }
        // 1) Connect once so both peers share the same base document.
        let initial_connected = crate::tincans::connect_repos(repos(&alice_acx), repos(&bob_acx));
        repos(&alice_acx)
            .when_connected(samod::PeerId::from(bob_peer_id.as_str()))
            .await?;
        let alice_on_bob = repos(&bob_acx)
            .when_connected(samod::PeerId::from(alice_peer_id.as_str()))
            .await?;

        let alice_handle = alice_acx.add_doc(automerge::Automerge::new()).await?;
        let bob_handle = bob_acx
            .find_doc(alice_handle.document_id())
            .await?
            .ok_or_eyre("bob could not find alice doc")?;
        let _synced_base = wait_for_peer_doc_state(&bob_handle, alice_on_bob.id(), |state| {
            state.shared_heads.is_some()
        })
        .await?;

        // 2) Disconnect to create independent diverging edits.
        initial_connected.disconnect().await;

        let base_heads_alice = alice_handle.with_document(|doc| doc.get_heads());
        let base_heads_bob = bob_handle.with_document(|doc| doc.get_heads());
        assert_eq!(
            base_heads_alice, base_heads_bob,
            "both nodes should share base"
        );

        // Diverge independently on disjoint keys while disconnected.
        let alice_heads_diverged =
            alice_handle.with_document(|doc| -> Res<Vec<automerge::ChangeHash>> {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "alice_only", "a")?;
                tx.commit();
                Ok(doc.get_heads())
            })?;
        let bob_heads_diverged =
            bob_handle.with_document(|doc| -> Res<Vec<automerge::ChangeHash>> {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "bob_only", "b")?;
                tx.commit();
                Ok(doc.get_heads())
            })?;

        assert_ne!(alice_heads_diverged, bob_heads_diverged);
        assert_eq!(alice_heads_diverged.len(), 1);
        assert_eq!(bob_heads_diverged.len(), 1);

        // 3) Reconnect and inspect PeerDocState convergence semantics.
        let connected = crate::tincans::connect_repos(repos(&alice_acx), repos(&bob_acx));
        repos(&alice_acx)
            .when_connected(samod::PeerId::from(bob_peer_id.as_str()))
            .await?;
        let alice_on_bob_after_reconnect = repos(&bob_acx)
            .when_connected(samod::PeerId::from(alice_peer_id.as_str()))
            .await?;

        // Right after reconnect, shared_heads may already be converged if sync is fast.
        // We only require that shared_heads exists and is a frontier set (heads, not history).
        let state_pre_merge =
            wait_for_peer_doc_state(&bob_handle, alice_on_bob_after_reconnect.id(), |state| {
                state.shared_heads.is_some()
            })
            .await?;
        let shared_pre_merge = state_pre_merge
            .shared_heads
            .clone()
            .ok_or_eyre("missing shared heads pre-merge")?;
        assert!(
            shared_pre_merge.len() <= 2,
            "shared_heads should be frontier heads, got too many"
        );

        // After sync converges, both sides should report same heads (two concurrent heads).
        let state_post_merge =
            wait_for_peer_doc_state(&bob_handle, alice_on_bob_after_reconnect.id(), |state| {
                state
                    .their_heads
                    .as_ref()
                    .is_some_and(|their| their.len() == 2)
                    && state
                        .shared_heads
                        .as_ref()
                        .is_some_and(|shared| shared.len() == 2)
            })
            .await?;
        let shared_post_merge = state_post_merge
            .shared_heads
            .clone()
            .ok_or_eyre("missing shared heads post-merge")?;
        let their_post_merge = state_post_merge
            .their_heads
            .clone()
            .ok_or_eyre("missing their_heads post-merge")?;

        let alice_heads_after_sync = alice_handle.with_document(|doc| doc.get_heads());
        let bob_heads_after_sync = bob_handle.with_document(|doc| doc.get_heads());
        assert_eq!(alice_heads_after_sync, bob_heads_after_sync);
        assert_eq!(alice_heads_after_sync.len(), 2);
        assert_eq!(shared_post_merge, bob_heads_after_sync);
        assert_eq!(their_post_merge, alice_heads_after_sync);

        connected.disconnect().await;
        alice_stop.stop().await?;
        bob_stop.stop().await?;
        Ok(())
    }

    #[test]
    fn automerge_sync_protocol_reconciles_full_history_but_sends_incremental_changes() -> Res<()> {
        let mut peer1 = automerge::AutoCommit::new();
        peer1.put(automerge::ROOT, "base", "v0")?;
        peer1.put(automerge::ROOT, "base2", "v1")?;

        let mut peer1_state = sync::State::new();
        let mut peer2 = automerge::AutoCommit::new();
        let mut peer2_state = sync::State::new();

        loop {
            let one_to_two = peer1.sync().generate_sync_message(&mut peer1_state);
            if let Some(message) = one_to_two.as_ref() {
                peer2
                    .sync()
                    .receive_sync_message(&mut peer2_state, message.clone())?;
            }
            let two_to_one = peer2.sync().generate_sync_message(&mut peer2_state);
            if let Some(message) = two_to_one.as_ref() {
                peer1
                    .sync()
                    .receive_sync_message(&mut peer1_state, message.clone())?;
            }
            if one_to_two.is_none() && two_to_one.is_none() {
                break;
            }
        }

        assert_eq!(peer1.get_heads(), peer2.get_heads());

        peer1.put(automerge::ROOT, "peer1_only", "a")?;
        peer2.put(automerge::ROOT, "peer2_only", "b")?;

        let peer1_heads_before = peer1.get_heads();
        let peer1_change_count = peer1.get_changes(&[]).len();
        let peer2_change_count = peer2.get_changes(&[]).len();

        let one_to_two = peer1
            .sync()
            .generate_sync_message(&mut peer1_state)
            .ok_or_eyre("peer1 should have a sync message after divergence")?;
        assert_eq!(one_to_two.heads, peer1_heads_before);
        assert!(
            !one_to_two.have.is_empty() || !one_to_two.need.is_empty(),
            "sync message should carry reconciliation metadata"
        );
        assert!(
            one_to_two.changes.len() < peer1_change_count,
            "peer1 should not send full history for a one-change divergence"
        );

        peer2
            .sync()
            .receive_sync_message(&mut peer2_state, one_to_two.clone())?;
        assert_eq!(peer2_state.their_heads, Some(peer1_heads_before.clone()));
        assert!(peer2_state.their_have.is_some());
        assert!(peer2_state.their_need.is_some());
        let peer2_heads_after_receive = peer2.get_heads();

        let two_to_one = peer2
            .sync()
            .generate_sync_message(&mut peer2_state)
            .ok_or_eyre("peer2 should answer with a sync message after divergence")?;
        assert_eq!(two_to_one.heads, peer2_heads_after_receive);
        assert!(
            two_to_one.changes.len() < peer2_change_count,
            "peer2 should not send full history for a one-change divergence"
        );

        peer1
            .sync()
            .receive_sync_message(&mut peer1_state, two_to_one.clone())?;

        loop {
            let one_to_two = peer1.sync().generate_sync_message(&mut peer1_state);
            if let Some(message) = one_to_two.as_ref() {
                peer2
                    .sync()
                    .receive_sync_message(&mut peer2_state, message.clone())?;
            }
            let two_to_one = peer2.sync().generate_sync_message(&mut peer2_state);
            if let Some(message) = two_to_one.as_ref() {
                peer1
                    .sync()
                    .receive_sync_message(&mut peer1_state, message.clone())?;
            }
            if one_to_two.is_none() && two_to_one.is_none() {
                break;
            }
        }

        assert_eq!(peer1.get_heads(), peer2.get_heads());
        assert_eq!(peer1.get_heads().len(), 2);
        Ok(())
    }
}
