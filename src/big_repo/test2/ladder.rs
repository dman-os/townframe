use super::harness::{Node, assert_materialized_parity};
use crate::{BigKeyhiveAgent, DocLookup};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;
use std::sync::Arc;
use subduction_keyhive::KeyhivePeerId;
use tokio::time::{Duration, timeout};

async fn wait_for_agent(
    repo: &Arc<crate::BigRepo>,
    peer_id: big_sync_core::PeerId,
) -> crate::Res<BigKeyhiveAgent> {
    timeout(Duration::from_secs(10), async {
        loop {
            let keyhive_peer_id = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
            if let Some(agent) = repo
                .keyhive()
                .get_agent_by_peer_id(&keyhive_peer_id)
                .await?
            {
                return Ok(agent);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

async fn wait_for_document_access(
    repo: &Arc<crate::BigRepo>,
    doc_id: crate::DocumentId,
    peer_id: big_sync_core::PeerId,
) -> crate::Res<()> {
    let agent_key = ed25519_dalek::VerifyingKey::from_bytes(peer_id.as_bytes())
        .expect("peer id must be a verifying key");
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("document id must be a verifying key");
    let agent = keyhive_core::principal::identifier::Identifier::from(agent_key);
    let document = keyhive_core::principal::identifier::Identifier::from(doc_key);
    timeout(Duration::from_secs(10), async {
        loop {
            if repo
                .keyhive()
                .agent_access_on(&agent, document)
                .await
                .is_some()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

async fn wait_for_global_membership(node: &Node, doc_id: crate::DocumentId) -> crate::Res<()> {
    timeout(Duration::from_secs(10), async {
        loop {
            if node
                .store
                .obj_parts(doc_id)
                .await?
                .contains(&crate::GLOBAL_PART_ID)
            {
                return Ok::<_, utils_rs::prelude::eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_connected_document_replicates_and_preserves_head_parity() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let left = Node::boot(31).await?;
    let right = Node::boot(32).await?;

    let left_connection = left.connect(&right).await?;
    let right_connection = right.accepted_connection().await;
    left_connection.sync_keyhive_with_peer(None).await?;

    let right_agent = wait_for_agent(&left.repo, right.peer_id()).await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "ladder rung 1"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let document = left.repo.create_doc(initial).await?;
    let doc_id = document.document_id();
    left.repo
        .grant_doc_access(doc_id, right_agent, Access::Read)
        .await?;

    left_connection.sync_keyhive_with_peer(None).await?;
    right_connection.sync_keyhive_with_peer(None).await?;
    wait_for_document_access(&right.repo, doc_id, right.peer_id()).await?;
    wait_for_global_membership(&right, doc_id).await?;
    let right_doc = timeout(Duration::from_secs(20), async {
        loop {
            right_connection
                .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
                .await?;
            if let DocLookup::Ready(handle) = right.repo.get_doc(&doc_id).await? {
                break Ok::<_, utils_rs::prelude::eyre::Report>(handle);
            }
            right_connection.sync_keyhive_with_peer(None).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    let title = right_doc
        .with_document_read(|doc| {
            let (value, _) = doc
                .get(automerge::ROOT, "title")
                .expect("title lookup should succeed")
                .expect("title should exist");
            let automerge::Value::Scalar(value) = value else {
                panic!("title should be scalar");
            };
            match value.as_ref() {
                ScalarValue::Str(value) => value.to_string(),
                _ => panic!("title should be a string"),
            }
        })
        .await;
    assert_eq!(title, "ladder rung 1");

    timeout(Duration::from_secs(10), async {
        loop {
            let left_state = left.repo.doc_head_state(doc_id).await?;
            let right_state = right.repo.doc_head_state(doc_id).await?;
            if left_state.sedimentree_heads == right_state.sedimentree_heads
                && left_state.materialized_heads == right_state.materialized_heads
            {
                break Ok::<_, utils_rs::prelude::eyre::Report>(());
            }
            right_connection
                .sync_doc_with_peer(doc_id, Some(Duration::from_secs(5)))
                .await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    assert_materialized_parity(&left, &right, doc_id).await?;
    drop(document);
    left_connection.stop().await?;
    right_connection.stop().await?;
    left.stop().await?;
    right.stop().await?;
    Ok(())
}
