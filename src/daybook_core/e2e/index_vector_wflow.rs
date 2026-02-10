use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_index_vector_query_after_embed_workflow() -> Res<()> {
    let test_cx = crate::e2e::test_cx(utils_rs::function_full!()).await?;

    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            FacetKey::from(WellKnownFacetTag::Note),
            WellKnownFacet::Note(daybook_types::doc::Note {
                mime: "text/plain".to_string(),
                content: "local embedding stack smoke test".to_string(),
            })
            .into(),
        )]
        .into(),
        user_path: None,
    };

    let doc_id = test_cx.drawer_repo.add(new_doc).await?;
    let (_doc, heads) = test_cx
        .drawer_repo
        .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("doc not found after add")?;

    let dispatch_id = test_cx
        .rt
        .dispatch(
            "@daybook/wip",
            "embed-text",
            crate::rt::DispatchArgs::DocFacet {
                doc_id: doc_id.clone(),
                branch_path: daybook_types::doc::BranchPath::from("main"),
                heads,
                facet_key: None,
            },
        )
        .await?;

    test_cx
        .rt
        .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
        .await?;

    let mut got_hit = false;
    for _ in 0..50 {
        let hits = test_cx
            .rt
            .doc_embedding_index_repo
            .query_text("local embedding stack smoke test", 3)
            .await?;
        if hits.iter().any(|hit| hit.doc_id == doc_id) {
            got_hit = true;
            let hit = hits.into_iter().find(|hit| hit.doc_id == doc_id).unwrap();
            assert_eq!(
                hit.facet_key,
                FacetKey::from(WellKnownFacetTag::Embedding).to_string()
            );
            assert!(!hit.heads.0.is_empty());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        got_hit,
        "expected embedded doc to be returned by vector index"
    );

    test_cx.stop().await?;
    Ok(())
}
