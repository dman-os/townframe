use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_embed_text_workflow() -> Res<()> {
    let test_cx = crate::e2e::test_cx_with_options(
        utils_rs::function_full!(),
        crate::e2e::DaybookTestCxOptions {
            provision_mltools_models: true,
        },
    )
    .await?;

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
    // Auto-triage also dispatches doc processors for Note changes.
    // Drain those jobs so this test's explicit embed-text dispatch is deterministic.
    test_cx._wait_until_no_active_jobs(120).await?;

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

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let updated_doc = test_cx
        .drawer_repo
        .get_doc_with_facets_at_branch(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("doc not found after embed-text workflow")?;

    let embedding_key = FacetKey::from(WellKnownFacetTag::Embedding);
    let embedding_raw = updated_doc
        .facets
        .get(&embedding_key)
        .ok_or_eyre("embed-text workflow did not write Embedding facet")?;
    let embedding_facet =
        WellKnownFacet::from_json(embedding_raw.clone(), WellKnownFacetTag::Embedding)?;
    let WellKnownFacet::Embedding(embedding) = embedding_facet else {
        eyre::bail!("embedding facet had unexpected type");
    };

    assert_eq!(embedding.model_tag, "nomic-ai/nomic-embed-text-v1.5");
    assert_eq!(embedding.dtype, daybook_types::doc::EmbeddingDtype::F32);
    assert_eq!(embedding.compression, None);
    assert_eq!(embedding.dim, 768);
    assert_eq!(embedding.vector.len(), 768 * 4);
    assert_eq!(
        embedding.facet_ref.scheme(),
        daybook_types::url::FACET_SCHEME
    );
    assert_eq!(
        embedding.facet_ref.path(),
        "/self/org.example.daybook.note/main"
    );

    test_cx.stop().await?;
    Ok(())
}
