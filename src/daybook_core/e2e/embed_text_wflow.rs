use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_embed_text_workflow() -> Res<()> {
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

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let updated_doc = test_cx
        .drawer_repo
        .get_doc_with_facets_at_branch(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("doc not found after embed-text workflow")?;

    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    let note_raw = updated_doc
        .facets
        .get(&note_key)
        .ok_or_eyre("embed-text workflow did not write Note facet")?;
    let note_facet = WellKnownFacet::from_json(note_raw.clone(), WellKnownFacetTag::Note)?;
    let WellKnownFacet::Note(note) = note_facet else {
        eyre::bail!("note facet had unexpected type");
    };

    assert_eq!(note.mime, "text/plain");
    assert!(
        note.content
            .contains("embedding(model=nomic-ai/nomic-embed-text-v1.5"),
        "unexpected note content: {}",
        note.content
    );
    assert!(
        note.content.contains("dims="),
        "expected dims field in note content: {}",
        note.content
    );
    assert!(
        note.content.contains('['),
        "expected vector preview in note content: {}",
        note.content
    );

    test_cx.stop().await?;
    Ok(())
}
