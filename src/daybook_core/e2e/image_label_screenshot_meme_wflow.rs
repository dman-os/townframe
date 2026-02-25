use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, Blob, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_image_label_fallback_multi_label_screenshot_meme() -> Res<()> {
    let test_cx = crate::e2e::test_cx_with_options(
        utils_rs::function_full!(),
        crate::e2e::DaybookTestCxOptions {
            provision_mltools_models: true,
        },
    )
    .await?;

    let image_bytes = include_bytes!("./sample-screenshot-meme.jpg");
    let blob_hash = test_cx.rt.blobs_repo.put(image_bytes).await?;

    let blob_facet = Blob {
        mime: "image/jpeg".to_string(),
        length_octets: image_bytes.len() as u64,
        digest: blob_hash.clone(),
        inline: None,
        urls: Some(vec![format!("db+blob:///{blob_hash}")]),
    };

    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            FacetKey::from(WellKnownFacetTag::Blob),
            WellKnownFacet::Blob(blob_facet).into(),
        )]
        .into(),
        user_path: None,
    };

    let doc_id = test_cx.drawer_repo.add(new_doc).await?;

    let embedding_key = FacetKey::from(WellKnownFacetTag::Embedding);
    let pseudo_label_key = FacetKey::from(WellKnownFacetTag::PseudoLabel);
    let mut updated_doc = None;
    for _ in 0..1200 {
        if let Some(doc) = test_cx
            .drawer_repo
            .get_doc_with_facets_at_branch(
                &doc_id,
                &daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?
        {
            if doc.facets.contains_key(&embedding_key) && doc.facets.contains_key(&pseudo_label_key)
            {
                updated_doc = Some(doc);
                break;
            }
            info!(was_embedded = doc.facets.contains_key(&embedding_key), "XXX");
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let updated_doc = updated_doc.ok_or_eyre(
        "doc not found with embedding+pseudo-label after screenshot meme image-label pipeline",
    )?;

    let pseudo_label_raw = updated_doc
        .facets
        .get(&pseudo_label_key)
        .ok_or_eyre("image classifier did not write PseudoLabel facet")?;
    let pseudo_label_facet =
        WellKnownFacet::from_json(pseudo_label_raw.clone(), WellKnownFacetTag::PseudoLabel)?;
    let WellKnownFacet::PseudoLabel(labels) = pseudo_label_facet else {
        eyre::bail!("pseudo-label facet had unexpected type");
    };

    assert!(
        labels.iter().any(|label| label == "twitter-screenshot"),
        "expected twitter-screenshot in pseudo labels, got {labels:?}"
    );
    assert!(
        labels.iter().any(|label| label == "minecraft"),
        "expected minecraft in pseudo labels, got {labels:?}"
    );

    test_cx._wait_until_no_active_jobs(120).await?;
    test_cx.stop().await?;
    Ok(())
}
