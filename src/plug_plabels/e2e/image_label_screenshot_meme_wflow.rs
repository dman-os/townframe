use utils_rs::prelude::*;

use daybook_types::doc::{AddDocArgs, Blob, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_image_label_fallback_multi_label_screenshot_meme() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx_with_options(
        utils_rs::function_full!(),
        daybook_core::test_support::DaybookTestCxOptions {
            provision_mltools_models: true,
        },
    )
    .await?;
    super::common::import_plabels_oci(&test_cx).await?;

    let image_bytes = include_bytes!("../../daybook_core/e2e/sample-screenshot-meme.jpg");
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
    let pseudo_label_key = crate::types::pseudo_label_key();
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
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let updated_doc = updated_doc.ok_or_eyre(
        "doc not found with embedding+pseudo-label after screenshot meme image-label pipeline",
    )?;

    let pseudo_label_raw = updated_doc
        .facets
        .get(&pseudo_label_key)
        .ok_or_eyre("image classifier did not write pseudo label facet")?;
    let labels: crate::types::PseudoLabel = serde_json::from_value(pseudo_label_raw.clone())?;
    assert_eq!(labels.algorithm_tag, "label-image/embed-gauntlet-nomic-v1");
    assert!(!labels.source_ref.as_str().is_empty());
    assert!(!labels.candidate_set_ref.as_str().is_empty());

    assert!(
        labels
            .labels
            .iter()
            .any(|label| label.label == "twitter-screenshot"),
        "expected twitter-screenshot in pseudo labels, got {labels:?}"
    );
    assert!(
        labels.labels.iter().any(|label| label.label == "minecraft"),
        "expected minecraft in pseudo labels, got {labels:?}"
    );

    test_cx._wait_until_no_active_jobs(120).await?;
    test_cx.stop().await?;
    Ok(())
}
