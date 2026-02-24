use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, Blob, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_image_label_fallback_nomic_pipeline() -> Res<()> {
    let test_cx = crate::e2e::test_cx_with_options(
        utils_rs::function_full!(),
        crate::e2e::DaybookTestCxOptions {
            provision_mltools_models: true,
        },
    )
    .await?;

    let image_path = std::path::Path::new("/tmp/sample.jpg");
    let image_bytes = std::fs::read(&image_path)
        .wrap_err_with(|| format!("error reading {}", image_path.display()))?;
    let blob_hash = test_cx.rt.blobs_repo.put(&image_bytes).await?;

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
    let label_key = FacetKey::from(WellKnownFacetTag::LabelGeneric);
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
            if doc.facets.contains_key(&embedding_key) && doc.facets.contains_key(&label_key) {
                updated_doc = Some(doc);
                break;
            }
            info!(
                was_embedded = doc.facets.contains_key(&embedding_key),
                "retrying XXX"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let updated_doc =
        updated_doc.ok_or_eyre("doc not found with embedding+label after image-label pipeline")?;
    info!(?updated_doc, "XXX");

    let embedding_raw = updated_doc
        .facets
        .get(&embedding_key)
        .ok_or_eyre("image pipeline did not write Embedding facet")?;
    let embedding_facet =
        WellKnownFacet::from_json(embedding_raw.clone(), WellKnownFacetTag::Embedding)?;
    let WellKnownFacet::Embedding(embedding) = embedding_facet else {
        eyre::bail!("embedding facet had unexpected type");
    };
    assert_eq!(embedding.model_tag, "nomic-ai/nomic-embed-vision-v1.5");
    assert_eq!(embedding.dim, 768);
    assert_eq!(embedding.dtype, daybook_types::doc::EmbeddingDtype::F32);
    assert_eq!(embedding.compression, None);
    let parsed_ref = daybook_types::url::parse_facet_ref(&embedding.facet_ref)?;
    assert_eq!(
        parsed_ref.facet_key,
        FacetKey::from(WellKnownFacetTag::Blob)
    );

    let label_raw = updated_doc
        .facets
        .get(&label_key)
        .ok_or_eyre("image classifier did not write LabelGeneric facet")?;
    let label_facet =
        WellKnownFacet::from_json(label_raw.clone(), WellKnownFacetTag::LabelGeneric)?;
    let WellKnownFacet::LabelGeneric(label) = label_facet else {
        eyre::bail!("label facet had unexpected type");
    };
    assert_eq!(label, "receipt-image");

    test_cx._wait_until_no_active_jobs(120).await?;

    test_cx.stop().await?;
    Ok(())
}
