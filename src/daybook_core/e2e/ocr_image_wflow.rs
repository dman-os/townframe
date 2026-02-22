use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, Blob, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_ocr_image_workflow() -> Res<()> {
    let test_cx = crate::e2e::test_cx_with_options(
        utils_rs::function_full!(),
        crate::e2e::DaybookTestCxOptions {
            provision_mltools_models: true,
        },
    )
    .await?;

    let image_bytes = include_bytes!("./sample.jpg");
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

    let mut dispatch_id: Option<String> = None;
    for _ in 0..600 {
        let dispatches = test_cx.dispatch_repo.list().await;
        if let Some((found_dispatch_id, _dispatch)) = dispatches.iter().find(|(_, dispatch)| {
            matches!(
                &dispatch.deets,
                crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == "ocr-image"
            )
        }) {
            dispatch_id = Some(found_dispatch_id.clone());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let dispatch_id = dispatch_id.ok_or_eyre("ocr-image dispatch not found")?;

    test_cx
        .rt
        .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let updated_doc = test_cx
        .drawer_repo
        .get_doc_with_facets_at_branch(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("doc not found")?;

    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    let note_raw = updated_doc
        .facets
        .get(&note_key)
        .ok_or_eyre("OCR workflow did not write Note facet")?;

    let note_facet = WellKnownFacet::from_json(note_raw.clone(), WellKnownFacetTag::Note)?;
    let WellKnownFacet::Note(note) = note_facet else {
        eyre::bail!("note facet had unexpected type");
    };

    assert_eq!(note.mime, "text/plain");

    test_cx.stop().await?;
    Ok(())
}
