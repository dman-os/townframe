use utils_rs::prelude::*;

use daybook_types::doc::{AddDocArgs, Blob, FacetKey, WellKnownFacet, WellKnownFacetTag};

const PROPOSAL_SET_CONFIG_FACET_ID: &str = "label-candidates";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "real multimodal e2e; slow and environment-dependent"]
async fn test_learned_image_label_proposals_receipt_twice_prints_labels() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx_with_options(
        utils_rs::function_full!(),
        daybook_core::test_support::DaybookTestCxOptions {
            provision_mltools_models: true,
        },
    )
    .await?;
    super::common::import_plabels_oci(&test_cx).await?;

    let image_bytes = include_bytes!("../../daybook_core/e2e/sample-receipt.jpg");

    let _doc_a = add_blob_image_doc(&test_cx, image_bytes).await?;
    let proposal_set_after_first = wait_for_proposal_set(&test_cx, 300).await?;
    println!(
        "learned-image-label-proposals after first receipt doc: {:#?}",
        proposal_set_after_first
    );
    assert!(
        !proposal_set_after_first.labels.is_empty(),
        "expected at least one learned proposal label after first receipt insert"
    );
    assert_proposal_set_invariants(&proposal_set_after_first)?;

    let _doc_b = add_blob_image_doc(&test_cx, image_bytes).await?;
    let proposal_set_after_second = wait_for_proposal_set(&test_cx, 300).await?;
    println!(
        "learned-image-label-proposals after second receipt doc: {:#?}",
        proposal_set_after_second
    );
    assert!(
        !proposal_set_after_second.labels.is_empty(),
        "expected at least one learned proposal label after second receipt insert"
    );
    assert_proposal_set_invariants(&proposal_set_after_second)?;

    assert_eq!(
        proposal_set_after_second.labels.len(),
        proposal_set_after_first.labels.len(),
        "expected deduped proposal label count to remain stable after inserting the same image twice"
    );

    test_cx._wait_until_no_active_jobs(300).await?;
    test_cx.stop().await?;
    Ok(())
}

fn assert_proposal_set_invariants(set: &crate::types::PseudoLabelCandidatesFacet) -> Res<()> {
    let mut seen_labels = std::collections::BTreeSet::new();

    for label in &set.labels {
        eyre::ensure!(
            !label.label.is_empty(),
            "proposal label must not be empty: {label:#?}"
        );
        eyre::ensure!(
            is_snake_case_label(&label.label),
            "proposal label must be snake_case, got {:?}",
            label.label
        );
        eyre::ensure!(
            seen_labels.insert(label.label.clone()),
            "duplicate proposal label entry found: {:?}",
            label.label
        );

        eyre::ensure!(
            !label.prompts.is_empty(),
            "proposal label {:?} must have positive prompts",
            label.label
        );
        eyre::ensure!(
            !label.negative_prompts.is_empty(),
            "proposal label {:?} must have negative prompts",
            label.label
        );

        let mut seen_positive = std::collections::BTreeSet::new();
        for prompt in &label.prompts {
            eyre::ensure!(
                !prompt.trim().is_empty(),
                "proposal label {:?} has empty positive prompt",
                label.label
            );
            eyre::ensure!(
                seen_positive.insert(prompt.clone()),
                "proposal label {:?} has duplicate positive prompt {:?}",
                label.label,
                prompt
            );
        }

        let mut seen_negative = std::collections::BTreeSet::new();
        for prompt in &label.negative_prompts {
            eyre::ensure!(
                !prompt.trim().is_empty(),
                "proposal label {:?} has empty negative prompt",
                label.label
            );
            eyre::ensure!(
                seen_negative.insert(prompt.clone()),
                "proposal label {:?} has duplicate negative prompt {:?}",
                label.label,
                prompt
            );
        }
    }

    Ok(())
}

fn is_snake_case_label(label: &str) -> bool {
    !label.is_empty()
        && !label.starts_with('_')
        && !label.ends_with('_')
        && !label.contains("__")
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

async fn add_blob_image_doc(
    test_cx: &daybook_core::test_support::DaybookTestContext,
    image_bytes: &[u8],
) -> Res<daybook_types::doc::DocId> {
    let blob_hash = test_cx.rt.blobs_repo.put(image_bytes).await?;
    let blob_facet = Blob {
        mime: "image/jpeg".to_string(),
        length_octets: image_bytes.len() as u64,
        digest: blob_hash.clone(),
        inline: None,
        urls: Some(vec![format!("db+blob:///{blob_hash}")]),
    };

    test_cx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::Blob),
                WellKnownFacet::Blob(blob_facet).into(),
            )]
            .into(),
            user_path: None,
        })
        .await
        .map_err(Into::into)
}

async fn wait_for_proposal_set(
    test_cx: &daybook_core::test_support::DaybookTestContext,
    timeout_secs: u64,
) -> Res<crate::types::PseudoLabelCandidatesFacet> {
    let config_doc_id = test_cx
        ._config_repo
        .get_or_init_global_props_doc_id(&test_cx.drawer_repo)
        .await?;
    let proposal_set_key = crate::types::pseudo_label_candidates_key(PROPOSAL_SET_CONFIG_FACET_ID);

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    loop {
        if start.elapsed() > timeout {
            eyre::bail!(
                "timeout waiting for learned proposal set facet '{}' after {:?}",
                proposal_set_key,
                start.elapsed()
            );
        }

        if let Some(doc) = test_cx
            .drawer_repo
            .get_doc_with_facets_at_branch(
                &config_doc_id,
                &daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?
        {
            if let Some(raw) = doc.facets.get(&proposal_set_key) {
                let value: crate::types::PseudoLabelCandidatesFacet =
                    serde_json::from_value(raw.clone())?;
                if !value.labels.is_empty() {
                    return Ok(value);
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
