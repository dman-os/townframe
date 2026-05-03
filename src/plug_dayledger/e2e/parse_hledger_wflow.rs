use std::collections::BTreeSet;

use utils_rs::prelude::*;

use daybook_types::doc::{AddDocArgs, FacetKey, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_parse_hledger_writes_one_claim_per_transaction() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx(utils_rs::function_full!()).await?;
    super::common::import_dayledger_oci(&test_cx).await?;

    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            FacetKey::from(WellKnownFacetTag::Note),
            WellKnownFacet::Note(daybook_types::doc::Note {
                mime: "text/x-hledger-journal".to_string(),
                content: include_str!("../hledger/tests/fixtures/sample.journal").to_string(),
            })
            .into(),
        )]
        .into(),
        user_path: None,
    };

    let doc_id = test_cx.drawer_repo.add(new_doc).await?;
    let claim_tag =
        daybook_types::doc::FacetKey::from(crate::types::DayledgerFacetTag::Claim.as_str()).tag;

    let mut updated_doc = None;
    for _ in 0..600 {
        if let Some(doc) = test_cx
            .drawer_repo
            .get_doc_with_facets_at_branch(
                &doc_id,
                &daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?
        {
            let claim_count = doc
                .facets
                .keys()
                .filter(|facet_key| facet_key.tag == claim_tag)
                .count();
            if claim_count == 5 {
                updated_doc = Some(doc);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let updated_doc = updated_doc.ok_or_eyre("doc not found after parse-hledger workflow")?;

    let mut claim_facets = Vec::new();
    for (facet_key, raw) in &updated_doc.facets {
        if facet_key.tag != claim_tag {
            continue;
        }
        let claim: crate::types::Claim = serde_json::from_value(raw.clone())?;
        let deets: crate::types::HledgerTxnDeets = serde_json::from_value(claim.deets.clone())?;
        claim_facets.push((facet_key.clone(), claim, deets));
    }

    assert_eq!(
        claim_facets.len(),
        5,
        "expected one claim facet per transaction"
    );

    let mut seen_ids = BTreeSet::new();
    let mut seen_indexes = BTreeSet::new();
    for (facet_key, claim, deets) in &claim_facets {
        assert!(
            seen_ids.insert(facet_key.id.clone()),
            "duplicate claim facet id"
        );
        assert_eq!(claim.deets_kind, "hledger");
        assert_eq!(
            claim.src_ref.r#ref.scheme(),
            daybook_types::url::FACET_SCHEME
        );
        assert!(
            !claim.src_ref.heads.is_empty(),
            "expected source heads to be recorded"
        );
        assert!(
            claim.src_refs.is_empty(),
            "first parse should not carry prior src refs"
        );
        assert!(seen_indexes.insert(deets.txn_index), "duplicate txn index");
        let expected_posting_count = match deets.txn_index {
            0 | 1 | 2 | 4 => 2,
            3 => 3,
            other => panic!("unexpected txn index {other}"),
        };
        assert_eq!(
            claim.posting_hints.len(),
            expected_posting_count,
            "unexpected posting count for sample txn {}",
            deets.txn_index
        );
    }

    let expected_indexes = [0usize, 1, 2, 3, 4].into_iter().collect();
    assert_eq!(seen_indexes, expected_indexes);

    test_cx._wait_until_no_active_jobs(120).await?;
    test_cx.stop().await?;
    Ok(())
}
