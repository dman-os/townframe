use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use utils_rs::prelude::*;

use daybook_types::doc::{AddDocArgs, Doc, DocPatch, FacetKey, WellKnownFacet, WellKnownFacetTag};

fn hledger_note(content: impl Into<String>) -> daybook_types::doc::FacetRaw {
    WellKnownFacet::Note(daybook_types::doc::Note {
        mime: "text/x-hledger-journal".to_string(),
        content: content.into(),
    })
    .into()
}

async fn wait_for_claims(
    test_cx: &daybook_core::test_support::DaybookTestContext,
    doc_id: &daybook_types::doc::DocId,
    expected_count: usize,
) -> Res<Arc<Doc>> {
    let claim_tag =
        daybook_types::doc::FacetKey::from(crate::types::DayledgerFacetTag::Claim.as_str()).tag;
    for _ in 0..600 {
        if let Some(doc) = test_cx
            .drawer_repo
            .get_doc_with_facets_at_branch(
                doc_id,
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
            if claim_count == expected_count {
                return Ok(doc);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    eyre::bail!("doc did not reach {expected_count} claim facets after parse-hledger workflow")
}

fn claim_facets_by_txn_index(
    doc: &Doc,
) -> Res<BTreeMap<usize, (FacetKey, crate::types::Claim, crate::types::HledgerTxnDeets)>> {
    let claim_tag =
        daybook_types::doc::FacetKey::from(crate::types::DayledgerFacetTag::Claim.as_str()).tag;
    let mut out = BTreeMap::new();
    for (facet_key, raw) in &doc.facets {
        if facet_key.tag != claim_tag {
            continue;
        }
        let claim: crate::types::Claim = serde_json::from_value(raw.clone())?;
        let deets: crate::types::HledgerTxnDeets = serde_json::from_value(claim.deets.clone())?;
        if out
            .insert(deets.txn_index, (facet_key.clone(), claim, deets))
            .is_some()
        {
            eyre::bail!("duplicate claim txn index");
        }
    }
    Ok(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parse_hledger_writes_one_claim_per_transaction() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx(utils_rs::function_full!()).await?;
    super::common::import_dayledger_oci(&test_cx).await?;

    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            FacetKey::from(WellKnownFacetTag::Note),
            hledger_note(include_str!("../hledger/tests/fixtures/sample.journal")),
        )]
        .into(),
        user_path: None,
    };

    let doc_id = test_cx.drawer_repo.add(new_doc).await?;
    let updated_doc = wait_for_claims(&test_cx, &doc_id, 5).await?;
    let claim_facets = claim_facets_by_txn_index(&updated_doc)?;

    assert_eq!(
        claim_facets.len(),
        5,
        "expected one claim facet per transaction"
    );

    let mut seen_ids = BTreeSet::new();
    let mut seen_indexes = BTreeSet::new();
    for (txn_index, (facet_key, claim, deets)) in &claim_facets {
        assert!(
            seen_ids.insert(facet_key.id.clone()),
            "duplicate claim facet id"
        );
        assert_eq!(*txn_index, deets.txn_index);
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

#[tokio::test(flavor = "multi_thread")]
async fn test_parse_hledger_reparse_preserves_claim_ids_when_transaction_inserted() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx(utils_rs::function_full!()).await?;
    super::common::import_dayledger_oci(&test_cx).await?;

    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [(
            note_key.clone(),
            hledger_note(include_str!("../hledger/tests/fixtures/sample.journal")),
        )]
        .into(),
        user_path: None,
    };

    let doc_id = test_cx.drawer_repo.add(new_doc).await?;
    let first_doc = wait_for_claims(&test_cx, &doc_id, 5).await?;
    let first_claims = claim_facets_by_txn_index(&first_doc)?;
    test_cx._wait_until_no_active_jobs(120).await?;

    let inserted_journal = format!(
        "2007/12/31 opening\n    assets:cash  $1\n    income:gifts\n\n{}",
        include_str!("../hledger/tests/fixtures/sample.journal")
    );
    test_cx
        .drawer_repo
        .update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(note_key, hledger_note(inserted_journal))].into(),
                facets_remove: vec![],
                user_path: None,
            },
            daybook_types::doc::BranchPath::from("main"),
            None,
        )
        .await?;

    let second_doc = wait_for_claims(&test_cx, &doc_id, 6).await?;
    let second_claims = claim_facets_by_txn_index(&second_doc)?;

    for old_index in 0..5 {
        let first_id = &first_claims
            .get(&old_index)
            .ok_or_eyre("missing first claim")?
            .0
            .id;
        let second_id = &second_claims
            .get(&(old_index + 1))
            .ok_or_eyre("missing shifted claim")?
            .0
            .id;
        assert_eq!(
            second_id, first_id,
            "expected shifted transaction {old_index} to preserve claim id"
        );
    }
    assert_ne!(
        second_claims
            .get(&0)
            .ok_or_eyre("missing inserted claim")?
            .0
            .id,
        first_claims.get(&0).ok_or_eyre("missing first claim")?.0.id
    );

    test_cx._wait_until_no_active_jobs(120).await?;
    test_cx.stop().await?;
    Ok(())
}
