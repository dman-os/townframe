mod interlude {
    pub use utils_rs::prelude::*;
}

mod types;

use crate::interlude::*;

pub fn plug_manifest() -> daybook_types::manifest::PlugManifest {
    use daybook_types::manifest::{
        FacetManifest, FacetReferenceKind, FacetReferenceManifest, PlugManifest,
    };
    use schemars::schema_for;
    use crate::types::*;

    PlugManifest {
        namespace: "daybook".into(),
        name: "dayledger".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Dayledger".into(),
        desc: "Personal accounting plug for daybook".into(),
        facets: vec![
            FacetManifest {
                key_tag: DayledgerFacetTag::Claim.as_str().into(),
                value_schema: schema_for!(Claim),
                display_config: Default::default(),
                references: vec![FacetReferenceManifest {
                    reference_kind: FacetReferenceKind::UrlFacet,
                    json_path: "/evidenceRefs".into(),
                    at_commit_json_path: None,
                }],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::Txn.as_str().into(),
                value_schema: schema_for!(Txn),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::Account.as_str().into(),
                value_schema: schema_for!(Account),
                display_config: Default::default(),
                references: vec![FacetReferenceManifest {
                    reference_kind: FacetReferenceKind::UrlFacet,
                    json_path: "/parentAccountRef".into(),
                    at_commit_json_path: None,
                }],
            },
            FacetManifest {
                key_tag: DayledgerFacetTag::LedgerMeta.as_str().into(),
                value_schema: schema_for!(LedgerMeta),
                display_config: Default::default(),
                references: vec![
                    FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "/accountRefs".into(),
                        at_commit_json_path: None,
                    },
                    FacetReferenceManifest {
                        reference_kind: FacetReferenceKind::UrlFacet,
                        json_path: "/transactionRefs".into(),
                        at_commit_json_path: None,
                    },
                ],
            },
        ],
        local_states: HashMap::new(),
        dependencies: HashMap::new(),
        routines: HashMap::new(),
        wflow_bundles: HashMap::new(),
        commands: HashMap::new(),
        processors: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use automerge::transaction::{Transactable, Transaction};

    fn rss_bytes_linux() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
        let kb = line
            .split_whitespace()
            .nth(1)
            .and_then(|part| part.parse::<u64>().ok())?;
        Some(kb * 1024)
    }

    fn insert_txn(tx: &mut Transaction<'_>, txns_obj: &automerge::ObjId, idx: usize) {
        let txn_id = format!("txn_{idx:05}");
        let txn_obj = tx
            .put_object(txns_obj, txn_id.as_str(), automerge::ObjType::Map)
            .unwrap();
        tx.put(&txn_obj, "txnId", txn_id).unwrap();
        tx.put(&txn_obj, "ts", "2026-03-27T18:45:00Z").unwrap();
        tx.put(&txn_obj, "status", "Pending").unwrap();
        tx.put(&txn_obj, "payee", "KFC").unwrap();
        tx.put(&txn_obj, "note", "dinner").unwrap();

        let balance_obj = tx
            .put_object(&txn_obj, "balance", automerge::ObjType::Map)
            .unwrap();
        tx.put(&balance_obj, "status", "Balanced").unwrap();
        tx.put(&balance_obj, "precisionDp", 2_i64).unwrap();
        let totals_obj = tx
            .put_object(&balance_obj, "commodityTotals", automerge::ObjType::List)
            .unwrap();
        let total0 = tx
            .insert_object(&totals_obj, 0, automerge::ObjType::Map)
            .unwrap();
        tx.put(&total0, "commodity", "USD").unwrap();
        tx.put(&total0, "totalDecimal", "0.00").unwrap();

        let claim_refs = tx
            .put_object(&txn_obj, "claimRefs", automerge::ObjType::List)
            .unwrap();
        let claim0 = tx
            .insert_object(&claim_refs, 0, automerge::ObjType::Map)
            .unwrap();
        tx.put(
            &claim0,
            "ref",
            format!("db+facet:///doc_receipt_{idx}/org.example.dayledger.claim/main"),
        )
        .unwrap();
        tx.put_object(&claim0, "heads", automerge::ObjType::List)
            .unwrap();
        let claim1 = tx
            .insert_object(&claim_refs, 1, automerge::ObjType::Map)
            .unwrap();
        tx.put(
            &claim1,
            "ref",
            format!("db+facet:///doc_sms_{idx}/org.example.dayledger.claim/main"),
        )
        .unwrap();
        tx.put_object(&claim1, "heads", automerge::ObjType::List)
            .unwrap();

        let postings = tx
            .put_object(&txn_obj, "postings", automerge::ObjType::List)
            .unwrap();

        let posting0 = tx
            .insert_object(&postings, 0, automerge::ObjType::Map)
            .unwrap();
        tx.put(&posting0, "accountId", "B7tQ2mPk9sLw4HdVc8NfYz")
            .unwrap();
        let amount0 = tx
            .put_object(&posting0, "amount", automerge::ObjType::Map)
            .unwrap();
        tx.put(&amount0, "decimal", "24.50").unwrap();
        tx.put(&amount0, "commodity", "USD").unwrap();
        tx.put(&posting0, "type", "Regular").unwrap();

        let posting1 = tx
            .insert_object(&postings, 1, automerge::ObjType::Map)
            .unwrap();
        tx.put(&posting1, "accountId", "3M9v7Kx2nQfR8cLdW1pTjB")
            .unwrap();
        let amount1 = tx
            .put_object(&posting1, "amount", automerge::ObjType::Map)
            .unwrap();
        tx.put(&amount1, "decimal", "-24.50").unwrap();
        tx.put(&amount1, "commodity", "USD").unwrap();
        tx.put(&posting1, "type", "Regular").unwrap();
    }

    #[test]
    fn automerge_10k_txns_save_size_and_rss() {
        const ENTRY_COUNT: usize = 10_000;

        let rss_before = rss_bytes_linux();

        let mut doc = automerge::Automerge::new();
        {
            let mut tx = doc.transaction();
            let txns_obj = tx
                .put_object(automerge::ROOT, "txns", automerge::ObjType::Map)
                .unwrap();
            for idx in 0..ENTRY_COUNT {
                insert_txn(&mut tx, &txns_obj, idx);
            }
            tx.commit();
        }

        let rss_after = rss_bytes_linux();
        let saved = doc.save();
        let saved_len = saved.len();

        let rss_delta = match (rss_before, rss_after) {
            (Some(before), Some(after)) => Some(after.saturating_sub(before)),
            _ => None,
        };

        eprintln!("dayledger automerge benchmark");
        eprintln!("entries: {ENTRY_COUNT}");
        eprintln!("save_bytes: {saved_len}");
        eprintln!("rss_before_bytes: {:?}", rss_before);
        eprintln!("rss_after_bytes: {:?}", rss_after);
        eprintln!("rss_delta_bytes: {:?}", rss_delta);

        assert!(saved_len > 0);
    }
}
