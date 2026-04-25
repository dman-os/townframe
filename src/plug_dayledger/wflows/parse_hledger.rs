use super::super::*;
use crate::interlude::*;
use crate::types::{Claim, ClaimPostingHint, HledgerTxnDeets, PostingSign};
use wflow_sdk::WflowCtx;

pub fn run(_cx: &mut WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    // Read the Note facet from ro_facet_tokens (the input/trigger)
    let note_facet_key_str =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_token =
        tuple_list_get(&args.ro_facet_tokens, &note_facet_key_str).ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("note facet token not found in ro_facet_tokens"))
        })?;

    let note_raw = note_token.get();
    let note_json: daybook_types::doc::FacetRaw =
        serde_json::from_str(&note_raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error parsing note facet json: {err}"))
        })?;
    let note = match WellKnownFacet::from_json(note_json, WellKnownFacetTag::Note)
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(err.wrap_err("input facet is not note")))?
    {
        WellKnownFacet::Note(value) => value,
        _ => unreachable!("expected WellKnownFacet::Note here for parse-hledger"),
    };

    let transactions = crate::hledger::parse::journal::parse_journal(&note.content)
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("hledger parse error: {err:?}")))?;

    let note_heads = note_token.heads();
    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note);
    let note_url_str = format!(
        "db+facet:///{}/{}/{}",
        args.doc_id, note_facet_key.tag, note_facet_key.id
    );
    let note_url: url::Url = note_url_str
        .parse()
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("invalid facet url: {err}")))?;
    let src_ref = daybook_types::doc::FacetRef {
        r#ref: note_url,
        heads: note_heads,
    };

    // Read existing claims from the rw claim token (claim/main)
    let claim_tag_str = crate::types::DayledgerFacetTag::Claim.as_str();
    let claim_facet_key_str = daybook_types::doc::FacetKey::from(claim_tag_str).to_string();
    let existing_claims: HashMap<String, Claim> =
        if let Some(claim_token) = tuple_list_get(&args.rw_facet_tokens, &claim_facet_key_str) {
            if claim_token.exists() {
                let claim_raw = claim_token.get();
                serde_json::from_str(&claim_raw).unwrap_or_default()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

    let mut new_claims: HashMap<String, Claim> = HashMap::new();

    for (txn_index, txn) in transactions.iter().enumerate() {
        let (claim_id, claim) = match_or_create_claim(txn, txn_index, &existing_claims, &src_ref);
        new_claims.insert(claim_id, claim);
    }

    // Write updated claims back
    if let Some(claim_token) = tuple_list_get(&args.rw_facet_tokens, &claim_facet_key_str) {
        let claims_json = serde_json::to_string(&new_claims).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("serde error serializing claims: {err}"))
        })?;
        claim_token.update(&claims_json).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("update claim facet error: {err:?}"))
        })?;
    }

    Ok(())
}

fn match_or_create_claim(
    txn: &crate::hledger::types::Transaction,
    txn_index: usize,
    existing: &HashMap<String, Claim>,
    src_ref: &daybook_types::doc::FacetRef,
) -> (String, Claim) {
    // 1. Try to find existing claim with matching txn_index in deets
    for (key, claim) in existing {
        if let Ok(deets) = serde_json::from_value::<HledgerTxnDeets>(claim.deets.clone()) {
            if deets.txn_index == txn_index {
                let updated = build_claim(txn, txn_index, src_ref, Some(claim));
                return (key.clone(), updated);
            }
        }
    }

    // 2. Try to find existing claim with matching content hash
    let content_hash = hash_txn(txn);
    for (key, claim) in existing {
        if let Ok(deets) = serde_json::from_value::<HledgerTxnDeets>(claim.deets.clone()) {
            if deets.content_hash == content_hash {
                let updated = build_claim(txn, txn_index, src_ref, Some(claim));
                return (key.clone(), updated);
            }
        }
    }

    // 3. Create new claim with content-hash-based ID
    let new_id = hash_txn(txn);
    let new_claim = build_claim(txn, txn_index, src_ref, None);
    (new_id, new_claim)
}

fn build_claim(
    txn: &crate::hledger::types::Transaction,
    txn_index: usize,
    src_ref: &daybook_types::doc::FacetRef,
    existing: Option<&Claim>,
) -> Claim {
    let posting_hints: Vec<ClaimPostingHint> = txn
        .postings
        .iter()
        .map(|p| ClaimPostingHint {
            account_hint: p.account.clone(),
            amount: crate::types::Amount {
                decimal: p.amount.quantity.clone(),
                commodity: p.amount.commodity.clone(),
            },
            sign: match p.posting_type {
                crate::hledger::types::PostingType::Regular => PostingSign::Debit,
                crate::hledger::types::PostingType::Virtual => PostingSign::Credit,
                crate::hledger::types::PostingType::BalancedVirtual => PostingSign::Debit,
            },
            hint_type: match p.posting_type {
                crate::hledger::types::PostingType::Regular => None,
                crate::hledger::types::PostingType::Virtual => Some("virtual".into()),
                crate::hledger::types::PostingType::BalancedVirtual => {
                    Some("balancedVirtual".into())
                }
            },
        })
        .collect();

    let deets = HledgerTxnDeets {
        txn_index,
        code: txn.code.clone(),
        tags: txn.tags.clone(),
        posting_comments: txn
            .postings
            .iter()
            .map(|p| {
                let c = p.comment.clone();
                if c.is_empty() {
                    None
                } else {
                    Some(c)
                }
            })
            .collect(),
        content_hash: hash_txn(txn),
    };

    Claim {
        ts: txn.date.to_string(),
        posting_hints,
        src_ref: src_ref.clone(),
        src_refs: existing.map(|c| c.src_refs.clone()).unwrap_or_default(),
        deets_kind: "hledger".into(),
        deets: serde_json::to_value(deets).unwrap(),
    }
}

// FIXME: use bs58 from utils_rs and also consider hash_obj from there
fn hash_txn(txn: &crate::hledger::types::Transaction) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    txn.date.to_string().hash(&mut hasher);
    txn.description.hash(&mut hasher);
    txn.code.hash(&mut hasher);
    for p in &txn.postings {
        p.account.hash(&mut hasher);
        p.amount.quantity.hash(&mut hasher);
        p.amount.commodity.hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}
