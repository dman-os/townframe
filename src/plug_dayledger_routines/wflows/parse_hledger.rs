use crate::interlude::*;
use plug_dayledger_manifest::hledger;
use plug_dayledger_manifest::hledger::projection::compute_txn_balance;
use plug_dayledger_manifest::hledger::projection::infer_account_type;
use plug_dayledger_manifest::hledger::projection::map_posting_type;
use plug_dayledger_manifest::hledger::projection::map_txn_status;
use plug_dayledger_manifest::types;
use types::Claim;
use wflow_sdk::WflowCtx;

pub async fn run(_cx: &mut WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::WellKnownFacet;
    use daybook_types::doc::WellKnownFacetTag;

    let args = facet_routine::get_args();

    let note_facet_key_str =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_token = args
        .primary_doc
        .facets
        .iter()
        .find(|tag| tag.key() == note_facet_key_str && tag.rights().contains(FacetRights::READ))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("note facet token with read rights not found"))
        })?;

    let note_raw = note_token.get().await.map_err(|err| {
        wflow_sdk::JobErrorX::Terminal(ferr!("error reading note facet: {err:?}"))
    })?;
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

    let transactions = hledger::parse::journal::parse_journal(&note.content)
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("hledger parse error: {err:?}")))?;

    let note_heads = note_token.heads().map_err(|err| {
        wflow_sdk::JobErrorX::Terminal(ferr!("error reading note heads: {err:?}"))
    })?;
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

    let claim_tag_str = types::DayledgerFacetTag::Claim.as_str();
    let claim_tag = daybook_types::doc::FacetKey::from(claim_tag_str).tag;

    let claim_tag_token = args
        .primary_doc
        .tags
        .iter()
        .find(|tag| tag.tag() == claim_tag_str && tag.rights().contains(FacetRights::CREATE))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("claim tag token with create rights not found"))
        })?;

    let mut existing_claims: HashMap<String, Claim> = HashMap::new();
    for claim_token in args.primary_doc.facets.iter() {
        let facet_key = daybook_types::doc::FacetKey::from(claim_token.key().as_str());
        if facet_key.tag != claim_tag || !claim_token.rights().contains(FacetRights::READ) {
            continue;
        }

        let claim_raw = claim_token.get().await.map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error reading claim facet: {err:?}"))
        })?;
        let claim = serde_json::from_str(&claim_raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error parsing claim facet json: {err}"))
        })?;
        let claim_id = facet_key.id;
        existing_claims.insert(claim_id, claim);
    }

    // ── Claims ────────────────────────────────────────────────────────
    let matched_claims =
        hledger::claim_matcher::match_claims(&transactions, &existing_claims, &src_ref);

    let mut claim_txn_pairs: Vec<(String, usize)> = Vec::with_capacity(matched_claims.len());
    for (txn_index, (claim_id, claim)) in matched_claims.iter().enumerate() {
        let claim_json = serde_json::to_string(&claim).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("serde error serializing claim: {err}"))
        })?;

        claim_tag_token
            .create(claim_id.clone(), claim_json)
            .await
            .map_err(|err| {
                wflow_sdk::JobErrorX::Terminal(ferr!(
                    "error creating/updating claim facet: {err:?}"
                ))
            })?;
        claim_txn_pairs.push((claim_id.clone(), txn_index));
    }

    // ── Txn facets ────────────────────────────────────────────────────
    let txn_tag_str = types::DayledgerFacetTag::Txn.as_str();
    let txn_tag_token = args
        .primary_doc
        .tags
        .iter()
        .find(|tag| tag.tag() == txn_tag_str && tag.rights().contains(FacetRights::CREATE))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("txn tag token with create rights not found"))
        })?;

    let claim_facet_tag = daybook_types::doc::FacetKey::from(claim_tag_str).tag;
    for (claim_id, txn_index) in &claim_txn_pairs {
        let txn = &transactions[*txn_index];

        let claim_ref_url: url::Url = format!(
            "db+facet:///{}/{}/{}",
            args.doc_id, claim_facet_tag, claim_id
        )
        .parse()
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("invalid claim ref url: {err}")))?;

        let balance = compute_txn_balance(txn);

        let dayledger_txn = types::Txn {
            txn_id: claim_id.clone(),
            ts: txn.date.to_string(),
            status: map_txn_status(&txn.status),
            payee: Some(txn.description.clone()),
            note: None,
            comment: if txn.comment.is_empty() {
                None
            } else {
                Some(txn.comment.clone())
            },
            balance,
            claim_refs: vec![daybook_types::doc::FacetRef {
                r#ref: claim_ref_url,
                heads: vec![],
            }],
            postings: txn
                .postings
                .iter()
                .map(|posting| types::Posting {
                    account_id: posting.account.clone(),
                    amount: types::Amount {
                        decimal: posting.amount.quantity.clone(),
                        commodity: posting.amount.commodity.clone(),
                    },
                    r#type: map_posting_type(&posting.posting_type),
                })
                .collect(),
            decision_log: vec![],
        };

        let txn_json = serde_json::to_string(&dayledger_txn).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("serde error serializing txn: {err}"))
        })?;

        txn_tag_token
            .create(claim_id.clone(), txn_json)
            .await
            .map_err(|err| {
                wflow_sdk::JobErrorX::Terminal(ferr!("error creating txn facet: {err:?}"))
            })?;
    }

    // ── Account facets ────────────────────────────────────────────────
    let account_tag_str = types::DayledgerFacetTag::Account.as_str();
    let account_tag_token = args
        .primary_doc
        .tags
        .iter()
        .find(|tag| tag.tag() == account_tag_str && tag.rights().contains(FacetRights::CREATE))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("account tag token with create rights not found"))
        })?;

    // Collect unique accounts with their commodities.
    let mut account_info: HashMap<String, HashSet<String>> = HashMap::new();
    for txn in &transactions {
        for posting in &txn.postings {
            let entry = account_info.entry(posting.account.clone()).or_default();
            if !posting.amount.commodity.is_empty() {
                entry.insert(posting.amount.commodity.clone());
            }
        }
    }

    for (account_name, commodities) in &account_info {
        let (account_type, normal_side) = infer_account_type(account_name);
        let title = account_name
            .rsplit(':')
            .next()
            .unwrap_or(account_name)
            .to_string();
        let allowed_commodities: Vec<String> = commodities.iter().cloned().collect();

        let parent_account_ref: Option<url::Url> = {
            account_name
                .rsplit_once(':')
                .map(|(parent, _)| {
                    format!("db+facet:///{}/{}/{}", args.doc_id, account_tag_str, parent).parse()
                })
                .transpose()
                .map_err(|err| {
                    wflow_sdk::JobErrorX::Terminal(ferr!("invalid parent account ref url: {err}"))
                })?
        };

        let account = types::Account {
            account_id: account_name.clone(),
            account_path: account_name.clone(),
            account_type,
            normal_side,
            allowed_commodities,
            parent_account_ref,
            title,
        };

        let account_json = serde_json::to_string(&account).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("serde error serializing account: {err}"))
        })?;

        account_tag_token
            .create(account_name.clone(), account_json)
            .await
            .map_err(|err| {
                wflow_sdk::JobErrorX::Terminal(ferr!("error creating account facet: {err:?}"))
            })?;
    }

    // ── LedgerMeta ────────────────────────────────────────────────────
    let meta_tag_str = types::DayledgerFacetTag::LedgerMeta.as_str();
    let meta_tag_token = args
        .primary_doc
        .tags
        .iter()
        .find(|tag| tag.tag() == meta_tag_str && tag.rights().contains(FacetRights::CREATE))
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("meta tag token with create rights not found"))
        })?;

    let meta_facet_key = daybook_types::doc::FacetKey {
        tag: daybook_types::doc::FacetTag::from(meta_tag_str),
        id: daybook_types::doc::DEFAULT_FACET_ID.into(),
    };
    let meta_facet_key_str = meta_facet_key.to_string();

    // Title: "hledger {note_id}" where note_id comes from the source note.
    let note_id = daybook_types::doc::FacetKey::from(note_token.key().as_str()).id;
    let title = format!("hledger {note_id}");

    // Compute the most common commodity across all postings.
    let mut commodity_counts: HashMap<String, usize> = HashMap::new();
    for txn in &transactions {
        for posting in &txn.postings {
            if !posting.amount.commodity.is_empty() {
                *commodity_counts
                    .entry(posting.amount.commodity.clone())
                    .or_default() += 1;
            }
        }
    }
    let journal_commodity = commodity_counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(commodity, _)| commodity)
        .unwrap_or_default();

    // Build ref URLs for every created Txn and Account facet.
    let txn_facet_tag = daybook_types::doc::FacetKey::from(txn_tag_str).tag;
    let transaction_refs: Vec<url::Url> = claim_txn_pairs
        .iter()
        .map(|(claim_id, _)| {
            format!("db+facet:///{}/{}/{}", args.doc_id, txn_facet_tag, claim_id)
                .parse()
                .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("invalid txn ref url: {err}")))
        })
        .collect::<Result<_, _>>()?;

    let account_facet_tag = daybook_types::doc::FacetKey::from(account_tag_str).tag;
    let account_refs: Vec<url::Url> = account_info
        .keys()
        .map(|account_name| {
            format!(
                "db+facet:///{}/{}/{}",
                args.doc_id, account_facet_tag, account_name
            )
            .parse()
            .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("invalid account ref url: {err}")))
        })
        .collect::<Result<_, _>>()?;

    let ledger_meta = types::LedgerMeta {
        ledger_id: args.doc_id.clone(),
        title,
        journal_commodity,
        account_refs,
        transaction_refs,
    };
    let meta_json = serde_json::to_string(&ledger_meta).map_err(|err| {
        wflow_sdk::JobErrorX::Terminal(ferr!("serde error serializing ledger meta: {err}"))
    })?;

    // Update the existing facet if present, otherwise create a new one.
    if let Some(existing) = args
        .primary_doc
        .facets
        .iter()
        .find(|facet| facet.key() == meta_facet_key_str)
    {
        if !existing.rights().contains(FacetRights::UPDATE) {
            return Err(wflow_sdk::JobErrorX::Terminal(ferr!(
                "existing ledger meta facet missing UPDATE right"
            )));
        }
        let update_result = existing.update(meta_json.clone()).await.map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("denied updating ledger meta facet: {err:?}"))
        })?;
        update_result.map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error updating ledger meta facet: {err:?}"))
        })?;
    } else {
        meta_tag_token
            .create(daybook_types::doc::DEFAULT_FACET_ID.to_string(), meta_json)
            .await
            .map_err(|err| {
                wflow_sdk::JobErrorX::Terminal(ferr!("error creating ledger meta facet: {err:?}"))
            })?;
    }

    Ok(())
}
