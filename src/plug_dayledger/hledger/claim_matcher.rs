use crate::interlude::*;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;

use rust_decimal::Decimal;

use crate::hledger::types::{Posting, PostingType, Transaction};
use crate::types::{Claim, ClaimPostingHint, HledgerTxnDeets, PostingSign};

const HEURISTIC_MIN_SCORE: f64 = 0.78;
const HEURISTIC_MIN_MARGIN: f64 = 0.08;

#[derive(Debug)]
struct ExistingClaimTxn<'a> {
    claim_id: String,
    claim: &'a Claim,
    deets: HledgerTxnDeets,
}

#[derive(Debug, Clone)]
struct MatchCandidate {
    new_index: usize,
    old_index: usize,
    score: f64,
}

#[derive(Debug, Clone)]
struct TxnMatchMetrics {
    date_score: f64,
    amount_total_score: f64,
    posting_pair_score: f64,
    account_score: f64,
    description_score: f64,
    code_score: f64,
    index_score: f64,
    hard_reject: bool,
}

struct MatchRun<'a> {
    src_ref: &'a daybook_types::doc::FacetRef,
    matched_new: HashSet<usize>,
    matched_old: HashSet<usize>,
    out: Vec<Option<(String, Claim)>>,
}

pub fn match_claims(
    transactions: &[Transaction],
    existing: &HashMap<String, Claim>,
    src_ref: &daybook_types::doc::FacetRef,
) -> Vec<(String, Claim)> {
    let existing = existing
        .iter()
        .filter(|(_, claim)| claim.deets_kind == "hledger")
        .map(|(claim_id, claim)| {
            let deets = serde_json::from_value::<HledgerTxnDeets>(claim.deets.clone())
                .expect("malformed hledger claim deets");
            ExistingClaimTxn {
                claim_id: claim_id.clone(),
                claim,
                deets,
            }
        })
        .collect::<Vec<_>>();

    let mut run = MatchRun {
        src_ref,
        matched_new: HashSet::new(),
        matched_old: HashSet::new(),
        out: vec![None; transactions.len()],
    };

    match_unique_hashes(transactions, &existing, &mut run);
    match_unique_codes(transactions, &existing, &mut run);
    match_high_confidence_heuristics(transactions, &existing, &mut run);

    for (txn_index, txn) in transactions.iter().enumerate() {
        if run.out[txn_index].is_some() {
            continue;
        }
        let new_id = format!("txn_{txn_index}_{}", hash_txn(txn));
        run.out[txn_index] = Some((new_id, build_claim(txn, txn_index, src_ref, None)));
    }

    run.out
        .into_iter()
        .map(|item| item.expect("all transaction outputs populated"))
        .collect()
}

fn match_unique_hashes(
    transactions: &[Transaction],
    existing: &[ExistingClaimTxn<'_>],
    run: &mut MatchRun<'_>,
) {
    let mut new_by_hash: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, txn) in transactions.iter().enumerate() {
        new_by_hash.entry(hash_txn(txn)).or_default().push(idx);
    }

    let mut old_by_hash: HashMap<&str, Vec<usize>> = HashMap::new();
    for (idx, old) in existing.iter().enumerate() {
        old_by_hash
            .entry(old.deets.content_hash.as_str())
            .or_default()
            .push(idx);
    }

    for (hash, new_indexes) in new_by_hash {
        if new_indexes.len() != 1 {
            continue;
        }
        let Some(old_indexes) = old_by_hash.get(hash.as_str()) else {
            continue;
        };
        if old_indexes.len() != 1 {
            continue;
        }
        let new_index = new_indexes[0];
        let old_index = old_indexes[0];
        accept_match(transactions, existing, new_index, old_index, run);
    }
}

fn match_unique_codes(
    transactions: &[Transaction],
    existing: &[ExistingClaimTxn<'_>],
    run: &mut MatchRun<'_>,
) {
    let mut new_by_code: HashMap<&str, Vec<usize>> = HashMap::new();
    for (idx, txn) in transactions.iter().enumerate() {
        if let Some(code) = non_empty(txn.code.as_deref()) {
            new_by_code.entry(code).or_default().push(idx);
        }
    }

    let mut old_by_code: HashMap<&str, Vec<usize>> = HashMap::new();
    for (idx, old) in existing.iter().enumerate() {
        if let Some(code) = non_empty(old.deets.code.as_deref()) {
            old_by_code.entry(code).or_default().push(idx);
        }
    }

    for (code, new_indexes) in new_by_code {
        if new_indexes.len() != 1 {
            continue;
        }
        let Some(old_indexes) = old_by_code.get(code) else {
            continue;
        };
        if old_indexes.len() != 1 {
            continue;
        }
        let new_index = new_indexes[0];
        let old_index = old_indexes[0];
        if run.matched_new.contains(&new_index) || run.matched_old.contains(&old_index) {
            continue;
        }
        let metrics = score_txn_match(new_index, &transactions[new_index], &existing[old_index]);
        if metrics.hard_reject || metrics.date_score < 0.65 || metrics.amount_total_score < 0.85 {
            continue;
        }
        accept_match(transactions, existing, new_index, old_index, run);
    }
}

fn match_high_confidence_heuristics(
    transactions: &[Transaction],
    existing: &[ExistingClaimTxn<'_>],
    run: &mut MatchRun<'_>,
) {
    let mut candidates = Vec::new();
    let mut scores_by_new: HashMap<usize, Vec<f64>> = HashMap::new();
    let mut scores_by_old: HashMap<usize, Vec<f64>> = HashMap::new();

    for (new_index, txn) in transactions.iter().enumerate() {
        if run.matched_new.contains(&new_index) {
            continue;
        }
        for (old_index, old) in existing.iter().enumerate() {
            if run.matched_old.contains(&old_index) {
                continue;
            }
            let metrics = score_txn_match(new_index, txn, old);
            if metrics.hard_reject {
                continue;
            }
            let score = metrics.composite_score();
            if score < HEURISTIC_MIN_SCORE {
                continue;
            }
            scores_by_new.entry(new_index).or_default().push(score);
            scores_by_old.entry(old_index).or_default().push(score);
            candidates.push(MatchCandidate {
                new_index,
                old_index,
                score,
            });
        }
    }

    for scores in scores_by_new.values_mut() {
        scores.sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
    }
    for scores in scores_by_old.values_mut() {
        scores.sort_by(|left, right| right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal));
    }

    candidates.retain(|candidate| {
        let new_margin = scores_by_new
            .get(&candidate.new_index)
            .and_then(|scores| scores.get(1).map(|runner_up| candidate.score - runner_up))
            .unwrap_or(f64::INFINITY);
        let old_margin = scores_by_old
            .get(&candidate.old_index)
            .and_then(|scores| scores.get(1).map(|runner_up| candidate.score - runner_up))
            .unwrap_or(f64::INFINITY);
        let new_is_best = scores_by_new
            .get(&candidate.new_index)
            .and_then(|scores| scores.first())
            .is_some_and(|best| *best == candidate.score);
        let old_is_best = scores_by_old
            .get(&candidate.old_index)
            .and_then(|scores| scores.first())
            .is_some_and(|best| *best == candidate.score);
        new_is_best
            && old_is_best
            && new_margin >= HEURISTIC_MIN_MARGIN
            && old_margin >= HEURISTIC_MIN_MARGIN
    });

    candidates.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for candidate in candidates {
        if run.matched_new.contains(&candidate.new_index)
            || run.matched_old.contains(&candidate.old_index)
        {
            continue;
        }
        accept_match(
            transactions,
            existing,
            candidate.new_index,
            candidate.old_index,
            run,
        );
    }
}

fn accept_match(
    transactions: &[Transaction],
    existing: &[ExistingClaimTxn<'_>],
    new_index: usize,
    old_index: usize,
    run: &mut MatchRun<'_>,
) {
    run.matched_new.insert(new_index);
    run.matched_old.insert(old_index);
    let old = &existing[old_index];
    run.out[new_index] = Some((
        old.claim_id.clone(),
        build_claim(
            &transactions[new_index],
            new_index,
            run.src_ref,
            Some(old.claim),
        ),
    ));
}

fn score_txn_match(
    new_index: usize,
    txn: &Transaction,
    old: &ExistingClaimTxn<'_>,
) -> TxnMatchMetrics {
    let code_score = code_score(txn.code.as_deref(), old.deets.code.as_deref());
    let hard_reject = code_score.is_sign_negative();
    TxnMatchMetrics {
        date_score: date_score(&txn.date, old.claim.ts.as_str()),
        amount_total_score: amount_total_score(txn, old.claim),
        posting_pair_score: posting_pair_score(txn, old.claim),
        account_score: account_score(txn, old.claim),
        description_score: description_score(
            txn.description.as_str(),
            old.deets.description.as_deref().unwrap_or_default(),
        ),
        code_score: code_score.max(0.0),
        index_score: index_score(new_index, old.deets.txn_index),
        hard_reject,
    }
}

impl TxnMatchMetrics {
    fn composite_score(&self) -> f64 {
        let weighted = (0.20 * self.date_score)
            + (0.25 * self.amount_total_score)
            + (0.15 * self.posting_pair_score)
            + (0.20 * self.account_score)
            + (0.12 * self.description_score)
            + (0.05 * self.code_score)
            + (0.03 * self.index_score);

        if self.date_score >= 0.90
            && self.amount_total_score >= 1.0
            && self.account_score >= 0.50
            && self.description_score >= 0.90
            && self.posting_pair_score > 0.0
        {
            return weighted.max(0.84);
        }

        weighted
    }
}

fn date_score(new_date: &crate::hledger::types::Date, old_ts: &str) -> f64 {
    let Ok(old_date) = jiff::civil::Date::from_str(old_ts) else {
        return 0.0;
    };
    if new_date.0 == old_date {
        return 1.0;
    }
    let abs_days = (new_date.0.since(old_date).ok())
        .and_then(|span| span.get_days().checked_abs())
        .unwrap_or(i32::MAX);
    if abs_days <= 1 {
        return 0.90;
    }
    if new_date.year() == old_date.year()
        && new_date.month() == old_date.day()
        && new_date.day() == old_date.month()
    {
        return 0.82;
    }
    if new_date.month() == old_date.month() && new_date.day() == old_date.day() {
        return 0.78;
    }
    if new_date.year() == old_date.year() && new_date.day() == old_date.day() {
        return 0.70;
    }
    if abs_days <= 7 {
        return 0.62;
    }
    0.0
}

fn amount_total_score(txn: &Transaction, claim: &Claim) -> f64 {
    let left = txn_amount_totals(txn);
    let right = claim_amount_totals(claim);
    if left.is_empty() || right.is_empty() {
        return 0.5;
    }
    let commodities = left
        .keys()
        .chain(right.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    if commodities
        .iter()
        .all(|commodity| left.get(commodity) == right.get(commodity))
    {
        return 1.0;
    }

    let mut exact = 0usize;
    let mut compatible = 0usize;
    for commodity in &commodities {
        match (left.get(commodity), right.get(commodity)) {
            (Some(left_value), Some(right_value)) if left_value == right_value => exact += 1,
            (Some(left_value), Some(right_value)) => {
                let delta = (*left_value - *right_value).abs();
                if delta <= Decimal::new(1, 2) {
                    compatible += 1;
                }
            }
            _ => {}
        }
    }

    ((exact as f64) + (0.65 * compatible as f64)) / commodities.len() as f64
}

fn posting_pair_score(txn: &Transaction, claim: &Claim) -> f64 {
    multiset_overlap_ratio(txn_posting_pairs(txn), claim_posting_pairs(claim))
}

fn account_score(txn: &Transaction, claim: &Claim) -> f64 {
    let left = txn
        .postings
        .iter()
        .map(|posting| posting.account.as_str())
        .collect::<BTreeSet<_>>();
    let right = claim
        .posting_hints
        .iter()
        .map(|posting| posting.account_hint.as_str())
        .collect::<BTreeSet<_>>();
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let overlap = left.intersection(&right).count();
    if overlap == left.len() && overlap == right.len() {
        return 1.0;
    }
    let smaller = left.len().min(right.len());
    let larger = left.len().max(right.len());
    if overlap == smaller {
        return 0.80 * (smaller as f64 / larger as f64);
    }
    overlap as f64 / larger as f64
}

fn description_score(new_desc: &str, old_desc: &str) -> f64 {
    let new_norm = normalize_text(new_desc);
    let old_norm = normalize_text(old_desc);
    if new_norm.is_empty() || old_norm.is_empty() {
        return 0.0;
    }
    if new_norm == old_norm {
        return 1.0;
    }
    let new_tokens = new_norm.split_whitespace().collect::<BTreeSet<_>>();
    let old_tokens = old_norm.split_whitespace().collect::<BTreeSet<_>>();
    if new_tokens.is_empty() || old_tokens.is_empty() {
        return 0.0;
    }
    let overlap = new_tokens.intersection(&old_tokens).count();
    overlap as f64 / new_tokens.len().max(old_tokens.len()) as f64
}

fn code_score(new_code: Option<&str>, old_code: Option<&str>) -> f64 {
    match (non_empty(new_code), non_empty(old_code)) {
        (Some(left), Some(right)) if left == right => 1.0,
        (Some(_), Some(_)) => -1.0,
        _ => 0.0,
    }
}

fn index_score(new_index: usize, old_index: usize) -> f64 {
    let gap = new_index.abs_diff(old_index);
    match gap {
        0 => 1.0,
        1 => 0.75,
        2..=4 => 0.45,
        _ => 0.0,
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn normalize_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn txn_amount_totals(txn: &Transaction) -> BTreeMap<String, Decimal> {
    let mut out = BTreeMap::new();
    for posting in &txn.postings {
        add_amount(
            &mut out,
            &posting.amount.commodity,
            &posting.amount.quantity,
        );
    }
    out
}

fn claim_amount_totals(claim: &Claim) -> BTreeMap<String, Decimal> {
    let mut out = BTreeMap::new();
    for posting in &claim.posting_hints {
        add_amount(&mut out, &posting.amount.commodity, &posting.amount.decimal);
    }
    out
}

fn add_amount(out: &mut BTreeMap<String, Decimal>, commodity: &str, decimal: &str) {
    if decimal.trim().is_empty() {
        return;
    }
    let Ok(value) = Decimal::from_str(decimal.trim()) else {
        return;
    };
    *out.entry(commodity.to_string()).or_default() += value;
}

fn txn_posting_pairs(txn: &Transaction) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for posting in &txn.postings {
        *out.entry(txn_posting_pair_key(posting)).or_default() += 1;
    }
    out
}

fn claim_posting_pairs(claim: &Claim) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for posting in &claim.posting_hints {
        let hint_type = posting.hint_type.as_deref().unwrap_or("regular");
        let key = format!(
            "{}\u{1f}{}\u{1f}{}\u{1f}{}",
            posting.account_hint, posting.amount.commodity, posting.amount.decimal, hint_type
        );
        *out.entry(key).or_default() += 1;
    }
    out
}

fn txn_posting_pair_key(posting: &Posting) -> String {
    let hint_type = match posting.posting_type {
        PostingType::Regular => "regular",
        PostingType::Virtual => "virtual",
        PostingType::BalancedVirtual => "balancedVirtual",
    };
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        posting.account, posting.amount.commodity, posting.amount.quantity, hint_type
    )
}

fn multiset_overlap_ratio(left: BTreeMap<String, usize>, right: BTreeMap<String, usize>) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let mut overlap = 0usize;
    for (key, left_count) in &left {
        let right_count = right.get(key).copied().unwrap_or_default();
        overlap += (*left_count).min(right_count);
    }
    overlap as f64
        / left
            .values()
            .sum::<usize>()
            .max(right.values().sum::<usize>()) as f64
}

fn build_claim(
    txn: &Transaction,
    txn_index: usize,
    src_ref: &daybook_types::doc::FacetRef,
    existing: Option<&Claim>,
) -> Claim {
    let posting_hints: Vec<ClaimPostingHint> = txn
        .postings
        .iter()
        .map(|posting| ClaimPostingHint {
            account_hint: posting.account.clone(),
            amount: crate::types::Amount {
                decimal: posting.amount.quantity.clone(),
                commodity: posting.amount.commodity.clone(),
            },
            sign: match posting.posting_type {
                PostingType::Regular => PostingSign::Debit,
                PostingType::Virtual => PostingSign::Credit,
                PostingType::BalancedVirtual => PostingSign::Debit,
            },
            hint_type: match posting.posting_type {
                PostingType::Regular => None,
                PostingType::Virtual => Some("virtual".into()),
                PostingType::BalancedVirtual => Some("balancedVirtual".into()),
            },
        })
        .collect();

    let deets = HledgerTxnDeets {
        txn_index,
        code: txn.code.clone(),
        description: Some(txn.description.clone()),
        tags: txn.tags.clone(),
        posting_comments: txn
            .postings
            .iter()
            .map(|posting| {
                let comment = posting.comment.clone();
                if comment.is_empty() {
                    None
                } else {
                    Some(comment)
                }
            })
            .collect(),
        content_hash: hash_txn(txn),
    };

    Claim {
        ts: txn.date.to_string(),
        posting_hints,
        src_refs: claim_src_refs(existing, src_ref),
        deets_kind: "hledger".into(),
        deets: serde_json::to_value(deets).unwrap(),
    }
}

fn claim_src_refs(
    existing: Option<&Claim>,
    src_ref: &daybook_types::doc::FacetRef,
) -> Vec<daybook_types::doc::FacetRef> {
    let mut src_refs = existing
        .map(|existing_claim| existing_claim.src_refs.clone())
        .unwrap_or_default();
    if !src_refs.iter().any(|existing_ref| existing_ref == src_ref) {
        src_refs.push(src_ref.clone());
    }
    src_refs
}

pub fn hash_txn(txn: &Transaction) -> String {
    use blake3::Hasher;

    let mut hasher = Hasher::new();
    hasher.update(txn.date.to_string().as_bytes());
    hasher.update(&[0]);
    hasher.update(txn.description.as_bytes());
    hasher.update(&[0]);
    if let Some(code) = txn.code.as_deref() {
        hasher.update(code.as_bytes());
    }
    hasher.update(&[0]);

    let mut postings = txn.postings.iter().collect::<Vec<_>>();
    postings.sort_by(|left, right| {
        (
            left.account.as_str(),
            left.amount.quantity.as_str(),
            left.amount.commodity.as_str(),
            posting_type_rank(&left.posting_type),
        )
            .cmp(&(
                right.account.as_str(),
                right.amount.quantity.as_str(),
                right.amount.commodity.as_str(),
                posting_type_rank(&right.posting_type),
            ))
    });
    for posting in postings {
        hasher.update(posting.account.as_bytes());
        hasher.update(&[0]);
        hasher.update(posting.amount.quantity.as_bytes());
        hasher.update(&[0]);
        hasher.update(posting.amount.commodity.as_bytes());
        hasher.update(&[0]);
    }

    utils_rs::hash::encode_base58_multibase(hasher.finalize().as_bytes())
}

fn posting_type_rank(posting_type: &PostingType) -> u8 {
    match posting_type {
        PostingType::Regular => 0,
        PostingType::Virtual => 1,
        PostingType::BalancedVirtual => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src_ref() -> daybook_types::doc::FacetRef {
        daybook_types::doc::FacetRef {
            r#ref: "db+facet:///doc/note/main".parse().unwrap(),
            heads: vec!["head".into()],
        }
    }

    fn txns(input: &str) -> Vec<Transaction> {
        crate::hledger::parse::journal::parse_journal(input).unwrap()
    }

    fn existing_from(txns: &[Transaction]) -> HashMap<String, Claim> {
        match_claims(txns, &HashMap::new(), &src_ref())
            .into_iter()
            .collect()
    }

    #[test]
    fn hash_matching_preserves_claim_ids_when_transaction_is_inserted_above() {
        let original = txns(
            "2024/01/02 coffee\n  expenses:food  $5\n  assets:cash\n\n2024/01/03 books\n  expenses:books  $12\n  assets:cash\n",
        );
        let existing = existing_from(&original);
        let original_ids = match_claims(&original, &existing, &src_ref())
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();

        let edited = txns(
            "2024/01/01 rent\n  expenses:rent  $100\n  assets:cash\n\n2024/01/02 coffee\n  expenses:food  $5\n  assets:cash\n\n2024/01/03 books\n  expenses:books  $12\n  assets:cash\n",
        );
        let matched = match_claims(&edited, &existing, &src_ref());

        assert_ne!(matched[0].0, original_ids[0]);
        assert_eq!(matched[1].0, original_ids[0]);
        assert_eq!(matched[2].0, original_ids[1]);
    }

    #[test]
    fn heuristic_matching_preserves_claim_id_when_description_changes() {
        let original = txns("2024/01/02 starbucks 123\n  expenses:food  $5\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited = txns("2024/01/02 coffee\n  expenses:food  $5\n  assets:cash\n");

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_eq!(matched[0].0, original_id);
    }

    #[test]
    fn heuristic_matching_handles_split_expansion() {
        let original = txns("2024/01/02 groceries\n  expenses:food  $40\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited = txns(
            "2024/01/02 groceries\n  expenses:food  $25\n  expenses:household  $15\n  assets:cash\n",
        );

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_eq!(matched[0].0, original_id);
    }

    #[test]
    fn heuristic_matching_handles_category_account_change() {
        let original = txns("2024/01/02 hardware\n  expenses:unknown  $10\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited = txns("2024/01/02 hardware\n  expenses:supplies  $10\n  assets:cash\n");

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_eq!(matched[0].0, original_id);
    }

    #[test]
    fn heuristic_matching_handles_small_amount_correction() {
        let original = txns("2024/01/02 lunch\n  expenses:food  $12.34\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited = txns("2024/01/02 lunch\n  expenses:food  $12.35\n  assets:cash\n");

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_eq!(matched[0].0, original_id);
    }

    #[test]
    fn heuristic_matching_handles_year_month_and_day_month_date_mistakes() {
        let original = txns(
            "2023/01/02 year typo\n  expenses:food  $5\n  assets:cash\n\n2024/01/10 month typo\n  expenses:books  $8\n  assets:cash\n\n2024/03/04 swapped date\n  expenses:travel  $9\n  assets:cash\n",
        );
        let existing = existing_from(&original);
        let original_ids = match_claims(&original, &existing, &src_ref())
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        let edited = txns(
            "2024/01/02 year typo\n  expenses:food  $5\n  assets:cash\n\n2024/02/10 month typo\n  expenses:books  $8\n  assets:cash\n\n2024/04/03 swapped date\n  expenses:travel  $9\n  assets:cash\n",
        );

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_eq!(matched[0].0, original_ids[0]);
        assert_eq!(matched[1].0, original_ids[1]);
        assert_eq!(matched[2].0, original_ids[2]);
    }

    #[test]
    #[should_panic(expected = "malformed hledger claim deets")]
    fn malformed_existing_hledger_claim_panics() {
        let original = txns("2024/01/02 coffee\n  expenses:food  $5\n  assets:cash\n");
        let mut existing = existing_from(&original);
        existing.insert(
            "malformed".into(),
            Claim {
                ts: "2024-01-02".into(),
                posting_hints: vec![],
                src_refs: vec![],
                deets_kind: "hledger".into(),
                deets: serde_json::Value::Null,
            },
        );

        let _ = match_claims(&original, &existing, &src_ref());
    }

    #[test]
    fn matched_claim_preserves_multi_source_provenance() {
        let original = txns("2024/01/02 coffee\n  expenses:food  $5\n  assets:cash\n");
        let mut existing = existing_from(&original);
        let existing_id = existing.keys().next().unwrap().clone();
        let second_source = daybook_types::doc::FacetRef {
            r#ref: "db+facet:///doc/note/secondary".parse().unwrap(),
            heads: vec!["head-2".into()],
        };
        let preserved_src_refs = vec![src_ref(), second_source.clone()];
        existing.get_mut(&existing_id).unwrap().src_refs = preserved_src_refs.clone();

        let matched = match_claims(&original, &existing, &src_ref());

        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].0, existing_id);
        assert_eq!(matched[0].1.src_refs, preserved_src_refs);
    }

    #[test]
    fn distant_date_change_does_not_match_without_code() {
        let original = txns("2024/01/02 old lunch\n  expenses:food  $5\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited = txns("2024/09/20 old lunch\n  expenses:food  $5\n  assets:cash\n");

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_ne!(matched[0].0, original_id);
    }

    #[test]
    fn unique_code_matching_handles_large_description_change() {
        let original = txns("2024/01/02 (bank-123) unknown\n  expenses:todo  $10\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited =
            txns("2024/01/02 (bank-123) hardware store\n  expenses:supplies  $10\n  assets:cash\n");

        let matched = match_claims(&edited, &existing, &src_ref());

        assert_eq!(matched[0].0, original_id);
    }

    #[test]
    fn ambiguous_duplicates_do_not_heuristically_steal_claim_ids() {
        let original = txns("2024/01/02 coffee\n  expenses:food  $5\n  assets:cash\n");
        let existing = existing_from(&original);
        let original_id = existing.keys().next().unwrap().clone();
        let edited = txns(
            "2024/01/02 cafe\n  expenses:food  $5\n  assets:cash\n\n2024/01/02 cafe\n  expenses:food  $5\n  assets:cash\n",
        );

        let matched = match_claims(&edited, &existing, &src_ref());

        assert!(matched.iter().all(|(id, _)| id != &original_id));
    }

    #[test]
    fn hash_txn_is_stable_across_posting_order() {
        let left = txns("2024/01/02 lunch\n  expenses:food  $5\n  assets:cash\n");
        let right = txns("2024/01/02 lunch\n  assets:cash\n  expenses:food  $5\n");

        assert_eq!(hash_txn(&left[0]), hash_txn(&right[0]));
    }
}
