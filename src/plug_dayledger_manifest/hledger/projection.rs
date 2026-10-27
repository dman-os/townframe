use crate::interlude::*;

// ── Helpers ────────────────────────────────────────────────────────────

pub fn map_txn_status(st: &crate::hledger::types::Status) -> crate::types::TxnStatus {
    match st {
        crate::hledger::types::Status::Unmarked => crate::types::TxnStatus::Unmarked,
        crate::hledger::types::Status::Pending => crate::types::TxnStatus::Pending,
        crate::hledger::types::Status::Cleared => crate::types::TxnStatus::Cleared,
    }
}

pub fn map_posting_type(pt: &crate::hledger::types::PostingType) -> crate::types::PostingType {
    match pt {
        crate::hledger::types::PostingType::Regular => crate::types::PostingType::Regular,
        crate::hledger::types::PostingType::Virtual => crate::types::PostingType::Virtual,
        crate::hledger::types::PostingType::BalancedVirtual => {
            crate::types::PostingType::BalancedVirtual
        }
    }
}

pub fn compute_txn_balance(txn: &crate::hledger::types::Transaction) -> crate::types::TxnBalance {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let mut commodity_totals: HashMap<String, Decimal> = HashMap::new();
    let mut max_dp: u32 = 0;

    for posting in &txn.postings {
        let quantity = posting.amount.quantity.trim();
        if quantity.is_empty() {
            continue;
        }
        let Ok(value) = Decimal::from_str(quantity) else {
            continue;
        };
        // Count decimal places for precision.
        if let Some(dot_pos) = quantity.find('.') {
            let dp = (quantity.len() - dot_pos - 1) as u32;
            max_dp = max_dp.max(dp);
        }
        *commodity_totals
            .entry(posting.amount.commodity.clone())
            .or_default() += value;
    }

    let status = if commodity_totals
        .values()
        .all(|total| total.abs() < Decimal::new(1, 2))
    {
        crate::types::TxnBalanceStatus::Balanced
    } else {
        crate::types::TxnBalanceStatus::Unbalanced
    };

    let totals: Vec<crate::types::CommodityTotal> = commodity_totals
        .into_iter()
        .map(|(commodity, total)| crate::types::CommodityTotal {
            commodity,
            total_decimal: total.to_string(),
        })
        .collect();

    crate::types::TxnBalance {
        status,
        precision_dp: max_dp,
        commodity_totals: totals,
    }
}

pub fn infer_account_type(
    account_name: &str,
) -> (crate::types::AccountType, crate::types::NormalSide) {
    let top_level = account_name.split(':').next().unwrap_or("").to_lowercase();
    match top_level.as_str() {
        "assets" | "asset" => (
            crate::types::AccountType::Asset,
            crate::types::NormalSide::Debit,
        ),
        "liabilities" | "liability" => (
            crate::types::AccountType::Liability,
            crate::types::NormalSide::Credit,
        ),
        "equity" => (
            crate::types::AccountType::Equity,
            crate::types::NormalSide::Credit,
        ),
        "revenue" | "income" => (
            crate::types::AccountType::Revenue,
            crate::types::NormalSide::Credit,
        ),
        "expenses" | "expense" => (
            crate::types::AccountType::Expense,
            crate::types::NormalSide::Debit,
        ),
        _ => (
            crate::types::AccountType::Asset,
            crate::types::NormalSide::Debit,
        ),
    }
}
