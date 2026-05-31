use crate::interlude::*;
use daybook_types::doc::FacetRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Amount {
    pub decimal: String,
    pub commodity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TxnStatus {
    Unmarked,
    Pending,
    Cleared,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PostingType {
    Regular,
    Virtual,
    BalancedVirtual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TxnBalanceStatus {
    Balanced,
    Unbalanced,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommodityTotal {
    pub commodity: String,
    pub total_decimal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TxnBalance {
    pub status: TxnBalanceStatus,
    pub precision_dp: u32,
    pub commodity_totals: Vec<CommodityTotal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Posting {
    pub account_id: String,
    pub amount: Amount,
    pub r#type: PostingType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DecisionLogEntry {
    pub by: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AccountType {
    Asset,
    Liability,
    Equity,
    Revenue,
    Expense,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NormalSide {
    Debit,
    Credit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PostingSign {
    Debit,
    Credit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClaimPostingHint {
    pub account_hint: String,
    pub amount: Amount,
    pub sign: PostingSign,
    pub hint_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HledgerTxnDeets {
    /// 0-based index of this transaction in the parsed hledger file.
    pub txn_index: usize,
    /// Optional transaction code (the text in parentheses).
    pub code: Option<String>,
    /// Transaction description/payee text as parsed from the hledger header.
    #[serde(default)]
    pub description: Option<String>,
    /// hledger tags as key-value pairs.
    pub tags: Vec<(String, String)>,
    /// Per-posting comments from the original hledger text.
    pub posting_comments: Vec<Option<String>>,
    /// Hash of the transaction's original text block for detecting edits.
    pub content_hash: String,
}

daybook_types::define_enum_and_tag!(
    "org.example.dayledger.",
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, schemars::JsonSchema)]
    DayledgerFacetTag,
    #[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
    #[serde(rename_all = "camelCase", untagged)]
    DayledgerFacet {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        Claim struct {
            pub ts: String,
            pub posting_hints: Vec<ClaimPostingHint>,
            #[serde(default)]
            pub src_refs: Vec<FacetRef>,
            pub deets_kind: String,
            pub deets: serde_json::Value,
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        Txn struct {
            pub txn_id: String,
            pub ts: String,
            pub status: TxnStatus,
            pub payee: Option<String>,
            pub note: Option<String>,
            pub comment: Option<String>,
            pub balance: TxnBalance,
            pub claim_refs: Vec<FacetRef>,
            pub postings: Vec<Posting>,
            pub decision_log: Vec<DecisionLogEntry>,
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        Account struct {
            pub account_id: String,
            pub account_path: String,
            pub account_type: AccountType,
            pub normal_side: NormalSide,
            pub allowed_commodities: Vec<String>,
            pub parent_account_ref: Option<Url>,
            pub title: String,
        },
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        "meta" LedgerMeta struct {
            pub ledger_id: String,
            pub title: String,
            pub journal_commodity: String,
            pub account_refs: Vec<Url>,
            pub transaction_refs: Vec<Url>,
        }
    }
);

#[cfg(test)]
mod tests {
    use super::*;

    fn facet_ref(id: &str) -> FacetRef {
        FacetRef {
            r#ref: format!("db+facet:///doc/{id}/main").parse().unwrap(),
            heads: vec!["head".into()],
        }
    }

    fn base_claim_json() -> serde_json::Value {
        serde_json::json!({
            "ts": "2024-01-02",
            "postingHints": [],
            "deetsKind": "hledger",
            "deets": {}
        })
    }

    #[test]
    fn claim_defaults_missing_src_refs_to_empty() {
        let claim: Claim = serde_json::from_value(base_claim_json()).unwrap();
        assert!(claim.src_refs.is_empty());
    }

    #[test]
    fn claim_rejects_legacy_single_source_field() {
        let mut claim_json = base_claim_json();
        claim_json["srcRef"] = serde_json::to_value(facet_ref("note")).unwrap();

        assert!(serde_json::from_value::<Claim>(claim_json).is_err());
    }

    #[test]
    fn claim_deserializes_multi_source_provenance() {
        let mut claim_json = base_claim_json();
        claim_json["srcRefs"] = serde_json::json!([facet_ref("note"), facet_ref("secondary")]);

        let claim: Claim = serde_json::from_value(claim_json).unwrap();

        assert_eq!(
            claim.src_refs,
            vec![facet_ref("note"), facet_ref("secondary")]
        );
    }
}
