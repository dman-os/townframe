use chumsky::prelude::*;

use super::parse::common::*;
use super::parse::journal::parse_journal;
use super::types::*;

fn parse_ok<'a, P, T>(parser: P, input: &'a str) -> T
where
    P: Parser<'a, &'a str, T, extra::Err<Rich<'a, char>>>,
    T: std::fmt::Debug,
{
    let result = parser.parse(input);
    let (output, errs) = result.into_output_errors();
    if let Some(output) = output {
        if !errs.is_empty() {
            let msg = errs
                .iter()
                .map(|e| format!("{:?}", e))
                .collect::<Vec<_>>()
                .join("\n");
            eprintln!("parse warnings for {:?}:\n{}", input, msg);
        }
        output
    } else {
        let msg = errs
            .iter()
            .map(|e| format!("{:?}", e))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("parse failed for {:?}:\n{}", input, msg);
    }
}

fn parse_err<'a, P, T>(parser: P, input: &'a str) -> Vec<Rich<'a, char>>
where
    P: Parser<'a, &'a str, T, extra::Err<Rich<'a, char>>>,
{
    let result = parser.parse(input);
    let (_, errs) = result.into_output_errors();
    errs
}

fn parse_journal_ok(input: &str) -> Vec<Transaction> {
    parse_journal(input).unwrap_or_else(|errs| {
        let msg = errs
            .iter()
            .map(|e| format!("{:?}", e))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("journal parse failed for {input:?}:\n{msg}");
    })
}

#[test]
fn test_date_basic() {
    let d = parse_ok(datep(), "2024-01-15");
    assert_eq!(d.year(), 2024);
    assert_eq!(d.month(), 1);
    assert_eq!(d.day(), 15);
}

#[test]
fn test_date_slash_separator() {
    let d = parse_ok(datep(), "2024/01/15");
    assert_eq!(d.year(), 2024);
    assert_eq!(d.month(), 1);
    assert_eq!(d.day(), 15);
}

#[test]
fn test_date_dot_separator() {
    let d = parse_ok(datep(), "2024.01.15");
    assert_eq!(d.year(), 2024);
    assert_eq!(d.month(), 1);
    assert_eq!(d.day(), 15);
}

#[test]
fn test_date_inconsistent_separators() {
    let errs = parse_err(datep(), "2024-01/15");
    assert!(
        !errs.is_empty(),
        "expected error for inconsistent separators"
    );
}

#[test]
fn test_status() {
    assert_eq!(parse_ok(statusp(), "*"), Status::Cleared);
    assert_eq!(parse_ok(statusp(), "!"), Status::Pending);
}

#[test]
fn test_commodity_symbol_simple() {
    assert_eq!(parse_ok(commoditysymbolp(), "USD"), "USD");
    assert_eq!(parse_ok(commoditysymbolp(), "$"), "$");
    assert_eq!(parse_ok(commoditysymbolp(), "EUR"), "EUR");
}

#[test]
fn test_commodity_symbol_quoted() {
    assert_eq!(
        parse_ok(commoditysymbolp(), "\"DE 0002 635307\""),
        "DE 0002 635307"
    );
}

#[test]
fn test_account_name() {
    assert_eq!(
        parse_ok(accountnamep(), "assets:bank:checking"),
        "assets:bank:checking"
    );
    assert_eq!(parse_ok(accountnamep(), "expenses:food"), "expenses:food");
}

#[test]
fn test_amount_left_symbol() {
    let amt = parse_ok(amountp(), "$47.18");
    assert_eq!(amt.commodity, "$");
    assert_eq!(amt.quantity, "47.18");
    assert_eq!(amt.style.commodity_side, CommoditySide::Left);
}

#[test]
fn test_amount_left_symbol_spaced() {
    let amt = parse_ok(amountp(), "$ 47.18");
    assert_eq!(amt.commodity, "$");
    assert!(amt.style.commodity_spaced);
}

#[test]
fn test_amount_no_symbol() {
    let amt = parse_ok(amountp(), "47.18");
    assert!(amt.commodity.is_empty());
    assert_eq!(amt.quantity, "47.18");
}

#[test]
fn test_ws_accepts_tabs() {
    assert_eq!(parse_ok(ws0(), "\t\t"), "\t\t");
    assert_eq!(parse_ok(ws1(), "\t \t"), "\t \t");
}

#[test]
fn test_balance_assertion_single() {
    let ba = parse_ok(balance_assertionp(), "= $100");
    assert!(!ba.is_total);
    assert!(!ba.is_inclusive);
}

#[test]
fn test_balance_assertion_double() {
    let ba = parse_ok(balance_assertionp(), "== $100");
    assert!(ba.is_total);
    assert!(!ba.is_inclusive);
}

#[test]
fn test_balance_assertion_star() {
    let ba = parse_ok(balance_assertionp(), "=*$100");
    assert!(!ba.is_total);
    assert!(ba.is_inclusive);
}

#[test]
fn test_parse_sample_journal_fixture() {
    let input = include_str!("fixtures/sample.journal");
    let txns = parse_journal_ok(input);

    assert_eq!(txns.len(), 5);
    assert_eq!(txns[0].date.to_string(), "2008-01-01");
    assert_eq!(txns[0].description, "income");
    assert_eq!(txns[0].postings.len(), 2);
    assert_eq!(txns[3].status, Status::Cleared);
    assert_eq!(txns[3].description, "eat & shop");
    assert_eq!(txns[3].postings.len(), 3);
}

#[test]
fn test_parse_journal_rejects_header_only_transaction() {
    assert!(
        parse_journal("2008/01/01 income\n").is_err(),
        "expected parser error for header-only txn"
    );
}

#[test]
fn test_parse_journal_handles_final_comment_without_looping() {
    let txns = parse_journal_ok(
        "2008/01/01 income\n    assets:bank:checking  $1\n    income:salary\n\n;final comment",
    );

    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].description, "income");
}

#[test]
fn test_parse_journal_inline_comments_and_tags() {
    let txns = parse_journal_ok(
        "2008/01/01 income ; ttag: val1, note text\n\tassets:bank:checking  $1 ; ptag: val2, posting note\n\tincome:salary\n",
    );

    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].comment, "note text");
    assert_eq!(txns[0].tags, vec![("ttag".to_string(), "val1".to_string())]);
    assert_eq!(txns[0].postings.len(), 2);
    assert_eq!(txns[0].postings[0].comment, "posting note");
    assert_eq!(
        txns[0].postings[0].tags,
        vec![("ptag".to_string(), "val2".to_string())]
    );
}

#[test]
fn test_parse_journal_handles_trailing_blank_lines() {
    let txns = parse_journal_ok(
        "2008/01/01 income\n    assets:bank:checking  $1\n    income:salary\n\n\n",
    );

    assert_eq!(txns.len(), 1);
    assert_eq!(txns[0].description, "income");
}
