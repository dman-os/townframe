use chumsky::prelude::*;

use super::super::types::*;
use super::common::*;

fn default_amount() -> Amount {
    Amount {
        commodity: String::new(),
        quantity: String::new(),
        style: AmountStyle::default(),
        cost: None,
        cost_basis: None,
    }
}

fn posting_linep<'a>() -> impl Parser<'a, &'a str, Posting, extra::Err<Rich<'a, char>>> {
    let indented = just(' ').repeated().at_least(1).ignored();

    let virtual_balanced = just('[').ignore_then(accountnamep()).then_ignore(just(']'));
    let virtual_unbalanced = just('(').ignore_then(accountnamep()).then_ignore(just(')'));
    let regular = accountnamep();

    let account_and_type = choice((
        virtual_balanced.map(|acct| (acct, PostingType::BalancedVirtual)),
        virtual_unbalanced.map(|acct| (acct, PostingType::Virtual)),
        regular.map(|acct| (acct, PostingType::Regular)),
    ));

    indented
        .ignore_then(
            statusp()
                .map(Some)
                .or(empty().to(None))
                .map(|opt| opt.unwrap_or(Status::Unmarked)),
        )
        .then_ignore(ws0())
        .then(account_and_type)
        .then_ignore(ws0())
        .then(
            amountp()
                .or_not()
                .map(|opt| opt.unwrap_or_else(default_amount)),
        )
        .then(balance_assertionp().or_not())
        .map(
            |(((status, (account, posting_type)), amount), assertion)| Posting {
                status,
                account,
                amount,
                posting_type,
                assertion,
                comment: String::new(),
                tags: Vec::new(),
                date: None,
                date2: None,
            },
        )
        .labelled("posting")
}

pub fn parse_journal(input: &str) -> Result<Vec<Transaction>, Vec<Rich<'_, char>>> {
    let comment_line = ws0()
        .then(just(';').then(none_of(['\n']).repeated()).or_not())
        .then_ignore(newline_or_eof())
        .ignored();

    let blank_line = ws0().then_ignore(newline_or_eof()).ignored();

    let item = choice((
        transactionp().map(Some),
        comment_line.to(None),
        blank_line.to(None),
    ));

    let parser = item.repeated().collect::<Vec<_>>().then_ignore(end());

    let result = parser.parse(input);
    let (output, errs) = result.into_output_errors();

    if !errs.is_empty() {
        return Err(errs);
    }

    Ok(output.unwrap_or_default().into_iter().flatten().collect())
}

pub fn transactionp<'a>() -> impl Parser<'a, &'a str, Transaction, extra::Err<Rich<'a, char>>> {
    let status = statusp()
        .map(Some)
        .or(empty().to(None))
        .map(|opt| opt.unwrap_or(Status::Unmarked));

    let header = datep()
        .then(just('=').ignore_then(datep()).or_not())
        .then_ignore(ws0())
        .then(status)
        .then(codep().or_not())
        .then(
            none_of([';', '\n'])
                .repeated()
                .to_slice()
                .map(|desc: &str| desc.trim_end().to_string()),
        )
        .then_ignore(newline_or_eof())
        .map(|((((date, date2), status), code), description)| {
            (date, date2, status, code, description)
        });

    let posting = posting_linep().then_ignore(newline_or_eof());

    header
        .then(posting.repeated().collect::<Vec<_>>())
        .map(|(header, postings)| {
            let (date, date2, status, code, description) = header;
            Transaction {
                date,
                date2,
                status,
                code,
                description,
                comment: String::new(),
                tags: Vec::new(),
                postings,
            }
        })
        .labelled("transaction")
}
