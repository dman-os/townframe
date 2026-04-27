use chumsky::prelude::*;

use super::super::types::*;

pub(crate) fn ws0<'a>() -> impl Parser<'a, &'a str, &'a str, extra::Err<Rich<'a, char>>> {
    just(' ').repeated().to_slice()
}

pub(crate) fn ws1<'a>() -> impl Parser<'a, &'a str, &'a str, extra::Err<Rich<'a, char>>> {
    just(' ').repeated().at_least(1).to_slice()
}

pub(crate) fn newline_or_eof<'a>() -> impl Parser<'a, &'a str, (), extra::Err<Rich<'a, char>>> {
    just('\n').ignored().or(end())
}

pub fn datep<'a>() -> impl Parser<'a, &'a str, Date, extra::Err<Rich<'a, char>>> {
    type DateInput<'a> = ((((&'a str, char), &'a str), char), &'a str);

    let digits = text::digits(10).to_slice();

    digits
        .then(one_of(['-', '/', '.']))
        .then(digits)
        .then(one_of(['-', '/', '.']))
        .then(digits)
        .try_map(|input: DateInput<'a>, span| {
            let (((year_s, sep1), month_s), sep2) = input.0;
            let day_s = input.1;
            if sep1 != sep2 {
                return Err(Rich::custom(span, "date separators must be consistent"));
            }
            let year: i16 = year_s
                .parse()
                .map_err(|_| Rich::custom(span, "invalid year"))?;
            let month: i8 = month_s
                .parse()
                .map_err(|_| Rich::custom(span, "invalid month"))?;
            let day: i8 = day_s
                .parse()
                .map_err(|_| Rich::custom(span, "invalid day"))?;
            Date::new(year, month, day).ok_or(Rich::custom(span, "invalid date"))
        })
        .labelled("date")
}

pub fn statusp<'a>() -> impl Parser<'a, &'a str, Status, extra::Err<Rich<'a, char>>> {
    just('*')
        .to(Status::Cleared)
        .or(just('!').to(Status::Pending))
}

pub fn codep<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> {
    just('(')
        .ignore_then(
            none_of([')', '\n'])
                .repeated()
                .to_slice()
                .map(|slice: &str| slice.to_string()),
        )
        .then_ignore(just(')'))
}

pub fn commoditysymbolp<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> {
    let quoted = just('"')
        .ignore_then(
            none_of(['"', ';', '\n'])
                .repeated()
                .to_slice()
                .map(|slice: &str| slice.to_string()),
        )
        .then_ignore(just('"'));

    let nonsimple = |ch: char| {
        ch.is_ascii_digit()
            || ch.is_whitespace()
            || matches!(
                ch,
                '@' | '('
                    | ')'
                    | '['
                    | ']'
                    | '{'
                    | '}'
                    | '"'
                    | '\''
                    | '-'
                    | '+'
                    | ','
                    | '/'
                    | '*'
                    | '='
                    | ';'
                    | '!'
                    | '#'
            )
    };

    let simple = any()
        .filter(move |ch: &char| !nonsimple(*ch) && *ch != '.')
        .repeated()
        .at_least(1)
        .to_slice()
        .map(|slice: &str| slice.to_string());

    quoted.or(simple).labelled("commodity symbol")
}

pub fn accountnamep<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> {
    none_of([' ', '\t', '\n', '\r', ';'])
        .repeated()
        .at_least(1)
        .to_slice()
        .map(|slice: &str| slice.to_string())
        .labelled("account name")
}

fn rawnumberp<'a>() -> impl Parser<'a, &'a str, (String, Option<char>), extra::Err<Rich<'a, char>>>
{
    let digit = one_of(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']);
    let digits = text::digits(10).to_slice();

    let leading_decimal = one_of(['.', ','])
        .then(digit.repeated().at_least(1).to_slice())
        .map(|(mark, frac): (char, &str)| (format!(".{}", frac), Some(mark)));

    let digits_with_decimal = digits
        .then(one_of(['.', ',']))
        .then(digit.repeated().to_slice())
        .map(|((int_part, mark), frac_part): ((&str, char), &str)| {
            (format!("{}{}{}", int_part, mark, frac_part), Some(mark))
        });

    let plain_digits = digits.map(|slice: &str| (slice.to_string(), None));

    leading_decimal
        .or(digits_with_decimal)
        .or(plain_digits)
        .labelled("number")
}

pub fn amountp<'a>() -> impl Parser<'a, &'a str, Amount, extra::Err<Rich<'a, char>>> {
    let left_symbol = commoditysymbolp()
        .then(ws1().map(|_| true).or(empty().to(false)))
        .then(
            just('-')
                .to(true)
                .or(just('+').to(false))
                .or(empty().to(false)),
        )
        .then(rawnumberp())
        .map(|(((commodity, spaced), neg), (num, decimal_mark))| Amount {
            commodity,
            quantity: if neg && !num.starts_with('-') {
                format!("-{}", num)
            } else {
                num
            },
            style: AmountStyle {
                commodity_side: CommoditySide::Left,
                commodity_spaced: spaced,
                decimal_mark,
            },
            cost: None,
            cost_basis: None,
        });

    let right_or_no_symbol = just('-')
        .to(true)
        .or(just('+').to(false))
        .or(empty().to(false))
        .then(rawnumberp())
        .then(ws1().ignore_then(commoditysymbolp()).or_not())
        .map(|((neg, (num, decimal_mark)), opt_commodity)| {
            let quantity = if neg && !num.starts_with('-') {
                format!("-{}", num)
            } else {
                num
            };
            match opt_commodity {
                Some(commodity) => Amount {
                    commodity,
                    quantity,
                    style: AmountStyle {
                        commodity_side: CommoditySide::Right,
                        commodity_spaced: true,
                        decimal_mark,
                    },
                    cost: None,
                    cost_basis: None,
                },
                None => Amount {
                    commodity: String::new(),
                    quantity,
                    style: AmountStyle {
                        commodity_side: CommoditySide::Right,
                        commodity_spaced: false,
                        decimal_mark,
                    },
                    cost: None,
                    cost_basis: None,
                },
            }
        });

    left_symbol.or(right_or_no_symbol).labelled("amount")
}

pub fn balance_assertionp<'a>(
) -> impl Parser<'a, &'a str, BalanceAssertion, extra::Err<Rich<'a, char>>> {
    let double_eq = just("==").to(true);
    let single_eq = just('=').to(false);

    double_eq
        .or(single_eq)
        .then(just('*').to(true).or(empty().to(false)))
        .then_ignore(ws0())
        .then(amountp())
        .map(|((is_total, is_inclusive), amount)| BalanceAssertion {
            amount,
            is_total,
            is_inclusive,
        })
        .labelled("balance assertion")
}
