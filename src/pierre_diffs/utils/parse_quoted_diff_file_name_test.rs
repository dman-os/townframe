// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/parseQuotedDiffFileName.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; `test.each` tables are loops. `rawLength` is
// in UTF-16 code units, as `input.length` is upstream.

use crate::utf16;
use crate::utils::parse_quoted_diff_file_name::QuotedDiffFileName;
use crate::utils::parse_quoted_diff_file_name::parse_quoted_diff_file_name;

#[test]
fn decodes_each_case() {
    let cases: &[(&str, String, &str)] = &[
        ("empty", String::from("\"\""), ""),
        (
            "ASCII",
            String::from("\"src/example.ts\""),
            "src/example.ts",
        ),
        (
            "whitespace",
            String::from("\" leading and trailing \""),
            " leading and trailing ",
        ),
        (
            "literal Unicode",
            String::from("\"src/日本語😀.ts\""),
            "src/日本語😀.ts",
        ),
        (
            "quote",
            String::from(r#""weird\"quote.ts""#),
            "weird\"quote.ts",
        ),
        (
            "backslash",
            String::from(r#""slash\\name.ts""#),
            "slash\\name.ts",
        ),
        (
            "named controls",
            String::from(r#""\a\b\t\n\v\f\r""#),
            "\x07\x08\t\n\x0b\x0c\r",
        ),
        (
            "accent",
            String::from(r#""docs/\303\274ber.md""#),
            "docs/über.md",
        ),
        (
            "invalid UTF-8 replacement",
            String::from(r#""\374ber.md""#),
            "\u{fffd}ber.md",
        ),
        (
            "Japanese",
            String::from(r#""src/\346\227\245\346\234\254\350\252\236.ts""#),
            "src/日本語.ts",
        ),
        ("emoji", String::from(r#""\360\237\230\200.ts""#), "😀.ts"),
        (
            "mixed Unicode",
            format!("\"日本/{}ber😀.ts\"", r"\303\274"),
            "日本/über😀.ts",
        ),
        (
            "separate byte runs",
            String::from(r#""\303\274\t\303\266""#),
            "ü\tö",
        ),
        (
            "octal controls and ASCII",
            String::from(r#""\000\037\040\101\177""#),
            "\0\x1f A\x7f",
        ),
        (
            "leading BOM",
            String::from(r#""\357\273\277file.ts""#),
            "\u{feff}file.ts",
        ),
        (
            "embedded BOM",
            String::from(r#""dir/\357\273\277file.ts""#),
            "dir/\u{feff}file.ts",
        ),
        (
            "literal octal text",
            String::from(r#""\\303\\274.ts""#),
            r"\303\274.ts",
        ),
        (
            "literal named escape",
            String::from(r#""\\n.ts""#),
            r"\n.ts",
        ),
        (
            "backslash before closing quote",
            String::from(r#""path\\""#),
            "path\\",
        ),
    ];
    for (name, input, file_name) in cases {
        assert_eq!(
            parse_quoted_diff_file_name(input),
            Some(QuotedDiffFileName {
                file_name: (*file_name).to_owned(),
                raw_length: utf16::len(input),
            }),
            "decodes {name}"
        );
    }
}

#[test]
fn stops_at_the_closing_quote_before_another_filename_or_timestamp() {
    let token = r#""a/\303\274\"name.ts""#;
    for suffix in [" \"b/other.ts\"", "\t2026-09-18 00:00:00\n"] {
        assert_eq!(
            parse_quoted_diff_file_name(&format!("{token}{suffix}")),
            Some(QuotedDiffFileName {
                file_name: String::from("a/ü\"name.ts"),
                raw_length: utf16::len(token),
            })
        );
    }
}

#[test]
fn returns_no_result_for_each_malformed_input() {
    for input in [
        "",
        "plain.ts",
        r"literal\303.ts",
        " \"leading-space.ts\"",
        "\"",
        "\"unterminated",
        "\"trailing\\",
        r#""escaped closing quote\""#,
        r#""\q""#,
        r#""\x41""#,
        r#""\u0041""#,
        r#""\1""#,
        r#""\12""#,
        r#""\128""#,
        r#""\400""#,
        r#""\777""#,
        r#""\800""#,
        r#""prefix\303\274\12""#,
        r#""prefix\303\274\q""#,
    ] {
        assert_eq!(parse_quoted_diff_file_name(input), None, "{input:?}");
    }
}
