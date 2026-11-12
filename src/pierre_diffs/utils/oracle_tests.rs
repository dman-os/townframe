//! Differential tests for the S3 utilities: `parsePatchFiles` (corpus plus
//! fuzzed multi-file git/unified/format-patch inputs, forgiving and strict
//! modes, with and without cache keys and line matching), the quoted-name
//! decoder, the file-type lookup, line-ending helpers and cache keys all run
//! through the published dist (`oracle/golden/{patches,utilities}.json`) and
//! must agree exactly: none of this logic depends on the diff algorithm.
//! Test-only, original to this crate.
//!
//! The S4 layout differential tests are the child module `layout_oracle_tests`
//! (nested here to keep `utils` inside the module item budget).

#[cfg(test)]
mod layout_oracle_tests;

use serde::Deserialize;
use serde_json::Value;

use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::ParseDiffOptions;
use crate::types::patch::ParsePatchOptions;
use crate::utils::clean_last_newline::clean_last_newline;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::get_filetype_from_file_name::get_filetype_from_file_name;
use crate::utils::get_line_ending_type::get_line_ending_type;
use crate::utils::parse_diff_from_file::parse_diff_from_file;
use crate::utils::parse_patch_files::parse_patch_files;
use crate::utils::parse_quoted_diff_file_name::parse_quoted_diff_file_name;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchGolden {
    cases: Vec<PatchCase>,
    from_files: Vec<FromFilesCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FromFilesCase {
    kind: String,
    old_file: Option<FileContents>,
    new_file: Option<FileContents>,
    expected: Value,
}

#[derive(Deserialize)]
struct PatchCase {
    patch: String,
    options: Value,
    expected: Option<Value>,
    error: Option<String>,
    errors: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UtilitiesGolden {
    quoted: Vec<QuotedCase>,
    filetypes: Vec<FiletypeCase>,
    endings: Vec<EndingCase>,
    cache_keys: Vec<CacheKeyCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuotedCase {
    input: String,
    expected: Option<QuotedExpected>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuotedExpected {
    file_name: String,
    raw_length: usize,
}

#[derive(Deserialize)]
struct FiletypeCase {
    name: String,
    expected: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EndingCase {
    text: String,
    clean_last_newline: String,
    line_ending: Value,
}

#[derive(Deserialize)]
struct CacheKeyCase {
    scope: String,
    segments: Vec<String>,
    expected: String,
}

fn parse_options(options: &Value) -> ParsePatchOptions {
    ParsePatchOptions {
        disable_line_matching: options.get("disableLineMatching").and_then(Value::as_bool),
        cache_key_prefix: options
            .get("cacheKeyPrefix")
            .and_then(Value::as_str)
            .map(str::to_owned),
        throw_on_error: options.get("throwOnError").and_then(Value::as_bool),
    }
}

#[test]
fn parse_patch_files_matches_the_published_parser() {
    let golden: PatchGolden = load_golden("patches.json");
    assert!(golden.cases.len() > 50);
    let (mut errors, mut warned) = (0, 0);
    for case in &golden.cases {
        let context = format!("options={} patch={:?}", case.options, case.patch);
        let result = parse_patch_files(&case.patch, &parse_options(&case.options));
        match (&case.expected, &case.error, result) {
            (Some(expected), None, Ok(parsed)) => {
                assert_json_eq(
                    &context,
                    expected,
                    &serde_json::to_value(&parsed.value).expect("serializes"),
                );
                assert_eq!(
                    parsed.warnings.len(),
                    case.errors,
                    "console.error calls vs warnings: {context}\n{:?}",
                    parsed.warnings
                );
                warned += usize::from(case.errors > 0);
            }
            (None, Some(message), Err(error)) => {
                assert_eq!(&error.message, message, "{context}");
                errors += 1;
            }
            (_, _, other) => panic!("outcome mismatch ({other:?}): {context}"),
        }
    }
    assert!(errors > 3, "strict-mode errors exercised: {errors}");
    assert!(warned > 10, "forgiving-mode warnings exercised: {warned}");
}

#[test]
fn quoted_names_match() {
    let golden: UtilitiesGolden = load_golden("utilities.json");
    assert!(golden.quoted.len() > 100);
    for case in &golden.quoted {
        let actual = parse_quoted_diff_file_name(&case.input);
        assert_eq!(
            actual
                .as_ref()
                .map(|parsed| (&parsed.file_name, parsed.raw_length)),
            case.expected
                .as_ref()
                .map(|expected| (&expected.file_name, expected.raw_length)),
            "input={:?}",
            case.input
        );
    }
}

#[test]
fn filetypes_match() {
    let golden: UtilitiesGolden = load_golden("utilities.json");
    assert!(golden.filetypes.len() > 500);
    for case in &golden.filetypes {
        assert_eq!(
            get_filetype_from_file_name(&case.name),
            case.expected,
            "name={:?}",
            case.name
        );
    }
}

#[test]
fn line_endings_and_cache_keys_match() {
    let golden: UtilitiesGolden = load_golden("utilities.json");
    for case in &golden.endings {
        assert_eq!(
            clean_last_newline(&case.text),
            case.clean_last_newline,
            "{:?}",
            case.text
        );
        assert_eq!(
            serde_json::to_value(get_line_ending_type(&case.text)).expect("serializes"),
            case.line_ending,
            "{:?}",
            case.text
        );
    }
    for case in &golden.cache_keys {
        let segments: Vec<&str> = case.segments.iter().map(String::as_str).collect();
        assert_eq!(compose_cache_key(&case.scope, &segments), case.expected);
    }
}

/// Sides that are empty or absent carry upstream's `-1` line indexes.
#[test]
fn parse_diff_from_file_matches_for_empty_and_absent_sides() {
    let golden: PatchGolden = load_golden("patches.json");
    assert!(golden.from_files.len() >= 16);
    let mut negative = 0;
    for case in &golden.from_files {
        let input = match (case.kind.as_str(), &case.old_file, &case.new_file) {
            ("both", Some(old_file), Some(new_file)) => DiffFileInput::Both {
                old_file: old_file.clone(),
                new_file: new_file.clone(),
            },
            ("added", _, Some(new_file)) => DiffFileInput::Added {
                new_file: new_file.clone(),
            },
            ("deleted", Some(old_file), _) => DiffFileInput::Deleted {
                old_file: old_file.clone(),
            },
            other => panic!("unexpected case shape {other:?}"),
        };
        let parsed = parse_diff_from_file(&input, &ParseDiffOptions::default(), true)
            .expect("valid input")
            .value;
        let actual = serde_json::to_value(&parsed).expect("serializes");
        negative += usize::from(case.expected.to_string().contains("LineIndex\":-1"));
        assert_json_eq(
            &format!("{} {:?} {:?}", case.kind, case.old_file, case.new_file),
            &case.expected,
            &actual,
        );
    }
    assert!(negative > 0, "the cases must exercise a -1 line index");
}
