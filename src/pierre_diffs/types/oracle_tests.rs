//! Shape tests for `types.rs`. Every `ParsedPatch` and `FileDiffMetadata`
//! that the published `dist` produced for the corpus (`oracle/golden/types.json`)
//! must deserialize with unknown fields rejected and serialize back to the same
//! JSON, which pins field names, optionality and the `type` discriminators.
//! The remaining tests round-trip hand-written JSON for the other tagged and
//! renamed shapes. Test-only, original to this crate.

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::AnnotationLineMap;
use crate::types::lines::LineIndex;
use crate::types::lines::LineInfo;
use crate::types::lines::LineSpans;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::patch::ParsedPatch;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewScrollTarget;

#[derive(Deserialize)]
struct Golden {
    parsed: Vec<ParsedCase>,
    diffs: Vec<DiffCase>,
}

#[derive(Deserialize)]
struct ParsedCase {
    name: String,
    expected: Value,
}

#[derive(Deserialize)]
struct DiffCase {
    expected: Value,
}

fn round_trip<Shape: serde::de::DeserializeOwned + serde::Serialize>(context: &str, json: &Value) {
    let typed: Shape = serde_json::from_value(json.clone())
        .unwrap_or_else(|error| panic!("{context}: deserializing: {error}"));
    let back = serde_json::to_value(&typed).expect("serializes");
    assert_json_eq(context, json, &back);
}

#[test]
fn parsed_patches_from_the_oracle_round_trip() {
    let golden: Golden = load_golden("types.json");
    assert!(golden.parsed.len() >= 5);
    let mut hunk_count = 0;
    for case in &golden.parsed {
        round_trip::<Vec<ParsedPatch>>(&case.name, &case.expected);
        let patches: Vec<ParsedPatch> =
            serde_json::from_value(case.expected.clone()).expect("deserializes");
        hunk_count += patches
            .iter()
            .flat_map(|patch| &patch.files)
            .map(|file| file.hunks.len())
            .sum::<usize>();
    }
    assert!(hunk_count >= 6, "corpus should exercise real hunks");
}

#[test]
fn diffs_from_files_from_the_oracle_round_trip() {
    let golden: Golden = load_golden("types.json");
    assert!(!golden.diffs.is_empty());
    for (index, case) in golden.diffs.iter().enumerate() {
        round_trip::<FileDiffMetadata>(&format!("parseDiffFromFile #{index}"), &case.expected);
    }
}

#[test]
fn hunk_content_is_tagged_by_type() {
    let context =
        json!({"type": "context", "lines": 3, "additionLineIndex": 0, "deletionLineIndex": 0});
    let change = json!({
        "type": "change", "deletions": 1, "deletionLineIndex": 3, "additions": 2, "additionLineIndex": 3
    });
    round_trip::<HunkContent>("context", &context);
    round_trip::<HunkContent>("change", &change);
    assert!(matches!(
        serde_json::from_value::<HunkContent>(context),
        Ok(HunkContent::Context(_))
    ));
    assert!(
        serde_json::from_value::<HunkContent>(json!({"type": "context", "lines": 1, "extra": 1}))
            .is_err()
    );
}

#[test]
fn unknown_fields_are_rejected() {
    let golden: Golden = load_golden("types.json");
    let mut file = golden.diffs[0].expected.clone();
    file["unexpectedField"] = json!(1);
    assert!(serde_json::from_value::<FileDiffMetadata>(file).is_err());
}

#[test]
fn line_index_is_a_number_or_a_pair_string() {
    assert_eq!(
        serde_json::to_value(LineIndex::Single(4)).unwrap(),
        json!(4)
    );
    assert_eq!(
        serde_json::to_value(LineIndex::Pair(1, 2)).unwrap(),
        json!("1,2")
    );
    assert_eq!(
        serde_json::from_value::<LineIndex>(json!("10,20")).unwrap(),
        LineIndex::Pair(10, 20)
    );
    assert!(serde_json::from_value::<LineIndex>(json!("10")).is_err());
    assert!(serde_json::from_value::<LineIndex>(json!("a,b")).is_err());
    round_trip::<LineInfo>(
        "LineInfo",
        &json!({"type": "change-addition", "lineNumber": 3, "altLineNumber": 4, "lineIndex": "7,8"}),
    );
}

#[test]
fn tagged_unions_round_trip() {
    round_trip::<LineSpans>("gap", &json!({"type": "gap", "rows": 3}));
    round_trip::<LineSpans>(
        "annotation",
        &json!({"type": "annotation", "hunkIndex": 1, "lineIndex": 2, "annotations": ["a"]}),
    );
    round_trip::<CodeViewScrollTarget>(
        "line",
        &json!({"type": "line", "id": "x", "lineNumber": 5, "side": "additions", "align": "nearest"}),
    );
    round_trip::<CodeViewScrollTarget>(
        "range",
        &json!({"type": "range", "id": "x", "range": {"start": 1, "end": 2, "endSide": "deletions"}}),
    );
    round_trip::<CodeViewScrollTarget>(
        "position",
        &json!({"type": "position", "position": 12.5, "behavior": "smooth-auto"}),
    );
    round_trip::<CodeViewItem<String>>(
        "file item",
        &json!({
            "type": "file", "id": "f", "file": {"name": "a.ts", "contents": "x"},
            "annotations": [{"lineNumber": 1, "metadata": "note"}], "version": 2
        }),
    );
    let annotations: AnnotationLineMap<String> = serde_json::from_value(json!({
        "3": [{"side": "additions", "lineNumber": 3, "metadata": "m"}], "4": null
    }))
    .expect("record of optional arrays");
    assert_eq!(annotations.len(), 2);
}

#[test]
fn virtual_file_metrics_omit_unset_optionals() {
    let metrics = crate::constants::DEFAULT_VIRTUAL_FILE_METRICS;
    assert_json_eq(
        "DEFAULT_VIRTUAL_FILE_METRICS",
        &json!({"hunkLineCount": 50, "lineHeight": 20, "diffHeaderHeight": 44, "spacing": 8}),
        &serde_json::to_value(metrics).expect("serializes"),
    );
    round_trip::<VirtualFileMetrics>(
        "with padding",
        &json!({
            "hunkLineCount": 1, "lineHeight": 20, "diffHeaderHeight": 44, "spacing": 8,
            "hunkSeparatorHeight": 30, "paddingTop": 1, "paddingBottom": 2
        }),
    );
}
