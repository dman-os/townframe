//! Differential tests for the S11a predicates: each function ran in the
//! pinned dist over seeded inputs (`oracle/golden/s11a.json`, `predicates`).
//! Test-only, original to this crate.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use crate::oracle_testing::load_golden;
use crate::types::layout::RenderRange;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::lines::HunkData;
use crate::types::options::PrePropertiesConfig;
use crate::types::options::ThemeOption;
use crate::types::patch::FileContents;
use crate::types::view::DiffLineAnnotation;
use crate::utils::predicates::are_diff_line_annotations_equal::are_diff_line_annotations_equal;
use crate::utils::predicates::are_files_equal::are_files_equal;
use crate::utils::predicates::are_hunk_data_equal::are_hunk_data_equal;
use crate::utils::predicates::are_objects_equal::are_objects_equal;
use crate::utils::predicates::are_pre_properties_equal::are_pre_properties_equal;
use crate::utils::predicates::are_render_ranges_equal::are_render_ranges_equal;
use crate::utils::predicates::are_themes_equal::are_themes_equal;
use crate::utils::predicates::are_virtual_window_specs_equal::are_virtual_window_specs_equal;
use crate::utils::predicates::is_default_render_range::is_default_render_range;

#[derive(Deserialize)]
struct Pair<T> {
    left: T,
    right: T,
    expected: bool,
}

#[derive(Deserialize)]
struct ObjectCase {
    left: Option<HashMap<String, Value>>,
    right: Option<HashMap<String, Value>>,
    same: bool,
    omit: Vec<String>,
    expected: bool,
}

#[derive(Deserialize)]
struct DefaultRangeCase {
    range: RenderRange,
    expected: bool,
}

#[derive(Deserialize)]
struct Predicates {
    annotations: Vec<Pair<DiffLineAnnotation<Value>>>,
    files: Vec<Pair<Option<FileContents>>>,
    hunks: Vec<Pair<HunkData>>,
    objects: Vec<ObjectCase>,
    pres: Vec<Pair<Option<PrePropertiesConfig>>>,
    ranges: Vec<Pair<Option<RenderRange>>>,
    #[serde(rename = "defaultRanges")]
    default_ranges: Vec<DefaultRangeCase>,
    themes: Vec<Pair<Option<ThemeOption>>>,
    #[serde(rename = "windowSpecs")]
    window_specs: Vec<Pair<Option<VirtualWindowSpecs>>>,
}

#[derive(Deserialize)]
struct Golden {
    predicates: Predicates,
}

#[test]
fn predicates_match_the_published_functions() {
    let golden: Golden = load_golden("s11a.json");
    let p = golden.predicates;
    assert!(p.objects.len() > 100);
    for case in &p.annotations {
        assert_eq!(
            are_diff_line_annotations_equal(&case.left, &case.right),
            case.expected,
            "{:?} {:?}",
            case.left,
            case.right
        );
    }
    for case in &p.files {
        assert_eq!(
            are_files_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected,
            "{:?} {:?}",
            case.left,
            case.right
        );
    }
    for case in &p.hunks {
        assert_eq!(are_hunk_data_equal(&case.left, &case.right), case.expected);
    }
    for case in &p.objects {
        let omit: Vec<&str> = case.omit.iter().map(String::as_str).collect();
        let actual = if case.same {
            are_objects_equal(case.left.as_ref(), case.left.as_ref(), &omit)
        } else {
            are_objects_equal(case.left.as_ref(), case.right.as_ref(), &omit)
        };
        assert_eq!(
            actual, case.expected,
            "{:?} {:?} {omit:?}",
            case.left, case.right
        );
    }
    for case in &p.pres {
        assert_eq!(
            are_pre_properties_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected,
            "{:?} {:?}",
            case.left,
            case.right
        );
    }
    for case in &p.ranges {
        assert_eq!(
            are_render_ranges_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected
        );
    }
    for case in &p.default_ranges {
        assert_eq!(is_default_render_range(&case.range), case.expected);
    }
    for case in &p.themes {
        assert_eq!(
            are_themes_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected
        );
    }
    for case in &p.window_specs {
        assert_eq!(
            are_virtual_window_specs_equal(case.left.as_ref(), case.right.as_ref()),
            case.expected
        );
    }
}
