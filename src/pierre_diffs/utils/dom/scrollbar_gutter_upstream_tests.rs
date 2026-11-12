// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/scrollbarGutter.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test became nextest; CSS string assertions are unchanged. The
// scrollbar probe itself is exercised through ScrollbarGutterHost by the S11a
// oracle test because the upstream test file only covers the CSS helpers.

use crate::constants::DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY;
use crate::types::options::ThemeTypes;
use crate::utils::dom::css_wrappers::patch_scrollbar_gutter_size;
use crate::utils::dom::css_wrappers::wrap_theme_css;

#[test]
fn wrap_theme_css_predefines_the_measured_gutter_with_the_fallback_value() {
    assert!(wrap_theme_css("--diffs-token: red;", ThemeTypes::Dark, None).contains(
        &format!(
            "{DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY}: var(--diffs-scrollbar-gutter-fallback);"
        )
    ));
}

#[test]
fn wrap_theme_css_writes_the_measured_gutter_into_the_theme_style() {
    assert!(wrap_theme_css("--diffs-token: red;", ThemeTypes::Dark, Some(6.0)).contains(
        &format!("{DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY}: 6px;")
    ));
}

#[test]
fn patch_scrollbar_gutter_size_updates_the_existing_measured_gutter_declaration() {
    let patched = patch_scrollbar_gutter_size(
        &wrap_theme_css("--diffs-token: red;", ThemeTypes::Dark, None),
        Some(6.0),
    );
    let updated = patch_scrollbar_gutter_size(&patched, Some(8.0));

    assert!(updated.contains(&format!("{DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY}: 8px;")));
    assert!(!updated.contains(&format!("{DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY}: 6px;")));
    assert_eq!(
        updated
            .matches(DIFFS_SCROLLBAR_GUTTER_MEASURED_PROPERTY)
            .count(),
        1
    );
}
