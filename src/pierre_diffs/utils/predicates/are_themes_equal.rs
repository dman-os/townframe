// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areThemesEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `DiffsThemeNames | ThemesType | undefined` is `Option<&ThemeOption>`; a name never equals a themes pair (upstream `===` between a string and an object).

use crate::types::options::ThemeOption;

pub fn are_themes_equal(theme_a: Option<&ThemeOption>, theme_b: Option<&ThemeOption>) -> bool {
    match (theme_a, theme_b) {
        (Some(ThemeOption::Themes(theme_a)), Some(ThemeOption::Themes(theme_b))) => {
            theme_a.dark == theme_b.dark && theme_a.light == theme_b.light
        }
        (Some(ThemeOption::Name(theme_a)), Some(ThemeOption::Name(theme_b))) => theme_a == theme_b,
        (None, None) => true,
        _ => false,
    }
}
