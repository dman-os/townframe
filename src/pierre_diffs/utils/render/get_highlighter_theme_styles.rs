// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getHighlighterThemeStyles.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `normalizeThemeColors` (from `@pierre/theming`, which only resolves the workbench `colors` map and returns the theme's own `fg`/`bg`) is the identity here: this function reads `fg`/`bg` from it and the git colors from the *original* theme, so nothing it computes is observable. The `highlights`-backend `cssVariables` branch (`theme.zed.cssVariables`) arrives with the S7 adapter.

use crate::highlighter_types::DiffsHighlighter;
use crate::highlighter_types::ThemeData;
use crate::types::options::ThemeOption;
use crate::utils::render::format_css_variable_prefix::CssVariablePrefix;
use crate::utils::render::format_css_variable_prefix::format_css_variable_prefix;

pub fn get_highlighter_theme_styles(
    theme: &ThemeOption,
    highlighter: &dyn DiffsHighlighter,
    prefix: Option<&str>,
) -> String {
    let global = format_css_variable_prefix(CssVariablePrefix::Global);
    let mut styles = String::new();
    match theme {
        ThemeOption::Name(theme_name) => {
            let theme_data = highlighter.get_theme(theme_name);
            styles.push_str(&format!("color:{};", theme_data.fg));
            styles.push_str(&format!("background-color:{};", theme_data.bg));
            styles.push_str(&format!("{global}fg:{};", theme_data.fg));
            styles.push_str(&format!("{global}bg:{};", theme_data.bg));
            styles.push_str(&get_git_variables(&theme_data, prefix));
        }
        ThemeOption::Themes(themes) => {
            let theme_data = highlighter.get_theme(&themes.dark);
            styles.push_str(&format!("{global}dark:{};", theme_data.fg));
            styles.push_str(&format!("{global}dark-bg:{};", theme_data.bg));
            styles.push_str(&get_git_variables(&theme_data, Some("dark")));

            let theme_data = highlighter.get_theme(&themes.light);
            styles.push_str(&format!("{global}light:{};", theme_data.fg));
            styles.push_str(&format!("{global}light-bg:{};", theme_data.bg));
            styles.push_str(&get_git_variables(&theme_data, Some("light")));
        }
    }
    styles
}

// Keep the gitDecoration -> terminal.ansi fallback; adding editorGutter colors
// here would change diff backgrounds for existing themes.
fn get_git_variables(theme_data: &ThemeData, mode_prefix: Option<&str>) -> String {
    let mode_prefix = mode_prefix.map_or_else(String::new, |prefix| format!("{prefix}-"));
    let global = format_css_variable_prefix(CssVariablePrefix::Global);
    let color = |primary: &str, fallback: &str| {
        theme_data
            .colors
            .get(primary)
            .or_else(|| theme_data.colors.get(fallback))
    };
    let mut styles = String::new();
    if let Some(addition_green) = color(
        "gitDecoration.addedResourceForeground",
        "terminal.ansiGreen",
    ) {
        styles.push_str(&format!(
            "{global}{mode_prefix}addition-color:{addition_green};"
        ));
    }
    if let Some(deletion_red) = color(
        "gitDecoration.deletedResourceForeground",
        "terminal.ansiRed",
    ) {
        styles.push_str(&format!(
            "{global}{mode_prefix}deletion-color:{deletion_red};"
        ));
    }
    if let Some(modified_blue) = color(
        "gitDecoration.modifiedResourceForeground",
        "terminal.ansiBlue",
    ) {
        styles.push_str(&format!(
            "{global}{mode_prefix}modified-color:{modified_blue};"
        ));
    }
    styles
}
