//! The shell's theme contract: the end user's stylesheet, the facet that stores it and the two
//! calls a theme editor makes of the host.
use clay_mdk::ConfigDecl;
use serde::Deserialize;
use serde::Serialize;

/// The end user's theme sheet, a second configuration facet of the shell: the CSS the host
/// applies to its own document and to every micro's mount as the last layer. It is a separate
/// facet from the arrangement because it is written by a different party (the theme editor)
/// and replaced wholesale. This facet is the one place the applied theme is stored.
pub const THEME_CONFIG_NAME: &str = "shell-theme";
pub const THEME_CONFIG_TAG: &str = "org.example.dayframe.shell.theme";
pub const THEME_CONFIG_KEY: &str = "main";

/// [`GET_THEME`]: `()` in, [`ThemeSheet`] out, the CSS the host currently applies. [`SET_THEME`]:
/// [`ThemeSheet`] in, `()` out, applies the CSS to the host's document and every micro's mount
/// live and keeps it across restarts, or is rejected when the CSS could make the host fetch
/// anything (`url(`, `@import`, `image-set(` and the rest: the rule that refuses a guest's
/// markup and stylesheet from fetching applies to the end user's theme too). The pair is all a
/// theme editor needs of the host; there is no call that runs arbitrary host actions.
pub const GET_THEME: &str = "get-theme";
pub const SET_THEME: &str = "set-theme";
pub const GET_THEME_CONTRACT: &str = "org.example.dayframe.shell.getTheme";
pub const SET_THEME_CONTRACT: &str = "org.example.dayframe.shell.setTheme";

/// The end user's theme: one stylesheet, an empty one meaning no theme. It is the [`SET_THEME`]
/// request, the [`GET_THEME`] answer and the value of the [`ShellTheme`] facet.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThemeSheet {
    #[serde(default)]
    pub css: String,
}

/// The end user's theme facet (see [`THEME_CONFIG_NAME`]).
pub struct ShellTheme;

impl ConfigDecl for ShellTheme {
    const BINDING: &'static str = THEME_CONFIG_NAME;
    const NSID: &'static str = THEME_CONFIG_TAG;
    const KEY: &'static str = THEME_CONFIG_KEY;
    type Value = ThemeSheet;
}
