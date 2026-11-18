//! The shell contract (ADR 032 §1): the identity and manifest of the native shell micro, the
//! `shell.panels` collection other micros contribute panels to, the navigation calls they make
//! of the shell, and the configuration datum that holds the arrangement weights. Consumers
//! depend on this crate and never on the shell micro itself, so an alternative shell (the
//! agentic IDE's) can implement the same contract. Everything is under the authority
//! `org.example.dayframe.shell`.
//!
//! A panel row is keyed by its contributor's chosen *registration id* (CBOR text) and holds a
//! [`Panel`]: a title, the sockets it may fill, and lazy presentation attachments. Reading rows
//! never starts a contributor; the shell mounts a panel through a slot over its attachment,
//! which activates or joins the contributor for as long as the panel is shown.
//!
//! # Navigation
//!
//! The shell owns the navigation stack. Its entries name *what is shown* (a panel, or one row
//! a list panel opened), never a rendered surface, and are addressed by the keys below. A
//! micro asks for navigation through the calls ([`NAVIGATE`], [`BACK`], ...) it requires; it
//! never keeps a stack of its own, so back behaves the same everywhere. The calls' request and
//! response types are [`NavigateRequest`], [`OpenSheetRequest`] and plain CBOR `bool`/`String`.
//!
//! A list panel that opens rows names a [`Panel::details`] attachment: the presentation the
//! shell mounts in the details position while one of its rows is open. That presentation
//! learns *which* row by asking the shell ([`OPENED`]), because a presentation takes no
//! parameters and the shell, not the list, owns the stack.
use clay_mdk::Attachment;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::RootRow;
use clay_mdk::ShellRoots;
use clay_mdk::Sidecar;
use serde::Deserialize;
use serde::Serialize;

mod declarations;
pub use declarations::Shell;
pub use declarations::ShellArrangement;
pub use declarations::ShellFrame;
pub use declarations::ShellHome;
pub use declarations::ShellNavButton;
pub use declarations::ShellPanels;

pub const ID: &str = "dayframe.shell";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the shell holds: every NSID it declares (its collection contract and
/// schemas) lies under it, everything else it names is a request. The contracts of the calls
/// it publishes are under it too, which is what makes an alternative shell implement them.
pub const AUTHORITY: &str = "org.example.dayframe.shell";
pub const CONFIG_NAME: &str = "shell";
pub const CONFIG_TAG: &str = "org.example.dayframe.shell.config";
pub const CONFIG_KEY: &str = "main";

/// The shell's frame presentation, which the composition mounts as a (non-pinned) root.
pub const FRAME: &str = "frame";
/// The home panel's presentation, which the shell contributes to its own panels collection.
pub const HOME: &str = "home";
/// The sidebar/back button presentation (ADR 032 §5). A micro requires it under a binding of
/// this same name and mounts it in its own app bar through a presentation slot. It shows
/// **back** while the current context can go back and **open the sidebar** otherwise, and acts
/// on the shell's state.
pub const NAV_BUTTON: &str = "nav-button";
/// The contract NSID a requirement of [`NAV_BUTTON`] names.
pub const NAV_BUTTON_CONTRACT: &str = "org.example.dayframe.shell.navButton";

/// Calls, each published under its own name; a requirement binds one under the same name.
/// [`NAVIGATE`]: [`NavigateRequest`] in, CBOR `()` out.
pub const NAVIGATE: &str = "navigate";
/// [`BACK`] and [`FORWARD`]: CBOR `()` in, CBOR `bool` out (whether the stack moved).
pub const BACK: &str = "back";
pub const FORWARD: &str = "forward";
/// [`OPEN_SHEET`]: [`OpenSheetRequest`] in, CBOR `()` out. [`CLOSE_SHEET`]: `()` in, `bool`
/// out (whether a sheet was open). A sheet is on the stack, so [`BACK`] dismisses it as well.
pub const OPEN_SHEET: &str = "open-sheet";
pub const CLOSE_SHEET: &str = "close-sheet";
/// [`OPENED`]: a panel key (CBOR text) in, the key of the row that panel has open in the
/// details position (CBOR `text` or null) out.
pub const OPENED: &str = "opened";
pub const NAVIGATE_CONTRACT: &str = "org.example.dayframe.shell.navigate";
pub const BACK_CONTRACT: &str = "org.example.dayframe.shell.back";
pub const FORWARD_CONTRACT: &str = "org.example.dayframe.shell.forward";
pub const OPEN_SHEET_CONTRACT: &str = "org.example.dayframe.shell.openSheet";
pub const CLOSE_SHEET_CONTRACT: &str = "org.example.dayframe.shell.closeSheet";
pub const OPENED_CONTRACT: &str = "org.example.dayframe.shell.opened";

/// The contract NSID, and so the declared collection's id, of the panels collection.
pub const PANELS: &str = "org.example.dayframe.shell.panels";
/// The local binding name under which the shell reads its panels collection, and (by
/// convention) under which a contributor requests write access to it.
pub const PANELS_BINDING: &str = "panels";
/// The local binding name under which the shell writes its own home panel row.
pub const HOME_PANEL_BINDING: &str = "home-panel";
/// The local binding name under which the shell registers its root into `shell.roots`.
pub const ROOTS_BINDING: &str = "roots";
/// Schema NSIDs of [`PANELS`] keys (the contributor's registration id, text) and values
/// ([`Panel`]), and of its selector (the whole collection).
pub const PANEL_KEY_SCHEMA: &str = "org.example.dayframe.shell.panelKey";
pub const PANEL_VALUE_SCHEMA: &str = "org.example.dayframe.shell.panel";
pub const PANEL_SELECTOR_SCHEMA: &str = "org.example.dayframe.shell.panelSelector";

/// A place in the chrome a panel may fill (FDR 006 "sockets").
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Socket {
    /// The left navigation across micros: a drawer on a phone, a side panel on a desktop.
    Sidebar,
    /// The main position: a list, or any content that opens other panels. A panel that fills
    /// it is a destination the home panel and the sidebar offer.
    List,
    /// The details position, over the list on a phone and beside it on a desktop.
    Details,
    /// An auxiliary dismissable page that belongs to the panel that opened it (ADR 032 §6).
    Sheet,
}

/// One panel: what the shell lists without starting its contributor, and the presentations it
/// mounts when the panel is shown.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Panel {
    pub title: String,
    /// The sockets the panel may fill. A panel that is not [`Socket::List`] is never a
    /// destination; [`NavigateRequest`] to it opens it in the socket it does fill.
    pub sockets: Vec<Socket>,
    /// The panel's own presentation.
    pub content: Attachment,
    /// For a list panel whose rows open details: the presentation of one opened row (see the
    /// module docs). Absent for a panel whose rows open nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Attachment>,
    /// Whether this is the home panel, the list the shell shows when nothing is open. Exactly
    /// one installed panel is.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub home: bool,
}

/// [`NAVIGATE`]: open `panel` (no `row`), or one `row` of the list panel `panel` in the details
/// position. Opening another row of a list replaces the row it shows; opening a panel pushes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NavigateRequest {
    pub panel: String,
    #[serde(default)]
    pub row: Option<String>,
}

/// [`OPEN_SHEET`]: present the [`Socket::Sheet`] panel `panel`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenSheetRequest {
    pub panel: String,
}

/// The arrangement weights, kept in the shell's configuration document and so synced across
/// devices (FDR 006 "Sync and restore"). Only the ratios matter: `sidebar` against the
/// content, and within the content `list` against `details`. Open-context state (the stack,
/// what is open) is never part of it.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShellConfig {
    #[serde(default = "ShellConfig::default_sidebar")]
    pub sidebar: f64,
    #[serde(default = "ShellConfig::default_list")]
    pub list: f64,
    #[serde(default = "ShellConfig::default_details")]
    pub details: f64,
}

impl ShellConfig {
    fn default_sidebar() -> f64 {
        1.0
    }
    fn default_list() -> f64 {
        2.0
    }
    fn default_details() -> f64 {
        3.0
    }
}

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            sidebar: Self::default_sidebar(),
            list: Self::default_list(),
            details: Self::default_details(),
        }
    }
}

/// The shell's package declaration: it declares the panels collection, publishes its frame, its
/// home panel, its sidebar/back button and its navigation calls, contributes the home panel to
/// its own collection, and registers the frame as a shell root.
pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .component::<ShellFrame>()
        .component::<ShellHome>()
        .component::<ShellNavButton>()
        .call(NAVIGATE, NAVIGATE_CONTRACT)
        .call(BACK, BACK_CONTRACT)
        .call(FORWARD, FORWARD_CONTRACT)
        .call(OPEN_SHEET, OPEN_SHEET_CONTRACT)
        .call(CLOSE_SHEET, CLOSE_SHEET_CONTRACT)
        .call(OPENED, OPENED_CONTRACT)
        .config::<ShellArrangement>()
        .declares::<ShellPanels>()
        // The shell reads panels through a binding configuration resolves; a second write
        // binding to the same contract carries its own home panel. It also registers its frame
        // into the runtime-defined `shell.roots` collection, which the runtime satisfies.
        .reads::<ShellPanels>(PANELS_BINDING, Necessity::Required)
        .writes::<ShellPanels>(HOME_PANEL_BINDING, Necessity::Required)
        .writes::<ShellRoots>(ROOTS_BINDING, Necessity::Required)
        .contributes::<ShellPanels>("home-panel", ID, |at| Panel {
            title: "Home".into(),
            sockets: vec![Socket::List],
            content: at.own::<ShellHome>(),
            details: None,
            home: true,
        })
        .contributes::<ShellRoots>("root", ID, |at| RootRow {
            title: "Shell".into(),
            content: at.own::<ShellFrame>(),
        })
        .build()
}

/// The shell package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<Shell>(manifest())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_mdk::CAPABILITY_ATTACHMENT_TAG;
    use clay_mdk::host::declared_rows;

    // The manifest is wire data (ADR 030 §11); the snapshot follows the current shape so drift
    // is a reviewable diff.
    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }

    #[test]
    fn panel_row_wire_shape_is_reviewable() {
        let mut bytes = Vec::new();
        ciborium::into_writer(
            &Panel {
                title: "Settings".into(),
                sockets: vec![Socket::List],
                content: Attachment::at(0),
                details: Some(Attachment::at(1)),
                home: false,
            },
            &mut bytes,
        )
        .unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        insta::assert_snapshot!(hex);
    }

    #[test]
    fn the_manifest_declares_a_home_panel_and_a_frame_root() {
        sidecar().validate().unwrap();
        let rows = declared_rows(&clay_mdk::MicroId::local(ID), &sidecar()).unwrap();
        assert_eq!(
            rows.keys().collect::<Vec<_>>(),
            [HOME_PANEL_BINDING, ROOTS_BINDING]
        );
    }

    // A contributor writes the row it wants: the optional fields are absent from a minimal
    // panel and read as their defaults.
    #[test]
    fn a_panel_without_details_or_home_reads_as_neither() {
        use ciborium::Value;
        let text = |text: &str| Value::Text(text.into());
        let row = Value::Map(vec![
            (text("title"), text("Files")),
            (
                text("sockets"),
                Value::Array(vec![text("sidebar"), text("list")]),
            ),
            (
                text("content"),
                Value::Tag(
                    CAPABILITY_ATTACHMENT_TAG,
                    Box::new(Value::Integer(0.into())),
                ),
            ),
        ]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&row, &mut bytes).unwrap();
        let panel: Panel = ciborium::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(panel.sockets, [Socket::Sidebar, Socket::List]);
        assert_eq!(panel.content, Attachment::at(0));
        assert_eq!(panel.details, None);
        assert!(!panel.home);
    }

    #[test]
    fn a_configuration_without_weights_reads_as_the_defaults() {
        assert_eq!(
            serde_json::from_value::<ShellConfig>(serde_json::json!({})).unwrap(),
            ShellConfig::default()
        );
    }
}
