//! The Settings contract (ADR 015 §9): the identity and manifest of the Settings micro, and the
//! `settings.sections` collection other micros register their sections into. Consumers depend on
//! this crate and never on the Settings micro itself.
//!
//! A section row is keyed by the contributing micro's id (CBOR text) and holds a [`Section`]: a
//! title, and an attachment for the section's detail presentation. A micro whose section should
//! exist while it is stopped declares the row in its manifest, which stores an endpoint to its
//! own presentation; a micro that only contributes while running writes the row at runtime with
//! a bound reference. Settings treats both alike.
//!
//! Settings is a **list panel** of the shell (ADR 032 §8): it contributes one `shell.panels` row
//! whose content is the list of its sections and whose details presentation shows the section
//! the shell has open. It arranges nothing itself; the shell owns the stack, the layout and the
//! back button, which Settings requires from it ([`SHELL_OPENED`], [`SHELL_NAV_BUTTON`]). Opening
//! a row is the list scaffold's `ListRow`, which reaches the shell's navigation through the
//! scaffold, so Settings never calls `navigate` itself.
use clay_mdk::Attachment;
use clay_mdk::CollectionCapability;
use clay_mdk::CollectionDecl;
use clay_mdk::ComponentDecl;
use clay_mdk::ConfigDecl;
use clay_mdk::DeclaredCollection;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PackageDecl;
use clay_mdk::Sidecar;
use micro_shell_manifest::ShellPanels;
use micro_shell_manifest::Socket;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;

pub const ID: &str = "dayframe.settings";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix Settings holds: every NSID it declares (its collection contract and
/// schemas) lies under it, everything else it names is a request.
pub const AUTHORITY: &str = "org.example.dayframe.settings";
pub const CONFIG_NAME: &str = "settings";
pub const CONFIG_TAG: &str = "org.example.dayframe.settings.config";
pub const CONFIG_KEY: &str = "main";
/// The Settings list: the content of its panel, the sections and the viewer's preferences.
pub const LIST: &str = "list";
/// The presentation of the section the shell has open, mounted in the details position.
pub const SECTION: &str = "section";
/// The contract NSID, and so the declared collection's id, of the sections collection.
pub const SECTIONS: &str = "org.example.dayframe.settings.sections";
/// The local binding name under which Settings reads its sections collection.
pub const SECTIONS_BINDING: &str = "sections";
/// The local binding name under which Settings contributes its panel to `shell.panels`.
pub const PANELS_BINDING: &str = "panels";
/// The local binding names under which Settings requires what it uses of the shell. Each is
/// the name of the shell publication it binds, so a catalog satisfies all of them from one
/// provider (`InstalledMicro::satisfied_by`).
pub const SHELL_OPENED: &str = micro_shell_manifest::OPENED;
pub const SHELL_NAV_BUTTON: &str = micro_shell_manifest::NAV_BUTTON;
/// Schema NSIDs of [`SECTIONS`] keys (the contributing micro's id, text) and values
/// ([`Section`]), and of its selector (the whole collection).
pub const SECTION_KEY_SCHEMA: &str = "org.example.dayframe.settings.sectionKey";
pub const SECTION_VALUE_SCHEMA: &str = "org.example.dayframe.settings.section";
pub const SECTION_SELECTOR_SCHEMA: &str = "org.example.dayframe.settings.sectionSelector";

/// One Settings section: its title, and the presentation published for its detail, as an
/// attachment of the row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Section {
    pub title: String,
    pub detail: Attachment,
}

/// The viewer's own preferences, kept in its configuration document: which sections are hidden
/// and which come first, by contributing micro id.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SettingsConfig {
    #[serde(default)]
    pub hidden: BTreeSet<String>,
    #[serde(default)]
    pub order: Vec<String>,
}

impl SettingsConfig {
    pub fn rank(&self, micro: &str) -> usize {
        self.order
            .iter()
            .position(|name| name == micro)
            .unwrap_or(usize::MAX)
    }
}

/// The Settings package's identity.
pub struct Settings;

impl PackageDecl for Settings {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The Settings list presentation, published as [`LIST`].
pub struct SettingsListView;

impl ComponentDecl for SettingsListView {
    const NAME: &'static str = LIST;
}

/// The presentation of the section the shell has open, published as [`SECTION`].
pub struct SettingsSectionView;

impl ComponentDecl for SettingsSectionView {
    const NAME: &'static str = SECTION;
}

/// The viewer's preferences facet.
pub struct SettingsPreferences;

impl ConfigDecl for SettingsPreferences {
    const BINDING: &'static str = CONFIG_NAME;
    const NSID: &'static str = CONFIG_TAG;
    const KEY: &'static str = CONFIG_KEY;
    type Value = SettingsConfig;
}

/// The sections collection, keyed by the contributing micro's id.
pub struct SettingsSections;

impl CollectionDecl for SettingsSections {
    const NSID: &'static str = SECTIONS;
    type Key = String;
    type Value = Section;
}

impl DeclaredCollection for SettingsSections {
    const KEY_SCHEMA: &'static str = SECTION_KEY_SCHEMA;
    const VALUE_SCHEMA: &'static str = SECTION_VALUE_SCHEMA;
    const SELECTOR_SCHEMA: &'static str = SECTION_SELECTOR_SCHEMA;
    const OFFERED: &'static [CollectionCapability] =
        &[CollectionCapability::Read, CollectionCapability::Register];
}

/// The Settings micro's package declaration: it declares the sections collection, publishes its
/// list and section presentations, requires the shell's navigation, and contributes its panel
/// row to the shell.
pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .component::<SettingsListView>()
        .component::<SettingsSectionView>()
        .requires(
            SHELL_OPENED,
            micro_shell_manifest::OPENED_CONTRACT,
            Necessity::Required,
        )
        .requires(
            SHELL_NAV_BUTTON,
            micro_shell_manifest::NAV_BUTTON_CONTRACT,
            Necessity::Required,
        )
        .config::<SettingsPreferences>()
        .declares::<SettingsSections>()
        // Settings reads sections through a binding configuration resolves; with one declared
        // sections collection the default binds it. It also writes its panel row into the
        // shell's panels collection.
        .reads::<SettingsSections>(SECTIONS_BINDING, Necessity::Required)
        .writes::<ShellPanels>(PANELS_BINDING, Necessity::Required)
        // Declared, not written at runtime: the shell lists Settings on home while it is
        // stopped, and opening it is what starts it.
        .contributes::<ShellPanels>("panel", ID, |at| micro_shell_manifest::Panel {
            title: "Settings".into(),
            sockets: vec![Socket::List],
            content: at.own::<SettingsListView>(),
            details: Some(at.own::<SettingsSectionView>()),
            home: false,
        })
        .build()
}

/// The Settings package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<Settings>(manifest())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_mdk::host::declared_rows;

    // The manifest is wire data (ADR 030 §11); the snapshot follows the current shape so drift
    // is a reviewable diff.
    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }

    #[test]
    fn section_row_wire_shape_is_reviewable() {
        let mut bytes = Vec::new();
        ciborium::into_writer(
            &Section {
                title: "Counter (Wasm)".into(),
                detail: Attachment::at(0),
            },
            &mut bytes,
        )
        .unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        insta::assert_snapshot!(hex);
    }

    #[test]
    fn the_manifest_declares_a_settings_panel_row() {
        sidecar().validate().unwrap();
        let rows = declared_rows(&clay_mdk::MicroId::local(ID), &sidecar()).unwrap();
        assert_eq!(rows.keys().collect::<Vec<_>>(), [PANELS_BINDING]);
    }
}
