//! The shell's declaration types: its package, the presentations it publishes, its
//! arrangement facet and its panels collection.
use super::*;
use clay_mdk::CollectionCapability;
use clay_mdk::CollectionDecl;
use clay_mdk::ComponentDecl;
use clay_mdk::ConfigDecl;
use clay_mdk::DeclaredCollection;
use clay_mdk::PackageDecl;

/// The shell package's identity.
pub struct Shell;

impl PackageDecl for Shell {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The shell's frame, published as [`FRAME`].
pub struct ShellFrame;

impl ComponentDecl for ShellFrame {
    const NAME: &'static str = FRAME;
}

/// The home panel's presentation, published as [`HOME`].
pub struct ShellHome;

impl ComponentDecl for ShellHome {
    const NAME: &'static str = HOME;
}

/// The sidebar/back button, published as [`NAV_BUTTON`].
pub struct ShellNavButton;

impl ComponentDecl for ShellNavButton {
    const NAME: &'static str = NAV_BUTTON;
}

/// The arrangement weights facet.
pub struct ShellArrangement;

impl ConfigDecl for ShellArrangement {
    const BINDING: &'static str = CONFIG_NAME;
    const NSID: &'static str = CONFIG_TAG;
    const KEY: &'static str = CONFIG_KEY;
    type Value = ShellConfig;
}

/// The panels collection, keyed by registration id.
pub struct ShellPanels;

impl CollectionDecl for ShellPanels {
    const NSID: &'static str = PANELS;
    type Key = String;
    type Value = Panel;
}

impl DeclaredCollection for ShellPanels {
    const KEY_SCHEMA: &'static str = PANEL_KEY_SCHEMA;
    const VALUE_SCHEMA: &'static str = PANEL_VALUE_SCHEMA;
    const SELECTOR_SCHEMA: &'static str = PANEL_SELECTOR_SCHEMA;
    const OFFERED: &'static [CollectionCapability] =
        &[CollectionCapability::Read, CollectionCapability::Register];
}
