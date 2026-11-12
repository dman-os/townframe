//! Engine-neutral Counter declarations. The manifest is portable MDK vocabulary (ADR 030
//! §§11–15): identity, the presentations and call it publishes, the configuration it requests, and the
//! Settings section row it declares into the `settings.sections` collection, whose attachment
//! is a lazy endpoint to the counter's own presentation, and the Increment button row it
//! declares into the `toolbar.buttons` collection, whose attachment is a lazy endpoint to the
//! counter's `increment` call, and the panel row it declares into the `layout.panels` collection,
//! whose attachment is a lazy endpoint to the counter's `view`. The wasm guest reads only the
//! constants; the composition root reads `manifest()`.
use clay_mdk::ComponentDecl;
use clay_mdk::ConfigDecl;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PackageDecl;
use clay_mdk::Sidecar;
use micro_layout_manifest::LayoutPanels;
use micro_layout_manifest::Panel;
use micro_settings_manifest::Section;
use micro_settings_manifest::SettingsSections;
use micro_toolbar_manifest::ACTION_CONTRACT;
use micro_toolbar_manifest::Button;
use micro_toolbar_manifest::ToolbarButtons;

pub const ID: &str = "dayframe.counter";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the counter holds: the contract of its [`COUNT`] call lies under it.
pub const AUTHORITY: &str = "org.example.dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.example.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";
/// The counter's full presentation, the one its Settings section mounts.
pub const VIEW: &str = "view";
/// A compact presentation of the same count, for any micro that wants to embed it.
pub const BADGE: &str = "badge";
/// The call that answers (CBOR `()` in, CBOR `u64` out) the counter's current value.
pub const COUNT: &str = "count";
/// The contract NSID of the [`COUNT`] call.
pub const COUNT_CONTRACT: &str = "org.example.dayframe.counter.count";
/// The local binding name under which the counter registers its Settings section.
pub const SECTIONS_BINDING: &str = "sections";
/// The call the toolbar's Increment button performs (CBOR `()` in, CBOR `()` out): count one more.
/// It implements the toolbar's action contract, which the toolbar owns.
pub const INCREMENT: &str = "increment";
/// The local binding name under which the counter contributes its toolbar button.
pub const TOOLBAR_BINDING: &str = "toolbar";
/// The local binding name under which the counter contributes its layout panel.
pub const PANELS_BINDING: &str = "panels";

/// The counter package's identity.
pub struct Counter;

impl PackageDecl for Counter {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The counter's configuration datum: the count, a plain `u64`.
pub struct CounterConfig;

impl ConfigDecl for CounterConfig {
    const BINDING: &'static str = CONFIG_NAME;
    const NSID: &'static str = CONFIG_TAG;
    const KEY: &'static str = CONFIG_KEY;
    type Value = u64;
}

/// The counter's full presentation, published as [`VIEW`].
pub struct CounterView;

impl ComponentDecl for CounterView {
    const NAME: &'static str = VIEW;
}

/// The counter's compact presentation, published as [`BADGE`].
pub struct CounterBadge;

impl ComponentDecl for CounterBadge {
    const NAME: &'static str = BADGE;
}

pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .component::<CounterView>()
        .component::<CounterBadge>()
        .call(COUNT, COUNT_CONTRACT)
        .call(INCREMENT, ACTION_CONTRACT)
        .config::<CounterConfig>()
        .writes::<SettingsSections>(SECTIONS_BINDING, Necessity::Optional)
        .writes::<ToolbarButtons>(TOOLBAR_BINDING, Necessity::Optional)
        .writes::<LayoutPanels>(PANELS_BINDING, Necessity::Optional)
        .contributes::<SettingsSections>("settings-section", ID, |at| Section {
            title: "Counter (Wasm)".into(),
            detail: at.own::<CounterView>(),
        })
        .contributes::<ToolbarButtons>("toolbar-button", INCREMENT, |at| Button {
            label: "Increment".into(),
            icon: "plus".into(),
            action: at.by_name(INCREMENT),
        })
        .contributes::<LayoutPanels>("layout-panel", ID, |at| Panel {
            title: "Counter (Wasm)".into(),
            content: at.own::<CounterView>(),
        })
        .build()
}

/// The counter package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<Counter>(manifest())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── manifest wire shape: the manifest is wire data (ADR 030 §11), including the declared
    //    contribution payload and its activation default; the snapshot follows the current
    //    shape so drift is a reviewable diff ─────────────────────────────────────────────────

    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }
}
