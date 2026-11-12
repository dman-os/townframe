//! Engine-neutral declarations of the Lab micro: the claytxt lane's experiments, one panel.
//! The lab publishes one presentation and declares one row into the `layout.panels`
//! collection, whose attachment is a lazy endpoint to that presentation, so the layout lists
//! the panel without starting the lab. It holds no configuration and no calls: an experiment
//! that needs either grows the manifest when it does. The wasm guest reads only the constants;
//! the composition root reads `manifest()`.
use clay_mdk::ComponentDecl;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PackageDecl;
use clay_mdk::Sidecar;
use micro_layout_manifest::LayoutPanels;
use micro_layout_manifest::Panel;

pub const ID: &str = "dayframe.lab";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the lab holds. It declares no contracts yet; the prefix is what its future
/// calls and schemas will lie under.
pub const AUTHORITY: &str = "org.example.dayframe.lab";
/// The lab's one presentation: the list of experiments and whichever one is open.
pub const VIEW: &str = "view";
/// The local binding name under which the lab contributes its layout panel.
pub const PANELS_BINDING: &str = "panels";

/// The lab package's identity.
pub struct Lab;

impl PackageDecl for Lab {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The lab's presentation, published as [`VIEW`].
pub struct LabView;

impl ComponentDecl for LabView {
    const NAME: &'static str = VIEW;
}

pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .component::<LabView>()
        .writes::<LayoutPanels>(PANELS_BINDING, Necessity::Optional)
        .contributes::<LayoutPanels>("layout-panel", ID, |at| Panel {
            title: "Lab".into(),
            content: at.own::<LabView>(),
        })
        .build()
}

/// The lab package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<Lab>(manifest())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The manifest is wire data (ADR 030 §11); the snapshot follows the current shape so drift
    // is a reviewable diff.
    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }
}
