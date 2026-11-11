//! The `trapper` fixture's declarations, shared by the guest (`trapper_fixture.rs`) and the native
//! emitter of its sidecar (`emit_trapper_manifest.rs`), which each declare it as a plain `mod trapper_manifest;` (it sits beside their roots).
//! The Settings section row mirrors the counter's, so the shell lists it and mounts its `view`.
use clay_mdk::Attachment;
use clay_mdk::CollectionDecl;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Sidecar;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.trapper";
pub const VIEW: &str = "view";
/// `micro_settings_manifest::SECTIONS`, which this crate cannot name (it sits below the settings
/// micro). The shell's Settings reads rows of this contract.
const SECTIONS: &str = "org.example.dayframe.settings.sections";

/// The trapper's one presentation, published as [`VIEW`].
pub struct TrapperView;

impl clay_mdk::ComponentDecl for TrapperView {
    const NAME: &'static str = VIEW;
}

/// The settings sections collection as this crate can name it (it sits below the settings
/// micro): a section row is a title and its detail presentation.
struct Sections;

impl CollectionDecl for Sections {
    const NSID: &'static str = SECTIONS;
    type Key = String;
    type Value = Section;
}

#[derive(Serialize, Deserialize)]
struct Section {
    title: String,
    detail: Attachment,
}

pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .component::<TrapperView>()
        .writes::<Sections>("sections", Necessity::Optional)
        .contributes::<Sections>("settings-section", ID, |at| Section {
            title: "Trapper (fixture)".into(),
            detail: at.own::<TrapperView>(),
        })
        .build()
}

/// Only the sidecar emitter reads it; the guest (the wasm32 build of this file) links `manifest` alone.
#[cfg_attr(
    target_arch = "wasm32",
    expect(dead_code, reason = "only the native sidecar emitter calls it")
)]
pub fn sidecar() -> Sidecar {
    Sidecar::new(ID, 1, manifest())
}
