//! A configurable contributor micro used by Dayframe compositions. Authored against the MDK
//! only (ADR 015 §5): activation receives a [`Context`], never the registry or renderer.
#[cfg(test)]
mod tests;
mod view;

use clay_mdk::Attachment;
use clay_mdk::ComponentDecl;
use clay_mdk::ConfigDecl;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PackageDecl;
use clay_mdk::RowAttachment;
use clay_mdk::Sidecar;
use clay_mdk::host::DocumentService;
use dioxus::prelude::*;
use micro_layout_manifest::LayoutPanels;
use micro_layout_manifest::Panel;
use micro_settings_manifest::Section;
use micro_settings_manifest::SettingsSections;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.sample";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the sample holds. It declares no NSID, only requests the sections contract.
pub const AUTHORITY: &str = "org.example.dayframe.sample";
pub const CONFIG_NAME: &str = "sample";
pub const CONFIG_TAG: &str = "org.example.dayframe.sample.config";
pub const CONFIG_KEY: &str = "main";
/// The one publication: the presentation Settings mounts for this micro's section.
pub const DETAIL: &str = "detail";
/// The local binding name under which the sample registers its Settings section.
pub const SECTIONS_BINDING: &str = "sections";
/// The local binding name under which the sample contributes its layout panel.
pub const PANELS_BINDING: &str = "panels";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleConfig {
    pub enabled: bool,
    pub greeting: String,
}

/// The Sample package's identity.
pub struct Sample;

impl PackageDecl for Sample {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The sample's configuration facet.
pub struct SampleFacet;

impl ConfigDecl for SampleFacet {
    const BINDING: &'static str = CONFIG_NAME;
    const NSID: &'static str = CONFIG_TAG;
    const KEY: &'static str = CONFIG_KEY;
    type Value = SampleConfig;
}

/// The detail presentation, published as [`DETAIL`].
pub struct SampleDetailView;

impl ComponentDecl for SampleDetailView {
    const NAME: &'static str = DETAIL;
}

/// The Sample micro's compile-time declaration: it publishes its detail presentation and
/// declares read/write access to its own configuration facet. Its Settings section is written
/// at runtime, so it exists only while the micro runs.
pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .component::<SampleDetailView>()
        .config::<SampleFacet>()
        .writes::<SettingsSections>(SECTIONS_BINDING, Necessity::Required)
        .writes::<LayoutPanels>(PANELS_BINDING, Necessity::Optional)
        // The layout panel is declared, not written at runtime: it is listed while the sample is
        // stopped, and showing it starts the sample.
        .contributes::<LayoutPanels>("layout-panel", ID, |at| Panel {
            title: "Sample micro".into(),
            content: at.own::<SampleDetailView>(),
        })
        .build()
}

/// The Sample package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<Sample>(manifest())
}

/// Activation receives already-bound dependencies through the context; it cannot mint grants or
/// reach the mock node. The host validated the manifest's facet ACL before this runs. The
/// micro registers its Settings section with a registration writer, so the row lives exactly as
/// long as this incarnation.
pub async fn activate(context: Context) -> Result<(), Error> {
    let config = context
        .config::<SampleFacet>()?
        .expect("manifest declares the sample configuration binding");
    context.publish::<SampleDetailView>(move || {
        dioxus::prelude::rsx! { view::SampleDetail { config: config.clone() } }
    })?;
    context
        .registration::<SettingsSections>()
        .expect("configuration binds the sections collection")
        .put(
            &ID.to_owned(),
            &Section {
                title: "Sample micro".into(),
                detail: Attachment::at(0),
            },
            vec![RowAttachment::own::<SampleDetailView>()],
        )
        .await?;
    Ok(())
}

/// Seed the demo configuration value for `sample` on a composition whose catalog
/// installation already bound the micro's config document (the composition root binds
/// `<name>-config` per manifest grant). A launcher's demo seed; the runtime authoring
/// surface is [`activate`].
pub fn seed_mock_defaults(documents: &DocumentService) {
    documents.mock_documents().seed(
        clay_mdk::host::FacetRoute {
            document: "sample-config".into(),
            branch: "main".into(),
            tag: CONFIG_TAG.into(),
            key_id: CONFIG_KEY.into(),
        },
        serde_json::to_value(SampleConfig {
            enabled: true,
            greeting: "Hello from a micro".into(),
        })
        .unwrap(),
    );
}
