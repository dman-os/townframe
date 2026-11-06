//! A configurable contributor micro used by Dayframe compositions. Authored against the MDK
//! only (ADR 015 §5): activation receives a [`Context`], never the registry or renderer.
#[cfg(test)]
mod tests;
mod view;

use clay_mdk::Access;
use clay_mdk::Attachment;
use clay_mdk::AttachmentDeclaration;
use clay_mdk::BindingResolution;
use clay_mdk::ComponentDecl;
use clay_mdk::ConfigDecl;
use clay_mdk::ConfigurationRequest;
use clay_mdk::Context;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::Error;
use clay_mdk::FacetAccess;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Request;
use clay_mdk::RowAttachment;
use clay_mdk::Sidecar;
use clay_mdk::host::DocumentService;
use dioxus::prelude::*;
use micro_layout_manifest::PANELS;
use micro_settings_manifest::SECTIONS;
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
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: DETAIL.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
        configuration_requests: vec![ConfigurationRequest {
            binding: CONFIG_NAME.into(),
            facets: vec![FacetAccess {
                tag: Nsid::literal(CONFIG_TAG),
                key_id: Some(CONFIG_KEY.into()),
                read: true,
                write: true,
                create: false,
                delete: false,
            }],
        }],
        collection_contracts: vec![],
        collection_requests: vec![
            Request {
                binding: SECTIONS_BINDING.into(),
                contract: Nsid::literal(SECTIONS),
                access: Some(Access::Write),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
            Request {
                binding: PANELS_BINDING.into(),
                contract: Nsid::literal(PANELS),
                access: Some(Access::Write),
                necessity: Necessity::Optional,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
        ],
        // The layout panel is declared, not written at runtime: it is listed while the sample is
        // stopped, and showing it starts the sample.
        contributions: vec![ContributionDeclaration {
            id: "layout-panel".into(),
            binding: PANELS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Sample micro",
                "content": { "$attachment": 0 },
            }),
            attachments: vec![AttachmentDeclaration {
                index: 0,
                publication: DETAIL.into(),
            }],
        }],
        extra: Default::default(),
    }
}

/// The Sample package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::new(ID, PACKAGE_VERSION, manifest()).with_authority(AUTHORITY)
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
