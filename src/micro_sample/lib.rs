//! A configurable contributor micro used by Dayframe compositions. Authored against the MDK
//! only (ADR 015 §5): activation receives a [`Context`], never the registry or renderer.
#[cfg(test)]
mod tests;
mod view;

use clay_mdk::Attachment;
use clay_mdk::BindingResolution;
use clay_mdk::CollectionAccess;
use clay_mdk::CollectionRequest;
use clay_mdk::ConfigurationRequest;
use clay_mdk::Context;
use clay_mdk::Entrypoints;
use clay_mdk::Error;
use clay_mdk::FacetAccess;
use clay_mdk::MANIFEST_SCHEMA;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::RowAttachment;
use clay_mdk::host::DocumentService;
use dioxus::prelude::*;
use micro_settings_manifest::SECTIONS;
use micro_settings_manifest::Section;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.sample";
pub const CONFIG_NAME: &str = "sample";
pub const CONFIG_TAG: &str = "org.dayframe.sample.config";
pub const CONFIG_KEY: &str = "main";
/// The one publication: the presentation Settings mounts for this micro's section.
pub const DETAIL: &str = "detail";
/// The local binding name under which the sample registers its Settings section.
pub const SECTIONS_BINDING: &str = "sections";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleConfig {
    pub enabled: bool,
    pub greeting: String,
}

/// The Sample micro's compile-time declaration: it publishes its detail presentation and
/// declares read/write access to its own configuration facet. Its Settings section is written
/// at runtime, so it exists only while the micro runs.
pub fn manifest() -> MicroManifest {
    MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: MicroId::new(ID),
        package_version: 1,
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
                tag: CONFIG_TAG.into(),
                key_id: Some(CONFIG_KEY.into()),
                read: true,
                write: true,
                ..Default::default()
            }],
        }],
        collection_contracts: vec![],
        collection_requests: vec![CollectionRequest {
            binding: SECTIONS_BINDING.into(),
            contract: SECTIONS.into(),
            access: CollectionAccess::Write,
            necessity: Necessity::Required,
            resolution: BindingResolution::Configured,
        }],
        contributions: vec![],
        extra: Default::default(),
    }
}

/// Activation receives already-bound dependencies through the context; it cannot mint grants or
/// reach the mock node. The host validated the manifest's facet ACL before this runs. The
/// micro registers its Settings section with a registration writer, so the row lives exactly as
/// long as this incarnation.
pub async fn activate(context: Context) -> Result<(), Error> {
    let config = context
        .config(CONFIG_NAME)
        .expect("manifest declares the sample configuration binding")
        .facet::<SampleConfig>(CONFIG_TAG, CONFIG_KEY)?;
    let detail = context.publish_component(DETAIL, move || {
        dioxus::prelude::rsx! { view::SampleDetail { config: config.clone() } }
    })?;
    context
        .registration::<String, Section>(SECTIONS_BINDING)
        .expect("configuration binds the sections collection")
        .put(
            &ID.to_owned(),
            &Section {
                title: "Sample micro".into(),
                detail: Attachment::at(0),
            },
            vec![RowAttachment::Bound(detail)],
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
