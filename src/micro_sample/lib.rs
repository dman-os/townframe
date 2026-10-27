//! A configurable contributor micro used by Dayframe compositions. Authored against the MDK
//! only (ADR 015 §5): activation receives a [`Context`], never the registry or renderer.
#[cfg(test)]
mod tests;
mod view;

use clay_mdk::ComponentRef;
use clay_mdk::ConfigGrant;
use clay_mdk::FacetAccess;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::Provision;
use clay_mdk::ProvisionRef;
use clay_mdk::native::AccessError;
use clay_mdk::native::Context;
use clay_mdk::native::DocumentService;
use clay_mdk::native::Facet;
use clay_mdk::native::FacetValue;
use clay_mdk::native::Instance;
use clay_mdk::native::RendererRegistry;
use dioxus::prelude::*;
use micro_settings_manifest::SettingsSection;
use micro_settings_manifest::SettingsSections;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.sample";
pub const CONFIG_NAME: &str = "sample";
pub const CONFIG_TAG: &str = "org.dayframe.sample.config";
pub const CONFIG_KEY: &str = "main";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleConfig {
    pub enabled: bool,
    pub greeting: String,
}

#[derive(Clone)]
pub struct SampleMicro {
    pub instance: Instance,
    config: Facet<SampleConfig>,
    detail: ComponentRef,
}

impl PartialEq for SampleMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl Provision for SampleMicro {
    const ID: &'static str = "sample.controller";
}

/// The Sample micro's compile-time declaration: it requires the Settings sections collection,
/// provides its controller, and declares read/write access to its own configuration facet.
pub fn manifest() -> MicroManifest {
    MicroManifest {
        id: MicroId::new(ID),
        version: 1,
        requirements: vec![ProvisionRef::of::<SettingsSections>()],
        provisions: vec![ProvisionRef {
            id: SampleMicro::ID.into(),
            version: 1,
        }],
        config: Some(ConfigGrant {
            name: CONFIG_NAME.into(),
            facets: vec![FacetAccess {
                tag: CONFIG_TAG.into(),
                key_id: Some(CONFIG_KEY.into()),
                read: true,
                write: true,
                ..Default::default()
            }],
        }),
        contributions: vec![],
    }
}

/// Activation receives already-bound dependencies through the context; it cannot mint grants or
/// reach the mock node. The host validated the manifest's facet ACL before this runs.
pub fn activate<R: RendererRegistry<Render = Element>>(
    context: &Context<R>,
) -> Result<(), AccessError> {
    let instance = context.instance();
    let settings = context.require::<SettingsSections>()?.bind(&instance)?;
    let config_document = context
        .config(CONFIG_NAME)
        .expect("manifest declares the sample configuration binding");
    let config = config_document.facet::<SampleConfig>(CONFIG_TAG.into(), CONFIG_KEY.into())?;
    let content = config.clone();
    let detail = context.publish_component(move || {
        dioxus::prelude::rsx! { view::SampleDetail { config: content.clone() } }
    })?;
    settings.insert(SettingsSection {
        title: "Sample micro".into(),
        detail,
    })?;
    context.publish(SampleMicro {
        instance,
        config,
        detail,
    });
    Ok(())
}

impl SampleMicro {
    pub fn detail(&self) -> ComponentRef {
        self.detail
    }

    pub fn watch(&self) -> &clay_mdk::native::DocumentWatch {
        &self.config.watch
    }

    pub fn state(&self, snapshot: &clay_mdk::native::DocumentSnapshot) -> FacetValue<SampleConfig> {
        self.config.state(snapshot)
    }

    pub fn decode(&self, snapshot: &clay_mdk::native::DocumentSnapshot) -> Option<SampleConfig> {
        self.config.value(snapshot)
    }

    pub fn edit(&self, value: SampleConfig) -> Result<clay_mdk::native::Operation, AccessError> {
        self.config.write(value)
    }
}

/// Bind and seed the mock configuration document for `sample` on the composition root's mock
/// document service. A composition-time helper; the runtime authoring surface is [`activate`].
pub fn bind_mock_defaults(documents: &DocumentService) {
    documents.mock_documents().bind_config(
        CONFIG_NAME.into(),
        "sample-config".into(),
        "main".into(),
    );
    documents.mock_documents().seed(
        clay_mdk::native::FacetRoute {
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
