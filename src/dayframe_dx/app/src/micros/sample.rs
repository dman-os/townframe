//! A configurable contributor micro used by the desktop composition.
#[cfg(test)]
mod tests;
mod view;

use crate::{
    documents::{DocumentRef, DocumentSnapshot, DocumentWatch, Facet},
    host::{AccessError, ComponentRef, Instance},
    micros::settings::contract::{SettingsContribution, SettingsSection},
    native::NativeComponents,
};
use dioxus::prelude::*;
use serde::{Deserialize, Serialize};

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

/// Activation receives already-bound dependencies; it cannot mint grants or reach the mock node.
pub struct SampleContext {
    pub instance: Instance,
    pub settings: SettingsContribution,
    pub config_document: DocumentRef,
}

impl SampleMicro {
    pub fn activate(
        context: SampleContext,
        native: &NativeComponents,
    ) -> Result<Self, AccessError> {
        let config = context
            .config_document
            .facet::<SampleConfig>(CONFIG_TAG.into(), CONFIG_KEY.into())?;
        let content = config.clone();
        let detail = native.publish(&context.instance, move || {
            dioxus::prelude::rsx! { view::SampleDetail { config: content.clone() } }
        })?;
        context.settings.insert(SettingsSection {
            title: "Sample micro".into(),
            detail,
        })?;
        Ok(Self {
            instance: context.instance,
            config,
            detail,
        })
    }

    pub fn detail(&self) -> ComponentRef {
        self.detail
    }

    pub fn watch(&self) -> &DocumentWatch {
        &self.config.watch
    }

    pub fn decode(&self, snapshot: &DocumentSnapshot) -> Option<SampleConfig> {
        self.config.value(snapshot)
    }

    pub fn edit(&self, value: SampleConfig) -> Result<u64, AccessError> {
        self.config.write(value)
    }

    pub fn close(&self) {
        self.instance.close();
    }
}

impl super::Provision for SampleMicro {
    const ID: &'static str = "sample.controller";
}

/// Native adapter resolves declared dependencies and grants this micro's manifest-shaped access.
/// The feature itself still receives only SampleContext, never the registry or privileged host.
pub fn provider(native: NativeComponents) -> super::MicroFactory {
    use super::settings::contract::SettingsSections;
    use super::{MicroDescriptor, MicroFactory, MicroId, MicroProvision, Provision, ProvisionKind};
    MicroFactory::new(
        MicroDescriptor {
            id: MicroId::new("dayframe.sample"),
            version: 1,
            requirements: vec![MicroProvision {
                id: SettingsSections::ID.into(),
                version: 1,
                kind: ProvisionKind::Context,
            }],
            provisions: vec![MicroProvision {
                id: SampleMicro::ID.into(),
                version: 1,
                kind: ProvisionKind::Context,
            }],
        },
        move |activation| {
            let instance = activation.instance();
            let sections = activation.require::<SettingsSections>()?;
            let settings = sections.bind(&instance)?;
            let mut config = activation.host().grant_config(
                &instance,
                "sample",
                vec![daybook_types::manifest::RoutineFacetAccess {
                    tag: CONFIG_TAG.into(),
                    key_id: Some(CONFIG_KEY.into()),
                    read: true,
                    write: true,
                    ..Default::default()
                }],
            )?;
            let micro = SampleMicro::activate(
                SampleContext {
                    settings,
                    instance,
                    config_document: config.remove("sample").unwrap(),
                },
                &native,
            )?;
            activation.publish(micro);
            Ok(())
        },
    )
}
