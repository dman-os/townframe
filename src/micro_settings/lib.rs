//! Settings owns its section vocabulary and navigation; the host owns storage and authority.
mod view;
pub use view::SettingsView;

use clay_compose::micros::{MicroDescriptor, MicroFactory, MicroId, MicroProvision, Provision};
use clay_dioxus::NativeComponents;
use clay_mdk::native::{
    AccessError, CollectionReader, CollectionWriter, HostedCollection, Instance, RootMount,
};
use contract::{SettingsSection, SettingsSections};
pub use micro_settings_manifest as contract;

#[derive(Clone)]
pub struct SettingsMicro {
    pub instance: Instance,
    pub sections: CollectionReader<SettingsSection>,
}

/// Settings requires storage for its own vocabulary and a granted root-presentation writer.
pub struct SettingsContext {
    pub instance: Instance,
    pub sections: HostedCollection<SettingsSection>,
    pub roots: CollectionWriter<RootMount>,
}

impl SettingsMicro {
    pub fn activate(
        // The native provider adapter below constructs the context from host facilities
        // during activation; the micro never assembles its own grants.
        context: SettingsContext,
        native: &NativeComponents,
    ) -> Result<Self, AccessError> {
        let micro = Self {
            sections: context.sections.reader(&context.instance),
            instance: context.instance,
        };
        let presentation = micro.clone();
        let root = native.publish(&micro.instance, move || {
            use dioxus::prelude::*;
            rsx! { SettingsView { micro: presentation.clone() } }
        })?;
        context.roots.insert(RootMount {
            title: "Settings".into(),
            content: root,
        })?;
        Ok(micro)
    }
}

impl Provision for SettingsMicro {
    const ID: &'static str = "settings.controller";
}

/// Native adapter owns Settings' context construction; composition only supplies host facilities.
pub fn provider(native: NativeComponents, roots: HostedCollection<RootMount>) -> MicroFactory {
    MicroFactory::new(
        MicroDescriptor {
            id: MicroId::new("dayframe.settings"),
            version: 1,
            requirements: vec![],
            provisions: vec![
                MicroProvision {
                    id: SettingsSections::ID.into(),
                    version: 1,
                },
                MicroProvision {
                    id: SettingsMicro::ID.into(),
                    version: 1,
                },
            ],
        },
        move |activation| {
            let instance = activation.instance();
            let sections = activation.collection()?;
            let output = SettingsSections::new(sections.clone());
            let micro = SettingsMicro::activate(
                SettingsContext {
                    sections,
                    roots: roots.writer(&instance)?,
                    instance,
                },
                &native,
            )?;
            activation.publish(output);
            activation.publish(micro);
            Ok(())
        },
    )
}
