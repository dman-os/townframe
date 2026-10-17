//! Settings owns its section vocabulary and navigation; the host owns storage and authority.
mod view;
pub use view::SettingsView;

pub mod contract;
use crate::host::{CollectionReader, CollectionWriter, HostedCollection, Instance, RootMount};
use contract::{SettingsSection, SettingsSections};

#[derive(Clone)]
pub struct SettingsMicro {
    pub instance: Instance,
    pub sections: CollectionReader<SettingsSection>,
    collection: HostedCollection<SettingsSection>,
}

/// Settings requires storage for its own vocabulary and a granted root-presentation writer.
pub struct SettingsContext {
    pub instance: Instance,
    pub sections: HostedCollection<SettingsSection>,
    pub roots: CollectionWriter<RootMount>,
}

impl SettingsMicro {
    pub fn activate(
        context: SettingsContext,
        native: &crate::native::NativeComponents,
    ) -> Result<Self, crate::host::AccessError> {
        let micro = Self {
            sections: context.sections.reader(&context.instance),
            instance: context.instance,
            collection: context.sections,
        };
        let presentation = micro.clone();
        let components = native.weak_context();
        let root = native.publish(&micro.instance, move || {
            use dioxus::prelude::*;
            rsx! { SettingsView { micro: presentation.clone(), components: components() } }
        })?;
        // Both the typed collection and root content must be published before activation succeeds.
        context.roots.insert(RootMount {
            title: "Settings".into(),
            content: root,
        })?;
        Ok(micro)
    }

    /// Publish the contribution facility separately from this micro's presentation controller.
    pub fn sections_output(&self) -> SettingsSections {
        SettingsSections {
            collection: self.collection.clone(),
            provider: self.instance.clone(),
        }
    }
}

impl super::Provision for SettingsMicro {
    const ID: &'static str = "settings.controller";
}

/// Native adapter owns Settings' context construction; composition only supplies host facilities.
pub fn provider(
    native: crate::native::NativeComponents,
    roots: HostedCollection<RootMount>,
) -> super::MicroFactory {
    use super::{MicroDescriptor, MicroFactory, MicroId, MicroProvision, Provision, ProvisionKind};
    MicroFactory::new(
        MicroDescriptor {
            id: MicroId::new("dayframe.settings"),
            version: 1,
            requirements: vec![],
            provisions: vec![
                MicroProvision {
                    id: SettingsSections::ID.into(),
                    version: 1,
                    kind: ProvisionKind::Context,
                },
                MicroProvision {
                    id: SettingsMicro::ID.into(),
                    version: 1,
                    kind: ProvisionKind::Context,
                },
            ],
        },
        move |activation| {
            let instance = activation.instance();
            let micro = SettingsMicro::activate(
                SettingsContext {
                    sections: HostedCollection::new(&activation.host(), &instance),
                    roots: roots.writer(&instance),
                    instance,
                },
                &native,
            )?;
            activation.publish(micro.sections_output());
            activation.publish(micro);
            Ok(())
        },
    )
}
