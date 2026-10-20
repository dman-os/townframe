//! Settings owns its section vocabulary and navigation; the host owns storage and authority.
//! Authored against the MDK only (ADR 015 §5): activation receives a [`Context`], never the
//! registry, renderer, or host kernel.
mod view;
pub use view::SettingsView;

use clay_mdk::Provision;
use clay_mdk::native::{AccessError, CollectionReader, Context, Instance};
use contract::{SettingsSection, SettingsSections};
pub use micro_settings_manifest as contract;

#[derive(Clone)]
pub struct SettingsMicro {
    pub instance: Instance,
    pub sections: CollectionReader<SettingsSection>,
}

impl PartialEq for SettingsMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl Provision for SettingsMicro {
    const ID: &'static str = "settings.controller";
}

/// Settings requires storage for its own vocabulary and a granted root-presentation writer.
/// The activation context supplies both; the micro never assembles its own grants.
pub fn activate(context: &Context) -> Result<(), AccessError> {
    let instance = context.instance();
    let sections = context.collection::<SettingsSection>()?;
    let micro = SettingsMicro {
        sections: sections.reader(&instance),
        instance: instance.clone(),
    };
    let presentation = micro.clone();
    let root = context.publish_component(move || {
        use dioxus::prelude::*;
        rsx! { SettingsView { micro: presentation.clone() } }
    })?;
    context.mount_root("Settings", root)?;
    context.publish(SettingsSections::new(sections));
    context.publish(micro);
    Ok(())
}
