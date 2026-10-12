//! Public Settings contribution contract. Consumers do not depend on the Settings controller.
#[cfg(test)]
mod tests;
use crate::host::{CollectionWriter, ComponentRef, Entry, HostedCollection, Instance};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingsSection {
    pub title: String,
    pub detail: ComponentRef,
}

/// Live provider output retained by trusted binding machinery, never handed to a contributor.
/// Binding produces a writer owned by the consumer; the host collection enforces withdrawal when
/// either the consumer or the provider closes.
#[derive(Clone)]
pub struct SettingsSections {
    pub(super) collection: HostedCollection<SettingsSection>,
    pub(super) provider: Instance,
}

impl crate::micros::Provision for SettingsSections {
    const ID: &'static str = "settings.sections";
}

impl SettingsSections {
    pub fn bind(
        &self,
        consumer: &Instance,
    ) -> Result<SettingsContribution, crate::host::AccessError> {
        self.provider.check()?;
        consumer.check()?;
        Ok(SettingsContribution {
            sections: self.collection.writer(consumer),
        })
    }
}

/// Consumer-bound authority to insert and update only its own Settings entries.
#[derive(Clone)]
pub struct SettingsContribution {
    sections: CollectionWriter<SettingsSection>,
}

impl SettingsContribution {
    pub fn insert(
        &self,
        section: SettingsSection,
    ) -> Result<Entry<SettingsSection>, crate::host::AccessError> {
        self.sections.insert(section)
    }
}
