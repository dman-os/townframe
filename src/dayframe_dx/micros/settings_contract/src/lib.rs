//! Public Settings contribution contract. Consumers do not depend on the Settings controller.
use dayframe_app::{
    host::{AccessError, CollectionWriter, ComponentRef, Entry, HostedCollection, Instance},
    micros::Provision,
};

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
    collection: HostedCollection<SettingsSection>,
    provider: Instance,
}

impl Provision for SettingsSections {
    const ID: &'static str = "settings.sections";
}

impl SettingsSections {
    pub fn new(collection: HostedCollection<SettingsSection>, provider: Instance) -> Self {
        Self {
            collection,
            provider,
        }
    }

    pub fn bind(&self, consumer: &Instance) -> Result<SettingsContribution, AccessError> {
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
    pub fn insert(&self, section: SettingsSection) -> Result<Entry<SettingsSection>, AccessError> {
        self.sections.insert(section)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dayframe_app::host::Host;

    #[test]
    fn output_binds_independent_consumers_and_cannot_outlive_provider() {
        let host = Host::new();
        let provider = host.start("settings");
        let first = host.start("first");
        let second = host.start("second");
        let collection = HostedCollection::new(&host, &provider);
        let output = SettingsSections::new(collection.clone(), provider.clone());
        let reader = collection.reader(&provider);
        let first_grant = output.bind(&first).unwrap();
        let second_grant = output.bind(&second).unwrap();
        let detail = host.publish_component(&first).unwrap();
        let first_entry = first_grant
            .insert(SettingsSection {
                title: "First".into(),
                detail,
            })
            .unwrap();
        let second_detail = host.publish_component(&second).unwrap();
        let second_entry = second_grant
            .insert(SettingsSection {
                title: "Second".into(),
                detail: second_detail,
            })
            .unwrap();
        first.close();
        assert_eq!(
            reader.snapshot().unwrap().into_values().collect::<Vec<_>>(),
            vec![SettingsSection {
                title: "Second".into(),
                detail: second_detail
            }]
        );
        second_entry
            .update(SettingsSection {
                title: "Updated".into(),
                detail: second_detail,
            })
            .unwrap();
        assert_eq!(
            first_entry.update(SettingsSection {
                title: "Stale".into(),
                detail
            }),
            Err(AccessError::Revoked)
        );
        provider.close();
        assert!(matches!(
            second_grant.insert(SettingsSection {
                title: "Stale".into(),
                detail
            }),
            Err(AccessError::Revoked)
        ));
        let late = host.start("late");
        assert!(matches!(output.bind(&late), Err(AccessError::Revoked)));
        host.close();
    }
}
