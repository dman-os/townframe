//! Public Settings contribution contract. Consumers do not depend on the Settings controller.
//!
//! This crate is the Settings micro's manifest surface (ADR 015 §9): the shared contract types,
//! the micro manifest declaration, and provision identity, in the `micro_<name>_manifest`
//! pattern the plug crates established (`plug_<name>_manifest` holds shared types and tests;
//! the micro crate itself compiles to wasm where applicable). Manifest data is written in Rust
//! so compatibility is type-checked; an OCI packaging step remains future work (ADR 015
//! Deferred). Contract types are portable MDK vocabulary: no host or composition crate types
//! cross this boundary (ADR 015 §Boundary correction).
use clay_mdk::native::{AccessError, Collection, CollectionWriter, Entry, Instance};
use clay_mdk::{ComponentRef, MicroId, MicroManifest, Provision, ProvisionRef};

pub const ID: &str = "dayframe.settings";

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
    collection: Collection<SettingsSection>,
}

impl Provision for SettingsSections {
    const ID: &'static str = "settings.sections";
}

impl SettingsSections {
    pub fn new(collection: Collection<SettingsSection>) -> Self {
        Self { collection }
    }

    pub fn bind(&self, consumer: &Instance) -> Result<SettingsContribution, AccessError> {
        Ok(SettingsContribution {
            sections: self.collection.writer(consumer)?,
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

/// The Settings micro's compile-time declaration: it provides the sections collection and the
/// controller provision, and requires nothing.
pub fn manifest() -> MicroManifest {
    MicroManifest {
        id: MicroId::new(ID),
        version: 1,
        requirements: vec![],
        provisions: vec![
            ProvisionRef::of::<SettingsSections>(),
            ProvisionRef {
                id: "settings.controller".into(),
                version: 1,
            },
        ],
        config: None,
        contributions: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_host::{Host, HostedCollection};

    #[test]
    fn output_binds_independent_consumers_and_cannot_outlive_provider() {
        let host = Host::new();
        let provider = host.start("settings");
        let first = host.start("first");
        let second = host.start("second");
        let collection = Collection::from_host(HostedCollection::new(&host, &provider));
        let output = SettingsSections::new(collection.clone());
        let reader = collection.host().reader(&provider);
        let first_grant = output.bind(&first).unwrap();
        let second_grant = output.bind(&second).unwrap();
        let detail = ComponentRef::from_host(host.publish_component(&first).unwrap());
        let first_entry = first_grant
            .insert(SettingsSection {
                title: "First".into(),
                detail,
            })
            .unwrap();
        let second_detail = ComponentRef::from_host(host.publish_component(&second).unwrap());
        let second_entry = second_grant
            .insert(SettingsSection {
                title: "Second".into(),
                detail: second_detail,
            })
            .unwrap();
        host.close_instance(&first).unwrap();
        assert!(matches!(output.bind(&first), Err(AccessError::Revoked)));
        let foreign = Host::new();
        let foreign_consumer = foreign.start("foreign");
        assert!(matches!(
            output.bind(&foreign_consumer),
            Err(AccessError::Denied)
        ));
        assert_eq!(foreign.check(&foreign_consumer), Ok(()));
        foreign.close();
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
        host.close_instance(&provider).unwrap();
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
