//! Public Settings contribution contract. Consumers do not depend on the Settings controller.
//!
//! This crate is the Settings micro's manifest surface (ADR 015 §9): the shared contract types,
//! the micro manifest declaration, and provision identity, in the `micro_<name>_manifest`
//! pattern the plug crates established (`plug_<name>_manifest` holds shared types and tests;
//! the micro crate itself compiles to wasm where applicable). Manifest data is written in Rust
//! so compatibility is type-checked; an OCI packaging step remains future work (ADR 015
//! Deferred). Contract types are portable MDK vocabulary: no host or composition crate types
//! cross this boundary (ADR 015 §Boundary correction).
use clay_mdk::ComponentRef;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::Provision;
use clay_mdk::ProvisionRef;
use clay_mdk::native::AccessError;
use clay_mdk::native::Collection;
use clay_mdk::native::CollectionWriter;
use clay_mdk::native::Contributable;
use clay_mdk::native::Entry;
use clay_mdk::native::Instance;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.settings";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingsSection {
    pub title: String,
    pub detail: ComponentRef,
}

/// The payload a declared contribution to [`SettingsSections`] carries (see
/// [`clay_mdk::Contribution`]): everything of a section except the detail component, which is
/// the contributor's published surface.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredSection {
    pub title: String,
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

impl Contributable for SettingsSections {
    fn contribute(
        &self,
        contributor: &Instance,
        payload: &serde_json::Value,
        surface: ComponentRef,
    ) -> Result<(), AccessError> {
        // The payload arrives as manifest-declared data, possibly delivered over the wire:
        // reject a shape violation as a denied contribution instead of trusting the caller.
        let Ok(DeclaredSection { title }) = serde_json::from_value(payload.clone()) else {
            return Err(AccessError::Denied);
        };
        self.bind(contributor)?.insert(SettingsSection {
            title,
            detail: surface,
        })?;
        Ok(())
    }
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
    use clay_host::Host;
    use clay_host::HostedCollection;

    // ── manifest and contract wire shapes: the manifest is wire data (ADR 015 §9) and
    //    `DeclaredSection` is the contribution payload acceptor; both snapshots follow the
    //    current shape so drift is a reviewable diff ────────────────────────────────────

    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(manifest());
    }

    #[test]
    fn declared_section_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(
            serde_json::to_value(DeclaredSection {
                title: "Counter (Wasm)".into(),
            })
            .unwrap()
        );
    }

    // ── payload acceptance boundary: `contribute` receives manifest-declared data that may
    //    have crossed the wire, so any shape violation is a denied contribution ────────────

    #[test]
    fn contribution_payload_must_match_declared_section_wire_shape() {
        let host = Host::new();
        let provider = host.start("settings");
        let consumer = host.start("consumer");
        let collection = Collection::from_host(HostedCollection::new(&host, &provider));
        let reader = collection.host().reader(&provider);
        let output = SettingsSections::new(collection);
        let detail = ComponentRef::from_host(host.publish_component(&consumer).unwrap());

        let malformed = output
            .contribute(&consumer, &serde_json::json!({"wrong": "shape"}), detail)
            .unwrap_err();
        assert_eq!(malformed, AccessError::Denied);
        let extra_field = output
            .contribute(
                &consumer,
                &serde_json::json!({"title": "Tolerant", "unexpected": 1}),
                detail,
            )
            .unwrap_err();
        assert_eq!(extra_field, AccessError::Denied);
        for payload in [
            serde_json::json!({}),
            serde_json::json!({"title": 1}),
            serde_json::json!({"title": null}),
            serde_json::json!({"title": ["section"]}),
            serde_json::json!({"title": {"nested": true}}),
            serde_json::json!(null),
            serde_json::json!("title"),
        ] {
            assert_eq!(
                output.contribute(&consumer, &payload, detail),
                Err(AccessError::Denied),
                "payload {payload} must be denied"
            );
        }
        assert!(
            reader.snapshot().unwrap().is_empty(),
            "denied payloads must not land in the collection"
        );

        output
            .contribute(&consumer, &serde_json::json!({"title": "Accepted"}), detail)
            .unwrap();
        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .into_values()
                .map(|section| section.title)
                .collect::<Vec<_>>(),
            vec!["Accepted"]
        );
        host.close();
    }

    // ── consumer binding: each consumer gets an independent writer scoped to its own
    //    instance; closing the consumer or the provider revokes exactly that authority ──────

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
        assert_eq!(reader.snapshot().unwrap().len(), 1);
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

    // ── host scoping: a consumer token minted by another host carries no authority here ──────

    #[test]
    fn bind_denies_consumers_from_a_foreign_host() {
        let host = Host::new();
        let provider = host.start("settings");
        let collection = Collection::from_host(HostedCollection::new(&host, &provider));
        let output = SettingsSections::new(collection);
        let foreign = Host::new();
        let foreign_consumer = foreign.start("foreign");
        assert!(matches!(
            output.bind(&foreign_consumer),
            Err(AccessError::Denied)
        ));
        assert_eq!(foreign.check(&foreign_consumer), Ok(()));
        foreign.close();
        host.close();
    }
}
