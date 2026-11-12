//! Settings owns its section vocabulary; the shell owns navigation, layout and the back button
//! (ADR 032 §8), and the host owns storage and authority. Settings contributes a list panel
//! whose rows are its sections: opening a row asks the shell to show that section's attachment
//! as the panel's details. Authored against the MDK only (ADR 015 §5): activation receives a
//! [`Context`], never the registry, renderer, or host kernel.
mod view;
pub use view::SettingsList;
pub use view::SettingsSection;

use clay_mdk::Capability;
use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Datum;
use clay_mdk::DatumSnapshot;
use clay_mdk::DatumState;
use clay_mdk::Instance;
use clay_mdk::Operation;
use contract::Section;
pub use micro_settings_manifest as contract;

pub use contract::SettingsConfig;

/// What the shell lets Settings use, each required under the name of the shell publication it
/// binds. Opening a section is not among them: a row opens itself through the list scaffold
/// (`clay_dioxus_kit::ListRow`).
#[derive(Clone)]
pub struct ShellHandles {
    /// Which row of the Settings list the shell has open (`String` panel key in, `Option<String>`
    /// out). The section presentation takes no parameters, so it asks.
    pub opened: Capability,
    /// The sidebar/back button presentation, mounted in the list's app bar.
    pub nav_button: Capability,
}

/// What the Settings views read: the sections collection, its preferences, the shell, and the
/// instance that identifies this activation (two views of one activation are equal).
#[derive(Clone)]
pub struct SettingsMicro {
    pub instance: Instance,
    pub sections: Collection<String, Section>,
    pub config: Datum<SettingsConfig>,
    pub shell: ShellHandles,
}

impl PartialEq for SettingsMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl SettingsMicro {
    /// The viewer's preferences in `snapshot`: the defaults while the document has none, and
    /// `None` while it is still loading.
    pub fn preferences(&self, snapshot: &DatumSnapshot<SettingsConfig>) -> Option<SettingsConfig> {
        match &snapshot.state {
            DatumState::Loading => None,
            DatumState::Absent => Some(SettingsConfig::default()),
            DatumState::Present(config) => Some(config.clone()),
        }
    }

    pub fn edit(&self, update: impl FnOnce(&mut SettingsConfig)) -> Result<Operation, Error> {
        let mut config = self
            .preferences(&self.config.current()?)
            .ok_or(Error::Denied)?;
        update(&mut config);
        self.config.write(config)
    }
}

/// Settings reads the declared sections collection (its configuration binds it) and publishes
/// its list and section presentations, which the shell mounts as its panel.
pub async fn activate(context: Context) -> Result<(), Error> {
    let required = |binding: &str| {
        context
            .require(binding)
            .unwrap_or_else(|| panic!("a required binding {binding:?} is always resolved"))
            .clone()
    };
    let micro = SettingsMicro {
        instance: context.instance(),
        sections: context
            .collection::<contract::SettingsSections>()
            .expect("configuration binds the sections collection"),
        config: context
            .config::<contract::SettingsPreferences>()?
            .expect("manifest declares Settings configuration"),
        shell: ShellHandles {
            opened: required(contract::SHELL_OPENED),
            nav_button: required(contract::SHELL_NAV_BUTTON),
        },
    };
    context.publish::<contract::SettingsListView>({
        let micro = micro.clone();
        move || {
            use dioxus::prelude::*;
            rsx! { SettingsList { micro: micro.clone() } }
        }
    })?;
    context.publish::<contract::SettingsSectionView>(move || {
        use dioxus::prelude::*;
        rsx! { SettingsSection { micro: micro.clone() } }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_dioxus::NativeComponents;
    use clay_guest_session::collections::bytes_registrations;
    use clay_host::CollectionContract;
    use clay_host::ConfiguredMicro;
    use clay_host::Host;
    use clay_host::MicroId;
    use clay_host::Requirement;
    use clay_host::Supervisor;
    use clay_mdk::Nsid;
    use clay_mdk::host::BoundCollection;
    use clay_mdk::host::CapabilityPublication;
    use clay_mdk::host::CollectionBindings;
    use clay_mdk::host::CollectionRegistry;
    use clay_mdk::host::DocumentService;
    use clay_mdk::host::ReplicaId;
    use clay_mdk::host::Resources;
    use clay_mdk::host::factory;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    #[test]
    fn preferences_rank_ordered_micros_before_the_rest() {
        let config = SettingsConfig {
            hidden: BTreeSet::new(),
            order: vec!["b".into(), "a".into()],
        };
        assert!(config.rank("b") < config.rank("a"));
        assert!(config.rank("a") < config.rank("unlisted"));
    }

    /// Stands in for the shell: the publications Settings requires, with the shell's real
    /// contracts, answering "nothing is open".
    fn shell_stub(resources: Resources) -> ConfiguredMicro<CapabilityPublication> {
        use clay_mdk::PublicationDeclaration;
        use clay_mdk::PublicationKind;
        let call = |name: &str, contract: &str| PublicationDeclaration {
            name: name.into(),
            kind: PublicationKind::Call,
            contract: Some(Nsid::literal(contract)),
        };
        let manifest = clay_mdk::MicroManifest {
            entrypoints: clay_mdk::Entrypoints::micro_client(),
            publications: vec![
                PublicationDeclaration {
                    name: micro_shell_manifest::NAV_BUTTON.into(),
                    kind: PublicationKind::Presentation,
                    contract: None,
                },
                call(
                    micro_shell_manifest::OPENED,
                    micro_shell_manifest::OPENED_CONTRACT,
                ),
            ],
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: vec![],
            collection_requests: vec![],
            contributions: vec![],
            extra: Default::default(),
        };
        let id = clay_mdk::MicroId::local(micro_shell_manifest::ID);
        ConfiguredMicro {
            identity: MicroId::from(&id),
            eager: false,
            enabled: true,
            requirements: BTreeMap::new(),
            unresolved_bindings: Vec::new(),
            factory: factory(
                resources,
                id,
                manifest,
                CollectionBindings::new([]),
                |context| async move {
                    use dioxus::prelude::*;
                    context.publish::<micro_shell_manifest::ShellNavButton>(|| {
                        rsx! { button { "back" } }
                    })?;
                    context.publish_call(micro_shell_manifest::OPENED, |_: String| async {
                        Ok(None::<String>)
                    })?;
                    Ok(())
                },
            ),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn activation_publishes_the_declared_list_and_section() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let host = Host::new();
                let resources = Resources::new(
                    NativeComponents::new(&host),
                    DocumentService::new(16),
                    CollectionRegistry::new(ReplicaId::from_bytes([1; ReplicaId::LEN])),
                );
                resources
                    .collections
                    .declare(
                        CollectionContract {
                            id: Nsid::literal(contract::SECTIONS),
                            key_schema: Nsid::literal(contract::SECTION_KEY_SCHEMA),
                            value_schema: Nsid::literal(contract::SECTION_VALUE_SCHEMA),
                        },
                        bytes_registrations(NonZeroUsize::new(8).unwrap(), [0, 0]),
                    )
                    .unwrap();
                resources.documents.mock_documents().bind_config(
                    contract::CONFIG_NAME.into(),
                    "settings-config".into(),
                    "main".into(),
                );
                let shell = shell_stub(resources.clone());
                let shell_id = shell.identity.clone();
                let settings = ConfiguredMicro {
                    identity: MicroId::from(&clay_mdk::MicroId::local(contract::ID)),
                    eager: false,
                    enabled: true,
                    requirements: [contract::SHELL_OPENED, contract::SHELL_NAV_BUTTON]
                        .map(|publication| {
                            (
                                publication.to_owned(),
                                Requirement {
                                    provider: shell_id.clone(),
                                    publication: publication.to_owned(),
                                },
                            )
                        })
                        .into(),
                    unresolved_bindings: Vec::new(),
                    factory: factory(
                        resources,
                        clay_mdk::MicroId::local(contract::ID),
                        contract::manifest(),
                        CollectionBindings::new([(
                            contract::SECTIONS_BINDING.to_owned(),
                            BoundCollection::read(Nsid::literal(contract::SECTIONS)),
                        )]),
                        activate,
                    ),
                };
                let supervisor = Supervisor::new(host.clone(), [shell, settings]);
                let settings = MicroId::from(&clay_mdk::MicroId::local(contract::ID));
                for publication in [contract::LIST, contract::SECTION] {
                    let view = supervisor.acquire(&settings, publication).await.unwrap();
                    assert!(view.component().is_some(), "{publication}");
                }
                supervisor.shutdown().await.unwrap();
                host.close();
            })
            .await;
    }
}
