//! Settings owns its section vocabulary and navigation; the host owns storage and authority.
//! Authored against the MDK only (ADR 015 §5): activation receives a [`Context`], never the
//! registry, renderer, or host kernel.
mod view;
pub use view::SettingsView;

use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use clay_mdk::FacetSnapshot;
use clay_mdk::FacetState;
use clay_mdk::Instance;
use clay_mdk::Operation;
use contract::Section;
pub use micro_settings_manifest as contract;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;

/// The viewer's own preferences, kept in its configuration document: which sections are hidden
/// and which come first, by contributing micro id.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SettingsConfig {
    #[serde(default)]
    pub hidden: BTreeSet<String>,
    #[serde(default)]
    pub order: Vec<String>,
}

impl SettingsConfig {
    pub fn rank(&self, micro: &str) -> usize {
        self.order
            .iter()
            .position(|name| name == micro)
            .unwrap_or(usize::MAX)
    }
}

/// What the Settings view reads: the sections collection, its preferences, and the instance
/// that mounts section presentations.
#[derive(Clone)]
pub struct SettingsMicro {
    pub instance: Instance,
    pub sections: Collection<String, Section>,
    pub config: Facet<SettingsConfig>,
}

impl PartialEq for SettingsMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl SettingsMicro {
    /// The viewer's preferences in `snapshot`: the defaults while the document has none, and
    /// `None` while it is still loading.
    pub fn preferences(&self, snapshot: &FacetSnapshot<SettingsConfig>) -> Option<SettingsConfig> {
        match &snapshot.state {
            FacetState::Loading => None,
            FacetState::Absent => Some(SettingsConfig::default()),
            FacetState::Present(config) => Some(config.clone()),
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
/// its view, which the shell mounts as a root.
pub async fn activate(context: Context) -> Result<(), Error> {
    let micro = SettingsMicro {
        instance: context.instance(),
        sections: context
            .collection(contract::SECTIONS_BINDING)
            .expect("configuration binds the sections collection"),
        config: context
            .config(contract::CONFIG_NAME)
            .expect("manifest declares Settings configuration")
            .facet::<SettingsConfig>(contract::CONFIG_TAG, contract::CONFIG_KEY)?,
    };
    context.publish_component(contract::VIEW, move || {
        use dioxus::prelude::*;
        rsx! { SettingsView { micro: micro.clone() } }
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
    use clay_host::Supervisor;
    use clay_mdk::Nsid;
    use clay_mdk::host::BoundCollection;
    use clay_mdk::host::CollectionBindings;
    use clay_mdk::host::CollectionRegistry;
    use clay_mdk::host::DocumentService;
    use clay_mdk::host::Resources;
    use clay_mdk::host::factory;
    use std::collections::BTreeMap;
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

    #[tokio::test(flavor = "current_thread")]
    async fn activation_publishes_the_declared_view() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let host = Host::new();
                let resources = Resources::new(
                    NativeComponents::new(&host),
                    DocumentService::new(16),
                    CollectionRegistry::default(),
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
                let settings = ConfiguredMicro {
                    identity: MicroId::new(contract::ID),
                    eager: false,
                    enabled: true,
                    requirements: BTreeMap::new(),
                    unresolved_bindings: Vec::new(),
                    factory: factory(
                        resources,
                        clay_mdk::MicroId::new(contract::ID),
                        contract::manifest(),
                        CollectionBindings::new([(
                            contract::SECTIONS_BINDING.to_owned(),
                            BoundCollection::read(Nsid::literal(contract::SECTIONS)),
                        )]),
                        activate,
                    ),
                };
                let supervisor = Supervisor::new(host.clone(), [settings]);
                let view = supervisor
                    .acquire(&MicroId::new(contract::ID), contract::VIEW)
                    .await
                    .unwrap();
                assert!(view.component().is_some());
                drop(view);
                supervisor.shutdown().await.unwrap();
                host.close();
            })
            .await;
    }
}
