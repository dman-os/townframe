//! Settings owns its section vocabulary and navigation; the host owns storage and authority.
//! Authored against the MDK only (ADR 015 §5): activation receives a [`Context`], never the
//! registry, renderer, or host kernel.
mod view;
pub use view::SettingsView;

use clay_mdk::native::AccessError;
use clay_mdk::native::CollectionWatch;
use clay_mdk::native::Context;
use clay_mdk::native::DocumentSnapshot;
use clay_mdk::native::Facet;
use clay_mdk::native::FacetValue;
use clay_mdk::native::Instance;
use clay_mdk::native::Operation;
use clay_mdk::native::RendererRegistry;
use contract::Section;
use dioxus::prelude::Element;
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
    pub sections: CollectionWatch<String, Section>,
    pub config: Facet<SettingsConfig>,
}

impl PartialEq for SettingsMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl SettingsMicro {
    pub fn preferences(&self, snapshot: &DocumentSnapshot) -> Option<SettingsConfig> {
        match self.config.state(snapshot) {
            FacetValue::Loading => None,
            FacetValue::Absent => Some(SettingsConfig::default()),
            FacetValue::Present(config) => Some(config),
        }
    }

    pub fn edit(&self, update: impl FnOnce(&mut SettingsConfig)) -> Result<Operation, AccessError> {
        let mut config = self
            .preferences(&self.config.watch.current()?)
            .ok_or(AccessError::Denied)?;
        update(&mut config);
        self.config.write(config)
    }
}

/// Settings reads the declared sections collection (its configuration binds it) and publishes
/// its view, which the shell mounts as a root.
pub fn activate<R: RendererRegistry<Render = Element>>(
    context: &Context<R>,
) -> Result<(), AccessError> {
    let micro = SettingsMicro {
        instance: context.instance(),
        sections: context.collection(contract::SECTIONS)?,
        config: context
            .config(contract::CONFIG_NAME)
            .expect("manifest declares Settings configuration")
            .facet::<SettingsConfig>(contract::CONFIG_TAG.into(), contract::CONFIG_KEY.into())?,
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
    use clay_mdk::native::CollectionBindings;
    use clay_mdk::native::CollectionRegistry;
    use clay_mdk::native::DocumentService;
    use clay_mdk::native::Resources;
    use clay_mdk::native::factory;
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
                let resources = Resources {
                    registry: NativeComponents::new(&host),
                    documents: DocumentService::new(16),
                    collections: CollectionRegistry::default(),
                };
                resources
                    .collections
                    .declare(
                        CollectionContract {
                            id: contract::SECTIONS.into(),
                            key_schema: contract::SECTION_KEY_SCHEMA.into(),
                            value_schema: contract::SECTION_VALUE_SCHEMA.into(),
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
                    factory: factory(
                        resources,
                        contract::manifest(),
                        CollectionBindings {
                            read: vec![contract::SECTIONS.into()],
                            write: vec![],
                        },
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
