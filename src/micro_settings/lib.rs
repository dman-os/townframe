//! Settings owns its section vocabulary and navigation; the host owns storage and authority.
//! Authored against the MDK only (ADR 015 §5): activation receives a [`Context`], never the
//! registry, renderer, or host kernel.
mod view;
pub use view::SettingsView;

use clay_mdk::Provision;
use clay_mdk::native::AccessError;
use clay_mdk::native::CollectionReader;
use clay_mdk::native::Context;
use clay_mdk::native::Instance;
use clay_mdk::native::RendererRegistry;
use contract::SettingsSection;
use contract::SettingsSections;
use dioxus::prelude::Element;
pub use micro_settings_manifest as contract;

use clay_mdk::native::Declaration;
use clay_mdk::native::DocumentSnapshot;
use clay_mdk::native::Facet;
use clay_mdk::native::FacetValue;
use clay_mdk::native::Operation;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use tokio::sync::watch;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SettingsConfig {
    #[serde(default)]
    pub hidden: BTreeSet<String>,
    #[serde(default)]
    pub order: Vec<String>,
}

impl SettingsConfig {
    pub fn rank(&self, instance: &str) -> usize {
        self.order
            .iter()
            .position(|name| name == instance)
            .unwrap_or(usize::MAX)
    }
}

#[derive(Clone)]
pub struct SettingsMicro {
    pub instance: Instance,
    pub sections: CollectionReader<SettingsSection>,
    pub config: Facet<SettingsConfig>,
    pub declarations: watch::Receiver<Vec<Declaration>>,
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

impl Provision for SettingsMicro {
    const ID: &'static str = "settings.controller";
}

/// Settings requires storage for its own vocabulary and a granted root-presentation writer.
/// The activation context supplies both; the micro never assembles its own grants.
pub fn activate<R: RendererRegistry<Render = Element>>(
    context: &Context<R>,
) -> Result<(), AccessError> {
    let instance = context.instance();
    let sections = context.collection::<SettingsSection>()?;
    let config = context
        .config(contract::CONFIG_NAME)
        .expect("manifest declares Settings configuration")
        .facet::<SettingsConfig>(contract::CONFIG_TAG.into(), contract::CONFIG_KEY.into())?;
    let broker = context.broker();
    let mut declarations = broker.watch(SettingsSections::ID)?;
    let (catalog, catalog_watch) = watch::channel(declarations.current()?);
    let micro = SettingsMicro {
        sections: sections.reader(&instance),
        instance: instance.clone(),
        config,
        declarations: catalog_watch,
    };
    let mut config_changes = micro.config.watch.subscribe()?;
    let selection = micro.clone();
    let apply = move |snapshot: &DocumentSnapshot, rows: &[Declaration]| {
        let accepted = selection
            .preferences(snapshot)
            .map(|config| {
                rows.iter()
                    .filter(|row| !config.hidden.contains(&row.instance))
                    .map(|row| (row.instance.clone(), row.contract.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        broker.accept(accepted)
    };
    apply(&config_changes.borrow_and_update(), &catalog.borrow())?;
    context.own_future(async move {
        loop {
            tokio::select! {
                result = config_changes.changed() => result.unwrap(),
                result = declarations.next() => { result.unwrap(); }
            }
            let rows = declarations.current().unwrap();
            apply(&config_changes.borrow_and_update(), &rows).unwrap();
            catalog.send_if_modified(|current| {
                if *current == rows {
                    return false;
                }
                *current = rows;
                true
            });
        }
    })?;
    let presentation = micro.clone();
    let root = context.publish_component(move || {
        use dioxus::prelude::*;
        rsx! { SettingsView { micro: presentation.clone() } }
    })?;
    context.mount_root("Settings", root)?;
    context.publish_contributable(SettingsSections::new(sections));
    context.publish(micro);
    Ok(())
}
