pub const ID: &str = "dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Contribution {
    SettingsSection { title: String },
}

#[cfg(feature = "composition")]
pub fn manifest() -> clay_compose::micros::MicroManifest<Contribution> {
    use clay_compose::micros::{
        ConfigManifest, MicroDescriptor, MicroId, MicroManifest, MicroProvision,
    };
    MicroManifest {
        descriptor: MicroDescriptor {
            id: MicroId::new(ID),
            version: 1,
            requirements: vec![MicroProvision {
                id: "settings.sections".into(),
                version: 1,
            }],
            provisions: vec![MicroProvision {
                id: "wasm.controller".into(),
                version: 1,
            }],
        },
        config: Some(ConfigManifest {
            name: CONFIG_NAME.into(),
            facets: vec![daybook_types::manifest::RoutineFacetAccess {
                tag: CONFIG_TAG.into(),
                key_id: Some(CONFIG_KEY.into()),
                read: true,
                write: true,
                ..Default::default()
            }],
        }),
        contributions: vec![Contribution::SettingsSection {
            title: "Counter (Wasm)".into(),
        }],
    }
}
