//! Engine-neutral Counter declarations. The manifest is portable MDK vocabulary (ADR 015 §9):
//! identity, contract references, facet ACL, and the host-side contribution the composition root
//! binds. The wasm guest reads only the constants; the composition root reads `manifest()`.
use clay_mdk::{ConfigGrant, FacetAccess, MicroId, MicroManifest, ProvisionRef};

pub const ID: &str = "dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Contribution {
    SettingsSection { title: String },
}

pub fn manifest() -> MicroManifest<Contribution> {
    MicroManifest {
        id: MicroId::new(ID),
        version: 1,
        requirements: vec![ProvisionRef {
            id: "settings.sections".into(),
            version: 1,
        }],
        provisions: vec![ProvisionRef {
            id: "wasm.controller".into(),
            version: 1,
        }],
        config: Some(ConfigGrant {
            name: CONFIG_NAME.into(),
            facets: vec![FacetAccess {
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
