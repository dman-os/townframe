//! Engine-neutral Counter declarations. The manifest is portable MDK vocabulary (ADR 015 §9):
//! identity, contract references, facet ACL, and the contribution the composition backend lands
//! on the counter's behalf. The wasm guest reads only the constants; the composition root reads
//! `manifest()`.
use clay_mdk::ConfigGrant;
use clay_mdk::Contribution;
use clay_mdk::FacetAccess;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::ProvisionRef;

pub const ID: &str = "dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";

pub fn manifest() -> MicroManifest {
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
        contributions: vec![Contribution {
            provision: "settings.sections".into(),
            payload: serde_json::json!({ "title": "Counter (Wasm)" }),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── manifest wire shape: the manifest is wire data (ADR 015 §9), including the declared
    //    contribution payload the composition backend lands on this guest's behalf; the
    //    snapshot follows the current shape so drift is a reviewable diff ──────────────────

    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(manifest());
    }
}
