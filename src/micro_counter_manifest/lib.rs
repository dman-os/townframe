//! Engine-neutral Counter declarations. The manifest is portable MDK vocabulary (ADR 024
//! §§11–15): identity, contract references, the configuration it requests, and the Settings
//! section it declares plus the default registration that activates it while Settings is live.
//! The wasm guest reads only the constants; the composition root reads `manifest()`.
use clay_mdk::ActivationRegistrationTemplate;
use clay_mdk::ConfigurationRequest;
use clay_mdk::ContractRef;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::FacetAccess;
use clay_mdk::MANIFEST_SCHEMA;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::ProvidedContract;
use clay_mdk::RequiredContract;
use clay_mdk::Retention;
use clay_mdk::Trigger;

pub const ID: &str = "dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";

pub fn manifest() -> MicroManifest {
    let settings_sections = ContractRef {
        id: "settings.sections".into(),
        version: 1,
    };
    MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: MicroId::new(ID),
        package_version: 1,
        entrypoints: Entrypoints::micro_client(),
        provides: vec![ProvidedContract {
            id: "controller".into(),
            contract: ContractRef {
                id: "wasm.controller".into(),
                version: 1,
            },
            retention: Retention::Strong,
        }],
        requires: vec![RequiredContract {
            binding: "settings".into(),
            contract: settings_sections.clone(),
            necessity: Necessity::Optional,
        }],
        configuration_requests: vec![ConfigurationRequest {
            binding: CONFIG_NAME.into(),
            facets: vec![FacetAccess {
                tag: CONFIG_TAG.into(),
                key_id: Some(CONFIG_KEY.into()),
                read: true,
                write: true,
                ..Default::default()
            }],
        }],
        collection_contracts: vec![],
        contributions: vec![ContributionDeclaration {
            id: "settings-section".into(),
            target_binding: "settings".into(),
            extension_contract: settings_sections.clone(),
            payload: serde_json::json!({ "title": "Counter (Wasm)" }),
            capability_slots: vec![],
        }],
        activation_defaults: vec![ActivationRegistrationTemplate {
            id: "settings-section".into(),
            trigger: Trigger::TargetLive {
                target_binding: "settings".into(),
                extension_contract: settings_sections,
            },
            declaration_id: "settings-section".into(),
        }],
        extra: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── manifest wire shape: the manifest is wire data (ADR 024 §11), including the declared
    //    contribution payload and its activation default; the snapshot follows the current
    //    shape so drift is a reviewable diff ─────────────────────────────────────────────────

    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(manifest());
    }
}
