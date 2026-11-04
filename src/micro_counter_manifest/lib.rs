//! Engine-neutral Counter declarations. The manifest is portable MDK vocabulary (ADR 024
//! §§11–15): identity, the presentation it publishes, the configuration it requests, and the
//! Settings section row it declares into the `settings.sections` collection, whose attachment
//! is a lazy endpoint to the counter's own presentation. The wasm guest reads only the
//! constants; the composition root reads `manifest()`.
use clay_mdk::AttachmentDeclaration;
use clay_mdk::ConfigurationRequest;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::FacetAccess;
use clay_mdk::MANIFEST_SCHEMA;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;

pub const ID: &str = "dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";
/// The counter's one publication: the presentation the host mounts.
pub const VIEW: &str = "view";

pub fn manifest() -> MicroManifest {
    MicroManifest {
        schema: MANIFEST_SCHEMA,
        id: MicroId::new(ID),
        package_version: 1,
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: VIEW.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
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
            collection: "settings.sections".into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Counter (Wasm)",
                "detail": { "$attachment": 0 },
            }),
            attachments: vec![AttachmentDeclaration {
                index: 0,
                publication: VIEW.into(),
            }],
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
