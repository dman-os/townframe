//! Engine-neutral Counter declarations. The manifest is portable MDK vocabulary (ADR 030
//! §§11–15): identity, the presentations and call it publishes, the configuration it requests, and the
//! Settings section row it declares into the `settings.sections` collection, whose attachment
//! is a lazy endpoint to the counter's own presentation, and the Increment button row it
//! declares into the `toolbar.buttons` collection, whose attachment is a lazy endpoint to the
//! counter's `increment` call. The wasm guest reads only the constants; the composition root
//! reads `manifest()`.
use clay_mdk::Access;
use clay_mdk::AttachmentDeclaration;
use clay_mdk::BindingResolution;
use clay_mdk::ConfigurationRequest;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::FacetAccess;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Request;
use clay_mdk::Sidecar;
use micro_settings_manifest::SECTIONS;
use micro_toolbar_manifest::ACTION_CONTRACT;
use micro_toolbar_manifest::BUTTONS;

pub const ID: &str = "dayframe.counter";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the counter holds: the contract of its [`COUNT`] call lies under it.
pub const AUTHORITY: &str = "org.example.dayframe.counter";
pub const CONFIG_NAME: &str = "counter";
pub const CONFIG_TAG: &str = "org.example.dayframe.counter.config";
pub const CONFIG_KEY: &str = "count";
/// The counter's full presentation, the one its Settings section mounts.
pub const VIEW: &str = "view";
/// A compact presentation of the same count, for any micro that wants to embed it.
pub const BADGE: &str = "badge";
/// The call that answers (CBOR `()` in, CBOR `u64` out) the counter's current value.
pub const COUNT: &str = "count";
/// The contract NSID of the [`COUNT`] call.
pub const COUNT_CONTRACT: &str = "org.example.dayframe.counter.count";
/// The local binding name under which the counter registers its Settings section.
pub const SECTIONS_BINDING: &str = "sections";
/// The call the toolbar's Increment button performs (CBOR `()` in, CBOR `()` out): count one more.
/// It implements the toolbar's action contract, which the toolbar owns.
pub const INCREMENT: &str = "increment";
/// The local binding name under which the counter contributes its toolbar button.
pub const TOOLBAR_BINDING: &str = "toolbar";

pub fn manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![
            PublicationDeclaration {
                name: VIEW.into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
            PublicationDeclaration {
                name: BADGE.into(),
                kind: PublicationKind::Presentation,
                contract: None,
            },
            PublicationDeclaration {
                name: COUNT.into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal(COUNT_CONTRACT)),
            },
            PublicationDeclaration {
                name: INCREMENT.into(),
                kind: PublicationKind::Call,
                contract: Some(Nsid::literal(ACTION_CONTRACT)),
            },
        ],
        requires: vec![],
        configuration_requests: vec![ConfigurationRequest {
            binding: CONFIG_NAME.into(),
            facets: vec![FacetAccess {
                tag: Nsid::literal(CONFIG_TAG),
                key_id: Some(CONFIG_KEY.into()),
                read: true,
                write: true,
                create: false,
                delete: false,
            }],
        }],
        collection_contracts: vec![],
        collection_requests: vec![
            Request {
                binding: SECTIONS_BINDING.into(),
                contract: Nsid::literal(SECTIONS),
                access: Some(Access::Write),
                necessity: Necessity::Optional,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
            Request {
                binding: TOOLBAR_BINDING.into(),
                contract: Nsid::literal(BUTTONS),
                access: Some(Access::Write),
                necessity: Necessity::Optional,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
        ],
        contributions: vec![
            ContributionDeclaration {
                id: "settings-section".into(),
                binding: SECTIONS_BINDING.into(),
                key: ID.into(),
                payload: serde_json::json!({
                    "title": "Counter (Wasm)",
                    "detail": { "$attachment": 0 },
                }),
                attachments: vec![AttachmentDeclaration {
                    index: 0,
                    publication: VIEW.into(),
                }],
            },
            ContributionDeclaration {
                id: "toolbar-button".into(),
                binding: TOOLBAR_BINDING.into(),
                key: INCREMENT.into(),
                payload: serde_json::json!({
                    "label": "Increment",
                    "icon": "plus",
                    "action": { "$attachment": 0 },
                }),
                attachments: vec![AttachmentDeclaration {
                    index: 0,
                    publication: INCREMENT.into(),
                }],
            },
        ],
        extra: Default::default(),
    }
}

/// The counter package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::new(ID, PACKAGE_VERSION, manifest()).with_authority(AUTHORITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── manifest wire shape: the manifest is wire data (ADR 030 §11), including the declared
    //    contribution payload and its activation default; the snapshot follows the current
    //    shape so drift is a reviewable diff ─────────────────────────────────────────────────

    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }
}
