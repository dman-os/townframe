//! The `trapper` fixture's declarations, shared by the guest (`trapper_fixture.rs`) and the native
//! emitter of its sidecar (`emit_trapper_manifest.rs`), which both `#[path]`-include this file.
//! The Settings section row mirrors the counter's, so the shell lists it and mounts its `view`.
use clay_mdk::Access;
use clay_mdk::AttachmentDeclaration;
use clay_mdk::BindingResolution;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Request;
use clay_mdk::Sidecar;

pub const ID: &str = "dayframe.trapper";
pub const VIEW: &str = "view";
/// `micro_settings_manifest::SECTIONS`, which this crate cannot name (it sits below the settings
/// micro). The shell's Settings reads rows of this contract.
const SECTIONS: &str = "org.example.dayframe.settings.sections";

pub fn manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: VIEW.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![Request {
            binding: "sections".into(),
            contract: Nsid::literal(SECTIONS),
            access: Some(Access::Write),
            necessity: Necessity::Optional,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: vec![ContributionDeclaration {
            id: "settings-section".into(),
            binding: "sections".into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Trapper (fixture)",
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

/// Only the sidecar emitter reads it; the guest links `manifest` alone.
#[allow(dead_code)]
pub fn sidecar() -> Sidecar {
    Sidecar::new(ID, 1, manifest())
}
