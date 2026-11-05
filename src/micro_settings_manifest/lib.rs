//! The Settings contract (ADR 015 §9): the identity and manifest of the Settings micro, and the
//! `settings.sections` collection other micros register their sections into. Consumers depend on
//! this crate and never on the Settings micro itself.
//!
//! A section row is keyed by the contributing micro's id (CBOR text) and holds a [`Section`]: a
//! title, and an attachment for the section's detail presentation. A micro whose section should
//! exist while it is stopped declares the row in its manifest, which stores an endpoint to its
//! own presentation; a micro that only contributes while running writes the row at runtime with
//! a bound reference. Settings treats both alike.
//!
//! Settings is a **list panel** of the shell (ADR 032 §8): it contributes one `shell.panels` row
//! whose content is the list of its sections and whose details presentation shows the section
//! the shell has open. It arranges nothing itself; the shell owns the stack, the layout and the
//! back button, which Settings requires from it ([`SHELL_OPENED`], [`SHELL_NAV_BUTTON`]). Opening
//! a row is the list scaffold's `ListRow`, which reaches the shell's navigation through the
//! scaffold, so Settings never calls `navigate` itself.
use clay_mdk::Access;
use clay_mdk::Attachment;
use clay_mdk::BindingResolution;
use clay_mdk::CollectionCapability;
use clay_mdk::CollectionContract;
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
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.settings";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix Settings holds: every NSID it declares (its collection contract and
/// schemas) lies under it, everything else it names is a request.
pub const AUTHORITY: &str = "org.example.dayframe.settings";
pub const CONFIG_NAME: &str = "settings";
pub const CONFIG_TAG: &str = "org.example.dayframe.settings.config";
pub const CONFIG_KEY: &str = "main";
/// The Settings list: the content of its panel, the sections and the viewer's preferences.
pub const LIST: &str = "list";
/// The presentation of the section the shell has open, mounted in the details position.
pub const SECTION: &str = "section";
/// The contract NSID, and so the declared collection's id, of the sections collection.
pub const SECTIONS: &str = "org.example.dayframe.settings.sections";
/// The local binding name under which Settings reads its sections collection.
pub const SECTIONS_BINDING: &str = "sections";
/// The local binding name under which Settings contributes its panel to `shell.panels`.
pub const PANELS_BINDING: &str = "panels";
/// The local binding names under which Settings requires what it uses of the shell. Each is
/// the name of the shell publication it binds, so a catalog satisfies all of them from one
/// provider (`InstalledMicro::satisfied_by`).
pub const SHELL_OPENED: &str = micro_shell_manifest::OPENED;
pub const SHELL_NAV_BUTTON: &str = micro_shell_manifest::NAV_BUTTON;
/// Schema NSIDs of [`SECTIONS`] keys (the contributing micro's id, text) and values
/// ([`Section`]), and of its selector (the whole collection).
pub const SECTION_KEY_SCHEMA: &str = "org.example.dayframe.settings.sectionKey";
pub const SECTION_VALUE_SCHEMA: &str = "org.example.dayframe.settings.section";
pub const SECTION_SELECTOR_SCHEMA: &str = "org.example.dayframe.settings.sectionSelector";

/// One Settings section: its title, and the presentation published for its detail, as an
/// attachment of the row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Section {
    pub title: String,
    pub detail: Attachment,
}

/// The Settings micro's package declaration: it declares the sections collection, publishes its
/// list and section presentations, requires the shell's navigation, and contributes its panel
/// row to the shell.
pub fn manifest() -> MicroManifest {
    let presentation = |name: &str| PublicationDeclaration {
        name: name.into(),
        kind: PublicationKind::Presentation,
        contract: None,
    };
    let requires = |binding: &str, contract: &str| Request {
        binding: binding.into(),
        contract: Nsid::literal(contract),
        access: None,
        necessity: Necessity::Required,
        resolution: BindingResolution::Configured,
        min_revision: None,
    };
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![presentation(LIST), presentation(SECTION)],
        requires: vec![
            requires(SHELL_OPENED, micro_shell_manifest::OPENED_CONTRACT),
            requires(SHELL_NAV_BUTTON, micro_shell_manifest::NAV_BUTTON_CONTRACT),
        ],
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
        collection_contracts: vec![CollectionContract {
            id: Nsid::literal(SECTIONS),
            selector_schema: Nsid::literal(SECTION_SELECTOR_SCHEMA),
            key_schema: Nsid::literal(SECTION_KEY_SCHEMA),
            value_schema: Nsid::literal(SECTION_VALUE_SCHEMA),
            write_context_schema: None,
            offered: vec![CollectionCapability::Read, CollectionCapability::Register],
        }],
        // Settings reads sections through a binding configuration resolves; with one declared
        // sections collection the default binds it. It also writes its panel row into the
        // shell's panels collection.
        collection_requests: vec![
            Request {
                binding: SECTIONS_BINDING.into(),
                contract: Nsid::literal(SECTIONS),
                access: Some(Access::Read),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
            Request {
                binding: PANELS_BINDING.into(),
                contract: Nsid::literal(micro_shell_manifest::PANELS),
                access: Some(Access::Write),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
        ],
        // Declared, not written at runtime: the shell lists Settings on home while it is
        // stopped, and opening it is what starts it.
        contributions: vec![ContributionDeclaration {
            id: "panel".into(),
            binding: PANELS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Settings",
                "sockets": ["list"],
                "content": { "$attachment": 0 },
                "details": { "$attachment": 1 },
            }),
            attachments: vec![
                clay_mdk::AttachmentDeclaration {
                    index: 0,
                    publication: LIST.into(),
                },
                clay_mdk::AttachmentDeclaration {
                    index: 1,
                    publication: SECTION.into(),
                },
            ],
        }],
        extra: Default::default(),
    }
}

/// The Settings package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::new(ID, PACKAGE_VERSION, manifest()).with_authority(AUTHORITY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_mdk::host::declared_rows;

    // The manifest is wire data (ADR 030 §11); the snapshot follows the current shape so drift
    // is a reviewable diff.
    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }

    #[test]
    fn section_row_wire_shape_is_reviewable() {
        let mut bytes = Vec::new();
        ciborium::into_writer(
            &Section {
                title: "Counter (Wasm)".into(),
                detail: Attachment::at(0),
            },
            &mut bytes,
        )
        .unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        insta::assert_snapshot!(hex);
    }

    #[test]
    fn the_manifest_declares_a_settings_panel_row() {
        sidecar().validate().unwrap();
        let rows = declared_rows(&clay_mdk::MicroId::local(ID), &sidecar()).unwrap();
        assert_eq!(rows.keys().collect::<Vec<_>>(), [PANELS_BINDING]);
    }
}
