//! The Layout contract (ADR 031 §13): the identity and manifest of the Layout micro, the
//! `layout.panels` collection other micros contribute panels to, and the configuration datum
//! that says which panels the viewer arranged. Consumers depend on this crate and never on the
//! Layout micro itself; Clay knows nothing of layouts.
//!
//! A panel row is keyed by its contributor's chosen *registration id* (CBOR text) and holds a
//! [`Panel`]: a title and an attachment for the panel's presentation. A micro whose panel
//! should be listed while it is stopped declares the row in its manifest, which stores a lazy
//! endpoint to its own presentation: the layout reads the title without starting the
//! contributor, and only a visible panel's slot redeems the endpoint, which activates or joins
//! the contributor for as long as the panel is mounted.
//!
//! The arrangement is the viewer's, not the contributors': [`LayoutConfig`] names registration
//! ids, so it survives a contributor being uninstalled, disabled, or reinstalled under the same
//! id (the layout shows a placeholder for that id and keeps it).
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

pub const ID: &str = "dayframe.layout";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the layout holds: every NSID it declares (its collection contract and
/// schemas) lies under it, everything else it names is a request.
pub const AUTHORITY: &str = "org.example.dayframe.layout";
pub const CONFIG_NAME: &str = "layout";
pub const CONFIG_TAG: &str = "org.example.dayframe.layout.config";
pub const CONFIG_KEY: &str = "main";
/// The Layout presentation, which the shell mounts as a list panel.
pub const VIEW: &str = "view";
/// The contract NSID, and so the declared collection's id, of the panels collection.
pub const PANELS: &str = "org.example.dayframe.layout.panels";
/// The local binding name under which the layout reads its panels collection, and under which
/// a contributor requests write access to it.
pub const PANELS_BINDING: &str = "panels";
/// The local binding name under which the layout contributes its panel to the shell's panels
/// (the layout's own `layout.panels` collection, bound as [`PANELS_BINDING`], is the panels it
/// arranges inside itself).
pub const SHELL_PANELS_BINDING: &str = "shell-panels";
/// The local binding name under which the layout requires the shell's sidebar/back button for
/// its app bar: the name of the shell publication it binds, so a catalog satisfies it from the
/// shell (`InstalledMicro::satisfied_by`).
pub const SHELL_NAV_BUTTON: &str = micro_shell_manifest::NAV_BUTTON;
/// Schema NSIDs of [`PANELS`] keys (the contributor's registration id, text) and values
/// ([`Panel`]), and of its selector (the whole collection).
pub const PANEL_KEY_SCHEMA: &str = "org.example.dayframe.layout.panelKey";
pub const PANEL_VALUE_SCHEMA: &str = "org.example.dayframe.layout.panel";
pub const PANEL_SELECTOR_SCHEMA: &str = "org.example.dayframe.layout.panelSelector";

/// One installed panel: its title, and the presentation published for its content, as an
/// attachment of the row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Panel {
    pub title: String,
    pub content: Attachment,
}

/// The viewer's arrangement, kept in the layout's configuration document: the registration ids
/// of the panels that are shown, in display order. An id need not name an installed panel.
///
/// With no document yet the arrangement is empty ([`Default`]): nothing is shown until the
/// viewer shows a panel, so a freshly installed contributor never starts just because it
/// registered a panel.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LayoutConfig {
    #[serde(default)]
    pub visible: Vec<String>,
}

/// The Layout micro's package declaration: it declares the panels collection, publishes its
/// view, requires the shell's sidebar/back button, and contributes the view as a list panel the
/// shell's home offers.
pub fn manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: VIEW.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![Request {
            binding: SHELL_NAV_BUTTON.into(),
            contract: Nsid::literal(micro_shell_manifest::NAV_BUTTON_CONTRACT),
            access: None,
            necessity: Necessity::Required,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
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
            id: Nsid::literal(PANELS),
            selector_schema: Nsid::literal(PANEL_SELECTOR_SCHEMA),
            key_schema: Nsid::literal(PANEL_KEY_SCHEMA),
            value_schema: Nsid::literal(PANEL_VALUE_SCHEMA),
            write_context_schema: None,
            offered: vec![CollectionCapability::Read, CollectionCapability::Register],
        }],
        // The layout reads panels through a binding configuration resolves; with one declared
        // panels collection the default binds it. It also writes its panel row into the shell's
        // panels collection.
        collection_requests: vec![
            Request {
                binding: PANELS_BINDING.into(),
                contract: Nsid::literal(PANELS),
                access: Some(Access::Read),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
            Request {
                binding: SHELL_PANELS_BINDING.into(),
                contract: Nsid::literal(micro_shell_manifest::PANELS),
                access: Some(Access::Write),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
        ],
        contributions: vec![ContributionDeclaration {
            id: "panel".into(),
            binding: SHELL_PANELS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Layout",
                "sockets": ["list"],
                "content": { "$attachment": 0 },
            }),
            attachments: vec![clay_mdk::AttachmentDeclaration {
                index: 0,
                publication: VIEW.into(),
            }],
        }],
        extra: Default::default(),
    }
}

/// The Layout package as its sidecar carries it: identity, authority and manifest.
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
    fn panel_row_wire_shape_is_reviewable() {
        let mut bytes = Vec::new();
        ciborium::into_writer(
            &Panel {
                title: "Counter (Wasm)".into(),
                content: Attachment::at(0),
            },
            &mut bytes,
        )
        .unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        insta::assert_snapshot!(hex);
    }

    #[test]
    fn the_manifest_declares_a_shell_panel_row() {
        sidecar().validate().unwrap();
        let rows = declared_rows(&clay_mdk::MicroId::local(ID), &sidecar()).unwrap();
        assert_eq!(rows.keys().collect::<Vec<_>>(), [SHELL_PANELS_BINDING]);
    }

    // The arrangement is a stored datum: a document written by an older or newer layout may
    // lack the field, which reads as the empty arrangement.
    #[test]
    fn an_arrangement_without_the_visible_list_is_empty() {
        let config: LayoutConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(config, LayoutConfig::default());
    }
}
