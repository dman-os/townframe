//! The Toolbar contract: the identity and manifest of the Toolbar micro, and the
//! `toolbar.buttons` collection other micros contribute buttons to. Consumers depend on this
//! crate and never on the Toolbar micro itself; Clay knows nothing of toolbars (ADR 031 §13).
//!
//! A button row is keyed by its contributor's chosen id (CBOR text) and holds a [`Button`]:
//! static metadata (a label and an icon name) and an attachment naming the call the button
//! performs, a call of the [`ACTION_CONTRACT`] contract (empty request and response) that the
//! toolbar owns and contributors implement. A micro whose button should exist while it is stopped declares the row in its
//! manifest, which stores a lazy endpoint to its own call publication: the toolbar renders the
//! metadata without starting the contributor, and pressing the button calls the endpoint, which
//! activates or joins the contributor for the length of the call.
use clay_mdk::Access;
use clay_mdk::Attachment;
use clay_mdk::BindingResolution;
use clay_mdk::CollectionCapability;
use clay_mdk::CollectionContract;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Request;
use clay_mdk::Sidecar;
use clay_mdk::host::SHELL_ROOTS;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.toolbar";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the toolbar holds: every NSID it declares (its collection contract and
/// schemas) lies under it, everything else it names is a request.
pub const AUTHORITY: &str = "org.example.dayframe.toolbar";
/// The contract NSID of the call a button's attachment performs: empty request, empty response
/// (CBOR `()` in and out). The toolbar defines it and a contributor's call publication
/// implements it; implementing is not owning (ADR 028 §5), so the contributor needs no request
/// and the NSID is under the toolbar's authority.
pub const ACTION_CONTRACT: &str = "org.example.dayframe.toolbar.action";
/// The Toolbar presentation, which the shell mounts as a root.
pub const VIEW: &str = "view";
/// The contract NSID, and so the declared collection's id, of the buttons collection.
pub const BUTTONS: &str = "org.example.dayframe.toolbar.buttons";
/// The local binding name under which the toolbar reads its buttons collection, and under which
/// a contributor requests write access to it.
pub const BUTTONS_BINDING: &str = "buttons";
/// The local binding name under which the toolbar registers its root into `shell.roots`.
pub const ROOTS_BINDING: &str = "roots";
/// Schema NSIDs of [`BUTTONS`] keys (the contributor's chosen id, text) and values
/// ([`Button`]), and of its selector (the whole collection).
pub const BUTTON_KEY_SCHEMA: &str = "org.example.dayframe.toolbar.buttonKey";
pub const BUTTON_VALUE_SCHEMA: &str = "org.example.dayframe.toolbar.button";
pub const BUTTON_SELECTOR_SCHEMA: &str = "org.example.dayframe.toolbar.buttonSelector";

/// One toolbar button: what it shows, and the call it performs, as an attachment of the row.
/// The attachment is a call of the [`ACTION_CONTRACT`] contract: an empty request and an empty
/// response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Button {
    pub label: String,
    /// A name from the toolbar's icon vocabulary (`"plus"`); the toolbar decides how it draws.
    pub icon: String,
    pub action: Attachment,
}

/// The Toolbar micro's package declaration: it declares the buttons collection, publishes its
/// view, and registers that view as a pinned shell root (it shows whether or not Settings is
/// open).
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
        collection_contracts: vec![CollectionContract {
            id: Nsid::literal(BUTTONS),
            selector_schema: Nsid::literal(BUTTON_SELECTOR_SCHEMA),
            key_schema: Nsid::literal(BUTTON_KEY_SCHEMA),
            value_schema: Nsid::literal(BUTTON_VALUE_SCHEMA),
            write_context_schema: None,
            offered: vec![CollectionCapability::Read, CollectionCapability::Register],
        }],
        // The toolbar reads buttons through a binding configuration resolves; with one declared
        // buttons collection the default binds it. It also registers its root row into the
        // runtime-defined `shell.roots` collection, which the runtime satisfies.
        collection_requests: vec![
            Request {
                binding: BUTTONS_BINDING.into(),
                contract: Nsid::literal(BUTTONS),
                access: Some(Access::Read),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
            Request {
                binding: ROOTS_BINDING.into(),
                contract: Nsid::literal(SHELL_ROOTS),
                access: Some(Access::Write),
                necessity: Necessity::Required,
                resolution: BindingResolution::Configured,
                min_revision: None,
            },
        ],
        contributions: vec![ContributionDeclaration {
            id: "root".into(),
            binding: ROOTS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Toolbar",
                "content": { "$attachment": 0 },
                "pinned": true,
            }),
            attachments: vec![clay_mdk::AttachmentDeclaration {
                index: 0,
                publication: VIEW.into(),
            }],
        }],
        extra: Default::default(),
    }
}

/// The Toolbar package as its sidecar carries it: identity, authority and manifest.
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
    fn button_row_wire_shape_is_reviewable() {
        let mut bytes = Vec::new();
        ciborium::into_writer(
            &Button {
                label: "Increment".into(),
                icon: "plus".into(),
                action: Attachment::at(0),
            },
            &mut bytes,
        )
        .unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        insta::assert_snapshot!(hex);
    }

    #[test]
    fn the_manifest_declares_a_pinned_toolbar_root_row() {
        sidecar().validate().unwrap();
        let rows = declared_rows(&clay_mdk::MicroId::local(ID), &sidecar()).unwrap();
        assert_eq!(rows.keys().collect::<Vec<_>>(), [ROOTS_BINDING]);
    }
}
