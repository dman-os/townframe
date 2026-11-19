//! Engine-neutral declarations of the intxt review micro (ADR 030 §§11-15): its identity, the one
//! presentation it publishes, the layout panel row that lists it, the configuration it reads and
//! the node methods it may call. The wasm guest reads only the constants; the composition root
//! reads `manifest()`.
use clay_mdk::ComponentDecl;
use clay_mdk::ConfigDecl;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PackageDecl;
use clay_mdk::Sidecar;
use micro_layout_manifest::LayoutPanels;
use micro_layout_manifest::Panel;
use serde::Deserialize;
use serde::Serialize;

pub const ID: &str = "dayframe.review";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the micro holds. It publishes no call yet, so it declares nothing under it.
pub const AUTHORITY: &str = "org.example.dayframe.review";
/// The review micro's presentation: the PR list, a PR and its intxts, an intxt and its entries.
/// The micro navigates among them itself, so it is one publication until a micro needs to mount
/// a PR or an intxt directly.
pub const VIEW: &str = "view";
/// The local binding name under which the micro contributes its layout panel.
pub const PANELS_BINDING: &str = "panels";
pub const CONFIG_NAME: &str = "review";
pub const CONFIG_TAG: &str = "org.example.dayframe.review.config";
pub const CONFIG_KEY: &str = "main";
/// The key under which the manifest carries the node methods the micro may call. See
/// [`manifest`].
pub const INTERIM_METHODS_KEY: &str = "interimMethods";
/// `org.example.daybook.docs.readDatums`: the datums of a document (a PR, an intxt).
pub const READ_DATUMS: &str = "org.example.daybook.docs.readDatums";
/// `org.example.daybook.docs.pollDatums`: which documents of the GitHub drawer hold a `githubPull`.
pub const POLL_DATUMS: &str = "org.example.daybook.docs.pollDatums";
/// `org.example.daybook.blobs.read`: the bytes of a blob, such as the diff a commit entry names.
pub const READ_BLOB: &str = "org.example.daybook.blobs.read";

/// The review package's identity.
pub struct Review;

impl PackageDecl for Review {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The micro's presentation, published as [`VIEW`].
pub struct ReviewView;

impl ComponentDecl for ReviewView {
    const NAME: &'static str = VIEW;
}

/// What a person configures at install for the micro to work on a real node. INTERIM: it becomes
/// `#drawers` permbinds plus the 6.2 domain handle, and this entry goes away.
///
/// The ids are text, as a configuration datum carries them; the micro parses them where it reads
/// them (configuration is external input).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewConfig {
    /// The node-level GitHub drawer `day x intxt github pull` imports PR documents into (a
    /// drawer id: its descriptor document's id).
    pub github_drawer: String,
    /// The drawers the intxts the PRs name live in, for the capability's scope.
    pub intxt_drawers: Vec<String>,
    /// A doc capability (a `CapId`) the composition minted for this micro's actor over the
    /// drawers above, with read rights. It is passed to `readDatums` and `pollDatums`; the micro
    /// never mints one.
    pub doc_cap: String,
    /// A blobs capability (a `CapId`) the composition minted for this micro's actor, with read
    /// scope over the patch blobs of the intxts above. It is passed to `blobs.read`; the micro
    /// never mints one.
    pub blob_cap: String,
}

/// The review configuration facet, requested through [`ReviewConfig`].
pub struct ReviewConfigDecl;

impl ConfigDecl for ReviewConfigDecl {
    const BINDING: &'static str = CONFIG_NAME;
    const NSID: &'static str = CONFIG_TAG;
    const KEY: &'static str = CONFIG_KEY;
    type Value = ReviewConfig;
}

pub fn manifest() -> MicroManifest {
    let mut manifest = ManifestBuilder::new()
        .view::<ReviewView>()
        .config::<ReviewConfigDecl>()
        .writes::<LayoutPanels>(PANELS_BINDING, Necessity::Optional)
        .contributes::<LayoutPanels>("layout-panel", ID, |at| Panel {
            title: "Review".into(),
            content: at.own::<ReviewView>(),
        })
        .build();
    // INTERIM: the manifest has no shape yet for the node methods a micro may call (a micro
    // reaches only Clay collections, config and other micros' publications today). Until the
    // Clay host import for micro -> node calls lands with its own request shape, the list rides
    // in `extra`, which readers carry unread. It replaces nothing and grants nothing: the grant
    // is whatever the composition gives the micro's `Context`.
    manifest.extra.insert(
        INTERIM_METHODS_KEY.into(),
        serde_json::json!([READ_DATUMS, POLL_DATUMS, READ_BLOB]),
    );
    manifest
}

/// The review package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<Review>(manifest())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The manifest is wire data (ADR 030 §11); the snapshot follows the current shape so drift
    // is a reviewable diff.
    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }
}
