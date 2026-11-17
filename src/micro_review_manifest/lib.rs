//! Engine-neutral declarations of the intxt review micro (ADR 030 §§11-15): its identity, the one
//! presentation it publishes and the node methods it may call. The wasm guest reads only the
//! constants; the composition root reads `manifest()`.
use clay_mdk::ComponentDecl;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::PackageDecl;
use clay_mdk::Sidecar;

pub const ID: &str = "dayframe.review";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the micro holds. It publishes no call yet, so it declares nothing under it.
pub const AUTHORITY: &str = "org.example.dayframe.review";
/// The review micro's presentation: the PR list, a PR and its intxts, an intxt and its entries.
/// The micro navigates among them itself, so it is one publication until a micro needs to mount
/// a PR or an intxt directly.
pub const VIEW: &str = "view";
/// The key under which the manifest carries the node methods the micro may call. See
/// [`manifest`].
pub const INTERIM_METHODS_KEY: &str = "interimMethods";
/// `org.example.intxt.summarizeIntxts`: the title and description of each intxt a PR names, in
/// one call.
pub const SUMMARIZE_INTXTS: &str = "org.example.intxt.summarizeIntxts";
/// `org.example.daybook.docs.readDatums`: the datums of a document (a PR, an intxt).
pub const READ_DATUMS: &str = "org.example.daybook.docs.readDatums";

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

pub fn manifest() -> MicroManifest {
    let mut manifest = ManifestBuilder::new().component::<ReviewView>().build();
    // INTERIM: the manifest has no shape yet for the node methods a micro may call (a micro
    // reaches only Clay collections, config and other micros' publications today). Until the
    // Clay host import for micro -> node calls lands with its own request shape, the list rides
    // in `extra`, which readers carry unread. It replaces nothing and grants nothing: the grant
    // is whatever the composition gives the micro's `Context`.
    manifest.extra.insert(
        INTERIM_METHODS_KEY.into(),
        serde_json::json!([SUMMARIZE_INTXTS, READ_DATUMS]),
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
