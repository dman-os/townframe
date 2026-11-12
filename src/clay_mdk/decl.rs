//! The declaration vocabulary of a micro: one Rust type per thing a micro names, carrying the
//! constants that identify it on the wire. A type is declared once, here by an `impl`, and then
//! every use of it is generic over the type: the manifest builder
//! ([`crate::manifest::ManifestBuilder`]) lowers the declared types into the wire
//! [`crate::MicroManifest`], and [`crate::Context`]'s accessors take the type instead of a
//! string, so the manifest and the code that activates against it cannot name different things.
//!
//! The wire manifest is unchanged by this layer; it is the lowering target.
//!
//! Calls are not declared here yet: they will implement the shared `Method` trait (the
//! request type is the contract) once that trait is reachable from wasm micro crates, and
//! `publish_call`, `require` and the `own_call` attachment keep their string forms until then.
use crate::Attachment;
use crate::manifest::CollectionCapability;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A package's identity: what its sidecar carries.
pub trait PackageDecl {
    /// The plug name (`dayframe.counter`), not a full [`crate::MicroId`].
    const ID: &'static str;
    const VERSION: u32;
    /// The reverse-DNS NSID prefix the package holds; every NSID it declares lies under it.
    const AUTHORITY: &'static str;
}

/// A collection contract as a requester sees it: its NSID and the types of its rows. This is
/// all a micro that only reads or registers into someone else's collection needs, so it is the
/// bound of [`crate::Context::collection`] and [`crate::Context::registration`].
pub trait CollectionDecl: 'static {
    /// The contract NSID, which a request names and a declared collection is identified by.
    const NSID: &'static str;
    type Key: Serialize + DeserializeOwned + 'static;
    type Value: Serialize + DeserializeOwned + 'static;
}

/// A collection contract as its owner declares it: the schema NSIDs the wire contract names and
/// the capabilities it offers.
pub trait DeclaredCollection: CollectionDecl {
    /// Schema NSIDs of the keys, the values and the selector (the whole collection).
    const KEY_SCHEMA: &'static str;
    const VALUE_SCHEMA: &'static str;
    const SELECTOR_SCHEMA: &'static str;
    const OFFERED: &'static [CollectionCapability];
}

/// A configuration datum a micro requests read and write access to: one datum (`NSID`, `KEY`)
/// of the document bound under `BINDING`.
pub trait ConfigDecl {
    /// The local binding name of the configuration document.
    const BINDING: &'static str;
    /// The datum's datum tag.
    const NSID: &'static str;
    const KEY: &'static str;
    type Value: Serialize + DeserializeOwned;
}

/// A presentation a micro publishes, by the name `initialize` returns it under. What it
/// presents (properties, events) is being designed separately, so the declaration names the
/// publication only.
pub trait ComponentDecl {
    const NAME: &'static str;
}

/// One [`crate::SHELL_ROOTS`] row, keyed by the registering micro's id: a title and the root
/// presentation (an attachment of the row). The composition mounts every root it finds, each
/// filling the frame; arranging anything inside the frame is the root's own business (the shell
/// micro's panels).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootRow {
    pub title: String,
    pub content: Attachment,
}

/// The runtime-defined collection shell micros register their roots into.
pub struct ShellRoots;

impl CollectionDecl for ShellRoots {
    const NSID: &'static str = crate::SHELL_ROOTS;
    type Key = String;
    type Value = RootRow;
}
