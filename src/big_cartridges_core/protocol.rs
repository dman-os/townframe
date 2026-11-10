//! The cartridge registry protocol (ADR 028 §1): the wire contract between the node-side registry
//! service and whoever publishes to or reads from it.
//!
//! Every operation names its caller by the request's `Authorization: Bearer` credential (the
//! envelope's HTTP metadata, `Authorization::from_metadata`), never by a body field; a request
//! without one fails with `ContextError::Unauthenticated`. The credential is the operator's own identity
//! capability, or a handle the registry minted through [`MintCartridgesCapability`] that carries a
//! [`CartridgesScope`]. An arbitrary actor presenting its identity is refused. The registry does no
//! enablement: it versions what is published and keeps a published version immutable, and each
//! runtime decides for itself what to enable from it.

use crate::Cartridge;
use crate::CartridgeId;
use crate::Revision;
use crate::Version;
use crate::signature::SignatureError;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::declare_service_protocol;
use service_core_rs::service_method;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// What a minted registry handle may do. Without `publish` it reads the registry (list, get,
/// watch); with it, it also publishes and removes. `namespaces` narrows publishing and removal to
/// cartridges whose id lives under one of those DIDs; absent means any namespace, which is what the
/// operator's own identity has. A handle can only be minted with a scope its minter covers, so a
/// namespace set only ever narrows.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CartridgesScope {
    #[serde(default)]
    pub publish: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<BTreeSet<String>>,
}

impl CartridgesScope {
    pub fn covers(&self, other: &Self) -> bool {
        (self.publish || !other.publish)
            && match (&self.namespaces, &other.namespaces) {
                (None, _) => true,
                // Without `publish` the namespaces are never consulted.
                (Some(_), None) => !other.publish,
                (Some(own), Some(narrower)) => narrower.is_subset(own),
            }
    }

    /// Whether this scope may publish or remove cartridges under `namespace`.
    pub fn may_publish_under(&self, namespace: &str) -> bool {
        self.publish
            && self
                .namespaces
                .as_ref()
                .is_none_or(|namespaces| namespaces.contains(namespace))
    }
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintCartridgesCapability {
    pub scope: CartridgesScope,
    pub rights: Rights,
}

/// Publish one version. Every artifact of the envelope must already be in the local blob store.
/// The envelope must carry the signature of its namespace DID, except that an unsigned envelope
/// under the node's own publisher DID (see [`GetPublisher`]) is signed by the node. The same
/// label with an identical envelope is an idempotent no-op; the same label with a different one
/// is refused with [`CartridgesError::VersionExists`]; a new label must be greater than every label
/// the cartridge has or had ([`CartridgesError::Downgrade`]).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishCartridgeVersion {
    pub envelope: Cartridge,
}

/// The DID this node publishes under: the `did:key` of the node's Ed25519 signing key. Cartridge ids
/// built under it can be published unsigned and are signed by the node.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPublisher {}

/// Installs a cartridge from an existing document, one an author wrote and signed elsewhere, by
/// entering it in the registry drawer, so the registry follows it as the author publishes more.
/// The document can be in any drawer the caller can read, including one that synced in from a
/// peer. The registry holds no standing access to anything but its own drawer: the caller lends
/// it two handles for this one call.
/// - `read` is a docs-domain handle that reads `doc_id`. The registry reads the cartridge identity
///   through it before anything is entered.
/// - `reach` is a drawers-domain handle that reaches `doc_id` at Read or above. Entering the
///   document gives the registry drawer's members Read on it, which the caller must itself hold.
///
/// The document is entered at Read: the registry serves an author's document and never writes
/// it, so publishing to or removing versions of a cartridge whose document was installed this way is
/// refused (`Denied`) rather than written into the author's document.
/// The document must carry a cartridge identity datum. Nothing is checked here beyond that: each
/// version in it is verified as it is folded in, exactly as for a document that synced into the
/// drawer, so an unsigned or badly signed version is listed as unverified and never served, and
/// no downgrade rule applies (arrival order is not version order).
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportCartridgeDocument {
    pub doc_id: DocId,
    pub read: CapId,
    pub reach: CapId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCartridges {}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCartridge {
    pub id: CartridgeId,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCartridgeVersion {
    pub id: CartridgeId,
    pub version: Version,
}

/// Drops one version's envelope and artifact references from the cartridge's document. The bytes are
/// released from retention unless another document still references them.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveCartridgeVersion {
    pub id: CartridgeId,
    pub version: Version,
}

/// Drops every version and the identity of a cartridge.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveCartridge {
    pub id: CartridgeId,
}

/// A long poll over registry changes (ADR 023). `after` is the cursor of the last answer the
/// caller consumed, or `None` for the registry's current state. `limit` and `hold_ms` are
/// requests the service clamps; `0` asks for the default page and no hold.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchCartridges {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<CartridgesCursor>,
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub hold_ms: u32,
}

/// Where a watcher is. The registry is derived from the drawer when a node boots, so its change
/// sequence restarts with the process: `epoch` names the process incarnation the `seq` counts
/// in, and a cursor from another epoch is answered with the full current state
/// ([`CartridgeChanges::reset`]) instead of a delta the service can no longer compute.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgesCursor {
    pub epoch: Uuid7,
    pub seq: u64,
}

/// One cartridge whose record changed (a version published, removed or found conflicting, the cartridge
/// removed). The caller re-reads the cartridge; it is not carried, so a change never goes stale.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeChange {
    pub seq: u64,
    pub cartridge_id: CartridgeId,
    /// The cartridge no longer exists in the registry.
    pub removed: bool,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeChanges {
    pub changes: Vec<CartridgeChange>,
    /// The cursor to send next. Every change here is at or below it.
    pub through: CartridgesCursor,
    /// No further change is waiting past `through`.
    pub caught_up: bool,
    /// The answer is the registry's full state rather than a delta: the caller had no cursor or
    /// one from another epoch, so a cartridge it holds and that is absent here is gone.
    pub reset: bool,
}

/// One version of a cartridge as the registry serves it: always the content the node first saw for
/// the label. `conflict` says the drawer's datum has since changed or become unreadable (another
/// node wrote it); the registry keeps serving the first-seen envelope and never resolves that
/// silently.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeVersionRecord {
    pub id: CartridgeId,
    pub version: Version,
    pub revision: Revision,
    pub envelope: Cartridge,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
}

/// A version in a cartridge's listing: its envelope is fetched with [`GetCartridgeVersion`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeVersionSummary {
    pub version: Version,
    pub revision: Revision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
    /// Why the version's signature did not verify (a synced write whose signature is bad, or
    /// whose DID method cannot be verified here). Such a version is listed so it is visible, but
    /// it is not served: [`GetCartridgeVersion`] refuses it with [`CartridgesError::Unverified`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unverified: Option<String>,
}

/// One cartridge in the registry, from its document in the drawer.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeRecord {
    pub id: CartridgeId,
    pub doc_id: DocId,
    /// Ascending by version.
    pub versions: Vec<CartridgeVersionSummary>,
    /// Version slots in the document that are not a servable version, by slot, with why: an
    /// unparseable label, an envelope that does not decode, one whose id or version disagrees
    /// with its slot, and no first-seen content to fall back on.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unreadable: BTreeMap<String, String>,
    /// Other documents in the drawer that also claim this cartridge id (two nodes each created one
    /// before they synced). The document with the lowest id is served; these are ignored, never
    /// merged, so a node that synced them sees the same cartridge as one that did not.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub duplicate_docs: Vec<DocId>,
}

/// The result of a publish: the version as stored, and whether it was already.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedCartridgeVersion {
    pub record: CartridgeVersionRecord,
    /// The label was already published with an identical envelope, so nothing was written.
    pub unchanged: bool,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum CartridgesError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("the presented capability does not permit this operation")]
    Denied,
    #[error("authority failed: {0}")]
    Authority(AuthorityServiceError),
    #[error("authority transport failed: {0}")]
    AuthorityUnavailable(String),
    #[error("registry state failed: {0}")]
    Storage(String),
    #[error("the envelope was refused: {0}")]
    Rejected(String),
    /// A published version never changes content: the label is taken by another envelope.
    #[error(
        "cartridge `{id}` {version} is already published at revision {existing}; this envelope is \
         {incoming}"
    )]
    VersionExists {
        id: CartridgeId,
        version: Box<Version>,
        existing: Revision,
        incoming: Revision,
    },
    /// The envelope's signature is missing or does not verify against its namespace DID, or the
    /// DID's method cannot be verified without resolution (fail closed).
    #[error("the envelope's signature was refused: {0}")]
    Signature(#[from] SignatureError),
    /// An unsigned envelope can be signed by the node only for the node's own DID.
    #[error(
        "the envelope is unsigned and `{namespace}` is not this node's publisher DID, so the node \
         cannot sign it"
    )]
    UnsignedForeignNamespace { namespace: String },
    /// A new label must order after every label the cartridge has or had, so a validly signed older
    /// version cannot be published as new (a downgrade attack). Taking an older version is an
    /// explicit runtime action, not a registry one.
    #[error(
        "cartridge `{id}` {version} does not order after {newest}, the newest version it has had"
    )]
    Downgrade {
        id: CartridgeId,
        version: Box<Version>,
        newest: Box<Version>,
    },
    /// The handle's namespace scope does not include the cartridge's namespace.
    #[error("the presented capability may not publish under `{namespace}`")]
    NamespaceNotPermitted { namespace: String },
    /// The version exists in the drawer but its signature did not verify, so it is not served.
    #[error("cartridge `{id}` {version} is not served: {reason}")]
    Unverified {
        id: CartridgeId,
        version: Box<Version>,
        reason: String,
    },
    /// An artifact's bytes are not in the local blob store, so the registry would pin bytes it
    /// cannot serve.
    #[error("artifact `{artifact}` of cartridge `{id}` is not in the local blob store")]
    ArtifactUnavailable { id: CartridgeId, artifact: String },
    #[error("cartridge `{0}` is not in the registry")]
    UnknownCartridge(CartridgeId),
    #[error("cartridge `{id}` has no version {version}")]
    UnknownVersion {
        id: CartridgeId,
        version: Box<Version>,
    },
}

service_method!(MintCartridgesCapability => Capability, CartridgesError, procedure("org.example.daybook.cartridges.mintCartridgesCapability"));
service_method!(PublishCartridgeVersion => PublishedCartridgeVersion, CartridgesError, procedure("org.example.daybook.cartridges.publishCartridgeVersion"));
service_method!(RemoveCartridgeVersion => (), CartridgesError, procedure("org.example.daybook.cartridges.removeCartridgeVersion"));
service_method!(RemoveCartridge => (), CartridgesError, procedure("org.example.daybook.cartridges.removeCartridge"));
service_method!(ImportCartridgeDocument => CartridgeRecord, CartridgesError, procedure("org.example.daybook.cartridges.importCartridgeDocument"));
/// The result of [`GetPublisher`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgePublisher {
    pub publisher: String,
}

/// The result of [`ListCartridges`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CartridgeList {
    pub cartridges: Vec<CartridgeRecord>,
}

service_method!(GetPublisher => CartridgePublisher, CartridgesError, query("org.example.daybook.cartridges.getPublisher"));
service_method!(ListCartridges => CartridgeList, CartridgesError, query("org.example.daybook.cartridges.listCartridges"));
service_method!(GetCartridge => CartridgeRecord, CartridgesError, query("org.example.daybook.cartridges.getCartridge"));
service_method!(GetCartridgeVersion => CartridgeVersionRecord, CartridgesError, query("org.example.daybook.cartridges.getCartridgeVersion"));
service_method!(WatchCartridges => CartridgeChanges, CartridgesError, procedure("org.example.daybook.cartridges.watchCartridges"));

declare_service_protocol! {
    pub enum CartridgesProtocol => CartridgesMessage {
        unary {
            MintCartridgesCapability(MintCartridgesCapability),
            PublishCartridgeVersion(PublishCartridgeVersion),
            RemoveCartridgeVersion(RemoveCartridgeVersion),
            RemoveCartridge(RemoveCartridge),
            ImportCartridgeDocument(ImportCartridgeDocument),
            GetPublisher(GetPublisher),
            ListCartridges(ListCartridges),
            GetCartridge(GetCartridge),
            GetCartridgeVersion(GetCartridgeVersion),
            WatchCartridges(WatchCartridges)
        }
        stream {}
    }
}
