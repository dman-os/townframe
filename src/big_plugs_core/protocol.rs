//! The plug registry protocol (ADR 028 §1): the wire contract between the node-side registry
//! service and whoever publishes to or reads from it.
//!
//! Every operation names its caller by the request's `Authorization: Bearer` credential (the
//! envelope's HTTP metadata, `Authorization::from_metadata`), never by a body field; a request
//! without one fails with `ContextError::Unauthenticated`. The credential is the operator's own identity
//! capability, or a handle the registry minted through [`MintPlugsCapability`] that carries a
//! [`PlugsScope`]. An arbitrary actor presenting its identity is refused. The registry does no
//! enablement: it versions what is published and keeps a published version immutable, and each
//! runtime decides for itself what to enable from it.

use crate::Plug;
use crate::PlugId;
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
/// plugs whose id lives under one of those DIDs; absent means any namespace, which is what the
/// operator's own identity has. A handle can only be minted with a scope its minter covers, so a
/// namespace set only ever narrows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlugsScope {
    #[serde(default)]
    pub publish: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<BTreeSet<String>>,
}

impl PlugsScope {
    pub fn covers(&self, other: &Self) -> bool {
        (self.publish || !other.publish)
            && match (&self.namespaces, &other.namespaces) {
                (None, _) => true,
                // Without `publish` the namespaces are never consulted.
                (Some(_), None) => !other.publish,
                (Some(own), Some(narrower)) => narrower.is_subset(own),
            }
    }

    /// Whether this scope may publish or remove plugs under `namespace`.
    pub fn may_publish_under(&self, namespace: &str) -> bool {
        self.publish
            && self
                .namespaces
                .as_ref()
                .is_none_or(|namespaces| namespaces.contains(namespace))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintPlugsCapability {
    pub scope: PlugsScope,
    pub rights: Rights,
}

/// Publish one version. Every artifact of the envelope must already be in the local blob store.
/// The envelope must carry the signature of its namespace DID, except that an unsigned envelope
/// under the node's own publisher DID (see [`GetPublisher`]) is signed by the node. The same
/// label with an identical envelope is an idempotent no-op; the same label with a different one
/// is refused with [`PlugsError::VersionExists`]; a new label must be greater than every label
/// the plug has or had ([`PlugsError::Downgrade`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishPlugVersion {
    pub envelope: Plug,
}

/// The DID this node publishes under: the `did:key` of the node's Ed25519 signing key. Plug ids
/// built under it can be published unsigned and are signed by the node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPublisher {}

/// Installs a plug from an existing document, one an author wrote and signed elsewhere, by
/// entering it in the registry drawer, so the registry follows it as the author publishes more.
/// The document can be in any drawer the caller can read, including one that synced in from a
/// peer. The registry holds no standing access to anything but its own drawer: the caller lends
/// it two handles for this one call.
/// - `read` is a docs-domain handle that reads `doc_id`. The registry reads the plug identity
///   through it before anything is entered.
/// - `reach` is a drawers-domain handle that reaches `doc_id` at Read or above. Entering the
///   document gives the registry drawer's members Read on it, which the caller must itself hold.
///
/// The document is entered at Read: the registry serves an author's document and never writes
/// it, so publishing to or removing versions of a plug whose document was installed this way is
/// refused (`Denied`) rather than written into the author's document.
/// The document must carry a plug identity datum. Nothing is checked here beyond that: each
/// version in it is verified as it is folded in, exactly as for a document that synced into the
/// drawer, so an unsigned or badly signed version is listed as unverified and never served, and
/// no downgrade rule applies (arrival order is not version order).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportPlugDocument {
    pub doc_id: DocId,
    pub read: CapId,
    pub reach: CapId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPlugs {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPlug {
    pub id: PlugId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPlugVersion {
    pub id: PlugId,
    pub version: Version,
}

/// Drops one version's envelope and artifact references from the plug's document. The bytes are
/// released from retention unless another document still references them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovePlugVersion {
    pub id: PlugId,
    pub version: Version,
}

/// Drops every version and the identity of a plug.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovePlug {
    pub id: PlugId,
}

/// A long poll over registry changes (ADR 023). `after` is the cursor of the last answer the
/// caller consumed, or `None` for the registry's current state. `limit` and `hold_ms` are
/// requests the service clamps; `0` asks for the default page and no hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchPlugs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<PlugsCursor>,
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub hold_ms: u32,
}

/// Where a watcher is. The registry is derived from the drawer when a node boots, so its change
/// sequence restarts with the process: `epoch` names the process incarnation the `seq` counts
/// in, and a cursor from another epoch is answered with the full current state
/// ([`PlugChanges::reset`]) instead of a delta the service can no longer compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugsCursor {
    pub epoch: Uuid7,
    pub seq: u64,
}

/// One plug whose record changed (a version published, removed or found conflicting, the plug
/// removed). The caller re-reads the plug; it is not carried, so a change never goes stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugChange {
    pub seq: u64,
    pub plug_id: PlugId,
    /// The plug no longer exists in the registry.
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugChanges {
    pub changes: Vec<PlugChange>,
    /// The cursor to send next. Every change here is at or below it.
    pub through: PlugsCursor,
    /// No further change is waiting past `through`.
    pub caught_up: bool,
    /// The answer is the registry's full state rather than a delta: the caller had no cursor or
    /// one from another epoch, so a plug it holds and that is absent here is gone.
    pub reset: bool,
}

/// One version of a plug as the registry serves it: always the content the node first saw for
/// the label. `conflict` says the drawer's datum has since changed or become unreadable (another
/// node wrote it); the registry keeps serving the first-seen envelope and never resolves that
/// silently.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugVersionRecord {
    pub id: PlugId,
    pub version: Version,
    pub revision: Revision,
    pub envelope: Plug,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
}

/// A version in a plug's listing: its envelope is fetched with [`GetPlugVersion`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugVersionSummary {
    pub version: Version,
    pub revision: Revision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
    /// Why the version's signature did not verify (a synced write whose signature is bad, or
    /// whose DID method cannot be verified here). Such a version is listed so it is visible, but
    /// it is not served: [`GetPlugVersion`] refuses it with [`PlugsError::Unverified`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unverified: Option<String>,
}

/// One plug in the registry, from its document in the drawer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlugRecord {
    pub id: PlugId,
    pub doc_id: DocId,
    /// Ascending by version.
    pub versions: Vec<PlugVersionSummary>,
    /// Version slots in the document that are not a servable version, by slot, with why: an
    /// unparseable label, an envelope that does not decode, one whose id or version disagrees
    /// with its slot, and no first-seen content to fall back on.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unreadable: BTreeMap<String, String>,
    /// Other documents in the drawer that also claim this plug id (two nodes each created one
    /// before they synced). The document with the lowest id is served; these are ignored, never
    /// merged, so a node that synced them sees the same plug as one that did not.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub duplicate_docs: Vec<DocId>,
}

/// The result of a publish: the version as stored, and whether it was already.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedPlugVersion {
    pub record: PlugVersionRecord,
    /// The label was already published with an identical envelope, so nothing was written.
    pub unchanged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum PlugsError {
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
        "plug `{id}` {version} is already published at revision {existing}; this envelope is \
         {incoming}"
    )]
    VersionExists {
        id: PlugId,
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
    /// A new label must order after every label the plug has or had, so a validly signed older
    /// version cannot be published as new (a downgrade attack). Taking an older version is an
    /// explicit runtime action, not a registry one.
    #[error("plug `{id}` {version} does not order after {newest}, the newest version it has had")]
    Downgrade {
        id: PlugId,
        version: Box<Version>,
        newest: Box<Version>,
    },
    /// The handle's namespace scope does not include the plug's namespace.
    #[error("the presented capability may not publish under `{namespace}`")]
    NamespaceNotPermitted { namespace: String },
    /// The version exists in the drawer but its signature did not verify, so it is not served.
    #[error("plug `{id}` {version} is not served: {reason}")]
    Unverified {
        id: PlugId,
        version: Box<Version>,
        reason: String,
    },
    /// An artifact's bytes are not in the local blob store, so the registry would pin bytes it
    /// cannot serve.
    #[error("artifact `{artifact}` of plug `{id}` is not in the local blob store")]
    ArtifactUnavailable { id: PlugId, artifact: String },
    #[error("plug `{0}` is not in the registry")]
    UnknownPlug(PlugId),
    #[error("plug `{id}` has no version {version}")]
    UnknownVersion { id: PlugId, version: Box<Version> },
}

service_method!(MintPlugsCapability => Capability, PlugsError, procedure("org.example.daybook.plugs.mintPlugsCapability"));
service_method!(PublishPlugVersion => PublishedPlugVersion, PlugsError, procedure("org.example.daybook.plugs.publishPlugVersion"));
service_method!(RemovePlugVersion => (), PlugsError, procedure("org.example.daybook.plugs.removePlugVersion"));
service_method!(RemovePlug => (), PlugsError, procedure("org.example.daybook.plugs.removePlug"));
service_method!(ImportPlugDocument => PlugRecord, PlugsError, procedure("org.example.daybook.plugs.importPlugDocument"));
service_method!(GetPublisher => String, PlugsError, query("org.example.daybook.plugs.getPublisher"));
service_method!(ListPlugs => Vec<PlugRecord>, PlugsError, query("org.example.daybook.plugs.listPlugs"));
service_method!(GetPlug => PlugRecord, PlugsError, query("org.example.daybook.plugs.getPlug"));
service_method!(GetPlugVersion => PlugVersionRecord, PlugsError, query("org.example.daybook.plugs.getPlugVersion"));
service_method!(WatchPlugs => PlugChanges, PlugsError, procedure("org.example.daybook.plugs.watchPlugs"));

declare_service_protocol! {
    pub enum PlugsProtocol => PlugsMessage {
        unary {
            MintPlugsCapability(MintPlugsCapability),
            PublishPlugVersion(PublishPlugVersion),
            RemovePlugVersion(RemovePlugVersion),
            RemovePlug(RemovePlug),
            ImportPlugDocument(ImportPlugDocument),
            GetPublisher(GetPublisher),
            ListPlugs(ListPlugs),
            GetPlug(GetPlug),
            GetPlugVersion(GetPlugVersion),
            WatchPlugs(WatchPlugs)
        }
        stream {}
    }
}
