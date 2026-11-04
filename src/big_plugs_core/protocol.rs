//! The plug registry protocol (ADR 028 §1): the wire contract between the node-side registry
//! service and whoever publishes to or reads from it.
//!
//! Every operation names its caller by an [`Authorization`]: the operator's own identity
//! capability, or a handle the registry minted through [`MintPlugsCapability`] that carries a
//! [`PlugsScope`]. An arbitrary actor presenting its identity is refused. The registry does no
//! enablement: it versions what is published and keeps a published version immutable, and each
//! runtime decides for itself what to enable from it.

use crate::Plug;
use crate::PlugId;
use crate::Revision;
use crate::Version;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::ContextError;
use service_core_rs::declare_service_protocol;
use service_core_rs::service_operation;
use std::collections::BTreeMap;

/// What a minted registry handle may do. Without `publish` it reads the registry (list, get,
/// watch); with it, it also publishes and removes. Publishing here is local and trusted under the
/// holder's capability; per-publisher signatures and namespace-scoped handles belong to the
/// publisher layer above this one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlugsScope {
    #[serde(default)]
    pub publish: bool,
}

impl PlugsScope {
    pub fn covers(&self, other: &Self) -> bool {
        self.publish || !other.publish
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintPlugsCapability {
    pub authorization: Authorization,
    pub scope: PlugsScope,
    pub rights: Rights,
}

/// Publish one version. Every artifact of the envelope must already be in the local blob store.
/// The same label with an identical envelope is an idempotent no-op; the same label with a
/// different one is refused with [`PlugsError::VersionExists`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishPlugVersion {
    pub authorization: Authorization,
    pub envelope: Plug,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPlugs {
    pub authorization: Authorization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPlug {
    pub authorization: Authorization,
    pub id: PlugId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPlugVersion {
    pub authorization: Authorization,
    pub id: PlugId,
    pub version: Version,
}

/// Drops one version's envelope and artifact references from the plug's document. The bytes are
/// released from retention unless another document still references them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovePlugVersion {
    pub authorization: Authorization,
    pub id: PlugId,
    pub version: Version,
}

/// Drops every version and the identity of a plug.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemovePlug {
    pub authorization: Authorization,
    pub id: PlugId,
}

/// A long poll over registry changes (ADR 023). `after` is the cursor of the last answer the
/// caller consumed, or `None` for the registry's current state. `limit` and `hold_ms` are
/// requests the service clamps; `0` asks for the default page and no hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchPlugs {
    pub authorization: Authorization,
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
    /// An artifact's bytes are not in the local blob store, so the registry would pin bytes it
    /// cannot serve.
    #[error("artifact `{artifact}` of plug `{id}` is not in the local blob store")]
    ArtifactUnavailable { id: PlugId, artifact: String },
    #[error("plug `{0}` is not in the registry")]
    UnknownPlug(PlugId),
    #[error("plug `{id}` has no version {version}")]
    UnknownVersion { id: PlugId, version: Box<Version> },
}

service_operation!(MintPlugsCapability => Capability, PlugsError, procedure("org.example.daybook.plugs.mintPlugsCapability"));
service_operation!(PublishPlugVersion => PublishedPlugVersion, PlugsError, procedure("org.example.daybook.plugs.publishPlugVersion"));
service_operation!(RemovePlugVersion => (), PlugsError, procedure("org.example.daybook.plugs.removePlugVersion"));
service_operation!(RemovePlug => (), PlugsError, procedure("org.example.daybook.plugs.removePlug"));
service_operation!(ListPlugs => Vec<PlugRecord>, PlugsError, query("org.example.daybook.plugs.listPlugs"));
service_operation!(GetPlug => PlugRecord, PlugsError, query("org.example.daybook.plugs.getPlug"));
service_operation!(GetPlugVersion => PlugVersionRecord, PlugsError, query("org.example.daybook.plugs.getPlugVersion"));
service_operation!(WatchPlugs => PlugChanges, PlugsError, procedure("org.example.daybook.plugs.watchPlugs"));

declare_service_protocol! {
    pub enum PlugsProtocol => PlugsMessage {
        unary {
            MintPlugsCapability(MintPlugsCapability),
            PublishPlugVersion(PublishPlugVersion),
            RemovePlugVersion(RemovePlugVersion),
            RemovePlug(RemovePlug),
            ListPlugs(ListPlugs),
            GetPlug(GetPlug),
            GetPlugVersion(GetPlugVersion),
            WatchPlugs(WatchPlugs)
        }
        stream {}
    }
}
