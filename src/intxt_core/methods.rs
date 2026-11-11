//! The methods intxt's nodule serves, as their request, output and error types: `createIntxt`,
//! `addToIntxt` and `recordObservations`. The handlers live in `intxt_nodule`; callers (the host's
//! CLI, the cartridge, tests) need only these types.

use big_cabinet_core::DocId;
use big_cabinet_core::ActorUrl;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::reference::DatumReference;
use crate::capture::Batch;
use crate::capture::RecordedIds;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;

/// Creates an intxt (ADR 036 §2): a drawer, marked as an intxt, with a title and an empty body.
/// The drawer is owned by the caller, whose authority the handler presents.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIntxt {
    pub title: String,
    /// Markdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The new intxt: its drawer, whose id is its descriptor document's id.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntxtCreated {
    pub drawer_id: DrawerId,
}

/// Includes a part (a worktree, a git repository or an agent session datum) in an intxt: writes
/// the entry for it and appends the entry to the intxt's body, then enters the part's document in
/// the intxt's drawer at Read so everyone the intxt is shared with can read it.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddToIntxt {
    pub intxt: DrawerId,
    /// The part, named by its document id (not `self`).
    pub part: DatumReference,
}

/// The entry the part has in the intxt. Adding the same part again answers the same entry.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddedToIntxt {
    pub entry: DatumReference,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum IntxtError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    /// A call the handler made was refused or failed; `reason` is what the callee answered.
    #[error("{operation} failed: {reason}")]
    Upstream { operation: String, reason: String },
    /// The request named something an intxt cannot include or a document that is not what it
    /// was said to be.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The caller reaches the part through no drawer at Read or above, so there is nothing to
    /// lend the intxt's drawer.
    #[error("the part's document {0} is reached through no drawer at Read or above")]
    NotReachable(DocId),
}

method_core::methods! {
    error = IntxtError, nsid_prefix = "org.example.intxt.";
    CreateIntxt => IntxtCreated, procedure, "createIntxt";
    AddToIntxt => AddedToIntxt, procedure, "addToIntxt";
    RecordObservations => RecordedIds, procedure, "recordObservations";
}

/// Records what a capture hook observed in `drawer`, the node's capture drawer.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordObservations {
    /// Where the documents are created.
    pub drawer: DrawerId,
    /// The node the clones are on (`gitClone.machine`): the host knows it, the recorder does not.
    pub machine: ActorUrl,
    pub batch: Batch,
}
