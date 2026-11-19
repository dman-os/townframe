//! The methods intxt's nodule serves, as their request, output and error types: `createIntxt`,
//! `addToIntxt` and `summarizeIntxts`. The handlers live in `intxt_nodule`; callers (the host's CLI, the cartridge,
//! tests) need only these types. Recording what capture observed is not a method: it is the
//! client library's recorder (`intxt_core::client`).

use big_cabinet_core::DocId;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::reference::DatumReference;
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

/// The most drawers one [`SummarizeIntxts`] names.
pub const MAX_SUMMARIZED: usize = 32;

/// Summarizes the intxts in `drawers`, for callers with no egress beyond dispatch (a micro on a
/// phone looking up the intxts a PR names). One result per drawer, in order; a drawer that cannot
/// be summarized does not fail the call.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SummarizeIntxts {
    /// At most [`MAX_SUMMARIZED`], each an intxt's drawer (its descriptor document's id).
    pub drawers: Vec<DrawerId>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntxtsSummarized {
    pub results: Vec<IntxtSummary>,
}

/// What became of one drawer of a [`SummarizeIntxts`].
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntxtSummary {
    pub drawer: DrawerId,
    pub status: SummaryStatus,
    /// Present when `found`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Present when `found` and the intxt has one. Markdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// How many entries the intxt's body lists. Present when `found`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<u32>,
    /// The newest logical clock among the descriptor's datums, which rises with every write to
    /// them: a change token, not a time. Present when `found`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated: Option<u64>,
    /// [`crate::generic::Intxt::reference`] text, the `ref` of an entry in the PR description block. Present when
    /// `found`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    /// Why the drawer is `unreadable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SummaryStatus {
    /// The drawer is an intxt the caller can read.
    Found,
    /// The caller reads the drawer's descriptor and it holds no intxt marker.
    NotFound,
    /// A read failed (the caller may not read it, or the node does not hold it) or the intxt's
    /// descriptor is malformed; `reason` says which.
    Unreadable,
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
    SummarizeIntxts => IntxtsSummarized, procedure, "summarizeIntxts";
}
