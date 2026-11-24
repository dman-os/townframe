//! The summary call: what a peer decides each part's strategy on.

use crate::interlude::*;

use crate::part_store::PartSummary;
use crate::rev::Rev;

/// Summaries of `parts`, each as the asker sees it from the cursor it holds for the part. At
/// most [`MAX_SUMMARY_PARTS`](super::MAX_SUMMARY_PARTS) parts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartSummariesRequest {
    pub parts: Map<PartKey, Rev>,
}

/// The summary of each asked part the asker may read. A part the asker may not read and a part
/// the store does not know are both left out, so the answer never confirms that a part exists.
pub type PartSummariesResponse = Map<PartKey, PartSummary>;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum PartSummariesError {
    /// the request names more parts than the responder answers at once; no part was answered
    TooManyParts { cap: usize },
    /// the request context failed: {0}
    Context(#[from] method_core::ContextError),
}
