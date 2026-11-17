use big_cabinet_core::datum::Extra;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// How a rewrite was recognised.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RewriteHow {
    /// The commits have the same `git patch-id --stable`, which is what `git range-diff` uses.
    #[serde(rename = "patchId")]
    PatchId,
    /// git's `post-rewrite` hook reported the exact old and new commit on an amend or a rebase,
    /// on the machine that did the rewrite.
    #[serde(rename = "hook")]
    Hook,
    #[datum(other)]
    Unknown(String),
}

/// A commit the intxt holds was rewritten (`rewrite/<old sha>`), written in the commits document
/// that holds the old commit. [`RewrittenFrom`] is its reverse, under the successor's slot, so
/// that following a rewrite in either direction is a slot query.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rewrite {
    pub successor: String,
    pub how: RewriteHow,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The reverse of [`Rewrite`] (`rewrittenFrom/<new sha>`): the commit it replaced.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RewrittenFrom {
    pub predecessor: String,
    pub how: RewriteHow,
    #[serde(flatten)]
    pub extra: Extra,
}
