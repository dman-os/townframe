use big_cabinet_core::datum::Extra;
use jiff::Timestamp;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Whether a pull request is open, closed unmerged, or merged.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PullState {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "closed")]
    Closed,
    #[serde(rename = "merged")]
    Merged,
    #[datum(other)]
    Unknown(String),
}

/// A GitHub pull request as `day x intxt github pull` imported it
/// (`githubPull/<owner>/<repo>/<number>`), in the repository's GitHub document. A snapshot: the
/// importer overwrites it when `updatedAt` moves.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GithubPull {
    pub title: String,
    /// Markdown, as written; empty when the PR has no description.
    pub body: String,
    pub state: PullState,
    /// The head branch's name, and the base branch's.
    pub head: String,
    pub base: String,
    pub head_sha: String,
    pub base_sha: String,
    /// GitHub's `updated_at`.
    pub updated_at: Timestamp,
    /// The PR's page.
    pub url: String,
    #[serde(flatten)]
    pub extra: Extra,
}
