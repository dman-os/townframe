use big_cabinet_core::DatumReference;
use big_cabinet_core::datum::Extra;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// An intxt includes a repository (`repoEntry/<repo uuid>`, on the intxt's descriptor).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepoEntry {
    pub repo: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt's commits document (`commitsDoc/main`, on the intxt's descriptor): the content
/// document, in the intxt's drawer, that holds `gitCommit/<sha>` for every commit the intxt
/// references and the `rewrite`/`rewrittenFrom` links between them. It is not an entry: it is
/// reached from the descriptor, not listed in the body. Related intxts name the same document
/// (`day x intxt new --with`), which is then in each of their drawers. A document id is text, as
/// in [`OpLogDoc`](super::OpLogDoc), because a datum reference names a datum and this names a whole document.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommitsDoc {
    pub doc: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt includes a worktree's capture (`worktreeEntry/<worktree uuid>`).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeEntry {
    pub worktree: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt includes a GitHub pull request (`githubPullEntry/<owner>/<repo>/<number>`): the
/// reference is the `githubPull` datum in the pull request's own document, which the intxt's
/// drawer shares so everyone the intxt is shared with reads it.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GithubPullEntry {
    pub pull: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt includes an agent session (`sessionEntry/<session id>`).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionEntry {
    pub session: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt names commits (`gitCommitEntry/<tip sha>`): everything reachable from `tip` and not
/// from `base`, resolved when it was named and never re-evaluated (a range read later would
/// answer differently). Adding the same tip again is the same entry.
///
/// The objects the commits need travel in `pack` (ADR 040 §4): a thin pack against `base` when
/// there is one, a full pack otherwise. `patch` is a derived reading aid and the pack stays the
/// truth.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitCommitEntry {
    /// The commit key `org.example.intxt.work.gitCommit/<sha>` of the newest commit named.
    /// A plain string: intxt resolves the key itself and verifies it against the
    /// sha, rather than the cabinet resolving it as a reference.
    pub tip: String,
    /// The commit key of the commit the recipient is assumed to hold: the merge base of the
    /// range. None when the range reaches the root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// The pack's `org.example.daybook.blob` datum in the intxt's commits document (the one its
    /// descriptor's `commitsDoc/main` names), not in the descriptor, which stays a small index.
    pub pack: DatumReference,
    /// The `org.example.daybook.blob` datum (`text/x-diff`) in the intxt's commits document
    /// holding `git diff -M <base>..<tip>` (the empty tree when there is no base), so a
    /// reader can show the diff without parsing a git pack. Derived from the pack's commits and
    /// absent when the diff was too large to carry (ADR 040 §4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch: Option<DatumReference>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt names a jj change (`jjChangeEntry/<change id>`) and follows it through its rewrites:
/// every commit the change has had is in the intxt, the latest is its current form. The pack
/// holds every commit of the change's evolution.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JjChangeEntry {
    /// The pack's `org.example.daybook.blob` datum in the intxt's commits document, as
    /// [`GitCommitEntry::pack`].
    pub pack: DatumReference,
    /// As [`GitCommitEntry::patch`], the diff of the change's current commit against its parent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch: Option<DatumReference>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt tracks a ref as one clone saw it (`gitRefEntry/<clone uuid>/<ref name>`, keyed like
/// the `gitRef` it follows). Tracking the same name on a second machine is a second entry:
/// clones are never merged (ADR 040 §8).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitRefEntry {
    /// The clone's `gitRef` datum.
    #[serde(rename = "ref")]
    pub reference: DatumReference,
    /// The commit key of what a review of the ref compares against; as `GitCommitEntry::base`,
    /// a plain string that intxt resolves itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}
