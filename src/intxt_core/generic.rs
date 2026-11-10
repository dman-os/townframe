//! Generic datums (ADR 036 §2, §5): not intxt's own, so any micro that understands them works on
//! intxt content. They come from daybook's vocabulary (`titleGeneric`, `pathGeneric`, `note`,
//! `body`), restated as Lexicon objects. Which crate owns them is ADR 036 §8 Q1; they live here
//! until that is settled.

use big_cabinet_core::datum::Extra;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Marks a drawer's descriptor document as an intxt (`org.example.intxt/main`). It says nothing
/// else yet; intxt-wide fields land beside `empty`.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Intxt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl Default for Intxt {
    fn default() -> Self {
        Self {
            empty: Some(true),
            extra: Extra::default(),
        }
    }
}

/// A document's title (`org.example.title/main`). One slot, last writer wins.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Title {
    pub title: String,
    /// Markdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The path a document's content lives at (`org.example.path/main`): a file document's path in
/// its worktree, `/`-separated and relative to the worktree root.
#[macros::datum(authority = "org.example", name = "path")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PathName {
    pub path: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A text body and its media type (`org.example.note/main`). A document holding a `path` and a
/// `note` is a file; one holding a `title` and a `note` is a note.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Note {
    /// `text/markdown`, `text/x-rust`, …
    pub mime: String,
    /// Collaborative text, so line-level splices are the stored edits (ADR 040 §4).
    #[schemars(extend("x-automerge" = "text"))]
    pub content: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The order in which readers show a document's parts (`org.example.body/main`). Each entry is a
/// `dtxt:datum:` reference, usually `self`-relative. An Automerge list, so concurrent appends by
/// two writers both land; readers show a duplicated reference once, and parts missing from it
/// after the listed ones (ADR 036 §2).
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    pub order: Vec<String>,
    #[serde(flatten)]
    pub extra: Extra,
}
