//! Generic datums (ADR 036 §2, §5), meant for any app, not only intxt: a title, a path, a text
//! body, and the order of a document's parts. They come from daybook's vocabulary
//! (`titleGeneric`, `pathGeneric`, `note`, `body`), restated as Lexicon objects.
//!
//! MIGRATE BEFORE BETA. No shared design for generic datums exists yet (ledger D39), so intxt owns
//! these under its own authority (`org.example.intxt.*`), because a cartridge registers only NSIDs
//! under its authority and the node refuses a record another holder registered. Before the beta
//! they move to a shared vocabulary, informed by the atproto community's generic lexicons
//! (`docs/scratch/atproto-generic-lexicons.md`), and every stored datum's tag is migrated.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::Location;
use big_cabinet_core::ReferenceDocId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Extra;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Marks a drawer's descriptor document as an intxt (`org.example.intxt/main`). It says nothing
/// else yet; intxt-wide fields land beside `empty`.
#[macros::datum(authority = "org.example", name = "intxt")]
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

impl Intxt {
    /// The text that names an intxt outside the node, such as a line in a GitHub PR description:
    /// a reference to this marker in the intxt's descriptor document,
    /// `dtxt:datum:<drawer id>/org.example.intxt/main`. A drawer is named by its descriptor
    /// document's id, which is the same on every node, so the text resolves on any node the drawer
    /// is shared with.
    pub fn reference(drawer: &DocId) -> DatumReference {
        DatumReference::new(
            ReferenceDocId::Concrete(drawer.clone()),
            DatumKey::new(Self::ID, "main").expect("the marker's key is a valid datum key"),
        )
    }

    /// The intxts `text` names with [`Self::reference`], in order of first mention and each once.
    ///
    /// A PR and its intxts are many to many: an intxt holds any number of PRs' commits, and a PR
    /// description names every intxt it belongs to, one reference each. A reference ends at
    /// whitespace or at the markdown punctuation that can close it (`)`, `]`, `>`, a backtick, a
    /// quote), and sentence punctuation right after it is not part of it. A pin or a fragment on
    /// the reference still names the intxt. Text that is not a reference, or a reference to any
    /// other datum, is ignored.
    pub fn named_in(text: &str) -> Vec<DocId> {
        let mut named = Vec::new();
        for (start, _) in text.match_indices("dtxt:datum:") {
            let rest = &text[start..];
            let end = rest
                .find(|c: char| {
                    c.is_whitespace() || matches!(c, ')' | ']' | '>' | '`' | '"' | '\'')
                })
                .unwrap_or(rest.len());
            let candidate = rest[..end].trim_end_matches(['.', ',', ';', ':']);
            let Ok(location) = Location::parse(candidate) else {
                continue;
            };
            let reference = location.into_reference();
            let ReferenceDocId::Concrete(drawer) = reference.document() else {
                continue;
            };
            if reference.key().tag().as_str() == Self::ID
                && reference.key().slot_bytes() == b"main"
                && !named.contains(drawer)
            {
                named.push(drawer.clone());
            }
        }
        named
    }
}

/// A document's title (`org.example.intxt.title/main`). One slot, last writer wins.
#[macros::datum(authority = "org.example.intxt")]
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

/// The path a document's content lives at (`org.example.intxt.path/main`): a file document's path in
/// its worktree, `/`-separated and relative to the worktree root.
#[macros::datum(authority = "org.example.intxt", name = "path")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PathName {
    pub path: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A text body and its media type (`org.example.intxt.note/main`). A document holding a `path` and a
/// `note` is a file; one holding a `title` and a `note` is a note.
#[macros::datum(authority = "org.example.intxt")]
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

/// The order in which readers show a document's parts (`org.example.intxt.body/main`). Each entry is a
/// `dtxt:datum:` reference, usually `self`-relative. An Automerge list, so concurrent appends by
/// two writers both land; readers show a duplicated reference once, and parts missing from it
/// after the listed ones (ADR 036 §2).
#[macros::datum(authority = "org.example.intxt")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    #[schemars(extend("x-automerge" = "list"))]
    pub order: Vec<DatumReference>,
    #[serde(flatten)]
    pub extra: Extra,
}
