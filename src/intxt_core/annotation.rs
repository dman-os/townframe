//! Annotations (ADR 041): comments, thread state, structure and what a reader has seen. Shaped
//! after the W3C Web Annotation Data Model (targets with quote selectors, an open motivation),
//! with an explicit `replyTo` and Patchwork's thread-level resolved flag. Not intxt-specific: any
//! drawer can hold annotations on anything it can reference.

use big_cabinet_core::datum::Extra;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

/// A comment (`org.example.comment/<uuidv7>`). Each comment is its own slot, so writers never
/// share one; authorship is what the cabinet attests, not a field.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    pub targets: Vec<Target>,
    pub body: Content,
    pub motivation: Motivation,
    /// The parent comment, for replies: a `dtxt:datum:` reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What a comment or structure is about. `ref` is a `dtxt:` location: the datum, and optionally
/// a position inside it in the fragment (an Automerge cursor range for text). A target with
/// `to` is a span of datums from `ref` to `to`, such as a run of prompt turns.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    #[serde(rename = "ref")]
    pub reference: String,
    /// The heads `ref` is pinned at, exactly as some op or observation recorded them, so a viewer
    /// can tell by equality which side of a diff the target is on (ADR 041 §2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_heads: Option<Vec<String>>,
    /// The hand-writable text position: 1-based lines at the pinned heads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lines: Option<LineRange>,
    /// Required on text targets: what re-attachment and every resolution check against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<TextQuote>,
    /// The last datum of a span.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Lines `start..=end`, 1-based, at a target's pin.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LineRange {
    pub start: u32,
    pub end: u32,
    #[serde(flatten)]
    pub extra: Extra,
}

/// W3C's `TextQuoteSelector`: the targeted text, with some context on either side to tell
/// repeated text apart.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TextQuote {
    pub exact: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A text body: the shape of the generic `note` datum, held inside another datum.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    pub mime: String,
    pub content: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Why a comment was written: W3C Web Annotation's motivations, open so a vocabulary can add its
/// own.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Motivation {
    #[serde(rename = "assessing")]
    Assessing,
    #[serde(rename = "bookmarking")]
    Bookmarking,
    #[serde(rename = "classifying")]
    Classifying,
    #[serde(rename = "commenting")]
    Commenting,
    #[serde(rename = "describing")]
    Describing,
    #[serde(rename = "editing")]
    Editing,
    #[serde(rename = "highlighting")]
    Highlighting,
    #[serde(rename = "identifying")]
    Identifying,
    #[serde(rename = "linking")]
    Linking,
    #[serde(rename = "moderating")]
    Moderating,
    #[serde(rename = "questioning")]
    Questioning,
    #[serde(rename = "replying")]
    Replying,
    #[serde(rename = "tagging")]
    Tagging,
    #[datum(other)]
    Unknown(String),
}

/// A thread's shared state (`org.example.threadState/<root comment uuid>`). One slot written by
/// anyone; concurrent resolve and reopen leave one of the two (ADR 041 §3).
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadState {
    pub resolved: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Tasks, decisions, sections and other groupings (`org.example.structure/<uuidv7>`, D9).
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Structure {
    pub kind: StructureKind,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Content>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Progress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<Target>,
    /// The enclosing structure: a `dtxt:datum:` reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Position among siblings as a fractional index: siblings sort by this string, so a writer
    /// inserts between `"1"` and `"2"` by writing `"15"`, with no renumbering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StructureKind {
    #[serde(rename = "task")]
    Task,
    #[serde(rename = "decision")]
    Decision,
    #[serde(rename = "section")]
    Section,
    #[datum(other)]
    Unknown(String),
}

/// Progress of a task or plan entry, spelled as ACP spells plan and tool-call status.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Progress {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "in_progress")]
    InProgress,
    #[serde(rename = "completed")]
    Completed,
    #[datum(other)]
    Unknown(String),
}

/// What one reader has seen (`org.example.seen/<actor uuid>/<range>`). Only that reader writes
/// it. Split into ranges because one map hits the native read budget at about 5,400 entries.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Seen {
    /// Datum key → the heads of the version last read.
    pub datums: BTreeMap<String, Vec<String>>,
    /// Vocabulary-defined review positions, such as a ref's target last reviewed.
    pub reviewed: BTreeMap<String, String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The intxt's entry for its annotations (`org.example.annotationsEntry/main`): they are found by
/// tag across the drawer's documents, so the entry names no document.
#[macros::datum(authority = "org.example")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationsEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}
