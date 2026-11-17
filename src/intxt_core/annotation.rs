//! Annotations (ADR 041): comments, thread state, structure and what a reader has seen. Shaped
//! after the W3C Web Annotation Data Model (targets with quote selectors, an open motivation),
//! with an explicit `replyTo` and Patchwork's thread-level resolved flag. Not intxt-specific: any
//! drawer can hold annotations on anything it can reference.
//!
//! MIGRATE BEFORE BETA, with the generic datums (see `generic`): intxt owns these under its own
//! authority only until a shared vocabulary exists (ledger D39).

use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumReference;
use big_cabinet_core::Location;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::reference::DatumPin;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

/// A comment (`org.example.intxt.comment/<uuidv7>`). Each comment is its own slot, so writers never
/// share one; authorship is what the cabinet attests, not a field.
#[macros::datum(authority = "org.example.intxt")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    pub targets: Vec<Target>,
    pub body: Content,
    pub motivation: Motivation,
    /// The parent comment, for replies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<DatumReference>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What a comment or structure is about. `source` is a [`DatumPin`] (ADR 035 §2.8): the datum,
/// optionally a position inside it in the fragment (an Automerge cursor range for text), and the
/// heads it was read at. A target with `to` is a span of datums from `source` to `to`, such as a
/// run of prompt turns; `to` is in the same document and is read at `source`'s heads, so a span has
/// one pin.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    /// The pinned heads are exactly as some op or observation recorded them, so a viewer can tell
    /// by equality which side of a diff the target is on (ADR 041 §2). No heads follows the
    /// target's current version.
    pub source: DatumPin,
    /// For a comment on code: the worktree-relative path. The `source` is then a `gitCommit` or a
    /// `worktreeOp` datum, and `lines` and `quote` count in this path's content at that commit or
    /// op (ADR 041 §2), which is git's blob, not a document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The hand-writable text position: 1-based lines at the pinned heads (or, with `path`, in
    /// the path's content).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lines: Option<LineRange>,
    /// Required on text targets: what re-attachment and every resolution check against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<TextQuote>,
    /// The last datum of a span.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<Location>,
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

/// A thread's shared state (`org.example.intxt.threadState/<root comment uuid>`). One slot written by
/// anyone; concurrent resolve and reopen leave one of the two (ADR 041 §3).
#[macros::datum(authority = "org.example.intxt")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadState {
    pub resolved: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Tasks, decisions, sections and other groupings (`org.example.intxt.structure/<uuidv7>`, D9).
#[macros::datum(authority = "org.example.intxt")]
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
    /// The enclosing structure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<DatumReference>,
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

/// What one reader has seen (`org.example.intxt.seen/<actor uuid>/<range>`). Only that reader writes
/// it. Split into ranges because one map hits the native read budget at about 5,400 entries.
#[macros::datum(authority = "org.example.intxt")]
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

/// The intxt's entry for its annotations (`org.example.intxt.annotationsEntry/main`): they are found by
/// tag across the drawer's documents, so the entry names no document.
#[macros::datum(authority = "org.example.intxt")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationsEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A comment as read from a notes document: its key, its value, the heads of the version read and
/// the actors the cabinet attests wrote that version (ADR 041 §2: authorship is not a field).
#[derive(Debug, Clone, PartialEq)]
pub struct Posted {
    pub key: DatumKey,
    pub comment: Comment,
    /// The version's heads, sorted, as hex: what a [`Seen`] records for the key.
    pub heads: Vec<String>,
    pub authors: Vec<Uuid7>,
}

/// A comment and the replies under it, however deep, oldest first. Comment slots are UUIDv7, so
/// key order is time order.
#[derive(Debug, Clone, PartialEq)]
pub struct Thread {
    /// The root comment's key, which names the thread's [`ThreadState`] slot. The comment itself
    /// is absent from `comments` when it was removed and replies to it remain.
    pub root: DatumKey,
    /// Never empty.
    pub comments: Vec<Posted>,
}

impl Thread {
    /// Groups `posted` into threads by following each `replyTo` to its root, ordered by their
    /// first comment. A `replyTo` chain that loops (a peer's data, not a writer of ours) ends
    /// where it closes.
    pub fn group(posted: Vec<Posted>) -> Vec<Thread> {
        let parents: BTreeMap<&DatumKey, &DatumKey> = posted
            .iter()
            .filter_map(|post| Some((&post.key, post.comment.reply_to.as_ref()?.key())))
            .collect();
        let root_of = |start: &DatumKey| -> DatumKey {
            let mut at = start;
            for _ in 0..=parents.len() {
                match parents.get(at) {
                    Some(parent) => at = parent,
                    None => return at.clone(),
                }
            }
            at.clone()
        };
        let roots: Vec<DatumKey> = posted.iter().map(|post| root_of(&post.key)).collect();
        let mut threads: BTreeMap<DatumKey, Vec<Posted>> = BTreeMap::new();
        for (post, root) in posted.into_iter().zip(roots) {
            threads.entry(root).or_default().push(post);
        }
        let mut threads: Vec<Thread> = threads
            .into_iter()
            .map(|(root, mut comments)| {
                comments.sort_by(|left, right| left.key.cmp(&right.key));
                Thread { root, comments }
            })
            .collect();
        threads.sort_by(|left, right| left.comments[0].key.cmp(&right.comments[0].key));
        threads
    }

    pub fn last(&self) -> &Posted {
        self.comments.last().expect("a thread has a comment")
    }

    /// Whether `reader` wrote nothing last: someone else is waiting for an answer (ADR 041 §5).
    /// A version no verified author wrote counts as someone else's.
    pub fn waiting_on(&self, reader: &Uuid7) -> bool {
        !self.last().authors.contains(reader)
    }

    /// Whether a comment of the thread that `reader` did not write has a version other than the
    /// one `seen` records for it (`Seen.datums`, ADR 041 §5).
    pub fn unseen_by(&self, reader: &Uuid7, seen: &BTreeMap<String, Vec<String>>) -> bool {
        self.comments.iter().any(|post| {
            !post.authors.contains(reader) && seen.get(&post.key.to_string()) != Some(&post.heads)
        })
    }
}
