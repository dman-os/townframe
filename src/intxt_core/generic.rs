//! Generic datums (ADR 036 §2, §5), meant for any app, not only intxt: a title, a path, a text
//! body, and the order of a document's parts. They come from daybook's vocabulary
//! (`titleGeneric`, `pathGeneric`, `note`, `body`), restated as Lexicon objects.
//!
//! MIGRATE BEFORE BETA. No shared design for generic datums exists yet (ledger D39), so intxt owns
//! these under its own authority (`org.example.intxt.*`), because a cartridge registers only NSIDs
//! under its authority and the node refuses a record another holder registered. Before the beta
//! they move to a shared vocabulary, informed by the atproto community's generic lexicons
//! (`docs/scratch/atproto-generic-lexicons.md`), and every stored datum's tag is migrated.

use big_cabinet_core::DatumReference;
use big_cabinet_core::DocId;
use big_cabinet_core::DocReference;
use big_cabinet_core::datum::Extra;
use big_cabinet_core::dmeta::MultibaseKey;
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
    /// The text that names an intxt outside the node, such as an entry of the PR description block
    /// ([`PrLinks`]): a reference to its descriptor document, `dtxt:doc:<drawer id>` (ADR 035
    /// §3). An intxt is named by its descriptor document, whose id is the drawer id and is the
    /// same on every node, so the text resolves on any node the drawer is shared with.
    ///
    /// `via` names the nodes that hold the intxt (this node, and the relays it syncs with), the
    /// reference's `via=` locator hint: a reader of the text learns where to look. It does not
    /// change the reference's identity.
    pub fn reference(drawer: &DocId, via: impl IntoIterator<Item = MultibaseKey>) -> DocReference {
        DocReference::new(drawer.clone()).with_via(via)
    }
}

/// The `$type` that marks the PR description block.
const PR_LINKS_TYPE: &str = "org.example.intxt.prLinks";

/// One intxt a PR description names (an entry of [`PrLinks::intxts`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrLink {
    /// The intxt's descriptor document ([`Intxt::reference`]), with its `via` hint and any pin
    /// kept. Equality ignores `via` (it is a hint); compare [`DocReference::via`] to see it.
    #[serde(rename = "ref", with = "doc_reference_text")]
    pub reference: DocReference,
    /// A name for readers who cannot open the intxt. The intxt itself is the truth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Git refs the intxt tracks, for the same readers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<String>,
    /// Fields a newer writer added, carried unchanged.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl PrLink {
    pub fn new(reference: DocReference) -> Self {
        Self {
            reference,
            title: None,
            refs: Vec::new(),
            extra: serde_json::Map::new(),
        }
    }
}

/// A `DocReference` as its `dtxt:doc:` text, which is how the block spells it.
mod doc_reference_text {
    use super::DocReference;
    use serde::Deserialize;

    pub fn serialize<S: serde::Serializer>(
        reference: &DocReference,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(reference)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DocReference, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse()
            .map_err(|error| serde::de::Error::custom(format!("`ref` {text:?}: {error}")))
    }
}

/// Why a PR description's intxt block could not be read.
#[derive(Debug, thiserror::Error)]
pub enum PrBlockError {
    /// The block is there but is not the shape: bad JSON, a missing `ref`, a `ref` that is not a
    /// `dtxt:doc:` URL.
    #[error("the intxt block in the PR description is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    /// A `<details>` holding the block never closes, so there is no span to read or replace.
    #[error("the `<details>` holding the intxt block in the PR description is never closed")]
    Unterminated,
}

/// The block a PR description carries to name its intxts (`docs/scratch/intxt-review-micro.md`,
/// "The PR description block"): a collapsed `<details>` holding a JSON code block whose `$type` is
/// `org.example.intxt.prLinks`.
///
/// ````markdown
/// <details>
/// <summary>intxt</summary>
///
/// ```json
/// { "$type": "org.example.intxt.prLinks", "intxts": [{ "ref": "dtxt:doc:z…?via=z…" }] }
/// ```
///
/// </details>
/// ````
///
/// A PR and its intxts are many to many, so the block lists every intxt the PR belongs to. Fields
/// this version does not know are carried on [`PrLinks::extra`] and each [`PrLink::extra`], so a
/// newer writer's block survives being read and written back.
///
/// # What the scanner reads
///
/// [`PrLinks::read`] is a line scanner, not a markdown parser. It takes the first fenced block
/// opened by a line of at least three backticks followed by `json`, that sits inside a `<details>`
/// element (a line that starts `<details` opens one and a line holding `</details>` closes one;
/// they nest) and whose JSON object has `"$type": "org.example.intxt.prLinks"`. It does not read
/// indented code, tilde fences, a `<details>` that is not at the start of a line, or a block
/// outside any `<details>`, and a `<details>` inside another fenced block is ignored. Both LF and
/// CRLF lines are read. A json block that is not valid JSON is skipped unless its text mentions
/// the `$type`, in which case it is the intxt block and malformed.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PrLinks {
    pub intxts: Vec<PrLink>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Where the intxt block sits in a description and what it holds.
struct FoundBlock {
    links: PrLinks,
    /// The byte range of the enclosing `<details>` element, from its opening line to the end of
    /// its closing line, line break excluded.
    span: std::ops::Range<usize>,
}

impl PrLinks {
    /// The intxts a PR `description` names. A description with no intxt block names none; a block
    /// that is there but malformed is an error for the caller to report, never an empty list.
    pub fn read(description: &str) -> Result<Self, PrBlockError> {
        Ok(Self::find(description)?
            .map(|found| found.links)
            .unwrap_or_default())
    }

    /// The block to paste at the end of a PR description.
    pub fn block(&self) -> String {
        let mut object = serde_json::Map::new();
        object.insert("$type".to_owned(), PR_LINKS_TYPE.into());
        object.insert(
            "intxts".to_owned(),
            serde_json::to_value(&self.intxts).expect("a link is plain JSON"),
        );
        object.extend(self.extra.clone());
        let json = serde_json::to_string_pretty(&object).expect("a block is plain JSON");
        format!("<details>\n<summary>intxt</summary>\n\n```json\n{json}\n```\n\n</details>")
    }

    /// `description` with its intxt block replaced by this one, or with the block appended when
    /// it has none. The rest of the text is untouched, and the line ending of the block follows
    /// the text it joins (CRLF if the description uses it), so writing the same links again
    /// returns the same text.
    pub fn write_into(&self, description: &str) -> Result<String, PrBlockError> {
        let crlf = description.contains("\r\n");
        let block = if crlf {
            self.block().replace('\n', "\r\n")
        } else {
            self.block()
        };
        let Some(found) = Self::find(description)? else {
            let newline = if crlf { "\r\n" } else { "\n" };
            // The text stays as written; the block is set off by a blank line.
            let (kept, separator) = if description.trim().is_empty() {
                ("", String::new())
            } else {
                let trailing = description
                    .chars()
                    .rev()
                    .take_while(|c| *c == '\n' || *c == '\r')
                    .filter(|c| *c == '\n')
                    .count();
                (description, newline.repeat(2usize.saturating_sub(trailing)))
            };
            return Ok(format!("{kept}{separator}{block}"));
        };
        Ok(format!(
            "{}{block}{}",
            &description[..found.span.start],
            &description[found.span.end..]
        ))
    }

    fn find(description: &str) -> Result<Option<FoundBlock>, PrBlockError> {
        // Line start offsets with each line's text, line break excluded.
        let mut lines = Vec::new();
        let mut offset = 0;
        for raw in description.split_inclusive('\n') {
            let text = raw.strip_suffix('\n').unwrap_or(raw);
            let text = text.strip_suffix('\r').unwrap_or(text);
            lines.push((offset, text));
            offset += raw.len();
        }

        // Start offsets of the open `<details>` elements, innermost last.
        let mut open: Vec<usize> = Vec::new();
        // The block found so far, how many `<details>` were open around it, and where the
        // innermost one starts.
        let mut found: Option<(PrLinks, usize, usize)> = None;
        let mut index = 0;
        while index < lines.len() {
            let (start, line) = lines[index];
            index += 1;
            if let Some(fence) = line.trim_start().strip_prefix("```") {
                let ticks = 3 + fence.chars().take_while(|c| *c == '`').count();
                let info = fence.trim_start_matches('`').trim();
                // The fence ends at a line of only backticks, at least as many as opened it.
                let closes = |text: &str| {
                    let text = text.trim();
                    text.len() >= ticks && text.bytes().all(|b| b == b'`')
                };
                let body_start = lines.get(index).map_or(description.len(), |(at, _)| *at);
                let close = lines[index.min(lines.len())..]
                    .iter()
                    .position(|(_, text)| closes(text))
                    .map(|at| at + index);
                let body_end = close.map_or(description.len(), |at| lines[at].0);
                index = close.map_or(lines.len(), |at| at + 1);
                if found.is_none()
                    && !open.is_empty()
                    && info == "json"
                    && close.is_some()
                    && let Some(links) = Self::parse_block(&description[body_start..body_end])?
                {
                    found = Some((links, open.len(), *open.last().expect("inside a details")));
                }
                continue;
            }
            let trimmed = line.trim_start();
            let opens = trimmed
                .match_indices("<details")
                .filter(|(at, _)| {
                    *at == 0
                        && trimmed[at + 8..].starts_with(|c: char| c == '>' || c.is_whitespace())
                })
                .count();
            for _ in 0..opens {
                open.push(start);
            }
            for (at, tag) in line.match_indices("</details>") {
                // The closing tag of the element that holds the block ends the span.
                if found
                    .as_ref()
                    .is_some_and(|(_, depth, _)| *depth == open.len())
                {
                    let (links, _, opened) = found.take().expect("just checked");
                    return Ok(Some(FoundBlock {
                        links,
                        span: opened..start + at + tag.len(),
                    }));
                }
                open.pop();
            }
        }
        match found {
            Some(_) => Err(PrBlockError::Unterminated),
            None => Ok(None),
        }
    }

    /// The block's links, `None` when this JSON is another element's. Text that is not JSON is
    /// ours only if it names the `$type`.
    fn parse_block(text: &str) -> Result<Option<Self>, PrBlockError> {
        let value: serde_json::Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(error) if text.contains(PR_LINKS_TYPE) => return Err(error.into()),
            Err(_) => return Ok(None),
        };
        if value.get("$type").and_then(serde_json::Value::as_str) != Some(PR_LINKS_TYPE) {
            return Ok(None);
        }
        let mut object = match value {
            serde_json::Value::Object(object) => object,
            _ => unreachable!("only an object has a `$type`"),
        };
        object.remove("$type");
        Ok(Some(serde_json::from_value(serde_json::Value::Object(
            object,
        ))?))
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
