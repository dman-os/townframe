//! What the review micro shows, as plain data, and how it is read from the node: the PR list, a PR
//! and the intxts its description names, an intxt and its entries. The view renders these and
//! nothing else, so every rule of "what is a row" is tested here against [`crate::fixture`].
//!
//! Errors split by who owns them. A failed [`CallError`] (the network edge) is reported by the
//! view. A document or datum the node answers but that the micro cannot show (not held, pending,
//! rejected) is a [`Read::Unreadable`] row, because it is data. A datum the node admitted that does
//! not decode as its own vocabulary is an invariant and panics.
use crate::node::CallError;
use crate::node::NodeCalls;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::ReadBlob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use big_cabinet_core::reference::DatumReference;
use intxt_core::generic;
use intxt_core::work;
use intxt_core::work::PullState;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// A datum key whose parts are fixed in code.
pub(crate) fn key(tag: &str, slot: &str) -> DatumKey {
    DatumKey::new(tag, slot).expect("a fixed key is well formed")
}

/// A thing the node answered for but may not be showable.
#[derive(Debug, Clone, PartialEq)]
pub enum Read<T> {
    Ready(T),
    /// Why not, in words: the node does not hold it, or the datum is pending, rejected or
    /// undecodable.
    Unreadable(String),
}

/// A pull request the node holds (`githubPull`).
#[derive(Debug, Clone, PartialEq)]
pub struct Pull {
    /// The document the pull request lives in.
    pub doc: DocId,
    /// `<owner>/<repo>/<number>`, the datum's slot.
    pub slot: String,
    pub title: String,
    pub state: PullState,
    /// The description, as written: markdown, with the intxt block still in it.
    pub body: String,
    pub url: String,
}

impl Pull {
    /// `owner/repo#number`.
    pub fn repo_ref(&self) -> String {
        match self.slot.rsplit_once('/') {
            Some((repo, number)) => format!("{repo}#{number}"),
            None => self.slot.clone(),
        }
    }

    /// The state as the chip says it.
    pub fn state_label(&self) -> &str {
        state_label(&self.state)
    }

    /// The description as a reader sees it: the intxt block (machine data) taken out, the rest in
    /// paragraphs, and how many intxts the block named. A block that cannot be read cannot be
    /// taken out, so the text is shown as written and names none; the links read of the screen
    /// reports the problem.
    pub fn reading(&self) -> PullReading {
        let (text, named) = match generic::PrLinks::split(&self.body) {
            Ok((text, links)) => (text, links.intxts.len()),
            Err(_) => (self.body.clone(), 0),
        };
        PullReading {
            paragraphs: paragraphs(&text),
            names: named,
        }
    }
}

/// A pull request's state as the chip says it.
pub fn state_label(state: &PullState) -> &str {
    match state {
        PullState::Open => "Open",
        PullState::Closed => "Closed",
        PullState::Merged => "Merged",
        PullState::Unknown(other) => other,
    }
}

/// A pull request's description for reading: see [`Pull::reading`].
#[derive(Debug, Clone, PartialEq)]
pub struct PullReading {
    /// The text without the intxt block, split at blank lines; a paragraph keeps its own line
    /// breaks.
    pub paragraphs: Vec<String>,
    /// How many intxts the block names.
    pub names: usize,
}

impl PullReading {
    /// The small line that stands for the hidden block, when it names anything.
    pub fn names_line(&self) -> Option<String> {
        match self.names {
            0 => None,
            1 => Some("names 1 intxt".to_owned()),
            many => Some(format!("names {many} intxts")),
        }
    }
}

/// `text` split at blank lines (LF or CRLF), each paragraph trimmed, none empty.
pub fn paragraphs(text: &str) -> Vec<String> {
    let mut paragraphs = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                paragraphs.push(current.join("\n"));
                current.clear();
            }
        } else {
            current.push(line.trim_end());
        }
    }
    if !current.is_empty() {
        paragraphs.push(current.join("\n"));
    }
    paragraphs
}

/// The longest a description is shown in a row, in characters: about three lines of a phone.
const TEASER_CHARS: usize = 140;

/// A markdown `description` for a row: its words on one line, cut at the last word boundary that
/// fits and ended with an ellipsis when it was cut.
pub fn teaser(description: &str) -> String {
    let flat = description.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= TEASER_CHARS {
        return flat;
    }
    let cut = flat
        .char_indices()
        .nth(TEASER_CHARS)
        .map_or(flat.len(), |(at, _)| at);
    // The word the cut falls in is dropped; a single word longer than the line is cut as is.
    let end = flat[..cut].rfind(' ').filter(|at| *at > 0).unwrap_or(cut);
    format!(
        "{}…",
        flat[..end].trim_end_matches([',', ';', ':', '.', '-', ' '])
    )
}

/// "1 entry" or "N entries".
pub fn entries_label(count: u32) -> String {
    match count {
        1 => "1 entry".to_owned(),
        many => format!("{many} entries"),
    }
}

/// The intxts a PR description names.
#[derive(Debug, Clone, PartialEq)]
pub enum Links {
    /// The block is there but is not the shape; never an empty list.
    Malformed(String),
    /// One row per named intxt, in the block's order. Empty when the description has no block.
    Named(Vec<LinkRow>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinkRow {
    /// The intxt's descriptor document.
    pub drawer: DocId,
    /// The name the block gave it, for readers who cannot open it. The intxt is the truth.
    pub hinted_title: Option<String>,
    pub outcome: LinkOutcome,
}

/// What the descriptor read said of one drawer.
#[derive(Debug, Clone, PartialEq)]
pub enum LinkOutcome {
    Found {
        title: String,
        /// Markdown.
        description: Option<String>,
        entries: u32,
    },
    /// The node reads the document and it is no intxt.
    NotFound,
    /// The node could not read it (it does not hold it, or the descriptor is malformed).
    Unreadable(String),
}

/// An intxt: its guide and what it includes.
#[derive(Debug, Clone, PartialEq)]
pub struct Intxt {
    pub title: String,
    /// Markdown.
    pub description: Option<String>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub kind: EntryKind,
    /// What names it: the commit range, the pull request, the ref.
    pub label: String,
    /// The entry's changes, read only when the view shows them ([`Review::patch`]).
    pub changes: Changes,
}

/// What an entry says of its changes, which are read lazily because a patch is a blob the node
/// serves separately from the entry.
#[derive(Debug, Clone, PartialEq)]
pub enum Changes {
    /// The entry's kind has no changes to show.
    Absent,
    /// The entry carries no patch: the diff was too large to carry (ADR 040 §4).
    TooLarge,
    /// The patch is the bytes of this blob datum.
    Blob(PatchSource),
}

/// The blob datum a patch reference names, with `self` already resolved against the document the
/// entry is in: the intxt's descriptor document today, its commits document once the blob moves
/// there. A concrete reference names that document, so both read the same way.
#[derive(Debug, Clone, PartialEq)]
pub struct PatchSource {
    pub doc: DocId,
    pub key: DatumKey,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EntryKind {
    Commits,
    JjChange,
    TrackedRef,
    Session,
    PullRequest,
    /// A vocabulary this version does not render, by its tag.
    Other(String),
    /// The entry's datum could not be read; why.
    Unreadable(String),
}

impl EntryKind {
    /// The heading of the group the kind is listed under.
    fn group(&self) -> &'static str {
        match self {
            Self::Commits => "Commits",
            Self::JjChange => "Jj changes",
            Self::TrackedRef => "Refs",
            Self::Session => "Sessions",
            Self::PullRequest => "Pull requests",
            Self::Other(_) => "Other",
            Self::Unreadable(_) => "Unreadable",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Commits => "commits",
            Self::JjChange => "jj change",
            Self::TrackedRef => "tracked ref",
            Self::Session => "session",
            Self::PullRequest => "pull request",
            Self::Other(tag) => tag,
            Self::Unreadable(_) => "unreadable",
        }
    }
}

/// The entries of one kind, under the heading the intxt screen gives them.
#[derive(Debug, Clone, PartialEq)]
pub struct EntryGroup<'intxt> {
    pub heading: &'static str,
    pub entries: Vec<&'intxt Entry>,
}

impl Intxt {
    /// The entries grouped by kind, a group where its first entry sits and entries in the body's
    /// order within it, so the screen reads in the order the intxt was written.
    pub fn groups(&self) -> Vec<EntryGroup<'_>> {
        let mut groups: Vec<EntryGroup<'_>> = Vec::new();
        for entry in &self.entries {
            let heading = entry.kind.group();
            match groups.iter_mut().find(|group| group.heading == heading) {
                Some(group) => group.entries.push(entry),
                None => groups.push(EntryGroup {
                    heading,
                    entries: vec![entry],
                }),
            }
        }
        groups
    }
}

/// Where the micro is: each screen is one node read.
#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Pulls,
    Pull(Pull),
    Intxt {
        drawer: DocId,
        /// The title the link gave it, shown while the intxt itself loads.
        title: String,
    },
}

/// What a [`Screen`] shows once read.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Pulls(Result<Vec<Read<Pull>>, CallError>),
    Pull(Result<Links, CallError>),
    Intxt(Result<Read<Intxt>, CallError>),
}

/// Reads the screens from a node.
pub struct Review<C> {
    node: C,
}

/// The maximum number of intxt descriptors a pull request opens.
const MAX_LINKS_PER_PULL: usize = 32;

impl<C: NodeCalls> Review<C> {
    pub fn new(node: C) -> Self {
        Self { node }
    }

    /// Read `screen`: nothing is cached, because the node has no reactive mirror to follow yet,
    /// so every open reads again.
    pub async fn open(&self, screen: &Screen) -> Content {
        match screen {
            Screen::Pulls => Content::Pulls(self.pulls().await),
            Screen::Pull(pull) => Content::Pull(self.links(&pull.body).await),
            Screen::Intxt { drawer, .. } => Content::Intxt(self.intxt(drawer).await),
        }
    }

    /// The datums of `doc` at `keys` (every datum when none), or why the node would not give
    /// them.
    async fn read(
        &self,
        doc: &DocId,
        keys: Option<BTreeSet<DatumKey>>,
    ) -> Result<Read<BTreeMap<DatumKey, DatumRead>>, CallError> {
        let read = self
            .node
            .call(ReadDatums {
                capability: self.node.configuration()?.doc_cap,
                doc_id: doc.clone(),
                at: At::Current,
                datums: keys,
                optimistic: false,
            })
            .await
            .map_err(CallError::from);
        match read {
            Ok(read) => Ok(Read::Ready(read.datums)),
            Err(CallError::Refused(reason)) => Ok(Read::Unreadable(format!("{doc}: {reason}"))),
            Err(other) => Err(other),
        }
    }

    /// The `githubPull` datums of the GitHub drawer's own documents, then each one read: a poll
    /// from the start with no hold, paged until caught up and asking only for the tag, so a page
    /// carries no values (the shape of `Caller::key_holders`).
    pub(crate) async fn pulls(&self) -> Result<Vec<Read<Pull>>, CallError> {
        let configuration = self.node.configuration()?;
        let tag = DatumTag::new(work::GithubPull::ID).expect("a datum id is a tag");
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: configuration.github_drawer.clone(),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasTag { tag: tag.clone() },
            ],
        };
        let datums = DatumSelection::Of {
            tags: [tag].into(),
            keys: BTreeSet::new(),
        };
        let mut held = BTreeSet::new();
        let mut after = 0;
        loop {
            let page = self
                .node
                .call(PollDatums {
                    capability: configuration.doc_cap,
                    predicate: predicate.clone(),
                    datums: datums.clone(),
                    after,
                    limit: 0,
                    hold_ms: 0,
                })
                .await?;
            // Events are in revision order, so the last word stands: a removed row is a pull
            // request taken out, and a `leave` is the document moving out of the result.
            for event in page.events {
                match event {
                    DatumEvent::Datum {
                        doc_id,
                        datum_key,
                        datum: ObservedDatum::Present { .. },
                        ..
                    } => {
                        held.insert((doc_id, datum_key));
                    }
                    DatumEvent::Datum {
                        doc_id,
                        datum_key,
                        datum: ObservedDatum::Removed { .. },
                        ..
                    } => {
                        held.remove(&(doc_id, datum_key));
                    }
                    DatumEvent::Leave { doc_id, .. } => held.retain(|(held, _)| *held != doc_id),
                }
            }
            if page.caught_up {
                break;
            }
            // A node that is not caught up and does not move the cursor would be asked forever.
            if page.through <= after {
                return Err(CallError::Refused(format!(
                    "the poll for pull requests is not caught up and made no progress past {after}"
                )));
            }
            after = page.through;
        }
        futures::future::try_join_all(held.into_iter().map(|(doc, key)| self.pull(doc, key))).await
    }

    async fn pull(&self, doc: DocId, key: DatumKey) -> Result<Read<Pull>, CallError> {
        let datums = match self.read(&doc, Some([key.clone()].into())).await? {
            Read::Ready(datums) => datums,
            Read::Unreadable(reason) => return Ok(Read::Unreadable(reason)),
        };
        let value = match datums.get(&key) {
            Some(DatumRead::Accepted { value, .. }) => value.clone(),
            other => {
                let why = other.map_or_else(|| "absent".to_owned(), why);
                return Ok(Read::Unreadable(format!("{doc}: {key} is {why}")));
            }
        };
        let pull: work::GithubPull =
            serde_json::from_value(value).expect("the node admitted this value as a githubPull");
        Ok(Read::Ready(Pull {
            doc,
            slot: String::from_utf8_lossy(key.slot_bytes()).into_owned(),
            title: pull.title,
            state: pull.state,
            body: pull.body,
            url: pull.url,
        }))
    }

    /// The intxt's index document (D70): the drawer's own document holds only the intxt marker
    /// and a pointer to the index content document, which holds the title, the body and the
    /// entries. `None` is a readable document with no intxt marker.
    async fn index(&self, drawer: &DocId) -> Result<Read<Option<DocId>>, CallError> {
        let marker = key(generic::Intxt::ID, "main");
        let pointer = key(generic::Index::ID, "main");
        let datums = match self
            .read(drawer, Some([marker.clone(), pointer.clone()].into()))
            .await?
        {
            Read::Ready(datums) => datums,
            Read::Unreadable(reason) => return Ok(Read::Unreadable(reason)),
        };
        match datums.get(&marker) {
            None | Some(DatumRead::Absent { .. }) => return Ok(Read::Ready(None)),
            Some(DatumRead::Accepted { .. }) => {}
            Some(other) => {
                return Ok(Read::Unreadable(format!(
                    "{drawer}: its intxt marker is {}",
                    why(other)
                )));
            }
        }
        let pointer = match accepted(&datums, &pointer) {
            Ok(pointer) => pointer,
            Err(reason) => {
                return Ok(Read::Unreadable(format!(
                    "{drawer}: its index pointer is {reason}"
                )));
            }
        };
        let pointer: generic::Index = serde_json::from_value(pointer.clone())
            .expect("the node admitted this value as an index pointer");
        // The pointer's text is the node's own writing, admitted as an index; an id that does
        // not parse is a broken invariant, not a reading to show.
        let index = pointer
            .doc
            .parse()
            .expect("the node admitted this index pointer's document id");
        Ok(Read::Ready(Some(index)))
    }

    /// The intxts `description` names, with the title and entry count each node lets this micro
    /// read: per drawer, in the block's order, its index pointer and then the index's title and
    /// body.
    pub(crate) async fn links(&self, description: &str) -> Result<Links, CallError> {
        let named = match generic::PrLinks::read(description) {
            Ok(named) => named,
            Err(malformed) => return Ok(Links::Malformed(malformed.to_string())),
        };
        let descriptor_keys = BTreeSet::from([
            key(generic::Title::ID, "main"),
            key(generic::Body::ID, "main"),
        ]);
        let mut rows = Vec::new();
        for link in named.intxts.into_iter().take(MAX_LINKS_PER_PULL) {
            let drawer = link.reference.document().clone();
            let outcome = match self.index(&drawer).await? {
                Read::Ready(None) => LinkOutcome::NotFound,
                Read::Ready(Some(index)) => {
                    match self.read(&index, Some(descriptor_keys.clone())).await? {
                        Read::Ready(datums) => match title_and_body(&datums) {
                            Ok((title, body)) => LinkOutcome::Found {
                                title: title.title,
                                description: title.description,
                                entries: u32::try_from(body.order.len()).expect("few entries"),
                            },
                            Err(reason) => LinkOutcome::Unreadable(format!("{index}: {reason}")),
                        },
                        Read::Unreadable(reason) => LinkOutcome::Unreadable(reason),
                    }
                }
                Read::Unreadable(reason) => LinkOutcome::Unreadable(reason),
            };
            rows.push(LinkRow {
                drawer,
                hinted_title: link.title,
                outcome,
            });
        }
        Ok(Links::Named(rows))
    }

    /// The text of a patch: the blob datum read from its document, then the bytes it names from
    /// the blob store. Called when the view shows an entry's changes, never for the whole intxt.
    pub(crate) async fn patch(&self, source: &PatchSource) -> Result<String, CallError> {
        let PatchSource { doc, key } = source;
        let datums = match self.read(doc, Some([key.clone()].into())).await? {
            Read::Ready(datums) => datums,
            Read::Unreadable(reason) => return Err(CallError::Refused(reason)),
        };
        let value = accepted(&datums, key)
            .map_err(|reason| CallError::Refused(format!("{doc}: {key} is {reason}")))?;
        let blob: Blob =
            serde_json::from_value(value.clone()).expect("the node admitted this value as a blob");
        let blob_id: BlobId = blob
            .digest
            .parse()
            .expect("the node admitted this blob's digest");
        let bytes = self
            .node
            .read_blob(ReadBlob {
                capability: self.node.configuration()?.blob_cap,
                blob_id,
            })
            .await?;
        String::from_utf8(bytes).map_err(|error| {
            CallError::Unshowable(format!("the {} patch is not text: {error}", blob.mime))
        })
    }

    /// The intxt in `drawer`, assembled from its index document.
    pub(crate) async fn intxt(&self, drawer: &DocId) -> Result<Read<Intxt>, CallError> {
        let index = match self.index(drawer).await? {
            Read::Ready(Some(index)) => index,
            Read::Ready(None) => {
                return Ok(Read::Unreadable(format!(
                    "{drawer}: it is not an intxt (no intxt marker)"
                )));
            }
            Read::Unreadable(reason) => return Ok(Read::Unreadable(reason)),
        };
        Ok(match self.read(&index, None).await? {
            Read::Ready(datums) => assemble(&index, &datums)
                .map_or_else(|reason| Read::Unreadable(format!("{index}: {reason}")), Read::Ready),
            Read::Unreadable(reason) => Read::Unreadable(reason),
        })
    }
}

/// Why a datum that is not accepted cannot be shown.
fn why(read: &DatumRead) -> String {
    match read {
        DatumRead::Accepted { .. } => unreachable!("an accepted datum is shown, not explained"),
        DatumRead::Absent { .. } => "absent".to_owned(),
        DatumRead::Pending { reason, .. } => format!("pending: {reason}"),
        DatumRead::Rejected { reason } => format!("rejected: {reason}"),
        DatumRead::Undecodable { reason, .. } => format!("undecodable: {reason}"),
    }
}

fn accepted<'datums>(
    datums: &'datums BTreeMap<DatumKey, DatumRead>,
    key: &DatumKey,
) -> Result<&'datums serde_json::Value, String> {
    match datums.get(key) {
        Some(DatumRead::Accepted { value, .. }) => Ok(value),
        Some(other) => Err(why(other)),
        None => Err("not in the document".to_owned()),
    }
}

/// The title and body are the index datums shared by the intxt page and its PR rows.
fn title_and_body(
    datums: &BTreeMap<DatumKey, DatumRead>,
) -> Result<(generic::Title, generic::Body), String> {
    let title_key = key(generic::Title::ID, "main");
    let title: generic::Title = serde_json::from_value(
        accepted(datums, &title_key)
            .map_err(|reason| format!("its title is {reason}"))?
            .clone(),
    )
    .expect("the node admitted this value as a title");
    let body_key = key(generic::Body::ID, "main");
    let body: generic::Body = serde_json::from_value(
        accepted(datums, &body_key)
            .map_err(|reason| format!("its body is {reason}"))?
            .clone(),
    )
    .expect("the node admitted this value as a body");
    Ok((title, body))
}

/// An intxt from its index document's datums: the title, and the body's entries in order, a
/// duplicated reference once (ADR 036 §2). The title and the body are what make it an intxt; an
/// entry that cannot be read is a row saying so. `index` is the document a `self` reference in
/// an entry resolves to.
fn assemble(index: &DocId, datums: &BTreeMap<DatumKey, DatumRead>) -> Result<Intxt, String> {
    let (title, body) = title_and_body(datums)?;
    let mut shown = BTreeSet::new();
    let entries = body
        .order
        .iter()
        .map(|reference| reference.key())
        .filter(|listed| shown.insert((*listed).clone()))
        .map(|listed| entry(index, listed, accepted(datums, listed)))
        .collect();
    Ok(Intxt {
        title: title.title,
        description: title.description,
        entries,
    })
}

fn entry(index: &DocId, key: &DatumKey, value: Result<&serde_json::Value, String>) -> Entry {
    let slot = String::from_utf8_lossy(key.slot_bytes()).into_owned();
    let value = match value {
        Ok(value) => value,
        Err(reason) => {
            return Entry {
                kind: EntryKind::Unreadable(reason),
                label: slot,
                changes: Changes::Absent,
            };
        }
    };
    let tag = key.tag().as_str();
    // A missing patch is "too large to carry" (the entry's own doc); a present one names a blob
    // datum, in this document or the one the reference names.
    let changes = |patch: &Option<DatumReference>| match patch {
        None => Changes::TooLarge,
        Some(reference) => Changes::Blob(PatchSource {
            doc: reference.document().resolve(index),
            key: reference.key().clone(),
        }),
    };
    if tag == work::GitCommitEntry::ID {
        let commits: work::GitCommitEntry = serde_json::from_value(value.clone())
            .expect("the node admitted this value as a gitCommitEntry");
        // The range as `git log` writes it. The sha is the commit key's last segment.
        let short = |commit: &str| {
            let sha = commit.rsplit('/').next().unwrap_or(commit);
            sha.get(..7).unwrap_or(sha).to_owned()
        };
        let label = match &commits.base {
            Some(base) => format!("{}..{}", short(base), short(&commits.tip)),
            None => short(&commits.tip),
        };
        return Entry {
            kind: EntryKind::Commits,
            label,
            changes: changes(&commits.patch),
        };
    }
    if tag == work::JjChangeEntry::ID {
        let change: work::JjChangeEntry = serde_json::from_value(value.clone())
            .expect("the node admitted this value as a jjChangeEntry");
        return Entry {
            kind: EntryKind::JjChange,
            label: slot,
            changes: changes(&change.patch),
        };
    }
    let kind = if tag == work::GitRefEntry::ID {
        EntryKind::TrackedRef
    } else if tag == work::SessionEntry::ID {
        EntryKind::Session
    } else if tag == work::GithubPullEntry::ID {
        EntryKind::PullRequest
    } else {
        EntryKind::Other(tag.to_owned())
    };
    Entry {
        kind,
        label: slot,
        changes: Changes::Absent,
    }
}

#[cfg(test)]
mod tests;
