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
use intxt_core::generic;
use intxt_core::methods::MAX_SUMMARIZED;
use intxt_core::methods::SummarizeIntxts;
use intxt_core::methods::SummaryStatus;
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

/// What `summarizeIntxts` said of one drawer.
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
    /// A commit entry's unified patch, when the datum carries one.
    pub patch: Option<String>,
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

    /// The intxts `description` names, with what the node says of each: one `summarizeIntxts`
    /// call (more only past its [`MAX_SUMMARIZED`] bound), none when nothing is named.
    pub(crate) async fn links(&self, description: &str) -> Result<Links, CallError> {
        let named = match generic::PrLinks::read(description) {
            Ok(named) => named,
            Err(malformed) => return Ok(Links::Malformed(malformed.to_string())),
        };
        let drawers: Vec<DocId> = named
            .intxts
            .iter()
            .map(|link| link.reference.document().clone())
            .collect();
        let mut summaries = Vec::with_capacity(drawers.len());
        for batch in drawers.chunks(MAX_SUMMARIZED) {
            let summarized = self
                .node
                .call(SummarizeIntxts {
                    drawers: batch.to_vec(),
                })
                .await?;
            summaries.extend(summarized.results);
        }
        assert_eq!(
            summaries.len(),
            drawers.len(),
            "summarizeIntxts answers one result per drawer, in order"
        );
        let rows = named
            .intxts
            .into_iter()
            .zip(summaries)
            .map(|(link, summary)| LinkRow {
                drawer: summary.drawer,
                hinted_title: link.title,
                outcome: match summary.status {
                    SummaryStatus::Found => LinkOutcome::Found {
                        title: summary.title.expect("a found intxt has a title"),
                        description: summary.description,
                        entries: summary.entries.expect("a found intxt counts its entries"),
                    },
                    SummaryStatus::NotFound => LinkOutcome::NotFound,
                    SummaryStatus::Unreadable => LinkOutcome::Unreadable(
                        summary.reason.expect("an unreadable drawer says why"),
                    ),
                },
            })
            .collect();
        Ok(Links::Named(rows))
    }

    pub(crate) async fn intxt(&self, drawer: &DocId) -> Result<Read<Intxt>, CallError> {
        Ok(match self.read(drawer, None).await? {
            Read::Ready(datums) => assemble(&datums).map_or_else(Read::Unreadable, Read::Ready),
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

/// An intxt from its descriptor document's datums: the title, and the body's entries in order, a
/// duplicated reference once (ADR 036 §2). The title and the body are what make it an intxt; an
/// entry that cannot be read is a row saying so.
fn assemble(datums: &BTreeMap<DatumKey, DatumRead>) -> Result<Intxt, String> {
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
    let mut shown = BTreeSet::new();
    let entries = body
        .order
        .iter()
        .map(|reference| reference.key())
        .filter(|listed| shown.insert((*listed).clone()))
        .map(|listed| entry(listed, accepted(datums, listed)))
        .collect();
    Ok(Intxt {
        title: title.title,
        description: title.description,
        entries,
    })
}

fn entry(key: &DatumKey, value: Result<&serde_json::Value, String>) -> Entry {
    let slot = String::from_utf8_lossy(key.slot_bytes()).into_owned();
    let value = match value {
        Ok(value) => value,
        Err(reason) => {
            return Entry {
                kind: EntryKind::Unreadable(reason),
                label: slot,
                patch: None,
            };
        }
    };
    let tag = key.tag().as_str();
    let kind = if tag == work::GitCommitEntry::ID {
        EntryKind::Commits
    } else if tag == work::JjChangeEntry::ID {
        EntryKind::JjChange
    } else if tag == work::GitRefEntry::ID {
        EntryKind::TrackedRef
    } else if tag == work::SessionEntry::ID {
        EntryKind::Session
    } else if tag == work::GithubPullEntry::ID {
        EntryKind::PullRequest
    } else {
        EntryKind::Other(tag.to_owned())
    };
    if kind != EntryKind::Commits {
        return Entry {
            kind,
            label: slot,
            patch: None,
        };
    }
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
    Entry {
        kind,
        label,
        // The datum's own text, carried beside the fields the vocabulary knows (see
        // `Fixture::sample`: where a commit's patch really comes from is an open design question).
        patch: value
            .get("patch")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
    }
}

#[cfg(test)]
mod tests;
