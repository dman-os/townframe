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
use big_cabinet_core::DocId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::ReadDatums;
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

    pub fn state_label(&self) -> &str {
        match &self.state {
            PullState::Open => "open",
            PullState::Closed => "closed",
            PullState::Merged => "merged",
            PullState::Unknown(other) => other,
        }
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

    /// Every datum of `doc`, or why the node would not give them.
    async fn read_all(
        &self,
        doc: &DocId,
    ) -> Result<Read<BTreeMap<DatumKey, DatumRead>>, CallError> {
        let capability = self.node.read_capability(doc).await?;
        let read = self
            .node
            .call(ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: None,
                optimistic: false,
            })
            .await;
        match read {
            Ok(read) => Ok(Read::Ready(read.datums)),
            Err(CallError::Refused(reason)) => Ok(Read::Unreadable(format!("{doc}: {reason}"))),
            Err(transport) => Err(transport),
        }
    }

    pub(crate) async fn pulls(&self) -> Result<Vec<Read<Pull>>, CallError> {
        let docs = self.node.held_pulls().await?;
        futures::future::try_join_all(docs.into_iter().map(|doc| self.pull(doc))).await
    }

    async fn pull(&self, doc: DocId) -> Result<Read<Pull>, CallError> {
        let datums = match self.read_all(&doc).await? {
            Read::Ready(datums) => datums,
            Read::Unreadable(reason) => return Ok(Read::Unreadable(reason)),
        };
        let Some((key, read)) = datums
            .into_iter()
            .find(|(key, _)| key.tag().as_str() == work::GithubPull::ID)
        else {
            return Ok(Read::Unreadable(format!("{doc} holds no pull request")));
        };
        let DatumRead::Accepted { value, .. } = read else {
            return Ok(Read::Unreadable(format!("{doc}: {}", why(&read))));
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
        Ok(match self.read_all(drawer).await? {
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
mod tests {
    use super::*;
    use crate::fixture::Fixture;
    use intxt_core::generic::Intxt as IntxtMarker;
    use intxt_core::generic::PrLink;
    use intxt_core::generic::PrLinks;

    fn review(fixture: &Fixture) -> Review<Fixture> {
        Review::new(fixture.clone())
    }

    fn found_titles(links: &Links) -> Vec<&str> {
        let Links::Named(rows) = links else {
            panic!("expected named links, got {links:?}")
        };
        rows.iter()
            .map(|row| match &row.outcome {
                LinkOutcome::Found { title, .. } => title.as_str(),
                LinkOutcome::NotFound => "(not an intxt)",
                LinkOutcome::Unreadable(_) => "(unreadable)",
            })
            .collect()
    }

    #[tokio::test]
    async fn the_list_shows_each_held_pull_with_its_repo_ref_title_and_state() {
        let pulls = review(&Fixture::sample()).pulls().await.unwrap();
        let shown: Vec<(String, String, &str)> = pulls
            .iter()
            .map(|read| match read {
                Read::Ready(pull) => (pull.repo_ref(), pull.title.clone(), pull.state_label()),
                Read::Unreadable(reason) => panic!("sample pulls are readable: {reason}"),
            })
            .collect();
        assert_eq!(
            shown,
            [
                (
                    "dman-os/townframe#150".to_owned(),
                    "intxt: read a PR and its intxts on the phone".to_owned(),
                    "open"
                ),
                (
                    "dman-os/townframe#151".to_owned(),
                    "chore: pin the review micro's lockfile".to_owned(),
                    "merged"
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_held_document_without_a_pull_is_an_unreadable_row_not_a_missing_one() {
        let bare = Fixture::doc_id("5");
        let fixture = Fixture::empty().doc(bare.clone(), []).held_pull(bare);
        let pulls = review(&fixture).pulls().await.unwrap();
        assert!(
            matches!(&pulls[..], [Read::Unreadable(reason)] if reason.contains("no pull request"))
        );
    }

    #[tokio::test]
    async fn the_block_names_its_intxts_and_one_call_reads_them_all() {
        let fixture = Fixture::sample();
        let review = review(&fixture);
        let Read::Ready(pull) = review.pulls().await.unwrap().remove(0) else {
            panic!("the first sample pull is readable")
        };
        fixture.forget_calls();
        let links = review.links(&pull.body).await.unwrap();
        assert_eq!(
            found_titles(&links),
            [
                "intxt: commits live with the intxt",
                "review micro: the PR on the phone",
                "(not an intxt)"
            ]
        );
        let Links::Named(rows) = &links else {
            unreachable!()
        };
        assert_eq!(
            rows[0].hinted_title.as_deref(),
            Some("intxt: commits live with the intxt")
        );
        assert!(matches!(
            &rows[1].outcome,
            LinkOutcome::Found { description: Some(text), entries: 1, .. } if text.contains("phone")
        ));
        assert_eq!(fixture.calls(), ["org.example.intxt.summarizeIntxts"]);
    }

    #[tokio::test]
    async fn a_description_without_a_block_names_nothing_and_asks_the_node_nothing() {
        let fixture = Fixture::sample();
        let links = review(&fixture).links("Just words.").await.unwrap();
        assert_eq!(links, Links::Named(vec![]));
        assert!(fixture.calls().is_empty());
    }

    #[tokio::test]
    async fn a_malformed_block_is_an_error_not_an_empty_list() {
        let body = "<details>\n<summary>intxt</summary>\n\n```json\n{ \"$type\": \"org.example.intxt.prLinks\", \"intxts\": [{ \"ref\": 5 }] }\n```\n\n</details>";
        let fixture = Fixture::sample();
        let links = review(&fixture).links(body).await.unwrap();
        assert!(
            matches!(&links, Links::Malformed(why) if why.contains("malformed")),
            "{links:?}"
        );
        assert!(fixture.calls().is_empty());
    }

    #[tokio::test]
    async fn a_drawer_the_node_does_not_hold_is_unreadable_and_an_unrelated_document_is_not_found()
    {
        let held_elsewhere = Fixture::doc_id("6");
        let body = PrLinks {
            intxts: vec![
                PrLink::new(IntxtMarker::reference(&Fixture::doc_id("41"), [])),
                PrLink::new(IntxtMarker::reference(&held_elsewhere, [])),
            ],
            ..Default::default()
        }
        .block();
        let Links::Named(rows) = review(&Fixture::sample()).links(&body).await.unwrap() else {
            panic!("the block is well formed")
        };
        assert_eq!(rows[0].outcome, LinkOutcome::NotFound);
        assert!(
            matches!(&rows[1].outcome, LinkOutcome::Unreadable(reason) if reason.contains("does not hold"))
        );
    }

    #[tokio::test]
    async fn more_drawers_than_one_call_may_name_are_read_in_batches_in_order() {
        let drawers: Vec<DocId> = ('a'..='k')
            .flat_map(|second| {
                ['2', '3', '4', '5'].map(|first| Fixture::doc_id(&format!("{first}{second}")))
            })
            .take(MAX_SUMMARIZED + 8)
            .collect();
        let body = PrLinks {
            intxts: drawers
                .iter()
                .map(|drawer| PrLink::new(IntxtMarker::reference(drawer, [])))
                .collect(),
            ..Default::default()
        }
        .block();
        let fixture = Fixture::empty();
        let Links::Named(rows) = review(&fixture).links(&body).await.unwrap() else {
            panic!("the block is well formed")
        };
        assert_eq!(
            rows.iter()
                .map(|row| row.drawer.clone())
                .collect::<Vec<_>>(),
            drawers
        );
        assert_eq!(fixture.calls().len(), 2);
    }

    #[tokio::test]
    async fn an_intxt_shows_its_title_description_and_entries_in_the_bodys_order() {
        let intxt = review(&Fixture::sample())
            .intxt(&Fixture::doc_id("31"))
            .await
            .unwrap();
        let Read::Ready(intxt) = intxt else {
            panic!("the sample intxt is readable")
        };
        assert_eq!(intxt.title, "intxt: commits live with the intxt");
        assert_eq!(
            intxt.description.as_deref(),
            Some("Commits travel with the intxt that names them.")
        );
        let shown: Vec<(&str, &str, bool)> = intxt
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.kind.name(),
                    entry.label.as_str(),
                    entry.patch.is_some(),
                )
            })
            .collect();
        assert_eq!(
            shown,
            [
                ("commits", "9a0e11c..4c1f0a7", true),
                ("commits", "77b2d03", false),
                ("pull request", "dman-os/townframe/150", false),
            ]
        );
    }

    #[tokio::test]
    async fn an_intxt_missing_its_title_or_an_entry_says_so_instead_of_showing_less() {
        let drawer = Fixture::doc_id("7");
        let fixture = Fixture::empty().doc(drawer.clone(), []);
        let reading = review(&fixture).intxt(&drawer).await.unwrap();
        assert!(
            matches!(&reading, Read::Unreadable(reason) if reason.contains("title")),
            "{reading:?}"
        );

        let Read::Ready(intxt) =
            review(&Fixture::sample().listing_a_missing_entry(&Fixture::doc_id("32")))
                .intxt(&Fixture::doc_id("32"))
                .await
                .unwrap()
        else {
            panic!("the title and body are there")
        };
        assert!(
            intxt
                .entries
                .iter()
                .any(|entry| matches!(&entry.kind, EntryKind::Unreadable(reason) if reason.contains("not in the document")))
        );
    }

    #[tokio::test]
    async fn an_intxt_the_node_does_not_hold_is_an_unreadable_row() {
        let reading = review(&Fixture::sample())
            .intxt(&Fixture::doc_id("8"))
            .await
            .unwrap();
        assert!(matches!(reading, Read::Unreadable(reason) if reason.contains("does not hold")));
    }
}
