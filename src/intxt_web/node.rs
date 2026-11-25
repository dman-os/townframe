//! The node-backed [`IntxtSource`]: one intxt, read from a day node over its XRPC.
//!
//! The reads are the ones `intxt_core::client::Caller` makes for the CLI (the drawer's `index/main`
//! pointer, the index document's title, body and entries, the content documents found by their
//! `<kind>/main` markers through `pollDatums`), but over capabilities the operator minted at setup
//! instead of ones the reader mints for itself: a viewer holds no authority to mint. A transport
//! ([`HttpTransport`]) only moves bytes; this module encodes and decodes the XRPC envelope with
//! `service_client_core_rs`, the same code the CLI and the nodules use.

use crate::diff::MAX_RENDERED_OCTETS;
use crate::source::IntxtSource;
use crate::source::LoadError;
use crate::source::Origin;
use crate::view::CommentDatum;
use crate::view::CommitDatum;
use crate::view::EntryDatum;
use crate::view::IntxtView;
use crate::view::PatchDatum;
use crate::view::SessionSummary;
use crate::view::sha_of;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::BlobsError;
use big_blobs_core::ReadBlob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumReference;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use big_cabinet_core::reference::ReferenceDocId;
use intxt_core::annotation::Comment;
use intxt_core::annotation::Notes;
use intxt_core::generic::Body;
use intxt_core::generic::Index;
use intxt_core::generic::Title;
use intxt_core::work::AgentSession;
use intxt_core::work::Commits;
use intxt_core::work::GitCommit;
use intxt_core::work::GitCommitEntry;
use intxt_core::work::GitRef;
use intxt_core::work::GitRefEntry;
use intxt_core::work::GithubPull;
use intxt_core::work::GithubPullEntry;
use intxt_core::work::JjChange;
use intxt_core::work::JjChangeEntry;
use intxt_core::work::PromptTurn;
use intxt_core::work::SessionEntry;
use method_core::ByteRange;
use method_core::Method;
use method_core::Unary;
use serde::de::DeserializeOwned;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_core_rs::xrpc::XrpcClientError;
use service_client_core_rs::xrpc::decode_response;
use service_client_core_rs::xrpc::encode_request;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

/// How many `promptTurn` datums one read asks for: the most turns whole the viewer holds at once.
const TURNS_PER_READ: usize = 16;

/// The environment variables the handler reads its configuration from.
pub const NODE_VAR: &str = "INTXT_WEB_NODE";
pub const DOC_CAP_VAR: &str = "INTXT_WEB_DOC_CAP";
pub const BLOB_CAP_VAR: &str = "INTXT_WEB_BLOB_CAP";
pub const DISPATCH_CAP_VAR: &str = "INTXT_WEB_DISPATCH_CAP";

/// Where the node is and what the viewer may read there.
///
/// INTERIM: the two capabilities are minted by the operator at setup, over the drawers to show
/// (`mintDocCapability` with `drawers: {<drawer>: read}` and `mintBlobsCapability` with the same),
/// and handed in through the environment. A capability in an environment variable is a secret
/// every page of this deployment shares. It becomes "public" read for a drawer the node's owner
/// published, which is citizenship entry C11; none of this survives that.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    /// The base of the node's XRPC over TCP, such as `http://127.0.0.1:8080`.
    pub node: String,
    pub doc_cap: CapId,
    pub blob_cap: CapId,
    /// The dispatch capability every request is presented with as its `Authorization: Bearer`.
    /// The node's TCP edge admits no anonymous request, and dispatch admits a call only for a
    /// bearer whose scope names the method, so the scope of this one is `readDatums`,
    /// `pollDatums` and `blobs.read`. The doc and blob capabilities above are the method
    /// parameters that say what those calls may reach.
    pub bearer: CapId,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ConfigError {
    #[error("{} is set, so {} must be set too", NODE_VAR, missing.join(" and "))]
    Incomplete { missing: Vec<&'static str> },
    #[error("{var} is not a capability id: {reason}")]
    BadCapability { var: &'static str, reason: String },
}

impl NodeConfig {
    /// The configuration `var` (an environment lookup) holds: none when no variable of the three
    /// is set, which means the fixture; an error when only some are, which is a misconfiguration
    /// to report and not to guess around.
    pub fn from_env(var: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, ConfigError> {
        let node = var(NODE_VAR).filter(|value| !value.is_empty());
        let doc = var(DOC_CAP_VAR).filter(|value| !value.is_empty());
        let blob = var(BLOB_CAP_VAR).filter(|value| !value.is_empty());
        let bearer = var(DISPATCH_CAP_VAR).filter(|value| !value.is_empty());
        let (Some(node), Some(doc), Some(blob), Some(bearer)) =
            (node.clone(), doc.clone(), blob.clone(), bearer.clone())
        else {
            if node.is_none() && doc.is_none() && blob.is_none() && bearer.is_none() {
                return Ok(None);
            }
            let missing = [
                (NODE_VAR, node.is_none()),
                (DOC_CAP_VAR, doc.is_none()),
                (BLOB_CAP_VAR, blob.is_none()),
                (DISPATCH_CAP_VAR, bearer.is_none()),
            ]
            .into_iter()
            .filter_map(|(name, absent)| absent.then_some(name))
            .collect();
            return Err(ConfigError::Incomplete { missing });
        };
        let cap = |name: &'static str, text: String| {
            serde_json::from_value(serde_json::Value::String(text)).map_err(|error| {
                ConfigError::BadCapability {
                    var: name,
                    reason: error.to_string(),
                }
            })
        };
        Ok(Some(Self {
            node,
            doc_cap: cap(DOC_CAP_VAR, doc)?,
            blob_cap: cap(BLOB_CAP_VAR, blob)?,
            bearer: cap(DISPATCH_CAP_VAR, bearer)?,
        }))
    }
}

/// Where a request goes: a base URL split into what `wasi:http` wants. The transport joins a
/// request's relative `xrpc/...` path onto `path_prefix`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseUrl {
    pub https: bool,
    pub authority: String,
    /// Empty, or starting with `/` and ending without one.
    pub path_prefix: String,
}

impl BaseUrl {
    pub fn parse(url: &str) -> Result<Self, String> {
        let (https, rest) = if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("{url:?} is not an http(s) URL"));
        };
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        if authority.is_empty() || authority.contains(['?', '#', '@']) {
            return Err(format!("{url:?} names no usable host"));
        }
        let path = path.trim_end_matches('/');
        Ok(Self {
            https,
            authority: authority.to_owned(),
            path_prefix: if path.is_empty() {
                String::new()
            } else {
                format!("/{path}")
            },
        })
    }

    /// The path and query of a request whose `path` is relative to the base (`xrpc/<nsid>?<query>`).
    pub fn target(&self, path: &str) -> String {
        format!("{}/{path}", self.path_prefix)
    }
}

/// Why one call did not give its answer.
enum Fault<E> {
    /// The method answered with its own error.
    Method(E),
    /// The call did not complete: nothing answered, or what answered was not the protocol.
    Transport(String),
}

/// Why a load stopped, before it is given the drawer it was for.
enum Failure {
    NotFound,
    Forbidden,
    Unreachable(String),
    Unreadable(String),
}

impl From<Fault<DocsError>> for Failure {
    fn from(fault: Fault<DocsError>) -> Self {
        match fault {
            Fault::Method(DocsError::DocNotFound { .. }) => Self::NotFound,
            Fault::Method(DocsError::Denied) => Self::Forbidden,
            Fault::Method(other) => Self::Unreadable(other.to_string()),
            Fault::Transport(reason) => Self::Unreachable(reason),
        }
    }
}

fn unreadable(reason: impl std::fmt::Display) -> Failure {
    Failure::Unreadable(reason.to_string())
}

/// An intxt read from a node: `H` moves the bytes (`wasi:http` in the component, an in-memory
/// node in tests).
pub struct NodeSource<H> {
    transport: H,
    config: NodeConfig,
}

impl<H: HttpTransport> NodeSource<H> {
    pub fn new(transport: H, config: NodeConfig) -> Self {
        Self { transport, config }
    }

    fn metadata(&self, range: Option<ByteRange>) -> HttpMetadata {
        HttpMetadata {
            authorization: Some(Authorization::identity(self.config.bearer).header_value()),
            idempotency_key: None,
            range,
        }
    }

    async fn call<M: Unary>(&self, input: M) -> Result<M::Output, Fault<M::Error>> {
        let context = RequestContext::root();
        let metadata = self.metadata(None);
        let request = encode_request(M::XRPC, &context, &metadata, &input)
            .expect("a typed method's request encodes");
        let response = self
            .transport
            .send(request)
            .await
            .map_err(|error| Fault::Transport(error.message))?;
        match decode_response::<M::Output, M::Error>(
            M::XRPC,
            context.request_id,
            response.status,
            &response.headers,
            &response.body,
        ) {
            Ok(answer) => Ok(answer.output),
            Err(XrpcClientError::Domain { error, .. }) => Err(Fault::Method(error)),
            Err(other) => Err(Fault::Transport(other.to_string())),
        }
    }

    /// The datums of `doc` (all of them when `keys` is none) that are accepted, as their values.
    /// A datum that is absent, pending or rejected is not in the answer.
    async fn read(
        &self,
        doc: &DocId,
        keys: Option<BTreeSet<DatumKey>>,
    ) -> Result<BTreeMap<DatumKey, serde_json::Value>, Failure> {
        let read = self
            .call(ReadDatums {
                capability: self.config.doc_cap,
                doc_id: doc.clone(),
                at: At::Current,
                datums: keys,
                optimistic: false,
            })
            .await?;
        Ok(read
            .datums
            .into_iter()
            .filter_map(|(key, read)| match read {
                DatumRead::Accepted { value, .. } => Some((key, value)),
                _ => None,
            })
            .collect())
    }

    /// The datum `reference` names, as `T`: it must be there and be a `T`.
    async fn read_referenced<T: DeserializeOwned>(
        &self,
        containing: &DocId,
        reference: &DatumReference,
    ) -> Result<T, Failure> {
        let doc = reference.document().resolve(containing);
        let key = reference.key();
        let mut datums = self.read(&doc, Some([key.clone()].into())).await?;
        let value = datums
            .remove(key)
            .ok_or_else(|| unreadable(format!("{key} is not readable in {doc}")))?;
        serde_json::from_value(value)
            .map_err(|error| unreadable(format!("{key} in {doc}: {error}")))
    }

    /// The content documents of `drawer` (and the drawers it reaches) holding the marker
    /// `<tag>/main`: how every writer's notes or commits are found. A poll from the start,
    /// paged until caught up, asking for the one key so a page carries no values.
    async fn marked_docs(&self, drawer: &DocId, tag: &str) -> Result<BTreeSet<DocId>, Failure> {
        let marker = DatumKey::from_text(tag, "main").expect("a marker key is valid");
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: drawer.clone(),
                    transitive: true,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasKey {
                    key: marker.clone(),
                },
            ],
        };
        let datums = DatumSelection::Of {
            tags: BTreeSet::new(),
            keys: [marker.clone()].into(),
        };
        let present = self
            .poll_present(predicate, datums, &marker.to_string())
            .await?;
        Ok(present.into_iter().map(|(doc, _)| doc).collect())
    }

    /// The `(document, key)` rows a poll from the start delivers and that are present once it is
    /// caught up, paged. A poll carries keys and versions, never values. `what` names the poll
    /// in the error for one that stalls.
    async fn poll_present(
        &self,
        predicate: Predicate,
        datums: DatumSelection,
        what: &str,
    ) -> Result<BTreeSet<(DocId, DatumKey)>, Failure> {
        let mut rows = BTreeSet::new();
        let mut after = 0;
        loop {
            let page = self
                .call(PollDatums {
                    capability: self.config.doc_cap,
                    predicate: predicate.clone(),
                    datums: datums.clone(),
                    after,
                    limit: 0,
                    hold_ms: 0,
                })
                .await?;
            // Events are in revision order, so the last word per row stands.
            for event in page.events {
                match event {
                    DatumEvent::Datum {
                        doc_id,
                        datum_key,
                        datum: ObservedDatum::Present { .. },
                        ..
                    } => {
                        rows.insert((doc_id, datum_key));
                    }
                    DatumEvent::Datum {
                        doc_id,
                        datum_key,
                        datum: ObservedDatum::Removed { .. },
                        ..
                    } => {
                        rows.remove(&(doc_id, datum_key));
                    }
                    DatumEvent::Leave { doc_id, .. } => {
                        rows.retain(|(doc, _)| *doc != doc_id);
                    }
                }
            }
            if page.caught_up {
                return Ok(rows);
            }
            if page.through <= after {
                return Err(unreadable(format!(
                    "the poll for {what} is not caught up and made no progress past {after}"
                )));
            }
            after = page.through;
        }
    }

    /// What happened in the session whose document is `session_doc`, folded from its
    /// `promptTurn` datums. The node has no projection: a datum is read whole, so a turn's
    /// messages and tool outputs come over the wire in full. What the viewer controls is what it
    /// holds: the turns' keys come from a poll (no values), the turns are read
    /// [`TURNS_PER_READ`] at a time, and each batch is folded into the summary and dropped.
    async fn session_summary(
        &self,
        drawer: &DocId,
        session_doc: &DocId,
    ) -> Result<SessionSummary, Failure> {
        let tag = DatumTag::new(PromptTurn::ID).expect("the turn tag is valid");
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: drawer.clone(),
                    transitive: true,
                    at_least: DrawerAccess::Read,
                },
                Predicate::IsDoc {
                    doc_id: session_doc.clone(),
                },
            ],
        };
        let datums = DatumSelection::Of {
            tags: [tag.clone()].into(),
            keys: BTreeSet::new(),
        };
        let keys: Vec<DatumKey> = self
            .poll_present(predicate, datums, PromptTurn::ID)
            .await?
            .into_iter()
            .map(|(_, key)| key)
            .collect();
        let mut summary = SessionSummary::default();
        for batch in keys.chunks(TURNS_PER_READ) {
            let mut read = self
                .read(session_doc, Some(batch.iter().cloned().collect()))
                .await?;
            // A turn that is not there to read (not accepted here yet) or is not a turn is
            // said so in the summary; it does not fail the page.
            for key in batch {
                match read.remove(key).map(serde_json::from_value::<PromptTurn>) {
                    Some(Ok(turn)) => summary.add(&turn),
                    Some(Err(_)) | None => summary.unreadable += 1,
                }
            }
        }
        Ok(summary)
    }

    /// The first octets (at most [`MAX_RENDERED_OCTETS`]) of a blob, none when the node does not
    /// hold it. The blob service honours `Range`; one that ignored it would answer the whole
    /// blob, which is cut here.
    async fn read_blob_prefix(&self, blob: &Blob) -> Result<Option<Vec<u8>>, Failure> {
        let blob_id: BlobId = blob
            .digest
            .parse()
            .map_err(|_| unreadable(format!("{:?} is not a blob digest", blob.digest)))?;
        let wanted = blob.length_octets.min(MAX_RENDERED_OCTETS as u64);
        if wanted == 0 {
            return Ok(Some(Vec::new()));
        }
        let context = RequestContext::root();
        let metadata = self.metadata(Some(ByteRange::Between {
            start: 0,
            end: wanted - 1,
        }));
        let input = ReadBlob {
            capability: self.config.blob_cap,
            blob_id,
        };
        let request = encode_request(ReadBlob::XRPC, &context, &metadata, &input)
            .expect("a typed method's request encodes");
        let response = self
            .transport
            .send(request)
            .await
            .map_err(|error| Failure::Unreachable(error.message))?;
        if response.status.is_success() {
            // The transport hands every body over whole, as the container `XrpcBody::Json`
            // whether or not the bytes are JSON: a blob's answer is the bytes themselves.
            let XrpcBody::Json(mut bytes) = response.body else {
                unreachable!("a transport of this viewer collects every body");
            };
            bytes.truncate(wanted as usize);
            return Ok(Some(bytes));
        }
        match decode_response::<serde_json::Value, BlobsError>(
            ReadBlob::XRPC,
            context.request_id,
            response.status,
            &response.headers,
            &response.body,
        ) {
            Ok(_) => Err(unreadable("a failed blob read decoded as an answer")),
            Err(XrpcClientError::Domain {
                error: BlobsError::NotFound,
                ..
            }) => Ok(None),
            Err(XrpcClientError::Domain {
                error: BlobsError::Denied,
                ..
            }) => Err(Failure::Forbidden),
            Err(XrpcClientError::Domain { error, .. }) => Err(unreadable(error)),
            Err(other) => Err(Failure::Unreachable(other.to_string())),
        }
    }

    /// The recorded patch an entry names, when the node holds it. The blob datum is in a commits
    /// document the intxt's drawer reaches; its bytes are in the node's blob store.
    async fn read_patch(
        &self,
        containing: &DocId,
        patch: Option<&DatumReference>,
    ) -> Result<Option<PatchDatum>, Failure> {
        let Some(patch) = patch else {
            return Ok(None);
        };
        let blob: Blob = self.read_referenced(containing, patch).await?;
        let bytes = match &blob.inline {
            Some(inline) => Some(inline.clone()),
            None => self.read_blob_prefix(&blob).await?,
        };
        Ok(bytes.map(|bytes| PatchDatum {
            text: String::from_utf8_lossy(&bytes).into_owned(),
            total_octets: blob.length_octets,
        }))
    }

    /// Every `gitCommit` the intxt's commits documents hold, by sha. Commits are content
    /// addressed, so two writers' copies of one are the same datum and either is taken.
    async fn held_commits(&self, drawer: &DocId) -> Result<BTreeMap<String, GitCommit>, Failure> {
        let mut held = BTreeMap::new();
        for doc in self.marked_docs(drawer, Commits::ID).await? {
            for (key, value) in self.read(&doc, None).await? {
                if key.tag().as_str() != GitCommit::ID {
                    continue;
                }
                let sha = String::from_utf8_lossy(key.slot_bytes()).into_owned();
                let commit = serde_json::from_value(value)
                    .map_err(|error| unreadable(format!("{key} in {doc}: {error}")))?;
                held.insert(sha, commit);
            }
        }
        Ok(held)
    }

    /// The intxt's comments in creation order. Comment slots are UUIDv7, so key order across the
    /// writers' notes documents is time order.
    async fn comments(&self, drawer: &DocId) -> Result<Vec<(DatumReference, Comment)>, Failure> {
        let mut comments = Vec::new();
        for doc in self.marked_docs(drawer, Notes::ID).await? {
            for (key, value) in self.read(&doc, None).await? {
                if key.tag().as_str() != Comment::ID {
                    continue;
                }
                let comment = serde_json::from_value(value)
                    .map_err(|error| unreadable(format!("{key} in {doc}: {error}")))?;
                comments.push((
                    DatumReference::new(ReferenceDocId::Concrete(doc.clone()), key),
                    comment,
                ));
            }
        }
        comments.sort_by(|(left, _), (right, _)| left.key().cmp(right.key()));
        Ok(comments)
    }

    async fn load_drawer(
        &self,
        drawer: &DocId,
        via: &[MultibaseKey],
    ) -> Result<IntxtView, Failure> {
        let pointer_key = DatumKey::from_text(Index::ID, "main").expect("the pointer key is valid");
        let pointer = self
            .read(drawer, Some([pointer_key.clone()].into()))
            .await?
            .remove(&pointer_key)
            // A drawer with no index pointer is not an intxt (or not one `createIntxt` finished).
            .ok_or(Failure::NotFound)?;
        let index: DocId = serde_json::from_value::<Index>(pointer)
            .map_err(|error| unreadable(format!("the index pointer: {error}")))?
            .doc
            .parse()
            .map_err(|error| unreadable(format!("the index pointer's document id: {error}")))?;

        let mut datums = self.read(&index, None).await?;
        let title: Title = singleton(&mut datums, &index, Title::ID)?;
        let body: Body = singleton(&mut datums, &index, Body::ID)?;
        // The two singletons are removed; what is left is the entries.
        let mut entry_values = datums;

        let mut commits_held = None;
        let mut resolved = Vec::new();
        for reference in &body.order {
            let key = reference.key();
            let value = entry_values
                .remove(key)
                .ok_or_else(|| unreadable(format!("{key} is in the body and not readable")))?;
            let malformed = |error: serde_json::Error| unreadable(format!("{key}: {error}"));
            match key.tag().as_str() {
                GitCommitEntry::ID => {
                    let entry: GitCommitEntry = serde_json::from_value(value).map_err(malformed)?;
                    if commits_held.is_none() {
                        commits_held = Some(self.held_commits(drawer).await?);
                    }
                    let held = commits_held.as_ref().expect("loaded just above");
                    let commits = range_commits(held, &entry.tip, entry.base.as_deref());
                    let patch = self.read_patch(&index, entry.patch.as_ref()).await?;
                    resolved.push(Resolved::Commits {
                        entry,
                        commits,
                        patch,
                    });
                }
                GitRefEntry::ID => {
                    let entry: GitRefEntry = serde_json::from_value(value).map_err(malformed)?;
                    let git_ref = self.read_referenced(&index, &entry.reference).await?;
                    resolved.push(Resolved::GitRef { entry, git_ref });
                }
                JjChangeEntry::ID => {
                    let entry: JjChangeEntry = serde_json::from_value(value).map_err(malformed)?;
                    if commits_held.is_none() {
                        commits_held = Some(self.held_commits(drawer).await?);
                    }
                    let held = commits_held.as_ref().expect("loaded just above");
                    // The change's own datum (`jjChange/<change id>`) is in the capture drawer's
                    // repository document, which an intxt's drawer does not share. The commits
                    // the intxt holds name their change (`changeId`), and the entry's slot is
                    // the change id, so the change is read back from them, oldest first as an
                    // evolog is.
                    let change_id = String::from_utf8_lossy(key.slot_bytes()).into_owned();
                    let mut commits: Vec<CommitDatum> = held
                        .iter()
                        .filter(|(_, commit)| commit.change_id.as_deref() == Some(&change_id))
                        .map(|(sha, commit)| CommitDatum {
                            sha: sha.clone(),
                            commit: commit.clone(),
                        })
                        .collect();
                    commits.sort_by(|left, right| {
                        left.commit.committer.at.cmp(&right.commit.committer.at)
                    });
                    let change = JjChange {
                        commits: commits.iter().map(|commit| commit.sha.clone()).collect(),
                        extra: Default::default(),
                    };
                    commits.reverse();
                    let patch = self.read_patch(&index, entry.patch.as_ref()).await?;
                    resolved.push(Resolved::JjChange {
                        entry,
                        change,
                        commits,
                        patch,
                    });
                }
                SessionEntry::ID => {
                    let entry: SessionEntry = serde_json::from_value(value).map_err(malformed)?;
                    let session = self.read_referenced(&index, &entry.session).await?;
                    let summary = self
                        .session_summary(drawer, &entry.session.document().resolve(&index))
                        .await?;
                    resolved.push(Resolved::Session {
                        entry,
                        session,
                        summary,
                    });
                }
                GithubPullEntry::ID => {
                    let entry: GithubPullEntry =
                        serde_json::from_value(value).map_err(malformed)?;
                    let pull = self.read_referenced(&index, &entry.pull).await?;
                    resolved.push(Resolved::Pull { entry, pull });
                }
                // A repository or worktree entry: captured work the page does not show.
                _ => {}
            }
        }

        let comments = self.comments(drawer).await?;
        let entries: Vec<EntryDatum<'_>> = resolved.iter().map(Resolved::datum).collect();
        let comments: Vec<CommentDatum<'_>> = comments
            .iter()
            .map(|(id, comment)| CommentDatum { id, comment })
            .collect();
        Ok(IntxtView::new(drawer, via, &title, &entries, &comments))
    }
}

/// The `<tag>/main` datum of the index document `index`, taken out of `datums`.
fn singleton<T: DeserializeOwned>(
    datums: &mut BTreeMap<DatumKey, serde_json::Value>,
    index: &DocId,
    tag: &str,
) -> Result<T, Failure> {
    let key = DatumKey::from_text(tag, "main").expect("a singleton key is valid");
    let value = datums
        .remove(&key)
        .ok_or_else(|| unreadable(format!("{key} is not readable in {index}")))?;
    serde_json::from_value(value).map_err(|error| unreadable(format!("{key}: {error}")))
}

/// An entry together with the datums it names, owned until the view is built from borrows of them.
enum Resolved {
    Commits {
        entry: GitCommitEntry,
        commits: Vec<CommitDatum>,
        patch: Option<PatchDatum>,
    },
    GitRef {
        entry: GitRefEntry,
        git_ref: GitRef,
    },
    JjChange {
        entry: JjChangeEntry,
        change: JjChange,
        commits: Vec<CommitDatum>,
        patch: Option<PatchDatum>,
    },
    Session {
        entry: SessionEntry,
        session: AgentSession,
        summary: SessionSummary,
    },
    Pull {
        entry: GithubPullEntry,
        pull: GithubPull,
    },
}

impl Resolved {
    fn datum(&self) -> EntryDatum<'_> {
        match self {
            Self::Commits {
                entry,
                commits,
                patch,
            } => EntryDatum::Commits {
                entry,
                commits,
                patch: patch.as_ref(),
            },
            Self::GitRef { entry, git_ref } => EntryDatum::GitRef { entry, git_ref },
            Self::JjChange {
                entry,
                change,
                commits,
                patch,
            } => EntryDatum::JjChange {
                entry,
                change,
                commits,
                patch: patch.as_ref(),
            },
            Self::Session {
                entry,
                session,
                summary,
            } => EntryDatum::Session {
                entry,
                session,
                summary,
            },
            Self::Pull { entry, pull } => EntryDatum::GithubPull { entry, pull },
        }
    }
}

/// The commits of `tip` and not of `base` (a commit key, or none for a range that reaches the
/// root) among those `held`, newest first. Reaches parents only as far as the intxt holds them: a
/// parent it does not hold is outside the range, since `add` writes every commit of the range and
/// the writers' commits documents may also hold other entries' commits.
fn range_commits(
    held: &BTreeMap<String, GitCommit>,
    tip: &str,
    base: Option<&str>,
) -> Vec<CommitDatum> {
    let base = base.map(sha_of);
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from([sha_of(tip)]);
    let mut range = Vec::new();
    while let Some(sha) = queue.pop_front() {
        if Some(sha) == base || !seen.insert(sha) {
            continue;
        }
        let Some(commit) = held.get(sha) else {
            continue;
        };
        queue.extend(commit.parents.iter().map(String::as_str));
        range.push(CommitDatum {
            sha: sha.to_owned(),
            commit: commit.clone(),
        });
    }
    range.sort_by_key(|commit| std::cmp::Reverse(commit.commit.committer.at));
    range
}

impl<H: HttpTransport> IntxtSource for NodeSource<H> {
    async fn load(&self, drawer: &DocId, via: &[MultibaseKey]) -> Result<IntxtView, LoadError> {
        self.load_drawer(drawer, via)
            .await
            .map_err(|failure| match failure {
                Failure::NotFound => LoadError::NotFound(drawer.clone()),
                Failure::Forbidden => LoadError::Forbidden(drawer.clone()),
                Failure::Unreachable(reason) => LoadError::Unreachable {
                    drawer: drawer.clone(),
                    reason,
                },
                Failure::Unreadable(reason) => LoadError::Unreadable {
                    drawer: drawer.clone(),
                    reason,
                },
            })
    }

    fn origin(&self) -> Origin {
        Origin::Node {
            url: self.config.node.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
