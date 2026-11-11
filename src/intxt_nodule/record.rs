//! `recordObservations` (ADR 039 §4): turns what one capture hook observed into the datums of
//! ADR 040.
//!
//! The host calls it once per hook, holding every touched worktree's lock, so the calls for one
//! worktree arrive one at a time and its op log stays single-writer. Every document is written
//! at most once per call (`writeDatums` carries all of a document's datums), and every request is
//! a function of the batch and the documents it names, with `AtCurrent` writes, so a retried
//! call under the same idempotency key replays the same commands. A retry after a complete
//! earlier success is answered without writing: the worktree datum is written last, and the
//! batch counts as recorded when that datum lists its last op and every path it changed.
//!
//! Order within a worktree: identity documents, file documents, git layer, op logs, the
//! worktree datum. The session is recorded before the worktrees, since an op's `cause` names a
//! turn of the session document.

use crate::Caller;
use crate::to_value;
use big_cabinet_core::ActorUrl;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::drawer_placement;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::Location;
use big_cabinet_core::reference::ReferenceDocId;
use big_cabinet_core::reference::head_text;
use intxt_core::capture::Batch;
use intxt_core::capture::Cause;
use intxt_core::capture::ChangeKind;
use intxt_core::capture::Content;
use intxt_core::capture::ContentClass;
use intxt_core::capture::Imported;
use intxt_core::capture::RecordedIds;
use intxt_core::capture::RecordedWorktree;
use intxt_core::capture::WorktreeBatch;
use intxt_core::capture::WorktreeDocs;
use intxt_core::generic;
use intxt_core::methods::IntxtError;
use intxt_core::methods::RecordObservations;
use intxt_core::work;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// The tag of the blob datum a file document holds for a blob-classified file (ledger D45).
const BLOB_TAG: &str = "org.example.daybook.blob";

/// Ops per op-log document (ADR 040 §3).
const OPS_PER_LOG: u64 = 256;

fn invalid(reason: impl Into<String>) -> IntxtError {
    IntxtError::Invalid(reason.into())
}

fn doc_id(text: &str) -> Result<DocId, IntxtError> {
    text.parse()
        .map_err(|error| invalid(format!("`{text}` is not a document id: {error}")))
}

fn key(tag: &str, slot: &str) -> Result<DatumKey, IntxtError> {
    DatumKey::new(tag, slot.as_bytes()).map_err(|error| invalid(format!("{tag}/{slot}: {error}")))
}

fn reference(doc: &DocId, tag: &str, slot: &str) -> Result<DatumReference, IntxtError> {
    Ok(DatumReference::new(
        ReferenceDocId::Concrete(doc.clone()),
        key(tag, slot)?,
    ))
}

/// `value` at `slot` of record `tag`; the key names the type, so the value carries no tag.
fn entry(value: Value, tag: &str, slot: &str) -> Result<(DatumKey, Value), IntxtError> {
    Ok((key(tag, slot)?, value))
}

/// `text` with every byte outside a URL fragment's unreserved characters (RFC 3986's `pchar`
/// without the sub-delimiters) percent-encoded.
fn percent_encode(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b':' | b'@' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// The media type a file document's `note` states, from the path's extension.
fn mime_of(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("rs") => "text/x-rust",
        Some("md") => "text/markdown",
        Some("toml") => "application/toml",
        Some("json") => "application/json",
        Some("ts") => "text/typescript",
        Some("kt") => "text/x-kotlin",
        Some("nix") => "text/x-nix",
        _ => "text/plain",
    }
}

/// The session documents a batch recorded, to name a cause's turn.
struct SessionDocs {
    main: Option<DocId>,
    subagents: BTreeMap<String, DocId>,
}

impl SessionDocs {
    /// The tool call a cause names, as a location in its session document: `toolCalls[id]` of
    /// `promptTurn/<turn>`. None when the transcript did not show the call's turn yet.
    fn cause(&self, cause: &Cause) -> Result<Option<Location>, IntxtError> {
        let doc = match &cause.agent_id {
            Some(agent) => self.subagents.get(agent),
            None => self.main.as_ref(),
        };
        let (Some(doc), Some(turn)) = (doc, &cause.turn) else {
            return Ok(None);
        };
        let reference = reference(doc, work::PromptTurn::ID, turn)?;
        let fragment = format!(
            "toolCalls/{}",
            percent_encode(&format!("{{\"id\":\"{}\"}}", cause.tool_use_id))
        );
        Location::new(reference, Some(fragment))
            .map(Some)
            .map_err(|error| invalid(format!("the cause of {}: {error}", cause.tool_use_id)))
    }
}

/// A file document to write: its worktree path and the state to bring it to.
struct PendingFile {
    /// The document, when the path already has one (an edit, or a rename's old document).
    doc: Option<DocId>,
    /// Whether the path is new to the document: a rename moved it, or the document is new.
    path_changed: bool,
    content: Option<Content>,
}

struct Recorder<'a> {
    caller: &'a Caller<'a>,
    placement: String,
    create_capability: CapId,
    machine: &'a ActorUrl,
}

impl Recorder<'_> {
    async fn create(
        &self,
        step: &str,
        datums: BTreeMap<DatumKey, Value>,
    ) -> Result<WriteReceipt, IntxtError> {
        self.caller
            .call(CreateDoc {
                capability: self.create_capability,
                command: self.caller.command(step, &datums)?,
                placement: self.placement.clone(),
                datums,
            })
            .await
    }

    /// One `writeDatums` to `doc` at its current heads: documents here have one writer.
    async fn write(
        &self,
        step: &str,
        doc: &DocId,
        set: BTreeMap<DatumKey, Value>,
    ) -> Result<WriteReceipt, IntxtError> {
        self.write_removing(step, doc, set, BTreeSet::new()).await
    }

    /// [`Self::write`], removing `remove` in the same write.
    async fn write_removing(
        &self,
        step: &str,
        doc: &DocId,
        set: BTreeMap<DatumKey, Value>,
        remove: BTreeSet<DatumKey>,
    ) -> Result<WriteReceipt, IntxtError> {
        let capability = self
            .caller
            .doc_capability(DocScope {
                write: [doc.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        self.caller
            .call(WriteDatums {
                capability,
                command: Some(self.caller.command(step, &(&set, &remove))?),
                doc_id: doc.clone(),
                mode: WriteMode::AtCurrent,
                set,
                remove,
            })
            .await
    }

    /// The accepted values of `keys` in `doc` now; a key the document lacks is absent.
    async fn read(
        &self,
        doc: &DocId,
        keys: &[DatumKey],
    ) -> Result<BTreeMap<DatumKey, Value>, IntxtError> {
        let capability = self
            .caller
            .doc_capability(DocScope {
                read: [doc.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let read = self
            .caller
            .call(ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: Some(keys.iter().cloned().collect()),
                optimistic: false,
            })
            .await?;
        let mut present = BTreeMap::new();
        for (key, read) in read.datums {
            match read {
                DatumRead::Accepted { value, .. } => {
                    present.insert(key, value);
                }
                DatumRead::Absent { .. } => {}
                other => {
                    return Err(invalid(format!(
                        "{key} in {doc} is not readable: {other:?}"
                    )));
                }
            }
        }
        Ok(present)
    }

    /// Writes the datums of one transcript import into its session document, creating the
    /// document when the host holds none. None when there is nothing to record.
    async fn session_doc(
        &self,
        step: &str,
        doc: Option<&str>,
        imported: &Imported,
    ) -> Result<Option<DocId>, IntxtError> {
        let existing = doc.map(doc_id).transpose()?;
        if imported.is_empty() {
            return Ok(existing);
        }
        let mut set = BTreeMap::new();
        if let Some(session) = &imported.session {
            set.extend([entry(to_value(session), work::AgentSession::ID, "main")?]);
        }
        for (turn, value) in &imported.turns {
            set.extend([entry(to_value(value), work::PromptTurn::ID, turn)?]);
        }
        if let Some(plan) = &imported.plan {
            set.extend([entry(to_value(plan), work::Plan::ID, "main")?]);
        }
        Ok(Some(match existing {
            Some(doc) => {
                self.write(step, &doc, set).await?;
                doc
            }
            None => self.create(step, set).await?.doc_id,
        }))
    }

    async fn record_session(&self, batch: &Batch) -> Result<SessionDocs, IntxtError> {
        let session = &batch.session;
        let main = self
            .session_doc("session", session.doc.as_deref(), &session.imported)
            .await?;
        let mut subagents = BTreeMap::new();
        for agent in &session.subagents {
            let step = format!("agent-{}", agent.agent_id);
            if let Some(doc) = self
                .session_doc(&step, agent.doc.as_deref(), &agent.imported)
                .await?
            {
                subagents.insert(agent.agent_id.clone(), doc);
            }
        }
        Ok(SessionDocs { main, subagents })
    }
}

/// What the worktree's own datum says, and what a call changes in it.
struct WorktreeState {
    doc: DocId,
    datum: work::Worktree,
    changed: bool,
}

impl Recorder<'_> {
    /// The repository, clone and worktree documents, created when missing. The repository's
    /// `gitRepo` slot is its smallest root commit, which every clone of the project on every
    /// machine derives alike (ledger D13); a repository with no commit yet has none, so it has no
    /// repository document until it has one, and the clone's `repo` is set then.
    async fn identity_docs(
        &self,
        worktree: &WorktreeBatch,
    ) -> Result<(Option<DocId>, DocId, WorktreeState), IntxtError> {
        let identity = &worktree.identity;
        let step = |what: &str| format!("{}/{what}", identity.worktree_id);
        let root = identity.roots.iter().min();
        let repo = match (&worktree.docs.repo, root) {
            (Some(doc), _) => Some(doc_id(doc)?),
            (None, Some(root)) => {
                let repo = work::GitRepo {
                    roots: identity.roots.clone(),
                    remotes: identity.remotes.clone(),
                    extra: Default::default(),
                };
                Some(
                    self.create(
                        &step("repo"),
                        [entry(to_value(&repo), work::GitRepo::ID, root)?].into(),
                    )
                    .await?
                    .doc_id,
                )
            }
            (None, None) => None,
        };
        let repo_ref = match (&repo, root) {
            (Some(doc), Some(root)) => Some(reference(doc, work::GitRepo::ID, root)?),
            (None, _) => None,
            (Some(_), None) => return Err(invalid("a repository document without a root commit")),
        };
        let clone_datum = || {
            entry(
                to_value(&work::GitClone {
                    repo: repo_ref.clone(),
                    machine: self.machine.clone(),
                    path: identity.clone_path.clone(),
                    vcs: vec![work::Vcs::Git],
                    extra: Default::default(),
                }),
                work::GitClone::ID,
                &identity.clone_id,
            )
        };
        let clone = match &worktree.docs.clone {
            Some(doc) => {
                let doc = doc_id(doc)?;
                if worktree.docs.repo.is_none() && repo.is_some() {
                    // The repository document is new: the clone points at it from now on.
                    self.write(&step("clone-repo"), &doc, [clone_datum()?].into())
                        .await?;
                }
                doc
            }
            None => {
                self.create(&step("clone"), [clone_datum()?].into())
                    .await?
                    .doc_id
            }
        };
        let worktree_key = key(work::Worktree::ID, "main")?;
        let state = match &worktree.docs.worktree {
            Some(doc) => {
                let doc = doc_id(doc)?;
                let mut read = self.read(&doc, std::slice::from_ref(&worktree_key)).await?;
                let value = read
                    .remove(&worktree_key)
                    .ok_or_else(|| invalid(format!("{doc} holds no worktree datum")))?;
                let datum = serde_json::from_value(value)
                    .map_err(|error| invalid(format!("the worktree datum of {doc}: {error}")))?;
                WorktreeState {
                    doc,
                    datum,
                    changed: false,
                }
            }
            None => {
                let datum = work::Worktree {
                    clone: reference(&clone, work::GitClone::ID, &identity.clone_id)?,
                    id: identity.worktree_id.clone(),
                    path: identity.root.clone(),
                    files: BTreeMap::new(),
                    op_logs: Vec::new(),
                    extra: Default::default(),
                };
                let doc = self
                    .create(&step("worktree"), [(worktree_key, to_value(&datum))].into())
                    .await?
                    .doc_id;
                WorktreeState {
                    doc,
                    datum,
                    changed: false,
                }
            }
        };
        Ok((repo, clone, state))
    }

    /// Whether an earlier call already recorded this batch completely: the worktree datum, which
    /// a call writes last, lists the op log holding the batch's last op, the op is there, and
    /// every path the batch changed has a document.
    async fn already_recorded(
        &self,
        worktree: &WorktreeBatch,
        state: &WorktreeState,
    ) -> Result<bool, IntxtError> {
        if worktree.ops.is_empty() {
            return Ok(false);
        }
        let last = worktree.next_seq + worktree.ops.len() as u64 - 1;
        let paths_have_docs = worktree
            .ops
            .iter()
            .flat_map(|op| &op.changes)
            .filter(|change| change.kind != ChangeKind::Deleted)
            .all(|change| state.datum.files.contains_key(&change.path));
        let Some(log) = state
            .datum
            .op_logs
            .iter()
            .rfind(|log| log.from <= last)
            .filter(|_| paths_have_docs)
        else {
            return Ok(false);
        };
        let op = key(work::WorktreeOp::ID, &last.to_string())?;
        Ok(self
            .read(&doc_id(&log.doc)?, std::slice::from_ref(&op))
            .await?
            .contains_key(&op))
    }

    /// Brings the file documents of the batch up to date and answers the heads each path's
    /// document has afterwards. A path touched by several ops of the batch is written once, at
    /// its last state, so its ops all name those heads.
    async fn write_files(
        &self,
        worktree: &WorktreeBatch,
        state: &mut WorktreeState,
    ) -> Result<BTreeMap<String, Vec<String>>, IntxtError> {
        let mut pending: BTreeMap<String, PendingFile> = BTreeMap::new();
        for change in worktree.ops.iter().flat_map(|op| &op.changes) {
            let path = &change.path;
            match change.kind {
                ChangeKind::Deleted => {
                    pending.remove(path);
                    continue;
                }
                ChangeKind::Renamed => {
                    // The old path's document continues under the new path.
                    let old = change.from.as_ref().and_then(|from| {
                        pending.remove(from).and_then(|file| file.doc).or_else(|| {
                            state
                                .datum
                                .files
                                .remove(from)
                                .and_then(|doc| doc.parse::<DocId>().ok())
                        })
                    });
                    state.changed |= old.is_some();
                    pending.insert(
                        path.clone(),
                        PendingFile {
                            path_changed: true,
                            doc: old,
                            content: change.content.clone(),
                        },
                    );
                    continue;
                }
                _ => {}
            }
            let doc = match pending.get(path).and_then(|file| file.doc.clone()) {
                Some(doc) => Some(doc),
                None => state
                    .datum
                    .files
                    .get(path)
                    .map(|doc| doc_id(doc))
                    .transpose()?,
            };
            pending.insert(
                path.clone(),
                PendingFile {
                    path_changed: doc.is_none(),
                    doc,
                    content: change.content.clone(),
                },
            );
        }

        let worktree_ref = reference(&state.doc, work::Worktree::ID, "main")?;
        let mut heads = BTreeMap::new();
        for (index, (path, file)) in pending.into_iter().enumerate() {
            let step = format!("{}/file-{index}", worktree.identity.worktree_id);
            let Some(receipt) = self.write_file(&step, &path, file, &worktree_ref).await? else {
                continue;
            };
            state
                .datum
                .files
                .insert(path.clone(), receipt.doc_id.to_string());
            state.changed = true;
            heads.insert(path, receipt.heads.iter().map(head_text).collect());
        }
        Ok(heads)
    }

    /// Writes one file document: created with its `path`, content and `file` provenance, or, for
    /// an existing one, only what changed. Content is text (a `note`) or a blob (an
    /// `org.example.daybook.blob` datum that `file.blob` references); when a path switches from
    /// one to the other the old one is removed in the same write. A blob the host did not
    /// upload writes no content. None when there is nothing to write.
    async fn write_file(
        &self,
        step: &str,
        path: &str,
        file: PendingFile,
        worktree_ref: &DatumReference,
    ) -> Result<Option<WriteReceipt>, IntxtError> {
        let note_key = key(generic::Note::ID, "main")?;
        let blob_key = key(BLOB_TAG, "main")?;
        let (had_note, had_blob) = match &file.doc {
            Some(doc) => {
                let present = self
                    .read(doc, &[note_key.clone(), blob_key.clone()])
                    .await?;
                (
                    present.contains_key(&note_key),
                    present.contains_key(&blob_key),
                )
            }
            None => (false, false),
        };
        let text = file.content.as_ref().and_then(text_of);
        let blob = file
            .content
            .as_ref()
            .filter(|content| content.class == ContentClass::Blob)
            .and_then(|content| content.blob.as_ref());
        let mut set = BTreeMap::new();
        let mut remove = BTreeSet::new();
        if file.path_changed {
            set.extend([entry(
                to_value(&generic::PathName {
                    path: path.to_owned(),
                    extra: Default::default(),
                }),
                generic::PathName::ID,
                "main",
            )?]);
        }
        let has_blob = match (&text, blob) {
            (Some(text), _) => {
                set.extend([entry(
                    to_value(&generic::Note {
                        mime: mime_of(path).to_owned(),
                        content: text.clone(),
                        extra: Default::default(),
                    }),
                    generic::Note::ID,
                    "main",
                )?]);
                if had_blob {
                    remove.insert(blob_key.clone());
                }
                false
            }
            (None, Some(blob)) => {
                set.extend([(
                    blob_key.clone(),
                    json!({
                        "mime": blob.mime,
                        "digest": blob.digest,
                        "lengthOctets": blob.length_octets,
                    }),
                )]);
                if had_note {
                    remove.insert(note_key.clone());
                }
                true
            }
            (None, None) => had_blob,
        };
        // `file` states whether the document has a blob: written new, and whenever that changes.
        if file.doc.is_none() || has_blob != had_blob {
            let blob_ref =
                has_blob.then(|| DatumReference::new(ReferenceDocId::SelfDoc, blob_key.clone()));
            set.extend([entry(
                to_value(&work::File {
                    worktree: worktree_ref.clone(),
                    blob: blob_ref,
                    extra: Default::default(),
                }),
                work::File::ID,
                "main",
            )?]);
        }
        match file.doc {
            Some(_) if set.is_empty() && remove.is_empty() => Ok(None),
            Some(doc) => Ok(Some(self.write_removing(step, &doc, set, remove).await?)),
            None => Ok(Some(self.create(step, set).await?)),
        }
    }

    /// The batch's commits into the repository document (a recorded commit is never rewritten)
    /// and its refs into the clone's (ADR 040 §7).
    async fn write_git(
        &self,
        worktree: &WorktreeBatch,
        repo: Option<&DocId>,
        clone: &DocId,
    ) -> Result<(), IntxtError> {
        let step = |what: &str| format!("{}/{what}", worktree.identity.worktree_id);
        let commits = worktree
            .commits
            .iter()
            .map(|(sha, commit)| Ok((key(work::GitCommit::ID, sha)?, to_value(commit))))
            .collect::<Result<BTreeMap<_, _>, IntxtError>>()?;
        if !commits.is_empty() {
            let repo = repo.ok_or_else(|| {
                invalid("commits were observed in a repository without a root commit")
            })?;
            let known = self
                .read(repo, &commits.keys().cloned().collect::<Vec<_>>())
                .await?;
            let new: BTreeMap<_, _> = commits
                .into_iter()
                .filter(|(key, _)| !known.contains_key(key))
                .collect();
            if !new.is_empty() {
                self.write(&step("commits"), repo, new).await?;
            }
        }
        let refs = worktree
            .refs
            .iter()
            .map(|(name, reference)| {
                Ok((
                    key(
                        work::GitRef::ID,
                        &format!("{}/{name}", worktree.identity.clone_id),
                    )?,
                    to_value(reference),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, IntxtError>>()?;
        if !refs.is_empty() {
            let known = self
                .read(clone, &refs.keys().cloned().collect::<Vec<_>>())
                .await?;
            let moved: BTreeMap<_, _> = refs
                .into_iter()
                .filter(|(key, value)| known.get(key) != Some(value))
                .collect();
            if !moved.is_empty() {
                self.write(&step("refs"), clone, moved).await?;
            }
        }
        Ok(())
    }

    /// Writes the batch's ops as `worktreeOp/<seq>` from its next sequence number, a new op-log
    /// document opened for each run of `OPS_PER_LOG` ops, and lists the new documents in the
    /// worktree datum.
    async fn write_ops(
        &self,
        worktree: &WorktreeBatch,
        state: &mut WorktreeState,
        sessions: &SessionDocs,
        heads: &BTreeMap<String, Vec<String>>,
    ) -> Result<(), IntxtError> {
        let mut by_log: BTreeMap<u64, BTreeMap<DatumKey, Value>> = BTreeMap::new();
        let mut first_seq: BTreeMap<u64, u64> = BTreeMap::new();
        for (index, op) in worktree.ops.iter().enumerate() {
            let seq = worktree.next_seq + index as u64;
            let mut files = BTreeMap::new();
            let mut deleted = Vec::new();
            let mut renames = BTreeMap::new();
            for change in &op.changes {
                match change.kind {
                    ChangeKind::Deleted => deleted.push(change.path.clone()),
                    ChangeKind::Renamed => {
                        if let Some(from) = &change.from {
                            renames.insert(change.path.clone(), from.clone());
                        }
                    }
                    _ => {}
                }
                if let Some(heads) = heads.get(&change.path) {
                    files.insert(change.path.clone(), heads.clone());
                }
            }
            let value = work::WorktreeOp {
                at: op.at,
                kind: op.kind.clone(),
                cause: op
                    .cause
                    .as_ref()
                    .map(|cause| sessions.cause(cause))
                    .transpose()?
                    .flatten(),
                files,
                deleted,
                renames,
                git: op.git.clone(),
                jj: None,
                extra: Default::default(),
            };
            let log = seq / OPS_PER_LOG;
            first_seq.entry(log).or_insert(seq);
            by_log.entry(log).or_default().extend([entry(
                to_value(&value),
                work::WorktreeOp::ID,
                &seq.to_string(),
            )?]);
        }
        for (log, ops) in by_log {
            let from = first_seq[&log];
            let step = format!("{}/oplog-{log}", worktree.identity.worktree_id);
            let existing = state
                .datum
                .op_logs
                .last()
                .filter(|last| last.from / OPS_PER_LOG == log)
                .map(|last| doc_id(&last.doc))
                .transpose()?;
            match existing {
                Some(doc) => {
                    self.write(&step, &doc, ops).await?;
                }
                None => {
                    let doc = self.create(&step, ops).await?.doc_id;
                    state.datum.op_logs.push(work::OpLogDoc {
                        doc: doc.to_string(),
                        from,
                        extra: Default::default(),
                    });
                    state.changed = true;
                }
            }
        }
        Ok(())
    }

    async fn record_worktree(
        &self,
        worktree: &WorktreeBatch,
        sessions: &SessionDocs,
    ) -> Result<RecordedWorktree, IntxtError> {
        let (repo, clone, mut state) = self.identity_docs(worktree).await?;
        let docs = WorktreeDocs {
            worktree: Some(state.doc.to_string()),
            clone: Some(clone.to_string()),
            repo: repo.as_ref().map(ToString::to_string),
            extra: Default::default(),
        };
        let recorded = RecordedWorktree {
            docs,
            next_seq: worktree.next_seq + worktree.ops.len() as u64,
            extra: Default::default(),
        };
        if self.already_recorded(worktree, &state).await? {
            return Ok(recorded);
        }
        let heads = self.write_files(worktree, &mut state).await?;
        self.write_git(worktree, repo.as_ref(), &clone).await?;
        self.write_ops(worktree, &mut state, sessions, &heads)
            .await?;
        if state.changed {
            self.write(
                &format!("{}/worktree", worktree.identity.worktree_id),
                &state.doc,
                [entry(to_value(&state.datum), work::Worktree::ID, "main")?].into(),
            )
            .await?;
        }
        Ok(recorded)
    }
}

fn text_of(content: &intxt_core::capture::Content) -> Option<String> {
    (content.class == ContentClass::Text)
        .then(|| content.text.clone())
        .flatten()
}

impl big_ndk::Handler<RecordObservations> for crate::Intxt {
    async fn call(
        &self,
        cx: &big_ndk::Cx,
        input: RecordObservations,
    ) -> Result<RecordedIds, IntxtError> {
        let caller = Caller::new(cx)?;
        let placement = drawer_placement(&input.drawer);
        let create_capability = caller
            .doc_capability(DocScope {
                create: [placement.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let recorder = Recorder {
            caller: &caller,
            placement,
            create_capability,
            machine: &input.machine,
        };
        let sessions = recorder.record_session(&input.batch).await?;
        let mut worktrees = BTreeMap::new();
        for worktree in &input.batch.worktrees {
            worktrees.insert(
                worktree.identity.root.clone(),
                recorder.record_worktree(worktree, &sessions).await?,
            );
        }
        Ok(RecordedIds {
            session: sessions.main.map(|doc| doc.to_string()),
            subagents: sessions
                .subagents
                .into_iter()
                .map(|(agent, doc)| (agent, doc.to_string()))
                .collect(),
            worktrees,
            extra: Default::default(),
        })
    }
}
