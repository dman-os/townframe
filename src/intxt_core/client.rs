//! The client library: what calls a node's cabinet methods on behalf of an operator, shared by
//! the places that do it. `intxt_nodule`'s `createIntxt` and `addToIntxt` handlers run it inside
//! the wasm guest, over the transport the host delivered the request with, presenting the
//! capability their own caller presented; `day x intxt capture` runs it in the CLI process, over
//! the connection to the node (the operator's own authority, so no bearer). Both build a
//! [`Caller`], so there is one way to mint a capability, make a call and name a command.
//!
//! [`Caller::record_observations`] (ADR 039 §4) is the recorder: it turns what one capture hook
//! observed into the datums of ADR 040. It is here, not in a nodule method, so a batch is not
//! bound by a guest's request size limit and the nodule adds no authority of its own: every
//! cabinet call is made with the caller's own authority.

mod record;

use crate::capture::Batch;
use crate::capture::RecordedIds;
use crate::generic;
use crate::methods::AddedToIntxt;
use crate::methods::IntxtError;
use crate::work;
use big_cabinet_core::ActorUrl;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumKinds;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::ListDatums;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::ListDocDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use method_core::Unary;
use serde::Serialize;
use serde_json::Value;
use service_client_core_rs::Client;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::Xrpc;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::xrpc::bearer_header_value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// How many rewrites deep [`Caller::commit_holders`] follows a commit back.
pub const MAX_REWRITE_HOPS: usize = 16;

/// What a caller needs to call cabinet methods on behalf of whoever it presents the authority of:
/// the transport to the node, the capability it presents (none when the node already admits the
/// connection as its operator), and the base of the command ids.
pub struct Caller<H> {
    client: Client<Xrpc<H>>,
    context: RequestContext,
    capability: Option<String>,
    /// The idempotency key, or the request id when the caller sent none (nothing to dedupe
    /// against, so every call is its own).
    command_base: String,
    /// The descendant actor the capabilities it mints are bound to, so its writes are attributed
    /// to that actor; none acts as the actor of the presented authority.
    subject: Option<Uuid7>,
}

impl<H: HttpTransport> Caller<H> {
    /// A caller over `transport`. Each call is the next hop of `context`. `capability` is the
    /// capability id presented as `authorization: Bearer <id>`; `command_base` starts every
    /// command id, so a retry under the same base reuses the same commands and the services that
    /// dedupe by command answer the earlier result.
    pub fn new(
        transport: H,
        context: RequestContext,
        capability: Option<&str>,
        command_base: String,
    ) -> Self {
        Self {
            client: Client::new(Xrpc(transport)),
            context,
            capability: capability.map(str::to_owned),
            command_base,
            subject: None,
        }
    }

    /// The same caller acting as `actor`, a descendant of the presented authority's actor: every
    /// document capability it mints is bound to `actor` (an introduction, `MintCapability`), so
    /// what it writes is attributed to `actor` and what it reads is read as it.
    pub fn acting_as(mut self, actor: Uuid7) -> Self {
        self.subject = Some(actor);
        self
    }

    /// The actor this caller writes as: the subject its document capabilities are bound to.
    pub async fn actor(&self) -> Result<Uuid7, IntxtError> {
        let capability = self.doc_capability(DocScope::default()).await?;
        self.call(ResolveAuthority { capability })
            .await?
            .capability
            .subject
            .ok_or_else(|| {
                IntxtError::Invalid("a document capability is bound to no actor".to_owned())
            })
    }

    /// Records what a capture hook observed in `drawer`, the node's capture drawer, as the
    /// datums of ADR 040, and answers the documents it recorded them in. `machine` is the node
    /// the clones are on (`gitClone.machine`): the host knows it, the recorder does not.
    pub async fn record_observations(
        &self,
        drawer: &DrawerId,
        machine: &ActorUrl,
        batch: &Batch,
    ) -> Result<RecordedIds, IntxtError> {
        record::record_observations(self, drawer, machine, batch).await
    }

    /// The command id of `step` writing `content`: the base, the step's fixed suffix and a short
    /// digest of the request's content (canonical JSON). An identical retry is the same command
    /// and dedupes; a write whose content changed since a partial first attempt is a new command
    /// instead of a `CommandReused` refusal.
    pub fn command(&self, step: &str, content: &impl Serialize) -> Result<CommandId, IntxtError> {
        let digest = blake3::hash(&serde_json::to_vec(content).expect("a request serializes"));
        CommandId::try_from(format!(
            "{}/{step}/{}",
            self.command_base,
            &digest.to_hex().as_str()[..16]
        ))
        .map_err(|error| {
            IntxtError::Invalid(format!("the idempotency key makes no command id: {error}"))
        })
    }

    /// Calls `input` presenting the caller's capability, if it has one, as the authority of the
    /// call.
    pub async fn call<M: Unary>(&self, input: M) -> Result<M::Output, IntxtError> {
        let metadata = HttpMetadata {
            authorization: self.capability.as_deref().map(bearer_header_value),
            idempotency_key: None,
            range: None,
        };
        self.client
            .call_with(self.context.next_hop(), metadata, input)
            .await
            .map_err(|error| IntxtError::Upstream {
                operation: M::XRPC.nsid.to_owned(),
                reason: error.to_string(),
            })
    }

    pub async fn doc_capability(&self, scope: DocScope) -> Result<CapId, IntxtError> {
        Ok(self
            .call(MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: self.subject,
            })
            .await?
            .capability_id)
    }

    /// A capability to write into `doc` the datums at `keys`, limited to those keys' kinds
    /// (ADR 047 §6): a write of any other kind into `doc` is refused at the docs service, so the
    /// handle cannot be spent on more than the write it was minted for.
    pub async fn write_capability<'k>(
        &self,
        doc: &DocId,
        keys: impl IntoIterator<Item = &'k DatumKey>,
    ) -> Result<CapId, IntxtError> {
        self.doc_capability(DocScope {
            write: [(
                doc.clone(),
                DatumKinds::Only(keys.into_iter().map(|key| key.tag().clone()).collect()),
            )]
            .into(),
            ..DocScope::default()
        })
        .await
    }

    /// The heads of `doc` and what each of `keys` reads as at them; every datum when `keys` is
    /// none.
    pub async fn read_datums(
        &self,
        doc: &DocId,
        keys: Option<BTreeSet<DatumKey>>,
    ) -> Result<(Vec<ChangeHash>, BTreeMap<DatumKey, DatumRead>), IntxtError> {
        let capability = self
            .doc_capability(DocScope {
                read: DocScope::read_all([doc.clone()]),
                ..DocScope::default()
            })
            .await?;
        let read = self
            .call(ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: keys,
                optimistic: false,
            })
            .await?;
        Ok((read.heads, read.datums))
    }

    /// The documents in `drawer`, and in the drawers it reaches, that hold the commit `sha` or
    /// any commit it rewrote: an intxt's commits document holds `gitCommit/<sha>` for every
    /// commit the intxt holds, so a hit is membership. `add` also writes
    /// `rewrittenFrom/<new sha>` beside a matched predecessor, and the lookup follows those back
    /// whether or not `sha` itself is held (each hop is the same query on the new slot), so the
    /// answer holds both the intxt of the new commit and the intxt whose work sits on a
    /// predecessor. A chain is followed at most `MAX_REWRITE_HOPS` deep, and a commit met twice
    /// is not asked again, so a cycle ends.
    ///
    /// The query is anchored on the drawer (`And[InDrawer, HasKey]`), which is what lets a
    /// capability of less than node-wide authority ask it, and the caller needs Read on the
    /// drawer.
    pub async fn commit_holders(
        &self,
        drawer: &DrawerId,
        sha: &str,
    ) -> Result<BTreeSet<DocId>, IntxtError> {
        let mut seen = BTreeSet::from([sha.to_owned()]);
        let mut wanted = vec![sha.to_owned()];
        let mut holders = BTreeSet::new();
        for _ in 0..=MAX_REWRITE_HOPS {
            let mut predecessors = Vec::new();
            for sha in &wanted {
                holders.extend(
                    self.key_holders(drawer, key(work::GitCommit::ID, sha)?)
                        .await?,
                );
                let from_key = key(work::RewrittenFrom::ID, sha)?;
                for doc in self.key_holders(drawer, from_key.clone()).await? {
                    // The poll just said the document holds it.
                    let (_, value) = self.try_read(&doc, &from_key).await?;
                    let value = value.ok_or_else(|| {
                        IntxtError::Invalid(format!(
                            "{doc} was found holding {from_key} and does not"
                        ))
                    })?;
                    let from: work::RewrittenFrom =
                        serde_json::from_value(value).map_err(|error| {
                            IntxtError::Invalid(format!("{from_key} in {doc}: {error}"))
                        })?;
                    if seen.insert(from.predecessor.clone()) {
                        predecessors.push(from.predecessor);
                    }
                }
            }
            if predecessors.is_empty() {
                break;
            }
            wanted = predecessors;
        }
        Ok(holders)
    }

    /// The documents in `drawer`, and in the drawers it reaches, that hold the datum `commit`:
    /// a poll from the start with no hold, paged until caught up, asking only for the one key,
    /// so a page carries no values.
    async fn key_holders(
        &self,
        drawer: &DrawerId,
        commit: DatumKey,
    ) -> Result<BTreeSet<DocId>, IntxtError> {
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: drawer.clone(),
                    transitive: true,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasKey {
                    key: commit.clone(),
                },
            ],
        };
        let datums = DatumSelection::Of {
            tags: BTreeSet::new(),
            keys: [commit.clone()].into(),
        };
        let capability = self
            .doc_capability(DocScope {
                drawers: [(drawer.clone(), DrawerAccess::Read)].into(),
                ..DocScope::default()
            })
            .await?;
        let mut holders = BTreeSet::new();
        let mut after = 0;
        loop {
            let page = self
                .call(PollDatums {
                    capability,
                    predicate: predicate.clone(),
                    datums: datums.clone(),
                    after,
                    limit: 0,
                    hold_ms: 0,
                })
                .await?;
            // Events are in revision order, so the last word per document stands: a removed row
            // is a commit taken out again, and a `leave` is the document moving out of the
            // result.
            for event in page.events {
                match event {
                    DatumEvent::Datum {
                        doc_id,
                        datum: ObservedDatum::Present { .. },
                        ..
                    } => {
                        holders.insert(doc_id);
                    }
                    DatumEvent::Datum {
                        doc_id,
                        datum: ObservedDatum::Removed { .. },
                        ..
                    }
                    | DatumEvent::Leave { doc_id, .. } => {
                        holders.remove(&doc_id);
                    }
                }
            }
            if page.caught_up {
                return Ok(holders);
            }
            if page.through <= after {
                return Err(IntxtError::Invalid(format!(
                    "the poll for {commit} in {drawer} is not caught up and made no progress past {after}"
                )));
            }
            after = page.through;
        }
    }

    /// The keys of every datum `doc` holds at its current heads, without their values.
    pub async fn datum_keys(&self, doc: &DocId) -> Result<BTreeSet<DatumKey>, IntxtError> {
        let capability = self
            .doc_capability(DocScope {
                read: DocScope::read_all([doc.clone()]),
                ..DocScope::default()
            })
            .await?;
        Ok(self
            .call(ListDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                with_authors: false,
            })
            .await?
            .datums
            .into_keys()
            .collect())
    }

    /// The document's heads and the accepted value of `key` in it at those heads; none when the
    /// datum is absent.
    pub async fn try_read(
        &self,
        doc: &DocId,
        key: &DatumKey,
    ) -> Result<(Vec<ChangeHash>, Option<Value>), IntxtError> {
        let (heads, mut datums) = self.read_datums(doc, Some([key.clone()].into())).await?;
        match datums.remove(key) {
            Some(DatumRead::Accepted { value, .. }) => Ok((heads, Some(value))),
            Some(DatumRead::Absent { .. }) | None => Ok((heads, None)),
            other => Err(IntxtError::Invalid(format!(
                "{key} in {doc} is not readable: {other:?}"
            ))),
        }
    }

    /// [`Self::try_read`] for a datum that must be there.
    pub async fn read(
        &self,
        doc: &DocId,
        key: &DatumKey,
    ) -> Result<(Vec<ChangeHash>, Value), IntxtError> {
        match self.try_read(doc, key).await? {
            (heads, Some(value)) => Ok((heads, value)),
            (_, None) => Err(IntxtError::Invalid(format!(
                "{key} in {doc} is not readable: absent"
            ))),
        }
    }

    /// Writes `set` into and removes `remove` from `doc`, on its current heads. For documents
    /// whose slots have one writer each or merge by themselves, such as a notes document's
    /// comments (one new key each) and seen records (one per actor).
    pub async fn write_datums(
        &self,
        doc: &DocId,
        set: BTreeMap<DatumKey, Value>,
        remove: BTreeSet<DatumKey>,
    ) -> Result<(), IntxtError> {
        let capability = self
            .write_capability(doc, set.keys().chain(&remove))
            .await?;
        self.call(WriteDatums {
            capability,
            command: Some(self.command("write-datums", &(doc, &set, &remove))?),
            doc_id: doc.clone(),
            mode: WriteMode::AtCurrent,
            set,
            remove,
        })
        .await?;
        Ok(())
    }

    /// Writes `set` into the intxt's descriptor document and appends `entries`, which `set`
    /// holds, to its body in the same write, based on the heads the body was read at, so two
    /// writers adding different entries both land in the body's list. Each answer says whether
    /// its entry was new to the body; an entry already listed is written again with the value
    /// given and answers the same reference.
    pub async fn include(
        &self,
        intxt: &DrawerId,
        mut set: BTreeMap<DatumKey, Value>,
        entries: &[DatumKey],
    ) -> Result<Vec<Included>, IntxtError> {
        let body_key = key(generic::Body::ID, "main")?;
        let (body_heads, body_value) = self.read(intxt, &body_key).await?;
        let mut body: generic::Body = serde_json::from_value(body_value)
            .map_err(|error| IntxtError::Invalid(format!("{intxt} has no valid body: {error}")))?;
        let mut included = Vec::with_capacity(entries.len());
        for entry in entries {
            let entry_ref = DatumReference::new(ReferenceDocId::SelfDoc, entry.clone());
            let added = !body.order.contains(&entry_ref);
            if added {
                body.order.push(entry_ref);
            }
            included.push(Included {
                entry: DatumReference::new(ReferenceDocId::Concrete(intxt.clone()), entry.clone()),
                added,
            });
        }
        set.insert(body_key, to_value(&body));
        let descriptor = self.write_capability(intxt, set.keys()).await?;
        self.call(WriteDatums {
            capability: descriptor,
            command: Some(self.command("write-entry", &(&set, &body_heads))?),
            doc_id: intxt.clone(),
            mode: WriteMode::AtHeads { heads: body_heads },
            set,
            remove: Default::default(),
        })
        .await?;
        Ok(included)
    }

    /// Removes `entries` from the intxt's body and deletes their datums, with `also` (datums only
    /// they referenced, such as a pack's blob datum), in one write based on the heads the body
    /// was read at. Documents lent to the intxt stay lent.
    pub async fn remove_entries(
        &self,
        intxt: &DrawerId,
        entries: &BTreeSet<DatumKey>,
        also: &BTreeSet<DatumKey>,
    ) -> Result<(), IntxtError> {
        let body_key = key(generic::Body::ID, "main")?;
        let (body_heads, body_value) = self.read(intxt, &body_key).await?;
        let mut body: generic::Body = serde_json::from_value(body_value)
            .map_err(|error| IntxtError::Invalid(format!("{intxt} has no valid body: {error}")))?;
        body.order
            .retain(|reference| !entries.contains(reference.key()));
        let set: BTreeMap<DatumKey, Value> = [(body_key, to_value(&body))].into();
        let remove: BTreeSet<DatumKey> = entries.union(also).cloned().collect();
        let descriptor = self
            .write_capability(intxt, set.keys().chain(&remove))
            .await?;
        self.call(WriteDatums {
            capability: descriptor,
            command: Some(self.command("remove-entries", &(&set, &remove, &body_heads))?),
            doc_id: intxt.clone(),
            mode: WriteMode::AtHeads { heads: body_heads },
            set,
            remove,
        })
        .await?;
        Ok(())
    }

    /// Enters `doc` in the intxt's drawer at `access`: Read for a document the intxt only shows
    /// (everyone the intxt is shared with can read it), Write for one its writers add to as well
    /// (a commits document shared between related intxts). The drawer it is lent from is the
    /// first, ordered by drawer id, that the caller reaches it through at `access` or above,
    /// other than the intxt's own: the drawers protocol does not say which of them are the
    /// node's local ones, so none is preferred. `AddDoc` admits the entry when the caller holds
    /// Write on the intxt's drawer and the reach handle, minted on that lending drawer at
    /// `access`, reaches the doc at the level it confers.
    pub async fn share(
        &self,
        intxt: &DrawerId,
        doc: &DocId,
        access: DrawerAccess,
    ) -> Result<(), IntxtError> {
        let from = self
            .call(ListDocDrawers {
                doc_id: doc.clone(),
            })
            .await?
            .drawers
            .into_iter()
            .find(|drawer| drawer.access >= access && &drawer.drawer_id != intxt)
            .ok_or_else(|| IntxtError::NotReachable(doc.clone()))?
            .drawer_id;
        let drawers = self
            .call(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(intxt.clone(), DrawerAccess::Write), (from, access)].into(),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id;
        self.call(AddDoc {
            capability: drawers,
            reach: drawers,
            command: self.command("add-doc", &(intxt, doc, access))?,
            drawer_id: intxt.clone(),
            doc_id: doc.clone(),
            access,
        })
        .await?;
        Ok(())
    }

    /// Includes a part (a worktree, a git repository, an agent session or a GitHub pull request datum, named by its
    /// document id, not `self`) in an intxt: writes the entry for it, appends the entry to the
    /// body, and shares the part's document. The entry's slot is the part's own identity (the
    /// worktree's UUID, the repository document, the harness's session id), so two writers
    /// adding the same part write one entry.
    pub async fn add_part(
        &self,
        intxt: &DrawerId,
        part: &DatumReference,
    ) -> Result<Included, IntxtError> {
        let ReferenceDocId::Concrete(part_doc) = part.document() else {
            return Err(IntxtError::Invalid(
                "a part is named by its document id, not `self`".to_owned(),
            ));
        };
        let (_, part_value) = self.read(part_doc, part.key()).await?;
        let tag = part.key().tag().to_string();
        let malformed = |error: serde_json::Error| {
            IntxtError::Invalid(format!("{part} is not a valid `{tag}`: {error}"))
        };
        let (entry_key, entry) = if tag == work::Worktree::ID {
            let worktree: work::Worktree = serde_json::from_value(part_value).map_err(malformed)?;
            (
                key(work::WorktreeEntry::ID, &worktree.id)?,
                to_value(&work::WorktreeEntry {
                    worktree: part.clone(),
                    extra: Default::default(),
                }),
            )
        } else if tag == work::GitRepo::ID {
            (
                // A fork and its upstream share a root commit, so the entry is keyed by the
                // repository's document, not by the `gitRepo` slot (ADR 040 §8).
                key(work::RepoEntry::ID, &part_doc.to_string())?,
                to_value(&work::RepoEntry {
                    repo: part.clone(),
                    extra: Default::default(),
                }),
            )
        } else if tag == work::AgentSession::ID {
            let session: work::AgentSession =
                serde_json::from_value(part_value).map_err(malformed)?;
            (
                key(work::SessionEntry::ID, &session.session_id)?,
                to_value(&work::SessionEntry {
                    session: part.clone(),
                    extra: Default::default(),
                }),
            )
        } else if tag == work::GithubPull::ID {
            (
                // The same pull request has one entry whoever adds it: its slot is the pull
                // request's own, `<owner>/<repo>/<number>`.
                key(
                    work::GithubPullEntry::ID,
                    &String::from_utf8_lossy(part.key().slot_bytes()),
                )?,
                to_value(&work::GithubPullEntry {
                    pull: part.clone(),
                    extra: Default::default(),
                }),
            )
        } else {
            return Err(IntxtError::Invalid(format!(
                "`{tag}` is not a part an intxt includes: name a worktree, gitRepo, agentSession or githubPull datum"
            )));
        };
        let [included] = self
            .include(
                intxt,
                [(entry_key.clone(), entry)].into(),
                std::slice::from_ref(&entry_key),
            )
            .await?
            .try_into()
            .expect("one entry was included");
        self.share(intxt, part_doc, DrawerAccess::Read).await?;
        Ok(included)
    }
}

/// What [`Caller::include`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Included {
    /// The entry, in the intxt's descriptor document.
    pub entry: DatumReference,
    /// The entry was new to the intxt's body.
    pub added: bool,
}

impl From<Included> for AddedToIntxt {
    fn from(included: Included) -> Self {
        Self {
            entry: included.entry,
        }
    }
}

/// The datum key `tag/slot`.
pub fn key(tag: &str, slot: &str) -> Result<DatumKey, IntxtError> {
    DatumKey::new(tag, slot.as_bytes())
        .map_err(|error| IntxtError::Invalid(format!("{tag}/{slot}: {error}")))
}

/// `value` as the JSON a datum holds.
pub fn to_value(value: &impl Serialize) -> Value {
    serde_json::to_value(value).expect("a record serializes")
}
