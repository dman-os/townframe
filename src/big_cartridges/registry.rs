//! The registry's state: the cartridges this node serves, derived from the registry drawer.
//!
//! Nothing here is authoritative except the drawer. A scan of the drawer (a `PollDatums` from
//! cursor 0, then the same poll held for changes) selects every document that carries a cartridge
//! identity datum, and each such document is folded into one [`DocView`]. A node that syncs the
//! drawer therefore discovers the same cartridges, and the whole state is rebuilt on every boot.
//!
//! The one thing the drawer cannot say is what a version label meant *first*. That is the node's
//! own record ([`FirstSeen`], in SQLite), taken the first time a label is read from the drawer and
//! never replaced. Every fold compares the drawer's datum against it: the same revision is the
//! version, a different one means another node rewrote the datum, and the version is then served
//! from the first-seen envelope and surfaced as conflicted. That detection runs on every read of
//! the document, so it is the same whether the change was synced in or found at boot.
//!
//! The state is owned by one task and reached by message ([`Registry::query`]), never shared.
//! State changes only through the follower: a publish or a removal writes the drawer and then
//! waits for the follower to fold the write in, so what a caller reads back is what every other
//! reader of the drawer will derive, and a write this node made is never believed before the
//! drawer's own event stream confirms it.

use crate::docs_error;
use crate::store;
use crate::store::FirstSeen;
use big_blobs::BlobService;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;

use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use big_cabinet_core::reference::DatumPin;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::Location;
use big_cabinet_core::reference::ReferenceDocId;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Component;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::Revision;
use big_cartridges_core::Version;

use big_cartridges_core::protocol::CartridgeChange;
use big_cartridges_core::protocol::CartridgeChanges;
use big_cartridges_core::protocol::CartridgeRecord;
use big_cartridges_core::protocol::CartridgeVersionRecord;
use big_cartridges_core::protocol::CartridgeVersionSummary;
use big_cartridges_core::protocol::CartridgesCursor;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::PublishedCartridgeVersion;
use big_cartridges_core::protocol::StageCartridgeVersion;
use big_cartridges_core::registry::CartridgeIdentity;
use big_cartridges_core::registry::artifact_key;
use big_cartridges_core::registry::artifact_slot_version;
use big_cartridges_core::registry::component_key;
use big_cartridges_core::registry::identity_key;
use big_cartridges_core::registry::lexicon_key;
use big_cartridges_core::registry::sidecar_key;
use big_cartridges_core::registry::version_key;
use serde_json::Value as Json;
use service_client_native_rs::Client;
use service_utils_rs::RequestContext;
use service_utils_rs::Spawner;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

/// How long the registry's own poll of the drawer is held once caught up (ADR 023): a change is
/// delivered the moment it lands, and an expired hold is an ordinary empty page.
const POLL_HOLD_MS: u32 = 5_000;

/// The caps a watch request is clamped to, from big_sync's (ADR 023 rule 3).
const WATCH_MAX_LIMIT: u32 = 1024;
pub(crate) const WATCH_MAX_HOLD_MS: u32 = 15_000;

type FirstSeenMap = BTreeMap<(CartridgeId, Version), FirstSeen>;

/// One version as served: the first-seen envelope, and whether the drawer disagrees with it. An
/// `unverified` version is one the drawer holds whose signature did not verify and that this node
/// never accepted (so it has no first-seen record): it is listed with the reason and never
/// served.
#[derive(Debug, Clone, PartialEq)]
struct VersionState {
    envelope: Cartridge,
    conflict: Option<String>,
    unverified: Option<String>,
    resolved: Option<ResolvedCartridge>,
}

#[derive(Debug, Clone, PartialEq)]
enum CachedResolution {
    Resolved(Box<ResolvedCartridge>),
    Unreadable(String),
    Invalid(String),
}

/// What one registry document says, after comparing it with the first-seen record.
#[derive(Debug, Clone, PartialEq)]
struct DocView {
    id: CartridgeId,
    versions: BTreeMap<Version, VersionState>,
    unreadable: BTreeMap<String, String>,
}

pub(crate) struct State {
    docs: Client<DocsProtocol>,
    capability: CapId,
    sql: Database,
    first_seen: FirstSeenMap,
    views: BTreeMap<DocId, DocView>,
    resolution_cache: BTreeMap<(DocId, Revision), (Vec<ChangeHash>, CachedResolution)>,
    /// The documents claiming each cartridge id. Two nodes can each create a document for one id
    /// before they sync, so the first (lowest) is served and the rest reported as duplicates.
    claims: BTreeMap<CartridgeId, BTreeSet<DocId>>,
    /// The incarnation `seq` counts in; see [`CartridgesCursor`].
    epoch: Uuid7,
    seq: u64,
    seq_tx: watch::Sender<u64>,
    /// The sequence each cartridge last changed at, tombstones included, for the watch.
    changes: BTreeMap<CartridgeId, (u64, bool)>,
}

/// Whether `envelope` is what the document slot `version` of cartridge `id` claims to hold.
fn check_envelope(envelope: &Cartridge, id: &CartridgeId, version: &Version) -> Result<(), String> {
    if envelope.id != *id {
        return Err(format!("the envelope is for cartridge `{}`", envelope.id));
    }
    if envelope.version != *version {
        return Err(format!("the envelope is version {}", envelope.version));
    }
    envelope.validate().map_err(|error| error.to_string())
}

/// Folds one registry document into what it serves, plus the envelopes seen for the first time
/// (which the caller records before serving them). `None` for a document with no readable cartridge
/// identity: it is not a cartridge.
fn fold_doc(read: &DatumsRead, first_seen: &FirstSeenMap) -> Option<(DocView, Vec<Cartridge>)> {
    let DatumRead::Accepted { value, .. } = read.datums.get(&identity_key())? else {
        return None;
    };
    let id = serde_json::from_value::<CartridgeIdentity>(value.clone())
        .ok()?
        .id;
    let mut view = DocView {
        id: id.clone(),
        versions: BTreeMap::new(),
        unreadable: BTreeMap::new(),
    };
    let mut fresh = Vec::new();
    for (key, datum) in &read.datums {
        if key.tag().as_str() != Cartridge::ID {
            continue;
        }
        let slot = String::from_utf8_lossy(key.slot_bytes()).into_owned();
        let version = match slot.parse::<Version>() {
            Ok(version) if version.to_string() == slot => version,
            _ => {
                view.unreadable
                    .insert(slot, "the slot is not a canonical semver version".into());
                continue;
            }
        };
        let content = match datum {
            DatumRead::Absent { .. } => continue,
            DatumRead::Accepted { value, .. } => serde_json::from_value::<Cartridge>(value.clone())
                .map_err(|error| format!("the envelope does not decode: {error}"))
                .and_then(|envelope| {
                    check_envelope(&envelope, &id, &version)?;
                    Ok(envelope)
                }),
            DatumRead::Pending { reason, .. } => Err(format!("not yet accepted: {reason}")),
            DatumRead::Rejected { reason } => Err(format!("rejected: {reason}")),
            DatumRead::Undecodable { reason, .. } => Err(format!("undecodable: {reason}")),
        };
        let pin = first_seen.get(&(id.clone(), version.clone()));
        match (content, pin) {
            // A version this node meets for the first time is accepted only with a signature that
            // verifies against its namespace DID. Until it is accepted it is not pinned as
            // first-seen, so a bad copy never reserves the label against the genuine one.
            (Ok(envelope), None) => match envelope.verify_signature() {
                Ok(()) => {
                    view.versions.insert(
                        version,
                        VersionState {
                            envelope: envelope.clone(),
                            conflict: None,
                            unverified: None,
                            resolved: None,
                        },
                    );
                    fresh.push(envelope);
                }
                Err(error) => {
                    view.versions.insert(
                        version,
                        VersionState {
                            envelope,
                            conflict: None,
                            unverified: Some(error.to_string()),
                            resolved: None,
                        },
                    );
                }
            },
            (Ok(envelope), Some(pin)) => {
                let conflict = (envelope.revision() != pin.revision).then(|| {
                    format!(
                        "the drawer's datum is revision {}, first seen as {}",
                        envelope.revision(),
                        pin.revision
                    )
                });
                view.versions.insert(
                    version,
                    VersionState {
                        envelope: pin.envelope.clone(),
                        conflict,
                        unverified: None,
                        resolved: None,
                    },
                );
            }
            (Err(reason), Some(pin)) => {
                view.versions.insert(
                    version,
                    VersionState {
                        envelope: pin.envelope.clone(),
                        conflict: Some(reason),
                        unverified: None,
                        resolved: None,
                    },
                );
            }
            (Err(reason), None) => {
                view.unreadable.insert(slot, reason);
            }
        }
    }
    Some((view, fresh))
}

impl State {
    fn primary_doc(&self, id: &CartridgeId) -> Option<&DocId> {
        self.claims.get(id)?.first()
    }

    fn record(&self, id: &CartridgeId) -> Option<CartridgeRecord> {
        let claims = self.claims.get(id)?;
        let doc_id = claims.first()?;
        let view = &self.views[doc_id];
        Some(CartridgeRecord {
            id: id.clone(),
            doc_id: doc_id.clone(),
            versions: view
                .versions
                .iter()
                .map(|(version, state)| CartridgeVersionSummary {
                    version: version.clone(),
                    revision: state.envelope.revision(),
                    conflict: state.conflict.clone(),
                    unverified: state.unverified.clone(),
                })
                .collect(),
            unreadable: view.unreadable.clone(),
            duplicate_docs: claims.iter().skip(1).cloned().collect(),
        })
    }

    fn version(
        &self,
        id: &CartridgeId,
        version: &Version,
    ) -> Result<CartridgeVersionRecord, CartridgesError> {
        let doc_id = self
            .primary_doc(id)
            .ok_or_else(|| CartridgesError::UnknownCartridge(id.clone()))?;
        let state = self.views[doc_id].versions.get(version).ok_or_else(|| {
            CartridgesError::UnknownVersion {
                id: id.clone(),
                version: Box::new(version.clone()),
            }
        })?;
        if let Some(reason) = &state.unverified {
            return Err(CartridgesError::Unverified {
                id: id.clone(),
                version: Box::new(version.clone()),
                reason: reason.clone(),
            });
        }
        Ok(CartridgeVersionRecord {
            id: id.clone(),
            version: version.clone(),
            revision: state.envelope.revision(),
            envelope: state.envelope.clone(),
            conflict: state.conflict.clone(),
        })
    }

    fn resolved_version(
        &self,
        id: &CartridgeId,
        version: &Version,
    ) -> Result<ResolvedCartridge, CartridgesError> {
        let doc_id = self
            .primary_doc(id)
            .ok_or_else(|| CartridgesError::UnknownCartridge(id.clone()))?;
        let state = self.views[doc_id].versions.get(version).ok_or_else(|| {
            CartridgesError::UnknownVersion {
                id: id.clone(),
                version: Box::new(version.clone()),
            }
        })?;
        state.resolved.clone().ok_or_else(|| {
            CartridgesError::Rejected(
                state
                    .conflict
                    .clone()
                    .unwrap_or_else(|| "the cartridge version has no resolved payload".into()),
            )
        })
    }

    async fn cached_resolution(
        &mut self,
        doc: &DocId,
        heads: &[ChangeHash],
        envelope: &Cartridge,
    ) -> CachedResolution {
        let key = (doc.clone(), envelope.revision());
        // Pins name fixed heads, so a resolved or invalid result never changes for this
        // envelope revision. An unreadable one only means the pinned heads had not arrived at
        // the document heads it was tried at, so it is retried once the document moves on.
        if let Some((cached_heads, cached)) = self.resolution_cache.get(&key)
            && (!matches!(cached, CachedResolution::Unreadable(_))
                || cached_heads.as_slice() == heads)
        {
            return cached.clone();
        }
        let resolution = match resolve_envelope(&self.docs, self.capability, doc, envelope).await {
            Ok(resolved) => CachedResolution::Resolved(Box::new(resolved)),
            Err(error @ CartridgesError::PinnedDatumUnreadable { .. }) => {
                CachedResolution::Unreadable(error.to_string())
            }
            Err(error) => CachedResolution::Invalid(error.to_string()),
        };
        self.resolution_cache
            .insert(key, (heads.to_vec(), resolution.clone()));
        resolution
    }

    async fn fold_doc_resolved(
        &mut self,
        doc: &DocId,
        read: &DatumsRead,
    ) -> Option<(DocView, Vec<Cartridge>)> {
        let (mut view, fresh) = fold_doc(read, &self.first_seen)?;
        let mut unreadable = Vec::new();
        let mut invalid_fresh = BTreeSet::new();
        for (version, state) in &mut view.versions {
            if state.unverified.is_some() {
                continue;
            }
            match self
                .cached_resolution(doc, &read.heads, &state.envelope)
                .await
            {
                CachedResolution::Resolved(resolved) => state.resolved = Some(*resolved),
                CachedResolution::Unreadable(reason) => unreadable.push((version.clone(), reason)),
                CachedResolution::Invalid(reason) => {
                    state.conflict = Some(reason);
                    invalid_fresh.insert(version.clone());
                }
            }
        }
        for (version, reason) in &unreadable {
            view.versions.remove(version);
            view.unreadable.insert(version.to_string(), reason.clone());
        }
        let fresh = fresh
            .into_iter()
            .filter(|envelope| {
                !invalid_fresh.contains(&envelope.version)
                    && !unreadable
                        .iter()
                        .any(|(version, _)| version == &envelope.version)
            })
            .collect();
        Some((view, fresh))
    }

    /// Re-reads `doc` from the drawer at its current heads and folds it in.
    async fn refresh(&mut self, doc: &DocId) -> Result<(), CartridgesError> {
        let read = read_doc(&self.docs, self.capability, doc).await?;
        self.apply(doc.clone(), Some(&read)).await
    }

    /// Replaces what `doc` contributes with the fold of `read` (nothing for `None`), records any
    /// first-seen envelope before it is served, and advances the change sequence for every cartridge
    /// whose record differs.
    async fn apply(
        &mut self,
        doc: DocId,
        read: Option<&DatumsRead>,
    ) -> Result<(), CartridgesError> {
        let folded = match read {
            Some(read) => self.fold_doc_resolved(&doc, read).await,
            None => None,
        };
        let previous = self.views.get(&doc).map(|view| view.id.clone());
        let next = folded.as_ref().map(|(view, _)| view.id.clone());
        let affected: BTreeSet<CartridgeId> = previous.iter().chain(next.iter()).cloned().collect();
        let before: Vec<Option<CartridgeRecord>> =
            affected.iter().map(|id| self.record(id)).collect();
        if let Some(previous) = &previous
            && let Some(claims) = self.claims.get_mut(previous)
        {
            claims.remove(&doc);
            if claims.is_empty() {
                self.claims.remove(previous);
            }
        }
        match folded {
            Some((view, fresh)) => {
                for envelope in fresh {
                    store::upsert_first_seen(&self.sql, &envelope).await?;
                    self.first_seen.insert(
                        (envelope.id.clone(), envelope.version.clone()),
                        FirstSeen {
                            revision: envelope.revision(),
                            envelope,
                        },
                    );
                }
                self.claims
                    .entry(view.id.clone())
                    .or_default()
                    .insert(doc.clone());
                self.views.insert(doc, view);
            }
            None => {
                self.views.remove(&doc);
            }
        }
        let mut advanced = false;
        for (id, before) in affected.iter().zip(before) {
            let after = self.record(id);
            if before != after {
                self.seq += 1;
                self.changes.insert(id.clone(), (self.seq, after.is_none()));
                advanced = true;
            }
        }
        if advanced {
            self.seq_tx.send_replace(self.seq);
        }
        Ok(())
    }

    fn claiming(&self, id: &CartridgeId) -> Result<Vec<DocId>, CartridgesError> {
        self.claims
            .get(id)
            .map(|docs| docs.iter().cloned().collect())
            .ok_or_else(|| CartridgesError::UnknownCartridge(id.clone()))
    }

    fn changes_after(&self, after: Option<CartridgesCursor>, limit: u32) -> CartridgeChanges {
        let (from, reset) = match after {
            Some(cursor) if cursor.epoch == self.epoch => (cursor.seq, false),
            _ => (0, true),
        };
        let limit = match limit {
            0 => WATCH_MAX_LIMIT,
            limit => limit.min(WATCH_MAX_LIMIT),
        } as usize;
        let mut pending: Vec<CartridgeChange> = self
            .changes
            .iter()
            .filter(|(_, (seq, _))| *seq > from)
            .map(|(id, (seq, removed))| CartridgeChange {
                seq: *seq,
                cartridge_id: id.clone(),
                removed: *removed,
            })
            .collect();
        pending.sort_by_key(|change| change.seq);
        let caught_up = pending.len() <= limit;
        pending.truncate(limit);
        let through = match pending.last() {
            Some(last) if !caught_up => last.seq,
            _ => self.seq,
        };
        CartridgeChanges {
            changes: pending,
            through: CartridgesCursor {
                epoch: self.epoch,
                seq: through,
            },
            caught_up,
            reset,
        }
    }
}

async fn read_doc(
    docs: &Client<DocsProtocol>,
    capability: CapId,
    doc: &DocId,
) -> Result<DatumsRead, CartridgesError> {
    docs.call(
        RequestContext::root(),
        ReadDatums {
            capability,
            doc_id: doc.clone(),
            at: At::Current,
            datums: None,
            optimistic: false,
        },
    )
    .await
    .map_err(docs_error)
}

fn pin_for(doc: &DocId, key: DatumKey, heads: &[ChangeHash]) -> DatumPin {
    let reference = DatumReference::new(ReferenceDocId::Concrete(doc.clone()), key)
        .pinned(heads.iter().copied());
    DatumPin::new(Location::new(reference, None).expect("a location without a fragment is valid"))
}

/// What the state-owning task is asked to do.
enum Command {
    /// Run a closure against the state; the closure sends its own reply.
    Run(Box<dyn FnOnce(&mut State) + Send>),
    /// Fold in the documents a poll page named: `left` leave, `touched` are re-read.
    Fold {
        touched: BTreeSet<DocId>,
        left: BTreeSet<DocId>,
        reply: oneshot::Sender<Result<(), CartridgesError>>,
    },
}

/// What importing an existing document needs: the drawers service, and the registry actor whose
/// identity enters the document in the registry drawer. The actor is Admin on the registry drawer
/// and holds nothing anywhere else: the document itself is reached only through the handles the
/// caller lends for the import, which is what bounds it.
pub(crate) struct DocImport {
    pub(crate) authority: Client<AuthorityProtocol>,
    pub(crate) drawers: Client<DrawersProtocol>,
    pub(crate) actor: CapId,
}

/// The registry: a handle to the task that owns the derived [`State`], plus the means to write
/// the drawer. `gate` serializes publishes and removals so two of them never both decide a cartridge
/// has no document yet.
pub(crate) struct Registry {
    commands: mpsc::Sender<Command>,
    import: DocImport,
    gate: Semaphore,
    docs: Client<DocsProtocol>,
    capability: CapId,
    drawer: DrawerId,
    blobs: Arc<BlobService>,
}

impl Registry {
    /// Loads the node's first-seen record and starts the state-owning task on `tasks`. The task
    /// ends when `tasks` is cancelled, after which a call on the handle panics: the host that
    /// serves callers is stopped first.
    pub(crate) async fn load(
        tasks: &Spawner,
        docs: Client<DocsProtocol>,
        capability: CapId,
        drawer: DrawerId,
        import: DocImport,
        blobs: Arc<BlobService>,
        sql: Database,
    ) -> Result<Self, CartridgesError> {
        let first_seen = store::first_seen(&sql)
            .await?
            .into_iter()
            .map(|(id, version, seen)| ((id, version), seen))
            .collect();
        let mut state = State {
            docs: docs.clone(),
            capability,
            sql,
            first_seen,
            views: BTreeMap::new(),
            resolution_cache: BTreeMap::new(),
            claims: BTreeMap::new(),
            epoch: Uuid7::new(),
            seq: 0,
            seq_tx: watch::channel(0).0,
            changes: BTreeMap::new(),
        };
        let (commands, mut inbox) = mpsc::channel(64);
        let cancelled = tasks.clone();
        tasks
            .spawn(async move {
                loop {
                    let command = tokio::select! {
                        biased;
                        _ = cancelled.cancelled() => return,
                        command = inbox.recv() => command,
                    };
                    match command {
                        None => return,
                        Some(Command::Run(run)) => run(&mut state),
                        Some(Command::Fold {
                            touched,
                            left,
                            reply,
                        }) => {
                            let folded = async {
                                for doc in left {
                                    state.apply(doc, None).await?;
                                }
                                for doc in touched {
                                    state.refresh(&doc).await?;
                                }
                                Ok(())
                            }
                            .await;
                            // A follower that stopped waiting has been cancelled.
                            drop(reply.send(folded));
                        }
                    }
                }
            })
            .expect("a fresh stop token accepts tasks");
        Ok(Self {
            commands,
            import,
            gate: Semaphore::new(1),
            docs,
            capability,
            drawer,
            blobs,
        })
    }

    /// Runs `run` against the state on its owning task and returns what it computed.
    async fn query<T: Send + 'static>(
        &self,
        run: impl FnOnce(&mut State) -> T + Send + 'static,
    ) -> T {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Run(Box::new(move |state| {
                // A caller that stopped waiting has been dropped; there is no one to tell.
                drop(reply.send(run(state)));
            })))
            .await
            .expect("the registry task outlives the calls it serves");
        answer
            .await
            .expect("the registry task answers every call it accepts")
    }

    pub(crate) async fn subscribe(&self) -> watch::Receiver<u64> {
        self.query(|state| state.seq_tx.subscribe()).await
    }

    pub(crate) async fn record(&self, id: &CartridgeId) -> Option<CartridgeRecord> {
        let id = id.clone();
        self.query(move |state| state.record(&id)).await
    }

    pub(crate) async fn records(&self) -> Vec<CartridgeRecord> {
        self.query(|state| {
            state
                .claims
                .keys()
                .map(|id| {
                    state
                        .record(id)
                        .expect("a claimed cartridge id has a record")
                })
                .collect()
        })
        .await
    }

    pub(crate) async fn version(
        &self,
        id: &CartridgeId,
        version: &Version,
    ) -> Result<CartridgeVersionRecord, CartridgesError> {
        let (id, version) = (id.clone(), version.clone());
        self.query(move |state| state.version(&id, &version)).await
    }
    pub(crate) async fn resolved(
        &self,
        id: &CartridgeId,
        version: &Version,
    ) -> Result<ResolvedCartridge, CartridgesError> {
        let (id, version) = (id.clone(), version.clone());
        self.query(move |state| state.resolved_version(&id, &version))
            .await
    }

    pub(crate) async fn changes_after(
        &self,
        after: Option<CartridgesCursor>,
        limit: u32,
    ) -> CartridgeChanges {
        self.query(move |state| state.changes_after(after, limit))
            .await
    }

    /// Waits until `done` holds of the state. Subscribed before the first look, so a change
    /// between the look and the wait wakes it.
    async fn settled(&self, done: impl Fn(&State) -> bool + Send + Sync + 'static) {
        let done = Arc::new(done);
        let mut changed = self.subscribe().await;
        loop {
            let check = Arc::clone(&done);
            if self.query(move |state| check(state)).await {
                return;
            }
            changed
                .changed()
                .await
                .expect("the registry outlives its waiters");
        }
    }

    pub(crate) async fn stage(
        &self,
        params: StageCartridgeVersion,
    ) -> Result<Cartridge, CartridgesError> {
        let _gate = self
            .gate
            .acquire()
            .await
            .expect("the registry's gate is never closed");
        let id = params.id.clone();
        let version = params.version.clone();
        // Refuse a component whose bytes are not here before any document is made for it.
        self.check_components(&id, &params.components).await?;
        let doc = self
            .query({
                let id = id.clone();
                let version = version.clone();
                move |state| {
                    if state
                        .first_seen
                        .contains_key(&(id.clone(), version.clone()))
                    {
                        return Err(CartridgesError::AlreadyPublished {
                            id: id.clone(),
                            version: Box::new(version.clone()),
                        });
                    }
                    let Some(doc) = state.primary_doc(&id).cloned() else {
                        return Ok(None);
                    };
                    if state.views[&doc].versions.contains_key(&version)
                        || state.views[&doc]
                            .unreadable
                            .contains_key(&version.to_string())
                    {
                        return Err(CartridgesError::AlreadyPublished {
                            id,
                            version: Box::new(version),
                        });
                    }
                    Ok(Some(doc))
                }
            })
            .await?;
        let (doc, current_heads) = match doc {
            Some(doc) => {
                let read = read_doc(&self.docs, self.capability, &doc).await?;
                (doc, read.heads)
            }
            None => {
                let receipt = self
                    .create(BTreeMap::from([(
                        identity_key(),
                        serde_json::to_value(CartridgeIdentity {
                            id: id.clone(),
                            extra: Default::default(),
                        })
                        .expect("an identity serializes"),
                    )]))
                    .await?;
                // The registry's projection follows its drawer: wait until it serves the new
                // document, so a publish that follows this stage finds its cartridge.
                let created = id.clone();
                self.settled(move |state| state.primary_doc(&created).is_some())
                    .await;
                (receipt.doc_id, receipt.heads)
            }
        };

        let blob_heads = self
            .stage_blobs(&doc, &version, &params.components, current_heads)
            .await?;

        let lexicon_keys: Vec<_> = params.lexicons.keys().cloned().collect();
        let sidecar_keys: Vec<_> = params.sidecars.keys().cloned().collect();
        let component_keys: Vec<_> = params.components.keys().cloned().collect();
        let mut payload = BTreeMap::new();
        for (nsid, lexicon) in params.lexicons {
            payload.insert(
                lexicon_key(&version, &nsid),
                serde_json::to_value(lexicon).expect("a Lexicon serializes"),
            );
        }
        for (nsid, sidecar) in params.sidecars {
            payload.insert(
                sidecar_key(&version, &nsid),
                serde_json::to_value(sidecar).expect("a sidecar serializes"),
            );
        }
        for (key, component) in params.components {
            let Component::Wasm(mut wasm) = component else {
                unreachable!("unknown components were rejected above")
            };
            wasm.bytes = pin_for(&doc, artifact_key(&version, &key), &blob_heads);
            payload.insert(
                component_key(&version, &key),
                serde_json::to_value(Component::Wasm(wasm)).expect("a component serializes"),
            );
        }
        let payload_heads = if payload.is_empty() {
            blob_heads
        } else {
            self.write(&doc, payload, BTreeSet::new()).await?.heads
        };
        let mut envelope = Cartridge::new(id, version.clone());
        envelope.lexicons = lexicon_keys
            .into_iter()
            .map(|nsid| {
                (
                    nsid.clone(),
                    pin_for(&doc, lexicon_key(&version, &nsid), &payload_heads),
                )
            })
            .collect();
        envelope.sidecars = sidecar_keys
            .into_iter()
            .map(|nsid| {
                (
                    nsid.clone(),
                    pin_for(&doc, sidecar_key(&version, &nsid), &payload_heads),
                )
            })
            .collect();
        envelope.components = component_keys
            .into_iter()
            .map(|key| {
                (
                    key.clone(),
                    pin_for(&doc, component_key(&version, &key), &payload_heads),
                )
            })
            .collect();
        Ok(envelope)
    }

    /// Each component is a wasm component whose bytes the blob store holds, at the size it states.
    async fn check_components(
        &self,
        id: &CartridgeId,
        components: &BTreeMap<String, Component>,
    ) -> Result<(), CartridgesError> {
        for (key, component) in components {
            let Component::Wasm(wasm) = component else {
                return Err(CartridgesError::Rejected(format!(
                    "component `{key}` is not a known publishable component"
                )));
            };
            let info = self
                .blobs
                .stat(&BlobId::new(wasm.digest.0))
                .await
                .map_err(|error| CartridgesError::Storage(error.to_string()))?
                .ok_or_else(|| CartridgesError::ArtifactUnavailable {
                    id: id.clone(),
                    artifact: key.clone(),
                })?;
            if info.length_octets != wasm.size {
                return Err(CartridgesError::Rejected(format!(
                    "component `{key}` declares {} octets, the stored blob has {}",
                    wasm.size, info.length_octets
                )));
            }
        }
        Ok(())
    }

    async fn stage_blobs(
        &self,
        doc: &DocId,
        version: &Version,
        components: &BTreeMap<String, Component>,
        current_heads: Vec<ChangeHash>,
    ) -> Result<Vec<ChangeHash>, CartridgesError> {
        let mut datums = BTreeMap::new();
        for (key, component) in components {
            let Component::Wasm(wasm) = component else {
                unreachable!("check_components rejected every other component")
            };
            let blob = Blob {
                mime: wasm.media_type.clone(),
                length_octets: wasm.size,
                digest: wasm.digest.to_string(),
                inline: None,
                urls: None,
                extra: Default::default(),
            };
            let mut raw = serde_json::to_value(&blob).expect("a blob serializes");
            raw.as_object_mut()
                .expect("a blob serializes as an object")
                .retain(|_, value| !value.is_null());
            datums.insert(artifact_key(version, key), raw);
        }
        if datums.is_empty() {
            Ok(current_heads)
        } else {
            Ok(self.write(doc, datums, BTreeSet::new()).await?.heads)
        }
    }
}

async fn read_pinned(
    docs: &Client<DocsProtocol>,
    capability: CapId,
    envelope: &Cartridge,
    doc: &DocId,
    key: &DatumKey,
    pin: &DatumPin,
) -> Result<Json, CartridgesError> {
    let reference = pin.location().reference();
    let actual = reference.document().resolve(doc);
    if actual != *doc {
        return Err(CartridgesError::PinnedDocumentMismatch {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            actual,
            expected: doc.clone(),
        });
    }
    if reference.key() != key {
        return Err(CartridgesError::PinnedDatumInvalid {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: format!("the pin names `{}`", reference.key()),
        });
    }
    let heads = reference.heads().to_vec();
    if heads.is_empty() {
        return Err(CartridgesError::PinnedDatumUnreadable {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: "the pin has no document heads".into(),
        });
    }
    let read = docs
        .call(
            RequestContext::root(),
            ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Heads(heads),
                datums: Some(BTreeSet::from([key.clone()])),
                optimistic: false,
            },
        )
        .await
        .map_err(|error| CartridgesError::PinnedDatumUnreadable {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: docs_error(error).to_string(),
        })?;
    match read.datums.get(key) {
        Some(DatumRead::Accepted { value, .. }) => Ok(value.clone()),
        Some(DatumRead::Pending { reason, .. }) => Err(CartridgesError::PinnedDatumUnreadable {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: reason.clone(),
        }),
        Some(DatumRead::Rejected { reason }) => Err(CartridgesError::PinnedDatumInvalid {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: reason.clone(),
        }),
        Some(DatumRead::Undecodable { reason, .. }) => Err(CartridgesError::PinnedDatumInvalid {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: reason.clone(),
        }),
        Some(DatumRead::Absent { .. }) | None => Err(CartridgesError::PinnedDatumInvalid {
            id: envelope.id.clone(),
            version: Box::new(envelope.version.clone()),
            key: key.to_string(),
            reason: "the datum is absent at the pinned heads".into(),
        }),
    }
}

async fn resolve_envelope(
    docs: &Client<DocsProtocol>,
    capability: CapId,
    doc: &DocId,
    envelope: &Cartridge,
) -> Result<ResolvedCartridge, CartridgesError> {
    envelope.verify_signature()?;
    let mut lexicons = BTreeMap::new();
    for (nsid, pin) in &envelope.lexicons {
        let key = lexicon_key(&envelope.version, nsid);
        let value = read_pinned(docs, capability, envelope, doc, &key, pin).await?;
        let file: big_cabinet_core::datum::Extra =
            serde_json::from_value(value).map_err(|error| CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: key.to_string(),
                reason: error.to_string(),
            })?;
        if file.get("id").and_then(Json::as_str) != Some(nsid.as_str()) {
            return Err(CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: key.to_string(),
                reason: "the Lexicon id does not match the envelope key".into(),
            });
        }
        lexicons.insert(nsid.clone(), file);
    }
    let mut sidecars = BTreeMap::new();
    for (nsid, pin) in &envelope.sidecars {
        let key = sidecar_key(&envelope.version, nsid);
        let value = read_pinned(docs, capability, envelope, doc, &key, pin).await?;
        let file: big_cabinet_core::datum::Extra =
            serde_json::from_value(value).map_err(|error| CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: key.to_string(),
                reason: error.to_string(),
            })?;
        sidecars.insert(nsid.clone(), file);
    }
    let mut components = BTreeMap::new();
    for (name, pin) in &envelope.components {
        let key = component_key(&envelope.version, name);
        let value = read_pinned(docs, capability, envelope, doc, &key, pin).await?;
        let component: Component =
            serde_json::from_value(value).map_err(|error| CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: key.to_string(),
                reason: error.to_string(),
            })?;
        let Component::Wasm(wasm) = &component else {
            return Err(CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: key.to_string(),
                reason: "the component kind is not supported".into(),
            });
        };
        let blob_key = artifact_key(&envelope.version, name);
        let blob_value =
            read_pinned(docs, capability, envelope, doc, &blob_key, &wasm.bytes).await?;
        let blob: Blob = serde_json::from_value(blob_value).map_err(|error| {
            CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: blob_key.to_string(),
                reason: error.to_string(),
            }
        })?;
        if blob.digest != wasm.digest.to_string() || blob.length_octets != wasm.size {
            return Err(CartridgesError::PinnedDatumInvalid {
                id: envelope.id.clone(),
                version: Box::new(envelope.version.clone()),
                key: blob_key.to_string(),
                reason: "the blob metadata does not match the component".into(),
            });
        }
        components.insert(name.clone(), component);
    }
    Ok(ResolvedCartridge::new(
        envelope.clone(),
        lexicons,
        sidecars,
        components,
    ))
}
impl Registry {
    pub(crate) async fn publish(
        &self,
        envelope: Cartridge,
    ) -> Result<PublishedCartridgeVersion, CartridgesError> {
        envelope
            .validate()
            .map_err(|error| CartridgesError::Rejected(error.to_string()))?;
        envelope.verify_signature()?;
        let _gate = self
            .gate
            .acquire()
            .await
            .expect("the registry's gate is never closed");
        let id = envelope.id.clone();
        let version = envelope.version.clone();
        let revision = envelope.revision();
        let doc = self
            .query({
                let id = id.clone();
                let version = version.clone();
                move |state| {
                    let doc = state
                        .primary_doc(&id)
                        .cloned()
                        .ok_or_else(|| CartridgesError::UnknownCartridge(id.clone()))?;
                    match state.first_seen.get(&(id.clone(), version.clone())) {
                        Some(seen) if seen.revision != revision => {
                            return Err(CartridgesError::VersionExists {
                                id,
                                version: Box::new(version),
                                existing: seen.revision,
                                incoming: revision,
                            });
                        }
                        Some(_) => {}
                        None => {
                            let lowest: Version = "0.0.0-0".parse().expect("a valid semver");
                            let newest = state
                                .first_seen
                                .range((id.clone(), lowest)..)
                                .take_while(|((seen, _), _)| *seen == id)
                                .map(|((_, seen), _)| seen)
                                .last();
                            if let Some(newest) = newest
                                && version <= *newest
                            {
                                return Err(CartridgesError::Downgrade {
                                    id,
                                    version: Box::new(version),
                                    newest: Box::new(newest.clone()),
                                });
                            }
                        }
                    }
                    Ok(doc)
                }
            })
            .await?;
        resolve_envelope(&self.docs, self.capability, &doc, &envelope).await?;
        if let Ok(record) = self.version(&id, &version).await
            && record.revision == revision
            && record.conflict.is_none()
        {
            return Ok(PublishedCartridgeVersion {
                record,
                unchanged: true,
            });
        }
        let mut set = BTreeMap::new();
        set.insert(
            version_key(&version),
            serde_json::to_value(&envelope).expect("an envelope serializes"),
        );
        self.write(&doc, set, BTreeSet::new()).await?;
        let waited = (id.clone(), version.clone());
        self.settled(move |state| {
            state
                .version(&waited.0, &waited.1)
                .is_ok_and(|record| record.revision == revision && record.conflict.is_none())
        })
        .await;
        Ok(PublishedCartridgeVersion {
            record: self.version(&id, &version).await?,
            unchanged: false,
        })
    }

    /// Retracts a handle this registry minted for one call; a handle is its own `via`.
    async fn retract(&self, capability: CapId) -> Result<(), CartridgesError> {
        self.import
            .authority
            .call(
                RequestContext::root(),
                RetractCapability {
                    via: capability,
                    capability,
                },
            )
            .await
            .map_err(|error| CartridgesError::AuthorityUnavailable(error.to_string()))
    }

    /// Enters an existing document in the registry drawer at Read, after checking through the
    /// caller's `read` handle that it is a cartridge document and that `permitted` accepts its cartridge id.
    /// The version contents are not looked at: the follower folds the document like any other that
    /// arrives in the drawer, which is where each version's signature is verified. Returns once the
    /// registry serves the document.
    pub(crate) async fn import_document(
        &self,
        doc: &DocId,
        read: CapId,
        reach: CapId,
        permitted: impl Fn(&CartridgeId) -> Result<(), CartridgesError>,
    ) -> Result<CartridgeRecord, CartridgesError> {
        let _gate = self
            .gate
            .acquire()
            .await
            .expect("the registry's gate is never closed");
        let read = read_doc(&self.docs, read, doc).await?;
        let Some(DatumRead::Accepted { value, .. }) = read.datums.get(&identity_key()) else {
            return Err(CartridgesError::Rejected(format!(
                "document {doc} has no readable cartridge identity datum"
            )));
        };
        let id = serde_json::from_value::<CartridgeIdentity>(value.clone())
            .map_err(|error| {
                CartridgesError::Rejected(format!(
                    "the cartridge identity does not decode: {error}"
                ))
            })?
            .id;
        permitted(&id)?;
        // The registry's own handle covers only its own drawer, where it is Admin. What the entry
        // gives away on the document is covered by the caller's `reach`, so the registry never
        // needs standing access to the drawer the document came from. It lives for this one call.
        let entry = self
            .import
            .drawers
            .call_with(
                RequestContext::root(),
                Authorization::identity(self.import.actor).metadata(),
                MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(self.drawer.clone(), DrawerAccess::Write)]),
                        create: false,
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .map_err(|error| CartridgesError::Storage(error.to_string()))?
            .capability_id;
        let entered = self
            .import
            .drawers
            .call(
                RequestContext::root(),
                AddDoc {
                    capability: entry,
                    reach,
                    // Entering a document is idempotent, so a fresh command per call costs
                    // nothing and never replays a stale ledger answer.
                    command: CommandId::try_from(format!(
                        "cartridge registry import {}",
                        Uuid7::new()
                    ))
                    .expect("an import command is within bounds"),
                    drawer_id: self.drawer.clone(),
                    doc_id: doc.clone(),
                    // Read is all the registry needs to serve the document, and the author's
                    // document is not the registry's to write: see `ImportCartridgeDocument`.
                    access: DrawerAccess::Read,
                },
            )
            .await
            .map_err(|error| match error {
                service_utils_rs::ClientError::Method(
                    big_cabinet_core::drawer::protocol::DrawersError::Denied,
                ) => CartridgesError::Denied,
                other => CartridgesError::Storage(other.to_string()),
            });
        // Retracted whether or not the entry succeeded.
        self.retract(entry).await?;
        entered?;
        let waited = (id.clone(), doc.clone());
        self.settled(move |state| {
            state
                .claims
                .get(&waited.0)
                .is_some_and(|docs| docs.contains(&waited.1))
        })
        .await;
        Ok(self
            .record(&id)
            .await
            .expect("a cartridge whose document was just folded in has a record"))
    }

    /// Drops one version from every document claiming the cartridge, and forgets what the node first
    /// saw for it: removing a version is the operator's explicit act, so the label is free again.
    pub(crate) async fn remove_version(
        &self,
        id: &CartridgeId,
        version: &Version,
    ) -> Result<(), CartridgesError> {
        let _gate = self
            .gate
            .acquire()
            .await
            .expect("the registry's gate is never closed");
        let label = version.to_string();
        let docs = {
            let (id, version) = (id.clone(), version.clone());
            self.query(move |state| {
                let docs = state.claiming(&id)?;
                let known = docs
                    .iter()
                    .any(|doc| state.views[doc].versions.contains_key(&version))
                    || state
                        .first_seen
                        .contains_key(&(id.clone(), version.clone()));
                if known {
                    Ok(docs)
                } else {
                    Err(CartridgesError::UnknownVersion {
                        id,
                        version: Box::new(version),
                    })
                }
            })
            .await?
        };
        for doc in docs {
            self.drop_datums(&doc, |key| {
                (key.tag().as_str() == Cartridge::ID && key.slot_bytes() == label.as_bytes())
                    || artifact_slot_version(key).as_deref() == Some(label.as_str())
            })
            .await?;
        }
        let waited = (id.clone(), version.clone(), label);
        self.settled(move |state| {
            state.primary_doc(&waited.0).is_none_or(|doc| {
                !state.views[doc].versions.contains_key(&waited.1)
                    && !state.views[doc].unreadable.contains_key(&waited.2)
            })
        })
        .await;
        // The first-seen record outlives the removal as a tombstone. As with npm, a removed
        // `id@version` never comes back with other content: republishing the identical envelope
        // is harmless and allowed, but a different one is refused, so a label can never be
        // reused to pass new code off under a version someone already trusted.
        Ok(())
    }

    /// Drops every version and the identity from every document claiming the cartridge.
    pub(crate) async fn remove_cartridge(&self, id: &CartridgeId) -> Result<(), CartridgesError> {
        let _gate = self
            .gate
            .acquire()
            .await
            .expect("the registry's gate is never closed");
        let docs = {
            let id = id.clone();
            self.query(move |state| state.claiming(&id)).await?
        };
        for doc in docs {
            self.drop_datums(&doc, |key| {
                matches!(key.tag().as_str(), Cartridge::ID | CartridgeIdentity::ID)
                    || artifact_slot_version(key).is_some()
            })
            .await?;
        }
        let waited = id.clone();
        self.settled(move |state| state.record(&waited).is_none())
            .await;
        // First-seen records stay as tombstones, as in `remove_version`.
        Ok(())
    }

    /// Removes the datums of `doc` that `selects` picks. The read is the document's own, so a
    /// datum a peer added since the last fold is dropped too.
    async fn drop_datums(
        &self,
        doc: &DocId,
        selects: impl Fn(&DatumKey) -> bool,
    ) -> Result<(), CartridgesError> {
        let read = read_doc(&self.docs, self.capability, doc).await?;
        let remove: BTreeSet<DatumKey> = read
            .datums
            .iter()
            .filter(|(key, datum)| selects(key) && !matches!(datum, DatumRead::Absent { .. }))
            .map(|(key, _)| key.clone())
            .collect();
        if remove.is_empty() {
            return Ok(());
        }
        self.write(doc, BTreeMap::new(), remove).await?;
        Ok(())
    }

    async fn write(
        &self,
        doc: &DocId,
        set: BTreeMap<DatumKey, Json>,
        remove: BTreeSet<DatumKey>,
    ) -> Result<big_cabinet_core::doc::WriteReceipt, CartridgesError> {
        self.docs
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: self.capability,
                    command: None,
                    doc_id: doc.clone(),
                    mode: WriteMode::AtCurrent,
                    set,
                    remove,
                },
            )
            .await
            .map_err(docs_error)
    }

    /// A new document in the registry drawer carrying `datums`: its placement enters it in the
    /// drawer, so it is part of the registry from its first write.
    async fn create(
        &self,
        datums: BTreeMap<DatumKey, Json>,
    ) -> Result<big_cabinet_core::doc::WriteReceipt, CartridgesError> {
        self.docs
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: self.capability,
                    command: CommandId::try_from(format!("cartridges/create/{}", Uuid7::new()))
                        .expect("a generated command id is within bounds"),
                    placement: format!("drawer {}", self.drawer),
                    datums,
                },
            )
            .await
            .map_err(docs_error)
    }

    /// Follows the registry drawer: scans it from cursor 0, signals `ready` once the scan has
    /// caught up, then folds every change as it lands. Runs until `cancelled`.
    pub(crate) async fn follow(
        &self,
        ready: oneshot::Sender<()>,
        cancelled: impl Future<Output = ()>,
    ) -> Result<(), CartridgesError> {
        let identity_tag = DatumTag::new(CartridgeIdentity::ID).expect("the identity tag is valid");
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: self.drawer.clone(),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasTag {
                    tag: identity_tag.clone(),
                },
            ],
        };
        let datums = DatumSelection::Of {
            tags: BTreeSet::from([
                identity_tag,
                DatumTag::new(Cartridge::ID).expect("the cartridge tag is valid"),
            ]),
            keys: BTreeSet::new(),
        };
        tokio::pin!(cancelled);
        let (mut after, mut caught_up, mut ready) = (0, false, Some(ready));
        loop {
            let page = tokio::select! {
                biased;
                _ = &mut cancelled => return Ok(()),
                page = self.docs.call(RequestContext::root(), PollDatums {
                    capability: self.capability,
                    predicate: predicate.clone(),
                    datums: datums.clone(),
                    after,
                    limit: 0,
                    hold_ms: if caught_up { POLL_HOLD_MS } else { 0 },
                }) => page.map_err(docs_error)?,
            };
            after = page.through;
            // Events are facts about documents; the fold re-reads each document, so only the
            // last word per document matters. A `leave` for a document never held is
            // idempotent, and the index can deliver one before the document's first row.
            let mut touched = BTreeSet::new();
            let mut left = BTreeSet::new();
            for event in &page.events {
                match event {
                    DatumEvent::Datum { doc_id, .. } => {
                        left.remove(doc_id);
                        touched.insert(doc_id.clone());
                    }
                    DatumEvent::Leave { doc_id, .. } => {
                        touched.remove(doc_id);
                        left.insert(doc_id.clone());
                    }
                }
            }
            if !touched.is_empty() || !left.is_empty() {
                let (reply, folded) = oneshot::channel();
                let sent = self
                    .commands
                    .send(Command::Fold {
                        touched,
                        left,
                        reply,
                    })
                    .await;
                // The state task ends when the registry is stopped, which also ends this loop.
                if sent.is_err() {
                    return Ok(());
                }
                match folded.await {
                    Ok(folded) => folded?,
                    Err(_) => return Ok(()),
                }
            }
            if page.caught_up {
                caught_up = true;
                if let Some(ready) = ready.take() {
                    ready
                        .send(())
                        .expect("boot waits for the registry's first scan");
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
