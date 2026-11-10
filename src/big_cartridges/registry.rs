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
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Version;
use big_cartridges_core::protocol::CartridgeChange;
use big_cartridges_core::protocol::CartridgeChanges;
use big_cartridges_core::protocol::CartridgeRecord;
use big_cartridges_core::protocol::CartridgeVersionRecord;
use big_cartridges_core::protocol::CartridgeVersionSummary;
use big_cartridges_core::protocol::CartridgesCursor;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::PublishedCartridgeVersion;
use big_cartridges_core::registry::CartridgeIdentity;
use big_cartridges_core::registry::artifact_key;
use big_cartridges_core::registry::artifact_slot_version;
use big_cartridges_core::registry::identity_key;
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
        let folded = read.and_then(|read| fold_doc(read, &self.first_seen));
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

    pub(crate) async fn publish(
        &self,
        envelope: Cartridge,
    ) -> Result<PublishedCartridgeVersion, CartridgesError> {
        envelope
            .validate()
            .map_err(|error| CartridgesError::Rejected(error.to_string()))?;
        // Signature checking is CPU work with no registry state in it, so it runs before the gate.
        // It also runs for an envelope the service just signed for the node: acceptance never
        // assumes who produced the signature.
        envelope.verify_signature()?;
        let _gate = self
            .gate
            .acquire()
            .await
            .expect("the registry's gate is never closed");
        let revision = envelope.revision();
        let (id, version) = (envelope.id.clone(), envelope.version.clone());
        let checked = {
            let (id, version) = (id.clone(), version.clone());
            self.query(move |state| {
                match state.first_seen.get(&(id.clone(), version.clone())) {
                    Some(seen) if seen.revision != revision => {
                        return Err(CartridgesError::VersionExists {
                            id,
                            version: Box::new(version),
                            existing: seen.revision,
                            incoming: revision,
                        });
                    }
                    // An existing or tombstoned label with identical content is a republish,
                    // which is allowed whatever its position in the version order.
                    Some(_) => {}
                    // A new label must order after every label this cartridge has or had, tombstones
                    // included (the first-seen record outlives removal), so a validly signed
                    // older version cannot be published as if it were an upgrade.
                    None => {
                        // `0.0.0-0` is the lowest semver precedence, so the range starts at the
                        // cartridge's first possible version.
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
                if let Ok(record) = state.version(&id, &version)
                    && record.revision == revision
                    && record.conflict.is_none()
                {
                    return Ok(Err(PublishedCartridgeVersion {
                        record,
                        unchanged: true,
                    }));
                }
                Ok(Ok(state.primary_doc(&id).cloned()))
            })
            .await
        };
        let doc = match checked? {
            Ok(doc) => doc,
            Err(unchanged) => return Ok(unchanged),
        };
        let mut set = BTreeMap::new();
        for artifact in &envelope.artifacts {
            let info = self
                .blobs
                .stat(&BlobId::new(artifact.digest.0))
                .await
                .map_err(|error| CartridgesError::Storage(error.to_string()))?
                .ok_or_else(|| CartridgesError::ArtifactUnavailable {
                    id: id.clone(),
                    artifact: artifact.key.clone(),
                })?;
            if let Some(size) = artifact.size
                && size != info.length_octets
            {
                return Err(CartridgesError::Rejected(format!(
                    "artifact `{}` declares {size} octets, the stored blob has {}",
                    artifact.key, info.length_octets
                )));
            }
            let blob = Blob {
                mime: artifact
                    .media_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".to_owned()),
                length_octets: info.length_octets,
                digest: artifact.digest.to_string(),
                inline: None,
                urls: None,
                extra: Default::default(),
            };
            // The datum wire form omits an unset option rather than writing `null`.
            let mut raw = serde_json::to_value(&blob).expect("a blob serializes");
            raw.as_object_mut()
                .expect("a blob serializes as an object")
                .retain(|_, value| !value.is_null());
            set.insert(artifact_key(&version, &artifact.key), raw);
        }
        set.insert(
            version_key(&version),
            serde_json::to_value(&envelope).expect("an envelope serializes"),
        );
        match doc {
            Some(doc) => self.write(&doc, set, BTreeSet::new()).await?,
            None => {
                set.insert(
                    identity_key(),
                    serde_json::to_value(CartridgeIdentity {
                        id: id.clone(),
                        extra: Default::default(),
                    })
                    .expect("an identity serializes"),
                );
                self.create(set).await?;
            }
        }
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
        self.write(doc, BTreeMap::new(), remove).await
    }

    async fn write(
        &self,
        doc: &DocId,
        set: BTreeMap<DatumKey, Json>,
        remove: BTreeSet<DatumKey>,
    ) -> Result<(), CartridgesError> {
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
            .map_err(docs_error)?;
        Ok(())
    }

    /// A new document in the registry drawer carrying `datums`: its placement enters it in the
    /// drawer, so it is part of the registry from its first write.
    async fn create(&self, datums: BTreeMap<DatumKey, Json>) -> Result<(), CartridgesError> {
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
            .map_err(docs_error)?;
        Ok(())
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
mod tests {
    use super::*;
    use big_cabinet_core::doc::DatumVersion;
    use big_cartridges_core::test_support::cartridge_id;
    use big_cartridges_core::test_support::sign_as_example;
    use big_cartridges_core::test_support::sign_with_seed;
    use serde_json::json;

    fn accepted(value: Json) -> DatumRead {
        DatumRead::Accepted {
            value,
            version: DatumVersion::default(),
        }
    }

    fn cartridge(version: &str, title: &str) -> Cartridge {
        let mut cartridge = Cartridge::new(cartridge_id("photos"), version.parse().unwrap());
        cartridge.title = Some(title.into());
        sign_as_example(cartridge)
    }

    /// A registry document for `photos` holding `versions` as `(slot, datum)`.
    fn document(versions: Vec<(&str, DatumRead)>) -> DatumsRead {
        let mut datums = BTreeMap::from([(
            identity_key(),
            accepted(json!({ "id": cartridge_id("photos").to_string() })),
        )]);
        for (slot, datum) in versions {
            datums.insert(DatumKey::from_text(Cartridge::ID, slot).unwrap(), datum);
        }
        DatumsRead {
            heads: Vec::new(),
            datums,
        }
    }

    fn pins(cartridges: &[&Cartridge]) -> FirstSeenMap {
        cartridges
            .iter()
            .map(|cartridge| {
                (
                    (cartridge.id.clone(), cartridge.version.clone()),
                    FirstSeen {
                        revision: cartridge.revision(),
                        envelope: (*cartridge).clone(),
                    },
                )
            })
            .collect()
    }

    fn datum(cartridge: &Cartridge) -> DatumRead {
        accepted(serde_json::to_value(cartridge).unwrap())
    }

    #[test]
    fn an_unseen_version_is_first_seen_and_a_document_without_identity_is_no_cartridge() {
        let v1 = cartridge("1.0.0", "Photos");
        let (view, fresh) = fold_doc(&document(vec![("1.0.0", datum(&v1))]), &pins(&[])).unwrap();
        assert_eq!(fresh, std::slice::from_ref(&v1));
        assert_eq!(view.versions[&v1.version].conflict, None);
        let anonymous = DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::new(),
        };
        assert!(fold_doc(&anonymous, &pins(&[])).is_none());
    }

    #[test]
    fn a_changed_datum_is_conflicted_and_the_first_seen_envelope_is_served() {
        let seen = cartridge("1.0.0", "Photos");
        let rewritten = cartridge("1.0.0", "Rewritten");
        let (view, fresh) = fold_doc(
            &document(vec![("1.0.0", datum(&rewritten))]),
            &pins(&[&seen]),
        )
        .unwrap();
        assert!(fresh.is_empty());
        let state = &view.versions[&seen.version];
        assert_eq!(state.envelope, seen);
        assert!(state.conflict.as_deref().unwrap().contains("first seen"));
        // The same revision is the version, unconflicted.
        let (view, _) =
            fold_doc(&document(vec![("1.0.0", datum(&seen))]), &pins(&[&seen])).unwrap();
        assert_eq!(view.versions[&seen.version].conflict, None);
    }

    #[test]
    fn unreadable_slots_are_reported_and_an_unreadable_pinned_version_is_conflicted() {
        let seen = cartridge("1.0.0", "Photos");
        let mislabelled = cartridge("2.0.0", "Photos");
        let view = fold_doc(
            &document(vec![
                ("1.0", datum(&seen)),
                ("1.1.0", datum(&mislabelled)),
                ("1.2.0", accepted(json!({ "nonsense": true }))),
                (
                    "1.0.0",
                    DatumRead::Rejected {
                        reason: "bad".into(),
                    },
                ),
            ]),
            &pins(&[&seen]),
        )
        .unwrap()
        .0;
        assert_eq!(
            view.unreadable
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["1.0", "1.1.0", "1.2.0"]
        );
        assert_eq!(
            view.versions[&seen.version].conflict.as_deref(),
            Some("rejected: bad")
        );
    }

    #[test]
    fn an_unseen_version_with_a_bad_signature_is_listed_unverified_and_never_pinned() {
        let genuine = cartridge("1.0.0", "Photos");
        let forged = sign_with_seed(cartridge("1.1.0", "Photos"), &[9; 32]);
        let mut unsigned = cartridge("1.2.0", "Photos");
        unsigned.signature = None;
        let mut web = cartridge("1.3.0", "Photos");
        web.id = big_cartridges_core::CartridgeId::new("did:web:example.com", "photos").unwrap();
        let (view, fresh) = fold_doc(
            &document(vec![
                ("1.0.0", datum(&genuine)),
                ("1.1.0", datum(&forged)),
                ("1.2.0", datum(&unsigned)),
            ]),
            &pins(&[]),
        )
        .unwrap();
        // Only the verified version is pinned as first-seen, so a bad copy cannot reserve a label.
        assert_eq!(fresh, std::slice::from_ref(&genuine));
        assert_eq!(view.versions[&genuine.version].unverified, None);
        for (version, reason) in [
            (&forged.version, "does not verify"),
            (&unsigned.version, "no signature"),
        ] {
            let reason_found = view.versions[version].unverified.as_deref().unwrap();
            assert!(reason_found.contains(reason), "{reason_found}");
        }
        // A version already pinned stays served from the pin even if the drawer's copy lost its
        // signature: the revision is the same content, and the pin was verified when it was taken.
        let (view, _) = fold_doc(
            &document(vec![(
                "1.0.0",
                datum(&{
                    let mut stripped = genuine.clone();
                    stripped.signature = None;
                    stripped
                }),
            )]),
            &pins(&[&genuine]),
        )
        .unwrap();
        let state = &view.versions[&genuine.version];
        assert_eq!(
            (state.conflict.as_deref(), state.unverified.as_deref()),
            (None, None)
        );
        assert_eq!(state.envelope, genuine);
    }
}
