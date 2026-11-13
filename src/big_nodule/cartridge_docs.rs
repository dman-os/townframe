//! The documents a cartridge's install is made of, beyond the node's own config document (ADR 044):
//! the registry documents its pin names, and its cartridge config document in the shared config
//! drawer.
//!
//! [`CartridgeDocs`] reads a pinned version out of the registry document at the pin's heads, finds
//! or creates the cartridge's config document, and keeps one [`DatumStoreHandle`] loaded per
//! installed cartridge so the state actor can snapshot them beside the node document and wake when
//! any of them changes.

use crate::store;
use big_cabinet::DatumStoreHandle;
use big_cabinet_core::DatumPin;
use big_cabinet_core::DatumTag;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use big_cabinet_core::reference::ReferenceDocId;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::ResolvedCartridge;
use big_cartridges_core::config::CartridgeConfigIdentity;
use big_cartridges_core::config::config_identity_key;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::CartridgesProtocol;
use big_cartridges_core::protocol::ResolveCartridgeVersion;
use big_nodule_core::config::Admission;
use big_nodule_core::config::AdmissionCache;
use big_nodule_core::config::CartridgeConfig;
use big_nodule_core::config::NoduleConfig;
use big_nodule_core::protocol::NodulesError;
use serde_json::json;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::Spawner;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Notify;

type Res<T> = Result<T, NodulesError>;

/// Why a pin could not be turned into a cartridge version.
pub(crate) enum PinError {
    /// The pinned heads, or the document, have not reached this node yet: a pending reference is a
    /// normal state (ADR 035 §2.4), read again on the next reconcile.
    Unavailable(String),
    /// The pin is not an install of a verified cartridge version, however often it is read.
    Invalid(String),
    /// The read itself failed in a way no refusal explains.
    Fatal(NodulesError),
}

struct LoadedDoc {
    doc: DocId,
    handle: DatumStoreHandle<CartridgeConfig>,
    stop: StopToken,
}

pub(crate) struct CartridgeDocs {
    docs: Client<DocsProtocol>,
    /// Reads the registry's documents and reads, writes and creates documents in the shared config
    /// drawer.
    capability: CapId,
    /// The cartridges service that resolves a verified version's pinned payloads, and the read
    /// handle presented to it.
    cartridges: Client<CartridgesProtocol>,
    cartridges_authorization: Authorization,
    shared_drawer: DrawerId,
    loaded: BTreeMap<CartridgeId, LoadedDoc>,
    /// The document this node made or found for each cartridge, persisted (see
    /// [`store::config_docs`]).
    known: BTreeMap<CartridgeId, DocId>,
    sql: Database,
    /// Rung whenever a loaded document changes; the state actor re-snapshots on it.
    wake: Arc<Notify>,
    tasks: Spawner,
}

impl CartridgeDocs {
    #[expect(
        clippy::too_many_arguments,
        reason = "the state actor hands over the service handles it already holds, one for one"
    )]
    pub(crate) async fn new(
        docs: Client<DocsProtocol>,
        capability: CapId,
        cartridges: Client<CartridgesProtocol>,
        cartridges_authorization: Authorization,
        shared_drawer: DrawerId,
        sql: Database,
        wake: Arc<Notify>,
        tasks: Spawner,
    ) -> Res<Self> {
        Ok(Self {
            docs,
            capability,
            cartridges,
            cartridges_authorization,
            shared_drawer,
            loaded: BTreeMap::new(),
            known: store::config_docs(&sql).await?,
            sql,
            wake,
            tasks,
        })
    }

    /// Reads the version datum `pin` names out of its registry document at the pin's heads, and
    /// checks what the registry checks before it serves a version: the envelope is well formed,
    /// signed by its namespace, and sits at the slot of its own version. The registry document
    /// may have synced in from a peer, so none of it is trusted unread.
    async fn read_envelope(&self, pin: &DatumPin) -> Result<Cartridge, PinError> {
        let reference = pin.location().reference();
        let ReferenceDocId::Concrete(doc_id) = reference.document() else {
            return Err(PinError::Invalid(
                "an install pins a registry document by id, not `self`".to_owned(),
            ));
        };
        let heads = reference.heads();
        if heads.is_empty() {
            return Err(PinError::Invalid(
                "an install pin states the document heads it selects".to_owned(),
            ));
        }
        let key = reference.key();
        if key.tag().as_str() != Cartridge::ID {
            return Err(PinError::Invalid(format!(
                "an install pins a `{}` datum, not `{}`",
                Cartridge::ID,
                key.tag()
            )));
        }
        let read = self
            .docs
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability: self.capability,
                    doc_id: doc_id.clone(),
                    at: At::Heads(heads.to_vec()),
                    datums: Some(BTreeSet::from([key.clone()])),
                    optimistic: false,
                },
            )
            .await;
        let mut read = match read {
            Ok(read) => read,
            Err(ClientError::Method(
                error @ (DocsError::DocNotFound
                | DocsError::UnknownHeads { .. }
                | DocsError::Pending { .. }),
            )) => return Err(PinError::Unavailable(error.to_string())),
            Err(ClientError::Method(error @ DocsError::Denied)) => {
                return Err(PinError::Invalid(format!(
                    "the registry document cannot be read: {error}"
                )));
            }
            Err(error) => return Err(PinError::Fatal(crate::docs_error(error))),
        };
        let value = match read.datums.remove(key) {
            Some(DatumRead::Accepted { value, .. }) => value,
            Some(DatumRead::Pending { reason, .. }) => return Err(PinError::Unavailable(reason)),
            Some(DatumRead::Absent { .. }) | None => {
                return Err(PinError::Invalid(
                    "the version is not in the registry document at the pinned heads".to_owned(),
                ));
            }
            Some(DatumRead::Rejected { reason } | DatumRead::Undecodable { reason, .. }) => {
                return Err(PinError::Invalid(format!(
                    "the pinned version datum is unusable: {reason}"
                )));
            }
        };
        let cartridge: Cartridge = serde_json::from_value(value)
            .map_err(|error| PinError::Invalid(format!("invalid cartridge envelope: {error}")))?;
        cartridge
            .validate()
            .map_err(|error| PinError::Invalid(error.to_string()))?;
        cartridge
            .verify_signature()
            .map_err(|error| PinError::Invalid(error.to_string()))?;
        if key.slot_bytes() != cartridge.version.to_string().as_bytes() {
            return Err(PinError::Invalid(format!(
                "the pinned datum sits at a slot other than its own version {}",
                cartridge.version
            )));
        }
        Ok(cartridge)
    }

    /// The version `pin` names, with the payloads its envelope pins, as the cartridges service
    /// resolves them. The envelope is read and verified at the pin first: the pin selects an exact
    /// revision, which the service's own view of the version must agree with. The service does not
    /// run nodule admission; the caller does, on this result.
    pub(crate) async fn read_pin(&self, pin: &DatumPin) -> Result<ResolvedCartridge, PinError> {
        let cartridge = self.read_envelope(pin).await?;
        let resolved = self
            .cartridges
            .call_with(
                RequestContext::root(),
                self.cartridges_authorization.metadata(),
                ResolveCartridgeVersion {
                    id: cartridge.id.clone(),
                    version: cartridge.version.clone(),
                },
            )
            .await
            .map_err(|error| match error {
                // The registry has not projected the version yet, or the payloads its pins name
                // have not reached this node: both clear once sync catches up.
                ClientError::Method(
                    error @ (CartridgesError::UnknownCartridge(_)
                    | CartridgesError::UnknownVersion { .. }
                    | CartridgesError::PinnedDatumUnreadable { .. }),
                ) => PinError::Unavailable(error.to_string()),
                ClientError::Method(
                    error @ (CartridgesError::Rejected(_)
                    | CartridgesError::PinnedDatumInvalid { .. }
                    | CartridgesError::PinnedDocumentMismatch { .. }),
                ) => PinError::Invalid(error.to_string()),
                error => PinError::Fatal(crate::cartridges_error(error)),
            })?;
        if resolved.cartridge.revision() != cartridge.revision() {
            return Err(PinError::Invalid(format!(
                "the registry resolves version {} of `{}` to another revision than the pinned one",
                cartridge.version, cartridge.id
            )));
        }
        Ok(resolved)
    }

    /// The config document an earlier install of `cartridge_id` left: the one this node made or
    /// found before, else one in the shared config drawer (a sister's that synced in), found by
    /// its identity datum. The one with the lowest id when two were created before they met, as
    /// the registry picks among duplicates.
    pub(crate) async fn find(&mut self, cartridge_id: &CartridgeId) -> Res<Option<DocId>> {
        if let Some(doc) = self.known.get(cartridge_id) {
            return Ok(Some(doc.clone()));
        }
        let Some(doc) = self.scan(cartridge_id).await? else {
            return Ok(None);
        };
        self.remember(cartridge_id, &doc).await?;
        Ok(Some(doc))
    }

    async fn remember(&mut self, cartridge_id: &CartridgeId, doc: &DocId) -> Res<()> {
        store::upsert_config_doc(&self.sql, cartridge_id, doc).await?;
        self.known.insert(cartridge_id.clone(), doc.clone());
        Ok(())
    }

    async fn scan(&self, cartridge_id: &CartridgeId) -> Res<Option<DocId>> {
        let tag = DatumTag::new(CartridgeConfigIdentity::ID).expect("the identity tag is valid");
        let predicate = Predicate::And {
            clauses: vec![
                Predicate::InDrawer {
                    drawer_id: self.shared_drawer.clone(),
                    transitive: false,
                    at_least: DrawerAccess::Read,
                },
                Predicate::HasTag { tag: tag.clone() },
            ],
        };
        let datums = DatumSelection::Of {
            tags: BTreeSet::from([tag]),
            keys: BTreeSet::new(),
        };
        let mut candidates = BTreeSet::new();
        let mut after = 0;
        loop {
            let page = self
                .docs
                .call(
                    RequestContext::root(),
                    PollDatums {
                        capability: self.capability,
                        predicate: predicate.clone(),
                        datums: datums.clone(),
                        after,
                        limit: 0,
                        hold_ms: 0,
                    },
                )
                .await
                .map_err(crate::docs_error)?;
            for event in page.events {
                match event {
                    DatumEvent::Datum { doc_id, .. } => candidates.insert(doc_id),
                    DatumEvent::Leave { doc_id, .. } => candidates.remove(&doc_id),
                };
            }
            after = page.through;
            if page.caught_up {
                break;
            }
        }
        let identity = config_identity_key();
        for doc_id in candidates {
            let mut read = self
                .docs
                .call(
                    RequestContext::root(),
                    ReadDatums {
                        capability: self.capability,
                        doc_id: doc_id.clone(),
                        at: At::Current,
                        datums: Some(BTreeSet::from([identity.clone()])),
                        optimistic: false,
                    },
                )
                .await
                .map_err(crate::docs_error)?;
            // A document whose identity is unreadable here is some other cartridge's, or not yet
            // synced: not this one's to reuse.
            if let Some(DatumRead::Accepted { value, .. }) = read.datums.remove(&identity)
                && serde_json::from_value::<CartridgeConfigIdentity>(value)
                    .is_ok_and(|named| &named.cartridge == cartridge_id)
            {
                return Ok(Some(doc_id));
            }
        }
        Ok(None)
    }

    /// A new config document for `cartridge_id` in the shared config drawer, naming it.
    pub(crate) async fn create(&mut self, cartridge_id: &CartridgeId) -> Res<DocId> {
        let receipt = self
            .docs
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: self.capability,
                    command: CommandId::try_from(format!(
                        "cartridge-config/create/{}",
                        Uuid7::new()
                    ))
                    .expect("a generated command id is within bounds"),
                    placement: format!("drawer {}", self.shared_drawer),
                    datums: BTreeMap::from([(
                        config_identity_key(),
                        json!({ "cartridge": cartridge_id.to_string() }),
                    )]),
                },
            )
            .await
            .map_err(crate::docs_error)?;
        self.remember(cartridge_id, &receipt.doc_id).await?;
        Ok(receipt.doc_id)
    }

    /// Reads each install's pinned version that the admission cache has no settled outcome for.
    /// A pin whose heads have not reached this node is recorded as refused and read again on the
    /// next reconcile; any other unusable pin is refused for good, as a malformed install is.
    pub(crate) async fn read_pins(
        &self,
        node: &NoduleConfig,
        cache: &AdmissionCache,
    ) -> Result<(), NodulesError> {
        for (cartridge_id, entry) in node.entries() {
            let Some(install) = entry.install.accepted() else {
                continue;
            };
            if !cache.needs_read(cartridge_id, &install.cartridge) {
                continue;
            }
            let (admission, settled) = match self.read_pin(&install.cartridge).await {
                Ok(resolved) if &resolved.cartridge.id != cartridge_id => (
                    Admission::Refused {
                        version: Some(resolved.cartridge.version),
                        revision: None,
                        reason: format!(
                            "install datum key `{cartridge_id}` does not match the pinned envelope's id `{}`",
                            resolved.cartridge.id
                        ),
                    },
                    true,
                ),
                Ok(resolved) => (Admission::of(&resolved), true),
                Err(PinError::Unavailable(reason)) => (
                    Admission::Refused {
                        version: None,
                        revision: None,
                        reason: format!("the pinned version is not available yet: {reason}"),
                    },
                    false,
                ),
                Err(PinError::Invalid(reason)) => (
                    Admission::Refused {
                        version: None,
                        revision: None,
                        reason,
                    },
                    true,
                ),
                Err(PinError::Fatal(error)) => return Err(error),
            };
            cache.insert(cartridge_id, install.cartridge.clone(), admission, settled);
        }
        Ok(())
    }

    /// The handle on `cartridge_id`'s config document `doc`, loaded now if it was not.
    pub(crate) async fn handle(
        &mut self,
        cartridge_id: &CartridgeId,
        doc: &DocId,
    ) -> Result<DatumStoreHandle<CartridgeConfig>, StoreError> {
        if let Some(loaded) = self.loaded.get(cartridge_id)
            && &loaded.doc == doc
        {
            return Ok(loaded.handle.clone());
        }
        if let Some(stale) = self.loaded.remove(cartridge_id) {
            stale.stop.stop().await;
        }
        let (handle, stop) = DatumStoreHandle::<CartridgeConfig>::load(
            &RequestContext::root(),
            self.docs.clone(),
            self.capability,
            doc.clone(),
        )
        .await?;
        let mut watch = handle.watch().await?;
        let wake = Arc::clone(&self.wake);
        // Ends when the document's store stops, which `stop_all` and `sync` do.
        self.tasks
            .spawn(async move {
                while watch.changed().await.is_ok() {
                    wake.notify_one();
                }
            })
            .expect("a config document is loaded before shutdown");
        self.loaded.insert(
            cartridge_id.clone(),
            LoadedDoc {
                doc: doc.clone(),
                handle: handle.clone(),
                stop,
            },
        );
        Ok(handle)
    }

    /// Brings the loaded documents to `wanted`, the document each installed cartridge names: a
    /// document no install names any more is released, and one that cannot be loaded yet (it has
    /// not synced to this node) is left out, which resolves its cartridge with no grants.
    pub(crate) async fn sync(&mut self, wanted: &BTreeMap<CartridgeId, DocId>) -> Res<()> {
        let released: Vec<CartridgeId> = self
            .loaded
            .keys()
            .filter(|cartridge_id| !wanted.contains_key(*cartridge_id))
            .cloned()
            .collect();
        for cartridge_id in released {
            let loaded = self
                .loaded
                .remove(&cartridge_id)
                .expect("selected from the loaded documents");
            loaded.stop.stop().await;
        }
        for (cartridge_id, doc) in wanted {
            match self.handle(cartridge_id, doc).await {
                Ok(_) => {}
                Err(StoreError::Docs(DocsError::DocNotFound)) => {
                    tracing::warn!(%cartridge_id, %doc, "a cartridge config document is not on this node yet");
                }
                Err(error) => return Err(crate::store_error(error)),
            }
        }
        Ok(())
    }

    /// The loaded documents as they are now, and their heads in cartridge order.
    pub(crate) async fn snapshots(
        &self,
    ) -> Res<(Vec<ChangeHash>, BTreeMap<CartridgeId, CartridgeConfig>)> {
        let mut heads = Vec::new();
        let mut docs = BTreeMap::new();
        for (cartridge_id, loaded) in &self.loaded {
            let snapshot = loaded.handle.snapshot().await.map_err(crate::store_error)?;
            heads.extend(snapshot.heads);
            docs.insert(cartridge_id.clone(), snapshot.value);
        }
        Ok((heads, docs))
    }

    pub(crate) async fn stop_all(&mut self) {
        for loaded in std::mem::take(&mut self.loaded).into_values() {
            loaded.stop.stop().await;
        }
    }
}
