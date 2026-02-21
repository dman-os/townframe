// FIXME: use nested ids for facet keys

use crate::interlude::*;

pub mod lru;
pub mod types;

pub use types::{DocEntry, DocEntryDiff, DocNBranches, DrawerEvent};

use lru::SharedKeyedLruPool;
use types::{DrawerError, FacetBlame, UpdateDocArgsV2, UpdateDocBatchErrV2};

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use daybook_types::doc::{AddDocArgs, ChangeHashSet, Doc, DocId, DocPatch, FacetKey, FacetRaw};
use daybook_types::url::{parse_facet_ref, FACET_SELF_DOC_ID};
mod facet_recovery;

// FIXME: refactor by hand?
pub mod dmeta {
    use crate::interlude::*;
    use automerge::transaction::Transactable;
    use automerge::ReadDoc;
    use daybook_types::doc::{
        ChangeHashSet, FacetKey, FacetMeta, WellKnownFacet, WellKnownFacetTag,
    };

    fn dmeta_key() -> String {
        // FIXME: make it const or make as_str const
        format!("{}/main", WellKnownFacetTag::Dmeta.as_str())
    }

    fn timestamp_scalar(now: Timestamp) -> automerge::ScalarValue {
        automerge::ScalarValue::Timestamp(now.as_second())
    }

    pub fn facet_meta_obj<D: ReadDoc>(
        doc: &D,
        facet_key: &FacetKey,
    ) -> Res<Option<automerge::ObjId>> {
        let facets_obj = match doc.get(automerge::ROOT, "facets")? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
            _ => return Ok(None),
        };

        let key = dmeta_key();
        let dmeta_obj = match doc.get(&facets_obj, &key)? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
            _ => return Ok(None),
        };

        let dmeta_facets_obj = match doc.get(&dmeta_obj, "facets")? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
            _ => return Ok(None),
        };

        let facet_key_str = facet_key.to_string();
        match doc.get(&dmeta_facets_obj, facet_key_str)? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => Ok(Some(id)),
            _ => Ok(None),
        }
    }

    pub fn facet_uuid_for_key<D: ReadDoc + autosurgeon::ReadDoc>(
        doc: &D,
        facet_key: &FacetKey,
    ) -> Res<Option<Uuid>> {
        let Some(facet_meta_obj) = facet_meta_obj(doc, facet_key)? else {
            return Ok(None);
        };
        match autosurgeon::hydrate_prop::<_, Option<Vec<Uuid>>, _, _>(doc, &facet_meta_obj, "uuid")
        {
            Ok(Some(uuids)) => Ok(uuids.into_iter().next()),
            _ => Ok(None),
        }
    }

    pub fn facet_heads_for_key(
        doc: &automerge::Automerge,
        facet_key: &FacetKey,
    ) -> Res<ChangeHashSet> {
        let heads = super::facet_recovery::recover_facet_heads(doc, facet_key)?;
        Ok(ChangeHashSet(Arc::from(heads)))
    }

    fn load_dmeta(
        tx: &mut automerge::transaction::Transaction,
        facets_obj: &automerge::ObjId,
    ) -> Res<(automerge::ObjId, automerge::ObjId, automerge::ObjId)> {
        let key = dmeta_key();
        let dmeta_obj = match tx.get(facets_obj, &key)? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
            _ => eyre::bail!("dmeta facet map not found"),
        };
        let dmeta_facets_obj = match tx.get(&dmeta_obj, "facets")? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
            _ => eyre::bail!("dmeta.facets map not found"),
        };
        let dmeta_facet_uuids_obj = match tx.get(&dmeta_obj, "facetUuids")? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
            _ => eyre::bail!("dmeta.facetUuids map not found"),
        };
        Ok((dmeta_obj, dmeta_facets_obj, dmeta_facet_uuids_obj))
    }

    fn set_updated_at_list(
        tx: &mut automerge::transaction::Transaction,
        obj: &automerge::ObjId,
        prop: &str,
        now: Timestamp,
    ) -> Res<()> {
        let updated_at_list = match tx.get(obj, prop)? {
            Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
            _ => eyre::bail!("missing or invalid {prop} list"),
        };

        let len = tx.length(&updated_at_list);
        for _ in 0..len {
            tx.delete(&updated_at_list, 0)?;
        }
        tx.insert(&updated_at_list, 0, timestamp_scalar(now))?;
        Ok(())
    }

    pub fn ensure_for_add(
        tx: &mut automerge::transaction::Transaction,
        facets_obj: &automerge::ObjId,
        facet_keys: &[FacetKey],
        now: Timestamp,
    ) -> Res<()> {
        let key = dmeta_key();
        let doc_id = match tx.get(automerge::ROOT, "id")? {
            Some((automerge::Value::Scalar(doc_id_scalar), _)) => {
                if let automerge::ScalarValue::Str(doc_id_str) = doc_id_scalar.as_ref() {
                    doc_id_str.to_string()
                } else {
                    eyre::bail!("content doc id is not a string");
                }
            }
            _ => eyre::bail!("content doc id not found"),
        };
        let mut facet_uuids = HashMap::new();
        let mut facets = HashMap::new();
        for facet_key in facet_keys {
            let facet_uuid = Uuid::new_v4();
            facet_uuids.insert(facet_uuid, facet_key.clone());
            facets.insert(
                facet_key.clone(),
                FacetMeta {
                    created_at: now,
                    uuid: vec![facet_uuid],
                    updated_at: vec![now],
                },
            );
        }
        autosurgeon::reconcile_prop(
            tx,
            facets_obj,
            &*key,
            ThroughJson(WellKnownFacet::Dmeta(daybook_types::doc::Dmeta {
                id: doc_id,
                created_at: now,
                updated_at: vec![now],
                facet_uuids,
                facets,
            })),
        )?;

        Ok(())
    }

    fn remove_facet_meta(
        tx: &mut automerge::transaction::Transaction,
        dmeta_facets_obj: &automerge::ObjId,
        dmeta_facet_uuids_obj: &automerge::ObjId,
        key_str: &str,
    ) -> Res<Vec<Uuid>> {
        let mut invalidated_uuids = Vec::new();
        if let Some((automerge::Value::Object(automerge::ObjType::Map), facet_meta_obj)) =
            tx.get(dmeta_facets_obj, key_str)?
        {
            if let Some((automerge::Value::Object(automerge::ObjType::List), uuid_list)) =
                tx.get(&facet_meta_obj, "uuid")?
            {
                let len = tx.length(&uuid_list);
                for ii in 0..len {
                    if let Some((automerge::Value::Scalar(uuid_scalar), _)) =
                        tx.get(&uuid_list, ii)?
                    {
                        if let automerge::ScalarValue::Str(uuid_str) = uuid_scalar.as_ref() {
                            if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                                invalidated_uuids.push(uuid);
                                tx.delete(dmeta_facet_uuids_obj, uuid.to_string())?;
                            }
                        }
                    }
                }
            }
        }
        tx.delete(dmeta_facets_obj, key_str)?;
        Ok(invalidated_uuids)
    }

    fn touch_facet_meta(
        tx: &mut automerge::transaction::Transaction,
        dmeta_facets_obj: &automerge::ObjId,
        dmeta_facet_uuids_obj: &automerge::ObjId,
        key_str: &str,
        now: Timestamp,
    ) -> Res<Uuid> {
        let (facet_meta_obj, is_new_meta) = match tx.get(dmeta_facets_obj, key_str)? {
            Some((automerge::Value::Object(automerge::ObjType::Map), id)) => (id, false),
            _ => (
                tx.put_object(dmeta_facets_obj, key_str, automerge::ObjType::Map)?,
                true,
            ),
        };

        if is_new_meta {
            tx.put(&facet_meta_obj, "createdAt", timestamp_scalar(now))?;
        } else if tx.get(&facet_meta_obj, "createdAt")?.is_none() {
            eyre::bail!("facet meta missing createdAt for key {key_str}");
        }

        let updated_at_list = match tx.get(&facet_meta_obj, "updatedAt")? {
            Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
            _ if is_new_meta => {
                tx.put_object(&facet_meta_obj, "updatedAt", automerge::ObjType::List)?
            }
            _ => eyre::bail!("facet meta missing updatedAt list for key {key_str}"),
        };

        let uuid_list = match tx.get(&facet_meta_obj, "uuid")? {
            Some((automerge::Value::Object(automerge::ObjType::List), id)) => id,
            _ if is_new_meta => tx.put_object(&facet_meta_obj, "uuid", automerge::ObjType::List)?,
            _ => eyre::bail!("facet meta missing uuid list for key {key_str}"),
        };
        let facet_uuid = if tx.length(&uuid_list) > 0 {
            match tx.get(&uuid_list, 0)? {
                Some((automerge::Value::Scalar(uuid_scalar), _)) => {
                    if let automerge::ScalarValue::Str(uuid_str) = uuid_scalar.as_ref() {
                        Uuid::parse_str(uuid_str)?
                    } else {
                        eyre::bail!("facet meta uuid is not a string for key {key_str}")
                    }
                }
                _ => eyre::bail!("facet meta uuid entry missing for key {key_str}"),
            }
        } else if is_new_meta {
            let uuid = Uuid::new_v4();
            tx.insert(&uuid_list, 0, uuid.to_string())?;
            uuid
        } else {
            eyre::bail!("facet meta uuid list empty for key {key_str}");
        };
        tx.put(dmeta_facet_uuids_obj, facet_uuid.to_string(), key_str)?;

        let len = tx.length(&updated_at_list);
        for _ in 0..len {
            tx.delete(&updated_at_list, 0)?;
        }
        tx.insert(&updated_at_list, 0, timestamp_scalar(now))?;

        Ok(facet_uuid)
    }

    pub fn apply_update(
        tx: &mut automerge::transaction::Transaction,
        facets_obj: &automerge::ObjId,
        facet_keys_set: &[FacetKey],
        facet_keys_remove: &[FacetKey],
        now: Timestamp,
    ) -> Res<Vec<Uuid>> {
        let (dmeta_obj, dmeta_facets_obj, dmeta_facet_uuids_obj) = load_dmeta(tx, facets_obj)?;
        set_updated_at_list(tx, &dmeta_obj, "updatedAt", now)?;
        let mut invalidated_uuids = Vec::new();

        for key in facet_keys_remove {
            let key_str = key.to_string();
            invalidated_uuids.extend(remove_facet_meta(
                tx,
                &dmeta_facets_obj,
                &dmeta_facet_uuids_obj,
                &key_str,
            )?);
        }

        for key in facet_keys_set {
            let key_str = key.to_string();
            let facet_uuid =
                touch_facet_meta(tx, &dmeta_facets_obj, &dmeta_facet_uuids_obj, &key_str, now)?;
            invalidated_uuids.push(facet_uuid);
        }

        Ok(invalidated_uuids)
    }

    pub fn apply_merge(
        tx: &mut automerge::transaction::Transaction,
        facets_obj: &automerge::ObjId,
        modified_facet_key_strs: &std::collections::HashSet<String>,
        now: Timestamp,
    ) -> Res<Vec<Uuid>> {
        if modified_facet_key_strs.is_empty() {
            return Ok(Vec::new());
        }
        let (dmeta_obj, dmeta_facets_obj, dmeta_facet_uuids_obj) = load_dmeta(tx, facets_obj)?;
        set_updated_at_list(tx, &dmeta_obj, "updatedAt", now)?;
        let mut invalidated_uuids = Vec::new();
        for key_str in modified_facet_key_strs {
            let facet_uuid =
                touch_facet_meta(tx, &dmeta_facets_obj, &dmeta_facet_uuids_obj, key_str, now)?;
            invalidated_uuids.push(facet_uuid);
        }
        Ok(invalidated_uuids)
    }
}

use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use utils_rs::am::changes::ChangeNotification;

type FacetCacheKey = (DocId, Uuid);

struct FacetCacheEntry {
    heads: ChangeHashSet,
    value: daybook_types::doc::ArcFacetRaw,
}

struct ValidatedReference {
    doc_id: DocId,
    facet_key: FacetKey,
    url_value: String,
}

fn parse_commit_heads_array(
    values: &[serde_json::Value],
    origin_facet_key: &FacetKey,
    at_commit_json_path: &str,
) -> Res<()> {
    let mut commit_head_strings = Vec::with_capacity(values.len());
    for value in values {
        let serde_json::Value::String(commit_head) = value else {
            eyre::bail!(
                "facet '{}' at_commit path '{}' must be an array of commit-hash strings",
                origin_facet_key,
                at_commit_json_path
            );
        };
        commit_head_strings.push(commit_head.clone());
    }
    utils_rs::am::parse_commit_heads(&commit_head_strings).wrap_err_with(|| {
        format!(
            "facet '{}' at_commit path '{}' contains invalid commit hash values",
            origin_facet_key, at_commit_json_path
        )
    })?;
    Ok(())
}

pub struct DrawerRepo {
    pub acx: AmCtx,
    drawer_doc_id: DocumentId,
    local_actor_id: automerge::ActorId,

    // LRU Caches
    entry_cache: Arc<DHashMap<DocId, DocEntry>>,
    facet_cache: std::sync::Mutex<FacetCacheState>,
    handles: Arc<DHashMap<DocId, samod::DocHandle>>,

    // LRU Pools (Policy only)
    entry_pool: SharedKeyedLruPool<DocId>,

    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
    current_heads: std::sync::RwLock<ChangeHashSet>,
    drawer_am_handle: samod::DocHandle,
    plugs_repo: std::sync::RwLock<Option<Arc<crate::plugs::PlugsRepo>>>,
}

struct FacetCacheState {
    entries: HashMap<FacetCacheKey, FacetCacheEntry>,
    by_doc: HashMap<DocId, HashSet<Uuid>>,
    pool: SharedKeyedLruPool<FacetCacheKey>,
    seen_once: HashSet<FacetCacheKey>,
    seen_order: std::collections::VecDeque<FacetCacheKey>,
    seen_capacity: usize,
}

impl FacetCacheState {
    fn new(pool: SharedKeyedLruPool<FacetCacheKey>) -> Self {
        Self {
            entries: HashMap::new(),
            by_doc: HashMap::new(),
            pool,
            seen_once: HashSet::new(),
            seen_order: std::collections::VecDeque::new(),
            seen_capacity: 4096,
        }
    }

    fn estimate_cost(value: &FacetRaw) -> usize {
        serde_json::to_vec(value)
            .map(|bytes| bytes.len().max(128))
            .unwrap_or(1024)
    }

    fn remember_seen_once(&mut self, key: FacetCacheKey) {
        if self.seen_once.contains(&key) {
            return;
        }
        self.seen_once.insert(key.clone());
        self.seen_order.push_back(key);
        while self.seen_order.len() > self.seen_capacity {
            if let Some(evicted) = self.seen_order.pop_front() {
                self.seen_once.remove(&evicted);
            }
        }
    }

    fn get_if_heads_match(
        &mut self,
        doc_id: &DocId,
        facet_uuid: &Uuid,
        heads: &ChangeHashSet,
    ) -> Option<daybook_types::doc::ArcFacetRaw> {
        let key = (doc_id.clone(), *facet_uuid);
        let cached = self.entries.get(&key)?;
        if &cached.heads != heads {
            return None;
        }
        self.pool.lock().unwrap().touch_key(&key);
        Some(Arc::clone(&cached.value))
    }

    fn put(
        &mut self,
        doc_id: &DocId,
        facet_uuid: Uuid,
        facet_heads: ChangeHashSet,
        value: daybook_types::doc::ArcFacetRaw,
    ) {
        let cache_key = (doc_id.clone(), facet_uuid);
        if let Some(existing_entry) = self.entries.get_mut(&cache_key) {
            existing_entry.heads = facet_heads;
            existing_entry.value = value;
            self.pool.lock().unwrap().touch_key(&cache_key);
            self.seen_once.remove(&cache_key);
            return;
        }

        if !self.seen_once.remove(&cache_key) {
            self.remember_seen_once(cache_key);
            return;
        }

        let cost = Self::estimate_cost(value.as_ref());
        let pruned = self.pool.lock().unwrap().insert_key(&cache_key, cost);
        for pkey in pruned {
            self.remove_without_pool(&pkey);
        }

        self.entries.insert(
            cache_key.clone(),
            FacetCacheEntry {
                heads: facet_heads,
                value,
            },
        );
        self.by_doc
            .entry(doc_id.clone())
            .or_default()
            .insert(facet_uuid);
    }

    fn invalidate_facet(&mut self, doc_id: &DocId, facet_uuid: &Uuid) {
        let key = (doc_id.clone(), *facet_uuid);
        self.pool.lock().unwrap().remove_key(&key);
        self.remove_without_pool(&key);
    }

    fn invalidate_doc(&mut self, doc_id: &DocId) {
        let Some(uuids) = self.by_doc.get(doc_id).cloned() else {
            return;
        };
        let keys: Vec<FacetCacheKey> = uuids
            .into_iter()
            .map(|uuid| (doc_id.clone(), uuid))
            .collect();
        self.pool.lock().unwrap().remove_keys(keys.clone());
        for key in keys {
            self.remove_without_pool(&key);
        }
    }

    fn remove_without_pool(&mut self, key: &FacetCacheKey) {
        let removed = self.entries.remove(key);
        self.seen_once.remove(key);
        if removed.is_none() {
            return;
        }
        let (doc_id, facet_uuid) = key;
        if let Some(per_doc) = self.by_doc.get_mut(doc_id) {
            per_doc.remove(facet_uuid);
            if per_doc.is_empty() {
                self.by_doc.remove(doc_id);
            }
        }
    }
}

impl DrawerRepo {
    pub async fn load(
        acx: AmCtx,
        drawer_doc_id: DocumentId,
        local_actor_id: automerge::ActorId,
        entry_pool: SharedKeyedLruPool<DocId>,
        doc_pool: SharedKeyedLruPool<FacetCacheKey>,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let drawer_am_handle = acx
            .find_doc(&drawer_doc_id)
            .await?
            .ok_or_eyre("drawer doc not found")?;

        let initial_heads =
            drawer_am_handle.with_document(|doc| ChangeHashSet(doc.get_heads().into()));

        let (broker, broker_stop) = {
            acx.change_manager()
                .add_doc(drawer_am_handle.clone())
                .await?
        };

        let (notif_tx, notif_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<ChangeNotification>>();

        // Listen for changes to docs.map
        let ticket = acx
            .change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    doc_id: Some(broker.filter()),
                    path: vec!["docs".into(), "map".into()],
                },
                Box::new(move |notifs| {
                    if let Err(err) = notif_tx.send(notifs) {
                        warn!("failed to send change notifications: {err}");
                    }
                }),
            )
            .await;

        let main_cancel_token = CancellationToken::new();
        let repo = Arc::new(Self {
            acx,
            drawer_doc_id,
            local_actor_id,
            entry_cache: Arc::new(DHashMap::new()),
            facet_cache: std::sync::Mutex::new(FacetCacheState::new(doc_pool)),
            handles: Arc::new(DHashMap::new()),
            entry_pool,
            registry: crate::repos::ListenersRegistry::new(),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
            current_heads: std::sync::RwLock::new(initial_heads),
            drawer_am_handle,
            plugs_repo: std::sync::RwLock::new(None),
        });

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                repo.handle_notifs(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
                broker_stop_tokens: broker_stop.into_iter().collect(),
            },
        ))
    }

    async fn handle_notifs(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<ChangeNotification>>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                msg = notif_rx.recv() => {
                    match msg {
                        Some(notifs) => notifs,
                        None => break,
                    }
                }
            };

            events.clear();
            let mut last_heads = None;

            for notif in notifs {
                last_heads = Some(ChangeHashSet(Arc::clone(&notif.heads)));

                // Skip local changes
                if let Some(actor_id) = utils_rs::am::get_actor_id_from_patch(&notif.patch) {
                    if actor_id == self.local_actor_id {
                        continue;
                    }
                }

                if let Err(err) = self
                    .events_for_patch(&notif.patch, &notif.heads, &mut events)
                    .await
                {
                    if cancel_token.is_cancelled() || self.cancel_token.is_cancelled() {
                        return Ok(());
                    }
                    return Err(err);
                }
            }

            if !events.is_empty() {
                let drawer_heads = last_heads.expect("notifs not empty");
                *self.current_heads.write().unwrap() = drawer_heads.clone();

                // Invalidate caches for updated docs
                for event in &events {
                    match event {
                        DrawerEvent::DocUpdated { id, .. } | DrawerEvent::DocAdded { id, .. } => {
                            self.invalidate_entry_cache(id);
                            self.invalidate_facet_cache_doc(id);
                        }
                        DrawerEvent::DocDeleted { id, .. } => {
                            self.invalidate_entry_cache(id);
                            self.invalidate_facet_cache_doc(id);
                        }
                        _ => {}
                    }
                }

                self.registry.notify(
                    events
                        .drain(..)
                        .chain(std::iter::once(DrawerEvent::ListChanged { drawer_heads })),
                );
            }
        }
        Ok(())
    }

    pub fn set_plugs_repo(&self, plugs_repo: Arc<crate::plugs::PlugsRepo>) {
        *self.plugs_repo.write().unwrap() = Some(plugs_repo);
    }

    async fn facet_manifest_for_tag(
        &self,
        facet_tag: &str,
    ) -> Option<crate::plugs::manifest::FacetManifest> {
        let plugs_repo = self.plugs_repo.read().unwrap().clone();
        if let Some(plugs_repo) = plugs_repo {
            return plugs_repo.get_facet_manifest_by_tag(facet_tag).await;
        }

        static SYSTEM_FACET_MANIFESTS: std::sync::OnceLock<
            HashMap<String, crate::plugs::manifest::FacetManifest>,
        > = std::sync::OnceLock::new();
        let system_facet_manifests = SYSTEM_FACET_MANIFESTS.get_or_init(|| {
            let mut out = HashMap::new();
            for plug_manifest in crate::plugs::system_plugs() {
                for facet_manifest in plug_manifest.facets {
                    out.insert(facet_manifest.key_tag.to_string(), facet_manifest);
                }
            }
            out
        });
        system_facet_manifests.get(facet_tag).cloned()
    }

    pub async fn validate_facets(
        &self,
        incoming_facets: &HashMap<FacetKey, daybook_types::doc::ArcFacetRaw>,
        resulting_facet_keys: &HashSet<FacetKey>,
    ) -> Res<()> {
        for (facet_key, facet_value) in incoming_facets {
            let facet_tag = facet_key.tag.to_string();
            let Some(facet_manifest) = self.facet_manifest_for_tag(&facet_tag).await else {
                eyre::bail!(
                    "facet tag '{}' has no registered manifest in plugs repo",
                    facet_tag
                );
            };

            let schema_json = serde_json::to_value(&facet_manifest.value_schema)?;
            let validator = jsonschema::validator_for(&schema_json)?;
            if let Err(validation_error) = validator.validate(facet_value.as_ref()) {
                eyre::bail!(
                    "facet '{}' failed schema validation: {}",
                    facet_key,
                    validation_error
                );
            }

            for reference_manifest in &facet_manifest.references {
                self.validate_facet_reference(
                    resulting_facet_keys,
                    facet_key,
                    facet_value.as_ref(),
                    reference_manifest,
                )?;
            }
        }
        Ok(())
    }

    fn validate_facet_reference(
        &self,
        resulting_facet_keys: &HashSet<FacetKey>,
        origin_facet_key: &FacetKey,
        origin_facet_value: &FacetRaw,
        reference_manifest: &crate::plugs::manifest::FacetReferenceManifest,
    ) -> Res<()> {
        let selected_values = crate::plugs::reference::select_json_path_values(
            origin_facet_value,
            &reference_manifest.json_path,
        )?;
        if selected_values.is_empty() {
            eyre::bail!(
                "facet '{}' reference path '{}' is missing",
                origin_facet_key,
                reference_manifest.json_path
            );
        }

        let mut referenced_facets = Vec::new();
        for selected_value in selected_values {
            match selected_value {
                serde_json::Value::String(url_value) => {
                    referenced_facets
                        .push(self.validate_reference_url(url_value, origin_facet_key)?);
                }
                serde_json::Value::Array(url_values) => {
                    for url_value in url_values {
                        let serde_json::Value::String(url_string) = url_value else {
                            eyre::bail!(
                                "facet '{}' reference path '{}' must contain URL strings",
                                origin_facet_key,
                                reference_manifest.json_path
                            );
                        };
                        referenced_facets
                            .push(self.validate_reference_url(url_string, origin_facet_key)?);
                    }
                }
                _ => {
                    eyre::bail!(
                        "facet '{}' reference path '{}' must contain URL strings",
                        origin_facet_key,
                        reference_manifest.json_path
                    );
                }
            }
        }

        if let Some(at_commit_json_path) = &reference_manifest.at_commit_json_path {
            let at_commit_values = crate::plugs::reference::select_json_path_values(
                origin_facet_value,
                at_commit_json_path,
            )?;
            if at_commit_values.is_empty() {
                eyre::bail!(
                    "facet '{}' at_commit path '{}' is missing",
                    origin_facet_key,
                    at_commit_json_path
                );
            }
            if at_commit_values.len() != 1 {
                eyre::bail!(
                    "facet '{}' at_commit path '{}' must resolve to a single value",
                    origin_facet_key,
                    at_commit_json_path
                );
            }

            let self_reference_mode = match at_commit_values[0] {
                serde_json::Value::Array(values) => {
                    if values.is_empty() {
                        true
                    } else {
                        parse_commit_heads_array(values, origin_facet_key, at_commit_json_path)?;
                        false
                    }
                }
                _ => {
                    eyre::bail!(
                        "facet '{}' at_commit path '{}' must be an array of commit hashes",
                        origin_facet_key,
                        at_commit_json_path
                    );
                }
            };

            if self_reference_mode {
                for referenced_facet in referenced_facets {
                    if referenced_facet.doc_id == FACET_SELF_DOC_ID
                        && !resulting_facet_keys.contains(&referenced_facet.facet_key)
                    {
                        eyre::bail!(
                            "facet '{}' self-reference target '{}' must exist in validated facet set",
                            origin_facet_key,
                            referenced_facet.facet_key
                        );
                    }
                }
            }
        } else {
            for referenced_facet in referenced_facets {
                let parsed_url = url::Url::parse(&referenced_facet.url_value)?;
                let Some(fragment) = parsed_url.fragment() else {
                    eyre::bail!(
                        "facet '{}' reference '{}' must include commit heads in URL fragment when at_commit_json_path is not declared",
                        origin_facet_key,
                        referenced_facet.url_value
                    );
                };
                let commit_head_strings: Vec<String> = fragment
                    .split('|')
                    .filter(|segment| !segment.is_empty())
                    .map(ToString::to_string)
                    .collect();
                if commit_head_strings.is_empty() {
                    eyre::bail!(
                        "facet '{}' reference '{}' has empty commit-head fragment",
                        origin_facet_key,
                        referenced_facet.url_value
                    );
                }
                utils_rs::am::parse_commit_heads(&commit_head_strings).wrap_err_with(|| {
                    format!(
                        "facet '{}' reference '{}' has invalid commit-head fragment",
                        origin_facet_key, referenced_facet.url_value
                    )
                })?;
            }
        }

        Ok(())
    }

    fn validate_reference_url(
        &self,
        url_value: &str,
        origin_facet_key: &FacetKey,
    ) -> Res<ValidatedReference> {
        let parsed_url = url::Url::parse(url_value).wrap_err_with(|| {
            format!(
                "facet '{}' contains invalid reference URL '{}'",
                origin_facet_key, url_value
            )
        })?;
        let parsed_facet_ref = parse_facet_ref(&parsed_url).wrap_err_with(|| {
            format!(
                "facet '{}' contains invalid facet reference URL '{}'",
                origin_facet_key, url_value
            )
        })?;
        Ok(ValidatedReference {
            doc_id: parsed_facet_ref.doc_id,
            facet_key: parsed_facet_ref.facet_key,
            url_value: url_value.to_string(),
        })
    }

    fn invalidate_entry_cache(&self, id: &DocId) {
        let mut pool = self.entry_pool.lock().unwrap();
        pool.remove_key(id);
        self.entry_cache.remove(id);
    }

    fn invalidate_facet_cache_entry(&self, doc_id: &DocId, facet_uuid: &Uuid) {
        self.facet_cache
            .lock()
            .unwrap()
            .invalidate_facet(doc_id, facet_uuid);
    }

    fn invalidate_facet_cache_doc(&self, doc_id: &DocId) {
        self.facet_cache.lock().unwrap().invalidate_doc(doc_id);
    }

    fn facet_cache_get(
        &self,
        doc_id: &DocId,
        facet_uuid: &Uuid,
        facet_heads: &ChangeHashSet,
    ) -> Option<daybook_types::doc::ArcFacetRaw> {
        self.facet_cache
            .lock()
            .unwrap()
            .get_if_heads_match(doc_id, facet_uuid, facet_heads)
    }

    fn facet_cache_put(
        &self,
        doc_id: &DocId,
        facet_uuid: Uuid,
        facet_heads: ChangeHashSet,
        value: daybook_types::doc::ArcFacetRaw,
    ) {
        self.facet_cache
            .lock()
            .unwrap()
            .put(doc_id, facet_uuid, facet_heads, value);
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<DrawerEvent>,
    ) -> Res<()> {
        // Prefix: docs.map
        if !utils_rs::am::changes::path_prefix_matches(&["docs".into(), "map".into()], &patch.path)
        {
            return Ok(());
        }

        let drawer_heads = ChangeHashSet(Arc::clone(patch_heads));

        match &patch.action {
            automerge::PatchAction::PutMap { key, .. }
                if patch.path.len() == 3 && key == "version" =>
            {
                // docs.map.<doc_id>.version changed
                let Some((_obj, automerge::Prop::Map(doc_id_str))) = patch.path.get(2) else {
                    return Ok(());
                };
                let doc_id = DocId::from(doc_id_str.clone());

                // Hydrate the entry at patch heads
                let entry_path = vec![
                    "docs".into(),
                    "map".into(),
                    autosurgeon::Prop::Key(doc_id.to_string().into()),
                ];

                let (new_entry, _) = self
                    .acx
                    .hydrate_path_at_heads::<DocEntry>(
                        &self.drawer_doc_id,
                        patch_heads,
                        automerge::ROOT,
                        entry_path,
                    )
                    .await?
                    .expect("failed to hydrate entry from patch");

                if new_entry.previous_version_heads.is_none() {
                    out.push(DrawerEvent::DocAdded {
                        id: doc_id,
                        entry: new_entry,
                        drawer_heads,
                    });
                } else {
                    let previous_heads = new_entry
                        .previous_version_heads
                        .as_ref()
                        .ok_or_eyre("doc update missing previous_version_heads")?;
                    let old_entry = self
                        .get_entry_at_heads(&doc_id, previous_heads)
                        .await?
                        .ok_or_eyre(
                            "doc update previous entry not found at previous_version_heads",
                        )?;
                    let diff = DocEntryDiff::new(&old_entry, &new_entry);
                    out.push(DrawerEvent::DocUpdated {
                        id: doc_id,
                        entry: new_entry,
                        diff,
                        drawer_heads,
                    });
                }
            }
            automerge::PatchAction::DeleteMap { key, .. } if patch.path.len() == 2 => {
                // docs.map.<doc_id> deleted
                let doc_id = DocId::from(key.clone());

                // We don't have the entry anymore in the current heads,
                // but V1 includes a placeholder entry.
                out.push(DrawerEvent::DocDeleted {
                    id: doc_id,
                    entry: DocEntry {
                        branches: default(),
                        facet_blames: default(),
                        users: default(),
                        version: Uuid::nil(),
                        previous_version_heads: None,
                    },
                    drawer_heads,
                });
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<DrawerEvent>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let (patches, heads) = self.drawer_am_handle.with_document(|am_doc| {
            let heads = to.unwrap_or_else(|| ChangeHashSet(am_doc.get_heads().into()));
            let patches = am_doc.diff_obj(&automerge::ROOT, &from, &heads, true)?;
            eyre::Ok((patches, heads))
        })?;

        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads.0, &mut events).await?;
        }
        Ok(events)
    }

    pub fn get_drawer_heads(&self) -> ChangeHashSet {
        self.current_heads.read().unwrap().clone()
    }

    pub async fn list(&self) -> Res<Vec<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        self.drawer_am_handle.with_document(|doc| {
            let map_id = match doc.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => {
                    match doc.get(&id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => return Ok(Vec::new()),
                    }
                }
                _ => return Ok(Vec::new()),
            };

            let mut results = Vec::new();
            for item in doc.map_range(&map_id, ..) {
                let doc_id = DocId::from(item.key.clone());
                let entry_id = item.id();

                let branches =
                    if let Some((automerge::Value::Object(automerge::ObjType::Map), b_id)) =
                        doc.get(&entry_id, "branches")?
                    {
                        let mut b_map = HashMap::new();
                        for b_item in doc.map_range(&b_id, ..) {
                            let b_heads: ChangeHashSet =
                                autosurgeon::hydrate_prop(doc, &b_id, b_item.key.clone())?;
                            b_map.insert(b_item.key.to_string(), b_heads);
                        }
                        b_map
                    } else {
                        HashMap::new()
                    };

                results.push(DocNBranches { doc_id, branches });
            }
            Ok(results)
        })
    }

    pub async fn add(&self, args: AddDocArgs) -> Result<DocId, DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(ferr!("repo is stopped"))?;
        }
        let incoming_facets: HashMap<FacetKey, daybook_types::doc::ArcFacetRaw> = args
            .facets
            .iter()
            .map(|(facet_key, facet_value)| (facet_key.clone(), Arc::new(facet_value.clone())))
            .collect();
        let resulting_keys: HashSet<FacetKey> = incoming_facets.keys().cloned().collect();
        self.validate_facets(&incoming_facets, &resulting_keys)
            .await?;
        let mutation_actor_id = if let Some(path) = &args.user_path {
            daybook_types::doc::user_path::to_actor_id(path)
        } else {
            self.local_actor_id.clone()
        };

        // 1. Create content doc
        let mut doc_am = automerge::Automerge::new();
        doc_am.set_actor(mutation_actor_id);
        let handle = self.acx.add_doc(doc_am).await?;
        let doc_id = handle.document_id().to_string();
        let now = Timestamp::now();

        let facet_keys: Vec<_> = args.facets.keys().cloned().collect();

        let heads = handle.with_document(|am_doc| {
            let mut tx = am_doc.transaction();
            tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
            tx.put(automerge::ROOT, "id", &doc_id)?;

            let facets_obj = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;

            for (key, value) in &args.facets {
                let key_str = key.to_string();
                autosurgeon::reconcile_prop(
                    &mut tx,
                    &facets_obj,
                    &*key_str,
                    ThroughJson(value.clone()),
                )?;
            }

            dmeta::ensure_for_add(&mut tx, &facets_obj, &facet_keys, now)?;

            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;

        // 2. Update drawer doc
        let mut users = HashMap::new();
        if let Some(user_path) = args.user_path {
            users.insert(
                self.local_actor_id.to_string(),
                crate::config::UserMeta {
                    user_path,
                    seen_at: now,
                },
            );
        }

        let entry = DocEntry {
            branches: [(
                args.branch_path.to_string_lossy().to_string(),
                heads.clone(),
            )]
            .into(),
            facet_blames: facet_keys
                .iter()
                .map(|facet_key| {
                    (
                        facet_key.to_string(),
                        FacetBlame {
                            heads: heads.clone(),
                        },
                    )
                })
                .collect(),
            users,
            version: Uuid::new_v4(),
            previous_version_heads: None,
        };

        let drawer_heads = self.drawer_am_handle.with_document(|doc| {
            let mut tx = doc.transaction();
            let docs_obj = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => tx.put_object(automerge::ROOT, "docs", automerge::ObjType::Map)?,
            };
            let map_id = match tx.get(&docs_obj, "map")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => tx.put_object(&docs_obj, "map", automerge::ObjType::Map)?,
            };

            autosurgeon::reconcile_prop(&mut tx, &map_id, &*doc_id, &entry)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;

        // 3. Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(&doc_id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.insert(doc_id.clone(), entry.clone());
        }
        self.handles.insert(doc_id.clone(), handle);

        self.registry.notify([
            DrawerEvent::DocAdded {
                id: doc_id.clone(),
                entry,
                drawer_heads: drawer_heads.clone(),
            },
            DrawerEvent::ListChanged {
                drawer_heads: drawer_heads.clone(),
            },
        ]);
        *self.current_heads.write().unwrap() = drawer_heads;

        Ok(doc_id)
    }

    pub async fn update_at_heads(
        &self,
        patch: DocPatch,
        branch_path: daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(ferr!("repo is stopped"))?;
        }
        if patch.is_empty() {
            return Ok(());
        }

        let handle = self
            .get_handle(&patch.id)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound {
                id: patch.id.clone(),
            })?;

        let latest_drawer_heads = self.current_heads.read().unwrap().clone();
        let entry = self
            .get_entry_at_heads(&patch.id, &latest_drawer_heads)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound {
                id: patch.id.clone(),
            })?;

        let heads = match heads {
            Some(selected_heads) => selected_heads,
            None => {
                let branch_name = branch_path.to_string_lossy().to_string();
                entry
                    .branches
                    .get(&branch_name)
                    .cloned()
                    .ok_or_else(|| DrawerError::BranchNotFound { name: branch_name })?
            }
        };

        let existing_facet_keys = self
            .facet_keys_at_heads(&patch.id, &heads)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound {
                id: patch.id.clone(),
            })?;
        let mut resulting_keys = existing_facet_keys;
        let incoming_facets: HashMap<FacetKey, daybook_types::doc::ArcFacetRaw> = patch
            .facets_set
            .iter()
            .map(|(facet_key, facet_value)| (facet_key.clone(), Arc::new(facet_value.clone())))
            .collect();
        for facet_key in incoming_facets.keys() {
            resulting_keys.insert(facet_key.clone());
        }
        for facet_key in &patch.facets_remove {
            resulting_keys.remove(facet_key);
        }
        self.validate_facets(&incoming_facets, &resulting_keys)
            .await?;

        let now = Timestamp::now();
        let facet_keys_set: Vec<_> = patch.facets_set.keys().cloned().collect();
        let facet_keys_remove = patch.facets_remove.clone();
        let mutation_actor_id = if let Some(path) = &patch.user_path {
            daybook_types::doc::user_path::to_actor_id(path)
        } else {
            self.local_actor_id.clone()
        };

        // 1. Update content doc
        let (new_heads, invalidated_uuids) = handle.with_document(|am_doc| {
            am_doc.set_actor(mutation_actor_id.clone());
            let mut tx = am_doc.transaction_at(automerge::PatchLog::null(), &heads);

            let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => eyre::bail!("facets object not found in content doc"),
            };

            for (key, value) in &patch.facets_set {
                let key_str = key.to_string();
                autosurgeon::reconcile_prop(
                    &mut tx,
                    &facets_obj,
                    &*key_str,
                    ThroughJson(value.clone()),
                )?;
            }
            for key in &patch.facets_remove {
                let key_str = key.to_string();
                tx.delete(&facets_obj, &*key_str)?;
            }

            let invalidated_uuids = dmeta::apply_update(
                &mut tx,
                &facets_obj,
                &facet_keys_set,
                &facet_keys_remove,
                now,
            )?;

            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok((ChangeHashSet(Arc::from([heads])), invalidated_uuids))
        })?;

        // 2. Update drawer doc
        let mut new_entry = entry.clone();
        new_entry
            .branches
            .insert(branch_path.to_string_lossy().to_string(), new_heads.clone());
        for key in &facet_keys_set {
            new_entry.facet_blames.insert(
                key.to_string(),
                FacetBlame {
                    heads: new_heads.clone(),
                },
            );
        }
        for key in &facet_keys_remove {
            new_entry.facet_blames.remove(&key.to_string());
        }
        if let Some(user_path) = patch.user_path {
            new_entry.users.insert(
                self.local_actor_id.to_string(),
                crate::config::UserMeta {
                    user_path,
                    seen_at: now,
                },
            );
        }
        new_entry.version = Uuid::new_v4();

        let drawer_heads = self.drawer_am_handle.with_document(|doc| {
            let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
            new_entry.previous_version_heads = Some(current_drawer_heads);

            let mut tx = doc.transaction();
            let map_id = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                    match tx.get(&docs_id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                        _ => eyre::bail!("drawer map not found"),
                    }
                }
                _ => eyre::bail!("drawer docs not found"),
            };

            autosurgeon::reconcile_prop(&mut tx, &map_id, &*patch.id, &new_entry)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;
        let diff = DocEntryDiff::new(&entry, &new_entry);

        // 3. Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(&patch.id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.insert(patch.id.clone(), new_entry.clone());
        }

        for uuid in invalidated_uuids {
            self.invalidate_facet_cache_entry(&patch.id, &uuid);
        }

        self.registry.notify([
            DrawerEvent::DocUpdated {
                id: patch.id,
                entry: new_entry,
                diff,
                drawer_heads: drawer_heads.clone(),
            },
            DrawerEvent::ListChanged {
                drawer_heads: drawer_heads.clone(),
            },
        ]);
        *self.current_heads.write().unwrap() = drawer_heads;

        Ok(())
    }

    pub async fn merge_from_heads(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }
        let handle = self
            .get_handle(id)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;

        let latest_drawer_heads = self.current_heads.read().unwrap().clone();
        let entry = self
            .get_entry_at_heads(id, &latest_drawer_heads)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;

        let to_branch_name = to_branch.to_string_lossy().to_string();
        let to_heads =
            entry
                .branches
                .get(&to_branch_name)
                .ok_or_else(|| DrawerError::BranchNotFound {
                    name: to_branch_name.clone(),
                })?;
        let mutation_actor_id = if let Some(path) = &user_path {
            daybook_types::doc::user_path::to_actor_id(path)
        } else {
            self.local_actor_id.clone()
        };

        // 1. Merge content docs
        let (new_heads, modified_facets, invalidated_uuids) = handle.with_document(|am_doc| {
            am_doc.set_actor(mutation_actor_id.clone());
            let mut am_to = am_doc.fork_at(to_heads)?;
            am_to.set_actor(mutation_actor_id);

            let mut am_from = am_doc.fork_at(from_heads)?;

            let mut patch_log = automerge::PatchLog::active();
            am_to.merge_and_log_patches(&mut am_from, &mut patch_log)?;

            let patches = am_to.make_patches(&mut patch_log);
            let heads = am_to.get_heads();
            let new_heads = ChangeHashSet(heads.into());

            // Merge back to main doc handle
            am_doc.merge(&mut am_to)?;

            // Identify modified facets from patches
            let mut modified_facets = HashSet::new();
            for patch in patches {
                if patch.path.len() >= 2 {
                    if let (_, automerge::Prop::Map(ref p0)) = &patch.path[0] {
                        if p0 == "facets" {
                            if let (_, automerge::Prop::Map(ref facet_key_str)) = &patch.path[1] {
                                modified_facets.insert(facet_key_str.to_string());
                            }
                        }
                    }
                }
            }

            let invalidated_uuids = if modified_facets.is_empty() {
                Vec::new()
            } else {
                let mut tx = am_doc.transaction();
                let facets_obj = match tx.get(automerge::ROOT, "facets")? {
                    Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                    _ => eyre::bail!("facets object not found in content doc"),
                };
                let now = Timestamp::now();
                let invalidated = dmeta::apply_merge(&mut tx, &facets_obj, &modified_facets, now)?;
                tx.commit();
                invalidated
            };

            eyre::Ok((new_heads, modified_facets, invalidated_uuids))
        })?;

        // 2. Update drawer doc
        let mut new_entry = entry.clone();
        new_entry.branches.insert(to_branch_name, new_heads.clone());
        for key_str in modified_facets {
            new_entry.facet_blames.insert(
                key_str,
                FacetBlame {
                    heads: new_heads.clone(),
                },
            );
        }
        if let Some(user_path) = user_path {
            new_entry.users.insert(
                self.local_actor_id.to_string(),
                crate::config::UserMeta {
                    user_path,
                    seen_at: Timestamp::now(),
                },
            );
        }
        new_entry.version = Uuid::new_v4();

        let drawer_heads = self.drawer_am_handle.with_document(|doc| {
            let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
            new_entry.previous_version_heads = Some(current_drawer_heads);

            let mut tx = doc.transaction();
            let map_id = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                    match tx.get(&docs_id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                        _ => eyre::bail!("drawer map not found"),
                    }
                }
                _ => eyre::bail!("drawer docs not found"),
            };

            autosurgeon::reconcile_prop(&mut tx, &map_id, &**id, &new_entry)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;
        let diff = DocEntryDiff::new(&entry, &new_entry);

        // 3. Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.insert(id.clone(), new_entry.clone());
        }

        for uuid in invalidated_uuids {
            self.invalidate_facet_cache_entry(id, &uuid);
        }

        self.registry.notify([
            DrawerEvent::DocUpdated {
                id: id.clone(),
                entry: new_entry,
                diff,
                drawer_heads: drawer_heads.clone(),
            },
            DrawerEvent::ListChanged {
                drawer_heads: drawer_heads.clone(),
            },
        ]);
        *self.current_heads.write().unwrap() = drawer_heads;

        Ok(())
    }

    pub async fn del(&self, id: &DocId) -> Result<bool, DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }

        let res = self.drawer_am_handle.with_document(|doc| {
            let map_id = match doc.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                    match doc.get(&docs_id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                        _ => eyre::bail!("drawer map not found"),
                    }
                }
                _ => eyre::bail!("drawer docs not found"),
            };

            let entry: Option<DocEntry> = autosurgeon::hydrate_prop(doc, &map_id, &**id)?;
            let Some(entry) = entry else {
                return Ok((
                    false,
                    ChangeHashSet::default(),
                    DocEntry {
                        branches: default(),
                        facet_blames: default(),
                        users: default(),
                        version: Uuid::nil(),
                        previous_version_heads: None,
                    },
                ));
            };

            let mut tx = doc.transaction();
            tx.delete(&map_id, &**id)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            Ok((true, ChangeHashSet(Arc::from([heads])), entry))
        });

        let (existed, drawer_heads, entry) = res?;

        if existed {
            self.invalidate_entry_cache(id);
            self.handles.remove(id);
            self.invalidate_facet_cache_doc(id);
            self.registry.notify([
                DrawerEvent::DocDeleted {
                    id: id.clone(),
                    entry,
                    drawer_heads: drawer_heads.clone(),
                },
                DrawerEvent::ListChanged {
                    drawer_heads: drawer_heads.clone(),
                },
            ]);
            *self.current_heads.write().unwrap() = drawer_heads;
        }

        Ok(existed)
    }

    pub async fn get_entry_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<DocEntry>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        // Entry cache is currently only for "latest" known heads of the drawer doc.
        // For specific heads, we always hydrate.
        let path = vec![
            "docs".into(),
            "map".into(),
            autosurgeon::Prop::Key(doc_id.to_string().into()),
        ];
        let entry = self
            .acx
            .hydrate_path_at_heads::<DocEntry>(&self.drawer_doc_id, heads, automerge::ROOT, path)
            .await?;
        Ok(entry.map(|(entry_value, _)| entry_value))
    }

    pub async fn get_entry(&self, doc_id: &DocId) -> Res<Option<DocEntry>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        if let Some(cached) = self.entry_cache.get(doc_id) {
            let mut pool = self.entry_pool.lock().unwrap();
            pool.touch_key(doc_id);
            return Ok(Some(cached.clone()));
        }

        let heads = self.current_heads.read().unwrap().clone();
        let entry = self.get_entry_at_heads(doc_id, &heads).await?;

        if let Some(entry) = entry {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(doc_id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.insert(doc_id.clone(), entry.clone());
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn get_handle(&self, id: &DocId) -> Res<Option<samod::DocHandle>> {
        if let Some(handle) = self.handles.get(id) {
            return Ok(Some(handle.clone()));
        }
        if self.get_entry(id).await?.is_none() {
            return Ok(None);
        }
        let doc_id = DocumentId::from_str(id)?;
        let Some(handle) = self.acx.find_doc(&doc_id).await? else {
            return Ok(None);
        };
        self.handles.insert(id.clone(), handle.clone());
        Ok(Some(handle))
    }

    /// Fetch facets at given heads with Arc-backed values to avoid deep-cloning on cache hits.
    pub(crate) async fn get_at_heads_with_facets_arc(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<HashMap<FacetKey, daybook_types::doc::ArcFacetRaw>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let Some(handle) = self.get_handle(doc_id).await? else {
            return Ok(None);
        };

        let (facets, to_cache) = handle.with_document(|am_doc| {
            let view: automerge::Automerge = am_doc.fork_at(heads)?;
            let mut facets = HashMap::new();
            let mut to_cache = Vec::new();

            match &facet_keys {
                None => {
                    let full: ThroughJson<Doc> = autosurgeon::hydrate(&view)?;
                    for (key, value) in full.0.facets {
                        let value = Arc::new(value);
                        facets.insert(key.clone(), Arc::clone(&value));
                        if let Some(facet_uuid) = dmeta::facet_uuid_for_key(&view, &key)? {
                            let facet_heads = dmeta::facet_heads_for_key(&view, &key)?;
                            to_cache.push((facet_uuid, facet_heads, value));
                        }
                    }
                }
                Some(keys) => {
                    let facets_obj = match view.get(automerge::ROOT, "facets")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                        _ => eyre::bail!("facets object not found in content doc"),
                    };
                    for key in keys {
                        let facet_uuid = dmeta::facet_uuid_for_key(&view, key)?;
                        let facet_heads = if facet_uuid.is_some() {
                            Some(dmeta::facet_heads_for_key(&view, key)?)
                        } else {
                            None
                        };

                        if let (Some(uuid), Some(heads)) = (facet_uuid, &facet_heads) {
                            if let Some(cached) = self.facet_cache_get(doc_id, &uuid, heads) {
                                facets.insert(key.clone(), cached);
                                continue;
                            }
                        }

                        let key_str = key.to_string();
                        let value: Option<ThroughJson<FacetRaw>> =
                            autosurgeon::hydrate_prop(&view, &facets_obj, &*key_str)?;
                        if let Some(facet_value) = value {
                            let facet_value = Arc::new(facet_value.0);
                            facets.insert(key.clone(), Arc::clone(&facet_value));
                            if let (Some(uuid), Some(heads)) = (facet_uuid, facet_heads) {
                                to_cache.push((uuid, heads, facet_value));
                            }
                        }
                    }
                }
            }
            eyre::Ok((facets, to_cache))
        })?;

        for (uuid, heads, value) in to_cache {
            self.facet_cache_put(doc_id, uuid, heads, value);
        }

        Ok(Some(facets))
    }

    /// Get a doc at specific branch.
    pub async fn get_doc_with_facets_at_branch(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        let entry = self.get_entry(doc_id).await?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        let branch_name = branch_path.to_string_lossy().to_string();
        let Some(heads) = entry.branches.get(&branch_name) else {
            return Ok(None);
        };

        self.get_doc_with_facets_at_heads(doc_id, heads, facet_keys)
            .await
    }

    pub async fn get_doc_branches_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let entry = self.get_entry_at_heads(doc_id, heads).await?;
        Ok(entry.map(|entry_value| DocNBranches {
            doc_id: doc_id.clone(),
            branches: entry_value.branches,
        }))
    }

    pub async fn get_doc_branches(&self, doc_id: &DocId) -> Res<Option<DocNBranches>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let heads = self.current_heads.read().unwrap().clone();
        self.get_doc_branches_at_heads(doc_id, &heads).await
    }

    /// Get a doc at specific heads (exact version). Delegates to get_at_heads_with_facets.
    pub async fn get_doc_with_facets_at_heads(
        &self,
        id: &DocId,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        let facets = self
            .get_at_heads_with_facets_arc(id, heads, facet_keys)
            .await?;
        let Some(facets) = facets else {
            return Ok(None);
        };
        let facets = facets
            .into_iter()
            .map(|(key, value)| (key, value.as_ref().clone()))
            .collect();
        Ok(Some(Arc::new(Doc {
            id: id.clone(),
            facets,
        })))
    }

    pub async fn get_with_heads_at_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let entry = self.get_entry_at_heads(doc_id, heads).await?;
        let Some(entry) = entry else {
            return Ok(None);
        };

        let branch_name = branch_path.to_string_lossy().to_string();
        let Some(branch_heads) = entry.branches.get(&branch_name) else {
            return Ok(None);
        };

        let doc = self
            .get_doc_with_facets_at_heads(doc_id, branch_heads, facet_keys)
            .await?;
        Ok(doc.map(|doc_value| (doc_value, branch_heads.clone())))
    }

    pub async fn get_with_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let heads = self.current_heads.read().unwrap().clone();
        self.get_with_heads_at_heads(doc_id, branch_path, &heads, facet_keys)
            .await
    }

    pub async fn get_if_latest_at_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        drawer_heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let entry = self.get_entry_at_heads(doc_id, drawer_heads).await?;
        let Some(entry) = entry else {
            return Ok(None);
        };

        let branch_name = branch_path.to_string_lossy().to_string();
        if let Some(latest_heads) = entry.branches.get(&branch_name) {
            if latest_heads == heads {
                return self
                    .get_doc_with_facets_at_heads(doc_id, heads, facet_keys)
                    .await;
            }
        }
        Ok(None)
    }

    pub async fn get_if_latest(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        facet_keys: Option<Vec<FacetKey>>,
    ) -> Res<Option<Arc<Doc>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let drawer_heads = self.current_heads.read().unwrap().clone();
        self.get_if_latest_at_heads(doc_id, branch_path, heads, &drawer_heads, facet_keys)
            .await
    }

    /// Returns the set of facet keys present for the doc at the given heads, without hydrating facet values.
    pub async fn facet_keys_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
    ) -> Res<Option<HashSet<FacetKey>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self.get_handle(doc_id).await? else {
            return Ok(None);
        };
        let keys = handle.with_document(|am_doc| {
            let view: automerge::Automerge = am_doc.fork_at(heads)?;
            let facets_obj = match view.get(automerge::ROOT, "facets")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => return Ok::<HashSet<FacetKey>, eyre::Report>(HashSet::new()),
            };
            let mut out = HashSet::new();
            for item in view.map_range(&facets_obj, ..) {
                let key_str = item.key.to_string();
                out.insert(FacetKey::from(key_str.as_str()));
            }
            Ok(out)
        })?;
        Ok(Some(keys))
    }

    /// Like get_if_latest but returns only facet keys (no facet values). Returns None if branch heads are stale.
    pub async fn get_facet_keys_if_latest(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
        drawer_heads: &ChangeHashSet,
    ) -> Res<Option<HashSet<FacetKey>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let entry = self.get_entry_at_heads(doc_id, drawer_heads).await?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        let branch_name = branch_path.to_string_lossy().to_string();
        if let Some(latest_heads) = entry.branches.get(&branch_name) {
            if latest_heads == heads {
                return self.facet_keys_at_heads(doc_id, heads).await;
            }
        }
        Ok(None)
    }

    pub async fn update_batch(
        &self,
        patches: Vec<UpdateDocArgsV2>,
    ) -> Result<(), UpdateDocBatchErrV2> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(patches.into_iter().enumerate().map(
            |(ii, args)| async move {
                self.update_at_heads(args.patch, args.branch_path, args.heads)
                    .await
                    .map_err(|err| (ii, err))
            },
        ))
        .buffer_unordered(16);

        let mut errors = HashMap::new();
        while let Some(res) = stream.next().await {
            if let Err((ii, err)) = res {
                errors.insert(ii as u64, err);
            }
        }

        if !errors.is_empty() {
            Err(UpdateDocBatchErrV2 { map: errors })
        } else {
            Ok(())
        }
    }

    pub async fn merge_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<(), DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }
        let entry = self
            .get_entry(id)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;

        let from_branch_name = from_branch.to_string_lossy().to_string();
        let from_heads =
            entry
                .branches
                .get(&from_branch_name)
                .ok_or_else(|| DrawerError::BranchNotFound {
                    name: from_branch_name,
                })?;

        self.merge_from_heads(id, to_branch, from_heads, user_path)
            .await
    }

    pub async fn delete_branch(
        &self,
        id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Result<bool, DrawerError> {
        if self.cancel_token.is_cancelled() {
            return Err(DrawerError::Other {
                inner: ferr!("repo is stopped"),
            });
        }

        let latest_drawer_heads = self.current_heads.read().unwrap().clone();
        let entry = self
            .get_entry_at_heads(id, &latest_drawer_heads)
            .await?
            .ok_or_else(|| DrawerError::DocNotFound { id: id.clone() })?;

        let branch_name = branch_path.to_string_lossy().to_string();
        if !entry.branches.contains_key(&branch_name) {
            return Ok(false);
        }

        let mut new_entry = entry.clone();
        new_entry.branches.remove(&branch_name);
        new_entry.version = Uuid::new_v4();
        if let Some(user_path) = user_path {
            new_entry.users.insert(
                self.local_actor_id.to_string(),
                crate::config::UserMeta {
                    user_path,
                    seen_at: Timestamp::now(),
                },
            );
        }

        let drawer_heads = self.drawer_am_handle.with_document(|doc| {
            let current_drawer_heads = ChangeHashSet(doc.get_heads().into());
            new_entry.previous_version_heads = Some(current_drawer_heads);

            let mut tx = doc.transaction();
            let map_id = match tx.get(automerge::ROOT, "docs")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), docs_id)) => {
                    match tx.get(&docs_id, "map")? {
                        Some((automerge::Value::Object(automerge::ObjType::Map), map_id)) => map_id,
                        _ => eyre::bail!("drawer map not found"),
                    }
                }
                _ => eyre::bail!("drawer docs not found"),
            };

            autosurgeon::reconcile_prop(&mut tx, &map_id, &**id, &new_entry)?;
            let (heads, _) = tx.commit();
            let heads = heads.expect("commit failed");
            eyre::Ok(ChangeHashSet(Arc::from([heads])))
        })?;
        let diff = DocEntryDiff::new(&entry, &new_entry);

        // Update caches and notify
        {
            let mut pool = self.entry_pool.lock().unwrap();
            let pruned = pool.insert_key(id, 1);
            for pkey in pruned {
                self.entry_cache.remove(&pkey);
            }
            self.entry_cache.insert(id.clone(), new_entry.clone());
        }

        self.registry.notify([
            DrawerEvent::DocUpdated {
                id: id.clone(),
                entry: new_entry,
                diff,
                drawer_heads: drawer_heads.clone(),
            },
            DrawerEvent::ListChanged {
                drawer_heads: drawer_heads.clone(),
            },
        ]);
        *self.current_heads.write().unwrap() = drawer_heads;

        Ok(true)
    }

    pub async fn get_facet_heads_at_heads(
        &self,
        doc_id: &DocId,
        heads: &ChangeHashSet,
        facet_key: &FacetKey,
    ) -> Res<Vec<automerge::ChangeHash>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let Some(handle) = self.get_handle(doc_id).await? else {
            eyre::bail!("doc not found");
        };
        handle.with_document(|am_doc| {
            let view: automerge::Automerge = am_doc.fork_at(heads)?;
            facet_recovery::recover_facet_heads(&view, facet_key)
        })
    }

    pub async fn get_facet_heads_at_branch(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        facet_key: &FacetKey,
    ) -> Res<Option<Vec<automerge::ChangeHash>>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let entry = self.get_entry(doc_id).await?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        let branch_name = branch_path.to_string_lossy().to_string();
        let Some(heads) = entry.branches.get(&branch_name) else {
            return Ok(None);
        };
        self.get_facet_heads_at_heads(doc_id, heads, facet_key)
            .await
            .map(Some)
    }
}

impl crate::repos::Repo for DrawerRepo {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::drawer::lru::KeyedLruPool;
    use crate::repos::Repo;
    use daybook_types::doc::{FacetKey, UserPath, WellKnownFacet, WellKnownFacetTag};
    use daybook_types::url::build_facet_ref;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        // 1. Add doc
        let facet_title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_title_key.clone(),
                    WellKnownFacet::TitleGeneric("Initial".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // 2. List docs
        let list = repo.list().await?;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].doc_id, doc_id);
        assert!(list[0].branches.contains_key("main"));

        // 3. Get doc
        let doc = repo
            .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
            .await?
            .unwrap();
        assert_eq!(
            doc.facets.get(&facet_title_key).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("Initial".into()))
        );

        // 4. Update doc
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title_key.clone(),
                    WellKnownFacet::TitleGeneric("Updated".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "main".into(),
            None,
        )
        .await?;

        let doc = repo
            .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
            .await?
            .unwrap();
        assert_eq!(
            doc.facets.get(&facet_title_key).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("Updated".into()))
        );

        // 5. Delete doc
        assert!(repo.del(&doc_id).await?);
        let list = repo.list().await?;
        assert_eq!(list.len(), 0);

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_merge() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-merge".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let facet_note = FacetKey::from(WellKnownFacetTag::Note);

        // 1. Add doc on main
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Base".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let entry = repo.get_entry(&doc_id).await?.unwrap();
        let main_heads = entry.branches.get("main").unwrap().clone();

        // 2. Update branch-a
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("A".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(main_heads.clone()),
        )
        .await?;

        // 3. Update branch-b
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(facet_note.clone(), WellKnownFacet::Note("B".into()).into())].into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-b".into(),
            Some(main_heads.clone()),
        )
        .await?;

        // 4. Merge branch-a to main
        let entry = repo.get_entry(&doc_id).await?.unwrap();
        let a_heads = entry.branches.get("branch-a").unwrap().clone();
        repo.merge_from_heads(&doc_id, &"main".into(), &a_heads, None)
            .await?;

        // 5. Merge branch-b to main
        let entry = repo.get_entry(&doc_id).await?.unwrap();
        let b_heads = entry.branches.get("branch-b").unwrap().clone();
        repo.merge_from_heads(&doc_id, &"main".into(), &b_heads, None)
            .await?;

        // 6. Verify merge
        let doc = repo
            .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
            .await?
            .unwrap();
        assert_eq!(
            doc.facets.get(&facet_title).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("A".into()))
        );
        assert_eq!(
            doc.facets.get(&facet_note).unwrap(),
            &serde_json::Value::from(WellKnownFacet::Note("B".into()))
        );

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_sync_smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (client_acx, client_acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "client".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let (server_acx, server_acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "server".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        // Connect repos
        {
            #[allow(deprecated)]
            fn repos(acx: &AmCtx) -> &samod::Repo {
                acx.repo()
            }
            crate::tincans::connect_repos(repos(&client_acx), repos(&server_acx));
            repos(&client_acx).when_connected("server".into()).await?;
            repos(&server_acx).when_connected("client".into()).await?;
        }

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = client_acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));

        let (client_repo, client_stop) = DrawerRepo::load(
            client_acx.clone(),
            drawer_doc_id.clone(),
            automerge::ActorId::random(),
            Arc::clone(&entry_pool),
            Arc::clone(&doc_pool),
        )
        .await?;
        let (server_repo, server_stop) = DrawerRepo::load(
            server_acx.clone(),
            drawer_doc_id.clone(),
            automerge::ActorId::random(),
            Arc::clone(&entry_pool),
            Arc::clone(&doc_pool),
        )
        .await?;

        let server_listener = server_repo.subscribe(crate::repos::SubscribeOpts::new(128));

        // 1. Client adds a doc
        let facet_note = FacetKey::from(WellKnownFacetTag::Note);
        let new_doc_id = client_repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_note.clone(),
                    WellKnownFacet::Note("Hello".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // 2. Server should receive DocAdded event
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server_listener.recv_lossy_async(),
        )
        .await
        .wrap_err("timeout waiting for DocAdded")?
        .map_err(|_| eyre::eyre!("listener closed"))?;

        match &*event {
            DrawerEvent::DocAdded { id, .. } => assert_eq!(id, &new_doc_id),
            _ => eyre::bail!("unexpected event: {:?}", event),
        }

        // 3. Server should be able to list and get the doc
        let list = server_repo.list().await?;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].doc_id, new_doc_id);

        let doc = server_repo
            .get_doc_with_facets_at_branch(&new_doc_id, &"main".into(), None)
            .await?
            .ok_or_eyre("doc not found on server")?;
        assert_eq!(
            doc.facets.get(&facet_note).unwrap(),
            &serde_json::Value::from(WellKnownFacet::Note("Hello".into()))
        );

        client_acx_stop.stop().await?;
        server_acx_stop.stop().await?;
        client_stop.stop().await?;
        server_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_additional_apis() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-apis".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);

        // 1. Add doc
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Base".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        // 2. Test get_doc_branches
        let branches = repo.get_doc_branches(&doc_id).await?.unwrap();
        assert!(branches.branches.contains_key("main"));

        // 3. Test get_with_heads
        let (doc, heads) = repo
            .get_with_heads(&doc_id, &"main".into(), None)
            .await?
            .unwrap();
        assert_eq!(
            doc.facets.get(&facet_title).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("Base".into()))
        );

        // 4. Test get_if_latest
        let doc_latest = repo
            .get_if_latest(&doc_id, &"main".into(), &heads, None)
            .await?
            .unwrap();
        assert_eq!(
            doc_latest.facets.get(&facet_title).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("Base".into()))
        );

        let wrong_heads = ChangeHashSet(Arc::from([automerge::ChangeHash([0u8; 32])]));
        assert!(repo
            .get_if_latest(&doc_id, &"main".into(), &wrong_heads, None)
            .await?
            .is_none());

        // 5. Test update_batch
        repo.update_batch(vec![UpdateDocArgsV2 {
            branch_path: "main".into(),
            heads: None,
            patch: DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Batch Updated".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
        }])
        .await
        .map_err(|e| eyre::eyre!("batch update failed: {:?}", e))?;

        let doc_updated = repo
            .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
            .await?
            .unwrap();
        assert_eq!(
            doc_updated.facets.get(&facet_title).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("Batch Updated".into()))
        );

        // 6. Test merge_from_branch
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Branch A".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(heads),
        )
        .await?;

        repo.merge_from_branch(&doc_id, &"main".into(), &"branch-a".into(), None)
            .await?;
        let doc_merged = repo
            .get_doc_with_facets_at_branch(&doc_id, &"main".into(), None)
            .await?
            .unwrap();
        assert_eq!(
            doc_merged.facets.get(&facet_title).unwrap(),
            &serde_json::Value::from(WellKnownFacet::TitleGeneric("Branch A".into()))
        );

        // 7. Test delete_branch
        assert!(
            repo.delete_branch(&doc_id, &"branch-a".into(), None)
                .await?
        );
        let branches_after_del = repo.get_doc_branches(&doc_id).await?.unwrap();
        assert!(!branches_after_del.branches.contains_key("branch-a"));

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_metadata_maintenance() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-meta".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let facet_note = FacetKey::from(WellKnownFacetTag::Note);
        let user_path = UserPath::from("/device1/plug1/routine1");

        // 1. Test 'add' metadata
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Initial".into()).into(),
                )]
                .into(),
                user_path: Some(user_path.clone()),
            })
            .await?;

        let entry = repo.get_entry(&doc_id).await?.unwrap();
        assert!(
            entry.users.contains_key(&repo.local_actor_id.to_string()),
            "user should be recorded on add"
        );
        assert!(
            entry.facet_blames.contains_key(&facet_title.to_string()),
            "facet_blame should exist for title"
        );

        // Check Dmeta in content doc
        let am_id = DocumentId::from_str(&doc_id)?;
        let handle = acx.find_doc(&am_id).await?.unwrap();
        handle.with_document(|doc| -> Res<()> {
            let heads = facet_recovery::recover_facet_heads(doc, &facet_title)?;
            assert_eq!(heads.len(), 1, "should have 1 head for title");
            Ok(())
        })?;

        // 2. Test 'update' metadata and user attribution
        let user_path2 = UserPath::from("/device2/plug2/routine2");
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_note.clone(),
                    WellKnownFacet::Note("New Note".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(user_path2.clone()),
            },
            "main".into(),
            None,
        )
        .await?;

        let entry = repo.get_entry(&doc_id).await?.unwrap();
        assert!(
            entry.users.contains_key(&repo.local_actor_id.to_string()),
            "user should still be recorded"
        );
        assert!(
            entry.facet_blames.contains_key(&facet_note.to_string()),
            "facet_blame should exist for note"
        );

        // 3. Test 'merge' metadata maintenance
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Branch Title".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(entry.branches.get("main").unwrap().clone()),
        )
        .await?;

        let entry_before_merge = repo.get_entry(&doc_id).await?.unwrap();
        let a_heads = entry_before_merge.branches.get("branch-a").unwrap().clone();

        repo.merge_from_heads(&doc_id, &"main".into(), &a_heads, Some(user_path.clone()))
            .await?;

        let entry_after_merge = repo.get_entry(&doc_id).await?.unwrap();
        assert_eq!(
            entry_after_merge
                .facet_blames
                .get(&facet_title.to_string())
                .unwrap()
                .heads
                .len(),
            1,
            "title blame should be updated after merge"
        );

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    fn latest_change_actor(handle: &samod::DocHandle) -> Res<automerge::ActorId> {
        handle.with_document(|doc| {
            let heads = doc.get_heads();
            let Some(latest_head) = heads.first() else {
                eyre::bail!("doc has no heads");
            };
            let change = doc
                .get_change_by_hash(latest_head)
                .ok_or_eyre("latest head change not found")?;
            Ok(change.actor_id().clone())
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_at_heads_uses_patch_user_path_actor() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-update-actor".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        )
        .await?;

        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::TitleGeneric),
                    WellKnownFacet::TitleGeneric("before".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let user_path = UserPath::from("/device-actor/plug/routine");
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    WellKnownFacet::Note("updated".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(user_path.clone()),
            },
            "main".into(),
            None,
        )
        .await?;

        let expected_actor = daybook_types::doc::user_path::to_actor_id(&user_path);
        let handle = acx
            .find_doc(&DocumentId::from_str(&doc_id)?)
            .await?
            .ok_or_eyre("doc not found")?;
        let latest_actor = latest_change_actor(&handle)?;
        assert_eq!(latest_actor, expected_actor);

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_from_heads_uses_user_path_actor() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-merge-actor".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        )
        .await?;

        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::TitleGeneric),
                    WellKnownFacet::TitleGeneric("base".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let main_heads = repo
            .get_entry(&doc_id)
            .await?
            .ok_or_eyre("missing entry")?
            .branches
            .get("main")
            .cloned()
            .ok_or_eyre("missing main branch")?;
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    FacetKey::from(WellKnownFacetTag::TitleGeneric),
                    WellKnownFacet::TitleGeneric("branch-change".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(main_heads),
        )
        .await?;

        let branch_heads = repo
            .get_entry(&doc_id)
            .await?
            .ok_or_eyre("missing entry after branch update")?
            .branches
            .get("branch-a")
            .cloned()
            .ok_or_eyre("missing branch-a")?;
        let merge_user_path = UserPath::from("/merge-device/plug/routine");
        repo.merge_from_heads(
            &doc_id,
            &"main".into(),
            &branch_heads,
            Some(merge_user_path.clone()),
        )
        .await?;

        let expected_actor = daybook_types::doc::user_path::to_actor_id(&merge_user_path);
        let handle = acx
            .find_doc(&DocumentId::from_str(&doc_id)?)
            .await?
            .ok_or_eyre("doc not found")?;
        let latest_actor = latest_change_actor(&handle)?;
        assert_eq!(latest_actor, expected_actor);

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[test]
    fn test_facet_cache_admission_requires_second_put() {
        let pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(10_000)));
        let mut cache = FacetCacheState::new(pool);
        let doc_id = "doc-1".to_string();
        let facet_uuid = Uuid::new_v4();
        let heads = ChangeHashSet(Vec::new().into());
        let value = Arc::new(serde_json::json!({"mime":"text/plain","content":"hello"}));

        cache.put(&doc_id, facet_uuid, heads.clone(), Arc::clone(&value));
        let miss = cache.get_if_heads_match(&doc_id, &facet_uuid, &heads);
        assert!(miss.is_none(), "first write should stay probationary");

        cache.put(&doc_id, facet_uuid, heads.clone(), Arc::clone(&value));
        let hit = cache.get_if_heads_match(&doc_id, &facet_uuid, &heads);
        assert!(hit.is_some(), "second write should be admitted");
    }

    #[test]
    fn test_facet_cache_miss_on_heads_change() {
        let pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(10_000)));
        let mut cache = FacetCacheState::new(pool);
        let doc_id = "doc-2".to_string();
        let facet_uuid = Uuid::new_v4();
        let heads_a = ChangeHashSet(Vec::new().into());
        let heads_b = ChangeHashSet(Arc::from([automerge::ChangeHash([1u8; 32])]));
        let value = Arc::new(serde_json::json!({"content":"a"}));

        cache.put(&doc_id, facet_uuid, heads_a.clone(), Arc::clone(&value));
        cache.put(&doc_id, facet_uuid, heads_a.clone(), Arc::clone(&value));
        assert!(cache
            .get_if_heads_match(&doc_id, &facet_uuid, &heads_a)
            .is_some());
        assert!(cache
            .get_if_heads_match(&doc_id, &facet_uuid, &heads_b)
            .is_none());
    }

    #[test]
    fn test_facet_cache_large_one_hit_entries_do_not_pollute() {
        let pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1024)));
        let mut cache = FacetCacheState::new(pool);
        let doc_id = "doc-3".to_string();
        let heads = ChangeHashSet(Vec::new().into());

        for index in 0..20u32 {
            let facet_uuid = Uuid::new_v4();
            let payload = "x".repeat(4 * 1024);
            let value = Arc::new(serde_json::json!({"idx": index, "payload": payload}));
            cache.put(&doc_id, facet_uuid, heads.clone(), Arc::clone(&value));
            assert!(
                cache
                    .get_if_heads_match(&doc_id, &facet_uuid, &heads)
                    .is_none(),
                "probationary one-hit values should not be admitted"
            );
        }
        assert!(
            cache.entries.is_empty(),
            "no one-hit payload should be cached"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_updated_at_merge() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-updated-at".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);

        // 1. Add doc on main
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Base".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let entry = repo.get_entry(&doc_id).await?.unwrap();
        let main_heads = entry.branches.get("main").unwrap().clone();

        // 2. Concurrent updates to the same facet on branch-a and branch-b
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("A".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(main_heads.clone()),
        )
        .await?;

        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("B".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-b".into(),
            Some(main_heads.clone()),
        )
        .await?;

        // 3. Merge both into main
        let entry = repo.get_entry(&doc_id).await?.unwrap();
        let a_heads = entry.branches.get("branch-a").unwrap().clone();
        let b_heads = entry.branches.get("branch-b").unwrap().clone();

        repo.merge_from_heads(&doc_id, &"main".into(), &a_heads, None)
            .await?;
        repo.merge_from_heads(&doc_id, &"main".into(), &b_heads, None)
            .await?;

        // 4. Verify updatedAt list in content doc
        let am_id = DocumentId::from_str(&doc_id)?;
        let handle = acx.find_doc(&am_id).await?.unwrap();
        handle.with_document(|doc| -> Res<()> {
            // recover_facet_heads should return 2 change hashes
            let heads = facet_recovery::recover_facet_heads(doc, &facet_title)?;

            // On a concurrent update where both sides clear and insert,
            // Automerge list merge will result in both inserted elements being present.
            assert_eq!(
                heads.len(),
                2,
                "updatedAt should have 2 elements after concurrent merge"
            );

            Ok(())
        })?;

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_facet_blame_maintenance() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-blame".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let facet_note = FacetKey::from(WellKnownFacetTag::Note);

        // 1. Add doc
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("Initial".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let entry = repo.get_entry(&doc_id).await?.unwrap();
        let initial_heads = entry.branches.get("main").unwrap().clone();
        assert_eq!(
            entry
                .facet_blames
                .get(&facet_title.to_string())
                .unwrap()
                .heads,
            initial_heads
        );

        // 2. Update facet on branch-a
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("A".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(initial_heads.clone()),
        )
        .await?;

        let entry_a = repo.get_entry(&doc_id).await?.unwrap();
        let a_heads = entry_a.branches.get("branch-a").unwrap().clone();
        assert_eq!(
            entry_a
                .facet_blames
                .get(&facet_title.to_string())
                .unwrap()
                .heads,
            a_heads
        );

        // 3. Update different facet on branch-b
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(facet_note.clone(), WellKnownFacet::Note("B".into()).into())].into(),
                facets_remove: vec![],
                user_path: None,
            },
            "branch-b".into(),
            Some(initial_heads.clone()),
        )
        .await?;

        let entry_b = repo.get_entry(&doc_id).await?.unwrap();
        let b_heads = entry_b.branches.get("branch-b").unwrap().clone();
        assert_eq!(
            entry_b
                .facet_blames
                .get(&facet_note.to_string())
                .unwrap()
                .heads,
            b_heads
        );

        // 4. Merge branch-a to main
        repo.merge_from_heads(&doc_id, &"main".into(), &a_heads, None)
            .await?;
        let entry_merged_a = repo.get_entry(&doc_id).await?.unwrap();
        let main_heads_a = entry_merged_a.branches.get("main").unwrap().clone();

        let blame_a = entry_merged_a
            .facet_blames
            .get(&facet_title.to_string())
            .unwrap()
            .clone();
        assert_eq!(
            blame_a.heads, main_heads_a,
            "title blame should match main heads after merge A"
        );

        // 5. Merge branch-b to main
        repo.merge_from_heads(&doc_id, &"main".into(), &b_heads, None)
            .await?;
        let entry_merged_b = repo.get_entry(&doc_id).await?.unwrap();
        let main_heads_b = entry_merged_b.branches.get("main").unwrap().clone();

        let blame_b = entry_merged_b
            .facet_blames
            .get(&facet_note.to_string())
            .unwrap()
            .clone();
        assert_eq!(
            blame_b.heads, main_heads_b,
            "note blame should match main heads after merge B"
        );

        // title blame should still be main_heads_a (from previous merge)
        // because it was NOT modified in the second merge.
        let blame_a_after_b = entry_merged_b
            .facet_blames
            .get(&facet_title.to_string())
            .unwrap()
            .clone();
        assert_eq!(
            blame_a_after_b.heads, main_heads_a,
            "title blame should remain unchanged after merge B"
        );

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_listener_is_scoped_to_drawer_doc() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-scope".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let make_drawer_doc = || async {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            eyre::Ok(handle.document_id().clone())
        };

        let drawer_doc_id_a = make_drawer_doc().await?;
        let drawer_doc_id_b = make_drawer_doc().await?;

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));

        let (repo_a, stop_a) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id_a,
            automerge::ActorId::random(),
            Arc::clone(&entry_pool),
            Arc::clone(&doc_pool),
        )
        .await?;
        let (repo_b, stop_b) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id_b,
            automerge::ActorId::random(),
            Arc::clone(&entry_pool),
            Arc::clone(&doc_pool),
        )
        .await?;

        let listener_b = repo_b.subscribe(crate::repos::SubscribeOpts::new(128));

        let facet_note = FacetKey::from(WellKnownFacetTag::Note);
        let _doc_id_a = repo_a
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    facet_note.clone(),
                    WellKnownFacet::Note("hello-from-a".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;

        let maybe_event = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            listener_b.recv_lossy_async(),
        )
        .await;
        assert!(
            maybe_event.is_err(),
            "repo_b received an event from foreign drawer doc"
        );

        stop_a.stop().await?;
        stop_b.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_v2_doc_updated_includes_changed_facet_keys() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-changed-facets".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let entry_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let doc_pool = Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000)));
        let (repo, stop_token) = DrawerRepo::load(
            acx.clone(),
            drawer_doc_id,
            automerge::ActorId::random(),
            entry_pool,
            doc_pool,
        )
        .await?;

        let facet_title = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let facet_note = FacetKey::from(WellKnownFacetTag::Note);
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [
                    (
                        facet_title.clone(),
                        WellKnownFacet::TitleGeneric("initial".into()).into(),
                    ),
                    (
                        facet_note.clone(),
                        WellKnownFacet::Note("initial".into()).into(),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        let listener = repo.subscribe(crate::repos::SubscribeOpts::new(256));

        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    facet_title.clone(),
                    WellKnownFacet::TitleGeneric("updated".into()).into(),
                )]
                .into(),
                facets_remove: vec![facet_note.clone()],
                user_path: None,
            },
            "main".into(),
            None,
        )
        .await?;

        let mut changed_facet_keys = None;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            let next_event = tokio::time::timeout_at(deadline, listener.recv_lossy_async())
                .await
                .wrap_err("timeout waiting for update event")?
                .map_err(|_| eyre::eyre!("listener closed"))?;
            if let DrawerEvent::DocUpdated { id, diff, .. } = &*next_event {
                if id == &doc_id {
                    changed_facet_keys = Some(diff.changed_facet_keys.clone());
                    break;
                }
            }
        }
        let changed_facet_keys =
            changed_facet_keys.ok_or_eyre("did not observe DocUpdated event for test doc")?;

        let changed: HashSet<FacetKey> = changed_facet_keys.into_iter().collect();
        assert!(changed.contains(&facet_title));
        assert!(changed.contains(&facet_note));
        assert_eq!(changed.len(), 2);

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_rejects_unknown_facet_tag() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-unknown-tag".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let (repo, stop_token) = DrawerRepo::load(
            acx,
            drawer_doc_id,
            automerge::ActorId::random(),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        )
        .await?;

        let unknown_facet_key = FacetKey::from("org.test.unknown/main");
        let add_result = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(unknown_facet_key, serde_json::json!({"hello":"world"}))].into(),
                user_path: None,
            })
            .await;
        assert!(add_result.is_err());
        assert!(add_result
            .unwrap_err()
            .to_string()
            .contains("no registered manifest"));

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_rejects_self_reference_without_target_facet() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test-v2-self-ref".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let mut doc = automerge::Automerge::new();
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "version", "0")?;
            tx.commit();
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };

        let (repo, stop_token) = DrawerRepo::load(
            acx,
            drawer_doc_id,
            automerge::ActorId::random(),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
            Arc::new(std::sync::Mutex::new(KeyedLruPool::new(1000))),
        )
        .await?;

        let blob_facet_key = FacetKey::from(WellKnownFacetTag::Blob);
        let image_metadata_facet_key = FacetKey::from(WellKnownFacetTag::ImageMetadata);
        let facet_ref = build_facet_ref(daybook_types::url::FACET_SELF_DOC_ID, &blob_facet_key)?;
        let image_metadata_facet =
            WellKnownFacet::ImageMetadata(daybook_types::doc::ImageMetadata {
                facet_ref,
                ref_heads: ChangeHashSet(Arc::new([])),
                mime: "image/jpeg".into(),
                width_px: 1,
                height_px: 1,
            });

        let add_result = repo
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(image_metadata_facet_key, image_metadata_facet.into())].into(),
                user_path: None,
            })
            .await;
        assert!(add_result.is_err());
        assert!(add_result
            .unwrap_err()
            .to_string()
            .contains("self-reference target"));

        stop_token.stop().await?;
        acx_stop.stop().await?;
        Ok(())
    }
}
