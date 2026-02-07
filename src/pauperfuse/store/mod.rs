use crate::interlude::*;

pub mod sqlite;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedObjectState {
    pub relative_path: PathBuf,
    pub provider_hash: Option<String>,
    pub backend_hash: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistedState {
    pub provider_state_id: u64,
    pub backend_state_id: u64,
    pub objects: BTreeMap<String, PersistedObjectState>,
}

#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    async fn load_state(&self) -> Res<PersistedState>;
    async fn save_state(&self, state: &PersistedState) -> Res<()>;
}

pub fn build_persisted_state(
    provider_state_id: u64,
    backend_state_id: u64,
    provider_snapshot: &BTreeMap<String, Vec<u8>>,
    backend_snapshot: &BTreeMap<String, Vec<u8>>,
) -> PersistedState {
    let mut all_doc_ids: std::collections::BTreeSet<String> = default();
    all_doc_ids.extend(provider_snapshot.keys().cloned());
    all_doc_ids.extend(backend_snapshot.keys().cloned());

    let mut objects = BTreeMap::new();
    for doc_id in all_doc_ids {
        let provider_hash = provider_snapshot
            .get(&doc_id)
            .map(|bytes| blake3::hash(bytes).to_hex().to_string());
        let backend_hash = backend_snapshot
            .get(&doc_id)
            .map(|bytes| blake3::hash(bytes).to_hex().to_string());
        objects.insert(
            doc_id.clone(),
            PersistedObjectState {
                relative_path: PathBuf::from(format!("{doc_id}.json")),
                provider_hash,
                backend_hash,
            },
        );
    }

    PersistedState {
        provider_state_id,
        backend_state_id,
        objects,
    }
}

pub fn count_changed_docs(
    previous_state: &PersistedState,
    current_state: &PersistedState,
) -> usize {
    let mut all_doc_ids: std::collections::BTreeSet<String> = default();
    all_doc_ids.extend(previous_state.objects.keys().cloned());
    all_doc_ids.extend(current_state.objects.keys().cloned());

    let mut changed = 0usize;
    for doc_id in all_doc_ids {
        let previous_object = previous_state.objects.get(&doc_id);
        let current_object = current_state.objects.get(&doc_id);
        if previous_object != current_object {
            changed = changed.saturating_add(1);
        }
    }

    changed
}
