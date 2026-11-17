use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;

/// Keys and the workload handles they name are published under one lock. A
/// lookup retains its handle, so unbinding prevents new sessions without
/// invalidating a session that already acquired the workload.
pub(super) struct WorkloadRegistry<W> {
    state: RwLock<State<W>>,
}

struct Registration<W> {
    keys: HashSet<Arc<str>>,
    workload: Arc<W>,
}

struct State<W> {
    pending: HashMap<Arc<str>, HashSet<Arc<str>>>,
    active: HashMap<Arc<str>, Registration<W>>,
    keys: HashMap<Arc<str>, Arc<str>>,
}

impl<W> Default for WorkloadRegistry<W> {
    fn default() -> Self {
        Self {
            state: RwLock::new(State {
                pending: HashMap::new(),
                active: HashMap::new(),
                keys: HashMap::new(),
            }),
        }
    }
}

impl<W> WorkloadRegistry<W> {
    pub(super) fn bind_pending(
        &self,
        workload: Arc<str>,
        keys: HashSet<Arc<str>>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.write().expect("workload registry poisoned");
        if state.pending.contains_key(&workload) {
            anyhow::bail!("workload '{workload}' was bound more than once");
        }
        for key in &keys {
            if let Some(owner) = state.keys.get(key)
                && owner != &workload
            {
                anyhow::bail!("workflow '{key}' is already owned by '{owner}'");
            }
            if let Some((owner, _)) = state
                .pending
                .iter()
                .find(|(owner, pending)| *owner != &workload && pending.contains(key))
            {
                anyhow::bail!("workflow '{key}' is reserved by '{owner}'");
            }
        }
        state.pending.insert(workload, keys);
        Ok(())
    }

    pub(super) fn publish(&self, id: Arc<str>, workload: Arc<W>) -> anyhow::Result<()> {
        let mut state = self.state.write().expect("workload registry poisoned");
        // Validate before changing the live registration. A resolution without
        // a pending binding must not tear down the previous usable workload.
        let keys = state
            .pending
            .remove(&id)
            .ok_or_else(|| anyhow::anyhow!("unrecognized workload was resolved: {id}"))?;
        if let Some(Registration { keys: old_keys, .. }) = state.active.remove(&id) {
            for key in old_keys {
                assert_eq!(state.keys.remove(&key).as_ref(), Some(&id));
            }
        }
        for key in &keys {
            assert!(
                state
                    .keys
                    .insert(Arc::clone(key), Arc::clone(&id))
                    .is_none()
            );
        }
        state.active.insert(id, Registration { keys, workload });
        Ok(())
    }

    pub(super) fn lookup(&self, key: &str) -> Option<Arc<W>> {
        let state = self.state.read().expect("workload registry poisoned");
        let id = state.keys.get(key)?;
        Some(Arc::clone(
            &state
                .active
                .get(id)
                .expect("published key has a workload")
                .workload,
        ))
    }

    pub(super) fn unbind(&self, id: &str) {
        let mut state = self.state.write().expect("workload registry poisoned");
        state.pending.remove(id);
        if let Some(Registration { keys, .. }) = state.active.remove(id) {
            for key in keys {
                assert_eq!(state.keys.remove(&key).as_deref(), Some(id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(values: &[&str]) -> HashSet<Arc<str>> {
        values.iter().map(|value| Arc::from(*value)).collect()
    }

    #[test]
    fn replacement_and_unbind_publish_complete_registrations() {
        let registry = WorkloadRegistry::default();
        let first = Arc::new(1);
        registry
            .bind_pending("owner".into(), keys(&["old", "shared"]))
            .unwrap();
        registry
            .publish("owner".into(), Arc::clone(&first))
            .unwrap();
        let retained = registry.lookup("old").unwrap();
        registry
            .bind_pending("owner".into(), keys(&["new", "shared"]))
            .unwrap();
        assert!(Arc::ptr_eq(&registry.lookup("shared").unwrap(), &first));
        let second = Arc::new(2);
        registry
            .publish("owner".into(), Arc::clone(&second))
            .unwrap();
        assert!(registry.lookup("old").is_none());
        assert!(Arc::ptr_eq(&registry.lookup("shared").unwrap(), &second));
        assert!(Arc::ptr_eq(&registry.lookup("new").unwrap(), &second));
        registry.unbind("owner");
        assert!(registry.lookup("new").is_none());
        assert!(registry.lookup("shared").is_none());
        assert!(Arc::ptr_eq(&retained, &first));
    }

    #[test]
    fn rejected_binding_and_resolution_preserve_live_registration() {
        let registry = WorkloadRegistry::default();
        registry
            .bind_pending("owner".into(), keys(&["key"]))
            .unwrap();
        assert!(
            registry
                .bind_pending("other".into(), keys(&["key"]))
                .is_err()
        );
        let live = Arc::new(1);
        registry.publish("owner".into(), Arc::clone(&live)).unwrap();
        assert!(registry.publish("owner".into(), Arc::new(2)).is_err());
        assert!(
            registry
                .bind_pending("other".into(), keys(&["key"]))
                .is_err()
        );
        assert!(Arc::ptr_eq(&registry.lookup("key").unwrap(), &live));
        registry.unbind("owner");
        registry
            .bind_pending("other".into(), keys(&["key"]))
            .unwrap();
    }
}
