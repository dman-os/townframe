#![allow(unused)]
/*!
We need to do a lot more:
- The livetree backend impl should live in pauperfuse
- find better names all over
- wasi fs backend

*/

/*

WRITTEN BY GPT-5.3
Pauperfuse is a small reconcile core for "poor man's fuse" workflows.

Idea:
- Keep a pure state/effects core that does not know about daybook, editors, or OS APIs.
- Model two sides of the world as deltas:
  - provider side (remote/canonical object state)
  - backend side (materialized filesystem state)
- Run reconcile to emit effects that move both sides toward convergence.

Direction:
- `Ctx` is the core reducer state.
- adapters (for example `daybook_fuse`) gather snapshots, convert them to deltas, call `Ctx`,
  and apply emitted effects.
- persisted incremental metadata/state is owned by pauperfuse under `store::*`.
- concrete storage implementations (currently SQLite) are first-party pauperfuse modules.

What is left to do:
1. Separate core types/reducer into a dedicated internal module (for readability and smaller files).
2. Add structured conflict/invalid states in core effects instead of relying only on adapter-level errors.
3. Add explicit object-level diagnostics in reports (which object changed, why, and chosen action).
4. Add deterministic per-object ordering guarantees and tests for larger mixed delta batches.
5. Add storage migrations/versioning for persisted state schema evolution.
6. Add optional journal-style persistence for debugging/replay (in addition to snapshots/state tables).
7. Add a first-class API for selecting reconcile policy (provider-preferred, backend-preferred, mark-only).
8. Add a pure simulation test harness that asserts end-to-end transitions from input deltas to output effects.
9. Add cancellation-friendly async orchestration helpers in pauperfuse (without coupling to any one adapter).
10. Define and document stability expectations for public types used by adapter crates.
*/

pub(crate) mod interlude {
    pub use std::collections::{BTreeMap, VecDeque};
    pub use std::path::{Path, PathBuf};
    pub use utils_rs::prelude::*;
}

use interlude::*;
pub mod store;

#[derive(Debug, Clone)]
pub struct Config {
    pub root_path: PathBuf,
}

// FIXME: unecessary new types
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MainStateId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderStateId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendStateId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub String);

impl<T> From<T> for ObjectId
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

// FIXME: this sucks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSnapshot {
    pub object_id: ObjectId,
    pub relative_path: PathBuf,
    // we want this to be the provider's impl
    // they should provide a seekable readear impl
    // to avoid holding all files in memory
    // additionally, we're missing passthrough object support and whatnot
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub object_id: ObjectId,
    // FIXME: do we need the path in the ref if we have the id?
    pub relative_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderDelta {
    Upsert(ObjectSnapshot),
    Remove(ObjectRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendDelta {
    Upsert(ObjectSnapshot),
    Remove(ObjectRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    ProviderObserveUpsert(ObjectSnapshot),
    ProviderObserveRemove(ObjectRef),
    BackendWriteFile(ObjectSnapshot),
    BackendRemoveFile(ObjectRef),
}

#[derive(Debug, Clone, Default)]
pub struct ReconcileReport {
    pub backend_delta_count: usize,
    pub provider_delta_count: usize,
    pub emitted_effect_count: usize,
}

#[derive(Debug, Clone)]
pub struct Ctx {
    pub config: Config,
    main_state_id: MainStateId,
    provider_state_id: ProviderStateId,
    backend_state_id: BackendStateId,
    provider_objects: BTreeMap<ObjectId, ObjectSnapshot>,
    backend_objects: BTreeMap<ObjectId, ObjectSnapshot>,
    pending_provider_deltas: Vec<ProviderDelta>,
    pending_backend_deltas: Vec<BackendDelta>,
    effect_queue: VecDeque<Effect>,
}

impl Ctx {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            main_state_id: MainStateId(0),
            provider_state_id: ProviderStateId(0),
            backend_state_id: BackendStateId(0),
            provider_objects: BTreeMap::new(),
            backend_objects: BTreeMap::new(),
            pending_provider_deltas: Vec::new(),
            pending_backend_deltas: Vec::new(),
            effect_queue: VecDeque::new(),
        }
    }

    pub fn ingest_provider_delta(&mut self, provider_delta: ProviderDelta) -> Res<()> {
        self.pending_provider_deltas.push(provider_delta);
        self.provider_state_id.0 = self.provider_state_id.0.saturating_add(1);
        Ok(())
    }

    pub fn ingest_backend_delta(&mut self, backend_delta: BackendDelta) -> Res<()> {
        self.pending_backend_deltas.push(backend_delta);
        self.backend_state_id.0 = self.backend_state_id.0.saturating_add(1);
        Ok(())
    }

    pub fn reconcile(&mut self) -> Res<ReconcileReport> {
        let mut report = ReconcileReport {
            backend_delta_count: self.pending_backend_deltas.len(),
            provider_delta_count: self.pending_provider_deltas.len(),
            ..ReconcileReport::default()
        };

        if report.backend_delta_count == 0 && report.provider_delta_count == 0 {
            return Ok(report);
        }

        for backend_delta in self.pending_backend_deltas.drain(..) {
            match backend_delta {
                BackendDelta::Upsert(next_snapshot) => {
                    let mut changed = true;
                    if let Some(previous_snapshot) =
                        self.backend_objects.get(&next_snapshot.object_id)
                    {
                        changed = previous_snapshot != &next_snapshot;
                    }
                    if changed {
                        self.backend_objects
                            .insert(next_snapshot.object_id.clone(), next_snapshot.clone());
                        self.effect_queue
                            .push_back(Effect::ProviderObserveUpsert(next_snapshot));
                    }
                }
                BackendDelta::Remove(object_ref) => {
                    if self.backend_objects.remove(&object_ref.object_id).is_some() {
                        self.effect_queue
                            .push_back(Effect::ProviderObserveRemove(object_ref));
                    }
                }
            }
        }

        for provider_delta in self.pending_provider_deltas.drain(..) {
            match provider_delta {
                ProviderDelta::Upsert(next_snapshot) => {
                    let mut changed = true;
                    if let Some(previous_snapshot) =
                        self.provider_objects.get(&next_snapshot.object_id)
                    {
                        changed = previous_snapshot != &next_snapshot;
                    }
                    if changed {
                        self.provider_objects
                            .insert(next_snapshot.object_id.clone(), next_snapshot.clone());
                        self.effect_queue
                            .push_back(Effect::BackendWriteFile(next_snapshot));
                    }
                }
                ProviderDelta::Remove(object_ref) => {
                    if self
                        .provider_objects
                        .remove(&object_ref.object_id)
                        .is_some()
                    {
                        self.backend_objects.remove(&object_ref.object_id);
                        self.effect_queue
                            .push_back(Effect::BackendRemoveFile(object_ref));
                    }
                }
            }
        }

        self.main_state_id.0 = self.main_state_id.0.saturating_add(1);
        report.emitted_effect_count = self.effect_queue.len();

        Ok(report)
    }

    pub fn effects(&mut self) -> Vec<Effect> {
        self.effect_queue.drain(..).collect()
    }

    pub fn main_state_id(&self) -> MainStateId {
        self.main_state_id
    }

    pub fn provider_state_id(&self) -> ProviderStateId {
        self.provider_state_id
    }

    pub fn backend_state_id(&self) -> BackendStateId {
        self.backend_state_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(label: &str, body: &str) -> ObjectSnapshot {
        ObjectSnapshot {
            object_id: ObjectId::from(label),
            relative_path: PathBuf::from(format!("{label}.json")),
            bytes: body.as_bytes().to_vec(),
        }
    }

    fn make_object_ref(label: &str) -> ObjectRef {
        ObjectRef {
            object_id: ObjectId::from(label),
            relative_path: PathBuf::from(format!("{label}.json")),
        }
    }

    #[test]
    fn test_reconcile_no_changes_emits_no_effects() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        let report = ctx.reconcile()?;
        assert_eq!(report.emitted_effect_count, 0);
        assert!(ctx.effects().is_empty());
        assert_eq!(ctx.main_state_id(), MainStateId(0));
        Ok(())
    }

    #[test]
    fn test_backend_edit_emits_provider_observation_effect() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        ctx.ingest_backend_delta(BackendDelta::Upsert(make_snapshot("alpha", "v1")))?;
        let report = ctx.reconcile()?;
        assert_eq!(report.backend_delta_count, 1);
        let observed_effects = ctx.effects();
        assert_eq!(observed_effects.len(), 1);
        match &observed_effects[0] {
            Effect::ProviderObserveUpsert(snapshot) => {
                assert_eq!(snapshot.object_id, ObjectId::from("alpha"));
            }
            _ => panic!("unexpected effect"),
        }
        Ok(())
    }

    #[test]
    fn test_provider_update_emits_backend_materialize_effect() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        ctx.ingest_provider_delta(ProviderDelta::Upsert(make_snapshot("alpha", "v2")))?;
        let report = ctx.reconcile()?;
        assert_eq!(report.provider_delta_count, 1);
        let observed_effects = ctx.effects();
        assert_eq!(observed_effects.len(), 1);
        match &observed_effects[0] {
            Effect::BackendWriteFile(snapshot) => {
                assert_eq!(snapshot.object_id, ObjectId::from("alpha"));
            }
            _ => panic!("unexpected effect"),
        }
        Ok(())
    }

    #[test]
    fn test_provider_remove_emits_backend_remove_effect() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        ctx.ingest_provider_delta(ProviderDelta::Upsert(make_snapshot("alpha", "v1")))?;
        ctx.reconcile()?;
        ctx.effects();

        ctx.ingest_provider_delta(ProviderDelta::Remove(make_object_ref("alpha")))?;
        let report = ctx.reconcile()?;
        assert_eq!(report.provider_delta_count, 1);
        let observed_effects = ctx.effects();
        assert_eq!(observed_effects.len(), 1);
        match &observed_effects[0] {
            Effect::BackendRemoveFile(object_ref) => {
                assert_eq!(object_ref.object_id, ObjectId::from("alpha"));
            }
            _ => panic!("unexpected effect"),
        }
        Ok(())
    }

    #[test]
    fn test_ordering_backend_then_provider_same_object_is_deterministic() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        ctx.ingest_backend_delta(BackendDelta::Upsert(make_snapshot("alpha", "backend")))?;
        ctx.ingest_provider_delta(ProviderDelta::Upsert(make_snapshot("alpha", "provider")))?;

        ctx.reconcile()?;
        let observed_effects = ctx.effects();
        assert_eq!(observed_effects.len(), 2);
        assert!(matches!(
            observed_effects[0],
            Effect::ProviderObserveUpsert(_)
        ));
        assert!(matches!(observed_effects[1], Effect::BackendWriteFile(_)));
        Ok(())
    }

    #[test]
    fn test_state_ids_advance_only_on_effective_transitions() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        assert_eq!(ctx.main_state_id(), MainStateId(0));
        assert_eq!(ctx.provider_state_id(), ProviderStateId(0));
        assert_eq!(ctx.backend_state_id(), BackendStateId(0));

        ctx.reconcile()?;
        assert_eq!(ctx.main_state_id(), MainStateId(0));

        ctx.ingest_provider_delta(ProviderDelta::Upsert(make_snapshot("alpha", "v1")))?;
        assert_eq!(ctx.provider_state_id(), ProviderStateId(1));
        ctx.reconcile()?;
        assert_eq!(ctx.main_state_id(), MainStateId(1));

        ctx.ingest_backend_delta(BackendDelta::Upsert(make_snapshot("alpha", "v1")))?;
        assert_eq!(ctx.backend_state_id(), BackendStateId(1));
        ctx.reconcile()?;
        assert_eq!(ctx.main_state_id(), MainStateId(2));
        Ok(())
    }

    #[test]
    fn test_effects_drain_semantics() -> Res<()> {
        let mut ctx = Ctx::new(Config {
            root_path: PathBuf::from("/tmp/test-pauperfuse"),
        });
        ctx.ingest_backend_delta(BackendDelta::Upsert(make_snapshot("alpha", "v1")))?;
        ctx.reconcile()?;

        let first_drain = ctx.effects();
        assert_eq!(first_drain.len(), 1);
        let second_drain = ctx.effects();
        assert!(second_drain.is_empty());
        Ok(())
    }
}
