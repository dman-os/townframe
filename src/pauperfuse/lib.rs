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

mod interlude {
    pub use std::collections::{BTreeMap, VecDeque};
    pub use std::path::{Path, PathBuf};
    pub use utils_rs::prelude::*;
}

mod livetree;

use std::sync::Mutex;

use crate::interlude::*;

pub struct Ctx {
    providers: HashMap<ProviderId, ProviderDeets>,
    state: State,
    backend: Box<dyn Backend + Sync + Send>,
}

#[async_trait]
trait Backend {
    async fn reconcile(
        &self,
        cx: &Ctx,
        effects: &[BackendEffect],
        report: &mut BackendReconcileReport,
    ) -> Res<()>;
}

pub struct BackendReconcileReport {
    events: &mut Vec<BackendEvent>,
}

trait Provider {}

pub struct ProviderDeets {
    pub id: ProviderId,
    pub r#impl: Box<dyn Provider + Sync + Send>,
}

pub type ProviderId = usize;
pub type VFileId = Uuid;

pub struct VFile {
    pub id: VFileId,
    pub provider_id: ProviderId,
    pub relative_path: PathBuf,
}
pub struct VFileStat {}

pub enum ProviderEvent {
    SetFile {
        id: VFileId,
        provider_id: ProviderId,
        relative_path: PathBuf,
    },
    RemoveFile {
        id: VFileId,
    },
}

pub enum BackendEffect {
    SetFile {
        id: VFileId,
        provider_id: ProviderId,
        relative_path: PathBuf,
    },
    RemoveFile {
        id: VFileId,
    },
}

pub enum BackendEvent {
    FileModified {},
    FileCreated {},
    FileDeleted {},
}

pub struct ReconcilationReport {}

pub async fn reconcile(cx: &Ctx, events: Vec<ProviderEvent>) -> Res<ReconcilationReport> {
    let mut effects = vec![];
    for evt in events {
        match evt {
            ProviderEvent::SetFile {
                id,
                provider_id,
                relative_path,
            } => effects.push(BackendEffect::SetFile {
                id,
                provider_id,
                relative_path,
            }),
            ProviderEvent::RemoveFile { id } => effects.push(BackendEffect::RemoveFile { id }),
        }
    }
    todo!()
}

pub fn stat_vfile(cx: &Ctx) -> Res<()> {
    todo!()
}

enum State {
    InMemory {
        vfiles: Mutex<HashMap<VFileId, VFile>>,
    },
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum StateError {
    /// Unexpected error {inner:?}
    Other {
        #[source]
        inner: eyre::Report,
    },
}

impl State {
    pub async fn set_file(&self, file: VFile) -> Result<(), StateError> {
        match self {
            State::InMemory { vfiles } => {
                vfiles
                    .lock()
                    .expect(ERROR_MUTEX)
                    .insert(file.id.clone(), file);
            }
        }
        Ok(())
    }
}
