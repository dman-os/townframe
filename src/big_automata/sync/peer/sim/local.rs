//! The local side: the fake backend over a local memory part store, and the cursors the
//! embedder persisted.
//!
//! `record` applies a job's removals and adds (an add is a pending membership until the object
//! has a payload) and answers `Done`, which also copies the payload (the record did the whole
//! work), or `Fetch`. `fetch` copies the payload after a number of progress steps, some very
//! long, and honours `Cancel`. The payload copied is the peer's current one, as a real fetch
//! of the bytes would get. A call applies all or nothing.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::executor::block_on;

use crate::interlude::*;

use crate::part_store::ObjPayload;
use crate::part_store::PartStoreRead;
use crate::part_store::laws;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartWrite;
use crate::rev::Rev;
use crate::sync::object_jobs::ObjJob;
use crate::sync::object_jobs::Outcome;
use crate::sync::object_jobs::Settle;
use crate::sync::peer::CallId;

#[derive(Debug)]
pub(super) enum Flight {
    Record {
        job: ObjJob,
        cancelled: bool,
    },
    Fetch {
        job: ObjJob,
        /// Progress steps left; the fetch lands when they run out.
        steps: u32,
        cancelled: bool,
    },
}

impl Flight {
    pub(super) fn job(&self) -> &ObjJob {
        match self {
            Self::Record { job, .. } | Self::Fetch { job, .. } => job,
        }
    }

    pub(super) fn cancelled(&self) -> bool {
        match self {
            Self::Record { cancelled, .. } | Self::Fetch { cancelled, .. } => *cancelled,
        }
    }

    pub(super) fn cancel(&mut self) {
        match self {
            Self::Record { cancelled, .. } | Self::Fetch { cancelled, .. } => *cancelled = true,
        }
    }
}

/// What a progress step did to a call, to be reported to the machine.
pub(super) enum Step {
    /// The call is still running.
    Running,
    Record(Outcome<Settle>),
    Fetch(Outcome<()>),
}

pub(super) struct Local {
    pub store: MemoryPartStore,
    /// What the embedder persisted per part; survives crashes.
    pub persisted: BTreeMap<PartKey, Rev>,
    pub calls: BTreeMap<CallId, Flight>,
    /// Scenario overrides: the next settlement and fetch length are not drawn.
    pub force_settle: Option<Settle>,
    pub force_steps: Option<u32>,
    /// Scenario override: every record answers this.
    pub settle_always: Option<Settle>,
    objs: Vec<ObjKey>,
    rng: u64,
}

impl Local {
    pub(super) fn new(objs: u8, seed: u64) -> Self {
        Self {
            store: default(),
            persisted: default(),
            calls: default(),
            force_settle: None,
            force_steps: None,
            settle_always: None,
            objs: (0..objs).map(laws::obj).collect(),
            rng: seed,
        }
    }

    /// A draw below `n` (splitmix64).
    pub(super) fn roll(&mut self, n: u32) -> u32 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        ((z ^ (z >> 31)) % u64::from(n)) as u32
    }

    /// The live members of `part`: objects with content that are members of it. A pending
    /// membership is not live.
    pub(super) fn members(&self, part: &PartKey) -> BTreeSet<ObjKey> {
        let states = block_on(self.store.obj_states(&self.objs)).unwrap();
        self.objs
            .iter()
            .zip(states)
            .filter_map(|(obj, state)| {
                let state = state?;
                (state.content.is_some() && state.parts.contains(part)).then(|| obj.clone())
            })
            .collect()
    }

    pub(super) fn payload(&self, obj: &ObjKey) -> Option<ObjPayload> {
        let state = block_on(self.store.obj_states(std::slice::from_ref(obj))).unwrap();
        state[0]
            .as_ref()?
            .content
            .as_ref()
            .map(|(payload, _)| payload.clone())
    }

    pub(super) fn issue_record(&mut self, call: CallId, job: ObjJob) {
        self.calls.insert(
            call,
            Flight::Record {
                job,
                cancelled: false,
            },
        );
    }

    pub(super) fn issue_fetch(&mut self, call: CallId, job: ObjJob) {
        // mostly short; one in six is very long, so cancellation and contradictions land
        // while a fetch is still running
        let steps = self.force_steps.take().unwrap_or_else(|| {
            if self.roll(6) == 0 {
                30
            } else {
                1 + self.roll(3)
            }
        });
        self.calls.insert(
            call,
            Flight::Fetch {
                job,
                steps,
                cancelled: false,
            },
        );
    }

    /// Whether a job moves the payload: any job that adds, and a content job (no parts at
    /// all). A removal alone does not.
    fn wants_payload(job: &ObjJob) -> bool {
        !job.add.is_empty() || job.remove.is_empty()
    }

    fn write(&self, writes: Vec<PartWrite>) {
        let mut tx = self.store.begin();
        for write in writes {
            block_on(laws::stage(&self.store, &mut tx, write)).unwrap();
        }
        tx.commit();
    }

    fn apply_membership(&self, job: &ObjJob, payload: Option<&ObjPayload>) {
        let mut writes = vec![];
        if !job.remove.is_empty() {
            writes.push(PartWrite::RemoveMembers(
                job.remove
                    .iter()
                    .map(|p| (job.obj.clone(), p.clone()))
                    .collect(),
            ));
        }
        if !job.add.is_empty() {
            writes.push(PartWrite::AddMembers(
                job.add
                    .iter()
                    .map(|p| (job.obj.clone(), p.clone()))
                    .collect(),
            ));
        }
        if let Some(payload) = payload {
            writes.push(PartWrite::SetPayloads(vec![(
                job.obj.clone(),
                payload.clone(),
            )]));
        }
        self.write(writes);
    }

    /// Run one step of `call`. `peer_payload` is the peer's current payload of the job's
    /// object. `finish` runs a fetch to its end and never fails a call (the quiet drain);
    /// otherwise a call fails one time in eight, and a cancelled call that is not caught up
    /// with its `Cancel` completes anyway one time in three (the race the contract allows).
    pub(super) fn step(
        &mut self,
        call: CallId,
        peer_payload: Option<ObjPayload>,
        finish: bool,
    ) -> Step {
        let raced = self.calls[&call].cancelled() && self.roll(3) == 0;
        let fails = !finish && self.roll(8) == 0;
        let cancelled = self.calls[&call].cancelled() && !raced;
        let settle = self
            .force_settle
            .take()
            .or(self.settle_always)
            .unwrap_or_else(|| {
                if self.roll(2) == 0 {
                    Settle::Done
                } else {
                    Settle::Fetch
                }
            });
        match self.calls.get_mut(&call).expect("a call in flight") {
            Flight::Record { job, .. } => {
                if cancelled || fails {
                    let outcome = if cancelled {
                        Outcome::Cancelled
                    } else {
                        Outcome::Failed
                    };
                    self.calls.remove(&call);
                    return Step::Record(outcome);
                }
                let job = job.clone();
                self.calls.remove(&call);
                let copy = settle == Settle::Done && Self::wants_payload(&job);
                self.apply_membership(&job, peer_payload.as_ref().filter(|_| copy));
                Step::Record(Outcome::Succeeded(settle))
            }
            Flight::Fetch { job, steps, .. } => {
                if cancelled || fails {
                    let outcome = if cancelled {
                        Outcome::Cancelled
                    } else {
                        Outcome::Failed
                    };
                    self.calls.remove(&call);
                    return Step::Fetch(outcome);
                }
                *steps = steps.saturating_sub(1);
                if *steps > 0 && !finish {
                    return Step::Running;
                }
                let job = job.clone();
                self.calls.remove(&call);
                self.land_fetch(&job, peer_payload.as_ref());
                Step::Fetch(Outcome::Succeeded(()))
            }
        }
    }

    /// A fetch's bytes arrive: the payload is set, which promotes the object's pending
    /// memberships.
    fn land_fetch(&self, job: &ObjJob, peer_payload: Option<&ObjPayload>) {
        if let Some(payload) = peer_payload.filter(|_| Self::wants_payload(job)) {
            self.write(vec![PartWrite::SetPayloads(vec![(
                job.obj.clone(),
                payload.clone(),
            )])]);
        }
    }

    /// The process dies with `call` in flight: its work either committed before the crash or
    /// never started, and nobody is told either way.
    pub(super) fn die_with(
        &mut self,
        call: CallId,
        committed: bool,
        peer_payload: Option<ObjPayload>,
    ) {
        let flight = self.calls.remove(&call).expect("a call in flight");
        if !committed {
            return;
        }
        match flight {
            Flight::Record { job, .. } => self.apply_membership(&job, None),
            Flight::Fetch { job, .. } => self.land_fetch(&job, peer_payload.as_ref()),
        }
    }
}
