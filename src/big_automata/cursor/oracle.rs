//! A naive model of the cursor machine and a differential proptest against it.
//!
//! The model keeps, per part, every slot the part ever admitted as a flat list of
//! `(job, cursor, done)` and recomputes the part's cursor from scratch on every query: no
//! watermark book, no incremental state.

use super::*;
use crate::ByteKey;
use proptest::prelude::*;

const PARTS: u8 = 3;
const OBJS: u8 = 3;
const MAX_CURSOR: Rev = 8;

fn obj(seed: u8) -> ObjKey {
    ObjKey(ByteKey::new(&[seed; 32]))
}

fn part(seed: u8) -> PartKey {
    PartKey(ByteKey::new(&[seed; 32]))
}

#[derive(Debug, Clone)]
enum Step {
    /// A membership touch of `obj` at `cursor` on the parts selected by the bitmask.
    Touch {
        cursor: Rev,
        obj: u8,
        mask: u8,
    },
    Remove {
        cursor: Rev,
        obj: u8,
        part: u8,
    },
    /// Settle the `pick`th job the model still owes, wrapping; nothing when it owes none.
    Settle {
        pick: usize,
    },
    /// A completion for an arbitrary job and cursor: mostly stale or duplicated.
    Complete {
        remove: bool,
        obj: u8,
        cursor: Rev,
    },
    RemovePart {
        part: u8,
    },
}

fn any_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (1..=MAX_CURSOR, 0..OBJS, 1u8..(1 << PARTS))
            .prop_map(|(cursor, obj, mask)| Step::Touch { cursor, obj, mask }),
        2 => (1..=MAX_CURSOR, 0..OBJS, 0..PARTS)
            .prop_map(|(cursor, obj, part)| Step::Remove { cursor, obj, part }),
        4 => any::<usize>().prop_map(|pick| Step::Settle { pick }),
        1 => (any::<bool>(), 0..OBJS, 1..=MAX_CURSOR)
            .prop_map(|(remove, obj, cursor)| Step::Complete { remove, obj, cursor }),
        1 => (0..PARTS).prop_map(|part| Step::RemovePart { part }),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Lane {
    Sync,
    Remove,
}

#[derive(Debug, Clone)]
struct Slot {
    lane: Lane,
    obj: u8,
    cursor: Rev,
    done: bool,
}

#[derive(Debug, Default)]
struct Oracle {
    parts: [Vec<Slot>; PARTS as usize],
    /// The cursor last reported for each part, to tell what a step newly reaches.
    reported: [Option<Rev>; PARTS as usize],
}

impl Oracle {
    /// The highest slot cursor such that every slot at or below it is done.
    fn reached(&self, part: usize) -> Option<Rev> {
        let slots = &self.parts[part];
        slots
            .iter()
            .filter(|slot| {
                slots
                    .iter()
                    .filter(|other| other.cursor <= slot.cursor)
                    .all(|other| other.done)
            })
            .map(|slot| slot.cursor)
            .max()
    }

    /// A part skips a cursor it already passed or already holds a slot at.
    fn saw(&self, part: usize, cursor: Rev) -> bool {
        self.reported[part].is_some_and(|reported| cursor <= reported)
            || self.parts[part].iter().any(|slot| slot.cursor == cursor)
    }

    fn owes(&self, lane: Lane, obj: u8, cursor: Rev) -> bool {
        self.parts
            .iter()
            .flatten()
            .any(|slot| slot.lane == lane && slot.obj == obj && slot.cursor == cursor && !slot.done)
    }

    fn owed_jobs(&self) -> Vec<(Lane, u8, Rev)> {
        let mut jobs: Vec<_> = self
            .parts
            .iter()
            .flatten()
            .filter(|slot| !slot.done)
            .map(|slot| (slot.lane, slot.obj, slot.cursor))
            .collect();
        jobs.sort();
        jobs.dedup();
        jobs
    }

    /// Advances every part reached since the last call, as `(part, cursor)`.
    fn advances(&mut self) -> Vec<(u8, Rev)> {
        let mut out = Vec::new();
        for index in 0..PARTS as usize {
            let reached = self.reached(index);
            if reached > self.reported[index] {
                out.push((index as u8, reached.expect("greater than something")));
                self.reported[index] = reached;
            }
        }
        out
    }

    fn settle(&mut self, lane: Lane, obj: u8, cursor: Rev) {
        for slot in self.parts.iter_mut().flatten() {
            if slot.lane == lane && slot.obj == obj && slot.cursor == cursor {
                slot.done = true;
            }
        }
    }
}

fn job_key(lane: Lane, obj_seed: u8) -> JobKey {
    match lane {
        Lane::Sync => JobKey::Sync(obj(obj_seed)),
        Lane::Remove => JobKey::RemoveFromPart(obj(obj_seed)),
    }
}

fn check(machine: &CursorSyncMachine, oracle: &Oracle) -> Result<(), TestCaseError> {
    for index in 0..PARTS as usize {
        let key = part(index as u8);
        let owes_nothing = oracle.parts[index].iter().all(|slot| slot.done);
        prop_assert_eq!(machine.is_settled(&key), owes_nothing, "part {}", index);
        prop_assert_eq!(
            machine.jobs.watermark(&key),
            oracle.reported[index],
            "part {}",
            index
        );
    }
    for lane in [Lane::Sync, Lane::Remove] {
        for obj_seed in 0..OBJS {
            for cursor in 0..=MAX_CURSOR {
                prop_assert_eq!(
                    machine.owes_obj_job(&job_key(lane, obj_seed), cursor),
                    oracle.owes(lane, obj_seed, cursor),
                    "{:?} {} {}",
                    lane,
                    obj_seed,
                    cursor
                );
            }
        }
    }
    Ok(())
}

/// The `SetPartCursor`s of a command list as `(part seed, cursor)` sorted by part.
fn set_cursors(out: &[CursorMachineCommand]) -> Vec<(u8, Rev)> {
    let mut sets: Vec<_> = out
        .iter()
        .filter_map(|cmd| match cmd {
            CursorMachineCommand::SetPartCursor { part_key, cursor } => Some((
                (0..PARTS)
                    .find(|seed| part(*seed) == *part_key)
                    .expect("a known part"),
                *cursor,
            )),
            _ => None,
        })
        .collect();
    sets.sort();
    sets
}

proptest! {
    #[test]
    fn matches_the_naive_oracle(steps in prop::collection::vec(any_step(), 1..60)) {
        let mut machine = CursorSyncMachine::default();
        let mut oracle = Oracle::default();
        for step in steps {
            let mut out = Vec::new();
            match step {
                Step::Touch { cursor, obj: obj_seed, mask } => {
                    let named: Vec<u8> = (0..PARTS).filter(|bit| mask & (1 << bit) != 0).collect();
                    let admitted: Vec<u8> = named
                        .iter()
                        .copied()
                        .filter(|index| !oracle.saw(*index as usize, cursor))
                        .collect();
                    for index in &admitted {
                        oracle.parts[*index as usize].push(Slot {
                            lane: Lane::Sync,
                            obj: obj_seed,
                            cursor,
                            done: false,
                        });
                    }
                    machine.on_subscription_evt(
                        CursorEvent::ObjChanged(ObjChanged {
                            cursor,
                            parts: named.iter().map(|index| part(*index)).collect(),
                            obj_key: obj(obj_seed),
                            payload: serde_json::json!(null),
                        }),
                        &mut out,
                    );
                    let sync: Vec<_> = out
                        .iter()
                        .filter_map(|cmd| match cmd {
                            CursorMachineCommand::SyncObj { parts, cursor: at, .. } => {
                                Some((parts.clone(), *at))
                            }
                            _ => None,
                        })
                        .collect();
                    let expected = if admitted.is_empty() {
                        vec![]
                    } else {
                        vec![(admitted.iter().map(|index| part(*index)).collect(), cursor)]
                    };
                    prop_assert_eq!(sync, expected);
                    prop_assert_eq!(out.len(), usize::from(!admitted.is_empty()));
                }
                Step::Remove { cursor, obj: obj_seed, part: part_seed } => {
                    let index = part_seed as usize;
                    let admitted = !oracle.saw(index, cursor);
                    if admitted {
                        oracle.parts[index].push(Slot {
                            lane: Lane::Remove,
                            obj: obj_seed,
                            cursor,
                            done: false,
                        });
                        for slot in &mut oracle.parts[index] {
                            if slot.lane == Lane::Sync && slot.obj == obj_seed && slot.cursor < cursor {
                                slot.done = true;
                            }
                        }
                    }
                    machine.on_subscription_evt(
                        CursorEvent::ObjRemovedFromPart(ObjRemovedFromPart {
                            cursor,
                            part_key: part(part_seed),
                            obj_key: obj(obj_seed),
                        }),
                        &mut out,
                    );
                    let removal = CursorMachineCommand::RemoveObjFromParts {
                        obj_key: obj(obj_seed),
                        part_key: part(part_seed),
                        cursor,
                    };
                    if admitted {
                        prop_assert_eq!(out.last(), Some(&removal));
                    } else {
                        prop_assert!(out.is_empty());
                    }
                    prop_assert_eq!(set_cursors(&out), oracle.advances());
                    check(&machine, &oracle)?;
                    continue;
                }
                Step::Settle { pick } => {
                    let owed = oracle.owed_jobs();
                    if owed.is_empty() {
                        continue;
                    }
                    let (lane, obj_seed, cursor) = owed[pick % owed.len()];
                    oracle.settle(lane, obj_seed, cursor);
                    machine.on_obj_sync_job_evt(job_key(lane, obj_seed), cursor, &mut out);
                }
                Step::Complete { remove, obj: obj_seed, cursor } => {
                    let lane = if remove { Lane::Remove } else { Lane::Sync };
                    oracle.settle(lane, obj_seed, cursor);
                    machine.on_obj_sync_job_evt(job_key(lane, obj_seed), cursor, &mut out);
                }
                Step::RemovePart { part: part_seed } => {
                    let index = part_seed as usize;
                    oracle.parts[index].clear();
                    oracle.reported[index] = None;
                    machine.remove_part(part(part_seed));
                }
            }
            prop_assert!(
                out.iter().all(|cmd| !matches!(cmd, CursorMachineCommand::RemoveObjFromParts { .. })),
                "only a removal event schedules a removal"
            );
            prop_assert_eq!(set_cursors(&out), oracle.advances());
            check(&machine, &oracle)?;
        }
    }
}
