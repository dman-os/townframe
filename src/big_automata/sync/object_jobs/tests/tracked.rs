//! Law 7's cap on tracked objects: objects waiting out a failure backoff leave the window but
//! still count against `max_tracked`, so a backend that fails everything stalls admission at
//! the cap instead of growing without bound.

use super::*;

/// Admit objects one at a time, each on `part_of(o)` at cursor `o`, for as long as `room`
/// allows, failing every record. Returns how many were admitted.
fn admit_failing_until_full(rig: &mut Rig, part_of: impl Fn(u8) -> u8) -> u8 {
    let mut admitted = 0;
    while rig.jobs.room() > 0 {
        admitted += 1;
        rig.cursor(
            admitted,
            part_of(admitted),
            present(admitted.into()),
            Rev::from(admitted),
        );
        for (call, _) in records(&rig.flush()) {
            rig.jobs.record_ended(call, Outcome::Failed);
        }
        assert!(
            usize::from(admitted) <= config().max_tracked,
            "admission ran past the cap"
        );
    }
    // arm the last failure's deadline, so one wait makes every object's retry due
    rig.flush();
    admitted
}

#[test]
fn law_7_persistently_failing_objects_stop_admission_exactly_at_the_cap() {
    let mut rig = Rig::new();
    let admitted = admit_failing_until_full(&mut rig, |_| 1);
    assert_eq!(usize::from(admitted), config().max_tracked);
    assert_eq!(rig.jobs.tracked(), config().max_tracked);
    assert_eq!(rig.jobs.room(), 0);
    // every one of them backs off outside the window, so the window alone would still admit
    let counted = rig
        .jobs
        .objs
        .values()
        .filter(|state| state.is_unsettled() && !state.backing_off())
        .count();
    assert_eq!(counted, 0);
    // they keep retrying on their ladder, and the cap still holds
    rig.wait(2);
    assert_eq!(records(&rig.flush()).len(), config().max_tracked);
    assert_eq!(rig.jobs.room(), 0);
}

#[test]
fn law_7_settling_one_tracked_object_reopens_room() {
    let mut rig = Rig::new();
    admit_failing_until_full(&mut rig, |_| 1);
    rig.wait(2);
    let mut retries = records(&rig.flush());
    assert_eq!(retries.len(), config().max_tracked);
    let (settled, _) = retries.remove(0);
    rig.jobs.record_ended(settled, DONE);
    for (call, _) in retries {
        rig.jobs.record_ended(call, Outcome::Failed);
    }
    assert_eq!(rig.jobs.tracked(), config().max_tracked - 1);
    assert_eq!(rig.jobs.room(), 1);
    rig.cursor(9, 1, present(9), 9);
    assert_eq!(rig.jobs.room(), 0, "the freed slot is taken");
}

#[test]
fn law_7_a_retired_part_frees_its_tracked_objects() {
    let mut rig = Rig::new();
    let half = config().max_tracked as u8 / 2;
    admit_failing_until_full(&mut rig, |o| if o <= half { 1 } else { 2 });
    assert_eq!(rig.jobs.room(), 0);
    rig.jobs.retire_part(&part(1));
    assert_eq!(rig.jobs.tracked(), usize::from(half));
    assert_eq!(rig.jobs.room(), usize::from(half).min(config().window));
}
