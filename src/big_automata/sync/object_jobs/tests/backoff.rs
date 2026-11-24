//! Law 7's exception: an object that only waits out a failure backoff does not count against
//! the receive window, and still holds its cursors back.

use super::*;

/// Record objects `1..=n` (object `o` at cursor `o`) and fail every record.
fn fail_records(rig: &mut Rig, n: u8) {
    for o in 1..=n {
        rig.cursor(o, 1, present(o.into()), Rev::from(o));
    }
    let records = records(&rig.flush());
    assert_eq!(records.len(), usize::from(n));
    for (call, _) in records {
        rig.jobs.record_ended(call, Outcome::Failed);
    }
}

#[test]
fn law_7_window_many_failing_objects_no_longer_stop_the_rounds() {
    let mut rig = Rig::new();
    let window = config().window as u8;
    fail_records(&mut rig, window);
    assert_eq!(
        rig.jobs.room(),
        config().window,
        "a failed record's object waits out its backoff outside the window"
    );
    rig.flush();
    assert_eq!(rig.jobs.room(), config().window, "armed, still waiting");
    // fresh objects are admitted and flow while the others keep backing off
    for o in window + 1..=window + 2 {
        rig.cursor(o, 1, present(o.into()), Rev::from(o));
    }
    assert_eq!(rig.jobs.room(), config().window - 2);
    let fresh = records(&rig.flush());
    assert_eq!(fresh.len(), 2, "only the new objects are due");
    for (call, _) in fresh {
        rig.jobs.record_ended(call, DONE);
    }
    assert_eq!(rig.jobs.room(), config().window);
    // from the moment its next call is issued, a failing object counts again
    rig.wait(2);
    let retries = records(&rig.flush());
    assert_eq!(retries.len(), usize::from(window));
    assert_eq!(rig.jobs.room(), 0, "in flight again");
    for (call, _) in retries {
        rig.jobs.record_ended(call, Outcome::Failed);
    }
    assert_eq!(rig.jobs.room(), config().window, "backing off again");
}

#[test]
fn law_7_a_failing_fetch_backs_off_outside_the_window_too() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    assert_eq!(rig.jobs.room(), config().window - 1);
    rig.jobs.fetch_ended(fetch, Outcome::Failed);
    assert_eq!(rig.jobs.room(), config().window);
    rig.flush();
    rig.wait(2);
    rig.fetch();
    assert_eq!(
        rig.jobs.room(),
        config().window - 1,
        "counts from its issue"
    );
}

#[test]
fn law_7_a_merged_want_does_not_bring_a_backing_off_object_back_into_the_window() {
    let mut rig = Rig::new();
    fail_records(&mut rig, 1);
    rig.flush();
    rig.cursor(1, 2, present(9), 9);
    assert_eq!(
        rig.jobs.room(),
        config().window,
        "a merge keeps the backoff"
    );
    rig.wait(2);
    rig.flush();
    assert_eq!(rig.jobs.room(), config().window - 1);
}

#[test]
fn law_7_an_object_with_other_due_work_counts() {
    let mut rig = Rig::new();
    // a fetch in flight is live work, even while the record lane waits out a failure
    rig.cursor(1, 1, present(1), 1);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, FETCH);
    let fetch = rig.fetch().0;
    rig.cursor(1, 2, present(2), 2);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, Outcome::Failed);
    rig.flush();
    assert_eq!(rig.jobs.room(), config().window - 1, "a fetch is in flight");
    rig.jobs.fetch_ended(fetch, Outcome::Failed);
    assert_eq!(rig.jobs.room(), config().window, "both lanes wait");
}

#[test]
fn law_7_a_failing_object_still_holds_its_cursor_while_the_others_settle() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    rig.cursor(2, 1, present(2), 2);
    for (call, job) in records(&rig.flush()) {
        let outcome = if job.obj == obj(1) {
            Outcome::Failed
        } else {
            DONE
        };
        rig.jobs.record_ended(call, outcome);
    }
    assert_eq!(
        settled(&rig.flush()),
        [sync_cursor(2, 2)],
        "object 2's cursor settles; object 1's, below it, is still owed"
    );
    assert!(rig.jobs.part_outstanding(&part(1)));
    // it keeps retrying on its ladder and settles when it finally succeeds
    for _ in 0..3 {
        rig.wait(5);
        let (call, _) = rig.record();
        rig.jobs.record_ended(call, Outcome::Failed);
        assert!(settled(&rig.flush()).is_empty());
        assert_eq!(rig.jobs.room(), config().window);
    }
    rig.wait(5);
    let (call, _) = rig.record();
    rig.jobs.record_ended(call, DONE);
    assert_eq!(settled(&rig.flush()), [sync_cursor(1, 1)]);
}
