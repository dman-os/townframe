//! Law tests, ported regressions and the differential proptest of [`ObjectJobs`].

use super::*;

use crate::sync::cursor::JobKey::RemoveFromPart;
use crate::sync::cursor::JobKey::Sync;

mod backoff;
mod sim;
mod tracked;

fn obj(n: u8) -> ObjKey {
    ObjKey::new(&[n])
}

fn part(n: u8) -> PartKey {
    PartKey::new(&[100 + n])
}

fn present(n: u64) -> Want {
    Want::Present {
        payload: serde_json::json!(n),
    }
}

pub(super) fn config() -> Config {
    Config {
        window: 4,
        max_tracked: 8,
        fetch_cap: 2,
        min_backoff: Duration::from_secs(2),
        max_backoff: Duration::from_secs(5),
    }
}

struct Rig {
    jobs: ObjectJobs,
    now: Instant,
}

impl Rig {
    fn new() -> Self {
        Self {
            jobs: ObjectJobs::new(config()),
            now: Instant::now(),
        }
    }

    fn cursor(&mut self, o: u8, p: u8, want: Want, cursor: Rev) {
        self.jobs
            .want(obj(o), part(p), want, Origin::Cursor(cursor));
    }

    fn bucket(&mut self, o: u8, p: u8, want: Want) {
        self.jobs.want(obj(o), part(p), want, Origin::Bucket);
    }

    fn flush(&mut self) -> Vec<JobsCommand> {
        let mut out = vec![];
        self.jobs.flush(self.now, &mut out);
        out
    }

    fn wait(&mut self, secs: u64) {
        self.now += Duration::from_secs(secs);
    }

    /// Flush, expecting exactly one record, and return it.
    fn record(&mut self) -> (CallId, ObjJob) {
        let mut records = records(&self.flush());
        assert_eq!(records.len(), 1, "one record expected");
        records.remove(0)
    }

    fn fetch(&mut self) -> (CallId, ObjJob) {
        let mut fetches = fetches(&self.flush());
        assert_eq!(fetches.len(), 1, "one fetch expected");
        fetches.remove(0)
    }
}

fn records(cmds: &[JobsCommand]) -> Vec<(CallId, ObjJob)> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            JobsCommand::Record { call, job } => Some((*call, job.clone())),
            _ => None,
        })
        .collect()
}

fn fetches(cmds: &[JobsCommand]) -> Vec<(CallId, ObjJob)> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            JobsCommand::Fetch { call, job } => Some((*call, job.clone())),
            _ => None,
        })
        .collect()
}

fn cancels(cmds: &[JobsCommand]) -> Vec<CallId> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            JobsCommand::Cancel { call } => Some(*call),
            _ => None,
        })
        .collect()
}

fn settled(cmds: &[JobsCommand]) -> Vec<Settlement> {
    cmds.iter()
        .filter_map(|cmd| match cmd {
            JobsCommand::Settled(settlement) => Some(settlement.clone()),
            _ => None,
        })
        .collect()
}

fn sync_cursor(o: u8, cursor: Rev) -> Settlement {
    Settlement::Cursor {
        job: Sync(obj(o)),
        cursor,
    }
}

fn remove_cursor(o: u8, cursor: Rev) -> Settlement {
    Settlement::Cursor {
        job: RemoveFromPart(obj(o)),
        cursor,
    }
}

const DONE: Outcome<Settle> = Outcome::Succeeded(Settle::Done);
const FETCH: Outcome<Settle> = Outcome::Succeeded(Settle::Fetch);

/// Record `p` of object 1 at `cursor`, answer `Fetch`, and return the fetch call.
fn fetching(rig: &mut Rig, p: u8, cursor: Rev) -> CallId {
    rig.cursor(1, p, present(cursor), cursor);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, FETCH);
    rig.fetch().0
}

#[test]
fn law_1_one_record_and_one_fetch_per_object() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    let (record, job) = rig.record();
    assert_eq!(job.add, [part(1)]);
    rig.cursor(1, 2, present(2), 2);
    assert!(rig.flush().is_empty(), "wants merge while the record runs");
    rig.jobs.record_ended(record, FETCH);
    let cmds = rig.flush();
    let (_, next) = &records(&cmds)[0];
    assert_eq!(
        next.add,
        [part(2)],
        "the merged want goes out after the event"
    );
    assert_eq!(fetches(&cmds)[0].1.add, [part(1)]);
    rig.cursor(1, 3, present(3), 3);
    assert!(
        rig.flush().is_empty(),
        "one record and one fetch are in flight"
    );
}

#[test]
fn law_2_terminal_events_match_by_call_id() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record + 100, DONE);
    rig.jobs.fetch_ended(record + 101, Outcome::Succeeded(()));
    assert!(rig.flush().is_empty(), "an unknown call changes nothing");
    rig.jobs.record_ended(record, Outcome::Failed);
    rig.jobs.record_ended(record, DONE);
    assert!(
        rig.flush().is_empty(),
        "the repeated event of an ended call is stale"
    );
    assert!(rig.jobs.part_outstanding(&part(1)));
}

#[test]
fn law_3_wants_are_per_part_newest_cursor_wins() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(5), 5);
    rig.cursor(1, 1, Want::Absent, 3);
    rig.cursor(1, 1, Want::Absent, 5);
    let (record, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![part(1)], vec![]));
    assert_eq!(job.payload, Some(serde_json::json!(5)));
    rig.jobs.record_ended(record, DONE);
    assert_eq!(
        settled(&rig.flush()),
        [sync_cursor(1, 5)],
        "the dropped wants owe nothing"
    );
}

#[test]
fn law_3_a_bucket_walk_part_is_newest_by_arrival() {
    let mut rig = Rig::new();
    rig.bucket(1, 1, Want::Absent);
    rig.bucket(1, 1, present(7));
    rig.bucket(1, 1, Want::Absent);
    rig.bucket(1, 1, present(9));
    let (record, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![part(1)], vec![]));
    assert_eq!(job.payload, Some(serde_json::json!(9)));
    rig.jobs.record_ended(record, DONE);
    assert_eq!(
        settled(&rig.flush()),
        [Settlement::Bucket {
            part: part(1),
            obj: obj(1)
        }],
        "only the newest want is reported"
    );
}

#[test]
fn law_4_an_addition_runs_beside_a_fetch() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 2, present(2), 2);
    let cmds = rig.flush();
    assert_eq!(records(&cmds)[0].1.add, [part(2)]);
    assert!(cancels(&cmds).is_empty());
    rig.jobs.record_ended(records(&cmds)[0].0, FETCH);
    assert!(
        rig.flush().is_empty(),
        "its fetch joins the next one, after the first ends"
    );
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
    let cmds = rig.flush();
    assert_eq!(settled(&cmds), [sync_cursor(1, 1)]);
    assert_eq!(fetches(&cmds)[0].1.add, [part(2)]);
}

#[test]
fn law_4_a_contradiction_cancels_and_the_follow_up_record_waits() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), [fetch]);
    assert!(
        records(&cmds).is_empty(),
        "the removal waits for the cancelled event"
    );
    assert!(rig.flush().is_empty(), "and the cancel is sent once");
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    let (_, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![], vec![part(1)]));
}

/// The fetch carries its snapshot's removals too, so an add on a part it removes from
/// contradicts it just as a removal on a part it adds to does.
#[test]
fn law_4_an_add_on_a_part_the_fetch_removes_from_cancels_it() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    rig.cursor(1, 2, Want::Absent, 1);
    let (record, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![part(1)], vec![part(2)]));
    rig.jobs.record_ended(record, FETCH);
    let (fetch, job) = rig.fetch();
    assert_eq!(job.remove, [part(2)], "the fetch removes from part 2");
    rig.cursor(1, 2, present(2), 2);
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), [fetch]);
    assert!(
        records(&cmds).is_empty(),
        "the add waits for the cancelled event"
    );
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    let (_, job) = rig.record();
    assert_eq!(job.add, [part(2)]);
}

#[test]
fn law_4_retiring_a_part_the_fetch_adds_to_cancels_it() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.jobs.retire_part(&part(1));
    assert_eq!(cancels(&rig.flush()), [fetch]);
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    assert!(rig.flush().is_empty(), "nothing is left to fetch");
}

#[test]
fn law_5_the_next_call_is_the_difference() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    let (record, _) = rig.record();
    rig.cursor(1, 2, Want::Absent, 2);
    rig.jobs.record_ended(record, Outcome::Failed);
    rig.flush();
    rig.wait(2);
    let (_, job) = rig.record();
    assert_eq!(
        (job.add, job.remove),
        (vec![part(1)], vec![part(2)]),
        "the failed snapshot stays in"
    );
}

#[test]
fn law_6_a_superseded_cursor_settles_with_the_want_that_covers_it() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    let (first, _) = rig.record();
    rig.cursor(1, 1, present(2), 2);
    rig.jobs.record_ended(first, DONE);
    assert_eq!(
        settled(&rig.flush()),
        [remove_cursor(1, 1)],
        "the removal settles alone"
    );
}

#[test]
fn law_6_a_removal_settles_with_the_add_that_supersedes_it() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    let (first, _) = rig.record();
    rig.cursor(1, 1, present(2), 2);
    rig.jobs.record_ended(first, Outcome::Failed);
    rig.flush();
    rig.wait(2);
    let (second, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![part(1)], vec![]));
    rig.jobs.record_ended(second, DONE);
    assert_eq!(
        settled(&rig.flush()),
        [remove_cursor(1, 1), sync_cursor(1, 2)]
    );
}

#[test]
fn law_6_a_cursor_on_several_parts_settles_once() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(3), 3);
    rig.cursor(1, 2, present(3), 3);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, DONE);
    assert_eq!(settled(&rig.flush()), [sync_cursor(1, 3)]);
}

#[test]
fn law_6_a_retired_part_does_not_hold_back_the_part_that_settled() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(3), 3);
    let (first, _) = rig.record();
    rig.cursor(1, 2, present(3), 3);
    rig.jobs.record_ended(first, DONE);
    assert!(
        settled(&rig.flush()).is_empty(),
        "part 2 still owes the cursor"
    );
    rig.jobs.retire_part(&part(2));
    assert_eq!(settled(&rig.flush()), [sync_cursor(1, 3)]);
}

#[test]
fn law_6_retiring_a_part_drops_its_cursors_unsettled() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(3), 3);
    let (record, _) = rig.record();
    rig.jobs.retire_part(&part(1));
    rig.jobs.record_ended(record, DONE);
    assert!(rig.flush().is_empty());
    assert!(!rig.jobs.part_outstanding(&part(1)));
}

#[test]
fn law_6_bucket_wants_of_either_kind_settle_to_the_bucket_machine() {
    let mut rig = Rig::new();
    rig.bucket(1, 1, present(1));
    rig.bucket(2, 1, Want::Absent);
    let records = records(&rig.flush());
    assert_eq!(records.len(), 2);
    for (call, _) in &records {
        rig.jobs.record_ended(*call, DONE);
    }
    assert_eq!(
        settled(&rig.flush()),
        [
            Settlement::Bucket {
                part: part(1),
                obj: obj(1)
            },
            Settlement::Bucket {
                part: part(1),
                obj: obj(2)
            },
        ]
    );
}

#[test]
fn law_6_retiring_a_part_drops_its_bucket_settlement() {
    let mut rig = Rig::new();
    rig.bucket(1, 1, Want::Absent);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, DONE);
    rig.jobs.retire_part(&part(1));
    assert!(rig.flush().is_empty());
}

#[test]
fn law_7_the_window_counts_unsettled_objects() {
    let mut rig = Rig::new();
    assert_eq!(rig.jobs.room(), 4);
    for o in 1..=3 {
        rig.cursor(o, 1, present(1), Rev::from(o));
    }
    assert_eq!(rig.jobs.room(), 1);
    rig.cursor(3, 2, present(1), 9);
    assert_eq!(rig.jobs.room(), 1, "per object, not per want");
    for (call, _) in records(&rig.flush()) {
        rig.jobs.record_ended(call, DONE);
    }
    assert_eq!(rig.jobs.room(), 4);
}

#[test]
fn law_7_fetches_start_in_cursor_order_within_the_cap() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 9);
    rig.cursor(2, 1, present(1), 3);
    rig.bucket(3, 1, present(1));
    rig.cursor(4, 1, present(1), 5);
    for (call, _) in records(&rig.flush()) {
        rig.jobs.record_ended(call, FETCH);
    }
    let started = fetches(&rig.flush());
    let objs: Vec<_> = started.iter().map(|(_, job)| job.obj.clone()).collect();
    assert_eq!(objs, [obj(2), obj(4)], "the cap is 2, lowest cursors first");
    rig.jobs.fetch_ended(started[0].0, Outcome::Succeeded(()));
    let next = fetches(&rig.flush());
    assert_eq!(next[0].1.obj, obj(1));
    rig.jobs.fetch_ended(started[1].0, Outcome::Succeeded(()));
    rig.jobs.fetch_ended(next[0].0, Outcome::Succeeded(()));
    assert_eq!(
        fetches(&rig.flush())[0].1.obj,
        obj(3),
        "bucket origin goes last"
    );
}

#[test]
fn law_7_a_cancelled_fetch_holds_its_slot_until_its_event() {
    let mut rig = Rig::new();
    let first = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    rig.cursor(2, 1, present(1), 3);
    rig.cursor(3, 1, present(1), 4);
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), [first]);
    for (call, _) in records(&cmds) {
        rig.jobs.record_ended(call, FETCH);
    }
    assert_eq!(
        fetches(&rig.flush()).len(),
        1,
        "the cancelled call still counts toward the cap"
    );
}

#[test]
fn law_8_retry_backs_off_and_keeps_it_across_merges() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    let (call, _) = rig.record();
    rig.jobs.record_ended(call, Outcome::Failed);
    assert!(rig.flush().is_empty());
    assert_eq!(rig.jobs.next_due(), Some(rig.now + Duration::from_secs(2)));
    rig.cursor(1, 2, present(2), 2);
    rig.wait(1);
    assert!(rig.flush().is_empty(), "a merge does not hurry the retry");
    rig.wait(1);
    let (call, job) = rig.record();
    assert_eq!(job.add, [part(1), part(2)]);
    rig.jobs.record_ended(call, Outcome::Failed);
    rig.flush();
    assert_eq!(
        rig.jobs.next_due(),
        Some(rig.now + Duration::from_secs(4)),
        "doubled"
    );
    rig.wait(4);
    let (call, _) = rig.record();
    rig.jobs.record_ended(call, Outcome::Failed);
    rig.flush();
    assert_eq!(
        rig.jobs.next_due(),
        Some(rig.now + Duration::from_secs(5)),
        "capped"
    );
    rig.wait(5);
    let (call, _) = rig.record();
    rig.jobs.record_ended(call, DONE);
    rig.cursor(1, 1, present(3), 3);
    let (call, _) = rig.record();
    rig.jobs.record_ended(call, Outcome::Failed);
    rig.flush();
    assert_eq!(
        rig.jobs.next_due(),
        Some(rig.now + Duration::from_secs(2)),
        "a success resets it"
    );
}

#[test]
fn law_8_a_failed_object_sync_is_paced_on_the_retry_ladder_instead_of_dropped() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.jobs.fetch_ended(fetch, Outcome::Failed);
    assert!(rig.flush().is_empty(), "no retry in the same flush");
    assert!(
        rig.jobs.part_outstanding(&part(1)),
        "the route survives the failure"
    );
    let due = rig.jobs.next_due().expect("the retry is paced");
    rig.now = due;
    let (_, job) = rig.fetch();
    assert_eq!(job.add, [part(1)], "the snapshot survives");
    assert!(rig.flush().is_empty(), "one failure buys one retry");
}

#[test]
fn law_9_an_object_route_keeps_content_alive() {
    let mut rig = Rig::new();
    rig.jobs.set_object_route(obj(1), true);
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    let cmds = rig.flush();
    assert!(
        cancels(&cmds).is_empty(),
        "removing the last part does not cancel the routed fetch"
    );
    assert!(
        records(&cmds).is_empty(),
        "but the removal waits for it, as the fetch may still be adding"
    );
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
    assert_eq!(records(&rig.flush())[0].1.remove, [part(1)]);
}

#[test]
fn without_a_route_the_same_removal_cancels_the_fetch() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    assert_eq!(cancels(&rig.flush()), [fetch]);
}

#[test]
fn an_object_route_alone_issues_a_job_without_part_or_payload() {
    let mut rig = Rig::new();
    rig.jobs.set_object_route(obj(1), true);
    let (record, job) = rig.record();
    assert_eq!((job.add.len(), job.remove.len(), job.payload), (0, 0, None));
    rig.jobs.record_ended(record, DONE);
    assert!(rig.flush().is_empty());
    assert_eq!(rig.jobs.room(), 4, "a met route is not an unsettled want");
}

#[test]
fn law_9_a_content_want_carries_its_payload_and_owes_no_cursor() {
    let mut rig = Rig::new();
    rig.jobs.want_content(obj(1), serde_json::json!(7));
    let (record, job) = rig.record();
    assert_eq!(
        (job.add.len(), job.remove.len(), job.payload),
        (0, 0, Some(serde_json::json!(7)))
    );
    rig.jobs.record_ended(record, FETCH);
    let (fetch, job) = rig.fetch();
    assert_eq!(
        job.payload,
        Some(serde_json::json!(7)),
        "the fetch carries it too"
    );
    assert_eq!(
        rig.jobs.room(),
        3,
        "an unmet content want holds a window slot"
    );
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
    assert!(settled(&rig.flush()).is_empty(), "no cursor was owed");
    assert_eq!(rig.jobs.room(), 4);
}

#[test]
fn law_9_a_newer_content_want_replaces_a_settled_one() {
    let mut rig = Rig::new();
    rig.jobs.want_content(obj(1), serde_json::json!(1));
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, DONE);
    assert!(rig.flush().is_empty());
    rig.jobs.want_content(obj(1), serde_json::json!(2));
    let (_, job) = rig.record();
    assert_eq!(job.payload, Some(serde_json::json!(2)));
}

#[test]
fn law_9_a_content_want_merges_beside_a_running_fetch_without_cancelling_it() {
    let mut rig = Rig::new();
    rig.jobs.want_content(obj(1), serde_json::json!(1));
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, FETCH);
    let (fetch, _) = rig.fetch();
    rig.jobs.want_content(obj(1), serde_json::json!(2));
    let cmds = rig.flush();
    assert!(
        cancels(&cmds).is_empty(),
        "newer content contradicts nothing"
    );
    assert_eq!(records(&cmds)[0].1.payload, Some(serde_json::json!(2)));
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
    assert_eq!(
        rig.jobs.room(),
        3,
        "the fetch covered the older want only; the newer one is still owed"
    );
}

#[test]
fn law_9_a_part_wants_hint_outranks_the_routes_in_a_shared_job() {
    let mut rig = Rig::new();
    rig.jobs.want_content(obj(1), serde_json::json!("route"));
    rig.cursor(1, 1, present(5), 5);
    let (_, job) = rig.record();
    assert_eq!(job.add, [part(1)]);
    assert_eq!(job.payload, Some(serde_json::json!(5)));
}

#[test]
fn law_9_a_content_want_keeps_a_fetch_alive_when_the_last_part_goes() {
    let mut rig = Rig::new();
    rig.jobs.want_content(obj(1), serde_json::json!(9));
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    assert!(
        cancels(&rig.flush()).is_empty(),
        "the route's content still wants the bytes"
    );
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
}

#[test]
fn law_9_turning_the_route_off_cancels_a_fetch_only_it_justified() {
    let mut rig = Rig::new();
    rig.jobs.set_object_route(obj(1), true);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, FETCH);
    let (fetch, _) = rig.fetch();
    rig.jobs.set_object_route(obj(1), false);
    assert_eq!(cancels(&rig.flush()), [fetch]);
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    assert!(rig.flush().is_empty());
    assert_eq!(rig.jobs.room(), 4);
}

#[test]
fn law_10_the_connections_end_ends_everything() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(2, 1, present(1), 2);
    let (record, _) = rig.record();
    rig.cursor(3, 1, present(1), 3);
    let (done, _) = rig.record();
    rig.jobs.record_ended(done, DONE);
    let mut out = vec![];
    rig.jobs.clear(&mut out);
    let mut cancelled = cancels(&out);
    cancelled.sort();
    assert_eq!(cancelled, [fetch, record]);
    assert_eq!(out.len(), 2, "nothing is reported");
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    rig.jobs.record_ended(record, DONE);
    assert!(rig.flush().is_empty());
    assert_eq!(rig.jobs.room(), 4);
    assert!(!rig.jobs.part_outstanding(&part(1)));
    rig.cursor(1, 1, present(9), 1);
    let (call, _) = rig.record();
    assert!(call > record, "ids are not reused");
}

#[test]
fn the_payload_is_the_newest_cursors() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(7), 5);
    rig.cursor(1, 2, present(8), 2);
    rig.cursor(1, 3, Want::Absent, 9);
    let (_, job) = rig.record();
    assert_eq!(job.payload, Some(serde_json::json!(7)));
}

#[test]
fn removal_cancels_retrying_sync_task_when_its_last_part_is_removed() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.jobs.fetch_ended(fetch, Outcome::Failed);
    rig.flush();
    rig.cursor(1, 1, Want::Absent, 2);
    rig.wait(2);
    let (_, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![], vec![part(1)]));
    assert!(rig.flush().is_empty(), "no fetch is left to retry");
}

#[test]
fn removal_keeps_sync_task_without_part_hints_while_object_is_subscribed() {
    let mut rig = Rig::new();
    rig.jobs.set_object_route(obj(1), true);
    let fetch = fetching(&mut rig, 1, 5);
    rig.cursor(1, 1, Want::Absent, 6);
    let cmds = rig.flush();
    assert!(cancels(&cmds).is_empty());
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
    let cmds = rig.flush();
    assert_eq!(
        settled(&cmds),
        [sync_cursor(1, 5)],
        "the claim survives and is acknowledged"
    );
}

#[test]
fn a_removal_keeps_an_object_routed_content_fetch_alive() {
    removal_keeps_sync_task_without_part_hints_while_object_is_subscribed();
}

#[test]
fn partial_removal_trim_restarts_task_with_remaining_hints() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, present(1), 1);
    rig.cursor(1, 2, present(2), 2);
    let (record, _) = rig.record();
    rig.jobs.record_ended(record, FETCH);
    let (fetch, job) = rig.fetch();
    assert_eq!(job.add, [part(1), part(2)]);
    rig.cursor(1, 1, Want::Absent, 3);
    assert_eq!(cancels(&rig.flush()), [fetch]);
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    let cmds = rig.flush();
    assert_eq!(
        fetches(&cmds)[0].1.add,
        [part(2)],
        "restarted with the remaining part"
    );
    assert_eq!(records(&cmds)[0].1.remove, [part(1)]);
}

#[test]
fn readd_after_removal_triggers_resync() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    let (removal, _) = rig.record();
    rig.cursor(1, 1, present(2), 2);
    assert!(
        rig.flush().is_empty(),
        "the re-add waits for the removal's event"
    );
    rig.jobs.record_ended(removal, DONE);
    let cmds = rig.flush();
    assert_eq!(records(&cmds)[0].1.add, [part(1)]);
}

#[test]
fn readd_settles_each_lane_from_the_worker_that_owes_it() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    let (removal, _) = rig.record();
    rig.cursor(1, 1, present(2), 2);
    rig.jobs.record_ended(removal, DONE);
    let cmds = rig.flush();
    assert_eq!(
        settled(&cmds),
        [remove_cursor(1, 1)],
        "the removal's own lane, nothing else"
    );
    let (readd, _) = records(&cmds)[0].clone();
    rig.jobs.record_ended(readd, DONE);
    assert_eq!(settled(&rig.flush()), [sync_cursor(1, 2)]);
    assert!(!rig.jobs.part_outstanding(&part(1)));
}

#[test]
fn a_failed_cancelled_removal_still_finishes_its_membership_lanes() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    let (removal, _) = rig.record();
    rig.cursor(1, 1, present(2), 2);
    rig.jobs.record_ended(removal, Outcome::Failed);
    rig.flush();
    rig.wait(2);
    let (readd, job) = rig.record();
    assert_eq!(job.add, [part(1)]);
    rig.jobs.record_ended(readd, DONE);
    assert_eq!(
        settled(&rig.flush()),
        [remove_cursor(1, 1), sync_cursor(1, 2)]
    );
}

#[test]
fn removed_peer_part_cancels_deferred_readd_before_removal_completes() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    let (removal, _) = rig.record();
    rig.cursor(1, 1, present(2), 2);
    rig.jobs.retire_part(&part(1));
    rig.jobs.record_ended(removal, DONE);
    assert!(
        rig.flush().is_empty(),
        "completion must not resurrect a retired part's re-add"
    );
}

/// `cancel_obj_removal_hint` removed the worker on the first matching hint, so the other
/// re-added parts stayed in the re-removal and could be evicted after their re-sync.
#[test]
fn regression_a_readd_of_several_parts_leaves_none_in_a_removal() {
    let mut rig = Rig::new();
    rig.cursor(1, 1, Want::Absent, 1);
    rig.cursor(1, 2, Want::Absent, 1);
    let (removal, job) = rig.record();
    assert_eq!(job.remove, [part(1), part(2)]);
    rig.cursor(1, 1, present(2), 2);
    rig.cursor(1, 2, present(2), 2);
    rig.jobs.record_ended(removal, DONE);
    let (_, job) = rig.record();
    assert_eq!((job.add, job.remove), (vec![part(1), part(2)], vec![]));
}

/// New work during a deferral spawned at once and raced the zombie; the resume then overwrote
/// the live worker and leaked its task.
#[test]
fn regression_work_during_a_deferral_neither_races_nor_leaks() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    assert_eq!(cancels(&rig.flush()), [fetch]);
    rig.cursor(1, 1, present(3), 3);
    rig.cursor(1, 2, present(3), 4);
    for _ in 0..3 {
        assert!(
            rig.flush().is_empty(),
            "no call starts while the zombie runs"
        );
    }
    rig.jobs.fetch_ended(fetch, Outcome::Cancelled);
    let cmds = rig.flush();
    assert_eq!(records(&cmds).len(), 1);
    assert!(fetches(&cmds).is_empty());
    assert!(
        rig.flush().is_empty(),
        "exactly one call took the work over"
    );
}

/// A zombie removal was told apart from the live one by "no remove worker" instead of by task
/// id, so a superseded task's completion released the deferred work early.
#[test]
fn regression_a_stale_event_does_not_release_a_deferral() {
    let mut rig = Rig::new();
    let first = fetching(&mut rig, 1, 1);
    rig.jobs.fetch_ended(first, Outcome::Failed);
    rig.flush();
    rig.wait(2);
    let (second, _) = rig.fetch();
    rig.cursor(1, 1, Want::Absent, 2);
    assert_eq!(cancels(&rig.flush()), [second]);
    rig.jobs.fetch_ended(first, Outcome::Succeeded(()));
    rig.jobs.fetch_ended(first, Outcome::Cancelled);
    assert!(rig.flush().is_empty(), "the ended call's event is stale");
    assert!(rig.jobs.part_outstanding(&part(1)));
    rig.jobs.fetch_ended(second, Outcome::Cancelled);
    assert_eq!(records(&rig.flush())[0].1.remove, [part(1)]);
}

#[test]
fn a_fetch_that_raced_its_cancel_and_succeeded_settles_only_what_it_covered() {
    let mut rig = Rig::new();
    let fetch = fetching(&mut rig, 1, 1);
    rig.cursor(1, 1, Want::Absent, 2);
    rig.flush();
    rig.jobs.fetch_ended(fetch, Outcome::Succeeded(()));
    let cmds = rig.flush();
    assert_eq!(settled(&cmds), [sync_cursor(1, 1)]);
    assert_eq!(records(&cmds)[0].1.remove, [part(1)]);
}

mod prop {
    use super::*;
    use proptest::prelude::*;
    use sim::Op;
    use sim::Sim;

    proptest! {
        #[test]
        fn differential_against_the_oracle(ops in proptest::collection::vec(sim::op(), 1..80)) {
            let mut sim = Sim::new();
            for op in ops {
                sim.step(op);
            }
            sim.drain();
        }
    }

    /// Found by the proptest: the oracle forgot that a part had settled a cursor once that
    /// part was retired, so it never reported the cursor the other part still held back.
    #[test]
    fn a_cursor_settled_by_one_part_is_reported_when_the_other_is_retired() {
        let w = |obj, part, cursor| Op::Want {
            obj,
            part,
            present: None,
            cursor: Some(cursor),
        };
        let mut sim = Sim::new();
        for op in [
            w(1, 1, 3),
            Op::Flush { secs: 0 },
            Op::Finish {
                pick: 0,
                how: sim::How::Fetch,
            },
            w(1, 0, 3),
            Op::Flush { secs: 0 },
            Op::Finish {
                pick: 1,
                how: sim::How::Done,
            },
            Op::Retire { part: 0 },
            Op::Retire { part: 1 },
        ] {
            sim.step(op);
        }
        sim.drain();
    }
}
