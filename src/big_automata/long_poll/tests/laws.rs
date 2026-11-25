//! One test per law of the machine, in the module docs' numbering.

use super::*;

const FAR: u64 = 1_000_000;

#[test]
fn law_1_only_a_relevant_commit_touches_a_waiter() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1, 2], 5, FAR, linger(0, 0));
    assert_eq!(rig.commit(1, 6, &[(3, 1)]), [], "other interests");
    assert_eq!(rig.commit(2, 5, &[(1, 1)]), [], "at after_rev is not news");
    assert_eq!(
        rig.commit(3, 4, &[(1, 1)]),
        [],
        "below after_rev, told late"
    );
    assert_eq!(rig.commit(4, 7, &[(2, 1)]), [ready(1, 7)]);
}

#[test]
fn law_2_the_window_is_fixed_from_the_first_commit() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, FAR, linger(100, 1_000));
    assert_eq!(rig.commit(10, 1, &[(1, 1)]), [lingering(1, 110)]);
    // A steady stream: nothing extends the window, nothing ends it early.
    for (n, now) in (20..110).step_by(10).enumerate() {
        assert_eq!(rig.commit(now, 2 + n as u64, &[(1, 1)]), []);
    }
    assert_eq!(rig.tick(109), []);
    assert_eq!(rig.tick(110), [ready(1, 10)]);
}

#[test]
fn law_3_the_amount_ends_the_window_early() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1, 2], 0, FAR, linger(100, 5));
    assert_eq!(rig.commit(10, 1, &[(1, 2)]), [lingering(1, 110)]);
    assert_eq!(
        rig.commit(20, 2, &[(3, 9)]),
        [],
        "unwatched amounts do not count"
    );
    // One commit's amounts sum over the waiter's interests: 2 + 1 + 2.
    assert_eq!(rig.commit(30, 3, &[(1, 1), (2, 2)]), [ready(1, 3)]);
    // A commit that alone fills the page skips the window.
    rig.park(40, 2, &[1], 3, FAR, linger(100, 3));
    assert_eq!(rig.commit(41, 4, &[(1, 3)]), [ready(2, 4)]);
}

#[test]
fn law_4_the_deadline_caps_the_window_and_times_out_the_quiet() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, 500, linger(1_000, 100));
    rig.park(0, 2, &[2], 0, 500, linger(1_000, 100));
    assert_eq!(rig.commit(0, 1, &[(1, 1)]), [lingering(1, 500)]);
    assert_eq!(rig.tick(499), []);
    // At the deadline: news pending is `Ready`, none is `TimedOut`.
    assert_eq!(rig.tick(500), [ready(1, 1), timed_out(2)]);
}

#[test]
fn law_4_a_commit_after_the_deadline_does_not_revive_the_waiter() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, 100, linger(0, 0));
    assert_eq!(rig.commit(150, 1, &[(1, 1)]), [timed_out(1)]);
    assert_eq!(rig.poll.parked(), 0);
}

#[test]
fn law_5_one_terminal_output_each() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, 100, linger(0, 0));
    rig.park(0, 2, &[1], 0, 100, linger(0, 0));
    assert_eq!(rig.commit(1, 1, &[(1, 1)]), [ready(1, 1), ready(2, 1)]);
    assert_eq!(rig.commit(2, 2, &[(1, 1)]), []);
    assert_eq!(rig.tick(500), [], "a ready waiter does not time out too");
    // The id is free again.
    rig.park(600, 1, &[1], 2, 700, linger(0, 0));
    assert_eq!(rig.tick(700), [timed_out(1)]);
    assert_eq!(rig.tick(800), []);
}

#[test]
fn law_6_a_commit_racing_the_park_is_not_lost() {
    // Told before the park, and the poll's read ended before it.
    let mut rig = Rig::new();
    rig.commit(0, 7, &[(9, 1)]);
    assert_eq!(rig.park(1, 1, &[1], 3, FAR, linger(0, 0)), [ready(1, 7)]);
    // With a window the waiter lingers first, then is ready.
    assert_eq!(
        rig.park(2, 2, &[1], 3, FAR, linger(50, 10)),
        [lingering(2, 52)]
    );
    assert_eq!(rig.tick(52), [ready(2, 7)]);
    // A poll that read through the head is not behind it.
    assert_eq!(rig.park(60, 3, &[1], 7, FAR, linger(0, 0)), []);
    assert_eq!(rig.poll.parked(), 1);
}

#[test]
fn law_7_cancel_removes_without_output() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, 100, linger(0, 0));
    rig.park(0, 2, &[1], 0, 100, linger(50, 9));
    rig.commit(1, 1, &[(1, 1)]);
    rig.cancel(1);
    rig.cancel(2);
    rig.cancel(3);
    assert_eq!(rig.poll.parked(), 0);
    assert_eq!(rig.commit(2, 2, &[(1, 1)]), []);
    assert_eq!(rig.tick(500), []);
}

#[test]
fn law_8_the_index_holds_exactly_the_parked() {
    // `Rig` audits after every input; this churns every way a waiter can leave.
    let mut rig = Rig::new();
    for id in 0..12 {
        let interests = [(id % 3) as u8, (id % 4) as u8 + 1, (id % 3) as u8];
        rig.park(0, id, &interests, 0, 100 + id * 10, linger(30, 4));
    }
    rig.commit(5, 1, &[(0, 1), (1, 1)]);
    rig.cancel(2);
    rig.tick(40);
    rig.commit(41, 2, &[(2, 5)]);
    rig.tick(125);
    rig.tick(10_000);
    assert_eq!(rig.poll.parked(), 0);
    assert_eq!(rig.poll.next_due(), None);
}

#[test]
fn law_9_a_commit_touches_only_its_watchers() {
    let mut rig = Rig::new();
    for id in 0..200u64 {
        rig.park(0, id, &[(id % 100) as u8 + 10], 0, FAR, linger(0, 0));
    }
    let before = rig.poll.touched();
    assert_eq!(
        rig.commit(1, 1, &[(1, 1), (2, 1)]),
        [],
        "nobody watches 1 or 2"
    );
    assert_eq!(rig.poll.touched(), before);
    // Interest 10 is watched by waiters 0 and 100 and no one else.
    assert_eq!(rig.commit(2, 2, &[(10, 1)]), [ready(0, 2), ready(100, 2)]);
    assert_eq!(rig.poll.touched(), before + 2);
    // Parked waiters cost nothing to tick until one is due.
    let before = rig.poll.touched();
    assert_eq!(rig.tick(10), []);
    assert_eq!(rig.poll.touched(), before);
}

#[test]
fn law_10_time_never_goes_back_and_expiry_comes_first() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, 100, linger(0, 0));
    assert_eq!(rig.tick(200), [timed_out(1)]);
    // A late input's old `now` is read as 200: a waiter due at 250 is not yet due.
    rig.park(210, 2, &[1], 0, 250, linger(0, 0));
    assert_eq!(rig.tick(100), []);
    assert_eq!(rig.tick(250), [timed_out(2)]);
    // No tick between the deadline and the commit.
    rig.park(260, 3, &[1], 0, 300, linger(0, 0));
    assert_eq!(rig.commit(310, 1, &[(1, 1)]), [timed_out(3)]);
}

#[test]
fn law_11_ready_carries_the_head_the_machine_knows() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, FAR, linger(100, 99));
    assert_eq!(rig.commit(1, 3, &[(1, 1)]), [lingering(1, 101)]);
    rig.commit(2, 9, &[(7, 1)]);
    // Told late, below the head: the head does not go back.
    rig.commit(3, 5, &[(7, 1)]);
    assert_eq!(rig.tick(101), [ready(1, 9)]);
    // Commits told out of order: rev 6 is not news to a poll that read through 6, rev 5 is
    // to one that read through 4.
    rig.park(110, 2, &[1], 4, FAR, linger(0, 0));
    assert_eq!(
        rig.commit(111, 6, &[(2, 1)]),
        [],
        "head 9 is past 4, so the park itself was ready"
    );
}

#[test]
fn law_11_a_late_commit_is_news_below_the_head() {
    let mut rig = Rig::new();
    // The head is 9, and a poll that read through 9 parks.
    rig.commit(0, 9, &[(7, 1)]);
    rig.park(1, 1, &[1], 9, FAR, linger(0, 0));
    // Rev 5 told late is not news to it (5 <= 9), and rev 10 is.
    assert_eq!(rig.commit(2, 5, &[(1, 1)]), []);
    assert_eq!(rig.commit(3, 10, &[(1, 1)]), [ready(1, 10)]);
}

#[test]
fn an_empty_interest_set_only_times_out() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[], 0, 100, linger(0, 0));
    assert_eq!(rig.commit(1, 1, &[(1, 1)]), []);
    assert_eq!(rig.tick(100), [timed_out(1)]);
}

#[test]
#[should_panic(expected = "already parked")]
fn parking_a_live_id_twice_is_a_caller_bug() {
    let mut rig = Rig::new();
    rig.park(0, 1, &[1], 0, 100, linger(0, 0));
    rig.park(0, 1, &[1], 0, 100, linger(0, 0));
}
