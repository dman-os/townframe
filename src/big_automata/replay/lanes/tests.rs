//! Law tests of [`Lanes`], one or more per law, over a real [`ReplayClient`] so rounds carry
//! real target ids. The differential proptest is in `sim`.

use super::*;

use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::replay::wire::SessionUpdated;

mod sim;

pub(super) fn part_route(index: u8) -> Route {
    Route::Part {
        part_key: part(index),
    }
}

pub(super) fn obj_route(index: u8) -> Route {
    Route::Object {
        obj_key: obj(index),
    }
}

pub(super) fn config() -> Config {
    Config {
        hold_ms: 300,
        live_limit: 2,
        bulk_batch: 1,
        bulk_linger: Duration::from_secs(1),
        min_backoff: Duration::from_secs(2),
        max_backoff: Duration::from_secs(5),
    }
}

pub(super) fn advance(route: Route, drained: bool) -> (TargetId, Route, TargetOutcome) {
    (0, route, TargetOutcome::Advance { resume: 9, drained })
}

pub(super) fn refused(route: Route) -> (TargetId, Route, TargetOutcome) {
    (0, route, TargetOutcome::Refused)
}

/// The routes a round names, through the client that built it.
pub(super) fn named(client: &ReplayClient, page: &PageSession) -> BTreeSet<Route> {
    page.targets
        .iter()
        .map(|(id, _)| client.route_of(*id).unwrap().clone())
        .collect()
}

struct Rig {
    lanes: Lanes,
    client: ReplayClient,
    now: Instant,
    /// A route missing here has cursor 0; `None` is no cursor.
    cursors: BTreeMap<Route, Option<Rev>>,
}

impl Rig {
    fn new() -> Self {
        Self {
            lanes: Lanes::new(config()),
            client: ReplayClient::new(Uuid::from_u128(1)),
            now: Instant::now(),
            cursors: default(),
        }
    }

    /// The server acknowledges `route`, and the parent tells the lanes.
    fn ack(&mut self, route: Route) {
        self.client.want(route.clone());
        let update = self.client.next_update().unwrap().clone();
        let answer = Ok(SessionUpdated {
            generation: update.generation,
            refused: vec![],
        });
        self.client
            .update_answered(&update, &answer, &mut vec![])
            .unwrap();
        self.lanes.acked(route);
    }

    fn drop_route(&mut self, route: &Route) {
        self.client.unwant(route);
        self.lanes.dropped(route);
    }

    fn flush_with(&mut self, window: usize) -> Vec<LanesCommand> {
        let mut out = vec![];
        let cursors = &self.cursors;
        self.lanes.flush(
            self.now,
            &self.client,
            window,
            |route| cursors.get(route).copied().unwrap_or(Some(0)),
            &mut out,
        );
        out
    }

    fn flush(&mut self) -> Vec<LanesCommand> {
        self.flush_with(8)
    }

    fn wait(&mut self, secs: u64) {
        self.now += Duration::from_secs(secs);
    }

    /// The one round of `lane` in `out`, as (call, routes it names, page).
    fn round_of(
        &self,
        out: &[LanesCommand],
        lane: Lane,
    ) -> Option<(CallId, BTreeSet<Route>, PageSession)> {
        out.iter().find_map(|cmd| {
            let LanesCommand::Round {
                call,
                lane: have,
                page,
            } = cmd;
            (*have == lane).then(|| (*call, named(&self.client, page), page.clone()))
        })
    }

    fn routes_of(&self, out: &[LanesCommand], lane: Lane) -> BTreeSet<Route> {
        self.round_of(out, lane)
            .map(|(_, routes, _)| routes)
            .unwrap_or_default()
    }

    fn call_of(&self, out: &[LanesCommand], lane: Lane) -> CallId {
        self.round_of(out, lane).expect("a round").0
    }
}

fn set<const N: usize>(routes: [Route; N]) -> BTreeSet<Route> {
    routes.into_iter().collect()
}

#[test]
fn law_1_an_object_route_is_live() {
    let mut rig = Rig::new();
    rig.ack(obj_route(1));
    assert_eq!(rig.lanes.lane_of(&obj_route(1)), Some(Lane::Live));
    // and stays there whatever an answer says
    let out = rig.flush();
    let call = rig.call_of(&out, Lane::Live);
    rig.lanes.answered(call, &[advance(obj_route(1), false)]);
    assert_eq!(rig.lanes.lane_of(&obj_route(1)), Some(Lane::Live));
}

#[test]
fn law_1_a_part_route_enters_bulk() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
}

#[test]
fn law_1_a_drained_answer_moves_a_part_route_to_live() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    let call = rig.call_of(&out, Lane::Bulk);
    rig.lanes.answered(call, &[advance(part_route(1), true)]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Live));
}

#[test]
fn law_1_an_undrained_advance_moves_a_part_route_back_to_bulk() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    let out = rig.flush();
    assert_eq!(rig.routes_of(&out, Lane::Live), set([part_route(1)]));
    // it fell behind again
    rig.lanes.answered(
        rig.call_of(&out, Lane::Live),
        &[advance(part_route(1), false)],
    );
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
}

#[test]
fn law_1_an_answer_moves_only_the_routes_it_names() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(part_route(2));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Live));
    assert_eq!(rig.lanes.lane_of(&part_route(2)), Some(Lane::Bulk));
}

#[test]
fn law_1_acknowledging_again_keeps_the_lane_and_dropping_forgets_it() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    rig.lanes.acked(part_route(1));
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Live));
    rig.drop_route(&part_route(1));
    assert_eq!(rig.lanes.lane_of(&part_route(1)), None);
    // a route that comes back is a new acknowledgement: it enters Bulk
    rig.ack(part_route(1));
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
}

#[test]
fn law_1_a_refused_route_is_no_longer_acknowledged() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes
        .answered(rig.call_of(&out, Lane::Bulk), &[refused(part_route(1))]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), None);
}

#[test]
fn law_2_a_round_names_only_its_lanes_routes() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(part_route(2));
    rig.ack(obj_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(2), true)],
    );
    rig.lanes.answered(
        rig.call_of(&out, Lane::Live),
        &[advance(obj_route(1), true)],
    );
    rig.ack(part_route(3));
    let out = rig.flush();
    assert_eq!(
        rig.routes_of(&out, Lane::Live),
        set([obj_route(1), part_route(2)])
    );
    assert_eq!(
        rig.routes_of(&out, Lane::Bulk),
        set([part_route(1), part_route(3)])
    );
}

#[test]
fn law_2_a_round_names_every_route_with_a_cursor_and_no_other() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(part_route(2));
    rig.ack(part_route(3));
    rig.cursors.insert(part_route(2), None);
    rig.cursors.insert(part_route(3), Some(7));
    let out = rig.flush();
    let (_, routes, page) = rig.round_of(&out, Lane::Bulk).unwrap();
    assert_eq!(routes, set([part_route(1), part_route(3)]));
    let cursor_of_3 = page
        .targets
        .iter()
        .find(|(id, _)| rig.client.route_of(*id) == Some(&part_route(3)))
        .map(|(_, cursor)| *cursor);
    assert_eq!(cursor_of_3, Some(7));
}

#[test]
fn law_2_a_lane_whose_routes_have_no_cursor_sends_no_round() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.cursors.insert(part_route(1), None);
    assert!(rig.flush().is_empty());
}

#[test]
fn law_2_a_route_that_moved_waits_out_the_other_lanes_round() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    // the Live round names the route, then the parent drops it and the server acknowledges
    // it again: it is a Bulk route now, while Live still carries it
    let out = rig.flush();
    let live = rig.call_of(&out, Lane::Live);
    rig.drop_route(&part_route(1));
    rig.ack(part_route(1));
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
    assert!(rig.flush().is_empty());
    // the Live round's answer frees it
    rig.lanes.answered(live, &[advance(part_route(1), false)]);
    let out = rig.flush();
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(1)]));
}

#[test]
fn law_2_a_failed_round_frees_its_routes_for_the_other_lane() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    let out = rig.flush();
    let live = rig.call_of(&out, Lane::Live);
    rig.drop_route(&part_route(1));
    rig.ack(part_route(1));
    assert!(rig.flush().is_empty());
    rig.lanes.failed(live);
    let out = rig.flush();
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(1)]));
}

#[test]
fn law_3_a_lane_has_one_round_in_flight() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    assert_eq!(rig.flush().len(), 2);
    assert!(rig.flush().is_empty());
    rig.ack(part_route(2));
    rig.ack(obj_route(2));
    assert!(rig.flush().is_empty());
}

#[test]
fn law_3_an_answer_frees_the_lane_for_the_next_round() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), false)],
    );
    let out = rig.flush();
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(1)]));
}

#[test]
fn law_3_a_lost_round_frees_the_lane_once_its_backoff_runs_out() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.failed(rig.call_of(&out, Lane::Bulk));
    assert!(rig.flush().is_empty());
    rig.wait(2);
    assert_eq!(rig.flush().len(), 1);
}

#[test]
fn law_3_the_lanes_fly_independently() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), false)],
    );
    // Live is still in flight; Bulk goes again
    let out = rig.flush();
    assert_eq!(out.len(), 1);
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(1)]));
}

#[test]
fn law_3_an_answer_for_a_call_not_in_flight_changes_nothing() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    let call = rig.call_of(&out, Lane::Bulk);
    rig.lanes
        .answered(call + 100, &[advance(part_route(1), true)]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
    assert!(rig.flush().is_empty());
    rig.lanes.failed(call);
    // answered after it was failed: stale
    rig.lanes.answered(call, &[advance(part_route(1), true)]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
}

#[test]
fn law_4_a_live_round_may_hold() {
    let mut rig = Rig::new();
    rig.ack(obj_route(1));
    let out = rig.flush();
    assert_eq!(rig.round_of(&out, Lane::Live).unwrap().2.hold_ms, 300);
}

#[test]
fn law_4_a_bulk_round_never_holds() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    assert_eq!(rig.round_of(&out, Lane::Bulk).unwrap().2.hold_ms, 0);
}

#[test]
fn law_5_a_route_that_changes_lane_stays_in_the_round_in_flight() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    let call = rig.call_of(&out, Lane::Bulk);
    // gone and back while the Bulk round flies; the round still names it
    rig.drop_route(&part_route(1));
    rig.ack(part_route(1));
    assert_eq!(
        rig.lanes.flight(Lane::Bulk).unwrap().1,
        &set([part_route(1)])
    );
    rig.lanes.answered(call, &[advance(part_route(1), true)]);
    // the move took effect with the answer, and the next round is Live's
    let out = rig.flush();
    assert_eq!(rig.routes_of(&out, Lane::Live), set([part_route(1)]));
}

#[test]
fn law_5_the_answer_of_a_moved_route_still_applies() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    let out = rig.flush();
    let live = rig.call_of(&out, Lane::Live);
    rig.drop_route(&part_route(1));
    rig.ack(part_route(1));
    // Bulk by re-acknowledgement; the Live round's answer says drained
    rig.lanes.answered(live, &[advance(part_route(1), true)]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Live));
}

#[test]
fn law_5_an_answer_for_a_dropped_route_does_not_bring_it_back() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    let call = rig.call_of(&out, Lane::Bulk);
    rig.drop_route(&part_route(1));
    rig.lanes.answered(call, &[advance(part_route(1), true)]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), None);
    assert!(rig.flush().is_empty());
}

#[test]
fn law_5_a_move_takes_effect_at_the_next_round_not_inside_the_one_in_flight() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(part_route(2));
    let out = rig.flush();
    // Bulk names both; 1 drains, so the next Bulk round names only 2
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true), advance(part_route(2), false)],
    );
    let out = rig.flush();
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(2)]));
    assert_eq!(rig.routes_of(&out, Lane::Live), set([part_route(1)]));
}

#[test]
fn law_6_no_round_goes_out_without_a_window() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    assert!(rig.flush_with(0).is_empty());
    // nothing was consumed: the same routes go out once there is room for both
    assert_eq!(rig.flush_with(3).len(), 2);
}

#[test]
fn law_6_live_takes_its_share_and_bulk_the_rest() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let out = rig.flush_with(5);
    let limit = |lane| rig.round_of(&out, lane).unwrap().2.limit;
    assert_eq!((limit(Lane::Live), limit(Lane::Bulk)), (2, 3));
}

#[test]
fn law_6_a_window_smaller_than_the_share_goes_to_live() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let out = rig.flush_with(1);
    assert_eq!(rig.round_of(&out, Lane::Live).unwrap().2.limit, 1);
    assert!(
        rig.round_of(&out, Lane::Bulk).is_none(),
        "nothing is left for Bulk"
    );
}

/// A held `Live` round keeps its rows against the window: the next `Bulk` round gets only
/// what is left, and none while the window is used up.
#[test]
fn law_6_rounds_in_flight_hold_their_limits_against_the_window() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let out = rig.flush_with(4);
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), false)],
    );
    let out = rig.flush_with(3);
    assert_eq!(
        rig.round_of(&out, Lane::Bulk).unwrap().2.limit,
        1,
        "the Live round in flight still holds 2"
    );
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), false)],
    );
    assert!(rig.flush_with(2).is_empty(), "the window is used up");
}

#[test]
fn law_7_a_stale_round_frees_its_lane_without_backoff() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.stale(rig.call_of(&out, Lane::Bulk));
    let out = rig.flush();
    assert!(
        rig.round_of(&out, Lane::Bulk).is_some(),
        "the repeat goes out at once"
    );
    assert_eq!(rig.lanes.next_due(), None, "no deadline was armed");
}

#[test]
fn law_7_a_stale_round_keeps_the_backoff_it_had() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.flush();
    let armed = fail_and_arm(&mut rig, Lane::Bulk);
    rig.now = armed;
    let out = rig.flush();
    rig.lanes.stale(rig.call_of(&out, Lane::Bulk));
    let out = rig.flush();
    rig.lanes.failed(rig.call_of(&out, Lane::Bulk));
    rig.flush();
    assert_eq!(
        rig.lanes.next_due(),
        Some(rig.now + Duration::from_secs(4)),
        "the ladder climbs from where it was, not from the floor"
    );
}

#[test]
fn law_6_live_is_served_first() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let lanes: Vec<_> = rig
        .flush()
        .iter()
        .map(|cmd| {
            let LanesCommand::Round { lane, .. } = cmd;
            *lane
        })
        .collect();
    assert_eq!(lanes, vec![Lane::Live, Lane::Bulk]);
}

#[test]
fn law_6_a_backed_off_live_lane_does_not_hold_bulk_back() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let out = rig.flush();
    rig.lanes.failed(rig.call_of(&out, Lane::Live));
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), false)],
    );
    let out = rig.flush();
    assert_eq!(out.len(), 1);
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(1)]));
}

/// Fail the round of `lane` in flight, let a second pass, and flush: that flush reads the
/// clock and arms the deadline, which it returns.
fn fail_and_arm(rig: &mut Rig, lane: Lane) -> Instant {
    let call = rig.lanes.flight(lane).unwrap().0;
    rig.lanes.failed(call);
    rig.wait(1);
    rig.flush();
    rig.lanes.next_due().expect("a deadline")
}

#[test]
fn law_7_a_failure_backs_off_from_the_floor_doubling_to_the_cap() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    assert_eq!(rig.flush().len(), 1);
    for secs in [2, 4, 5, 5] {
        let armed_at = rig.now + Duration::from_secs(1);
        let due = fail_and_arm(&mut rig, Lane::Bulk);
        // the deadline counts from the flush that read the clock, not from the failure
        assert_eq!(due, armed_at + Duration::from_secs(secs));
        rig.now = due - Duration::from_millis(1);
        assert!(rig.flush().is_empty());
        rig.now = due;
        assert_eq!(rig.flush().len(), 1);
    }
}

#[test]
fn law_7_an_answer_resets_the_ladder() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.flush();
    for _ in 0..2 {
        let due = fail_and_arm(&mut rig, Lane::Bulk);
        rig.now = due;
        assert_eq!(rig.flush().len(), 1);
    }
    let call = rig.lanes.flight(Lane::Bulk).unwrap().0;
    rig.lanes.answered(call, &[advance(part_route(1), false)]);
    // answered: the lane is open at once, and the next failure waits the floor again
    assert_eq!(rig.flush().len(), 1);
    let armed_at = rig.now + Duration::from_secs(1);
    let due = fail_and_arm(&mut rig, Lane::Bulk);
    assert_eq!(due, armed_at + Duration::from_secs(2));
}

#[test]
fn law_7_a_failure_before_the_clock_is_read_blocks_the_lane() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.failed(rig.call_of(&out, Lane::Bulk));
    // no deadline yet; the flush that reads the clock also keeps the round back
    assert_eq!(rig.lanes.next_due(), None);
    assert!(rig.flush().is_empty());
    assert!(rig.lanes.next_due().is_some());
}

#[test]
fn law_7_a_failure_delays_only_its_own_lane() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    let out = rig.flush();
    rig.lanes.failed(rig.call_of(&out, Lane::Live));
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), false)],
    );
    // Bulk goes at once; Live waits
    let out = rig.flush();
    assert_eq!(out.len(), 1);
    assert_eq!(rig.routes_of(&out, Lane::Bulk), set([part_route(1)]));
}

#[test]
fn law_7_a_failure_leaves_the_other_lanes_ladder_alone() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    rig.flush();
    // Bulk fails twice, so its next wait is 4s
    for _ in 0..2 {
        let due = fail_and_arm(&mut rig, Lane::Bulk);
        rig.now = due;
        rig.flush();
    }
    // Live's first failure waits the floor, whatever Bulk has climbed to
    let call = rig.lanes.flight(Lane::Live).unwrap().0;
    rig.lanes.failed(call);
    let armed_at = rig.now;
    rig.flush();
    assert_eq!(
        rig.lanes.next_due(),
        Some(armed_at + Duration::from_secs(2))
    );
}

#[test]
fn law_8_a_reset_clears_both_lanes() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.ack(obj_route(1));
    rig.flush();
    rig.lanes.reset();
    assert_eq!(rig.lanes.lane_of(&part_route(1)), None);
    assert_eq!(rig.lanes.lane_of(&obj_route(1)), None);
    assert_eq!(rig.lanes.flight(Lane::Live), None);
    assert_eq!(rig.lanes.flight(Lane::Bulk), None);
}

#[test]
fn law_8_a_reset_lane_does_not_remember_the_old_sessions_failures() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    rig.flush();
    let _due: Instant = fail_and_arm(&mut rig, Lane::Bulk);
    rig.lanes.reset();
    rig.lanes.acked(part_route(1));
    assert_eq!(rig.flush().len(), 1);
}

#[test]
fn law_8_routes_re_enter_by_law_1_once_acknowledged_again() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    rig.lanes.answered(
        rig.call_of(&out, Lane::Bulk),
        &[advance(part_route(1), true)],
    );
    rig.lanes.reset();
    assert!(rig.flush().is_empty());
    rig.lanes.acked(part_route(1));
    rig.lanes.acked(obj_route(1));
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
    assert_eq!(rig.lanes.lane_of(&obj_route(1)), Some(Lane::Live));
}

#[test]
fn law_8_an_answer_to_a_round_of_the_old_session_is_ignored() {
    let mut rig = Rig::new();
    rig.ack(part_route(1));
    let out = rig.flush();
    let old = rig.call_of(&out, Lane::Bulk);
    rig.lanes.reset();
    rig.lanes.acked(part_route(1));
    let out = rig.flush();
    let new = rig.call_of(&out, Lane::Bulk);
    assert_ne!(old, new);
    rig.lanes.answered(old, &[advance(part_route(1), true)]);
    assert_eq!(rig.lanes.lane_of(&part_route(1)), Some(Lane::Bulk));
    assert!(
        rig.lanes
            .flight(Lane::Bulk)
            .is_some_and(|(call, _)| call == new)
    );
}

fn batching() -> Rig {
    let mut rig = Rig::new();
    rig.lanes = Lanes::new(Config {
        bulk_batch: 3,
        ..config()
    });
    rig
}

#[test]
fn law_9_bulk_waits_for_its_batch_then_lingers_out() {
    let mut rig = batching();
    rig.ack(part_route(1));
    assert!(
        rig.flush_with(2).is_empty(),
        "2 rows are below the batch of 3"
    );
    assert_eq!(
        rig.lanes.next_due(),
        Some(rig.now + Duration::from_secs(1)),
        "the linger is due"
    );
    rig.wait(1);
    let out = rig.flush_with(2);
    assert_eq!(rig.round_of(&out, Lane::Bulk).unwrap().2.limit, 2);
}

#[test]
fn law_9_bulk_goes_at_once_with_its_batch() {
    let mut rig = batching();
    rig.ack(part_route(1));
    assert!(rig.flush_with(1).is_empty());
    let out = rig.flush_with(3);
    assert_eq!(
        rig.round_of(&out, Lane::Bulk).unwrap().2.limit,
        3,
        "the window grew to the batch before the linger ran out"
    );
    assert_eq!(rig.lanes.next_due(), None, "nothing lingers once it went");
}

#[test]
fn law_9_live_never_waits_for_a_batch() {
    let mut rig = batching();
    rig.ack(obj_route(1));
    let out = rig.flush_with(1);
    assert_eq!(rig.round_of(&out, Lane::Live).unwrap().2.limit, 1);
}

#[test]
fn law_9_a_lane_with_nothing_to_send_does_not_linger() {
    let mut rig = batching();
    rig.ack(obj_route(1));
    rig.flush_with(1);
    assert_eq!(
        rig.lanes.next_due(),
        None,
        "Bulk has no routes, so no linger is due"
    );
}
