//! The law tests of both machines, driven by hand, and an instantiation shaped like
//! `big_blobs` (blob hashes as objects). The simulation in [`super::sim`] drives both together.

use std::collections::BTreeMap;

use web_time::Instant;

use super::interest::Command as IC;
use super::interest::Interest;
use super::interest::Phase as IPhase;
use super::interest::Tier;
use super::session::Command as SC;
use super::session::Config;
use super::session::Phase;
use super::session::Sessions;
use crate::interlude::*;

const GRACE: Duration = Duration::from_secs(30);

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn eligible(items: &[((u8, u8), Tier)]) -> BTreeMap<(u8, u8), Tier> {
    items.iter().cloned().collect()
}

fn iflush(i: &mut Interest<u8, (u8, u8)>, now: Instant) -> Vec<IC<u8, (u8, u8)>> {
    let mut out = vec![];
    i.flush(now, &mut out);
    out
}

fn sflush(s: &mut Sessions<u8, u8>, now: Instant) -> Vec<SC<u8, u8>> {
    let mut out = vec![];
    s.flush(now, &mut out);
    out
}

fn cfg() -> Config {
    Config {
        grace: secs(20),
        per_peer: 2,
        total: 3,
        backoff: secs(1),
        max_backoff: secs(8),
    }
}

const OWN: ((u8, u8), Tier) = ((1, 0), Tier::Default);
const SHARED: ((u8, u8), Tier) = ((1, 7), Tier::OnDemand);

#[test]
fn law_1_routes_only_on_open_eligible_sessions() {
    let now = Instant::now();
    let mut i = Interest::new(GRACE);
    i.lease_acquired(1u8);
    i.standing(&1, eligible(&[OWN]));
    i.session_opened((2, 0));
    i.session_opened((1, 0));
    let out = iflush(&mut i, now);
    assert!(out.contains(&IC::AddRoute { object: 1, session: (1, 0) }));
    assert!(!out.iter().any(|c| matches!(c, IC::AddRoute { session: (2, 0), .. })));
    // a revoke removes the route and releases the session at the next flush
    i.standing(&1, eligible(&[]));
    let out = iflush(&mut i, now);
    assert_eq!(
        out,
        vec![
            IC::RemoveRoute { object: 1, session: (1, 0) },
            IC::ReleaseSession { session: (1, 0) },
        ]
    );
    assert_eq!(i.phase(&1), Some(IPhase::Wanted));
}

#[test]
fn law_2_default_before_on_demand() {
    let now = Instant::now();
    let mut i = Interest::new(GRACE);
    i.lease_acquired(1u8);
    i.session_opened((1, 7));
    i.standing(&1, eligible(&[SHARED]));
    let out = iflush(&mut i, now);
    assert!(out.contains(&IC::NeedSession { session: (1, 7) }));
    assert!(out.contains(&IC::AddRoute { object: 1, session: (1, 7) }));
    // a default session becomes eligible: the shared route and need go
    i.standing(&1, eligible(&[OWN, SHARED]));
    let out = iflush(&mut i, now);
    assert!(out.contains(&IC::RemoveRoute { object: 1, session: (1, 7) }));
    assert!(out.contains(&IC::ReleaseSession { session: (1, 7) }));
    assert!(out.contains(&IC::NeedSession { session: (1, 0) }));
    assert!(!i.needed(&(1, 7)));
}

#[test]
fn law_3_routed_within_one_flush_of_the_session_opening() {
    let now = Instant::now();
    let mut i = Interest::new(GRACE);
    i.lease_acquired(1u8);
    i.standing(&1, eligible(&[OWN]));
    assert_eq!(iflush(&mut i, now), vec![IC::NeedSession { session: (1, 0) }]);
    assert_eq!(i.phase(&1), Some(IPhase::Wanted));
    i.session_opened((1, 0));
    assert_eq!(
        iflush(&mut i, now),
        vec![IC::AddRoute { object: 1, session: (1, 0) }]
    );
    assert_eq!(i.phase(&1), Some(IPhase::Routed));
    // the session is lost: routes go silently, the need stays
    i.session_closed(&(1, 0));
    assert!(iflush(&mut i, now).is_empty());
    assert_eq!(i.phase(&1), Some(IPhase::Wanted));
    assert!(i.needed(&(1, 0)));
}

#[test]
fn law_4_and_5_idle_keeps_routes_then_releases_at_the_deadline() {
    let now = Instant::now();
    let mut i = Interest::new(GRACE);
    i.lease_acquired(1u8);
    i.standing(&1, eligible(&[OWN]));
    i.session_opened((1, 0));
    iflush(&mut i, now);
    i.lease_dropped(&1, now);
    assert!(iflush(&mut i, now).is_empty());
    assert_eq!(i.next_due(), Some(now + GRACE));
    // standing grows while idle: nothing is added
    i.standing(&1, eligible(&[OWN, ((2, 0), Tier::Default)]));
    i.session_opened((2, 0));
    assert!(iflush(&mut i, now + secs(29)).is_empty());
    assert_eq!(i.routes(&1).unwrap().len(), 1);
    // a lease inside the grace cancels the release and keeps the routes
    i.lease_acquired(1);
    let out = iflush(&mut i, now + secs(29));
    assert_eq!(out, vec![IC::AddRoute { object: 1, session: (2, 0) }, IC::NeedSession { session: (2, 0) }]);
    i.lease_dropped(&1, now + secs(29));
    let out = iflush(&mut i, now + secs(59));
    assert_eq!(out.len(), 4, "{out:?}");
    assert!(i.is_empty());
}

#[test]
fn law_6_need_edges_are_reported_once() {
    let now = Instant::now();
    let mut i = Interest::new(GRACE);
    for object in [1u8, 2] {
        i.lease_acquired(object);
        i.standing(&object, eligible(&[OWN]));
    }
    let out = iflush(&mut i, now);
    assert_eq!(out, vec![IC::NeedSession { session: (1, 0) }]);
    i.lease_dropped(&1, now);
    i.lease_dropped(&2, now);
    i.standing(&1, eligible(&[]));
    i.standing(&2, eligible(&[]));
    let out = iflush(&mut i, now + GRACE);
    assert_eq!(out, vec![IC::ReleaseSession { session: (1, 0) }]);
    assert!(i.is_empty());
}

#[test]
fn sessions_dial_back_off_and_redial_only_while_wanted() {
    let now = Instant::now();
    let mut s = Sessions::new(cfg());
    s.peer_connected(1u8, 0u8);
    let SC::Dial { call, .. } = sflush(&mut s, now)[0].clone() else {
        panic!("the default session is dialed");
    };
    s.dialed(call, false, now);
    assert!(sflush(&mut s, now).is_empty());
    assert_eq!(s.next_due(), Some(now + secs(1)));
    let out = sflush(&mut s, now + secs(1));
    let [SC::Dial { call, .. }] = out[..] else { panic!("{out:?}") };
    s.dialed(call, false, now + secs(1));
    assert_eq!(s.next_due(), Some(now + secs(3)), "the backoff doubles");
    // the peer leaves while the retry waits: no more dials
    s.peer_disconnected(&1);
    assert!(sflush(&mut s, now + secs(60)).is_empty());
    assert_eq!(s.sessions().count(), 0);
}

#[test]
fn sessions_default_is_never_idle_and_shared_closes_after_the_grace() {
    let now = Instant::now();
    let mut s = Sessions::new(cfg());
    s.peer_connected(1u8, 0u8);
    s.need(1, 7);
    let out = sflush(&mut s, now);
    assert_eq!(out.len(), 2);
    for command in out {
        let SC::Dial { call, .. } = command else { panic!() };
        s.dialed(call, true, now);
    }
    s.release(1, 0, now);
    s.release(1, 7, now);
    assert_eq!(s.phase(&1, &0), Some(Phase::Open));
    assert_eq!(s.phase(&1, &7), Some(Phase::Idle { until: now + secs(20) }));
    assert!(sflush(&mut s, now + secs(19)).is_empty());
    // a need inside the grace keeps it
    s.need(1, 7);
    assert!(sflush(&mut s, now + secs(25)).is_empty());
    s.release(1, 7, now + secs(25));
    assert_eq!(sflush(&mut s, now + secs(45)), vec![SC::Close { peer: 1, principal: 7 }]);
    assert_eq!(s.phase(&1, &7), None);
}

#[test]
fn sessions_caps_hold_and_defaults_come_first() {
    let now = Instant::now();
    let mut s = Sessions::new(cfg());
    for peer in 1u8..=3 {
        s.need(peer, 7);
        s.need(peer, 8);
    }
    for peer in 1u8..=3 {
        s.peer_connected(peer, 0);
    }
    let out = sflush(&mut s, now);
    let dials: Vec<_> = out.iter().map(|c| match c { SC::Dial { peer, principal, .. } => (*peer, *principal), _ => panic!() }).collect();
    assert_eq!(dials, vec![(1, 0), (2, 0), (3, 0)], "total is 3 and the defaults won it");
    for command in out {
        let SC::Dial { call, .. } = command else { panic!() };
        s.dialed(call, true, now);
    }
    assert_eq!(s.phase(&1, &7), Some(Phase::Wanted { admitted: false }));
    // releasing a session that never held a slot frees nothing
    s.release(1, 7, now);
    assert!(sflush(&mut s, now).is_empty());
}

#[test]
fn sessions_lost_connection_is_dialed_again_only_while_wanted() {
    let now = Instant::now();
    let mut s = Sessions::new(cfg());
    s.peer_connected(1u8, 0u8);
    s.need(1, 7);
    for command in sflush(&mut s, now) {
        let SC::Dial { call, .. } = command else { panic!() };
        s.dialed(call, true, now);
    }
    s.closed(1, 7);
    s.closed(1, 0);
    let out = sflush(&mut s, now);
    assert_eq!(out.len(), 2, "both are wanted and dialed afresh");
    for command in out {
        let SC::Dial { call, .. } = command else { panic!() };
        s.dialed(call, true, now);
    }
    s.release(1, 7, now);
    s.closed(1, 7);
    assert!(sflush(&mut s, now).is_empty());
    assert_eq!(s.phase(&1, &7), None);
}

/// The machines are generic: here an object is a blob hash and a peer and a principal are
/// public keys, which is the shape `big_blobs` has.
#[test]
fn instantiated_for_blob_hashes_and_public_keys() {
    use crate::ByteKey;
    use crate::PeerKey;

    let now = Instant::now();
    let hash = ByteKey::new(&[7u8; 32]);
    let peer = PeerKey::new([1u8; 32]);
    let node = PeerKey::new([2u8; 32]);
    let session = (peer.clone(), node.clone());
    let mut interest = Interest::new(GRACE);
    let mut sessions = Sessions::new(cfg());
    sessions.peer_connected(peer.clone(), node.clone());
    interest.lease_acquired(hash.clone());
    interest.standing(&hash, [(session.clone(), Tier::Default)].into());
    interest.session_opened(session.clone());
    let mut out = vec![];
    interest.flush(now, &mut out);
    assert_eq!(
        out,
        vec![
            IC::AddRoute { object: hash.clone(), session: session.clone() },
            IC::NeedSession { session: session.clone() },
        ]
    );
    sessions.need(peer.clone(), node.clone());
    let mut dials = vec![];
    sessions.flush(now, &mut dials);
    assert!(matches!(dials[..], [SC::Dial { .. }]));
}
