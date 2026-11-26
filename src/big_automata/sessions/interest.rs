//! The Interest machine: which sessions a wanted object is routed to.
//!
//! One machine holds every object somebody holds a lease on, and decides, from what the caller
//! says about *standing*, on which sessions that object is routed and which sessions must exist
//! for that. It knows nothing about documents, blobs, keyhive or Keyline: the object key `K` and
//! the session key `S` are type parameters, and which sessions may serve an object is an input
//! the caller computes ([`Interest::standing`]). It is sans-IO like every machine in this crate:
//! inputs are methods, the clock is a parameter, and everything wanted leaves as a [`Command`]
//! from [`Interest::flush`].
//!
//! # The states of an object
//!
//! - `Wanted`: somebody holds a lease and no route is installed yet (no eligible session is
//!   open).
//! - `Routed`: somebody holds a lease and at least one route is installed.
//! - `Idle { until }`: the last lease was dropped. The routes stay, so a lease taken again
//!   inside the grace keeps its subscriptions instead of tearing them down and adding them
//!   again, but nothing new is asked for. At `until` the object is `Released`: its routes are
//!   removed and the object is forgotten.
//!
//! # Which sessions an object is routed to
//!
//! The caller gives, per object, the sessions that may serve it, each with a [`Tier`]. A
//! [`Tier::Default`] session is one that exists anyway (our own principal toward a connected
//! peer); a [`Tier::OnDemand`] session is one that is opened only because an object needs it (a
//! shared principal). The machine's policy is:
//!
//! - the *desired* sessions of an object are its default eligible sessions, or, only when it has
//!   none, its on-demand eligible sessions;
//! - the object is routed on its desired sessions that are open, and on no other session, so it
//!   is never routed to a peer without standing, nor to every peer;
//! - while the object is leased, every desired session is *needed* ([`Command::NeedSession`]);
//!   a session nobody needs is released ([`Command::ReleaseSession`]) when the last object
//!   stops needing it. An idle object needs only the sessions it still holds routes on.
//!
//! # Inputs and commands
//!
//! Inputs: [`Interest::lease_acquired`], [`Interest::lease_dropped`], [`Interest::standing`]
//! (the full replacement of an object's eligible sessions, empty when standing is revoked; it
//! is ignored for an object nobody holds, so the caller computes it after the lease),
//! [`Interest::session_opened`] and [`Interest::session_closed`] (a session that is lost loses
//! its routes with it, silently; if it is still needed the session machine dials it again).
//!
//! Commands: [`Command::AddRoute`] and [`Command::RemoveRoute`] for an object on an open
//! session, [`Command::NeedSession`] when a session goes from unneeded to needed, and
//! [`Command::ReleaseSession`] when it goes back.
//!
//! # Laws
//!
//! After every flush:
//!
//! 1. **Standing bounds routes.** An object is routed only on sessions the caller named
//!    eligible, and only on open ones. A revoke removes the routes at the next flush.
//! 2. **Default before on-demand.** An object with an eligible default session has no on-demand
//!    route and needs no on-demand session.
//! 3. **Routed promptly.** A leased object is routed on every desired session that is open.
//! 4. **Idle adds nothing.** An idle object's routes only shrink.
//! 5. **Grace.** An object is released at the first flush at or after its deadline, and not
//!    before: a lease taken again before then cancels it.
//! 6. **Need is exact.** A session is needed exactly when some object's needs include it, and
//!    each edge is reported once.
//! 7. **Quiet when idle.** With no object left the machine holds nothing and asks for nothing.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;

use web_time::Instant;

use crate::interlude::*;
#[cfg(test)]
use super::Plant;

/// How a session that may serve an object comes into being.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    /// It exists anyway: our own principal toward a connected peer.
    Default,
    /// It is opened only because an object needs it, and closed when none does.
    OnDemand,
}

/// What the machine wants done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<K, S> {
    AddRoute { object: K, session: S },
    RemoveRoute { object: K, session: S },
    NeedSession { session: S },
    ReleaseSession { session: S },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Wanted,
    Routed,
    Idle { until: Instant },
}

#[derive(Debug)]
struct Object<S> {
    leases: usize,
    phase: Phase,
    eligible: BTreeMap<S, Tier>,
    routed: BTreeSet<S>,
    /// The sessions this object currently counts towards in `Interest::demand`.
    needs: BTreeSet<S>,
}

#[derive(Debug)]
pub struct Interest<K, S> {
    grace: Duration,
    objects: BTreeMap<K, Object<S>>,
    open: BTreeSet<S>,
    /// How many objects need each session.
    demand: BTreeMap<S, usize>,
    /// The objects each session is eligible for, so opening it finds them without a scan.
    eligible_for: BTreeMap<S, BTreeSet<K>>,
    /// Idle objects by deadline.
    deadlines: BTreeSet<(Instant, K)>,
    dirty: BTreeSet<K>,
    #[cfg(test)]
    plant: Option<Plant>,
}

impl<K, S> Interest<K, S>
where
    K: Ord + Clone + Debug,
    S: Ord + Clone + Debug,
{
    pub fn new(grace: Duration) -> Self {
        Self {
            grace,
            objects: default(),
            open: default(),
            demand: default(),
            eligible_for: default(),
            deadlines: default(),
            dirty: default(),
            #[cfg(test)]
            plant: None,
        }
    }

    #[cfg(test)]
    pub(super) fn plant(&mut self, plant: Plant) {
        self.plant = Some(plant);
    }

    pub fn lease_acquired(&mut self, object: K) {
        let obj = self.objects.entry(object.clone()).or_insert_with(|| Object {
            leases: 0,
            phase: Phase::Wanted,
            eligible: default(),
            routed: default(),
            needs: default(),
        });
        if let Phase::Idle { until } = obj.phase {
            self.deadlines.remove(&(until, object.clone()));
            obj.phase = Phase::Wanted;
        }
        obj.leases += 1;
        self.dirty.insert(object);
    }

    /// A lease was dropped. The last one starts the grace.
    pub fn lease_dropped(&mut self, object: &K, now: Instant) {
        let obj = self
            .objects
            .get_mut(object)
            .expect("a lease was dropped that was never acquired");
        obj.leases = obj.leases.checked_sub(1).expect(ERROR_IMPOSSIBLE);
        if obj.leases == 0 {
            let until = now + self.grace;
            obj.phase = Phase::Idle { until };
            self.deadlines.insert((until, object.clone()));
            self.dirty.insert(object.clone());
        }
    }

    /// Replace the sessions that may serve `object`.
    pub fn standing(&mut self, object: &K, eligible: BTreeMap<S, Tier>) {
        let Some(obj) = self.objects.get_mut(object) else {
            return;
        };
        for session in obj.eligible.keys().filter(|s| !eligible.contains_key(*s)) {
            if let Some(set) = self.eligible_for.get_mut(session) {
                set.remove(object);
                if set.is_empty() {
                    self.eligible_for.remove(session);
                }
            }
        }
        for session in eligible.keys() {
            self.eligible_for
                .entry(session.clone())
                .or_default()
                .insert(object.clone());
        }
        #[cfg(test)]
        let eligible = if self.plant == Some(Plant::IgnoreRevoke) {
            // planted: a revoke is never applied, only grants are
            obj.eligible.iter().map(|(s, t)| (s.clone(), *t)).chain(eligible).collect()
        } else {
            eligible
        };
        obj.eligible = eligible;
        self.dirty.insert(object.clone());
    }

    pub fn session_opened(&mut self, session: S) {
        if let Some(objects) = self.eligible_for.get(&session) {
            self.dirty.extend(objects.iter().cloned());
        }
        self.open.insert(session);
    }

    /// The session closed or was lost: its routes are gone with it.
    pub fn session_closed(&mut self, session: &S) {
        self.open.remove(session);
        // A scan, not an index: a session is lost rarely, and an index of routes by session
        // would be one more table to keep equal to `routed`.
        for (key, obj) in &mut self.objects {
            if obj.routed.remove(session) {
                self.dirty.insert(key.clone());
            }
        }
    }

    pub fn next_due(&self) -> Option<Instant> {
        self.deadlines.first().map(|(until, _)| *until)
    }

    pub fn flush(&mut self, now: Instant, out: &mut Vec<Command<K, S>>) {
        while let Some((until, object)) = self.deadlines.first().cloned() {
            if until > now {
                break;
            }
            self.deadlines.pop_first();
            self.release(&object, out);
        }
        let mut edges = vec![];
        for object in std::mem::take(&mut self.dirty) {
            if self.objects.contains_key(&object) {
                self.reconcile(&object, out, &mut edges);
            }
        }
        out.extend(edges);
        #[cfg(test)]
        if self.plant.is_none() {
            self.check();
        }
    }

    pub fn phase(&self, object: &K) -> Option<Phase> {
        self.objects.get(object).map(|obj| obj.phase)
    }

    pub fn routes(&self, object: &K) -> Option<&BTreeSet<S>> {
        self.objects.get(object).map(|obj| &obj.routed)
    }

    /// Whether any object needs `session`.
    pub fn needed(&self, session: &S) -> bool {
        self.demand.contains_key(session)
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.demand.is_empty() && self.eligible_for.is_empty()
    }

    fn release(&mut self, object: &K, out: &mut Vec<Command<K, S>>) {
        let Some(mut obj) = self.objects.remove(object) else {
            return;
        };
        self.dirty.remove(object);
        for session in obj.eligible.keys() {
            if let Some(set) = self.eligible_for.get_mut(session) {
                set.remove(object);
                if set.is_empty() {
                    self.eligible_for.remove(session);
                }
            }
        }
        for session in std::mem::take(&mut obj.routed) {
            out.push(Command::RemoveRoute {
                object: object.clone(),
                session,
            });
        }
        // The releases go after the route removals: the commands of one flush are in order.
        let mut edges = vec![];
        Self::move_needs(&mut self.demand, &mut obj.needs, default(), &mut edges);
        out.extend(edges);
    }

    fn reconcile(
        &mut self,
        object: &K,
        out: &mut Vec<Command<K, S>>,
        edges: &mut Vec<Command<K, S>>,
    ) {
        let obj = self.objects.get_mut(object).expect(ERROR_IMPOSSIBLE);
        #[allow(unused_mut)]
        let mut has_default = obj.eligible.values().any(|tier| *tier == Tier::Default);
        #[cfg(test)]
        if self.plant == Some(Plant::EagerShared) {
            // planted: on-demand sessions are asked for even when a default one can serve
            has_default = false;
        }
        #[allow(unused_mut)]
        let mut desired: BTreeSet<S> = obj
            .eligible
            .iter()
            .filter(|(_, tier)| !has_default || **tier == Tier::Default)
            .map(|(session, _)| session.clone())
            .collect();
        #[cfg(test)]
        if self.plant == Some(Plant::RouteEverywhere) {
            // planted: every open session, whoever stands where
            desired = self.open.clone();
        }
        let removed: Vec<S> = obj.routed.difference(&desired).cloned().collect();
        for session in removed {
            obj.routed.remove(&session);
            out.push(Command::RemoveRoute {
                object: object.clone(),
                session,
            });
        }
        let idle = matches!(obj.phase, Phase::Idle { .. });
        if !idle {
            for session in desired.iter().filter(|s| self.open.contains(*s)) {
                if obj.routed.insert(session.clone()) {
                    out.push(Command::AddRoute {
                        object: object.clone(),
                        session: session.clone(),
                    });
                }
            }
            obj.phase = if obj.routed.is_empty() {
                Phase::Wanted
            } else {
                Phase::Routed
            };
        }
        let needs = if idle { obj.routed.clone() } else { desired };
        Self::move_needs(&mut self.demand, &mut obj.needs, needs, edges);
    }

    /// Make `held` equal `needs`, counting each change in `demand` and reporting the edges.
    fn move_needs(
        demand: &mut BTreeMap<S, usize>,
        held: &mut BTreeSet<S>,
        needs: BTreeSet<S>,
        edges: &mut Vec<Command<K, S>>,
    ) {
        for session in held.difference(&needs) {
            let count = demand.get_mut(session).expect(ERROR_IMPOSSIBLE);
            *count -= 1;
            if *count == 0 {
                demand.remove(session);
                edges.push(Command::ReleaseSession {
                    session: session.clone(),
                });
            }
        }
        for session in needs.difference(held) {
            let count = demand.entry(session.clone()).or_default();
            *count += 1;
            if *count == 1 {
                edges.push(Command::NeedSession {
                    session: session.clone(),
                });
            }
        }
        *held = needs;
    }

    /// The machine's own invariants, asserted at the end of every flush in test builds.
    #[cfg(test)]
    fn check(&self) {
        let mut demand: BTreeMap<&S, usize> = BTreeMap::new();
        for obj in self.objects.values() {
            for session in &obj.needs {
                *demand.entry(session).or_default() += 1;
            }
            assert!(obj.routed.is_subset(&obj.needs), "a route without a need");
            for session in &obj.routed {
                assert!(self.open.contains(session), "a route on a closed session");
                assert!(obj.eligible.contains_key(session), "a route without standing");
            }
        }
        assert_eq!(
            demand,
            self.demand.iter().map(|(s, n)| (s, *n)).collect::<BTreeMap<_, _>>()
        );
        assert_eq!(
            self.deadlines.len(),
            self.objects
                .values()
                .filter(|obj| matches!(obj.phase, Phase::Idle { .. }))
                .count()
        );
    }
}
