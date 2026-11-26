//! The laws every [`SyncBackend`] obeys, as a check an implementation's own tests call with a
//! rig for it (the memory backend here, `big_repo`, the blob stores and `big_blobs` in their
//! crates). They state the backend contract of
//! [`object_jobs`](crate::sync::object_jobs) as things observable on the backend's store.
//!
//! A job is `record`, then `fetch` when `record` answered [`Settle::Fetch`]; the job is done
//! when both succeeded. Every law below applies each job **twice in a row** (replays repeat
//! after a reconnect), and the second pass must leave the backend exactly as the first did: the
//! same observable state, and nothing stamped.
//!
//! 1. **Adoption.** A job for an object the backend has never seen leaves it with the job's
//!    payload and exactly the parts the job adds: one hinted part, several, or none (a content
//!    job).
//! 2. **An equal payload is a no-op, a changed one replaces.** Applying a job whose payload the
//!    backend already holds changes nothing; a different payload replaces it.
//! 3. **Memberships are exact.** An add keeps the memberships the object already had; a job's
//!    removals and additions together leave exactly `held - remove + add`.
//! 4. **A removal-only job keeps the payload and needs nothing from the peer.** It removes
//!    the memberships named and no others, it is idempotent, it does not fail for an object or
//!    a part the backend never had, and it succeeds when the peer no longer holds the object.
//! 5. **A failed call is all-or-nothing.** After a failed `record` the backend is as it was,
//!    memberships and payload alike; the job then succeeds on retry.
//! 6. **A fetch of content the peer no longer has errors and leaves local state untouched**
//!    (rigs whose `fetch` downloads, [`BackendRig::DOWNLOADS`]); the job succeeds once the
//!    peer has the content again.
//! 7. **Concurrent payloads converge.** Two backends that each wrote a different payload for
//!    one object and then take each other's payload, both ways at once, end holding equal
//!    payloads, and taking them again changes nothing on either. The peer's payload is merged
//!    (commutative and idempotent), never written over: a blind overwrite swaps the two
//!    payloads on every exchange and the nodes never settle.
//! 8. **A peer never changes membership.** A job's `add` and `remove` are a notification. Whatever
//!    they name, however stale, the object's parts are exactly what the backend's own source
//!    set (the rig's `member_added` and `member_removed`); a stale echo of an old add cannot
//!    resurrect a removed member. Laws 1 to 7 deliver a job's membership through the source
//!    first.
//! 9. **A notification before the membership is not lost.** A job for an object that is not
//!    yet a member of its parts changes no membership (a downloading backend has not even
//!    fetched it); the source's addition then leaves the object fetched, and an object nobody
//!    advertised is not.
//! 10. **A member that was away catches up.** A removed member whose peer advertised a newer
//!     payload meanwhile ends with it once the source re-adds it.
//! 11. **Release is by part.** A membership added to a part the notification did not name
//!     fetches nothing; the addition to a named part does.
//!
//! A rig supplies a fresh backend per law and the handful of operations the laws need that the
//! trait cannot express: what the peer holds, what the backend holds, and how to make its next
//! write fail.

use std::collections::BTreeSet;

use super::ObjJob;
use super::Settle;
use super::SyncBackend;
use crate::interlude::*;
use crate::part_store::ObjPayload;
use crate::part_store::ObjState;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;

/// What a backend under test is, for [`sync_backend_laws`].
pub trait BackendRig {
    type Backend: SyncBackend;
    /// A reading of the backend's change counter: equal exactly when nothing was committed
    /// between two readings (the memory store's latest rev, a SQLite `data_version`).
    type Stamp: PartialEq + std::fmt::Debug;

    /// Whether `fetch` downloads the payload from the peer, so that it fails when the peer
    /// no longer holds it. A backend that takes the payload from the job has no such failure
    /// and law 6 does not apply.
    const DOWNLOADS: bool;

    /// An empty backend whose peer holds nothing.
    fn fresh(&self) -> Self::Backend;

    /// From now on the peer holds `payload` for `obj`, or nothing when `None`.
    fn peer_holds(
        &self,
        backend: &Self::Backend,
        obj: &ObjKey,
        payload: Option<&ObjPayload>,
    ) -> impl Future<Output = Res<()>>;

    /// The backend's own membership source says `obj` is now a member of `part`: the
    /// simulated local keyhive stream, the only way membership changes. The call also does what
    /// the source's hook owes the outbox (see law 9): a notification parked while `obj` was not
    /// a member becomes fetchable and is fetched.
    fn member_added(
        &self,
        backend: &Self::Backend,
        obj: &ObjKey,
        part: &PartKey,
    ) -> impl Future<Output = Res<()>>;

    /// The backend's own membership source says `obj` is no longer a member of `part`.
    fn member_removed(
        &self,
        backend: &Self::Backend,
        obj: &ObjKey,
        part: &PartKey,
    ) -> impl Future<Output = Res<()>>;

    /// What the backend holds for `obj`: `None` while it has no payload.
    fn observe(
        &self,
        backend: &Self::Backend,
        obj: &ObjKey,
    ) -> impl Future<Output = Res<Option<ObjState>>>;

    fn stamp(&self, backend: &Self::Backend) -> impl Future<Output = Res<Self::Stamp>>;

    /// The next write of the backend fails after it has done all it can short of committing.
    fn fail_next_write(&self, backend: &Self::Backend);

    /// The payload of `value` the backend can take: a larger value is a later write, so a
    /// backend that orders its payloads takes the larger over the smaller.
    fn payload(value: u8) -> ObjPayload;

    /// Two different payloads that two nodes may write for one object at the same time, neither
    /// derived from the other: whichever way the backend merges (an order of the writes, a
    /// CRDT), it has to settle both nodes on one result.
    fn concurrent_payloads(&self) -> (ObjPayload, ObjPayload);
}

/// What the backend holds for an object: its payload, and the parts it is a member of.
type Held = (Option<ObjPayload>, BTreeSet<PartKey>);

struct Probe<'a, R: BackendRig> {
    rig: &'a R,
    backend: R::Backend,
}

impl<R: BackendRig> Probe<'_, R> {
    async fn held(&self, obj: &ObjKey) -> Res<Held> {
        Ok(match self.rig.observe(&self.backend, obj).await? {
            None => (None, BTreeSet::new()),
            Some(state) => (state.content.map(|(payload, _)| payload), state.parts),
        })
    }

    async fn peer(&self, index: u8, value: Option<u8>) -> Res<()> {
        let payload = value.map(R::payload);
        self.rig
            .peer_holds(&self.backend, &obj(index), payload.as_ref())
            .await
    }

    /// The job the machine builds for a want at `value`: `add` and `remove` are part indices,
    /// and only an adding job carries the hint.
    fn job(obj_index: u8, add: &[u8], remove: &[u8], value: u8) -> ObjJob {
        ObjJob {
            obj: obj(obj_index),
            add: add.iter().map(|&at| part(at)).collect(),
            remove: remove.iter().map(|&at| part(at)).collect(),
            payload: (!add.is_empty() || remove.is_empty()).then(|| R::payload(value)),
        }
    }

    /// What the backend's membership source delivers for `job` before the notification
    /// arrives: the job's removals and additions, as the embedder's own stream would see them.
    /// The job itself never carries membership (law 8).
    async fn source_follows(&self, job: &ObjJob) -> Res<()> {
        for part in &job.remove {
            self.rig
                .member_removed(&self.backend, &job.obj, part)
                .await?;
        }
        for part in &job.add {
            self.rig.member_added(&self.backend, &job.obj, part).await?;
        }
        Ok(())
    }

    /// `record`, then `fetch` when asked to: one pass of a job.
    async fn pass(&self, job: &ObjJob) -> Res<()> {
        if self.backend.record(job.clone()).await? == Settle::Fetch {
            self.backend.fetch(job.clone()).await?;
        }
        Ok(())
    }

    /// The source delivers `job`'s membership, then `job` is applied twice, and the second
    /// pass is checked to change nothing.
    async fn apply(&self, job: &ObjJob) -> Res<()> {
        self.source_follows(job).await?;
        self.pass(job).await?;
        let (held, stamp) = (
            self.held(&job.obj).await?,
            self.rig.stamp(&self.backend).await?,
        );
        self.pass(job).await?;
        assert_eq!(self.held(&job.obj).await?, held, "a replay changed {job:?}");
        assert_eq!(
            self.rig.stamp(&self.backend).await?,
            stamp,
            "a replay committed something for {job:?}"
        );
        Ok(())
    }

    /// The backend takes `payload` from its peer for object `obj_index`, as a job that adds
    /// part 1 with the payload as its hint: one pass, as the driver applies it.
    async fn take_from_peer(&self, obj_index: u8, payload: &ObjPayload) -> Res<()> {
        self.rig
            .member_added(&self.backend, &obj(obj_index), &part(1))
            .await?;
        self.rig
            .peer_holds(&self.backend, &obj(obj_index), Some(payload))
            .await?;
        self.pass(&ObjJob {
            obj: obj(obj_index),
            add: vec![part(1)],
            remove: vec![],
            payload: Some(payload.clone()),
        })
        .await
    }

    /// The backend holds object `obj_index` in exactly `parts`, whatever its payload.
    async fn expect_parts(&self, obj_index: u8, parts: &[u8], why: &str) -> Res<()> {
        let expected: BTreeSet<PartKey> = parts.iter().map(|&at| part(at)).collect();
        assert_eq!(self.held(&obj(obj_index)).await?.1, expected, "{why}");
        Ok(())
    }

    /// The backend holds `value` for object `obj_index`, in exactly `parts`.
    async fn expect(&self, obj_index: u8, value: Option<u8>, parts: &[u8], why: &str) -> Res<()> {
        let expected: Held = (
            value.map(R::payload),
            parts.iter().map(|&at| part(at)).collect(),
        );
        assert_eq!(self.held(&obj(obj_index)).await?, expected, "{why}");
        Ok(())
    }
}

/// Run every law above against backends the `rig` makes.
pub async fn sync_backend_laws<R: BackendRig>(rig: &R) -> Res<()> {
    let probe = || Probe {
        rig,
        backend: rig.fresh(),
    };

    // 1: adoption, one part, several, none
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    p.expect(1, Some(10), &[1], "one hinted part").await?;
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1, 2, 3], &[], 10)).await?;
    p.expect(1, Some(10), &[1, 2, 3], "several hinted parts")
        .await?;
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[], &[], 10)).await?;
    p.expect(1, Some(10), &[], "a content job has no parts")
        .await?;

    // 2: equal is a no-op (every `apply` replays it), changed replaces
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    let stamp = rig.stamp(&p.backend).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    assert_eq!(
        rig.stamp(&p.backend).await?,
        stamp,
        "a job the backend already satisfies committed something"
    );
    p.peer(1, Some(11)).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 11)).await?;
    p.expect(1, Some(11), &[1], "a changed payload replaces")
        .await?;

    // 3: memberships are exact
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    p.apply(&Probe::<R>::job(1, &[2], &[], 10)).await?;
    p.expect(1, Some(10), &[1, 2], "an add keeps the memberships held")
        .await?;
    p.apply(&Probe::<R>::job(1, &[3], &[1], 10)).await?;
    p.expect(1, Some(10), &[2, 3], "remove and add in one job")
        .await?;
    p.apply(&Probe::<R>::job(1, &[2, 3], &[], 10)).await?;
    p.expect(1, Some(10), &[2, 3], "adding what is held")
        .await?;

    // 4: a removal-only job keeps the payload and needs nothing from the peer
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1, 2], &[], 10)).await?;
    p.peer(1, None).await?;
    p.apply(&Probe::<R>::job(1, &[], &[1], 10)).await?;
    p.expect(1, Some(10), &[2], "removes the part named only")
        .await?;
    p.apply(&Probe::<R>::job(1, &[], &[3], 10)).await?;
    p.expect(1, Some(10), &[2], "removing a part never held")
        .await?;
    p.apply(&Probe::<R>::job(1, &[], &[2], 10)).await?;
    p.expect(1, Some(10), &[], "the payload outlives the last part")
        .await?;
    p.apply(&Probe::<R>::job(7, &[], &[1], 10)).await?;
    p.expect(7, None, &[], "removing from an object never seen")
        .await?;

    // 5: a failed record is all-or-nothing
    let p = probe();
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    p.peer(1, Some(11)).await?;
    let job = Probe::<R>::job(1, &[2, 3], &[1], 11);
    p.source_follows(&job).await?;
    let (held, stamp) = (p.held(&obj(1)).await?, rig.stamp(&p.backend).await?);
    rig.fail_next_write(&p.backend);
    let _failure = p
        .pass(&job)
        .await
        .expect_err("a write told to fail succeeded");
    assert_eq!(p.held(&obj(1)).await?, held, "a failed job left a trace");
    assert_eq!(
        rig.stamp(&p.backend).await?,
        stamp,
        "a failed job committed"
    );
    p.apply(&job).await?;
    p.expect(1, Some(11), &[2, 3], "the job succeeds on retry")
        .await?;

    // 6: a fetch of content the peer no longer has
    if R::DOWNLOADS {
        let p = probe();
        p.peer(1, Some(10)).await?;
        p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
        p.peer(1, None).await?;
        for job in [
            Probe::<R>::job(1, &[1, 2], &[], 12),
            Probe::<R>::job(1, &[], &[], 12),
        ] {
            p.source_follows(&job).await?;
            assert_eq!(
                p.backend.record(job.clone()).await?,
                Settle::Fetch,
                "a downloading backend fetches {job:?}"
            );
            let (held, stamp) = (p.held(&obj(1)).await?, rig.stamp(&p.backend).await?);
            let _failure = p
                .backend
                .fetch(job)
                .await
                .expect_err("a fetch of content the peer dropped succeeded");
            assert_eq!(p.held(&obj(1)).await?, held, "a failed fetch changed state");
            assert_eq!(
                rig.stamp(&p.backend).await?,
                stamp,
                "a failed fetch committed"
            );
        }
        // an object the backend never had: still pending, no payload
        p.peer(2, None).await?;
        let job = Probe::<R>::job(2, &[1], &[], 12);
        p.source_follows(&job).await?;
        assert_eq!(p.backend.record(job.clone()).await?, Settle::Fetch);
        let _failure = p
            .backend
            .fetch(job.clone())
            .await
            .expect_err("a fetch of an object the peer never held succeeded");
        // whether the memberships recorded so far show while pending is the backend's own
        assert_eq!(
            p.held(&obj(2)).await?.0,
            None,
            "a payload appeared without a download"
        );
        // the peer has it again
        p.peer(2, Some(12)).await?;
        p.apply(&job).await?;
        p.expect(
            2,
            Some(12),
            &[1],
            "the job succeeds once the peer has the content",
        )
        .await?;
    }

    // 7: two nodes that wrote different payloads concurrently converge, and then stay quiet
    concurrent_payloads_converge(rig).await?;

    // 8 to 10: membership is the backend's own
    a_peer_never_changes_membership(rig).await?;
    notification_before_membership(rig).await?;
    a_notification_while_away_is_caught_up(rig).await?;
    a_membership_elsewhere_releases_nothing(rig).await
}

/// Law 11: the notification names part 1; the source adds the object to part 2, which is not
/// its release, and then to part 1, which is.
async fn a_membership_elsewhere_releases_nothing<R: BackendRig>(rig: &R) -> Res<()> {
    let p = Probe {
        rig,
        backend: rig.fresh(),
    };
    p.peer(1, Some(10)).await?;
    p.pass(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    rig.member_added(&p.backend, &obj(1), &part(2)).await?;
    if R::DOWNLOADS {
        assert_eq!(
            p.held(&obj(1)).await?.0,
            None,
            "a membership to a part the notification did not name fetched it"
        );
    }
    rig.member_added(&p.backend, &obj(1), &part(1)).await?;
    p.expect(
        1,
        Some(10),
        &[1, 2],
        "the named part did not release the want",
    )
    .await
}

/// Law 8: a job's `add` and `remove` change nothing about membership, whatever they name and
/// however stale they are.
async fn a_peer_never_changes_membership<R: BackendRig>(rig: &R) -> Res<()> {
    let p = Probe {
        rig,
        backend: rig.fresh(),
    };
    let parts = async |p: &Probe<'_, R>| Ok::<_, eyre::Report>(p.held(&obj(1)).await?.1);
    p.peer(1, Some(10)).await?;
    rig.member_added(&p.backend, &obj(1), &part(3)).await?;
    // an add of parts the source never delivered
    p.pass(&Probe::<R>::job(1, &[1, 2], &[], 10)).await?;
    assert_eq!(
        parts(&p).await?,
        [part(3)].into(),
        "a peer's add was applied"
    );
    // a removal of a part the source still has
    p.pass(&Probe::<R>::job(1, &[], &[3], 10)).await?;
    assert_eq!(
        parts(&p).await?,
        [part(3)].into(),
        "a peer's removal was applied"
    );
    // both at once
    p.pass(&Probe::<R>::job(1, &[1], &[3], 10)).await?;
    assert_eq!(
        parts(&p).await?,
        [part(3)].into(),
        "a peer's add and remove were applied"
    );
    // the source removes it, then a stale echo of the peer's add comes back
    rig.member_removed(&p.backend, &obj(1), &part(3)).await?;
    assert_eq!(parts(&p).await?, BTreeSet::new(), "the source's removal");
    p.pass(&Probe::<R>::job(1, &[3], &[], 10)).await?;
    assert_eq!(
        parts(&p).await?,
        BTreeSet::new(),
        "a stale echo resurrected a removed member"
    );
    Ok(())
}

/// Law 9: the notification of an object arrives before the source makes the object a member.
/// Nothing is applied while it is not a member (a downloading backend has not even fetched
/// it), and the source's addition then leaves the object fetched.
async fn notification_before_membership<R: BackendRig>(rig: &R) -> Res<()> {
    let p = Probe {
        rig,
        backend: rig.fresh(),
    };
    p.peer(1, Some(10)).await?;
    let job = Probe::<R>::job(1, &[1], &[], 10);
    // the same notification twice: a replay of a parked want is the same want
    p.pass(&job).await?;
    p.pass(&job).await?;
    p.expect_parts(1, &[], "a notification made a non-member a member")
        .await?;
    if R::DOWNLOADS {
        assert_eq!(
            p.held(&obj(1)).await?.0,
            None,
            "a downloading backend fetched an object that is not a member"
        );
    }
    rig.member_added(&p.backend, &obj(1), &part(1)).await?;
    p.expect(
        1,
        Some(10),
        &[1],
        "the membership did not fetch what was parked",
    )
    .await?;
    // an object the notification never named is not fetched by another's membership
    rig.member_added(&p.backend, &obj(2), &part(1)).await?;
    assert_eq!(
        p.held(&obj(2)).await?.0,
        None,
        "a membership fetched an object nobody advertised"
    );
    Ok(())
}

/// Law 10: a member that the source removed and re-adds catches up with what the peer
/// advertised while it was away.
async fn a_notification_while_away_is_caught_up<R: BackendRig>(rig: &R) -> Res<()> {
    let p = Probe {
        rig,
        backend: rig.fresh(),
    };
    p.peer(1, Some(10)).await?;
    p.apply(&Probe::<R>::job(1, &[1], &[], 10)).await?;
    rig.member_removed(&p.backend, &obj(1), &part(1)).await?;
    p.peer(1, Some(11)).await?;
    p.pass(&Probe::<R>::job(1, &[1], &[], 11)).await?;
    p.expect_parts(1, &[], "an echo resurrected a removed member")
        .await?;
    rig.member_added(&p.backend, &obj(1), &part(1)).await?;
    p.expect(1, Some(11), &[1], "the re-added member missed the update")
        .await
}

/// Law 7: two backends that each hold a different payload for one object take each other's,
/// both ways at once, and agree; taking them again changes nothing.
async fn concurrent_payloads_converge<R: BackendRig>(rig: &R) -> Res<()> {
    let probe = || Probe {
        rig,
        backend: rig.fresh(),
    };
    let (a, b) = (probe(), probe());
    let (from_a, from_b) = rig.concurrent_payloads();
    assert_ne!(from_a, from_b, "the rig's concurrent payloads are the same");
    a.take_from_peer(1, &from_a).await?;
    b.take_from_peer(1, &from_b).await?;
    // Both ways at once: each side takes what the other held before the exchange.
    for round in 0..2 {
        let (held_a, held_b) = (a.held(&obj(1)).await?.0, b.held(&obj(1)).await?.0);
        let (stamp_a, stamp_b) = (rig.stamp(&a.backend).await?, rig.stamp(&b.backend).await?);
        a.take_from_peer(1, &held_b.expect("b holds its payload"))
            .await?;
        b.take_from_peer(1, &held_a.expect("a holds its payload"))
            .await?;
        assert_eq!(
            a.held(&obj(1)).await?,
            b.held(&obj(1)).await?,
            "the nodes hold different payloads after exchange {round}"
        );
        if round == 1 {
            assert_eq!(
                (rig.stamp(&a.backend).await?, rig.stamp(&b.backend).await?),
                (stamp_a, stamp_b),
                "an exchange between nodes that agree committed something"
            );
        }
    }
    Ok(())
}
