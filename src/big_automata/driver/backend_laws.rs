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
use crate::part_store::laws::payload;

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

    /// What the backend holds for `obj`: `None` while it has no payload.
    fn observe(
        &self,
        backend: &Self::Backend,
        obj: &ObjKey,
    ) -> impl Future<Output = Res<Option<ObjState>>>;

    fn stamp(&self, backend: &Self::Backend) -> impl Future<Output = Res<Self::Stamp>>;

    /// The next write of the backend fails after it has done all it can short of committing.
    fn fail_next_write(&self, backend: &Self::Backend);
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
        let payload = value.map(payload);
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
            payload: (!add.is_empty() || remove.is_empty()).then(|| payload(value)),
        }
    }

    /// `record`, then `fetch` when asked to: one pass of a job.
    async fn pass(&self, job: &ObjJob) -> Res<()> {
        if self.backend.record(job.clone()).await? == Settle::Fetch {
            self.backend.fetch(job.clone()).await?;
        }
        Ok(())
    }

    /// Apply `job` twice, and check the second pass changed nothing.
    async fn apply(&self, job: &ObjJob) -> Res<()> {
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

    /// The backend holds `value` for object `obj_index`, in exactly `parts`.
    async fn expect(&self, obj_index: u8, value: Option<u8>, parts: &[u8], why: &str) -> Res<()> {
        let expected: Held = (
            value.map(payload),
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
    let (held, stamp) = (p.held(&obj(1)).await?, rig.stamp(&p.backend).await?);
    let job = Probe::<R>::job(1, &[2, 3], &[1], 11);
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
    Ok(())
}
