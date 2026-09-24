//! Durable admission-log consumer that drives the prekey janitor.
//!
//! Modeled on [`super::causal_checkpoint_worker`], but deliberately simpler:
//! the janitor's work is cheap and strictly serial (rotate a consumed
//! prekey, refill under the floor), so there is no keyed-scheduler or
//! watermark machinery — just a durable cursor and an in-order tail.
//!
//! Guarantees:
//!
//! - **At-least-once.** Admission rows exist only after their effects are
//!   visible in the keyhive graph ([`admission_events_after`]); this consumer's
//!   retention-registry entry is written once when the runtime is assembled,
//!   before the workers and the maintenance loop, so pruning cannot outrun it.
//!   The walker's durable progress is only advanced after a row's housekeeping
//!   *completed successfully*, so a crash or runtime outage re-reads the
//!   un-advanced window on the next boot. Replay safety is structural, not
//!   tracked: a replayed
//!   `CgkaOperation::Add` naming an already-rotated prekey fails the
//!   published-set precheck (the rotate tombstoned it out of `prekeys()`),
//!   so reprocessing an old row is a no-op.
//! - **Replay from durable progress.** A new janitor starts at revision zero and
//!   replays the admission log through the same serial walker used by other
//!   durable consumers, resuming from its own progress row thereafter. Replaying
//!   an already-handled Add is safe because the published-set guard makes it a
//!   no-op.
use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::runtime2::keyhive_admission;
use crate::store::sqlite::SqliteBigRepoStore;
use big_sync::SqliteDeltaWalkerStateRepo;
use big_sync_core::delta_walker_state::DeltaWalkerStateRepo;
use big_sync_core::revisioned_store::{RevisionRead, RevisionedStore};
use big_sync_core::serial_delta_walker::SerialDeltaWalker;
use keyhive_core::event::static_event::StaticEvent;

pub struct PrekeyJanitorWorkerStopToken {
    pub(crate) abort: futures::future::AbortHandle,
}

impl PrekeyJanitorWorkerStopToken {
    pub fn cancel(&self) {
        self.abort.abort();
    }
}

pub struct SpawnedPrekeyJanitorWorker {
    pub stop: PrekeyJanitorWorkerStopToken,
    pub run: <future_form::Sendable as future_form::FutureForm>::Future<'static, eyre::Result<()>>,
}

pub fn spawn_prekey_janitor_worker(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    timer: std::sync::Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
) -> SpawnedPrekeyJanitorWorker {
    let (abort_handle, abort_registration) = futures::future::AbortHandle::new_pair();

    let run = future_form::Sendable::from_future(async move {
        let fut = run_prekey_janitor_tail(store, keyhive, timer);
        match futures::future::Abortable::new(fut, abort_registration).await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    });

    SpawnedPrekeyJanitorWorker {
        stop: PrekeyJanitorWorkerStopToken {
            abort: abort_handle,
        },
        run,
    }
}

/// Process one batch of admitted rows in causal order, running the janitor
/// policy for each `CgkaOperation::Add` naming our own published prekeys.
///
/// Returns the sequence through which the durable cursor may advance
/// (`None` when the batch is empty). Any housekeeping failure aborts the
/// batch with the un-advanced rows left for the next poll.
pub(crate) async fn process_admissions(
    keyhive: &BigKeyhiveHandle,
    rows: Vec<(u64, Vec<u8>)>,
) -> Res<Option<u64>> {
    let mut handled_through: Option<u64> = None;
    for (seq, bytes) in rows {
        let event: StaticEvent<Vec<u8>> =
            bincode::deserialize(&bytes).expect("persisted keyhive admission event must decode");
        if let StaticEvent::CgkaOperation(operation) = event
            && let beekem::operation::CgkaOperation::Add { added_id, pk, .. } = operation.payload()
        {
            super::prekey_janitor::housekeep_after_add(keyhive, added_id, pk).await?;
        }
        handled_through = Some(seq);
    }
    Ok(handled_through)
}

async fn run_prekey_janitor_tail(
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    timer: std::sync::Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
) -> Res<()> {
    let identity = crate::store::sqlite::KEYHIVE_ADMISSION_CONSUMER_PREKEY_JANITOR;
    let state = SqliteDeltaWalkerStateRepo::new(
        store.sql.read_pool.clone(),
        store.sql.write_pool.clone(),
        identity.0,
        identity.1,
    )
    .await?;
    let source = keyhive_admission::Store {
        store: store.clone(),
        timer,
    };
    let durable = state.progress().await?.upstream_revision;
    let reader = source.open((), durable).await?;
    let mut walker: SerialDeltaWalker<'_, keyhive_admission::Store, SqliteDeltaWalkerStateRepo> =
        SerialDeltaWalker::open(reader, &state).await?;
    tracing::info!(cursor = durable, "prekey janitor: tail starting");

    loop {
        match walker.next().await? {
            RevisionRead::ReplayComplete { through } => {
                tracing::debug!(through, "prekey janitor: admission replay complete");
            }
            RevisionRead::Entries { revision, entries } => {
                let rows: Vec<(u64, Vec<u8>)> = entries
                    .into_iter()
                    .map(|row: keyhive_admission::AdmittedRow| (row.seq, row.bytes.to_vec()))
                    .collect();
                process_admissions(&keyhive, rows).await?;
                walker.settle(revision).await?;
                tracing::debug!(revision, "prekey janitor: advanced admission cursor");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyhive_listener::BigRepoKeyhiveListener;
    use crate::keyhive_storage::BigRepoKeyhiveStorage;
    use beekem::id::{MemberId, TreeId};
    use beekem::operation::CgkaOperation;
    use keyhive_core::principal::individual::op::rotate_key::RotateKeyOp;
    use keyhive_crypto::share_key::ShareKey;
    use keyhive_crypto::signed::Signed;
    use keyhive_crypto::signer::memory::MemorySigner;

    /// Boot a real keyhive handle over in-memory storage.
    ///
    /// This worker's policy is guarded by the *published-set precheck* rather
    /// than by incorporation verification, so a handle plus fabricated
    /// admission rows exercises exactly the durable-cursor path: no runtime, no
    /// network, no SQLite file, no second repo. The event receiver is returned
    /// to keep the listener's channel open (dropping it only makes the listener
    /// log a dropped event).
    async fn boot_handle(
        seed: [u8; 32],
    ) -> (
        BigKeyhiveHandle,
        async_channel::Receiver<crate::runtime2::Runtime2Evt>,
    ) {
        let (evt_tx, evt_rx) = async_channel::unbounded::<crate::runtime2::Runtime2Evt>();
        let keyhive = BigKeyhiveHandle::new(
            seed,
            BigRepoKeyhiveListener {
                evt_tx,
                storage: BigRepoKeyhiveStorage::memory(),
            },
        )
        .await
        .expect("boot a keyhive handle over in-memory storage");
        (keyhive, evt_rx)
    }

    /// The signer matching [`boot_handle`]'s seed. [`process_admissions`] does
    /// not verify signatures (incorporation does), but signing with the handle's
    /// own identity keeps these rows shaped like the ones the log really holds.
    fn signer_for(seed: [u8; 32]) -> MemorySigner {
        MemorySigner::from(ed25519_dalek::SigningKey::from_bytes(&seed))
    }

    fn tree_id(seed: [u8; 32]) -> TreeId {
        TreeId(ed25519_dalek::VerifyingKey::from_bytes(&seed).expect("valid tree id point"))
    }

    fn member_id(seed: [u8; 32]) -> MemberId {
        MemberId(ed25519_dalek::VerifyingKey::from_bytes(&seed).expect("valid member id point"))
    }

    fn encode(event: &StaticEvent<Vec<u8>>) -> Vec<u8> {
        bincode::serialize(event).expect("serialize admission event")
    }

    /// A `CgkaOperation` that is not an `Add`: the outer `if let` matches while
    /// the inner `Add` arm must not fire.
    fn cgka_remove_event(
        signer: &MemorySigner,
        doc_id: TreeId,
        id: MemberId,
    ) -> StaticEvent<Vec<u8>> {
        let op = CgkaOperation::Remove {
            id,
            leaf_idx: 0,
            removed_keys: Vec::new(),
            predecessors: Vec::new(),
            doc_id,
        };
        let payload = bincode::serialize(&op).expect("serialize remove op");
        StaticEvent::CgkaOperation(Box::new(Signed::new(
            op,
            signer.0.verifying_key(),
            ed25519_dalek::Signer::sign(&signer.0, &payload),
        )))
    }

    /// A prekey op: not a `CgkaOperation` at all, so the outer `if let` misses.
    fn prekey_rotated_event(
        signer: &MemorySigner,
        old: ShareKey,
        new: ShareKey,
    ) -> StaticEvent<Vec<u8>> {
        let op = RotateKeyOp { old, new };
        let payload = bincode::serialize(&op).expect("serialize rotate op");
        StaticEvent::PrekeyRotated(Box::new(Signed::new(
            op,
            signer.0.verifying_key(),
            ed25519_dalek::Signer::sign(&signer.0, &payload),
        )))
    }

    fn cgka_add_event(
        signer: &MemorySigner,
        doc_id: TreeId,
        added_id: MemberId,
        pk: ShareKey,
    ) -> StaticEvent<Vec<u8>> {
        let op = CgkaOperation::init_add(doc_id, added_id, pk);
        let payload = bincode::serialize(&op).expect("serialize add op");
        StaticEvent::CgkaOperation(Box::new(Signed::new(
            op,
            signer.0.verifying_key(),
            ed25519_dalek::Signer::sign(&signer.0, &payload),
        )))
    }

    /// The durable cursor advances past every row it was handed, whatever the
    /// row's variant: `handled_through = Some(seq)` is assigned outside the
    /// `CgkaOperation::Add` arm.
    ///
    /// The mutation this pins: an early `continue` on the non-`Add` arm, or an
    /// `if` that skips the assignment, freezes the janitor's cursor with no
    /// error and no other progress signal. The rotation work is skipped, which is
    /// correct — the cursor is not. A frozen cursor here re-reads the same window
    /// on every poll forever.
    ///
    /// The last row (`Add` naming another agent) covers the other early return:
    /// `housekeep_after_add` must be a no-op for a foreign member, *and* the row
    /// must still advance. Asserting the pool is untouched is what makes that
    /// half discriminating — if the `added_id != local` guard were dropped, the
    /// row would rotate our own published key.
    #[tokio::test]
    async fn non_add_admission_rows_advance_the_janitor_cursor() -> Res<()> {
        let seed = [31; 32];
        let (keyhive, _evt_rx) = boot_handle(seed).await;
        let signer = signer_for(seed);
        let doc_id = tree_id([0x2a; 32]);

        let pool_before = keyhive.prekeys().await;
        assert!(
            pool_before.len() >= 2,
            "the genesis pool must publish enough prekeys for this fixture; got {}",
            pool_before.len()
        );
        let mut pool_iter = pool_before.iter();
        let first = *pool_iter.next().expect("pool is non-empty");
        let second = *pool_iter.next().expect("pool has at least two keys");
        let foreign_member = member_id([0x33; 32]);

        let rows = vec![
            (
                5u64,
                encode(&cgka_remove_event(&signer, doc_id, foreign_member)),
            ),
            (6u64, encode(&prekey_rotated_event(&signer, first, second))),
            (
                7u64,
                encode(&cgka_add_event(&signer, doc_id, foreign_member, first)),
            ),
        ];

        assert_eq!(
            process_admissions(&keyhive, rows).await?,
            Some(7),
            "every row must advance the cursor, not only the rows that do housekeeping"
        );
        assert_eq!(
            keyhive.prekeys().await,
            pool_before,
            "none of these rows may rotate a prekey: removing, rotating and \
             naming-a-foreign-member are all policy no-ops"
        );

        assert_eq!(
            process_admissions(&keyhive, Vec::new()).await?,
            None,
            "an empty batch reports no advance, so the caller must not settle a cursor it never earned"
        );
        assert_eq!(
            keyhive.prekeys().await,
            pool_before,
            "an empty batch must not touch the pool"
        );
        Ok(())
    }

    /// A replayed `Add` must rotate the consumed prekey exactly once, and the
    /// batch must still report its advance.
    ///
    /// This is the deterministic, sleep-free form of what
    /// `test2::cgka::tier6_prekey_janitor_replay_is_noop` proves by booting two
    /// repos and polling on a wall-clock deadline. The rule it pins is the
    /// at-least-once contract documented at the top of this file: the cursor may
    /// re-read a window after a crash or outage, so reprocessing a handled row
    /// must be a structural no-op via the published-set precheck (a rotated key
    /// is tombstoned out of `prekeys()`), and the cursor must still move.
    ///
    /// The mutation this catches: a janitor that rotates on every replay, or one
    /// whose precheck misses so that a replayed row resurrects the retired key.
    #[tokio::test]
    async fn a_replayed_add_is_a_noop_and_advances_the_cursor() -> Res<()> {
        let seed = [37; 32];
        let (keyhive, _evt_rx) = boot_handle(seed).await;
        let signer = signer_for(seed);
        let doc_id = tree_id([0x2a; 32]);

        let local = keyhive.local_individual_id().await;
        let added_id = MemberId(local.0.0);
        let pool_before = keyhive.prekeys().await;
        assert!(
            !pool_before.is_empty(),
            "a booted handle must publish a genesis prekey pool"
        );
        let consumed = *pool_before
            .iter()
            .next()
            .expect("the genesis pool is non-empty");

        let rows = vec![(
            11u64,
            encode(&cgka_add_event(&signer, doc_id, added_id, consumed)),
        )];

        assert_eq!(
            process_admissions(&keyhive, rows.clone()).await?,
            Some(11),
            "the Add naming our own prekey must advance the cursor"
        );

        let after_first = keyhive.prekeys().await;
        assert!(
            !after_first.contains(&consumed),
            "the consumed prekey must be retired from the published set"
        );
        assert!(
            after_first.len() >= crate::runtime2::prekey_janitor::PREKEY_POOL_FLOOR,
            "the pool must be refilled to the floor after a rotation; got {}",
            after_first.len()
        );
        assert_eq!(
            keyhive.rotate_op_count_for(consumed).await,
            1,
            "consuming our own prekey must rotate it exactly once"
        );

        assert_eq!(
            process_admissions(&keyhive, rows).await?,
            Some(11),
            "a replayed row must still report its advance; the cursor is at-least-once"
        );
        assert_eq!(
            keyhive.prekeys().await,
            after_first,
            "replay must not rotate again or resurrect the retired prekey"
        );
        assert_eq!(
            keyhive.rotate_op_count_for(consumed).await,
            1,
            "exactly one rotation must exist across replays"
        );
        Ok(())
    }

    /// A published view that stops reflecting publication must crash the refill, not spin.
    ///
    /// `refill_to_floor` used to be a bare `while pool < FLOOR { expand; pool = len() }`. A
    /// successful expansion that does not move the published set — the view and the core's
    /// prekey state having diverged — made it spin forever: burning a core, logging nothing,
    /// and holding the durable cursor frozen, because `walker.settle` is only reached once
    /// housekeeping returns. The incoherence is a broken invariant rather than a race between
    /// data planes, so the loop must crash naming it instead of proceeding below the floor
    /// (which would hand concurrent inviters a pool with no distinct slot left).
    ///
    /// The mutations this catches: dropping the growth check from `refill_to_floor` (the loop
    /// is then bounded by `PREKEY_POOL_FLOOR` expansions and panics with the *exhaustion*
    /// message, so this test fails on the message rather than hanging), or downgrading either
    /// crash into a `warn!` and continuing (the spin, or a sub-floor pool, comes back).
    ///
    /// `pin_published_prekeys` is the test-only seam that makes the divergence
    /// constructible: the pinned view names only the key this row consumes, a set the core
    /// does not agree with while still passing the published-set precheck, so the row reaches
    /// the refill. Production cannot reach it (see the field's doc comment on
    /// `BigKeyhiveHandle`).
    #[tokio::test]
    #[should_panic(expected = "publishing a prekey did not grow the pool")]
    async fn a_published_view_that_stops_growing_crashes_the_refill_instead_of_spinning() {
        let seed = [41; 32];
        let (keyhive, _evt_rx) = boot_handle(seed).await;
        let signer = signer_for(seed);
        let doc_id = tree_id([0x2a; 32]);

        let local = keyhive.local_individual_id().await;
        let added_id = MemberId(local.0.0);
        let published = keyhive.prekeys().await;
        let consumed = *published
            .iter()
            .next()
            .expect("the genesis pool is non-empty");

        // The view reports one key — below the floor — and stops moving; the core keeps
        // publishing into it.
        keyhive.pin_published_prekeys([consumed].into_iter().collect());

        let rows = vec![(
            13u64,
            encode(&cgka_add_event(&signer, doc_id, added_id, consumed)),
        )];
        // Reaching this line at all is the wrong outcome: the refill must not return
        // while the published view is stuck below the floor. The test is `#[should_panic]`
        // on the growth assertion, so a normal return is reported as "did not panic".
        // (Bound rather than `let _ =` because the crate denies `let_underscore_drop`.)
        let _outcome = process_admissions(&keyhive, rows).await;
    }
}
