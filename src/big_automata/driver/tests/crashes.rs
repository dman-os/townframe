//! Law 9: a failure of the driver's own dependencies, the peer-cursor table and the local part
//! store, is a bug and crashes the loop; it is never reported as a lost call and retried.
//!
//! The old `big_sync` trapped such errors so a test crashed on them; here the loop future panics
//! and the embedder's spawn surfaces it (the process panic hook natively), which these tests
//! observe as the loop task's join error. Transport and backend failures are the contrast:
//! `reconnect` and `laws` pin that they retry.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::rig::*;
use crate::BuckId;
use crate::driver::DriverConfig;
use crate::driver::Facts;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::memory::Failing;
use crate::driver::memory::MemoryBackend;
use crate::driver::memory::MemoryPeerCursors;
use crate::interlude::*;
use crate::part_store::ObjState;
use crate::part_store::PartStoreRead;
use crate::part_store::bucket_summary::BucketSummary;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;

/// The local store the driver reads, with reads that can be told to fail and a count of the
/// bucket reads that went through.
#[derive(Clone, Default)]
struct FlakyStore {
    inner: MemoryPartStore,
    failing: Arc<AtomicBool>,
    bucket_reads: Arc<AtomicUsize>,
}

impl FlakyStore {
    fn check(&self) -> Res<()> {
        if self.failing.load(Ordering::SeqCst) {
            eyre::bail!("the store was told to fail its reads");
        }
        Ok(())
    }
}

impl PartStoreRead for FlakyStore {
    fn obj_states(
        &self,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send {
        let checked = self.check();
        let read = self.inner.obj_states(objs);
        async move {
            checked?;
            read.await
        }
    }

    fn bucket_summaries(
        &self,
        part: &PartKey,
        ids: &[BuckId],
    ) -> impl Future<Output = Res<Vec<BucketSummary>>> + Send {
        self.bucket_reads.fetch_add(1, Ordering::SeqCst);
        let checked = self.check();
        let read = self.inner.bucket_summaries(part, ids);
        async move {
            checked?;
            read.await
        }
    }

    fn member_count(&self, part: &PartKey) -> impl Future<Output = Res<u64>> + Send {
        self.inner.member_count(part)
    }

    fn partless_objects(
        &self,
        limit: usize,
        after: Option<&ObjKey>,
    ) -> impl Future<Output = Res<Vec<ObjKey>>> + Send {
        self.inner.partless_objects(limit, after)
    }
}

/// A driver over `peer` that wants part 1 and connects, its loop spawned on its own task. The
/// task is the return value: the loop only ends by panicking, since the handle is kept alive
/// by the task that drives it.
fn start(
    peer: &ScriptedPeer,
    cursors: MemoryPeerCursors,
    local: FlakyStore,
) -> tokio::task::JoinHandle<()> {
    let backend = MemoryBackend::new(local.inner.clone(), peer.store().clone());
    let (driver, driver_loop) = PeerDriver::start(
        DriverConfig::new(principal(LOCAL_PEER), "memory"),
        clock(),
        Facts::new(),
        backend,
        cursors,
        local,
    );
    let handle = tokio::spawn(driver_loop);
    let peer = peer.clone();
    tokio::spawn(async move {
        driver
            .update_routes(RouteUpdate {
                added_parts: vec![part(1)],
                ..default()
            })
            .await;
        driver.connect(peer);
        std::future::pending::<()>().await;
    });
    handle
}

/// The message the loop crashed with.
async fn crash_of(loop_task: tokio::task::JoinHandle<()>) -> String {
    let error = within(loop_task)
        .await
        .expect_err("the loop ended without crashing");
    assert!(error.is_panic(), "the loop was cancelled: {error}");
    let payload = error.into_panic();
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .expect("the loop panicked with a message")
}

#[tokio::test(start_paused = true)]
async fn law_9_a_failing_cursor_read_crashes_the_loop() {
    let peer = ScriptedPeer::new();
    peer.publish(1, &[1, 2], 10).await;
    let cursors = MemoryPeerCursors::default();
    cursors.fail(Failing::Reads);
    let message = crash_of(start(&peer, cursors, FlakyStore::default())).await;
    assert!(
        message.contains("the peer-cursor table failed a read"),
        "{message}"
    );
}

#[tokio::test(start_paused = true)]
async fn law_9_a_failing_cursor_write_crashes_the_loop() {
    let peer = ScriptedPeer::new();
    peer.publish(1, &[1, 2], 10).await;
    let cursors = MemoryPeerCursors::default();
    cursors.fail(Failing::Writes);
    let message = crash_of(start(&peer, cursors, FlakyStore::default())).await;
    assert!(
        message.contains("the peer-cursor table failed a write"),
        "{message}"
    );
}

/// The part holds more than the bucket-walk threshold of changes, so the driver walks the
/// peer's buckets and filters them against the local store, which is then told to fail. The
/// control run, with the store healthy, proves the walk reads the store at all: a crash from a
/// walk that never happened would pass for the law.
#[tokio::test(start_paused = true)]
async fn law_9_a_failing_local_store_read_crashes_the_loop() {
    let objs: Vec<u8> = (0..=255).collect();

    let peer = ScriptedPeer::new();
    peer.publish(1, &objs, 10).await;
    let healthy = FlakyStore::default();
    let facts_loop = start(&peer, MemoryPeerCursors::default(), healthy.clone());
    until(|| healthy.bucket_reads.load(Ordering::SeqCst) > 0).await;
    facts_loop.abort();

    let peer = ScriptedPeer::new();
    peer.publish(1, &objs, 10).await;
    let flaky = FlakyStore::default();
    flaky.failing.store(true, Ordering::SeqCst);
    let message = crash_of(start(&peer, MemoryPeerCursors::default(), flaky.clone())).await;
    assert!(
        message.contains("the local part store failed a read"),
        "{message}"
    );
    assert!(flaky.bucket_reads.load(Ordering::SeqCst) > 0);
}
