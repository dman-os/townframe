//! Law 1 and law 5 together: the loop waits for a cursor write, and every call is a future
//! polled only by the loop. A call that holds something the cursor table also needs, across
//! one of its awaits, must therefore still be polled while the loop waits for the write.
//!
//! The case that matters is a SQLite embedder, where the part store and the peer-cursor table
//! share one file and so its single writer lease: a record's transaction holds the lease
//! between statements, and the cursor write needs it. If the loop awaits the write without
//! polling its calls, neither side ever moves.

use super::rig::*;
use crate::driver::DriverConfig;
use crate::driver::Facts;
use crate::driver::ObjJob;
use crate::driver::PeerCursor;
use crate::driver::PeerCursors;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::Settle;
use crate::driver::SyncBackend;
use crate::driver::memory::MemoryBackend;
use crate::driver::memory::MemoryPeerCursors;
use crate::interlude::*;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;

/// The one writer lease of a file that holds both the part store and the cursor table.
type Writer = Arc<tokio::sync::Semaphore>;

/// Holds the writer across the record of `held`, until the cursor table has been asked to
/// write: a transaction that is open when the loop flushes a cursor.
struct HoldingBackend {
    memory: MemoryBackend,
    writer: Writer,
    held: ObjKey,
    cursor_write_asked: tokio::sync::watch::Receiver<bool>,
}

impl SyncBackend for HoldingBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        if job.obj != self.held {
            return self.memory.record(job).await;
        }
        let _lease = self
            .writer
            .acquire()
            .await
            .expect("the lease is never closed");
        self.cursor_write_asked
            .clone()
            .wait_for(|asked| *asked)
            .await
            .expect("the table holds its sender");
        self.memory.record(job).await
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        self.memory.fetch(job).await
    }
}

struct SharedWriterCursors {
    table: MemoryPeerCursors,
    writer: Writer,
    cursor_write_asked: Arc<tokio::sync::watch::Sender<bool>>,
}

impl PeerCursors for SharedWriterCursors {
    async fn read(
        &self,
        peer: &PeerKey,
        backend: &str,
        parts: &[PartKey],
    ) -> Res<Vec<Option<PeerCursor>>> {
        self.table.read(peer, backend, parts).await
    }

    async fn write(
        &self,
        peer: &PeerKey,
        backend: &str,
        writes: &[(PartKey, PeerCursor)],
    ) -> Res<()> {
        self.cursor_write_asked.send_replace(true);
        let _lease = self
            .writer
            .acquire()
            .await
            .expect("the lease is never closed");
        self.table.write(peer, backend, writes).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_call_holding_what_the_cursor_write_needs_is_polled_while_the_loop_waits_for_it() {
    // two revs: the first object settles (and its cursor is flushed) while the second one's
    // record holds the writer
    let peer = ScriptedPeer::new();
    peer.publish(1, &[1], 10).await;
    peer.publish(1, &[2], 10).await;
    let local = MemoryPartStore::default();
    let writer = Writer::new(tokio::sync::Semaphore::new(1));
    let (asked_tx, asked_rx) = tokio::sync::watch::channel(false);
    let (driver, driver_loop) = PeerDriver::start(
        DriverConfig::new(principal(LOCAL_PEER), "memory"),
        clock(),
        Facts::new(),
        HoldingBackend {
            memory: MemoryBackend::new(local.clone(), peer.store().clone()),
            writer: Arc::clone(&writer),
            held: obj(2),
            cursor_write_asked: asked_rx,
        },
        SharedWriterCursors {
            table: MemoryPeerCursors::default(),
            writer,
            cursor_write_asked: Arc::new(asked_tx),
        },
        local.clone(),
    );
    tokio::spawn(driver_loop);
    driver
        .update_routes(RouteUpdate {
            added_parts: vec![part(1)],
            ..default()
        })
        .await;
    driver.connect(peer.clone());
    until_synced(&local, &peer).await;
}

/// Wait until the local store holds what the peer does for part 1.
async fn until_synced(local: &MemoryPartStore, peer: &ScriptedPeer) {
    let (local, peer) = (local.clone(), peer.clone());
    within(async {
        loop {
            if Rig::members(&local, 1, 2).await == Rig::members(peer.store(), 1, 2).await
                && !Rig::members(&local, 1, 2).await.is_empty()
            {
                return;
            }
            n0_future::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}
