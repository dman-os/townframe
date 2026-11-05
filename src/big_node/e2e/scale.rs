//! Replication at scale (ADR 032 section 9): one node writes ten thousand notes into a drawer it
//! shares with another, they connect, and every note must become readable, with its content, on
//! the other. The phases report their wall time and per-tenth rate, so a step that degrades
//! with the document count shows as a rising rate rather than as a single slow total.

use super::harness::Cluster;
use super::harness::Topology;
use futures::StreamExt;
use std::time::Instant;

/// How many notes are replicated. `BIG_NODE_SCALE_DOCS` overrides it, to bisect a degradation.
fn documents() -> usize {
    std::env::var("BIG_NODE_SCALE_DOCS").map_or(10_000, |count| count.parse().unwrap())
}

/// Notes written at once on the writing node: enough to keep its services busy without queueing
/// the whole set behind one another.
const WRITERS: usize = 16;

fn text(ordinal: usize) -> String {
    format!("note {ordinal}")
}

/// Logs the elapsed time every tenth of the items, and the time the last tenth took.
struct Pace {
    phase: &'static str,
    every: usize,
    start: Instant,
    lap: Instant,
}

impl Pace {
    fn new(phase: &'static str, count: usize) -> Self {
        let now = Instant::now();
        Self {
            phase,
            every: (count / 10).max(1),
            start: now,
            lap: now,
        }
    }

    fn tick(&mut self, done: usize) {
        if done.is_multiple_of(self.every) {
            let now = Instant::now();
            eprintln!(
                "SCALE {} {done}: {:.1?} total, last {} in {:.1?}",
                self.phase,
                now - self.start,
                self.every,
                now - self.lap,
            );
            self.lap = now;
        }
    }

    fn finish(&self, done: usize) -> std::time::Duration {
        let elapsed = self.start.elapsed();
        eprintln!("SCALE {} done: {done} in {elapsed:.1?}", self.phase);
        elapsed
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn long_test_ten_thousand_notes_replicate() {
    let count = documents();
    let mut cluster = Cluster::boot(&["alice", "bob"]).await;
    let drawer = cluster.share_drawer(0, &[1]).await;

    let mut pace = Pace::new("create", count);
    let mut notes = vec![None; count];
    let mut created = futures::stream::iter(0..count)
        .map(|ordinal| {
            let cluster = &cluster;
            let drawer = &drawer;
            async move {
                (
                    ordinal,
                    cluster.create_note(0, drawer, &text(ordinal)).await,
                )
            }
        })
        .buffer_unordered(WRITERS);
    let mut done = 0;
    while let Some((ordinal, note)) = created.next().await {
        notes[ordinal] = Some(note);
        done += 1;
        pace.tick(done);
    }
    drop(created);
    let create = pace.finish(count);
    let notes: Vec<_> = notes.into_iter().map(Option::unwrap).collect();

    let pace = Pace::new("replicate", count);
    cluster.connect(Topology::Pair).await;
    // The notes are checked in order, from the first one Bob could not yet read, so each wake
    // re-reads only what is still outstanding and the whole wait reads each note about once.
    let next = std::cell::Cell::new(0);
    let pace = std::cell::RefCell::new(pace);
    cluster
        .until(async |cluster| {
            while next.get() < count {
                let ordinal = next.get();
                let Some(note) = cluster.read_note(1, &notes[ordinal]).await else {
                    return false;
                };
                assert_eq!(note.text("main"), Some(text(ordinal)));
                next.set(ordinal + 1);
                pace.borrow_mut().tick(ordinal + 1);
            }
            true
        })
        .await;
    let replicate = pace.borrow().finish(count);
    eprintln!("SCALE {count} notes: created in {create:.1?}, replicated in {replicate:.1?}");
    cluster.stop_all().await;
}
