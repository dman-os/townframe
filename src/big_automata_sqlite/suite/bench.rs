//! A baseline of commit throughput and serve rounds at a few store sizes. It prints `BENCH`
//! lines (run with `--no-capture`, in release for numbers worth quoting) and asserts only that
//! what it measured answered. `AUTOMATA_SQLITE_BENCH_SIZES` (comma separated object counts)
//! sets the sizes; the default stays small enough for an ordinary test run.

use std::num::NonZeroUsize;
use std::time::Duration;
use std::time::Instant;

use big_automata::part_store::AccessLevel;
use big_automata::part_store::AuthorizedPage;
use big_automata::part_store::LogRoutes;
use big_automata::part_store::PartStoreServe;
use big_automata::part_store::PartStoreWrite;
use big_automata::rev::Limits;
use futures::executor::block_on;

use super::Store;
use crate::interlude::*;

const BATCH: usize = 100;
const ROUNDS: usize = 50;

fn obj(index: usize) -> ObjKey {
    ObjKey::new(&(index as u64).to_be_bytes())
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

/// Add objects `first..first + BATCH` to `part` in one transaction.
async fn commit_batch(store: &Store, part: &PartKey, first: usize) -> Res<()> {
    let objs = (first..first + BATCH).map(obj).collect::<Vec<_>>();
    let mut tx = store.begin().await?;
    store
        .set_payloads(
            &mut tx,
            objs.iter()
                .enumerate()
                .map(|(index, obj)| {
                    (
                        obj.clone(),
                        serde_json::json!({ "n": first + index, "pad": "xxxxxxxxxxxxxxxx" }),
                    )
                })
                .collect(),
        )
        .await?;
    store
        .add_members(
            &mut tx,
            &objs
                .into_iter()
                .map(|obj| (obj, part.clone()))
                .collect::<Vec<_>>(),
        )
        .await?;
    tx.commit().await?;
    Ok(())
}

#[test]
fn bench_commit_and_serve_baseline() -> Res<()> {
    let sizes = std::env::var("AUTOMATA_SQLITE_BENCH_SIZES")
        .unwrap_or_else(|_| "200,2000".to_owned())
        .split(',')
        .map(|size| {
            size.trim()
                .parse::<usize>()
                .expect("bench sizes are integers")
        })
        .collect::<Vec<_>>();
    block_on(async {
        let store = super::fresh().await?;
        let (part, principal) = (PartKey::new(b"bench-part"), PeerKey::new(b"bench-peer"));
        let mut tx = store.begin().await?;
        store
            .grant_access(
                &mut tx,
                &[(part.clone(), principal.clone(), AccessLevel::Read)],
            )
            .await?;
        tx.commit().await?;

        let mut size = 0;
        for target in sizes {
            let mut commits = Vec::new();
            let filling = Instant::now();
            while size < target {
                let started = Instant::now();
                commit_batch(&store, &part, size).await?;
                commits.push(started.elapsed());
                size += BATCH;
            }
            if !commits.is_empty() {
                let total = filling.elapsed();
                println!(
                    "BENCH commit batch={BATCH} fill_to={size}: {} commits, p50 {:?}, {:.0} objs/s",
                    commits.len(),
                    median(commits),
                    (target as f64) / total.as_secs_f64()
                );
            }
            serve_round(&store, &part, &principal, size).await?;
        }
        Ok(())
    })
}

/// One serve round as a responder runs it: the part's summary, a page of its rows from the
/// start and from near the head, a bucket list and a leaf page.
async fn serve_round(store: &Store, part: &PartKey, principal: &PeerKey, size: usize) -> Res<()> {
    let latest = PartStoreServe::latest_rev(store).await?;
    let limits = Limits {
        max_entries: NonZeroUsize::new(BATCH).expect("nonzero"),
        max_bytes: None,
    };
    let routes = |bound| LogRoutes {
        parts: [(part.clone(), bound)].into(),
        objects: default(),
    };
    let mut page = AuthorizedPage::default();
    let mut samples = BTreeMap::<&str, Vec<Duration>>::new();
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let summary = store
            .part_summaries(principal, &[(part.clone(), 0)])
            .await?;
        samples
            .entry("part_summaries")
            .or_default()
            .push(started.elapsed());
        assert_eq!(summary[0].expect("readable").member_count, size as u64);

        let started = Instant::now();
        store
            .read_authorized(principal, &routes(0), 0, latest, limits, &mut page)
            .await?;
        samples
            .entry("read_authorized_from_start")
            .or_default()
            .push(started.elapsed());
        assert_eq!(page.rows.entries.len(), BATCH.min(size));

        let started = Instant::now();
        store
            .read_authorized(
                principal,
                &routes(latest - 1),
                latest - 1,
                latest,
                limits,
                &mut page,
            )
            .await?;
        samples
            .entry("read_authorized_at_head")
            .or_default()
            .push(started.elapsed());
        assert_eq!(page.rows.entries.len(), BATCH);

        let started = Instant::now();
        let list = store
            .bucket_list(principal, part, BuckId::ROOT, 1, 0, usize::MAX)
            .await?;
        samples
            .entry("bucket_list_to_level_1")
            .or_default()
            .push(started.elapsed());
        assert!(list.expect("readable").len() > 1);

        let started = Instant::now();
        let leaves = store
            .bucket_leaves(principal, part, &[(BuckId::new(1, 0), None)], 0, BATCH)
            .await?;
        samples
            .entry("bucket_leaves_page")
            .or_default()
            .push(started.elapsed());
        assert!(leaves.expect("readable")[0].len() <= BATCH);
    }
    for (what, samples) in samples {
        println!("BENCH serve size={size} {what}: p50 {:?}", median(samples));
    }
    Ok(())
}
