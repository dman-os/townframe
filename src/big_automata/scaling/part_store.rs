//! The part store's writes and listings at 10k and 100k members of one part.

use futures::executor::block_on;
use utils_rs::testing::alloc_count::measure;

use super::assert_linear;
use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::AuthorizedPage;
use crate::part_store::LogRoutes;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;
use crate::rev::laws::limits;

const SMALL: usize = 10_000;
/// Members per transaction, so a part of `n` members has `n / BATCH` revs.
const BATCH: usize = 100;

fn obj_of(row: usize) -> ObjKey {
    ObjKey::new(&u32::try_from(row).expect("rows fit u32").to_be_bytes())
}

/// Write members `rows` of part 0 in `BATCH`-sized transactions.
async fn write_members(store: &MemoryPartStore, rows: std::ops::Range<usize>) {
    for start in rows.clone().step_by(BATCH) {
        let chunk = start..(start + BATCH).min(rows.end);
        let mut tx = store.begin();
        store
            .set_payloads(
                &mut tx,
                chunk
                    .clone()
                    .map(|row| (obj_of(row), serde_json::json!({ "row": row })))
                    .collect(),
            )
            .await
            .expect("payloads");
        store
            .add_members(
                &mut tx,
                &chunk.map(|row| (obj_of(row), part(0))).collect::<Vec<_>>(),
            )
            .await
            .expect("members");
        tx.commit();
    }
}

/// A store whose part 0 holds `n` live members and is readable by principal 0.
fn filled(n: usize) -> MemoryPartStore {
    let store = MemoryPartStore::default();
    block_on(async {
        let mut tx = store.begin();
        store
            .grant_access(&mut tx, &[(part(0), principal(0), AccessLevel::Read)])
            .await
            .expect("grant");
        tx.commit();
        write_members(&store, 0..n).await;
    });
    store
}

#[test]
fn part_store_member_writes_are_linear() {
    assert_linear("part store add n members", SMALL, |n| {
        let store = MemoryPartStore::default();
        measure(|| block_on(write_members(&store, 0..n))).1
    });
}

/// Paging the part's membership log through the authorized read, 256 rows a page.
#[test]
fn part_store_log_paging_is_linear() {
    assert_linear("part store page the log", SMALL, |n| {
        let store = filled(n);
        let latest = block_on(store.latest_rev()).expect("latest");
        let routes = LogRoutes {
            parts: [(part(0), 0)].into(),
            objects: default(),
        };
        let mut page = AuthorizedPage::default();
        measure(|| {
            let mut after = 0;
            while after < latest {
                block_on(store.read_authorized(
                    &principal(0),
                    &routes,
                    after,
                    latest,
                    limits(256, None),
                    &mut page,
                ))
                .expect("read");
                after = page.rows.through;
            }
        })
        .1
    });
}

/// Listing every bucket of one level, 256 summaries a page: what a bucket walk's listing does.
#[test]
fn part_store_bucket_listing_is_linear() {
    assert_linear("part store list the deepest buckets", SMALL, |n| {
        let store = filled(n);
        let level = BuckId::MAX_LEVEL;
        measure(|| {
            let mut from = BuckId::new(level, 0);
            let mut listed = 0;
            loop {
                let page =
                    block_on(store.bucket_list(&principal(0), &part(0), from, level, 0, 256))
                        .expect("list")
                        .expect("readable");
                let Some(last) = page.last() else {
                    break;
                };
                from = last.id.increment();
                listed += page.len();
            }
            assert!(listed > 0);
        })
        .1
    });
}

/// Reading every leaf of the root bucket, 1024 entries a page.
#[test]
fn part_store_leaf_paging_is_linear() {
    assert_linear("part store page the root leaves", SMALL, |n| {
        let store = filled(n);
        measure(|| {
            let mut after = None;
            let mut read = 0;
            loop {
                let page = block_on(store.bucket_leaves(
                    &principal(0),
                    &part(0),
                    &[(BuckId::ROOT, after.clone())],
                    0,
                    1024,
                ))
                .expect("leaves")
                .expect("readable")
                .remove(0);
                let Some(last) = page.last() else {
                    break;
                };
                after = Some(last.obj.clone());
                read += page.len();
            }
            assert_eq!(read, n);
        })
        .1
    });
}

/// The object states of every member, 256 a read.
#[test]
fn part_store_object_reads_are_linear() {
    assert_linear("part store read n object states", SMALL, |n| {
        let store = filled(n);
        let objs: Vec<_> = (0..n).map(obj_of).collect();
        measure(|| {
            for chunk in objs.chunks(256) {
                let states = block_on(store.obj_states(chunk)).expect("states");
                assert!(states.iter().all(Option::is_some));
            }
        })
        .1
    });
}
