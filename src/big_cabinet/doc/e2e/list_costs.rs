//! Growth pin for `ListDatums`: the work to list a drawer is linear in its slot count.
//!
//! Wall time is not asserted. Allocations on the listing thread are the deterministic proxy: every
//! explicit-heads Automerge read rebuilds a clock the size of the document, so a per-slot `*_at`
//! read shows up as allocations growing with slots squared.

use super::*;
use crate::doc::list::list_datums;
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

// SAFETY: defers to the system allocator; the counter is a thread-local `Cell` touched only here.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

fn allocations_during<T>(work: impl FnOnce() -> T) -> (u64, T) {
    let before = ALLOCATIONS.with(Cell::get);
    let out = work();
    (ALLOCATIONS.with(Cell::get) - before, out)
}

/// A doc of `slots` signed notes, a tenth of them edited once, as a plain Automerge copy, with the
/// heads from before the edits.
pub(super) async fn drawer_of(slots: usize) -> (Automerge, Vec<ChangeHash>) {
    let node = Node::boot().await;
    let alice = node.writer("alice").await;
    let keys: Vec<DatumKey> = (0..slots).map(|i| key(&format!("n{i:05}"))).collect();
    let datums = keys.iter().map(|k| (k.clone(), note("first")));
    let created = node.create(&alice, "create", datums).await.unwrap();
    let before = node
        .read(&alice, &created.doc_id, At::Current, false)
        .await
        .unwrap()
        .heads;
    let edits = keys
        .iter()
        .step_by(10)
        .map(|k| (k.clone(), note("second")));
    node.write(&alice, &created.doc_id, WriteMode::AtCurrent, edits, [])
        .await
        .unwrap();
    let mut copy = None;
    node.sabotage(&created.doc_id, |doc| copy = Some(doc.clone()))
        .await;
    node.stop().await;
    (copy.unwrap(), before)
}

#[tokio::test(flavor = "multi_thread")]
async fn listing_a_fuller_drawer_costs_in_proportion_to_its_slots() {
    let (small, small_before) = drawer_of(40).await;
    let (large, large_before) = drawer_of(160).await;
    for (label, small_at, large_at) in [
        ("current", At::Current, At::Current),
        (
            "historical",
            At::Heads(small_before),
            At::Heads(large_before),
        ),
    ] {
        let (small_cost, small_listing) = allocations_during(|| list_datums(&small, &small_at));
        let (large_cost, large_listing) = allocations_during(|| list_datums(&large, &large_at));
        assert_eq!(small_listing.unwrap().datums.len(), 40);
        assert_eq!(large_listing.unwrap().datums.len(), 160);
        // 4x the slots: linear work is ~4x the allocations, quadratic ~16x.
        assert!(
            large_cost < small_cost * 6,
            "{label}: listing 160 slots took {large_cost} allocations against {small_cost} for 40"
        );
    }
}
