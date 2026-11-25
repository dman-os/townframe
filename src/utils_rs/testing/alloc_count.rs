//! A counting allocator for deterministic cost checks: how many allocations (and bytes) an
//! operation makes does not depend on the machine, so it can gate CI where wall time cannot.
//!
//! A test binary installs it once and measures closures:
//!
//! ```ignore
//! #[global_allocator]
//! static HEAP: utils_rs::testing::alloc_count::CountingAllocator = utils_rs::testing::alloc_count::CountingAllocator;
//!
//! let (out, counted) = utils_rs::testing::alloc_count::measure(|| work());
//! ```
//!
//! Counting is per thread: only the thread inside [`measure`] is counted, so concurrent tests
//! and background threads do not pollute a measurement (a multi-threaded runtime's workers are
//! not counted either; measure on a current-thread runtime). Outside [`measure`] the allocator
//! costs one thread-local read per call. A `realloc` counts as one allocation of its new size,
//! as dhat-rs counts it. Works unchanged on `wasm32-unknown-unknown`.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

thread_local! {
    // Const-initialised and without a destructor, so touching it from inside the allocator
    // never allocates and never fails during thread teardown.
    static TALLY: Cell<Option<Counted>> = const { Cell::new(None) };
}

/// What an operation allocated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counted {
    pub allocations: u64,
    pub bytes: u64,
}

/// [`System`], counting while a [`measure`] is open on the calling thread.
pub struct CountingAllocator;

fn count(bytes: usize) {
    TALLY.with(|tally| {
        if let Some(mut counted) = tally.get() {
            counted.allocations += 1;
            counted.bytes += bytes as u64;
            tally.set(Some(counted));
        }
    });
}

// SAFETY: every call is forwarded unchanged to `System`; the counting touches only a
// const-initialised thread-local `Cell` and never allocates.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() {
            count(layout.size());
        }
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc_zeroed(layout) };
        if !block.is_null() {
            count(layout.size());
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        unsafe { System.dealloc(block, layout) };
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(block, layout, new_size) };
        if !moved.is_null() {
            count(new_size);
        }
        moved
    }
}

/// Run `op` and count what this thread allocated meanwhile. Nesting is a bug and panics.
/// Counts nothing unless [`CountingAllocator`] is the binary's global allocator.
pub fn measure<R>(op: impl FnOnce() -> R) -> (R, Counted) {
    start();
    let out = op();
    (out, finish())
}

/// Open a count on this thread, for work that cannot be one closure: a future awaited on the
/// browser's single thread, where everything the page allocates until [`finish`] is counted.
pub fn start() {
    assert!(
        TALLY
            .with(|tally| tally.replace(Some(Counted::default())))
            .is_none(),
        "counts do not nest"
    );
}

/// Close the count [`start`] opened.
pub fn finish() -> Counted {
    TALLY
        .with(|tally| tally.replace(None))
        .expect("a count is open until finish closes it")
}
