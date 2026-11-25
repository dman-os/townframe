//! The bucket walk over a part of `n` and `10n` members: a clean part (the roots agree), a part
//! the requester lacks a few members of (the walk should cost the dirt, not the part), and a
//! part the requester holds nothing of (the walk costs every member).

use futures::executor::block_on;
use utils_rs::alloc_count::measure;

use super::harness::Driver;
use super::harness::Member;
use super::harness::ObjSpec;
use super::harness::Responder;
use super::harness::Walk;
use super::harness::build;
use super::harness::part;
use super::harness::payload;
use crate::part_store::memory::MemoryPartStore;
use crate::scaling::assert_linear;

/// A part is 16-way bucketed with 1024-member working buckets, so these sizes put the larger
/// part on a deeper working level than the smaller.
const SMALL: usize = 2_000;
/// Members the requester lacks in the sparse case.
const LACKING: usize = 16;

fn live() -> ObjSpec {
    ObjSpec {
        payload: Some(payload(1)),
        parts: vec![Member::Live],
    }
}

fn missing() -> ObjSpec {
    ObjSpec {
        payload: None,
        parts: vec![Member::Absent],
    }
}

/// Walk part 0 from a requester that holds every member except those `lacks` says it does
/// not, and count what the walk allocated.
fn walked(
    n: usize,
    lacks: impl Fn(usize) -> bool,
    expect: impl Fn(&Walk, u64),
) -> utils_rs::alloc_count::Counted {
    let theirs = vec![live(); n];
    let ours: Vec<_> = (0..n)
        .map(|at| if lacks(at) { missing() } else { live() })
        .collect();
    let responder = MemoryPartStore::default();
    let requester = MemoryPartStore::default();
    block_on(async {
        build(&responder, &theirs, &[part(0)])
            .await
            .expect("responder");
        build(&requester, &ours, &[]).await.expect("requester");
    });
    let mut driver = Driver::new(requester, Responder::new(responder, 1024));
    let ((), counted) = measure(|| {
        block_on(driver.run(&[part(0)])).expect("the walk ends");
    });
    expect(
        &driver.walks[&part(0)],
        driver.applied.get(&part(0)).map_or(0, |a| a.syncs),
    );
    counted
}

#[test]
fn bucket_walk_of_a_clean_part_is_linear() {
    assert_linear("bucket walk, clean part", SMALL, |n| {
        walked(
            n,
            |_| false,
            |walk, syncs| {
                assert_eq!((*walk, syncs), (Walk::Clean, 0));
            },
        )
    });
}

#[test]
fn bucket_walk_of_a_sparsely_dirty_part_is_linear() {
    assert_linear("bucket walk, 16 members lacking", SMALL, |n| {
        walked(
            n,
            |at| at % (n / LACKING) == 0,
            |walk, syncs| {
                assert!(matches!(walk, Walk::Walked { .. }));
                assert_eq!(syncs, LACKING as u64);
            },
        )
    });
}

#[test]
fn bucket_walk_of_an_empty_requester_is_linear() {
    assert_linear("bucket walk, requester holds nothing", SMALL, |n| {
        walked(
            n,
            |_| true,
            |walk, syncs| {
                assert!(matches!(walk, Walk::Walked { .. }));
                assert_eq!(syncs, n as u64);
            },
        )
    });
}
