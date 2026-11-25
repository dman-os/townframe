//! Walks that reach the last bucket of their working level.
//!
//! Listing is in bucket order and a page resumes at the id after its last bucket
//! ([`BuckId::increment`]). The id after a level's last bucket is not a bucket: below the deepest
//! level it is a non-canonical id on the same level, which lists nothing at the level; at the
//! deepest level it is past every level. Either way the walk is done once it has listed the last
//! bucket and answered its leaves. A step that stayed on the last bucket would list it for ever.

use super::harness::Driver;
use super::harness::Member;
use super::harness::ObjSpec;
use super::harness::Responder;
use super::harness::Walk;
use super::harness::build_keyed;
use super::harness::part;
use super::harness::payload;
use super::*;
use crate::part_store::laws::obj_in_bucket;

/// The last bucket of `level`.
fn last_of(level: BuckLevel) -> BuckId {
    BuckId::new(level, u16::MAX)
}

fn live() -> ObjSpec {
    ObjSpec {
        payload: Some(payload(1)),
        parts: vec![Member::Live, Member::Absent, Member::Absent],
    }
}

/// A clean last bucket ends a listing at every working level: below the deepest it relists once
/// from the id past it, which names nothing at the level, and at the deepest there is nothing
/// to relist.
#[test]
fn filter_buckets_ends_the_listing_past_the_last_bucket_of_the_working_level() {
    for level in 1..=BuckId::MAX_LEVEL {
        let last = last_of(level);
        let store = SummaryStore {
            summaries: HashMap::from([(last, summary(last, 2, 2, 5))]),
        };
        let remote = vec![summary(last, 2, 2, 9)];
        let answer = futures::executor::block_on(filter_buckets(part(0), level, remote, &store))
            .expect("the summary store reads infallibly");
        match (level < BuckId::MAX_LEVEL, answer) {
            (true, FilteredBuckets::Relist(offset)) => {
                assert_eq!(offset, last.increment(), "level {level}");
                assert_eq!(offset.level(), level, "level {level}");
            }
            (false, FilteredBuckets::Done) => {}
            (_, FilteredBuckets::Relist(offset)) => {
                panic!(
                    "level {level}: relisting {offset:?} would repeat or overrun the last bucket"
                )
            }
            (_, FilteredBuckets::Done) => {
                panic!("level {level}: the id past the level is a relist")
            }
            (_, FilteredBuckets::Handoff(out)) => {
                panic!("level {level}: clean, handed off {out:?}")
            }
        }
    }
}

/// A dirty last bucket is walked and then the machine stops listing: it asks for the id past
/// the bucket once, below the deepest level, and never at the deepest.
#[test]
fn the_machine_stops_listing_past_the_last_bucket_and_emits_its_member() {
    for level in 1..=BuckId::MAX_LEVEL {
        let last = last_of(level);
        let member = obj_in_bucket(last, 0);
        let mut machine = BucketMachine::with_limits(part(0), level, 1, 0, 1024, 1024);
        let mut out = vec![];
        machine.on_bucket_page(vec![summary(last, 1, 1, 5)], &mut out);
        let relists = out
            .iter()
            .filter_map(|cmd| match cmd {
                BucketMachineCommand::ListBuckets { offset, .. } => Some(*offset),
                _ => None,
            })
            .collect::<Vec<_>>();
        if level < BuckId::MAX_LEVEL {
            assert_eq!(relists, [last.increment()], "level {level}");
        } else {
            assert_eq!(relists, [], "level {level}: nothing is left to list");
        }
        assert!(!machine.walk_done(), "the leaf is still to read");
        out.clear();
        machine.on_obj_page(
            Map::from([(
                last,
                BucketObjLeafPage {
                    after: None,
                    entries: vec![BucketObjEntry {
                        obj_key: member.clone(),
                        delta: PartObjDelta::New,
                    }],
                    next_after: None,
                },
            )]),
            &mut out,
        );
        machine.pull(8, &mut out);
        assert!(
            out.iter().any(|cmd| matches!(
                cmd,
                BucketMachineCommand::SyncObj { obj_key, .. } if *obj_key == member
            )),
            "level {level}: the member of the last bucket is emitted: {out:?}"
        );
        if level < BuckId::MAX_LEVEL {
            assert!(!machine.walk_done(), "the relist is still outstanding");
            out.clear();
            machine.on_bucket_page(vec![], &mut out);
        }
        assert!(machine.walk_done(), "level {level}");
        assert!(
            out.iter()
                .all(|cmd| !matches!(cmd, BucketMachineCommand::ListBuckets { .. })),
            "level {level}: a finished listing asks for no more: {out:?}"
        );
    }
}

/// Whole walks over stores with a member in the last bucket of the working level, a stranger
/// to it in the first, and the requester holding nothing or everything: the walk terminates,
/// the requester converges, and the listings stay a handful.
#[test]
fn a_walk_over_the_last_bucket_of_its_working_level_terminates_and_strands_nothing() -> Res<()> {
    futures::executor::block_on(async {
        for level in 1..=BuckId::MAX_LEVEL {
            let members = vec![
                (obj_in_bucket(last_of(level), 0), live()),
                (obj_in_bucket(BuckId::new(level, 0), 0), live()),
            ];
            for requester_holds in [false, true] {
                let responder = MemoryPartStore::default();
                build_keyed(&responder, &members, &[part(0)]).await?;
                let requester = MemoryPartStore::default();
                if requester_holds {
                    build_keyed(&requester, &members, &[]).await?;
                }
                let mut driver = Driver::new(requester, Responder::new(responder, 1024));
                driver.working_level = Some(level);
                driver.run(&[part(0)]).await?;

                harness::assert_converged(&driver.requester, driver.responder.store(), &part(0))
                    .await?;
                let traffic = driver.responder.traffic[&part(0)];
                assert!(
                    traffic.list_requests <= 2,
                    "level {level}, requester holds {requester_holds}: {traffic:?}"
                );
                let walk = driver.walks[&part(0)];
                if requester_holds {
                    assert_eq!(walk, Walk::Clean, "level {level}");
                    assert_eq!(traffic.leaf_requests, 0);
                } else {
                    assert_eq!(
                        walk,
                        Walk::Walked {
                            working_level: level
                        }
                    );
                    assert_eq!(driver.applied[&part(0)].syncs, 2, "level {level}");
                }
            }
        }
        Ok(())
    })
}
