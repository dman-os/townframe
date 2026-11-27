//! Law 1: a plan is all or nothing, at every one of its writes; and what survives a fault is a
//! database the next plan runs on as if nothing had happened.

use std::collections::BTreeSet;

use big_automata::part_store::PartStoreServe;
use futures::executor::block_on;
use futures::future::Either;
use futures::future::select;
use sqlite_rs::Database;

use super::Doc;
use super::Rig;
use super::committed;
use super::dump;
use super::file_db;
use super::flaky::Break;
use super::flaky::Flaky;
use super::fresh_db;
use super::plan;
use super::rich_plan;
use crate::executor::Stage;

/// A rig on `db` that already holds one committed plan (revision 1), so a failing plan changes
/// state that exists (trees with rows, a frontier) and not just an empty file.
async fn with_history(db: &Database) -> Rig<Flaky> {
    let rig = Rig::on(Flaky::new(db.clone())).await;
    let earlier = plan(vec![
        Doc::new(0).commits(&[5]).frontier(&[9], 0),
        Doc::new(2).commits(&[6]),
    ])
    .await;
    committed(rig.exec.execute(earlier).await.unwrap());
    rig.db.control.disarm();
    rig
}

/// How many write statements [`rich_plan`] makes.
async fn rich_writes() -> usize {
    let rig = with_history(&fresh_db().await).await;
    committed(rig.exec.execute(rich_plan().await).await.unwrap());
    rig.db.control.writes()
}

#[test]
fn a_failure_at_every_write_of_a_plan_leaves_nothing_behind() {
    block_on(async {
        let writes = rich_writes().await;
        assert!(
            writes >= 8,
            "the plan writes to every store: {writes} writes"
        );

        let mut stages = BTreeSet::new();
        for at in 0..writes {
            let db = fresh_db().await;
            let rig = with_history(&db).await;
            let before = dump(&db).await;
            let feed = rig.feed().await;
            let parts_rev = rig.parts.latest_rev().await.unwrap();

            rig.db.control.arm(at, Break::Fail);
            let fault = rig
                .exec
                .execute(rich_plan().await)
                .await
                .expect_err("the armed write fails the plan");
            stages.insert(format!("{:?}", fault.stage));
            assert_eq!(dump(&db).await, before, "a fault at write {at} left rows");
            assert_eq!(
                rig.feed().await,
                feed,
                "a fault at write {at} moved the feed"
            );
            assert_eq!(rig.parts.latest_rev().await.unwrap(), parts_rev);

            // The database is as good as it was: the same plan now lands, on the revision after
            // the history's.
            rig.db.control.disarm();
            let executed = committed(rig.exec.execute(rich_plan().await).await.unwrap());
            assert_eq!(executed.tree_rev, Some(2), "after a fault at write {at}");
        }
        // Every store's write was among the ones broken.
        for stage in ["Certificates", "Keyhive", "Tree", "Frontier", "Membership"] {
            assert!(stages.contains(stage), "no fault hit {stage}: {stages:?}");
        }
    });
}

#[test]
fn a_dropped_connection_mid_plan_leaves_nothing_and_the_file_reopens_clean() {
    block_on(async {
        let writes = rich_writes().await;
        for at in [0, writes / 2, writes - 1] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("hive.sqlite");
            let db = file_db(&path).await;
            let rig = with_history(&db).await;
            let before = dump(&db).await;

            // Stop the plan in the middle of its writes and abandon it: its task is gone, as in
            // a crash, and takes the transaction with it.
            rig.db.control.arm(at, Break::Hang);
            {
                let stuck = rig.exec.execute(rich_plan().await);
                let reached = rig.db.control.reached.notified();
                futures::pin_mut!(stuck, reached);
                let Either::Right(_) = select(stuck, reached).await else {
                    panic!("the plan finished before it stopped at write {at}");
                };
            }
            drop(rig);
            drop(db);

            let reopened = file_db(&path).await;
            assert_eq!(dump(&reopened).await, before, "stopped at write {at}");
            let rig = Rig::on(reopened).await;
            let executed = committed(rig.exec.execute(rich_plan().await).await.unwrap());
            assert_eq!(
                executed.tree_rev,
                Some(2),
                "it carries on from the committed revision"
            );
        }
    });
}

#[test]
#[should_panic(expected = "names each document and tree once")]
fn a_plan_that_names_a_tree_twice_is_refused() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;
        let mut twice = plan(vec![Doc::new(0)]).await;
        let mut other = Doc::new(1).build().await;
        other.tree = super::tree(0);
        twice.docs.push(other);
        rig.exec.execute(twice).await.unwrap();
    });
}

#[test]
fn a_fault_goes_to_the_supervisor_and_the_plan_is_never_answered() {
    block_on(async {
        let rig = with_history(&fresh_db().await).await;
        rig.db.control.arm(0, Break::Fail);
        let (trap, mut faults) = crate::executor::Trap::new();
        let run = rig.exec.run_or_trap(rich_plan().await, &trap);
        let fault = faults.recv();
        futures::pin_mut!(run, fault);
        match select(run, fault).await {
            Either::Right((fault, _never_answered)) => {
                assert_eq!(fault.unwrap().stage, Stage::Certificates);
            }
            Either::Left((verdict, _)) => panic!("answered: {verdict:?}"),
        }
    });
}
