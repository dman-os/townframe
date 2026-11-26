//! Law 3: a plan's key epochs are checked first, over the plan's own documents only.

use futures::executor::block_on;

use super::Doc;
use super::Rig;
use super::certificate;
use super::cgka_chain;
use super::committed;
use super::doc_key;
use super::dump;
use super::flaky::Break;
use super::flaky::Flaky;
use super::fresh_db;
use super::plan;
use crate::executor::Verdict;

/// A membership change of `doc`, as a plan of its own: the next operation of its CGKA history.
async fn membership_change(rig: &Rig<sqlite_rs::Database>, doc: u8) {
    let mut change = plan(vec![]).await;
    change.keyhive.cgka_ops = vec![(doc_key(doc), cgka_chain(doc, rig.epoch(doc).await + 1))];
    committed(rig.exec.execute(change).await.unwrap());
}

#[test]
fn a_membership_change_between_encrypt_and_commit_makes_the_plan_stale_and_it_leaves_nothing() {
    block_on(async {
        let db = fresh_db().await;
        let rig = Rig::on(db.clone()).await;
        // Both documents were encrypted under epoch 0.
        let encrypted_under = (rig.epoch(0).await, rig.epoch(1).await);
        assert_eq!(encrypted_under, (0, 0));
        membership_change(&rig, 1).await;
        let (before, feed) = (dump(&db).await, rig.feed().await);

        let mut stale = plan(vec![
            Doc::new(0).commits(&[0]).frontier(&[1], 0).if_epoch(0),
            Doc::new(1).commits(&[0]).if_epoch(0),
        ])
        .await;
        stale.certificates = vec![certificate(3)];
        let verdict = rig.exec.execute(stale).await.unwrap();
        assert_eq!(
            verdict,
            Verdict::Stale { doc: 1 },
            "the document whose epoch moved"
        );
        assert_eq!(
            dump(&db).await,
            before,
            "a stale plan writes nothing, not even for the document that was fine"
        );
        assert_eq!(rig.feed().await, feed);

        // Encrypted again under the epoch it is now, the same plan commits.
        let again = rig
            .run(vec![
                Doc::new(0).commits(&[0]).frontier(&[1], 0).if_epoch(0),
                Doc::new(1).commits(&[0]).if_epoch(1),
            ])
            .await;
        assert_eq!(again.tree_rev, Some(1));
    });
}

#[test]
fn a_cgka_change_on_an_unrelated_document_does_not_make_a_plan_stale() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;
        membership_change(&rig, 7).await;
        membership_change(&rig, 7).await;
        assert_eq!(rig.epoch(7).await, 2);
        assert_eq!(rig.epoch(0).await, 0);
        let executed = rig.run(vec![Doc::new(0).commits(&[0]).if_epoch(0)]).await;
        assert_eq!(executed.tree_rev, Some(1));
    });
}

#[test]
fn a_plan_that_carries_no_expectation_is_never_stale() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;
        membership_change(&rig, 0).await;
        let executed = rig.run(vec![Doc::new(0).commits(&[0])]).await;
        assert_eq!(executed.tree_rev, Some(1));
    });
}

#[test]
fn a_plans_own_cgka_operations_move_the_epoch_it_was_checked_at() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;
        let mut rotating = plan(vec![Doc::new(0).commits(&[0]).if_epoch(0)]).await;
        rotating.keyhive.cgka_ops = vec![(doc_key(0), cgka_chain(0, 2))];
        committed(rig.exec.execute(rotating).await.unwrap());
        assert_eq!(rig.epoch(0).await, 2);
        // An operation the store already holds is ignored and does not move the epoch.
        membership_change_replay(&rig).await;
        assert_eq!(rig.epoch(0).await, 2);
        // So a plan encrypted before the rotation is stale now.
        let stale = plan(vec![Doc::new(0).commits(&[1]).if_epoch(0)]).await;
        assert_eq!(
            rig.exec.execute(stale).await.unwrap(),
            Verdict::Stale { doc: 0 }
        );
    });
}

async fn membership_change_replay(rig: &Rig<sqlite_rs::Database>) {
    let mut replay = plan(vec![]).await;
    replay.keyhive.cgka_ops = vec![(doc_key(0), cgka_chain(0, 2))];
    committed(rig.exec.execute(replay).await.unwrap());
}

#[test]
fn validation_comes_before_any_write() {
    block_on(async {
        let db = fresh_db().await;
        let rig = Rig::on(Flaky::new(db.clone())).await;
        let mut change = plan(vec![]).await;
        change.keyhive.cgka_ops = vec![(doc_key(0), cgka_chain(0, 1))];
        committed(rig.exec.execute(change).await.unwrap());

        // The first write of the plan would fail, but the plan is stale first.
        rig.db.control.arm(0, Break::Fail);
        let mut stale = plan(vec![Doc::new(0).commits(&[0]).if_epoch(0)]).await;
        stale.certificates = vec![certificate(3)];
        let verdict = rig.exec.execute(stale).await.unwrap();
        assert_eq!(verdict, Verdict::Stale { doc: 0 });
        assert_eq!(rig.db.control.writes(), 0, "no write statement ran");
    });
}
