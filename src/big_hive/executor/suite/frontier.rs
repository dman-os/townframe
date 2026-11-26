//! Law 4: the frontier row is written exactly when the tree's prior revision is the one it was
//! computed against. Law 2: a plan is one revision, and `Landed` is what the change feed shows.

use futures::executor::block_on;

use super::Doc;
use super::Rig;
use super::fresh_db;
use super::head;
use super::tree;
use crate::document::Landed;
use crate::executor::Frontier;
use crate::executor::FrontierOutcome;

fn written(heads: &[u8], rev: u64) -> Option<Frontier> {
    Some(Frontier {
        heads: heads.iter().copied().map(head).collect(),
        rev,
    })
}

async fn frontier_of(rig: &Rig<sqlite_rs::Database>, doc: u8) -> Option<Frontier> {
    rig.frontier(doc).await.map(|(frontier, _)| frontier)
}

#[test]
fn the_frontier_row_is_written_only_if_the_tree_is_where_the_heads_were_computed() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;

        // A new tree is at revision 0.
        let first = rig
            .run(vec![Doc::new(0).commits(&[0]).frontier(&[1], 0)])
            .await;
        assert_eq!(
            first.docs[0].landed,
            Landed {
                rev: 1,
                prior_rev: 0
            }
        );
        assert_eq!(first.docs[0].frontier, FrontierOutcome::Written);
        assert_eq!(frontier_of(&rig, 0).await, written(&[1], 1));

        // Computed against revision 0, but the tree has moved to 1: the row is skipped, and the
        // commit is not.
        let stale = rig
            .run(vec![Doc::new(0).commits(&[1]).frontier(&[2], 0)])
            .await;
        assert_eq!(
            stale.docs[0].landed,
            Landed {
                rev: 2,
                prior_rev: 1
            }
        );
        assert_eq!(stale.docs[0].frontier, FrontierOutcome::Stale);
        assert_eq!(frontier_of(&rig, 0).await, written(&[1], 1), "unchanged");
        assert!(rig.feed().await.contains_key(&tree(0)));

        // Computed against the tree's current revision: written, and it records the revision of
        // the plan's batch.
        let current = rig
            .run(vec![Doc::new(0).commits(&[2]).frontier(&[3], 2)])
            .await;
        assert_eq!(
            current.docs[0].landed,
            Landed {
                rev: 3,
                prior_rev: 2
            }
        );
        assert_eq!(current.docs[0].frontier, FrontierOutcome::Written);
        assert_eq!(frontier_of(&rig, 0).await, written(&[3], 3));

        // A frontier alone, no commit (how remote content is published): the tree does not move,
        // the row records the revision it is at.
        let publish = rig.run(vec![Doc::new(0).frontier(&[4], 3)]).await;
        assert_eq!(
            publish.docs[0].landed,
            Landed {
                rev: 3,
                prior_rev: 3
            }
        );
        assert_eq!(publish.tree_rev, None);
        assert_eq!(frontier_of(&rig, 0).await, written(&[4], 3));
        assert_eq!(rig.trees_latest().await, 3);

        // A document with no row asks for none.
        let plain = rig.run(vec![Doc::new(1).commits(&[0])]).await;
        assert_eq!(plain.docs[0].frontier, FrontierOutcome::Absent);
        assert_eq!(frontier_of(&rig, 1).await, None);
    });
}

#[test]
fn a_frontier_row_puts_its_object_in_the_documents_group_parts() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;
        rig.run(vec![Doc::new(0).commits(&[0]).frontier(&[1], 0)])
            .await;
        let (_, parts) = rig.frontier(0).await.unwrap();
        assert_eq!(parts, [super::group()].into());
    });
}

#[test]
fn a_multi_document_plan_is_one_revision_and_landed_is_what_the_feed_shows() {
    block_on(async {
        let rig = Rig::on(fresh_db().await).await;
        let first = rig
            .run(vec![
                Doc::new(0).commits(&[0]),
                Doc::new(1).commits(&[0, 1]),
                Doc::new(2).commits(&[3]),
            ])
            .await;
        assert_eq!(first.tree_rev, Some(1));
        for report in &first.docs {
            assert_eq!(
                report.landed,
                Landed {
                    rev: 1,
                    prior_rev: 0
                }
            );
        }
        let feed = rig.feed().await;
        assert_eq!(feed.len(), 3);
        assert!(
            feed.values().all(|rev| *rev == 1),
            "one step for the batch: {feed:?}"
        );
        assert_eq!(rig.trees_latest().await, 1);

        // Two documents change; the third is saved a commit it already holds, which changes
        // nothing and so stays where it was.
        let second = rig
            .run(vec![
                Doc::new(0).commits(&[1]),
                Doc::new(1).commits(&[2]),
                Doc::new(2).commits(&[3]),
            ])
            .await;
        assert_eq!(second.tree_rev, Some(2));
        assert_eq!(
            second.landed(),
            vec![
                (
                    0,
                    Landed {
                        rev: 2,
                        prior_rev: 1
                    }
                ),
                (
                    1,
                    Landed {
                        rev: 2,
                        prior_rev: 1
                    }
                ),
                (
                    2,
                    Landed {
                        rev: 1,
                        prior_rev: 1
                    }
                ),
            ]
        );
        let feed = rig.feed().await;
        for (doc, rev) in [(0u8, 2), (1, 2), (2, 1)] {
            assert_eq!(feed[&tree(doc)], rev, "document {doc}");
        }
        assert_eq!(rig.trees_latest().await, 2);

        // A plan that changes no tree draws no revision.
        let none = rig.run(vec![Doc::new(2).commits(&[3])]).await;
        assert_eq!(none.tree_rev, None);
        assert_eq!(rig.trees_latest().await, 2);
    });
}
