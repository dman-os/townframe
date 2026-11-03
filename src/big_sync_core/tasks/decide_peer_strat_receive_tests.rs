use super::*;
use crate::part_store::PartDirtyCount;

#[tokio::test(flavor = "current_thread")]
async fn durable_receive_ignores_bucket_hint_and_resumes_from_received_not_summary() {
    for content_cursor in [0, 99_000] {
        let (strategy, bucket_walk_entered) = tests::decide(
            100_000,
            content_cursor,
            PartDirtyCount {
                member_changes: 200,
                access_changes: 100,
            },
            Some(10),
        )
        .await;
        assert!(!bucket_walk_entered);
        assert!(matches!(
            strategy,
            PeerPartStratDecision::Cursor(CursorStrat {
                latest_cursor: 100_000,
                last_cursor: 10,
            })
        ));
    }
}
