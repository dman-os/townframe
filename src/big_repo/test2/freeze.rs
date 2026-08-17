//! Quiescence freeze/reopen contract (B12).
//!
//! `wait_for_quiescence_freeze` resolves at a quiescent point and then freezes
//! the hub: no further events are processed and every command except
//! `unfreeze` is buffered until the matching `unfreeze`. This pins the
//! "nothing can slip past the barrier" guarantee tests rely on when they
//! snapshot state at quiescence, and that buffered commands resume FIFO after
//! the hub is reopened.

use super::harness::Pair;
use crate::Res;
use std::time::Duration;
use tokio::time::timeout;

/// A frozen quiescence wait holds commands until the matching unfreeze.
#[tokio::test(flavor = "multi_thread")]
async fn freeze_holds_commands_until_unfreeze() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(254, 255, "Owner", "FrozenPeer").await?;

    // Reach a quiescent point and freeze.
    pair.left()
        .repo
        .wait_for_quiescence_freeze(Some(Duration::from_secs(10)))
        .await?;

    // A query command sent while frozen must not complete until unfreeze:
    // the hub buffers it instead of processing it.
    let doc_id = crate::DocumentId::new([9; 32]);
    let held_fut = pair.left().repo.contains_sedimentree_id(doc_id);
    tokio::pin!(held_fut);
    let held = timeout(Duration::from_millis(500), &mut held_fut).await;
    assert!(
        held.is_err(),
        "command must be held while the hub is frozen"
    );

    // Reopen: the original buffered command replays and resolves.
    pair.left().repo.unfreeze().await?;
    let resolved = timeout(Duration::from_secs(5), held_fut).await??;
    assert!(!resolved, "unknown document must not be present");

    // The hub is a normal hub again: commands flow without freezing.
    pair.left().repo.wait_for_quiescence(None).await?;
    Ok(())
}
