//! The runtime's tests, split by what they are about.
//!
//! - `shared.rs` holds the tests that must hold under every executor. This file includes it
//!   twice, once in `pool` and once in `inline`, each naming its executor as `EXECUTION`, so a
//!   test that passes in one and fails in the other points at the executor.
//! - `pool_only.rs` is what only the pool has: the queue hop, a statement that outlives its
//!   cancelled caller, panics resumed across the hop.
//! - `inline_only.rs` is what only `Inline` promises: statements ready on the first poll, a
//!   dropped transaction rolled back on the spot, idle eviction without a background task.

use super::*;

use crate::IntoValue;
use crate::gen_tests::queries::MIGRATIONS;
use crate::query::FromRow;
use crate::query::Sql;

fn runtime_on(execution: Execution) -> SqliteRuntime {
    SqliteRuntime::new(RuntimeConfig {
        execution,
        workers: 2,
        max_readers_per_db: 2,
        idle_timeout: Duration::from_millis(50),
        cache_size_kib: -2000,
        statement_cache_size: 64,
    })
}

pub(super) async fn block_workers(runtime: &SqliteRuntime) -> Vec<std::sync::mpsc::Sender<()>> {
    let mut blockers = Vec::new();
    for _ in 0..runtime.inner.cfg.workers.max(1) {
        let (release, released) = std::sync::mpsc::channel();
        let (started, ready) = oneshot::channel();
        assert!(runtime.inner.exec.try_call(move || {
            started.send(()).expect("worker start receiver dropped");
            assert!(released.recv().is_err());
        }));
        blockers.push(release);
        ready.await.expect("worker did not start");
    }
    blockers
}

async fn assert_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    assert!(
        std::future::poll_fn(|context| std::task::Poll::Ready(
            future.as_mut().poll(context).is_pending()
        ))
        .await
    );
}

mod pool {
    use super::*;
    const EXECUTION: Execution = Execution::Pool;
    include!("tests/shared.rs");
}

mod inline {
    use super::*;
    const EXECUTION: Execution = Execution::Inline;
    include!("tests/shared.rs");
}

mod inline_only;
mod pool_only;
mod wal;
