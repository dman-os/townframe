//! End-to-end native activation batches, including journal open, default
//! maintenance, durable receipt reads, and graceful final snapshot. This is a
//! memory-store benchmark, not a SQLite durability or Wasm-instantiation claim.
use std::sync::Arc;
use std::time::Instant;

use utils_rs::prelude::*;
use wflow_core::kvstore::atomic::MemoryStore;
use wflow_tokio::authority_runtime::AuthorityRuntime;
use wflow_tokio::authority_runtime::Submission;
use wflow_tokio::authority_runtime::session::Host;
use wflow_tokio::local_native_host::LocalNativeHost;
use wflow_wdk::Json;

const JOBS: usize = 64;
const SAMPLES: usize = 16;

fn main() -> Res<()> {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let native = Arc::new(LocalNativeHost::new());
    native.register_typed("sum", |cx, steps: u64| {
        Box::pin(async move {
            let mut sum = 0_u64;
            for value in 0..steps {
                sum += cx.effect(async || Ok(Json(value))).await?;
            }
            Ok(sum)
        })
    });
    let host: Arc<dyn Host> = native;
    println!("store=memory jobs_per_batch={JOBS} samples={SAMPLES} lifecycle=included");
    for steps in [0_u64, 1, 8] {
        // Warm the executor/host before measuring independent, bounded stores.
        executor.block_on(batch(Arc::clone(&host), steps))?;
        let started = Instant::now();
        for _ in 0..SAMPLES {
            executor.block_on(batch(Arc::clone(&host), steps))?;
        }
        let elapsed = started.elapsed();
        let jobs = JOBS * SAMPLES;
        println!(
            "steps_per_job={steps} jobs={jobs} elapsed_ms={:.3} jobs_per_second={:.1} effects_per_second={:.1}",
            elapsed.as_secs_f64() * 1000.0,
            jobs as f64 / elapsed.as_secs_f64(),
            jobs as f64 * steps as f64 / elapsed.as_secs_f64(),
        );
    }
    Ok(())
}

async fn batch(host: Arc<dyn Host>, steps: u64) -> Res<()> {
    let store = MemoryStore::default();
    let runtime = AuthorityRuntime::start(store.writer()?, 0, host).await?;
    let mut handles = Vec::with_capacity(JOBS);
    for job in 0..JOBS {
        let (_, handle) = runtime
            .submit_handle(Submission::new(job.to_string(), "sum", steps.to_string()))
            .await
            .unwrap();
        handles.push(handle);
    }
    let expected = (steps * steps.saturating_sub(1) / 2).to_string();
    for handle in handles {
        let receipt = handle.await_receipt().await.unwrap();
        assert_eq!(receipt.steps, steps);
        assert_eq!(handle.get_result().await?, Some(Ok(expected.clone())));
        std::hint::black_box(receipt);
    }
    runtime.stop().await;
    Ok(())
}
