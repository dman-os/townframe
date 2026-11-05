use std::sync::Arc;

use tokio::time::Duration;
use wflow_wdk::JobErrorX;
use wflow_wdk::Json;
use wflow_wdk::session::ReplayStep;
use wflow_wdk::session::SessionYield;
use wflow_wdk::session::WaitYield;
use wflow_wdk::types::JobError;

use crate::NativeHostDriver;
use utils_rs::prelude::ferr;

#[tokio::test]
async fn native_session_yields_effect_sleep_recv_and_complete() {
    let driver = NativeHostDriver::new();
    driver.register_typed("steps", |cx, (): ()| {
        Box::pin(async move {
            let value: i64 = cx.effect(|| async { Ok(Json(7_i64)) }).await?;
            cx.sleep(Duration::from_millis(1)).await?;
            let Json(message): Json<i64> = cx.recv().await?;
            Ok::<_, JobErrorX>(value + message)
        })
    });

    let session = driver
        .spawn(Arc::from("job"), "steps", "null".into(), Vec::new())
        .expect("registered workflow");
    let Some(SessionYield::PersistStep { step_id: 0, .. }) = session.next_yield().await else {
        panic!("effect must yield first");
    };
    assert!(session.resume());
    let Some(SessionYield::WaitStep {
        step_id: 1,
        deets: WaitYield::Timer { .. },
        ..
    }) = session.next_yield().await
    else {
        panic!("sleep must yield second");
    };
    assert!(session.resume());
    let Some(SessionYield::WaitStep {
        step_id: 2,
        deets: WaitYield::Message { .. },
        ..
    }) = session.next_yield().await
    else {
        panic!("recv must yield third");
    };
    assert!(session.deliver(Arc::from("5")));
    let Some(SessionYield::Complete(Ok(value))) = session.next_yield().await else {
        panic!("workflow must complete");
    };
    assert_eq!(value, "12");
}

#[tokio::test]
async fn native_session_replays_completed_effect_without_running_it() {
    let driver = NativeHostDriver::new();
    driver.register_typed("replay", |cx, (): ()| {
        Box::pin(async move {
            let value: i64 = cx.effect(|| async { Ok(Json(99_i64)) }).await?;
            Ok::<_, JobErrorX>(value)
        })
    });

    let session = driver
        .spawn(
            Arc::from("job-replay"),
            "replay",
            "null".into(),
            vec![ReplayStep::completed("4")],
        )
        .expect("registered workflow");
    let Some(SessionYield::Complete(Ok(value))) = session.next_yield().await else {
        panic!("replayed workflow must complete");
    };
    assert_eq!(value, "4");
}

#[tokio::test]
async fn native_session_maps_terminal_failure() {
    let driver = NativeHostDriver::new();
    driver.register_typed("fails", |_cx, (): ()| {
        Box::pin(async { Err::<(), _>(JobErrorX::Terminal(ferr!("bad input"))) })
    });
    let session = driver
        .spawn(Arc::from("job-fails"), "fails", "null".into(), Vec::new())
        .expect("registered workflow");
    let Some(SessionYield::Complete(Err(JobError::Terminal(value)))) = session.next_yield().await
    else {
        panic!("failure must be mapped to terminal job error");
    };
    assert!(value.contains("bad input"));
}
