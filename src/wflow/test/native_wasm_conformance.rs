use std::time::Duration;

use wflow_core::partition::job_events::JobError;
use wflow_core::partition::job_events::JobRunResult;
use wflow_core::partition::log::PartitionLogEntry;
use wflow_wdk::JobErrorX;
use wflow_wdk::Json;
use wflow_wdk::WflowCtx;

use crate::interlude::*;
use crate::test::InitialWorkload;
use crate::test::WflowTestContext;
use crate::test::keyvalue_plugin::WasiKeyvalue;
use crate::test::test_wflows_wasm_path;

#[derive(Debug, Serialize, Deserialize)]
struct FailsOnceArgs {
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EffectChainArgs {
    steps: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SleepThenEffectArgs {
    millis: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RecvMessageThenEffectArgs {}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct DeferOnTerminalArgs {
    pub(super) key: String,
    pub(super) fail: bool,
    /// Park on a message after the effect, so a cancel can land on the wait.
    #[serde(default)]
    pub(super) wait: bool,
    /// Fail transiently in a second effect on every run, so the forward
    /// retry budget runs out.
    #[serde(default)]
    pub(super) flake: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct NondeterministicReplayArgs {
    key: String,
}

async fn native_fails_once(
    cx: &mut WflowCtx,
    kv: Arc<WasiKeyvalue>,
    args: FailsOnceArgs,
) -> Result<(), JobErrorX> {
    cx.effect(async || {
        const VALUE: u64 = 42;
        match kv.get_value("workload_123", "default", &args.key).await {
            None => {
                kv.set_value(
                    "workload_123",
                    "default",
                    &args.key,
                    VALUE.to_le_bytes().to_vec(),
                )
                .await
                .map_err(|err| JobErrorX::Terminal(ferr!("error setting keyvalue: {err:?}")))?;
                Err(JobErrorX::Transient(ferr!("first run, woohoo!")))
            }
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(JobErrorX::Transient(ferr!("not u64")));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                let number = u64::from_le_bytes(buf);
                if number != VALUE {
                    Err(JobErrorX::Terminal(ferr!("unexpected value: {number}")))
                } else {
                    Ok(Json(()))
                }
            }
        }
    })
    .await?;
    Ok(())
}

async fn native_defer_on_terminal(
    cx: &mut WflowCtx,
    kv: Arc<WasiKeyvalue>,
    args: DeferOnTerminalArgs,
) -> Result<String, JobErrorX> {
    let kv_a = Arc::clone(&kv);
    let key_a = args.key.clone();
    cx.defer("cleanup-a", async move || {
        let mut bytes = kv_a
            .get_value("workload_123", "default", &key_a)
            .await
            .unwrap_or_default();
        bytes.push(b'A');
        kv_a.set_value("workload_123", "default", &key_a, bytes)
            .await
            .map_err(|err| JobErrorX::Terminal(ferr!("error setting keyvalue: {err:?}")))?;
        Ok(())
    })?;

    let kv_b = Arc::clone(&kv);
    let key_b = args.key.clone();
    cx.defer("cleanup-b", async move || {
        let mut bytes = kv_b
            .get_value("workload_123", "default", &key_b)
            .await
            .unwrap_or_default();
        bytes.push(b'B');
        kv_b.set_value("workload_123", "default", &key_b, bytes)
            .await
            .map_err(|err| JobErrorX::Terminal(ferr!("error setting keyvalue: {err:?}")))?;
        Ok(())
    })?;

    let step1: String = cx
        .named_effect("step-1", async || Ok(Json("prepared".to_owned())))
        .await?;

    if args.wait {
        let Json(_message) = cx.recv::<Json<String>>().await?;
    }
    if args.flake {
        cx.named_effect::<_, (), _>("step-2", async || {
            Err(JobErrorX::Transient(ferr!("flaky after {step1}")))
        })
        .await?;
    }
    if args.fail {
        return Err(JobErrorX::Terminal(ferr!("boom after {step1}")));
    }
    Ok(step1)
}

async fn native_nondeterministic_replay(
    cx: &mut WflowCtx,
    kv: Arc<WasiKeyvalue>,
    args: NondeterministicReplayArgs,
) -> Result<(), JobErrorX> {
    let flipped = match kv.get_value("workload_123", "default", &args.key).await {
        None => {
            kv.set_value("workload_123", "default", &args.key, vec![1])
                .await
                .map_err(|err| JobErrorX::Terminal(ferr!("error setting keyvalue: {err:?}")))?;
            false
        }
        Some(_) => true,
    };

    if !flipped {
        cx.named_effect("step-alpha", async || Ok(Json(1_u64)))
            .await?;
        Err(JobErrorX::Transient(ferr!("force replay after step-alpha")))
    } else {
        cx.named_effect("step-beta", async || Ok(Json(1_u64)))
            .await?;
        Ok(())
    }
}

pub(super) async fn setup_dual_host_context() -> Res<WflowTestContext> {
    let wasm_keys = vec![
        "authority_probe".to_string(),
        "fails_once".to_string(),
        "effect_chain".to_string(),
        "sleep_then_effect".to_string(),
        "recv_message_then_effect".to_string(),
        "defer_on_terminal".to_string(),
        "nondeterministic_replay".to_string(),
    ];
    let native_keys = vec![
        "native_fails_once".to_string(),
        "native_effect_chain".to_string(),
        "native_sleep_then_effect".to_string(),
        "native_recv_message_then_effect".to_string(),
        "native_defer_on_terminal".to_string(),
        "native_nondeterministic_replay".to_string(),
    ];

    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
            wflow_keys: wasm_keys,
        }])
        .build()
        .await?;

    let kv = Arc::clone(&test_cx.keyvalue_plugin);
    test_cx
        .local_native_host
        .register_bundle(native_keys.clone(), move |args, backend| {
            let kv = Arc::clone(&kv);
            Box::pin(wflow_wdk::route_wflows!(args, backend, {
                "native_fails_once" => |cx, args: FailsOnceArgs| {
                    native_fails_once(cx, Arc::clone(&kv), args)
                },
                "native_effect_chain" => |cx, args: EffectChainArgs| async move {
                    for ii in 0..args.steps {
                        cx.effect(async || Ok(Json(ii))).await?;
                    }
                    Ok(())
                },
                "native_sleep_then_effect" => |cx, args: SleepThenEffectArgs| async move {
                    cx.sleep(Duration::from_millis(args.millis)).await?;
                    cx.effect(async || Ok(Json(serde_json::json!({"slept": true}))))
                        .await?;
                    Ok(())
                },
                "native_recv_message_then_effect" => |cx, _args: RecvMessageThenEffectArgs| async move {
                    let Json(_value) = cx.recv::<Json<serde_json::Value>>().await?;
                    cx.effect(async || Ok(Json(serde_json::json!({"ok": true}))))
                        .await?;
                    Ok(())
                },
                "native_defer_on_terminal" => |cx, args: DeferOnTerminalArgs| {
                    native_defer_on_terminal(cx, Arc::clone(&kv), args)
                },
                "native_nondeterministic_replay" => |cx, args: NondeterministicReplayArgs| {
                    native_nondeterministic_replay(cx, Arc::clone(&kv), args)
                },
            }))
        });

    let test_cx = test_cx.start().await?;
    test_cx.register_native_wflows(native_keys).await?;
    Ok(test_cx)
}

#[derive(Debug, PartialEq, Eq)]
enum TerminalSummary {
    Success(String),
    TerminalErr(String),
}

fn collect_job_summary(
    entries: &[(u64, PartitionLogEntry)],
    job_id: &str,
) -> (usize, usize, Option<TerminalSummary>) {
    let mut step_effects = 0;
    let mut step_waits = 0;
    let mut terminal = None;
    for (_, entry) in entries {
        let PartitionLogEntry::JobEffectResult(event) = entry else {
            continue;
        };
        if event.job_id.as_ref() != job_id {
            continue;
        }
        match &event.result {
            JobRunResult::StepEffect(_) => step_effects += 1,
            JobRunResult::StepWait(_) => step_waits += 1,
            JobRunResult::Success { value_json } => {
                terminal = Some(TerminalSummary::Success(value_json.to_string()));
            }
            JobRunResult::WflowErr(JobError::Terminal { error_json }) => {
                terminal = Some(TerminalSummary::TerminalErr(error_json.to_string()));
            }
            JobRunResult::WflowErr(JobError::Transient { .. })
            | JobRunResult::WorkerErr(_)
            | JobRunResult::Aborted => {}
        }
    }
    (step_effects, step_waits, terminal)
}

#[tokio::test(flavor = "multi_thread")]
async fn wasm_and_native_hosts_match_across_workflows() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let test_cx = setup_dual_host_context().await?;

    // 1. Multi-step warm effect chain + fails_once retry + sleep_then_effect
    for (prefix, fails_key, chain_key, sleep_key) in [
        ("wasm", "fails_once", "effect_chain", "sleep_then_effect"),
        (
            "native",
            "native_fails_once",
            "native_effect_chain",
            "native_sleep_then_effect",
        ),
    ] {
        test_cx
            .schedule_job(
                format!("{prefix}-chain").into(),
                chain_key,
                serde_json::json!({ "steps": 3 }).to_string(),
            )
            .await?;
        test_cx
            .schedule_job(
                format!("{prefix}-fails-once").into(),
                fails_key,
                serde_json::json!({ "key": format!("kv-{prefix}-fails-once") }).to_string(),
            )
            .await?;
        test_cx
            .schedule_job(
                format!("{prefix}-sleep").into(),
                sleep_key,
                serde_json::json!({ "millis": 20_u64 }).to_string(),
            )
            .await?;
    }
    test_cx.wait_until_no_active_jobs(60).await?;

    // 2. Message wait + effect
    for (prefix, recv_key) in [
        ("wasm", "recv_message_then_effect"),
        ("native", "native_recv_message_then_effect"),
    ] {
        let job_id: Arc<str> = format!("{prefix}-recv").into();
        test_cx
            .schedule_job(Arc::clone(&job_id), recv_key, "{}".to_string())
            .await?;
        test_cx
            .wait_until_entry(0, 30, |_id, entry| {
                matches!(
                    entry,
                    PartitionLogEntry::JobEffectResult(ev)
                        if ev.job_id == job_id && matches!(ev.result, JobRunResult::StepWait(_))
                )
            })
            .await?;
        test_cx
            .send_job_message(
                job_id,
                format!("msg-{prefix}").into(),
                serde_json::json!({"hello": prefix}).to_string(),
            )
            .await?;
    }
    test_cx.wait_until_no_active_jobs(60).await?;

    // 3. Defer compensation on terminal failure vs disarmed on success
    for (prefix, defer_key) in [
        ("wasm", "defer_on_terminal"),
        ("native", "native_defer_on_terminal"),
    ] {
        test_cx
            .schedule_job(
                format!("{prefix}-defer-ok").into(),
                defer_key,
                serde_json::json!({
                    "key": format!("kv-{prefix}-defer-ok"),
                    "fail": false,
                })
                .to_string(),
            )
            .await?;
        test_cx
            .schedule_job(
                format!("{prefix}-defer-fail").into(),
                defer_key,
                serde_json::json!({
                    "key": format!("kv-{prefix}-defer-fail"),
                    "fail": true,
                })
                .to_string(),
            )
            .await?;
    }
    test_cx.wait_until_no_active_jobs(60).await?;

    // 4. Nondeterministic replay detection across cold replay
    for (prefix, nd_key) in [
        ("wasm", "nondeterministic_replay"),
        ("native", "native_nondeterministic_replay"),
    ] {
        test_cx
            .schedule_job(
                format!("{prefix}-nd").into(),
                nd_key,
                serde_json::json!({ "key": format!("kv-{prefix}-nd") }).to_string(),
            )
            .await?;
    }
    test_cx.wait_until_no_active_jobs(60).await?;

    let snapshot = test_cx.get_partition_log_snapshot().await?;

    for suffix in ["chain", "fails-once", "sleep", "recv", "defer-ok"] {
        let wasm_summary = collect_job_summary(&snapshot, &format!("wasm-{suffix}"));
        let native_summary = collect_job_summary(&snapshot, &format!("native-{suffix}"));
        assert_eq!(
            wasm_summary, native_summary,
            "mismatch for workflow '{suffix}'"
        );
    }

    // Disarmed defers must not write to the keyvalue store.
    assert_eq!(
        test_cx.get_keyvalue("default", "kv-wasm-defer-ok").await,
        None
    );
    assert_eq!(
        test_cx.get_keyvalue("default", "kv-native-defer-ok").await,
        None
    );

    // Terminal failure runs defers in reverse registration order (B then A) as 2 additional journaled steps (1 forward + 2 defer = 3 step effects).
    assert_eq!(
        test_cx.get_keyvalue("default", "kv-wasm-defer-fail").await,
        Some(b"BA".to_vec())
    );
    assert_eq!(
        test_cx
            .get_keyvalue("default", "kv-native-defer-fail")
            .await,
        Some(b"BA".to_vec())
    );
    let (wasm_defer_steps, wasm_defer_waits, wasm_defer_term) =
        collect_job_summary(&snapshot, "wasm-defer-fail");
    let (native_defer_steps, native_defer_waits, native_defer_term) =
        collect_job_summary(&snapshot, "native-defer-fail");
    assert_eq!((wasm_defer_steps, wasm_defer_waits), (3, 0));
    assert_eq!((native_defer_steps, native_defer_waits), (3, 0));
    match (wasm_defer_term, native_defer_term) {
        (Some(TerminalSummary::TerminalErr(w)), Some(TerminalSummary::TerminalErr(n))) => {
            assert!(
                w.contains("boom after prepared"),
                "unexpected wasm err: {w}"
            );
            assert!(
                n.contains("boom after prepared"),
                "unexpected native err: {n}"
            );
        }
        other => panic!("expected terminal errors for defer-fail, got {other:?}"),
    }

    // Nondeterministic replay surfaces as Terminal error on step 0 on both hosts.
    let (wasm_nd_steps, _, wasm_nd_term) = collect_job_summary(&snapshot, "wasm-nd");
    let (native_nd_steps, _, native_nd_term) = collect_job_summary(&snapshot, "native-nd");
    assert_eq!(wasm_nd_steps, 1);
    assert_eq!(native_nd_steps, 1);
    match (wasm_nd_term, native_nd_term) {
        (Some(TerminalSummary::TerminalErr(w)), Some(TerminalSummary::TerminalErr(n))) => {
            assert!(
                w.contains("nondeterministic replay at step 0"),
                "unexpected wasm nd err: {w}"
            );
            assert!(
                n.contains("nondeterministic replay at step 0"),
                "unexpected native nd err: {n}"
            );
        }
        other => panic!("expected terminal errors for nondeterministic replay, got {other:?}"),
    }

    test_cx.stop().await?;
    Ok(())
}
