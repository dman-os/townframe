use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use wash_plugin_wflow::WflowPlugin;
use wash_runtime::engine::Engine;
use wash_runtime::host::Host as WashHost;
use wash_runtime::host::HostApi;
use wash_runtime::host::HostBuilder;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::wasi_config::DynamicConfig;
use wash_runtime::types::Component;
use wash_runtime::types::Workload;
use wash_runtime::types::WorkloadStartRequest;
use wash_runtime::wit::WitInterface;
use wflow_core::kvstore::atomic::MemoryStore;
use wflow_tokio::authority_runtime::AuthorityRuntime;
use wflow_tokio::authority_runtime::Submission;
use wflow_tokio::authority_runtime::session::Host;
use wflow_tokio::authority_runtime::session::Session;
use wflow_tokio::local_native_host::LocalNativeHost;
use wflow_wdk::Json;

use crate::authority_conformance_provider::WasiKeyvalue;
use crate::interlude::*;

#[derive(Debug, Deserialize)]
struct EffectChainArgs {
    steps: u64,
}

#[derive(Debug, Deserialize)]
struct SleepThenEffectArgs {
    millis: u64,
}

#[derive(Debug, Deserialize)]
struct RecvMessageThenEffectArgs {}

#[derive(Debug, Deserialize)]
struct AuthorityProbeArgs {
    key: String,
    millis: u64,
}

/// The authority sees both adapters through its one canonical host seam.
struct ConformanceHost {
    wasm: Arc<WflowPlugin>,
    native: Arc<LocalNativeHost>,
    replay_gates: HashMap<&'static str, Arc<CommitReplyGate>>,
}

#[derive(Default)]
struct CommitReplyGate {
    held: std::sync::atomic::AtomicBool,
    committed: tokio::sync::Notify,
}

struct LostCommitReply {
    session: Box<dyn Session>,
    gate: Arc<CommitReplyGate>,
}

#[async_trait]
impl Session for LostCommitReply {
    async fn next_yield(&mut self) -> Option<wflow_wdk::session::SessionYield> {
        self.session.next_yield().await
    }

    fn resume(&self) -> bool {
        if self
            .gate
            .held
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return self.session.resume();
        }
        // Resume is invoked only after the step commits. Lose that first reply
        // while keeping the transport live until the runtime stops this activation.
        self.gate.committed.notify_one();
        true
    }

    fn deliver(&self, value_json: Arc<str>) -> bool {
        self.session.deliver(value_json)
    }
}

#[async_trait]
impl Host for ConformanceHost {
    fn contains_wflow(&self, key: &str) -> bool {
        self.wasm.contains_workflow(key) || self.native.contains_wflow(key)
    }

    async fn spawn(
        &self,
        job_id: Arc<str>,
        wflow_key: &str,
        args_json: String,
        replay: Vec<wflow_wdk::session::ReplayStep>,
    ) -> Result<Box<dyn Session>, wflow_tokio::authority_runtime::session::SpawnError> {
        let gate = self.replay_gates.get(job_id.as_ref()).map(Arc::clone);
        // Both adapters classify their own failures, so the conformance host
        // only has to decide which adapter to ask; forwarding the class keeps
        // the parity test on the same durable outcomes as production.
        let session = if self.wasm.contains_workflow(wflow_key) {
            <WflowPlugin as Host>::spawn(&*self.wasm, job_id, wflow_key, args_json, replay).await?
        } else {
            <LocalNativeHost as Host>::spawn(&*self.native, job_id, wflow_key, args_json, replay)
                .await?
        };
        match gate {
            Some(gate) => Ok(Box::new(LostCommitReply { session, gate })),
            None => Ok(session),
        }
    }

    fn forget_job(&self, job_id: &str) {
        <WflowPlugin as Host>::forget_job(&*self.wasm, job_id);
        <LocalNativeHost as Host>::forget_job(&*self.native, job_id);
    }
}

struct ConformanceHarness {
    runtime: AuthorityRuntime,
    // The Wash host owns the real resolved component workload and its engine.
    wasm_host: Arc<WashHost>,
    store: MemoryStore,
    host: Arc<ConformanceHost>,
}

fn wasm_path() -> Res<PathBuf> {
    let path = std::env::var_os("TEST_WFLOWS_WASM")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .map(|target| target.join("wasm32-wasip3/release/test_wflows.wasm"))
        })
        .ok_or_else(|| {
            ferr!(
                "test_wflows.wasm path is unset; set TEST_WFLOWS_WASM or external CARGO_TARGET_DIR"
            )
        })?;
    eyre::ensure!(
        path.is_file(),
        "{} not found; build with `cargo build --locked --manifest-path src/wasm_utils_rs/Cargo.toml --release --target wasm32-wasip3 -p test_wflows`",
        path.display()
    );
    Ok(path)
}

async fn register_wasm_workload(host: &WashHost, path: PathBuf, keys: &[&str]) -> Res<()> {
    let bytes = tokio::fs::read(path).await?;
    let request = WorkloadStartRequest {
        workload_id: "authority-conformance".to_owned(),
        workload: Workload {
            namespace: "test".to_owned(),
            name: "authority-conformance".to_owned(),
            annotations: Default::default(),
            service: None,
            components: vec![Component {
                bytes: bytes.into(),
                ..Default::default()
            }],
            host_interfaces: vec![
                WitInterface {
                    config: [("wflow_keys".to_owned(), keys.join(","))].into(),
                    ..WitInterface::from("townframe:wflow/bundle")
                },
                WitInterface::from("wasi:keyvalue/store"),
            ],
            volumes: vec![],
        },
    };
    host.workload_start(request).await.to_eyre()?;
    Ok(())
}

async fn setup() -> Res<ConformanceHarness> {
    let wasm_keys = [
        "effect_chain",
        "sleep_then_effect",
        "recv_message_then_effect",
        "authority_probe",
    ];
    let wasm = Arc::new(WflowPlugin::new());
    let keyvalue = Arc::new(WasiKeyvalue::new());
    let engine = Engine::builder().build().to_eyre()?;
    let builder = HostBuilder::new()
        .with_engine(engine)
        .with_plugin(Arc::clone(&wasm) as Arc<dyn HostPlugin>)
        .to_eyre()?
        .with_plugin(Arc::new(DynamicConfig::default()))
        .to_eyre()?
        .with_plugin(keyvalue as Arc<dyn HostPlugin>)
        .to_eyre()?;
    let wasm_host = builder.build().to_eyre()?.start().await.to_eyre()?;
    register_wasm_workload(&wasm_host, wasm_path()?, &wasm_keys).await?;

    let native = Arc::new(LocalNativeHost::new());
    native.register_typed("native_effect_chain", |cx, args: EffectChainArgs| {
        Box::pin(async move {
            let mut values = Vec::new();
            for step in 0..args.steps {
                values.push(cx.effect(async || Ok(Json(step))).await?);
            }
            Ok(values)
        })
    });
    native.register_typed(
        "native_sleep_then_effect",
        |cx, args: SleepThenEffectArgs| {
            Box::pin(async move {
                cx.sleep(std::time::Duration::from_millis(args.millis))
                    .await?;
                cx.effect(async || Ok(Json(serde_json::json!({"slept": true}))))
                    .await?;
                Ok(())
            })
        },
    );
    native.register_typed(
        "native_recv_message_then_effect",
        |cx, _args: RecvMessageThenEffectArgs| {
            Box::pin(async move {
                let Json(message) = cx.recv::<Json<serde_json::Value>>().await?;
                cx.effect(async || Ok(Json(serde_json::json!({"ok": true}))))
                    .await?;
                Ok(message)
            })
        },
    );

    let counters = Arc::new(std::sync::Mutex::new(HashMap::<String, u64>::new()));
    native.register_typed(
        "native_authority_probe",
        move |cx, args: AuthorityProbeArgs| {
            let counters = Arc::clone(&counters);
            Box::pin(async move {
                let count: u64 = cx
                    .effect(async || {
                        let mut counters = counters.lock().expect("probe counter poisoned");
                        let count = counters.entry(args.key).or_default();
                        *count += 1;
                        Ok(Json(*count))
                    })
                    .await?;
                let Json(message) = cx.recv::<Json<String>>().await?;
                cx.sleep(Duration::from_millis(args.millis)).await?;
                cx.effect(async || Ok(Json(format!("{count}:{message}"))))
                    .await
            })
        },
    );

    let host = Arc::new(ConformanceHost {
        wasm,
        native,
        replay_gates: ["wasm-replay", "native-replay"]
            .into_iter()
            .map(|key| (key, Arc::new(CommitReplyGate::default())))
            .collect(),
    });
    let store = MemoryStore::default();
    let runtime =
        AuthorityRuntime::start(store.writer()?, 0, Arc::clone(&host) as Arc<dyn Host>).await?;
    Ok(ConformanceHarness {
        runtime,
        wasm_host,
        store,
        host,
    })
}

#[derive(Debug, PartialEq, Eq)]
struct ReceiptProjection {
    steps: u64,
    value: String,
}

impl ReceiptProjection {
    fn success(steps: u64, value_json: &str) -> Self {
        let result: wflow_wdk::types::JobResult = Ok(value_json.to_owned());
        Self {
            steps,
            value: serde_json::to_string(&result).unwrap(),
        }
    }
}

fn project(receipt: &wflow_core::partition::authority::Receipt) -> ReceiptProjection {
    let value = match &receipt.outcome {
        wflow_core::partition::authority::Outcome::Completed(terminal) => terminal.value.clone(),
        wflow_core::partition::authority::Outcome::Faulted(fault) => {
            format!("fault:{:?}", fault.failure)
        }
    };
    ReceiptProjection {
        steps: receipt.steps,
        value,
    }
}

async fn assert_pair(
    runtime: &AuthorityRuntime,
    wasm_key: &str,
    native_key: &str,
    wasm_workflow: &str,
    native_workflow: &str,
    args: &str,
    expected: ReceiptProjection,
) -> Res<()> {
    let (_, wasm) = runtime
        .submit_handle(Submission::new(wasm_key, wasm_workflow, args))
        .await
        .unwrap();
    let (_, native) = runtime
        .submit_handle(Submission::new(native_key, native_workflow, args))
        .await
        .unwrap();
    let wasm_receipt = wasm
        .await_receipt()
        .await
        .ok_or_else(|| ferr!("missing durable receipt for {wasm_key}"))?;
    let native_receipt = native
        .await_receipt()
        .await
        .ok_or_else(|| ferr!("missing durable receipt for {native_key}"))?;
    let expected_result = serde_json::from_str::<wflow_wdk::types::JobResult>(&expected.value)?;
    assert_eq!(wasm.get_result().await?.as_ref(), Some(&expected_result));
    assert_eq!(native.get_result().await?.as_ref(), Some(&expected_result));
    assert_eq!(project(&wasm_receipt), expected);
    assert_eq!(project(&native_receipt), expected);
    Ok(())
}

async fn assert_cold_replay(harness: &ConformanceHarness) -> Res<()> {
    for (key, workflow) in [
        ("wasm-replay", "authority_probe"),
        ("native-replay", "native_authority_probe"),
    ] {
        harness
            .runtime
            .submit(Submission::new(
                key,
                workflow,
                r#"{"key":"replay-counter","millis":1}"#,
            ))
            .await
            .unwrap();
    }
    for gate in harness.host.replay_gates.values() {
        gate.committed.notified().await;
    }
    for key in ["wasm-replay", "native-replay"] {
        assert!(harness.runtime.get_result(key).await?.is_none());
    }
    // Both first steps are durable, but neither workflow received its reply.
    // Reopening must reconstruct fresh activations from that committed prefix.
    harness.runtime.stop().await;
    let runtime = AuthorityRuntime::start(
        harness.store.writer()?,
        0,
        Arc::clone(&harness.host) as Arc<dyn Host>,
    )
    .await?;
    for key in ["wasm-replay", "native-replay"] {
        runtime
            .send_message(key, "after-reopen", r#""after-reopen""#)
            .await
            .unwrap();
    }
    // The external counters survive the actor restart. Rerunning the first
    // effect would return 2, not the committed 1, even if all steps were reset.
    assert_pair(
        &runtime,
        "wasm-replay",
        "native-replay",
        "authority_probe",
        "native_authority_probe",
        r#"{"key":"replay-counter","millis":1}"#,
        ReceiptProjection::success(4, r#""1:after-reopen""#),
    )
    .await?;
    runtime.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn authority_native_and_wasm_conformance() -> Res<()> {
    let harness = setup().await?;
    let runtime = &harness.runtime;

    assert_pair(
        runtime,
        "wasm-effect-chain",
        "native-effect-chain",
        "effect_chain",
        "native_effect_chain",
        r#"{"steps":3}"#,
        ReceiptProjection::success(3, "[0,1,2]"),
    )
    .await?;
    assert_pair(
        runtime,
        "wasm-sleep-effect",
        "native-sleep-effect",
        "sleep_then_effect",
        "native_sleep_then_effect",
        r#"{"millis":1}"#,
        ReceiptProjection::success(2, "null"),
    )
    .await?;

    let (_, wasm_recv) = runtime
        .submit_handle(Submission::new(
            "wasm-receive-effect",
            "recv_message_then_effect",
            "{}",
        ))
        .await
        .unwrap();
    let (_, native_recv) = runtime
        .submit_handle(Submission::new(
            "native-receive-effect",
            "native_recv_message_then_effect",
            "{}",
        ))
        .await
        .unwrap();
    runtime
        .send_message("wasm-receive-effect", "wasm-message", r#"{"value":1}"#)
        .await
        .unwrap();
    runtime
        .send_message("native-receive-effect", "native-message", r#"{"value":1}"#)
        .await
        .unwrap();
    let wasm_receipt = wasm_recv.await_receipt().await.unwrap();
    let native_receipt = native_recv.await_receipt().await.unwrap();
    assert_eq!(
        wasm_recv.get_result().await?,
        Some(Ok(r#"{"value":1}"#.to_owned()))
    );
    assert_eq!(
        wasm_recv.get_result().await?,
        native_recv.get_result().await?
    );
    let expected = ReceiptProjection::success(2, r#"{"value":1}"#);
    assert_eq!(project(&wasm_receipt), expected);
    assert_eq!(project(&native_receipt), expected);

    // A receive with no delivered message cannot complete before cancellation;
    // this exercises permanent abort without relying on a long timer.
    let (_, wasm_cancel) = runtime
        .submit_handle(Submission::new(
            "wasm-abort",
            "recv_message_then_effect",
            "{}",
        ))
        .await
        .unwrap();
    let (_, native_cancel) = runtime
        .submit_handle(Submission::new(
            "native-abort",
            "native_recv_message_then_effect",
            "{}",
        ))
        .await
        .unwrap();
    runtime
        .cancel("wasm-abort", "conformance abort")
        .await
        .unwrap();
    runtime
        .cancel("native-abort", "conformance abort")
        .await
        .unwrap();
    let wasm_cancel_receipt = wasm_cancel.await_receipt().await.unwrap();
    let native_cancel_receipt = native_cancel.await_receipt().await.unwrap();
    assert_eq!(
        wasm_cancel.get_result().await?,
        native_cancel.get_result().await?
    );
    assert_eq!(
        project(&wasm_cancel_receipt),
        project(&native_cancel_receipt)
    );

    assert!(matches!(wasm_cancel.get_result().await?, Some(Err(_))));
    for receipt in [&wasm_cancel_receipt, &native_cancel_receipt] {
        assert!(matches!(
            &receipt.outcome,
            wflow_core::partition::authority::Outcome::Faulted(fault)
                if matches!(&fault.failure, wflow_core::partition::JobFailure::Cancelled { reason }
                    if reason == "conformance abort")
        ));
    }
    for key in ["wasm-abort", "native-abort"] {
        assert_eq!(
            runtime.cancel(key, "late cancellation").await.unwrap_err(),
            wflow_tokio::authority_runtime::CancelError::AlreadyTerminal
        );
        assert_eq!(
            runtime.send_message(key, "late", "null").await.unwrap_err(),
            wflow_tokio::authority_runtime::SendError::AlreadyTerminal
        );
    }

    assert_cold_replay(&harness).await?;
    Arc::clone(&harness.wasm_host).stop().await.to_eyre()?;
    Ok(())
}
