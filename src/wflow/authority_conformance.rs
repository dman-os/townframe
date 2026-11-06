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

/// The authority sees both adapters through its one canonical host seam.
struct ConformanceHost {
    wasm: Arc<WflowPlugin>,
    native: Arc<LocalNativeHost>,
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
    ) -> Res<Box<dyn Session>> {
        if self.wasm.contains_workflow(wflow_key) {
            <WflowPlugin as Host>::spawn(&*self.wasm, job_id, wflow_key, args_json, replay).await
        } else {
            <LocalNativeHost as Host>::spawn(&*self.native, job_id, wflow_key, args_json, replay)
                .await
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
    _wasm_host: Arc<WashHost>,
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
                WitInterface::from("townframe:daybook/drawer"),
                WitInterface::from("townframe:utils/llm-chat"),
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
    ];
    let wasm = Arc::new(WflowPlugin::new());
    let keyvalue = Arc::new(WasiKeyvalue::new());
    let engine = Engine::builder().build().to_eyre()?;
    let builder = HostBuilder::new()
        .with_engine(engine)
        .with_plugin(Arc::clone(&wasm) as Arc<dyn HostPlugin>)?
        .with_plugin(Arc::new(DynamicConfig::default()))?
        .with_plugin(keyvalue as Arc<dyn HostPlugin>)?;
    let wasm_host = builder.build().to_eyre()?.start().await.to_eyre()?;
    register_wasm_workload(&wasm_host, wasm_path()?, &wasm_keys).await?;

    let native = Arc::new(LocalNativeHost::new());
    native.register_typed("native_effect_chain", |cx, args: EffectChainArgs| {
        Box::pin(async move {
            for step in 0..args.steps {
                cx.effect(async || Ok(Json(step))).await?;
            }
            Ok(())
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
                let Json(_message) = cx.recv::<Json<serde_json::Value>>().await?;
                cx.effect(async || Ok(Json(serde_json::json!({"ok": true}))))
                    .await?;
                Ok(())
            })
        },
    );

    let host: Arc<dyn Host> = Arc::new(ConformanceHost { wasm, native });
    let store = MemoryStore::default();
    let runtime = AuthorityRuntime::start(store.writer()?, 0, host).await?;
    Ok(ConformanceHarness {
        runtime,
        _wasm_host: wasm_host,
    })
}

#[derive(Debug, PartialEq, Eq)]
struct ReceiptProjection {
    steps: u64,
    value: String,
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
    expected_steps: u64,
) -> Res<()> {
    let (_, wasm) = runtime
        .submit_handle(Submission::new(wasm_key, wasm_workflow, args))
        .await?;
    let (_, native) = runtime
        .submit_handle(Submission::new(native_key, native_workflow, args))
        .await?;
    let wasm_receipt = wasm
        .await_receipt()
        .await
        .ok_or_else(|| ferr!("missing durable receipt for {wasm_key}"))?;
    let native_receipt = native
        .await_receipt()
        .await
        .ok_or_else(|| ferr!("missing durable receipt for {native_key}"))?;
    assert_eq!(wasm.get_result().await?, native.get_result().await?);
    assert_eq!(project(&wasm_receipt), project(&native_receipt));
    assert_eq!(wasm_receipt.steps, expected_steps);
    assert_eq!(native_receipt.steps, expected_steps);
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
        3,
    )
    .await?;
    assert_pair(
        runtime,
        "wasm-sleep-effect",
        "native-sleep-effect",
        "sleep_then_effect",
        "native_sleep_then_effect",
        r#"{"millis":1}"#,
        2,
    )
    .await?;

    let (_, wasm_recv) = runtime
        .submit_handle(Submission::new(
            "wasm-receive-effect",
            "recv_message_then_effect",
            "{}",
        ))
        .await?;
    let (_, native_recv) = runtime
        .submit_handle(Submission::new(
            "native-receive-effect",
            "native_recv_message_then_effect",
            "{}",
        ))
        .await?;
    runtime
        .send_message("wasm-receive-effect", "wasm-message", r#"{"value":1}"#)
        .await?;
    runtime
        .send_message("native-receive-effect", "native-message", r#"{"value":1}"#)
        .await?;
    let wasm_receipt = wasm_recv.await_receipt().await.unwrap();
    let native_receipt = native_recv.await_receipt().await.unwrap();
    assert_eq!(
        wasm_recv.get_result().await?,
        native_recv.get_result().await?
    );
    assert_eq!(project(&wasm_receipt), project(&native_receipt));
    assert_eq!(wasm_receipt.steps, 2);
    assert_eq!(native_receipt.steps, 2);

    // Cancellation is permanent and aborts the in-flight session; the long
    // timer ensures this is not a completed sleep/effect invocation.
    let (_, wasm_cancel) = runtime
        .submit_handle(Submission::new(
            "wasm-abort",
            "sleep_then_effect",
            r#"{"millis":60000}"#,
        ))
        .await?;
    let (_, native_cancel) = runtime
        .submit_handle(Submission::new(
            "native-abort",
            "native_sleep_then_effect",
            r#"{"millis":60000}"#,
        ))
        .await?;
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

    runtime.stop().await;
    Ok(())
}
