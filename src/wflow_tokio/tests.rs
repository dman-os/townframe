use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;
use wflow_core::r#gen::metastore::PartitionsMeta;
use wflow_core::r#gen::metastore::WasmcloudWflowServiceMeta;
use wflow_core::r#gen::metastore::WflowMeta;
use wflow_core::r#gen::metastore::WflowServiceMeta;
use wflow_core::kvstore::CasError;
use wflow_core::kvstore::CasGuard;
use wflow_core::kvstore::KvStore;
use wflow_core::partition::job_events;
use wflow_core::partition::log::PartitionLogEntry;
use wflow_core::partition::state;
use wflow_wdk::JobErrorX;
use wflow_wdk::Json;
use wflow_wdk::route_wflows;

use crate::interlude::*;
use crate::local_native_host::LocalNativeHost;
use crate::partition::PartitionCtx;
use crate::partition::PartitionLogRef;
use crate::partition::service;
use crate::partition::start_tokio_worker;
use crate::partition::state::PartitionWorkingState;

type KvEntries = BTreeMap<Arc<[u8]>, Arc<[u8]>>;

#[derive(Default, Clone)]
struct BTreeKvStore {
    inner: Arc<Mutex<KvEntries>>,
}

impl BTreeKvStore {
    fn make_cas_guard(&self, key: Arc<[u8]>, snapshot: Option<Arc<[u8]>>) -> CasGuard {
        let current_snapshot = snapshot.clone();
        let current_cb = move || current_snapshot.clone();
        let store = self.clone();
        let swap_cb = move |value: Arc<[u8]>| -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>> {
            let store = store.clone();
            let key = Arc::clone(&key);
            let snapshot = snapshot.clone();
            Box::pin(async move {
                let mut map = store.inner.lock().expect(ERROR_MUTEX);
                let current = map.get(&key).cloned();
                if current.as_deref() == snapshot.as_deref() {
                    map.insert(key, value);
                    Ok(Ok(()))
                } else {
                    drop(map);
                    Ok(Err(CasError::CasFailed(store.make_cas_guard(key, current))))
                }
            })
        };
        CasGuard::new(current_cb, swap_cb)
    }
}

#[async_trait]
impl KvStore for BTreeKvStore {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.inner.lock().expect(ERROR_MUTEX).get(key).cloned())
    }

    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        Ok(self.inner.lock().expect(ERROR_MUTEX).insert(key, value))
    }

    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.inner.lock().expect(ERROR_MUTEX).remove(key))
    }

    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        let mut map = self.inner.lock().expect(ERROR_MUTEX);
        let key: Arc<[u8]> = key.into();
        let next = if let Some(existing) = map.get(&key) {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(existing.as_ref());
            i64::from_le_bytes(buf)
                .checked_add(delta)
                .expect("increment overflow")
        } else {
            delta
        };
        map.insert(key, next.to_le_bytes().into());
        Ok(next)
    }

    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        let key_arc: Arc<[u8]> = key.into();
        let current = self.inner.lock().expect(ERROR_MUTEX).get(&key_arc).cloned();
        Ok(self.make_cas_guard(key_arc, current))
    }
}

struct UnusedWasmHost;

#[async_trait]
impl service::WflowServiceHost for UnusedWasmHost {
    type ExtraArgs = WasmcloudWflowServiceMeta;

    async fn run(
        &self,
        _ctx: &service::RunJobCtx,
        _job_id: Arc<str>,
        _journal: state::JobState,
        _session: Option<Box<dyn service::WflowServiceSession>>,
        _cancel_token: CancellationToken,
        _args: &Self::ExtraArgs,
    ) -> service::RunJobReply {
        service::RunJobReply {
            result: Err(job_events::JobRunResult::WorkerErr(
                job_events::JobRunWorkerError::WflowNotFound,
            )),
            session: None,
        }
    }
}

async fn wait_for_archived_job(
    working_state: &PartitionWorkingState,
    job_id: &str,
) -> state::JobState {
    loop {
        {
            let jobs = working_state.read_jobs().await;
            if let Some(done) = jobs.archive.get(job_id) {
                return done.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn partition_worker_executes_local_native_host_workflows() -> Res<()> {
    let meta_kv = Arc::new(BTreeKvStore::default());
    let log_kv = Arc::new(BTreeKvStore::default());
    let snap_kv = Arc::new(BTreeKvStore::default());

    let metastore = Arc::new(
        wflow_core::kvstore::metastore::KvStoreMetadtaStore::new(
            meta_kv as Arc<dyn KvStore + Send + Sync>,
            PartitionsMeta {
                version: "0".into(),
                partition_count: 1,
            },
        )
        .await?,
    );
    let logstore = Arc::new(wflow_core::kvstore::log::KvStoreLog::new(log_kv).await?);
    let snapstore = Arc::new(wflow_core::kvstore::snapstore::KvSnapStore::new(snap_kv));

    let native_host = Arc::new(LocalNativeHost::new());
    let step0_count = Arc::new(AtomicUsize::new(0));
    let step1_attempts = Arc::new(AtomicUsize::new(0));
    let defer_order = Arc::new(Mutex::new(Vec::<String>::new()));

    let s0 = Arc::clone(&step0_count);
    let s1 = Arc::clone(&step1_attempts);
    let defers = Arc::clone(&defer_order);
    native_host.register_bundle(
        ["synthetic-native", "compensated-failure"],
        move |args, backend| {
            let s0 = Arc::clone(&s0);
            let s1 = Arc::clone(&s1);
            let defers = Arc::clone(&defers);
            Box::pin(async move {
                let local_cell = Rc::new(Cell::new(100i64));
                route_wflows!(args, backend, {
                    "synthetic-native" => |cx, base: i64| async move {
                        let a: i64 = cx
                            .named_effect("step-0", || {
                                let s0 = Arc::clone(&s0);
                                let local_cell = Rc::clone(&local_cell);
                                async move {
                                    s0.fetch_add(1, Ordering::SeqCst);
                                    local_cell.set(local_cell.get() + base);
                                    Ok(Json(local_cell.get()))
                                }
                            })
                            .await?;
                        cx.named_sleep("nap", Duration::from_millis(15)).await?;
                        let Json(msg): Json<i64> = cx.named_recv("inbox").await?;
                        let b: i64 = cx
                            .named_effect("flaky-step", || {
                                let s1 = Arc::clone(&s1);
                                async move {
                                    let attempt = s1.fetch_add(1, Ordering::SeqCst);
                                    if attempt == 0 {
                                        return Err(JobErrorX::Transient(ferr!("try again")));
                                    }
                                    Ok(Json(a + msg))
                                }
                            })
                            .await?;
                        Ok(b)
                    },
                    "compensated-failure" => |cx, label: String| async move {
                        let d1 = Arc::clone(&defers);
                        let l1 = format!("{label}-cleanup-1");
                        cx.defer("cleanup-1", move || async move {
                            d1.lock().expect(ERROR_MUTEX).push(l1);
                            Ok(())
                        })?;
                        let d2 = Arc::clone(&defers);
                        let l2 = format!("{label}-cleanup-2");
                        cx.defer("cleanup-2", move || async move {
                            d2.lock().expect(ERROR_MUTEX).push(l2);
                            Ok(())
                        })?;
                        let _acquired: String = cx
                            .named_effect("acquire", || async { Ok(Json("ok".to_owned())) })
                            .await?;
                        Err::<String, JobErrorX>(JobErrorX::Terminal(ferr!("abort after acquire")))
                    },
                })
                .await
            })
        },
    );

    let pcx = PartitionCtx::new(
        0,
        metastore,
        Arc::clone(&logstore) as Arc<dyn wflow_core::log::LogStore>,
        0,
        Arc::new(UnusedWasmHost),
        native_host,
    );
    let working_state = Arc::new(PartitionWorkingState::new(
        0,
        state::PartitionJobsState::default(),
        default(),
    ));
    let worker = start_tokio_worker(pcx.clone(), Arc::clone(&working_state), snapstore).await;
    let mut log_ref = PartitionLogRef::new(logstore);

    log_ref
        .append(&PartitionLogEntry::JobInit(job_events::JobInitEvent {
            job_id: Arc::from("native-job-1"),
            timestamp: Timestamp::now(),
            args_json: Arc::from("23"),
            override_wflow_retry_policy: None,
            wflow: WflowMeta {
                key: "synthetic-native".into(),
                service: WflowServiceMeta::LocalNative,
            },
        }))
        .await?;
    log_ref
        .append(&PartitionLogEntry::JobMessage(
            job_events::JobMessageEvent {
                job_id: Arc::from("native-job-1"),
                message_id: Arc::from("msg-1"),
                timestamp: Timestamp::now(),
                payload_json: Arc::from("7"),
            },
        ))
        .await?;

    let archived = wait_for_archived_job(&working_state, "native-job-1").await;
    let last_run = archived.runs.last().expect("job must have runs");
    assert!(
        matches!(&last_run.result, job_events::JobRunResult::Success { value_json } if value_json.as_ref() == "130"),
        "unexpected final run result: {:?}",
        last_run.result
    );
    assert_eq!(
        step0_count.load(Ordering::SeqCst),
        1,
        "step 0 must not re-run across sleep, message, or transient retry of step 3"
    );
    assert_eq!(step1_attempts.load(Ordering::SeqCst), 2);

    log_ref
        .append(&PartitionLogEntry::JobInit(job_events::JobInitEvent {
            job_id: Arc::from("native-job-defer"),
            timestamp: Timestamp::now(),
            args_json: Arc::from(r#""res""#),
            override_wflow_retry_policy: None,
            wflow: WflowMeta {
                key: "compensated-failure".into(),
                service: WflowServiceMeta::LocalNative,
            },
        }))
        .await?;

    let archived_defer = wait_for_archived_job(&working_state, "native-job-defer").await;
    let defer_last = archived_defer
        .runs
        .last()
        .expect("defer job must have runs");
    assert!(matches!(
        &defer_last.result,
        job_events::JobRunResult::WflowErr(job_events::JobError::Terminal { .. })
    ));
    assert_eq!(
        &*defer_order.lock().expect(ERROR_MUTEX),
        &["res-cleanup-2".to_owned(), "res-cleanup-1".to_owned()]
    );
    assert_eq!(
        archived_defer.steps.len(),
        3,
        "acquire + 2 reverse-order defers must all be journaled as steps"
    );

    worker.stop().await?;
    Ok(())
}
