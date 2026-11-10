//! Initializers as a lifecycle binding (ledger D7): gating, run modes, failure and retry,
//! idempotency keys, the dispatch path of foreign targets and stale results, against the fake
//! executor with gated providers (no Wasm, no sleeps).

use super::Node;
use super::dispatch::not_ready;
use super::executor::FakeExecutor;
use super::id;
use super::lexicon;
use async_trait::async_trait;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::dmeta::Uuid7;
use big_cartridges_core::Artifact;
use big_cartridges_core::Cartridge;
use big_cartridges_core::Contribution;
use big_cartridges_core::Digest;
use big_cartridges_core::NoduleContribution;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::request::Necessity;
use big_cartridges_core::request::Request;
use big_dispatch::Caller;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_nodule_core::Handler;
use big_nodule_core::Initializer;
use big_nodule_core::Nodule;
use big_nodule_core::RunMode;
use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::NoduleInitStatus;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::SetNoduleDrawerGrants;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::SetNoduleOperationGrants;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::watch;

const ALPHA: &str = example_cartridge!("alpha");
pub(super) const PING: &str = "org.example.alpha.ping";
const SETUP: &str = "org.example.alpha.setup";
const WARM: &str = "org.example.alpha.warm";
const REMOTE: &str = "org.example.remote.sync";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Call {
    operation: String,
    idempotency_key: Option<String>,
    actor: Uuid7,
}

/// Records every call and can hold or fail them: the provider behind a nodule's handler, or a
/// native provider of a foreign operation.
pub(super) struct Probe {
    calls: Mutex<Vec<Call>>,
    /// How many calls have been entered, for a test to wait on without polling.
    entered: watch::Sender<usize>,
    /// Gates for the next calls, consumed in order: a call that finds one waits for its permit.
    holds: Mutex<VecDeque<Arc<Notify>>>,
    failing: Mutex<BTreeSet<String>>,
}

impl Probe {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::default(),
            entered: watch::channel(0).0,
            holds: Mutex::default(),
            failing: Mutex::default(),
        })
    }

    /// The next call that has not started yet blocks until the returned gate is notified.
    fn hold(&self) -> Arc<Notify> {
        let gate = Arc::new(Notify::new());
        self.holds.lock().unwrap().push_back(Arc::clone(&gate));
        gate
    }

    fn fail(&self, operation: &str, failing: bool) {
        let mut set = self.failing.lock().unwrap();
        if failing {
            set.insert(operation.to_owned());
        } else {
            set.remove(operation);
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn operations(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .map(|call| call.operation)
            .collect()
    }

    async fn entered(&self, count: usize) {
        self.entered
            .subscribe()
            .wait_for(|entered| *entered >= count)
            .await
            .unwrap();
    }
}

#[async_trait]
impl Provider for Probe {
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        _request: &RequestContext,
        context: &InvocationContext,
        _input: Value,
    ) -> Result<Value, ProviderError> {
        self.calls.lock().unwrap().push(Call {
            operation: operation.to_owned(),
            idempotency_key: context.idempotency_key.clone(),
            actor: caller.actor,
        });
        self.entered.send_modify(|entered| *entered += 1);
        let hold = self.holds.lock().unwrap().pop_front();
        if let Some(gate) = hold {
            gate.notified().await;
        }
        if self.failing.lock().unwrap().contains(operation) {
            return Err(ProviderError::Failed {
                status: 500,
                body: json!({ "error": "boom" }),
            });
        }
        Ok(json!({}))
    }
}

fn query(nsid: &str) -> serde_json::Map<String, Value> {
    json!({
        "lexicon": 1,
        "id": nsid,
        "defs": { "main": {
            "type": "query",
            "parameters": { "type": "params", "properties": {} },
            "output": {
                "encoding": "application/json",
                "schema": { "type": "object", "properties": {} }
            }
        }}
    })
    .as_object()
    .unwrap()
    .clone()
}

/// A package `<example did>/<name>` whose nodule `org.example.<name>` serves `serves` (all under its
/// authority), requires `requires`, and declares `inits` as `(key, target, mode)` in order.
/// Each `version` has a handler of its own, so each is a revision of its own.
pub(super) fn init_cartridge(
    name: &str,
    version: &str,
    serves: &[&str],
    requires: &[&str],
    inits: &[(&str, &str, RunMode)],
) -> Cartridge {
    let mut nodule = Nodule::default();
    for nsid in serves {
        nodule.lexicons.push(lexicon(Value::Object(query(nsid))));
    }
    nodule.handler = Some(Handler {
        artifact: "handler".into(),
        extra: Default::default(),
    });
    nodule.implements = serves.iter().map(|nsid| (*nsid).to_owned()).collect();
    // Optional, so the nodule stays runnable before its grant: these tests exercise how an
    // initializer waits on and calls a provider, including the dispatch refusal without a grant.
    // A required request holds the nodule at `NeedsConfiguration` instead (see `requests`).
    nodule.requires = requires
        .iter()
        .map(|nsid| {
            let mut request = Request::new(*nsid, *nsid);
            request.necessity = Necessity::Optional;
            request
        })
        .collect();
    nodule.initialization = inits
        .iter()
        .map(|(key, target, mode)| Initializer {
            key: (*key).to_owned(),
            target: (*target).to_owned(),
            mode: mode.clone(),
            extra: Default::default(),
        })
        .collect();
    let bytes = format!("{name}-{version}");
    let mut cartridge = Cartridge::new(
        big_cartridges_core::test_support::cartridge_id(name),
        version.parse().unwrap(),
    );
    cartridge.authority = Some(format!("org.example.{name}"));
    cartridge.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest::of(bytes.as_bytes()),
        size: Some(bytes.len() as u64),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    cartridge
        .contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: lexicon(serde_json::to_value(nodule).unwrap()),
            extra: Default::default(),
        }));
    cartridge
}

/// Alpha with a `setup` initializer in `mode` and its provider already hosted.
async fn alpha_node(mode: RunMode) -> (Node, Arc<Probe>) {
    let probe = Probe::new();
    let node = node_hosting(&probe).await;
    node.admit(
        node.operator(),
        init_cartridge(
            "alpha",
            "1.0.0",
            &[PING, SETUP],
            &[],
            &[("setup", SETUP, mode)],
        ),
    )
    .await
    .unwrap();
    (node, probe)
}

pub(super) async fn node_hosting(probe: &Arc<Probe>) -> Node {
    super::node_with(Some(FakeExecutor::hosting(
        Arc::clone(probe) as Arc<dyn Provider>,
        true,
    )))
    .await
}

fn initializing() -> DispatchError {
    not_ready(NotReadyReason::Provider(Readiness::Initializing))
}

impl Node {
    /// Reads `cartridge_id` until `readiness` shows. Every read is a round trip through the state
    /// actor, which handles an initializer's result as a message of its own, so this ends once
    /// the awaited result has been applied; it waits on no clock.
    async fn until(&self, cartridge_id: &str, readiness: Readiness) -> NoduleRecord {
        loop {
            let record = self.get(self.operator(), cartridge_id).await.unwrap();
            if record.readiness == readiness {
                return record;
            }
            tokio::task::yield_now().await;
        }
    }

    /// A config change that touches nothing: every config mutation reconciles once, which is
    /// what retries a failed initializer.
    async fn touch(&self, cartridge_id: &str) -> NoduleRecord {
        self.call(
            self.operator(),
            SetNoduleDrawerGrants {
                cartridge_id: id(cartridge_id),
                drawers: BTreeMap::new(),
            },
        )
        .await
        .unwrap()
    }

    pub(super) async fn set_enabled_record(&self, cartridge_id: &str) -> NoduleRecord {
        self.call(
            self.operator(),
            SetNoduleEnabled {
                cartridge_id: id(cartridge_id),
                enabled: true,
            },
        )
        .await
        .unwrap()
    }

    async fn list_records(&self) -> Vec<NoduleRecord> {
        self.call(self.operator(), ListNodules {})
            .await
            .unwrap()
            .nodules
    }

    pub(super) async fn grant_operations(
        &self,
        cartridge_id: &str,
        authorities: &[&str],
    ) -> NoduleRecord {
        self.call(
            self.operator(),
            SetNoduleOperationGrants {
                cartridge_id: id(cartridge_id),
                operations: DispatchScope {
                    authorities: authorities.iter().map(|a| (*a).to_owned()).collect(),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn calls_are_not_ready_while_an_initializer_runs_and_served_once_it_succeeded() {
    let (node, probe) = alpha_node(RunMode::Once).await;
    let operator = node.operator();
    let gate = probe.hold();

    node.set_enabled(ALPHA, true).await;
    probe.entered(1).await;
    // The initializer is running: nothing queues behind it and the handler is not entered.
    assert_eq!(
        node.invoke(operator, PING, json!({})).await,
        Err(initializing())
    );
    assert_eq!(probe.operations(), [SETUP]);
    let record = node.get(operator, ALPHA).await.unwrap();
    assert_eq!(record.readiness, Readiness::Initializing);
    assert_eq!(
        record.init,
        Some(NoduleInitStatus {
            attempts: 1,
            error: None,
            waiting_on: None,
        })
    );
    assert_eq!(
        node.dispatch.router.resolve(PING).unwrap().state,
        ProviderState::NotReady(Readiness::Initializing)
    );
    assert_eq!(
        node.list_records().await[0].readiness,
        Readiness::Initializing
    );

    gate.notify_one();
    let record = node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(record.init, None);
    assert!(matches!(
        node.dispatch.router.resolve(PING).unwrap().state,
        ProviderState::Ready(ProviderKind::Executor { kind: "fake", .. })
    ));
    node.invoke(operator, PING, json!({})).await.unwrap();
    assert_eq!(probe.operations(), [SETUP, PING]);
    node.shutdown().await;
}

#[tokio::test]
async fn once_runs_once_per_node_revision_and_every_start_runs_on_each_start() {
    let probe = Probe::new();
    let mut node = node_hosting(&probe).await;
    let operator = node.operator();
    let inits = [
        ("seed", SETUP, RunMode::Once),
        ("warm", WARM, RunMode::EveryStart),
    ];
    node.admit(
        operator,
        init_cartridge("alpha", "1.0.0", &[PING, SETUP, WARM], &[], &inits),
    )
    .await
    .unwrap();

    // Declaration order, not completion order of anything else.
    node.set_enabled(ALPHA, true).await;
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(probe.operations(), [SETUP, WARM]);

    // A node restart is a start: `EveryStart` runs again, `Once` does not.
    node.restart_nodules().await.unwrap();
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(probe.operations(), [SETUP, WARM, WARM]);

    // So is enabling after a disable; the `Once` record survives it.
    node.set_enabled(ALPHA, false).await;
    node.set_enabled(ALPHA, true).await;
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(probe.operations(), [SETUP, WARM, WARM, WARM]);

    // Reaching Ready means no initializer was left pending, so the count is final. A new
    // revision is a new `Once`.
    node.admit(
        operator,
        init_cartridge("alpha", "1.1.0", &[PING, SETUP, WARM], &[], &inits),
    )
    .await
    .unwrap();
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(probe.operations(), [SETUP, WARM, WARM, WARM, SETUP, WARM]);

    // ... and that revision's `Once` survives a restart.
    node.restart_nodules().await.unwrap();
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(
        probe.operations(),
        [SETUP, WARM, WARM, WARM, SETUP, WARM, WARM]
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_failing_initializer_is_reported_and_retried_on_the_next_config_change() {
    let (node, probe) = alpha_node(RunMode::Once).await;
    let operator = node.operator();
    probe.fail(SETUP, true);

    node.set_enabled(ALPHA, true).await;
    let record = node.until(ALPHA, Readiness::InitFailed).await;
    let init = record.init.unwrap();
    assert_eq!(init.attempts, 1);
    let error = init.error.unwrap();
    assert!(error.contains("500") && error.contains("boom"), "{error}");
    assert_eq!(
        node.invoke(operator, PING, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(Readiness::InitFailed)))
    );
    assert_eq!(
        node.list_records().await[0].readiness,
        Readiness::InitFailed
    );

    // Nothing retries on its own: reads and the failure's own result invoke nothing.
    node.get(operator, ALPHA).await.unwrap();
    assert_eq!(probe.operations(), [SETUP]);

    // The next config change retries it, and the last error is kept while it runs.
    probe.fail(SETUP, false);
    let gate = probe.hold();
    let record = node.touch(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Initializing);
    let init = record.init.unwrap();
    assert_eq!(init.attempts, 2);
    assert_eq!(init.error, Some(error));
    gate.notify_one();
    let record = node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(record.init, None);
    assert_eq!(probe.operations(), [SETUP, SETUP]);
    node.shutdown().await;
}

#[tokio::test]
async fn the_idempotency_key_names_the_cartridge_revision_and_initializer() {
    let (node, probe) = alpha_node(RunMode::EveryStart).await;
    let record = node.set_enabled_record(ALPHA).await;
    let revision = record.revision.unwrap();
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(
        probe.calls()[0].idempotency_key,
        Some(format!("nodule/{ALPHA}/{revision}/setup"))
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_foreign_initializer_goes_through_dispatch_under_the_nodules_operations_grant() {
    let probe = Probe::new();
    let remote = Probe::new();
    let node = node_hosting(&probe).await;
    let operator = node.operator();
    node.admit(
        operator,
        init_cartridge(
            "alpha",
            "1.0.0",
            &[PING],
            &[REMOTE],
            &[("sync", REMOTE, RunMode::Once)],
        ),
    )
    .await
    .unwrap();

    // Nobody provides the operation yet: the nodule waits, which is not an attempt.
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Initializing);
    assert_eq!(
        record.init,
        Some(NoduleInitStatus {
            attempts: 0,
            error: None,
            waiting_on: Some(REMOTE.to_owned()),
        })
    );
    let nodule_actor = record.actor.unwrap().actor_id;
    let revision = record.revision.unwrap();

    node.dispatch.router.register(
        "remote",
        [(REMOTE.to_owned(), Some(OperationKind::Query))],
        ProviderState::Ready(ProviderKind::BuiltIn(
            Arc::clone(&remote) as Arc<dyn Provider>
        )),
    );
    // Without a grant dispatch refuses the nodule's actor, and the provider is never entered.
    node.touch(ALPHA).await;
    let record = node.until(ALPHA, Readiness::InitFailed).await;
    let init = record.init.unwrap();
    assert_eq!(init.attempts, 1);
    assert!(
        init.error.as_deref().unwrap().contains("does not permit"),
        "{init:?}"
    );
    assert!(remote.calls().is_empty());

    // A grant that does not cover the operation is as good as none.
    node.grant_operations(ALPHA, &["org.example.other"]).await;
    let record = node.until(ALPHA, Readiness::InitFailed).await;
    assert_eq!(record.init.unwrap().attempts, 2);
    assert!(remote.calls().is_empty());

    let record = node.grant_operations(ALPHA, &["org.example.remote"]).await;
    assert!(record.operations.unwrap().granted);
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(
        remote.calls(),
        [Call {
            operation: REMOTE.to_owned(),
            idempotency_key: Some(format!("nodule/{ALPHA}/{revision}/sync")),
            // The call is the nodule actor's own, not the operator's or the service's.
            actor: nodule_actor,
        }]
    );
    // The foreign target is not the nodule's handler.
    assert!(probe.calls().is_empty());
    node.shutdown().await;
}

#[tokio::test]
async fn a_result_from_a_superseded_revision_is_dropped() {
    let probe = Probe::new();
    let mut node = node_hosting(&probe).await;
    let operator = node.operator();
    let cartridge = |version| {
        init_cartridge(
            "alpha",
            version,
            &[PING, SETUP],
            &[],
            &[("setup", SETUP, RunMode::Once)],
        )
    };
    node.admit(operator, cartridge("1.0.0")).await.unwrap();

    let first = probe.hold();
    node.set_enabled(ALPHA, true).await;
    probe.entered(1).await;
    let second = probe.hold();
    node.admit(operator, cartridge("1.1.0")).await.unwrap();
    probe.entered(2).await;

    // The first revision's call returns while the second's is still running. Its result names
    // an epoch that no longer exists, so the nodule is not made ready by it. (A result applied
    // wrongly would be: the key matches. The yields only give the message time to arrive, so
    // a wrong application is caught and a late one can pass, never flake.)
    first.notify_one();
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        node.invoke(operator, PING, json!({})).await,
        Err(initializing())
    );
    assert_eq!(
        node.get(operator, ALPHA).await.unwrap().readiness,
        Readiness::Initializing
    );

    second.notify_one();
    node.until(ALPHA, Readiness::Ready).await;
    // Only the second revision's success is on record: a restart does not run it again.
    node.restart_nodules().await.unwrap();
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(probe.operations(), [SETUP, SETUP]);
    node.shutdown().await;
}

#[tokio::test]
async fn initializers_that_wait_on_each_other_are_reported_as_a_dependency_cycle() {
    let probe = Probe::new();
    let node = node_hosting(&probe).await;
    let operator = node.operator();
    let alpha_ping = "org.example.alpha.ping";
    let beta_ping = "org.example.beta.ping";
    node.admit(
        operator,
        init_cartridge(
            "alpha",
            "1.0.0",
            &[alpha_ping],
            &[beta_ping],
            &[("needs-beta", beta_ping, RunMode::Once)],
        ),
    )
    .await
    .unwrap();
    node.admit(
        operator,
        init_cartridge(
            "beta",
            "1.0.0",
            &[beta_ping],
            &[alpha_ping],
            &[("needs-alpha", alpha_ping, RunMode::Once)],
        ),
    )
    .await
    .unwrap();
    node.set_enabled(ALPHA, true).await;
    node.set_enabled(example_cartridge!("beta"), true).await;

    // Each saw the other unregistered or not blocked yet; the next reconcile sees both waiting
    // on each other, and each reports it.
    node.touch(ALPHA).await;
    for record in node.list_records().await {
        assert_eq!(record.readiness, Readiness::InitFailed, "{record:?}");
        let error = record.init.unwrap().error.unwrap();
        assert!(error.contains("dependency cycle"), "{error}");
    }
    // Waiting is not invoking.
    assert!(probe.calls().is_empty());
    node.shutdown().await;
}

#[tokio::test]
async fn a_waiting_initializer_proceeds_when_its_provider_becomes_ready_by_any_path() {
    let probe = Probe::new();
    let node = node_hosting(&probe).await;
    let operator = node.operator();
    let beta_ping = "org.example.beta.ping";
    node.admit(
        operator,
        init_cartridge(
            "alpha",
            "1.0.0",
            &[PING],
            &[beta_ping],
            &[("needs-beta", beta_ping, RunMode::Once)],
        ),
    )
    .await
    .unwrap();
    // Beta is installed after alpha but stays disabled, so nobody provides its operation.
    node.admit(
        operator,
        init_cartridge("beta", "1.0.0", &[beta_ping], &[], &[]),
    )
    .await
    .unwrap();
    node.grant_operations(ALPHA, &["org.example.beta"]).await;
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::Initializing);
    assert_eq!(record.init.unwrap().waiting_on.as_deref(), Some(beta_ping));

    // Enabling beta is the only change: its transition to ready wakes alpha, whose
    // initializer then runs through dispatch (alpha sorts before beta, so a plain reconcile
    // would have looked at beta too early).
    node.set_enabled(example_cartridge!("beta"), true).await;
    node.until(ALPHA, Readiness::Ready).await;
    assert_eq!(probe.operations(), [beta_ping]);
    node.shutdown().await;
}

/// `cartridge` with its nodule's requests replaced by `requests`, for tests of what a request does
/// to the nodule (`init_cartridge` states its requests as optional).
pub(super) fn with_requests(mut cartridge: Cartridge, requests: Vec<Request>) -> Cartridge {
    let Some(Contribution::Nodule(nodule)) = cartridge.contributions.first_mut() else {
        panic!("init cartridges carry a nodule");
    };
    nodule
        .description
        .insert("requires".into(), serde_json::to_value(requests).unwrap());
    cartridge
}

pub(super) fn necessity(nsid: &str, necessity: Necessity) -> Request {
    let mut request = Request::new(nsid, nsid);
    request.necessity = necessity;
    request
}

#[tokio::test]
async fn a_grant_beyond_the_requests_is_not_applied_and_is_reported() {
    let probe = Probe::new();
    let node = node_hosting(&probe).await;
    let operator = node.operator();
    node.admit(
        operator,
        with_requests(
            init_cartridge("alpha", "1.0.0", &[PING], &[], &[]),
            vec![necessity(REMOTE, Necessity::Optional)],
        ),
    )
    .await
    .unwrap();

    node.set_enabled(ALPHA, true).await;

    // Granted: the requested operation (under a covering prefix), a prefix that covers nothing
    // requested, and an operation nobody requested.
    let record = node
        .call(
            operator,
            SetNoduleOperationGrants {
                cartridge_id: id(ALPHA),
                operations: DispatchScope {
                    operations: ["org.example.mail.send".to_owned()].into(),
                    authorities: [
                        "org.example.remote".to_owned(),
                        "org.example.other".to_owned(),
                    ]
                    .into(),
                },
            },
        )
        .await
        .unwrap();
    let operations = record.operations.unwrap();
    assert!(operations.granted);
    // The prefix is applied as the one requested operation it covers, not as the prefix.
    assert_eq!(
        operations.desired,
        DispatchScope {
            operations: [REMOTE.to_owned()].into(),
            authorities: BTreeSet::new(),
        }
    );
    assert_eq!(
        operations.unrequested,
        DispatchScope {
            operations: ["org.example.mail.send".to_owned()].into(),
            authorities: ["org.example.other".to_owned()].into(),
        }
    );

    // Grants alone confer nothing: with no request they are all reported and none minted.
    node.admit(
        operator,
        with_requests(
            init_cartridge("alpha", "1.1.0", &[PING], &[], &[]),
            Vec::new(),
        ),
    )
    .await
    .unwrap();
    let record = node.get(operator, ALPHA).await.unwrap();
    let operations = record.operations.unwrap();
    assert!(!operations.granted);
    assert!(operations.desired.is_empty());
    assert_eq!(operations.unrequested.operations.len(), 1);
    assert_eq!(operations.unrequested.authorities.len(), 2);
    node.shutdown().await;
}

#[tokio::test]
async fn a_required_request_needs_configuration_until_granted_and_an_optional_one_does_not() {
    let probe = Probe::new();
    let node = node_hosting(&probe).await;
    let operator = node.operator();
    let beta_ping = "org.example.beta.ping";
    node.admit(
        operator,
        with_requests(
            init_cartridge("alpha", "1.0.0", &[PING], &[], &[]),
            vec![necessity(REMOTE, Necessity::Required)],
        ),
    )
    .await
    .unwrap();
    node.admit(
        operator,
        with_requests(
            init_cartridge("beta", "1.0.0", &[beta_ping], &[], &[]),
            vec![necessity(REMOTE, Necessity::Optional)],
        ),
    )
    .await
    .unwrap();

    // A native provider of the requested operation, so only the grant is missing.
    node.dispatch.router.register(
        "remote",
        [(REMOTE.to_owned(), Some(OperationKind::Query))],
        ProviderState::Ready(ProviderKind::BuiltIn(Probe::new() as Arc<dyn Provider>)),
    );

    // The required request has no grant: not ready, not a fault, and callers get the
    // retryable not-ready. The optional one has no readiness effect.
    let record = node.set_enabled_record(ALPHA).await;
    assert_eq!(record.readiness, Readiness::NeedsConfiguration);
    assert_eq!(record.error, None);
    assert_eq!(
        node.invoke(operator, PING, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::NeedsConfiguration
        )))
    );
    let beta = node.set_enabled_record(example_cartridge!("beta")).await;
    assert_eq!(beta.readiness, Readiness::Ready);

    // A grant that covers the request makes it ready and serving; one that does not does not.
    let record = node.grant_operations(ALPHA, &["org.example.other"]).await;
    assert_eq!(record.readiness, Readiness::NeedsConfiguration);
    let record = node.grant_operations(ALPHA, &["org.example.remote"]).await;
    assert_eq!(record.readiness, Readiness::Ready);
    assert!(node.invoke(operator, PING, json!({})).await.is_ok());

    // Revoking the grant takes it back.
    let record = node.grant_operations(ALPHA, &[]).await;
    assert_eq!(record.readiness, Readiness::NeedsConfiguration);
    node.shutdown().await;
}
