//! A guest micro calls node methods, end to end: the `dispatch` fixture (`clay_mdk`'s example,
//! prepared as `DAYFRAME_DISPATCH_COMPONENT`) runs under Wasmtime and calls an in-process node's
//! dispatch with `Context::call`, over `wasi:http` to `dispatch.local`, answered by
//! [`MicroEgress`] as the micro's own actor. The node is an authority and a dispatch with one
//! provider, the way `big_dispatch`'s own tests build one.

use crate::MicroEgress;
use async_trait::async_trait;
use big_cabinet::Authority;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::operation::OperationKind;
use big_dispatch::Caller;
use big_dispatch::Dispatch;
use big_dispatch::DispatchConfig;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::Body;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use clay_guest_session::InitialContext;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_guest_session::capabilities::PublicationKind;
use clay_guest_session::capabilities::presentation_component;
use clay_guest_session::egress::DispatchEgress;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::MicroId;
use clay_rt_wasmtime::WasmPackage;
use clay_rt_wasmtime::WasmRuntime;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::SurfaceSink;
use secrets_rs::SecretStore;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use std::sync::Arc;
use std::sync::Mutex;

const ECHO: &str = "org.example.clay.test.dispatch.echo";
const REVEAL: &str = "org.example.clay.test.dispatch.reveal";

/// Answers `echo` with its input and the actor dispatch verified the call was signed by, and
/// `reveal` with a marker that must never be reached by a micro not granted it.
struct TestMethods;

#[async_trait]
impl Provider for TestMethods {
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        _request: &RequestContext,
        _context: &InvocationContext,
        input: Value,
        _body: Option<Body>,
    ) -> Result<Body, ProviderError> {
        Ok(Body::Json(json!({
            "operation": operation,
            "echo": input["text"],
            "actor": caller.actor,
        })))
    }
}

struct Node {
    authority: Authority,
    authority_stop: StopToken,
    dispatch: Dispatch,
    dispatch_stop: StopToken,
}

async fn node() -> Node {
    let keyring = keyring_core::mock::Store::new().expect("mock keyring")
        as Arc<keyring_core::CredentialStore>;
    let secrets = SecretStore::boot_with_store(keyring)
        .await
        .expect("mock keyring");
    let sql = sqlite_rs::test_support::db(&[]).expect("database");
    let (authority, authority_stop) =
        big_cabinet::authority::boot(&secrets, "test-node", sql.clone())
            .await
            .expect("authority boot");
    let dispatch_actor = authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: authority.root,
                path: "dispatch".into(),
                rights: Rights::all(),
            },
        )
        .await
        .expect("dispatch actor");
    let namespace = authority
        .service
        .open_namespace(dispatch_actor.actor.actor_id)
        .await
        .expect("dispatch namespace");
    let (dispatch, dispatch_stop) = big_dispatch::boot(DispatchConfig {
        authority: authority.client.clone(),
        namespace,
        operator: authority.service.operator_uuid(),
    });
    dispatch
        .router
        .register(
            "clay-test",
            [ECHO, REVEAL].map(|nsid| {
                (
                    nsid.to_owned(),
                    Operation::unchecked(Some(OperationKind::Procedure)),
                )
            }),
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(TestMethods))),
        )
        .expect("the test methods are unclaimed");
    Node {
        authority,
        authority_stop,
        dispatch,
        dispatch_stop,
    }
}

impl Node {
    /// The micro's own actor, under the `micros` subsystem actor, as the host opens one per
    /// installed micro (Clay on the node §2).
    async fn open_micro_actor(&self, name: &str) -> OpenedActor {
        let open = |parent, path: &str, rights| {
            self.authority.client.call(
                RequestContext::root(),
                OpenActor {
                    parent,
                    path: path.into(),
                    rights,
                },
            )
        };
        // A subsystem actor holds every right, as `big_node` opens `nodules`; a micro's own
        // actor can only be used.
        let micros = open(self.authority.root, "micros", Rights::all())
            .await
            .expect("the micros subsystem actor");
        open(micros.capability, &format!("micros/{name}"), Rights::USE)
            .await
            .expect("the micro's actor")
    }

    async fn stop(self) {
        self.dispatch_stop.stop().await;
        self.authority_stop.stop().await;
    }
}

/// Collects the text the guest renders, and wakes a waiter on each batch.
struct TextSink(Mutex<Vec<String>>, tokio::sync::Notify);

impl SurfaceSink<ComponentRef> for TextSink {
    fn is_live(&self) -> bool {
        true
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        let mut texts = self.0.lock().unwrap();
        texts.extend(edits.iter().filter_map(|edit| match edit {
            Mutation::CreateText(text) => Some(text.clone()),
            _ => None,
        }));
        self.1.notify_one();
        Ok(0)
    }

    fn withdraw(&self) {}
}

/// Runs the `dispatch` fixture with `egress`, and returns what it reported, by attempt.
async fn run_fixture(
    egress: Option<Arc<dyn DispatchEgress>>,
) -> std::collections::BTreeMap<String, String> {
    let host = Host::new();
    let instance = host.start("dispatch-fixture");
    let package = WasmPackage::from_env("dispatch")
        .await
        .expect("the prepared dispatch component is valid");
    let context = InitialContext {
        publications: [(
            "view".to_owned(),
            DeclaredPublication {
                kind: PublicationKind::Presentation,
                contract: presentation_component(),
            },
        )]
        .into(),
        micro: Some(MicroId::new("dispatch-fixture")),
        egress,
        ..InitialContext::default()
    };
    let (runtime, driver) = WasmRuntime::initialize(&host, instance.clone(), &package, context)
        .await
        .unwrap_or_else(|error| panic!("the fixture activates: {error}"));
    let sink = Arc::new(TextSink(Mutex::new(Vec::new()), tokio::sync::Notify::new()));
    let session = runtime
        .mount("view", Arc::clone(&sink) as _)
        .unwrap_or_else(|error| {
            panic!(
                "the fixture mounts: {error:?}, fault {:?}",
                host.fault(&instance)
            )
        });
    // The `resource` line renders once the view's call answers, which takes the driver: it runs
    // the incarnation's spawned work after `initialize` returned.
    let mut driver = Box::pin(driver);
    let rendered = async {
        loop {
            let notified = sink.1.notified();
            let texts = sink.0.lock().unwrap().clone();
            if texts.iter().any(|text| text.starts_with("resource=")) {
                return texts;
            }
            notified.await;
        }
    };
    let texts = tokio::select! {
        texts = rendered => texts,
        () = driver.as_mut() => panic!(
            "the incarnation ended before the view's call answered, fault {:?}",
            host.fault(&instance)
        ),
    };
    let attempts = texts
        .iter()
        .flat_map(|text| text.lines())
        .map(|line| {
            let (name, outcome) = line.split_once('=').expect("name=outcome");
            (name.to_owned(), outcome.to_owned())
        })
        .collect();
    drop(session);
    drop(driver);
    host.close();
    attempts
}

fn methods(nsids: &[&str]) -> DispatchScope {
    DispatchScope {
        operations: nsids.iter().map(|nsid| (*nsid).to_owned()).collect(),
        ..DispatchScope::default()
    }
}

/// The micro's calls reach dispatch as its own actor under the capability the host minted for
/// the granted methods: an answer for a granted method (which names the micro's actor, taken
/// from the host's table and not from anything the guest said), a denial from dispatch for a
/// method not granted, an answer even when the guest forges an `authorization` header (the host
/// replaced it), and a refusal by the runtime for any origin but `dispatch.local`.
#[tokio::test]
async fn a_micro_calls_granted_node_methods_as_its_own_actor() {
    let node = node().await;
    let actor = node.open_micro_actor("dispatch-fixture").await;
    let egress = MicroEgress::install(
        node.dispatch.client.clone(),
        Authorization::identity(node.authority.root),
        &actor,
        methods(&[ECHO]),
    )
    .await
    .expect("minting the micro's capability");

    let attempts = run_fixture(Some(Arc::new(egress))).await;

    let answer = |name: &str| -> Value {
        let outcome = &attempts[name];
        let body = outcome
            .strip_prefix("ok(")
            .and_then(|rest| rest.strip_suffix(')'))
            .unwrap_or_else(|| panic!("{name} should answer: {outcome}"));
        serde_json::from_str(body).expect("the fixture prints the answer as JSON")
    };
    for (name, echoed) in [
        ("granted", "ping"),
        ("forged", "forged"),
        ("resource", "resource"),
    ] {
        assert_eq!(
            answer(name),
            json!({
                "operation": ECHO,
                "echo": echoed,
                // Dispatch verified the signer: the micro's actor, from the host's table.
                "actor": actor.actor.actor_id,
            }),
            "{name}"
        );
    }
    let ungranted = &attempts["ungranted"];
    assert!(
        ungranted.starts_with("err(") && ungranted.contains("403") && ungranted.contains("Denied"),
        "an ungranted method is denied by dispatch: {ungranted}"
    );
    let refused = &attempts["refused"];
    assert!(
        refused.starts_with("err(") && refused.contains("HttpRequestDenied"),
        "an origin other than dispatch.local is refused: {refused}"
    );
    node.stop().await;
}

/// A micro granted no methods is minted no capability and can call nothing: dispatch is not even
/// reached for want of a credential (an empty scope never means "everything").
#[tokio::test]
async fn a_micro_granted_no_methods_calls_nothing() {
    let node = node().await;
    let actor = node.open_micro_actor("dispatch-fixture").await;
    let egress = MicroEgress::install(
        node.dispatch.client.clone(),
        Authorization::identity(node.authority.root),
        &actor,
        methods(&[]),
    )
    .await
    .expect("installing a micro with no methods mints nothing");

    let attempts = run_fixture(Some(Arc::new(egress))).await;

    for name in ["granted", "ungranted", "forged"] {
        let outcome = &attempts[name];
        assert!(
            outcome.starts_with("err(")
                && outcome.contains("401")
                && outcome.contains("AuthRequired"),
            "{name} has no credential to present: {outcome}"
        );
    }
    node.stop().await;
}

/// A micro its composition gave no egress has no way out at all: even `dispatch.local` is refused
/// by the runtime.
#[tokio::test]
async fn a_micro_without_an_egress_cannot_reach_dispatch() {
    let attempts = run_fixture(None).await;

    for name in ["granted", "ungranted", "forged", "refused"] {
        let outcome = &attempts[name];
        assert!(
            outcome.starts_with("err(") && outcome.contains("HttpRequestDenied"),
            "{name} is refused outright: {outcome}"
        );
    }
}
