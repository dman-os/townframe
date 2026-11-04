//! Dispatch through `Client<DispatchProtocol>` against the same real authority the nodule tests
//! use: operator and minted-handle admission, built-in providers, deadlines, and the nodule
//! reconciler's registration of admitted `implements`.

use super::Node;
use super::id;
use super::lexicon;
use crate::dispatch::Caller;
use crate::dispatch::Provider;
use crate::dispatch::ProviderError;
use crate::dispatch::ProviderKind;
use crate::dispatch::ProviderState;
use async_trait::async_trait;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::operation::OperationKind;
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use big_nodule_core::dispatch::DispatchError;
use big_nodule_core::dispatch::DispatchScope;
use big_nodule_core::dispatch::InvocationContext;
use big_nodule_core::dispatch::Invoke;
use big_nodule_core::dispatch::MintDispatchCapability;
use big_nodule_core::dispatch::NotReadyReason;
use big_nodule_core::inventory::Readiness;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::UninstallNodule;
use big_plugs_core::Artifact;
use big_plugs_core::Contribution;
use big_plugs_core::Digest;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use futures::FutureExt;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::sync::Arc;
use tokio::sync::Notify;

const ECHO: &str = "org.example.echo.ping";

/// Echoes its input, idempotency key and the caller dispatch verified; `{"invalid": ..}` and `{"fail": ..}` inputs make it
/// refuse the way a provider does; `{"wait": true}` blocks on `release` after signalling
/// `started`, so a test decides when (and whether) the call finishes.
#[derive(Default)]
pub(super) struct Echo {
    started: Notify,
    release: Notify,
}

#[async_trait]
impl Provider for Echo {
    async fn call(
        &self,
        caller: &Caller,
        _operation: &str,
        context: &InvocationContext,
        input: Value,
    ) -> Result<Value, ProviderError> {
        if let Some(reason) = input.get("invalid") {
            return Err(ProviderError::InvalidInput(reason.to_string()));
        }
        if let Some(body) = input.get("fail") {
            return Err(ProviderError::Failed {
                status: 418,
                body: body.clone(),
            });
        }
        if input.get("wait").is_some() {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(json!({
            "echo": input,
            "idempotencyKey": context.idempotency_key,
            "caller": {
                "authorization": caller.authorization,
                "actor": caller.actor,
                "scope": caller.scope,
            },
        }))
    }
}

/// A package serving `serves` through a handler component. `serves` is declared in its own
/// Lexicons when it lives under `org.example.<name>`, otherwise it is a foreign operation.
pub(super) fn serving_plug(plug_id: &str, serves: &str) -> Plug {
    serving_plug_of(
        plug_id,
        "1.0.0",
        serves,
        plug_id.rsplit('/').next().unwrap(),
    )
}

/// [`serving_plug`] at `version`, whose handler artifact is `handler_bytes`.
pub(super) fn serving_plug_of(
    plug_id: &str,
    version: &str,
    serves: &str,
    handler_bytes: &str,
) -> Plug {
    let name = plug_id.rsplit('/').next().unwrap();
    let nodule_id = format!("org.example.{name}");
    let mut nodule = Nodule::new(&nodule_id);
    if serves.starts_with(&format!("{nodule_id}.")) {
        nodule.lexicons.push(lexicon(json!({
            "lexicon": 1,
            "id": serves,
            "defs": { "main": {
                "type": "query",
                "parameters": { "type": "params", "properties": {} },
                "output": {
                    "encoding": "application/json",
                    "schema": { "type": "object", "properties": {} }
                }
            }}
        })));
    }
    nodule.handler = Some(Handler {
        artifact: "handler".into(),
        extra: Default::default(),
    });
    nodule.implements = vec![serves.to_owned()];
    let mut plug = Plug::new(plug_id.parse().unwrap(), version.parse().unwrap());
    plug.artifacts.push(Artifact {
        key: "handler".into(),
        digest: Digest::of(handler_bytes.as_bytes()),
        size: Some(handler_bytes.len() as u64),
        media_type: Some("application/wasm".into()),
        extra: Default::default(),
    });
    plug.contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: lexicon(serde_json::to_value(nodule).unwrap()),
            extra: Default::default(),
        }));
    plug
}

impl Node {
    fn register_echo(&self) -> Arc<Echo> {
        let echo = Arc::new(Echo::default());
        self.dispatch.router.register(
            "echo",
            [(ECHO.to_owned(), Some(OperationKind::Query))],
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::clone(&echo) as Arc<dyn Provider>)),
        );
        echo
    }

    async fn invoke_with(
        &self,
        authorization: Authorization,
        operation: &str,
        input: Value,
        context: InvocationContext,
    ) -> Result<Value, DispatchError> {
        self.dispatch
            .client
            .call(
                RequestContext::root(),
                Invoke {
                    authorization,
                    operation: operation.to_owned(),
                    input,
                    context,
                },
            )
            .await
            .map(|output| output.output)
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })
    }

    pub(super) async fn invoke(
        &self,
        authorization: Authorization,
        operation: &str,
        input: Value,
    ) -> Result<Value, DispatchError> {
        self.invoke_with(authorization, operation, input, Default::default())
            .await
    }

    async fn mint_dispatch(
        &self,
        authorization: Authorization,
        scope: DispatchScope,
    ) -> Result<Authorization, DispatchError> {
        let capability = self
            .dispatch
            .client
            .call(
                RequestContext::root(),
                MintDispatchCapability {
                    authorization,
                    scope,
                    rights: Rights::USE | Rights::NARROW,
                },
            )
            .await
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })?;
        Ok(Authorization {
            authority: capability.capability_id,
            signer: authorization.signer,
        })
    }

    pub(super) async fn set_enabled(&self, plug_id: &str, enabled: bool) {
        self.call(SetNoduleEnabled {
            authorization: self.operator(),
            plug_id: id(plug_id),
            enabled,
        })
        .await
        .unwrap();
    }
}

fn authorities(prefixes: &[&str]) -> DispatchScope {
    DispatchScope {
        authorities: prefixes.iter().map(|p| (*p).to_owned()).collect(),
        ..Default::default()
    }
}

pub(super) fn not_ready(reason: NotReadyReason) -> DispatchError {
    DispatchError::NotReady { reason }
}

#[tokio::test]
async fn the_operator_invokes_a_built_in_provider_and_its_refusals_pass_through() {
    let node = super::node().await;
    node.register_echo();
    let operator = node.operator();

    let context = InvocationContext {
        idempotency_key: Some("key-1".into()),
        ..Default::default()
    };
    assert_eq!(
        node.invoke_with(operator, ECHO, json!({ "n": 1 }), context)
            .await,
        Ok(json!({
            "echo": { "n": 1 },
            "idempotencyKey": "key-1",
            "caller": {
                "authorization": operator,
                "actor": node.authority.service.operator_uuid(),
                "scope": null,
            },
        }))
    );
    assert_eq!(
        node.invoke(operator, ECHO, json!({ "invalid": "n" })).await,
        Err(DispatchError::InvalidInput("\"n\"".into()))
    );
    assert_eq!(
        node.invoke(operator, ECHO, json!({ "fail": { "error": "Teapot" } }))
            .await,
        Err(DispatchError::Provider {
            status: 418,
            body: json!({ "error": "Teapot" })
        })
    );
    assert_eq!(
        node.invoke(operator, "org.example.echo.missing", json!({}))
            .await,
        Err(DispatchError::UnknownOperation)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn a_minted_scope_invokes_only_what_it_covers_and_only_narrows() {
    let node = super::node().await;
    node.register_echo();
    let operator = node.operator();
    let bystander = node.bystander().await;

    let echo_only = node
        .mint_dispatch(operator, authorities(&["org.example.echo"]))
        .await
        .unwrap();
    assert!(node.invoke(echo_only, ECHO, json!({})).await.is_ok());
    // Outside the scope is Denied whether or not the operation exists, so a handle cannot
    // probe for operations it may not call.
    for operation in ["org.example.other.ping", "org.example.echo2.ping"] {
        assert_eq!(
            node.invoke(echo_only, operation, json!({})).await,
            Err(DispatchError::Denied)
        );
    }

    // A handle mints only what it covers.
    let one_op = node
        .mint_dispatch(
            echo_only,
            DispatchScope {
                operations: [ECHO.to_owned()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(node.invoke(one_op, ECHO, json!({})).await.is_ok());
    assert_eq!(
        node.mint_dispatch(echo_only, authorities(&["org.example"]))
            .await,
        Err(DispatchError::Denied)
    );

    // Identity is not access: another actor's own capability is refused.
    assert_eq!(
        node.invoke(bystander, ECHO, json!({})).await,
        Err(DispatchError::Denied)
    );
    assert_eq!(
        node.mint_dispatch(bystander, DispatchScope::default())
            .await,
        Err(DispatchError::Denied)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_elapsed_deadline_fails_before_the_provider_and_a_running_call_is_cut_off() {
    let node = super::node().await;
    let echo = node.register_echo();
    let operator = node.operator();

    // The deadline is already behind us: the call fails without entering the provider. A
    // provider that was entered would have signalled `started`, so an unconsumed permit here
    // means it was.
    let elapsed = InvocationContext {
        deadline_unix_ms: Some(1),
        ..Default::default()
    };
    assert_eq!(
        node.invoke_with(operator, ECHO, json!({ "wait": true }), elapsed)
            .await,
        Err(DispatchError::DeadlineExceeded)
    );
    assert!(
        echo.started.notified().now_or_never().is_none(),
        "the provider was entered for a call whose deadline had passed"
    );

    // A deadline still ahead lets the call in; the provider parks on its gate until the deadline
    // passes, which cuts it off. The gate is never released.
    let soon = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1_000;
    let future = InvocationContext {
        deadline_unix_ms: Some(soon),
        ..Default::default()
    };
    assert_eq!(
        node.invoke_with(operator, ECHO, json!({ "wait": true }), future)
            .await,
        Err(DispatchError::DeadlineExceeded)
    );
    echo.started.notified().await;

    // A call without a deadline completes once released.
    echo.release.notify_one();
    assert!(
        node.invoke(operator, ECHO, json!({ "wait": true }))
            .await
            .is_ok()
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_admitted_nodules_operations_follow_its_lifecycle() {
    let mut node = super::node().await;
    let operator = node.operator();
    let own = "org.example.photos.list";
    node.admit(operator, serving_plug("@example/photos", own))
        .await
        .unwrap();

    // Admitted but disabled is unbound: it claims nothing yet.
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    node.set_enabled("@example/photos", true).await;
    // Its handler artifact is not fetched and there is no executor, so the operation is known
    // but not serving.
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::ArtifactsUnavailable
        )))
    );
    let entry = node.dispatch.router.resolve(own).unwrap();
    assert_eq!(entry.provider_id, "@example/photos");
    assert_eq!(entry.operation_kind, Some(OperationKind::Query));
    // The nodule's record reports the readiness dispatch answers with.
    let record = node.get(operator, "@example/photos").await.unwrap();
    assert_eq!(record.readiness, entry.state.readiness());
    assert_eq!(record.readiness, Readiness::ArtifactsUnavailable);

    // Registrations come from the config, so a restart of the nodule service re-derives them.
    node.restart_nodules().await.unwrap();
    assert!(matches!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::NotReady { .. })
    ));

    node.set_enabled("@example/photos", false).await;
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    node.set_enabled("@example/photos", true).await;
    node.call(UninstallNodule {
        authorization: operator,
        plug_id: id("@example/photos"),
    })
    .await
    .unwrap();
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn two_nodules_implementing_one_operation_are_ambiguous_until_one_is_disabled() {
    let node = super::node().await;
    let operator = node.operator();
    let shared = "org.example.shared.ping";
    for plug_id in ["@example/alpha", "@example/beta"] {
        node.admit(operator, serving_plug(plug_id, shared))
            .await
            .unwrap();
        node.set_enabled(plug_id, true).await;
    }
    assert_eq!(
        node.invoke(operator, shared, json!({})).await,
        Err(not_ready(NotReadyReason::Ambiguous))
    );
    node.set_enabled("@example/alpha", false).await;
    let entry = node.dispatch.router.resolve(shared).unwrap();
    assert_eq!(entry.provider_id, "@example/beta");
    assert_eq!(entry.operation_kind, None);
    assert_eq!(
        node.invoke(operator, shared, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::ArtifactsUnavailable
        )))
    );
    node.shutdown().await;
}
