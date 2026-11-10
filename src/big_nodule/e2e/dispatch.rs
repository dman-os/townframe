//! The nodule reconciler's registration of admitted `implements` in the dispatch router, seen
//! through `Client<DispatchProtocol>` against the same real authority the nodule tests use. The
//! router's own admission, built-in providers and deadlines are tested in `big_dispatch`.

use super::Node;
use super::id;
use super::lexicon;
use async_trait::async_trait;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::operation::OperationKind;
use big_dispatch::Caller;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch_core::DispatchError;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_nodule_core::Handler;
use big_nodule_core::Nodule;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::UninstallNodule;
use big_plugs_core::Artifact;
use big_plugs_core::Contribution;
use big_plugs_core::Digest;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use big_plugs_core::example_plug;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use tokio::sync::Notify;

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
        _request: &RequestContext,
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
    let mut nodule = Nodule::default();
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
    plug.authority = Some(nodule_id);
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
    async fn invoke_with(
        &self,
        authorization: Authorization,
        operation: &str,
        input: Value,
        context: InvocationContext,
    ) -> Result<Value, DispatchError> {
        self.dispatch
            .client
            .call_with(
                RequestContext::root(),
                authorization.metadata(),
                Invoke {
                    operation: operation.to_owned(),
                    input,
                    context,
                },
            )
            .await
            .map(|output| output.output)
            .map_err(|error| match error {
                ClientError::Method(error) => error,
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

    pub(super) async fn set_enabled(&self, plug_id: &str, enabled: bool) {
        self.call(
            self.operator(),
            SetNoduleEnabled {
                plug_id: id(plug_id),
                enabled,
            },
        )
        .await
        .unwrap();
    }
}

pub(super) fn not_ready(reason: NotReadyReason) -> DispatchError {
    DispatchError::NotReady { reason }
}

#[tokio::test]
async fn an_admitted_nodules_operations_follow_its_lifecycle() {
    let mut node = super::node().await;
    let operator = node.operator();
    let own = "org.example.photos.list";
    node.admit(operator, serving_plug(example_plug!("photos"), own))
        .await
        .unwrap();

    // Admitted but disabled is unbound: it claims nothing yet.
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    node.set_enabled(example_plug!("photos"), true).await;
    // Its handler artifact is not fetched and there is no executor, so the operation is known
    // but not serving.
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::ArtifactsUnavailable
        )))
    );
    let entry = node.dispatch.router.resolve(own).unwrap();
    assert_eq!(entry.provider_id, example_plug!("photos"));
    assert_eq!(entry.operation_kind, Some(OperationKind::Query));
    // The nodule's record reports the readiness dispatch answers with.
    let record = node.get(operator, example_plug!("photos")).await.unwrap();
    assert_eq!(record.readiness, entry.state.readiness());
    assert_eq!(record.readiness, Readiness::ArtifactsUnavailable);

    // Registrations come from the config, so a restart of the nodule service re-derives them.
    node.restart_nodules().await.unwrap();
    assert!(matches!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::NotReady { .. })
    ));

    node.set_enabled(example_plug!("photos"), false).await;
    assert_eq!(
        node.invoke(operator, own, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    node.set_enabled(example_plug!("photos"), true).await;
    node.call(
        operator,
        UninstallNodule {
            plug_id: id(example_plug!("photos")),
        },
    )
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
    for plug_id in [example_plug!("alpha"), example_plug!("beta")] {
        node.admit(operator, serving_plug(plug_id, shared))
            .await
            .unwrap();
        node.set_enabled(plug_id, true).await;
    }
    assert_eq!(
        node.invoke(operator, shared, json!({})).await,
        Err(not_ready(NotReadyReason::Ambiguous))
    );
    node.set_enabled(example_plug!("alpha"), false).await;
    let entry = node.dispatch.router.resolve(shared).unwrap();
    assert_eq!(entry.provider_id, example_plug!("beta"));
    assert_eq!(entry.operation_kind, None);
    assert_eq!(
        node.invoke(operator, shared, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::ArtifactsUnavailable
        )))
    );
    node.shutdown().await;
}
