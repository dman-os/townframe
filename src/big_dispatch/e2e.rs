//! Dispatch through `Client<DispatchProtocol>` against a real authority: operator and
//! minted-handle admission, built-in providers, deadlines and the [`ServiceProvider`] adapter.
//! Routing a node's services and nodules through one router is tested in `big_node`.

use crate::Caller;
use crate::Dispatch;
use crate::DispatchConfig;
use crate::LexiconSource;
use crate::Operation;
use crate::Provider;
use crate::ProviderError;
use crate::ProviderKind;
use crate::ProviderState;
use crate::ServiceProvider;
use async_trait::async_trait;
use big_cabinet::Authority;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::operation::OperationKind;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::MintDispatchCapability;
use futures::FutureExt;
use method_core::ContextError;
use secrets_rs::SecretStore;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const ECHO: &str = "org.example.echo.ping";

/// Echoes its input, idempotency key and the caller dispatch verified; `{"invalid": ..}` and
/// `{"fail": ..}` inputs make it refuse the way a provider does; `{"wait": true}` blocks on
/// `release` after signalling `started`, so a test decides when (and whether) the call finishes.
#[derive(Default)]
struct Echo {
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

struct Harness {
    authority: Authority,
    authority_stop: StopToken,
    dispatch: Dispatch,
    dispatch_stop: StopToken,
}

async fn harness() -> Harness {
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
    let actor = authority
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
        .open_namespace(actor.actor.actor_id)
        .await
        .expect("dispatch namespace");
    let (dispatch, dispatch_stop) = crate::boot(DispatchConfig {
        authority: authority.client.clone(),
        namespace,
        operator: authority.service.operator_uuid(),
    });
    Harness {
        authority,
        authority_stop,
        dispatch,
        dispatch_stop,
    }
}

fn authorities(prefixes: &[&str]) -> DispatchScope {
    DispatchScope {
        authorities: prefixes.iter().map(|p| (*p).to_owned()).collect(),
        ..Default::default()
    }
}

impl Harness {
    async fn shutdown(self) {
        self.dispatch_stop.stop().await;
        self.authority_stop.stop().await;
    }

    fn operator(&self) -> Authorization {
        Authorization::identity(self.authority.root)
    }

    async fn bystander(&self) -> Authorization {
        let actor = self
            .authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: self.authority.root,
                    path: "bystander".into(),
                    rights: Rights::all(),
                },
            )
            .await
            .expect("bystander actor");
        Authorization::identity(actor.capability)
    }

    fn register_echo(&self) -> Arc<Echo> {
        let echo = Arc::new(Echo::default());
        self.dispatch
            .router
            .register(
                "echo",
                [(
                    ECHO.to_owned(),
                    Operation::unchecked(Some(OperationKind::Query)),
                )],
                ProviderState::Ready(ProviderKind::BuiltIn(Arc::clone(&echo) as Arc<dyn Provider>)),
            )
            .unwrap();
        echo
    }

    async fn invoke_with(
        &self,
        authorization: Authorization,
        operation: &str,
        input: Value,
        context: InvocationContext,
        request: RequestContext,
    ) -> Result<Value, DispatchError> {
        self.dispatch
            .client
            .call_with(
                request,
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

    async fn invoke(
        &self,
        authorization: Authorization,
        operation: &str,
        input: Value,
    ) -> Result<Value, DispatchError> {
        self.invoke_with(
            authorization,
            operation,
            input,
            Default::default(),
            RequestContext::root(),
        )
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
            .call_with(
                RequestContext::root(),
                authorization.metadata(),
                MintDispatchCapability {
                    scope,
                    rights: Rights::USE | Rights::NARROW,
                    subject: None,
                },
            )
            .await
            .map_err(|error| match error {
                ClientError::Method(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })?;
        Ok(Authorization {
            authority: capability.capability_id,
            signer: authorization.signer,
        })
    }
}

#[tokio::test]
async fn the_operator_invokes_a_built_in_provider_and_its_refusals_pass_through() {
    let h = harness().await;
    h.register_echo();
    let operator = h.operator();

    let context = InvocationContext {
        idempotency_key: Some("key-1".into()),
        ..Default::default()
    };
    assert_eq!(
        h.invoke_with(
            operator,
            ECHO,
            json!({ "n": 1 }),
            context,
            RequestContext::root()
        )
        .await,
        Ok(json!({
            "echo": { "n": 1 },
            "idempotencyKey": "key-1",
            "caller": {
                "authorization": operator,
                "actor": h.authority.service.operator_uuid(),
                "scope": null,
            },
        }))
    );
    assert_eq!(
        h.invoke(operator, ECHO, json!({ "invalid": "n" })).await,
        Err(DispatchError::InvalidInput("\"n\"".into()))
    );
    assert_eq!(
        h.invoke(operator, ECHO, json!({ "fail": { "error": "Teapot" } }))
            .await,
        Err(DispatchError::Provider {
            status: 418,
            body: json!({ "error": "Teapot" })
        })
    );
    assert_eq!(
        h.invoke(operator, "org.example.echo.missing", json!({}))
            .await,
        Err(DispatchError::UnknownOperation)
    );
    h.shutdown().await;
}

#[tokio::test]
async fn a_minted_scope_invokes_only_what_it_covers_and_only_narrows() {
    let h = harness().await;
    h.register_echo();
    let operator = h.operator();
    let bystander = h.bystander().await;

    let echo_only = h
        .mint_dispatch(operator, authorities(&["org.example.echo"]))
        .await
        .unwrap();
    assert!(h.invoke(echo_only, ECHO, json!({})).await.is_ok());
    // Outside the scope is Denied whether or not the operation exists, so a handle cannot
    // probe for operations it may not call.
    for operation in ["org.example.other.ping", "org.example.echo2.ping"] {
        assert_eq!(
            h.invoke(echo_only, operation, json!({})).await,
            Err(DispatchError::Denied)
        );
    }

    // A handle mints only what it covers.
    let one_op = h
        .mint_dispatch(
            echo_only,
            DispatchScope {
                operations: [ECHO.to_owned()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(h.invoke(one_op, ECHO, json!({})).await.is_ok());
    assert_eq!(
        h.mint_dispatch(echo_only, authorities(&["org.example"]))
            .await,
        Err(DispatchError::Denied)
    );

    // Identity is not access: another actor's own capability is refused.
    assert_eq!(
        h.invoke(bystander, ECHO, json!({})).await,
        Err(DispatchError::Denied)
    );
    assert_eq!(
        h.mint_dispatch(bystander, DispatchScope::default()).await,
        Err(DispatchError::Denied)
    );
    h.shutdown().await;
}

#[tokio::test]
async fn a_scoped_handle_presented_by_an_actor_it_is_not_bound_to_is_refused() {
    let h = harness().await;
    h.register_echo();
    let operator = h.operator();
    let bystander = h.bystander().await;
    let echo_only = h
        .mint_dispatch(operator, authorities(&["org.example.echo"]))
        .await
        .unwrap();
    assert!(h.invoke(echo_only, ECHO, json!({})).await.is_ok());

    // The handle is bound to the operator. Another actor presenting it under its own signer is
    // refused on every path through the domain, the invoke and the mint alike.
    let stolen = Authorization {
        authority: echo_only.authority,
        signer: bystander.signer,
    };
    assert_eq!(
        h.invoke(stolen, ECHO, json!({})).await,
        Err(DispatchError::Denied)
    );
    assert_eq!(
        h.mint_dispatch(stolen, DispatchScope::default()).await,
        Err(DispatchError::Denied)
    );
    h.shutdown().await;
}

#[tokio::test]
async fn an_elapsed_budget_fails_before_the_provider_and_a_running_call_is_cut_off() {
    let h = harness().await;
    let echo = h.register_echo();
    let operator = h.operator();

    // The budget is already spent: the call fails without entering the provider. A
    // provider that was entered would have signalled `started`, so an unconsumed permit here
    // means it was.
    let elapsed = RequestContext::root().with_time_budget(Duration::ZERO);
    assert_eq!(
        h.invoke_with(
            operator,
            ECHO,
            json!({ "wait": true }),
            Default::default(),
            elapsed
        )
        .await,
        Err(DispatchError::Context(ContextError::DeadlineExceeded))
    );
    assert!(
        echo.started.notified().now_or_never().is_none(),
        "the provider was entered for a call whose deadline had passed"
    );

    // A budget still ahead lets the call in; the provider parks on its gate until the budget
    // runs out, which cuts it off. The gate is never released.
    let future = RequestContext::root().with_time_budget(Duration::from_secs(1));
    assert_eq!(
        h.invoke_with(
            operator,
            ECHO,
            json!({ "wait": true }),
            Default::default(),
            future
        )
        .await,
        Err(DispatchError::Context(ContextError::DeadlineExceeded))
    );
    echo.started.notified().await;

    // A call without a budget completes once released.
    echo.release.notify_one();
    assert!(
        h.invoke(operator, ECHO, json!({ "wait": true }))
            .await
            .is_ok()
    );
    h.shutdown().await;
}

#[tokio::test]
async fn a_request_without_a_credential_is_unauthenticated() {
    let h = harness().await;
    h.register_echo();
    let result = h
        .dispatch
        .client
        .call(
            RequestContext::root(),
            Invoke {
                operation: ECHO.to_owned(),
                input: json!({}),
                context: Default::default(),
            },
        )
        .await;
    assert_eq!(
        result.map(|_| ()).map_err(|error| match error {
            ClientError::Method(error) => error,
            ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
        }),
        Err(DispatchError::Context(
            method_core::ContextError::Unauthenticated
        ))
    );
    h.shutdown().await;
}

/// The adapter, exercised on dispatch's own protocol: serving `Client<DispatchProtocol>` as a
/// provider makes `mintCapability` reachable by NSID through an `Invoke`. The caller holds one
/// credential: dispatch admits it for the NSID, then hands it on as the request's `Authorization`
/// and the service checks it again itself.
#[tokio::test]
async fn a_service_provider_routes_by_nsid_and_carries_the_services_own_errors() {
    const MINT: &str = "org.example.daybook.dispatch.mintCapability";
    let h = harness().await;
    ServiceProvider::default()
        .serve(&h.dispatch.client)
        .register(&h.dispatch.router, "builtin:self");
    assert_eq!(
        h.dispatch.router.operations_of("builtin:self"),
        ["org.example.daybook.dispatch.invoke", MINT]
    );
    let entry = h.dispatch.router.resolve(MINT).unwrap();
    assert_eq!(entry.operation_kind, Some(OperationKind::Procedure));

    let operator = h.operator();
    let request = json!({
        "scope": { "authorities": ["org.example"] },
        "rights": Rights::USE,
    });
    // The operator's identity is admitted by dispatch and mints in the service.
    let minted = h.invoke(operator, MINT, request.clone()).await.unwrap();
    assert!(minted.get("capabilityId").is_some(), "{minted}");

    // A handle that admits `mintCapability` but covers no authority passes admission; the
    // service refuses it itself, because the scope it is asked to mint is wider. Its error
    // travels in the XRPC domain envelope.
    let handle = h
        .mint_dispatch(
            operator,
            DispatchScope {
                operations: [MINT.to_owned()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let Err(DispatchError::Provider { status, body }) = h.invoke(handle, MINT, request).await
    else {
        panic!("the service must refuse a scope the handle does not cover");
    };
    assert_eq!(status, 400);
    assert_eq!(body["error"], "Denied");
    assert_eq!(body["operationError"], json!("Denied"));

    // Input that is not the operation's request is the caller's mistake, not the service's.
    assert!(matches!(
        h.invoke(operator, MINT, json!({ "scope": 7 })).await,
        Err(DispatchError::InvalidInput(_))
    ));
    h.shutdown().await;
}

const PUT: &str = "org.example.contract.put";
const GET: &str = "org.example.contract.get";

/// A provider that answers `output` whatever it is asked, after recording the input it was given.
struct Canned {
    output: Value,
    seen: std::sync::Mutex<Vec<Value>>,
}

#[async_trait]
impl Provider for Canned {
    async fn call(
        &self,
        _: &Caller,
        _: &str,
        _: &RequestContext,
        _: &InvocationContext,
        input: Value,
    ) -> Result<Value, ProviderError> {
        self.seen.lock().unwrap().push(input);
        Ok(self.output.clone())
    }
}

impl Harness {
    /// Registers a procedure `PUT` (`{name: string, count?: integer}` -> `{ok: boolean}`) and a
    /// query `GET` (`{limit: integer = 10, flag?: boolean, tags?: string[]}` -> `{ok: boolean}`),
    /// both answered with `output`.
    fn register_contract(&self, output: Value) -> Arc<Canned> {
        let canned = Arc::new(Canned {
            output,
            seen: Default::default(),
        });
        let body = json!({"encoding": "application/json", "schema": {
            "type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}
        }});
        let put = json!({"lexicon": 1, "id": PUT, "defs": {"main": {
            "type": "procedure",
            "input": {"encoding": "application/json", "schema": {
                "type": "object", "required": ["name"],
                "properties": {"name": {"type": "string"}, "count": {"type": "integer"}}
            }},
            "output": body,
        }}});
        let get = json!({"lexicon": 1, "id": GET, "defs": {"main": {
            "type": "query",
            "parameters": {"type": "params", "properties": {
                "limit": {"type": "integer", "default": 10},
                "flag": {"type": "boolean"},
                "tags": {"type": "array", "items": {"type": "string"}},
            }},
            "output": body,
        }}});
        let operation = |kind, file| Operation {
            kind: Some(kind),
            lexicon: Some(LexiconSource {
                file,
                carried: Vec::new(),
            }),
        };
        self.dispatch
            .router
            .register(
                "contract",
                [
                    (PUT.to_owned(), operation(OperationKind::Procedure, put)),
                    (GET.to_owned(), operation(OperationKind::Query, get)),
                ],
                ProviderState::Ready(ProviderKind::BuiltIn(
                    Arc::clone(&canned) as Arc<dyn Provider>
                )),
            )
            .unwrap();
        canned
    }

    async fn invoke_http(
        &self,
        operation: &str,
        input: Value,
        http: HttpForm,
    ) -> Result<Value, DispatchError> {
        self.invoke_with(
            self.operator(),
            operation,
            input,
            InvocationContext {
                http: Some(http),
                ..Default::default()
            },
            RequestContext::root(),
        )
        .await
    }
}

#[tokio::test]
async fn an_input_off_the_methods_lexicon_is_refused_before_the_provider_is_called() {
    let h = harness().await;
    let canned = h.register_contract(json!({"ok": true}));
    let operator = h.operator();
    assert_eq!(
        h.invoke(operator, PUT, json!({"name": "x", "count": 2}))
            .await,
        Ok(json!({"ok": true}))
    );
    for broken in [json!({"count": 2}), json!({"name": 7}), json!([])] {
        let Err(DispatchError::InvalidInput(message)) =
            h.invoke(operator, PUT, broken.clone()).await
        else {
            panic!("{broken} must be refused");
        };
        assert!(message.contains("Lexicon"), "{message}");
    }
    assert_eq!(
        canned.seen.lock().unwrap().len(),
        1,
        "only the valid call arrived"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn a_built_ins_input_is_checked_against_its_method_lexicon() {
    const MINT: &str = "org.example.daybook.dispatch.mintCapability";
    let h = harness().await;
    ServiceProvider::default()
        .serve(&h.dispatch.client)
        .register(&h.dispatch.router, "builtin:self");
    let Err(DispatchError::InvalidInput(message)) = h
        .invoke(h.operator(), MINT, json!({"scope": 7, "rights": "all"}))
        .await
    else {
        panic!("a scope that is not an object is refused by dispatch");
    };
    assert!(message.contains("scope"), "{message}");
    h.shutdown().await;
}

/// atproto's xrpc-server fails a call whose handler's output departs from the Lexicon with
/// `InternalServerError` (500); dispatch does the same and does not hand the body on.
#[tokio::test]
async fn an_output_off_the_methods_lexicon_is_a_provider_failure() {
    let h = harness().await;
    h.register_contract(json!({"ok": "yes"}));
    let Err(DispatchError::InvalidOutput(message)) =
        h.invoke(h.operator(), PUT, json!({"name": "x"})).await
    else {
        panic!("an output that is not the declared shape is a provider failure");
    };
    assert!(message.contains(PUT), "{message}");
    h.shutdown().await;
}

#[tokio::test]
async fn a_get_is_typed_by_the_parameters_with_defaults_applied_and_unknown_names_ignored() {
    let h = harness().await;
    let canned = h.register_contract(json!({"ok": true}));
    let get = |query: &str| HttpForm::Get {
        query: query.to_owned(),
    };
    h.invoke_http(
        GET,
        Value::Null,
        get("flag=true&limit=3&tags=a&tags=b&cacheBuster=9"),
    )
    .await
    .unwrap();
    h.invoke_http(GET, Value::Null, get("")).await.unwrap();
    assert_eq!(
        *canned.seen.lock().unwrap(),
        [
            json!({"limit": 3, "flag": true, "tags": ["a", "b"]}),
            json!({"limit": 10}),
        ]
    );
    let Err(DispatchError::InvalidInput(message)) =
        h.invoke_http(GET, Value::Null, get("limit=many")).await
    else {
        panic!("a parameter of the wrong type is refused");
    };
    assert!(message.contains("limit"), "{message}");
    h.shutdown().await;
}

/// atproto answers `InvalidRequest` ("Incorrect HTTP method (POST) expected GET") for the wrong
/// verb; so does dispatch, after admission.
#[tokio::test]
async fn the_wrong_http_verb_for_a_methods_kind_is_invalid() {
    let h = harness().await;
    h.register_contract(json!({"ok": true}));
    for (operation, http, expected) in [
        (GET, HttpForm::Post, "expected GET"),
        (
            PUT,
            HttpForm::Get {
                query: String::new(),
            },
            "expected POST",
        ),
    ] {
        let Err(DispatchError::InvalidInput(message)) =
            h.invoke_http(operation, json!({}), http).await
        else {
            panic!("{operation}: the wrong verb is refused");
        };
        assert!(message.contains(expected), "{message}");
    }
    h.shutdown().await;
}
