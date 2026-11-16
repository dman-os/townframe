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
use big_dispatch_core::Body;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::HttpForm;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::MintDispatchCapability;
use big_dispatch_core::Origin;
use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use method_core::ByteBody;
use method_core::ContextError;
use method_core::Encoding;
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
        _body: Option<ByteBody>,
    ) -> Result<Body, ProviderError> {
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
        Ok(Body::Json(json!({
            "echo": input,
            "idempotencyKey": context.idempotency_key,
            "caller": {
                "authorization": caller.authorization,
                "actor": caller.actor,
                "scope": caller.scope,
            },
        })))
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
                Invoke::json(operation, input, context),
            )
            .await
            .map(|output| output.body.into_json())
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
            Invoke::json(ECHO, json!({}), Default::default()),
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

/// A scope that names every authority still reaches no privileged method, a scope reaches one only
/// when it says `privileged`, and the bit attenuates like the names do: a handle without it cannot
/// mint one with it, while minting a narrower handle still works.
#[tokio::test]
async fn privilege_is_a_scope_bit_that_names_cannot_confer() {
    const SENSITIVE: &str = "org.example.admin.setGrants";
    let h = harness().await;
    let echo = h.register_echo();
    h.dispatch
        .router
        .register(
            "admin",
            [(
                SENSITIVE.to_owned(),
                Operation {
                    privileged: true,
                    ..Operation::unchecked(Some(OperationKind::Query))
                },
            )],
            ProviderState::Ready(ProviderKind::BuiltIn(echo as Arc<dyn Provider>)),
        )
        .unwrap();
    let operator = h.operator();

    let everything = h
        .mint_dispatch(operator, authorities(&[DispatchScope::ALL_AUTHORITIES]))
        .await
        .unwrap();
    assert!(h.invoke(everything, ECHO, json!({})).await.is_ok());
    assert_eq!(
        h.invoke(everything, SENSITIVE, json!({})).await,
        Err(DispatchError::Denied)
    );
    // Naming the method exactly is no more than naming its authority.
    let named = h
        .mint_dispatch(
            operator,
            DispatchScope {
                operations: [SENSITIVE.to_owned()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        h.invoke(named, SENSITIVE, json!({})).await,
        Err(DispatchError::Denied)
    );

    // Only a handle that holds the bit can mint one with it.
    let privileged = DispatchScope {
        privileged: true,
        ..authorities(&[DispatchScope::ALL_AUTHORITIES])
    };
    assert_eq!(
        h.mint_dispatch(everything, privileged.clone()).await,
        Err(DispatchError::Denied)
    );
    let holder = h.mint_dispatch(operator, privileged).await.unwrap();
    assert!(h.invoke(holder, SENSITIVE, json!({})).await.is_ok());
    // Narrowing still works from either, and a narrowed handle of the holder keeps the bit only
    // if it asks for it.
    let narrowed = h
        .mint_dispatch(everything, authorities(&["org.example.echo"]))
        .await
        .unwrap();
    assert!(h.invoke(narrowed, ECHO, json!({})).await.is_ok());
    let dropped = h
        .mint_dispatch(holder, authorities(&["org.example.admin"]))
        .await
        .unwrap();
    assert_eq!(
        h.invoke(dropped, SENSITIVE, json!({})).await,
        Err(DispatchError::Denied)
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
        _: Option<ByteBody>,
    ) -> Result<Body, ProviderError> {
        self.seen.lock().unwrap().push(input);
        Ok(Body::Json(self.output.clone()))
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
            lexicon: Some(LexiconSource {
                file,
                carried: Vec::new(),
            }),
            ..Operation::unchecked(Some(kind))
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
        (
            GET,
            HttpForm::Post {
                query: String::new(),
            },
            "expected GET",
        ),
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

/// A cartridge's operation that refers to the node's reference defs is checked with them
/// resolved, whether the provider carried no copy or carried the node's own: the input's
/// reference is validated as a reference (a malformed one is refused at its path), not left as
/// an unresolved `ref` that no object satisfies.
#[tokio::test]
async fn an_operation_referring_to_the_nodes_reference_defs_is_validated_with_them() {
    let h = harness().await;
    for (nsid, carried) in [
        ("org.example.contract.tagBare", Vec::new()),
        (
            "org.example.contract.tagVendored",
            vec![big_cabinet_core::reference::defs_lexicon()],
        ),
    ] {
        let tag = json!({"lexicon": 1, "id": nsid, "defs": {"main": {
            "type": "procedure",
            "input": {"encoding": "application/json", "schema": {
                "type": "object", "required": ["part"],
                "properties": {"part": {"type": "ref", "ref": "org.example.daybook.defs#datumRef"}}
            }},
            "output": {"encoding": "application/json", "schema": {"type": "object", "properties": {}}},
        }}});
        let canned = Arc::new(Canned {
            output: json!({}),
            seen: Default::default(),
        });
        h.dispatch
            .router
            .register(
                nsid,
                [(
                    nsid.to_owned(),
                    Operation {
                        lexicon: Some(LexiconSource { file: tag, carried }),
                        ..Operation::unchecked(Some(OperationKind::Procedure))
                    },
                )],
                ProviderState::Ready(ProviderKind::BuiltIn(
                    Arc::clone(&canned) as Arc<dyn Provider>
                )),
            )
            .unwrap();
        let valid = json!({"part": "dtxt:datum:self/org.example.note/one"});
        h.invoke(h.operator(), nsid, valid.clone()).await.unwrap();
        assert_eq!(*canned.seen.lock().unwrap(), [valid], "{nsid}");
        let Err(DispatchError::InvalidInput(message)) =
            h.invoke(h.operator(), nsid, json!({"part": 7})).await
        else {
            panic!("{nsid}: a value that is not a reference is refused");
        };
        // Refused against the def's own `string` shape at the field's path: unresolved, the
        // `ref` would have been judged as an `object` (the error a cartridge that neither
        // requested nor vendored the defs used to see).
        assert!(message.contains("/part"), "{nsid}: {message}");
        assert!(message.contains(r#"of type "string""#), "{nsid}: {message}");
        // A string that is not a URI is refused at the same path by the def's `format: "uri"`,
        // which an operation's compilation keeps: dispatch has no reference walk to refuse it.
        let Err(DispatchError::InvalidInput(message)) = h
            .invoke(h.operator(), nsid, json!({"part": "not a reference"}))
            .await
        else {
            panic!("{nsid}: a string that is not a URI is refused");
        };
        assert!(message.contains("/part"), "{nsid}: {message}");
        assert!(
            !message.contains("of type"),
            "{nsid}: refused by the format, not as a type error: {message}"
        );
    }
    h.shutdown().await;
}

const INGEST: &str = "org.example.bytes.ingest";
const SPIT: &str = "org.example.bytes.spit";

/// A provider of byte methods. `ingest` pulls its request body and reports each chunk the moment
/// it arrives; `spit` answers a byte body; `lie` is declared JSON and answers bytes.
struct Chunks {
    seen: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

#[async_trait]
impl Provider for Chunks {
    async fn call(
        &self,
        _: &Caller,
        operation: &str,
        _: &RequestContext,
        _: &InvocationContext,
        input: Value,
        body: Option<ByteBody>,
    ) -> Result<Body, ProviderError> {
        match operation {
            INGEST => {
                let mut body = body.expect("a bytes-input method is called with its body");
                let mime = body.mime.clone();
                while let Some(chunk) = body.next().await {
                    self.seen.send(chunk.expect("the chunk")).unwrap();
                }
                Ok(Body::Json(json!({ "mime": mime, "parameters": input })))
            }
            SPIT => Ok(Body::Bytes(
                ByteBody::new(futures::stream::iter([
                    Ok(Bytes::from_static(b"ab")),
                    Ok(Bytes::from_static(b"cd")),
                ]))
                .with_mime("text/plain"),
            )),
            _ => Ok(Body::Bytes(ByteBody::from_bytes(Bytes::from_static(b"x")))),
        }
    }
}

impl Harness {
    fn register_chunks(&self) -> tokio::sync::mpsc::UnboundedReceiver<Bytes> {
        let (seen, chunks) = tokio::sync::mpsc::unbounded_channel();
        let bytes = Encoding::Bytes { mime: None };
        self.dispatch
            .router
            .register(
                "chunks",
                [
                    (
                        INGEST.to_owned(),
                        Operation {
                            input: bytes,
                            ..Operation::unchecked(Some(OperationKind::Procedure))
                        },
                    ),
                    (
                        SPIT.to_owned(),
                        Operation {
                            output: bytes,
                            ..Operation::unchecked(Some(OperationKind::Query))
                        },
                    ),
                    (
                        ECHO.to_owned(),
                        Operation::unchecked(Some(OperationKind::Query)),
                    ),
                ],
                ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(Chunks { seen }))),
            )
            .unwrap();
        chunks
    }

    async fn call_raw(
        &self,
        invoke: Invoke,
    ) -> Result<big_dispatch_core::InvokeOutput, DispatchError> {
        self.dispatch
            .client
            .call_with(RequestContext::root(), self.operator().metadata(), invoke)
            .await
            .map_err(|error| match error {
                ClientError::Method(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })
    }
}

/// A body of the other case than the method declares is refused before the provider is called,
/// in both directions; an answer of the other case is the provider's bug (ADR 042 §2).
#[tokio::test]
async fn a_body_of_the_other_case_than_the_method_declares_is_refused() {
    let h = harness().await;
    let mut chunks = h.register_chunks();
    let mismatch = |expected: &str, found: &str| DispatchError::BodyMismatch {
        expected: expected.to_owned(),
        found: found.to_owned(),
    };
    // JSON for a method whose input is bytes.
    let refused = h
        .call_raw(Invoke::json(INGEST, json!({"n": 1}), Default::default()))
        .await;
    assert_eq!(refused.unwrap_err(), mismatch("bytes", "JSON"));
    // Bytes for a method whose input is JSON (a call that did not come over HTTP, which would
    // have said how to read the body).
    let refused = h
        .call_raw(Invoke {
            operation: ECHO.to_owned(),
            parameters: Value::Null,
            body: Body::Bytes(ByteBody::from_bytes(Bytes::from_static(b"{}"))),
            context: Default::default(),
        })
        .await;
    assert_eq!(refused.unwrap_err(), mismatch("JSON", "bytes"));
    // Neither reached the provider.
    assert!(chunks.try_recv().is_err());
    // The matching cases are served, and the byte answer is declared by its method.
    let spat = h
        .call_raw(Invoke::json(SPIT, json!({}), Default::default()))
        .await
        .unwrap();
    let Body::Bytes(body) = spat.body else {
        panic!("a byte method answers bytes")
    };
    assert_eq!(body.mime.as_deref(), Some("text/plain"));
    assert_eq!(body.collect(16).await.unwrap().as_ref(), b"abcd");
    // A provider that answers bytes for a method declared JSON is a provider failure.
    h.dispatch
        .router
        .register(
            "liar",
            [(
                "org.example.bytes.lie".to_owned(),
                Operation::unchecked(Some(OperationKind::Query)),
            )],
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(Chunks {
                seen: tokio::sync::mpsc::unbounded_channel().0,
            }))),
        )
        .unwrap();
    let lied = h
        .call_raw(Invoke::json(
            "org.example.bytes.lie",
            json!({}),
            Default::default(),
        ))
        .await;
    assert!(
        matches!(lied, Err(DispatchError::InvalidOutput(_))),
        "{lied:?}"
    );
    h.shutdown().await;
}

/// The provider pulls the request body: each chunk reaches it before the sender produces the
/// next (with the stream's own backpressure, a provider that gathered the body first would never
/// see the first chunk and this would not finish), and the parameters the body cannot carry
/// arrive beside it.
#[tokio::test]
async fn a_byte_body_reaches_the_provider_chunk_by_chunk() {
    let h = harness().await;
    let mut seen = h.register_chunks();
    let (send, mut receive) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(1);
    let body = ByteBody::new(futures::stream::poll_fn(move |cx| receive.poll_recv(cx)))
        .with_mime("image/png");
    let call = h.call_raw(Invoke {
        operation: INGEST.to_owned(),
        parameters: json!({"capability": "c"}),
        body: Body::Bytes(body),
        context: Default::default(),
    });
    let sender = async {
        for chunk in [&b"one"[..], b"two", b"three"] {
            send.send(Ok(Bytes::copy_from_slice(chunk))).await.unwrap();
            assert_eq!(seen.recv().await.unwrap().as_ref(), chunk);
        }
        drop(send);
    };
    let (output, ()) = tokio::join!(call, sender);
    assert_eq!(
        output.unwrap().body.into_json(),
        json!({"mime": "image/png", "parameters": {"capability": "c"}})
    );
    h.shutdown().await;
}

/// A local-only method is refused unless the edge transport that accepted the call vouched that
/// it came from the node's own machine, however the call is otherwise authorized (ADR 042 §3).
/// Nothing the caller serializes can claim to be local.
#[tokio::test]
async fn a_local_only_method_is_served_only_to_a_local_origin() {
    use tower::ServiceExt;
    const NSID: &str = "org.example.local.only";
    let h = harness().await;
    h.dispatch
        .router
        .register(
            "local",
            [(
                NSID.to_owned(),
                Operation {
                    local_only: true,
                    ..Operation::unchecked(Some(OperationKind::Query))
                },
            )],
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(Echo::default()))),
        )
        .unwrap();
    let call = |origin| {
        h.call_raw(Invoke::json(
            NSID,
            json!({}),
            InvocationContext {
                origin,
                ..Default::default()
            },
        ))
    };
    // The operator holds every capability and is still refused from a remote origin, which is
    // what a call with no edge behind it, a TCP peer and a nodule's egress all have.
    assert_eq!(
        call(Origin::Remote).await.unwrap_err(),
        DispatchError::LocalOnly
    );
    assert_eq!(
        call(Default::default()).await.unwrap_err(),
        DispatchError::LocalOnly,
        "no origin is not a local one"
    );
    call(Origin::UnixPeer { uid: 1000 }).await.unwrap();
    call(Origin::Embedded).await.unwrap();
    // The origin has no wire form: a serialized call carrying one comes back remote.
    let wire = serde_json::to_value(InvocationContext {
        origin: Origin::Embedded,
        ..Default::default()
    })
    .unwrap();
    let decoded: InvocationContext = serde_json::from_value(wire).unwrap();
    assert_eq!(decoded.origin, Origin::Remote);

    // Through the HTTP surface the listener, not the request, says where a call came from.
    let operator = h.operator();
    let status = |edge, peer: Option<service_utils_rs::xrpc::PeerCredentials>| {
        let router = crate::xrpc_router(
            h.dispatch.client.clone(),
            crate::Anonymous::Acts(operator),
            edge,
        )
        .into_router();
        let router = match peer {
            Some(peer) => router.layer(axum::Extension(peer)),
            None => router,
        };
        async move {
            let response = router
                .oneshot(
                    http::Request::get(format!("/xrpc/{NSID}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            (status, serde_json::from_slice::<Value>(&body).unwrap())
        }
    };
    let peer = service_utils_rs::xrpc::PeerCredentials {
        uid: 1000,
        gid: 1000,
        pid: None,
    };
    let (refused, body) = status(crate::Edge::Tcp, None).await;
    assert_eq!((refused, body["error"].as_str()), (403, Some("LocalOnly")));
    assert_eq!(status(crate::Edge::Unix, Some(peer)).await.0, 200);
    assert_eq!(status(crate::Edge::Embedded, None).await.0, 200);
    h.shutdown().await;
}
