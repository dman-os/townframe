use crate::Cx;
use crate::Handler;
use crate::cx::call_through;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use method_core::ContextError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_core_rs::ClientError;
use service_core_rs::RequestContext;
use service_core_rs::TraceContext;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum TestError {
    Context(ContextError),
    Refused { reason: String },
}

impl From<ContextError> for TestError {
    fn from(error: ContextError) -> Self {
        Self::Context(error)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Greet {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Whoami {}

method_core::methods! {
    error = TestError, nsid_prefix = "org.example.test.";
    Greet => String, procedure, "greet";
    Whoami => Value, procedure, "whoami";
}

#[derive(Default)]
struct Greeter;

impl Handler<Greet> for Greeter {
    async fn call(&self, _cx: &Cx, input: Greet) -> Result<String, TestError> {
        if input.name.is_empty() {
            return Err(TestError::Refused {
                reason: "no name".to_owned(),
            });
        }
        Ok(format!("hello {}", input.name))
    }
}

impl Handler<Whoami> for Greeter {
    async fn call(&self, cx: &Cx, _input: Whoami) -> Result<Value, TestError> {
        Ok(json!({
            "caller": cx.caller_capability(),
            "nodule": cx.nodule_capability(),
            "idempotencyKey": cx.idempotency_key(),
            "requestId": cx.request().request_id,
        }))
    }
}

crate::export_nodule!(Greeter => [Greet, Whoami]);

async fn post(path: &str, headers: &[(&str, &str)], body: &str) -> (StatusCode, Value, HeaderMap) {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        map.insert(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let response = crate::serve(&Greeter, &Method::POST, path, &map, body.as_bytes()).await;
    let (parts, body) = response.into_parts();
    (
        parts.status,
        serde_json::from_slice(&body).unwrap(),
        parts.headers,
    )
}

#[tokio::test]
async fn a_method_is_served_under_its_nsid_with_the_request_id_echoed() {
    let id = "0198f0b2-7c2e-7d3a-8f1e-2b4c6d8e0a1b";
    let (status, body, headers) = post(
        "/xrpc/org.example.test.greet",
        &[("x-request-id", id)],
        r#"{"name":"ada"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!("hello ada"));
    assert_eq!(headers["x-request-id"], id);
    assert_eq!(headers["content-type"], "application/json");
}

#[tokio::test]
async fn a_domain_error_is_the_envelope_the_router_writes() {
    let (status, body, _) = post("/xrpc/org.example.test.greet", &[], r#"{"name":""}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({
            "error": "TestError",
            "message": "Refused { reason: \"no name\" }",
            "operationError": { "Refused": { "reason": "no name" } },
        })
    );
}

#[tokio::test]
async fn refusals_that_are_not_the_methods_are_bare_errors() {
    let unknown = post("/xrpc/org.example.test.other", &[], "{}").await;
    assert_eq!(
        (unknown.0, unknown.1),
        (
            StatusCode::NOT_FOUND,
            json!({ "error": "UnknownOperation" })
        )
    );
    let elsewhere = post("/health", &[], "{}").await;
    assert_eq!(
        (elsewhere.0, elsewhere.1),
        (StatusCode::NOT_FOUND, json!({ "error": "NotFound" }))
    );
    let get = crate::serve(
        &Greeter,
        &Method::GET,
        "/xrpc/org.example.test.greet",
        &HeaderMap::new(),
        b"",
    )
    .await;
    assert_eq!(get.status(), StatusCode::NOT_FOUND);

    let malformed = post("/xrpc/org.example.test.greet", &[], "{").await;
    assert_eq!(malformed.0, StatusCode::BAD_REQUEST);
    assert_eq!(malformed.1["error"], "InvalidRequest");
    let bad_header = post(
        "/xrpc/org.example.test.greet",
        &[("x-timeout-ms", "soon")],
        r#"{"name":"ada"}"#,
    )
    .await;
    assert_eq!(bad_header.0, StatusCode::BAD_REQUEST);
    assert_eq!(bad_header.1["error"], "InvalidRequest");
}

#[tokio::test]
async fn cx_reads_the_delivered_headers() {
    let (status, body, _) = post(
        "/xrpc/org.example.test.whoami",
        &[
            ("authorization", "bearer caller-cap"),
            ("x-nodule-capability", "own-cap"),
            ("idempotency-key", "key-1"),
        ],
        "{}",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["caller"], "caller-cap");
    assert_eq!(body["nodule"], "own-cap");
    assert_eq!(body["idempotencyKey"], "key-1");

    let (_, bare, _) = post("/xrpc/org.example.test.whoami", &[], "{}").await;
    assert_eq!(bare["caller"], Value::Null);
    assert_eq!(bare["nodule"], Value::Null);
}

#[tokio::test]
async fn a_spent_budget_is_the_methods_deadline_error_and_the_handler_is_not_entered() {
    let (status, body, _) = post(
        "/xrpc/org.example.test.greet",
        &[("x-timeout-ms", "0")],
        r#"{"name":"ada"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["operationError"],
        json!({ "Context": "DeadlineExceeded" })
    );
}

/// Answers every call with an echo of what it was sent, recording the request.
#[derive(Default)]
struct Recording(Mutex<Vec<XrpcRequest>>);

impl HttpTransport for &Recording {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        self.0.lock().unwrap().push(request);
        Ok(XrpcResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: serde_json::to_vec("hello back").unwrap(),
        })
    }
}

fn cx_with(nodule: Option<&str>, budget: Option<Duration>) -> Cx {
    let mut request = RequestContext::root();
    request.trace = TraceContext::from_headers(
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        None,
    );
    if let Some(budget) = budget {
        request = request.with_time_budget(budget);
    }
    Cx {
        request,
        caller: Some("caller-cap".to_owned()),
        nodule: nodule.map(str::to_owned),
        idempotency_key: Some("outer-key".to_owned()),
    }
}

// These run on `futures`' bare executor, as a guest does: no tokio runtime, so a timer in the
// call path would panic.
#[test]
fn a_nested_call_is_the_next_hop_authorized_with_the_nodules_own_capability() {
    let transport = Recording::default();
    let cx = cx_with(Some("own-cap"), Some(Duration::from_secs(60)));
    let output = futures::executor::block_on(call_through(
        &transport,
        &cx,
        Greet {
            name: "ada".to_owned(),
        },
    ))
    .unwrap();
    assert_eq!(output, "hello back");

    let sent = transport.0.lock().unwrap();
    let [request] = sent.as_slice() else {
        panic!("one request, got {}", sent.len());
    };
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path, "xrpc/org.example.test.greet");
    assert_eq!(request.headers["authorization"], "Bearer own-cap");
    assert!(
        !request.headers.contains_key("idempotency-key"),
        "a different call is not the same command"
    );
    assert_ne!(
        request.headers["x-request-id"].to_str().unwrap(),
        cx.request().request_id.to_string(),
        "each hop has its own request id"
    );
    let traceparent = request.headers["traceparent"].to_str().unwrap();
    assert!(traceparent.contains("0af7651916cd43dd8448eb211c80319c"));
    assert!(!traceparent.contains("b7ad6b7169203331"), "a new span");
    let timeout: u64 = request.headers["x-timeout-ms"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=60_000).contains(&timeout), "{timeout}");
}

#[test]
fn a_nested_call_without_a_capability_of_its_own_is_unauthenticated() {
    let transport = Recording::default();
    let result =
        futures::executor::block_on(call_through(&transport, &cx_with(None, None), Whoami {}));
    assert!(matches!(
        result,
        Err(ClientError::Method(TestError::Context(
            ContextError::Unauthenticated
        )))
    ));
    assert!(transport.0.lock().unwrap().is_empty());
}
