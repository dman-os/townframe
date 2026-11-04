//! Tiny `wasi:http/handler@0.3.0` guest for exercising the nodule Wasm provider.
//!
//! The host dispatches XRPC calls to this component as plain HTTP requests, and
//! the header contract is `authorization: Capability <cap-id>`,
//! `idempotency-key`, `x-request-deadline-unix-ms` and `x-nodule-capability`
//! (the nodule's own dispatch capability, which the guest presents on its
//! outgoing calls).
//!
//! Routes (all `POST`), served for any NSID whose last segment is the route's
//! name, so one component can stand in for several nodules:
//! - `/xrpc/<nsid>.echo` answers 200 with
//!   `{ input, authorization, idempotencyKey, deadline, noduleCapability }`,
//!   where the header members are `null` when absent.
//! - `/xrpc/<nsid>.relay` takes `{ operation, authority?, input }` and
//!   POSTs `input` to `http://dispatch.local/xrpc/<operation>` with
//!   `authorization: Capability <authority>`, where `authority` defaults to the
//!   capability id in `x-nodule-capability`. It forwards the incoming
//!   `idempotency-key` and `x-request-deadline-unix-ms` unchanged and answers
//!   200 with `{ status, body }` (`body` is the upstream JSON, or a string when
//!   the upstream body is not JSON) and 502 when the outgoing request fails.
//! - Any other `/xrpc/` operation answers 404 `{ "error": "UnknownOperation" }`
//!   and any other path answers 404 `{ "error": "NotFound" }`.

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        generate_all,
        async: ["export:wasi:http/handler@0.3.0#handle"],
    });
}

use serde_json::{Value, json};
use wit::wasi::http::client;
use wit::wasi::http::types::{ErrorCode, Fields, Method, Request, Response, Scheme};

struct Component;
wit::export!(Component with_types_in wit);

impl wit::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let is_post = matches!(request.get_method(), Method::Post);
        let path = request.get_path_with_query().unwrap_or_default();
        let path = path.split('?').next().unwrap_or_default().to_owned();
        let headers = request.get_headers();
        let header = |name: &str| -> Option<String> {
            headers
                .get(name)
                .into_iter()
                .next()
                .map(|value| String::from_utf8(value).expect("non-utf8 header value"))
        };
        let authorization = header("authorization");
        let idempotency_key = header("idempotency-key");
        let deadline = header("x-request-deadline-unix-ms");
        let nodule_capability = header("x-nodule-capability");
        drop(headers);

        let Some(operation) = path.strip_prefix("/xrpc/").filter(|_| is_post) else {
            return Ok(json_response(404, &json!({ "error": "NotFound" })));
        };
        let route = operation.rsplit('.').next().unwrap_or_default();
        if !matches!(route, "echo" | "relay") {
            return Ok(json_response(
                404,
                &json!({ "error": "UnknownOperation" }),
            ));
        }
        let route = route.to_owned();

        let body = read_request_body(request).await;
        let Ok(input) = serde_json::from_slice::<Value>(&body) else {
            return Ok(json_response(400, &json!({ "error": "InvalidRequest" })));
        };

        if route == "echo" {
            return Ok(json_response(
                200,
                &json!({
                    "input": input,
                    "authorization": authorization,
                    "idempotencyKey": idempotency_key,
                    "deadline": deadline,
                    "noduleCapability": nodule_capability,
                }),
            ));
        }

        let authority = input
            .get("authority")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                nodule_capability
                    .as_deref()
                    .and_then(|value| value.strip_prefix("Capability "))
                    .map(str::to_owned)
            });
        let (Some(target), Some(authority)) = (
            input.get("operation").and_then(Value::as_str),
            authority.as_deref(),
        ) else {
            return Ok(json_response(400, &json!({ "error": "InvalidRequest" })));
        };
        let upstream_body = serde_json::to_vec(input.get("input").unwrap_or(&Value::Null))
            .expect("serializing a Value cannot fail");
        match relay(
            target,
            authority,
            idempotency_key.as_deref(),
            deadline.as_deref(),
            upstream_body,
        )
        .await
        {
            Ok((status, body)) => {
                let body = serde_json::from_slice::<Value>(&body)
                    .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
                Ok(json_response(200, &json!({ "status": status, "body": body })))
            }
            Err(error) => Ok(json_response(
                502,
                &json!({ "error": "UpstreamFailed", "message": format!("{error:?}") }),
            )),
        }
    }
}

async fn read_request_body(request: Request) -> Vec<u8> {
    let (result_tx, result_rx) = wit::wit_future::new(|| Ok(()));
    let (body, _trailers) = Request::consume_body(request, result_rx);
    let bytes = body.collect().await;
    result_tx
        .write(Ok(()))
        .await
        .expect("request body receiver disappeared");
    bytes
}

/// Sends the nested call through the host-provided `dispatch.local` egress.
async fn relay(
    operation: &str,
    authority: &str,
    idempotency_key: Option<&str>,
    deadline: Option<&str>,
    body: Vec<u8>,
) -> Result<(u16, Vec<u8>), ErrorCode> {
    let headers = Fields::new();
    let set = |name: &str, value: &str| {
        headers
            .append(name, value.as_bytes())
            .expect("invalid outgoing header");
    };
    set("authorization", &format!("Capability {authority}"));
    set("content-type", "application/json");
    if let Some(value) = idempotency_key {
        set("idempotency-key", value);
    }
    if let Some(value) = deadline {
        set("x-request-deadline-unix-ms", value);
    }

    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (request, _completion) = Request::new(headers, Some(body_rx), trailers_rx, None);
    request.set_method(&Method::Post).expect("set method");
    request.set_scheme(Some(&Scheme::Http)).expect("set scheme");
    request
        .set_authority(Some("dispatch.local"))
        .expect("set authority");
    request
        .set_path_with_query(Some(&format!("/xrpc/{operation}")))
        .expect("set path");

    wit_bindgen::spawn_local(async move {
        let remaining = body_tx.write_all(body).await;
        assert!(remaining.is_empty(), "outgoing body stream closed early");
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("outgoing trailers receiver disappeared");
    });

    let response = client::send(request).await?;
    let status = response.get_status_code();
    let (result_tx, result_rx) = wit::wit_future::new(|| Ok(()));
    let (body, _trailers) = Response::consume_body(response, result_rx);
    let bytes = body.collect().await;
    result_tx
        .write(Ok(()))
        .await
        .expect("response body receiver disappeared");
    Ok((status, bytes))
}

fn json_response(status: u16, value: &Value) -> Response {
    let headers = Fields::new();
    headers
        .append("content-type", b"application/json")
        .expect("invalid content-type header");
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (response, _completion) = Response::new(headers, Some(body_rx), trailers_rx);
    response.set_status_code(status).expect("invalid status");
    let bytes = serde_json::to_vec(value).expect("serializing a Value cannot fail");
    wit_bindgen::spawn_local(async move {
        let remaining = body_tx.write_all(bytes).await;
        assert!(remaining.is_empty(), "response body stream closed early");
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("response trailers receiver disappeared");
    });
    response
}
