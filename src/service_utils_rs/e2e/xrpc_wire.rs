//! What is specific to reaching the sample over HTTP: the wire shape of a
//! typed domain error, and how transport loss is classified per operation
//! kind. The HTTP server and client themselves are tested in `xrpc::tests`.

use super::Stack;
use crate::Client;
use crate::ClientError;
use crate::Operation;
use crate::RequestContext;
use crate::XrpcClient;
use crate::sample::GetValue;
use crate::sample::PutValue;
use crate::sample::SampleError;
use crate::sample::SampleProtocol;
use crate::xrpc::DOMAIN_ERROR_FIELD;
use crate::xrpc::DOMAIN_ERROR_STATUS;
use url::Url;

#[tokio::test]
async fn domain_errors_travel_in_the_standard_envelope_with_a_typed_detail() {
    let stack = Stack::boot(2, 1).await;
    let response = reqwest::Client::new()
        .post(
            stack
                .http_base
                .join("xrpc/org.example.sample.putValue")
                .expect("procedure URL"),
        )
        .json(&PutValue {
            value: String::new(),
        })
        .send()
        .await
        .expect("HTTP response");
    assert_eq!(response.status(), DOMAIN_ERROR_STATUS);
    let body: serde_json::Value = response.json().await.expect("JSON envelope");
    assert_eq!(body["error"], PutValue::XRPC.error_schema);
    assert!(body["message"].is_string());
    let detail: SampleError =
        serde_json::from_value(body[DOMAIN_ERROR_FIELD].clone()).expect("typed detail");
    assert!(matches!(detail, SampleError::InvalidValue { .. }));
    stack.shutdown().await;
}

#[tokio::test]
async fn connection_loss_is_outcome_unknown_only_for_procedures() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve a port");
    let address = listener.local_addr().expect("address");
    drop(listener);
    let client = Client::<SampleProtocol>::xrpc(
        XrpcClient::new(
            Url::parse(&format!("http://{address}/")).expect("base URL"),
            1024,
        )
        .expect("client"),
    );

    let query = client.call(RequestContext::root(), GetValue {}).await;
    let procedure = client
        .call(RequestContext::root(), PutValue { value: "x".into() })
        .await;
    assert!(matches!(
        query,
        Err(ClientError::Transport {
            outcome_unknown: false,
            ..
        })
    ));
    assert!(matches!(
        procedure,
        Err(ClientError::Transport {
            outcome_unknown: true,
            ..
        })
    ));
}
