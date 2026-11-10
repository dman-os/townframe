use super::*;
use futures::future::BoxFuture;
use method_core::ContextError;
use method_core::Method;
use serde::Deserialize;
use serde::Serialize;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::ResponseMetadata;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SampleEvent {
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SubscribeEvents {}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Echo {
    value: String,
}

service_core_rs::service_method!(SubscribeEvents => stream SampleEvent, ContextError, "org.example.client.subscribeEvents");
service_core_rs::service_method!(Echo => String, ContextError, query("org.example.client.echo"));

service_core_rs::declare_service_protocol! {
    enum Protocol => Message {
        unary { Echo(Echo) }
        stream { SubscribeEvents(SubscribeEvents) }
    }
}

struct Route;
impl InlineRoute<Protocol> for Route {
    fn route_inline(self: Arc<Self>, message: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            match message {
                Message::Echo(message) => {
                    drop(
                        message
                            .tx
                            .send(Ok(ResponseEnvelope {
                                output: message.inner.params.value,
                                metadata: ResponseMetadata {
                                    etag: Some("client-only".into()),
                                    ..ResponseMetadata::default()
                                },
                            }))
                            .await,
                    );
                }
                Message::SubscribeEvents(message) => {
                    message
                        .tx
                        .send(Ok(EventFrame::Open(ResponseMetadata::default())))
                        .await
                        .unwrap();
                    message
                        .tx
                        .send(Ok(EventFrame::Event(SampleEvent {
                            value: "client-only".into(),
                        })))
                        .await
                        .unwrap();
                    message.tx.send(Ok(EventFrame::End)).await.unwrap();
                }
            }
        })
    }
}

#[tokio::test]
async fn protocol_and_direct_client_need_no_server_dependencies() {
    let client = Client::<Protocol>::new(Transport::direct(Arc::new(Route)));
    assert_eq!(Protocol::METHODS.unary, &[Echo::XRPC]);
    assert_eq!(Protocol::METHODS.stream, &[SubscribeEvents::XRPC]);
    assert_eq!(Echo::XRPC.output_schema, "String");
    assert_eq!(SubscribeEvents::XRPC.output_schema, "SampleEvent");
    assert_eq!(Echo::XRPC.error_schema, "ContextError");
    assert_eq!(
        client
            .call_with_metadata(Envelope::with_metadata(
                RequestContext::root(),
                HttpMetadata::default(),
                Echo {
                    value: "typed".into()
                }
            ))
            .await
            .unwrap(),
        ResponseEnvelope {
            output: "typed".to_owned(),
            metadata: ResponseMetadata {
                etag: Some("client-only".into()),
                ..ResponseMetadata::default()
            },
        }
    );
    let mut events = client
        .subscribe(RequestContext::root(), SubscribeEvents {})
        .await
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap(),
        Some(SampleEvent {
            value: "client-only".into()
        })
    );
    assert_eq!(events.recv().await.unwrap(), None);
}
