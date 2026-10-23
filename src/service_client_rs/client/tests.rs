use super::*;
use crate::{
    ContextError, EventFrame, HttpMetadata, Operation, ResponseEnvelope, ResponseMetadata,
};
use serde::{Deserialize, Serialize};

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

crate::service_operation!(SubscribeEvents => stream SampleEvent, ContextError, "org.example.client.subscribeEvents");
crate::service_operation!(Echo => String, ContextError, query("org.example.client.echo"));

crate::declare_service_protocol! {
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
    let client = Client::<Protocol>::direct(Arc::new(Route));
    assert_eq!(Protocol::OPERATIONS.unary, &[Echo::XRPC]);
    assert_eq!(Protocol::OPERATIONS.stream, &[SubscribeEvents::XRPC]);
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

#[tokio::test]
async fn end_marker_distinguishes_completion_from_truncation() {
    let event = SampleEvent {
        value: "one".into(),
    };
    for ended in [true, false] {
        let (sender, receiver) = mpsc::channel(3);
        sender
            .send(Ok(EventFrame::Open(ResponseMetadata::default())))
            .await
            .unwrap();
        sender
            .send(Ok(EventFrame::Event(event.clone())))
            .await
            .unwrap();
        if ended {
            sender.send(Ok(EventFrame::End)).await.unwrap();
        }
        drop(sender);
        let mut stream =
            ClientStream::<SubscribeEvents>::open(receiver, None, RequestContext::root())
                .await
                .unwrap()
                .output;
        assert_eq!(stream.recv().await.unwrap(), Some(event.clone()));
        if !ended {
            assert_eq!(
                stream.recv().await,
                Err(ClientError::transport(
                    "subscription closed without an end marker",
                    false
                ))
            );
        }
        assert_eq!(stream.recv().await.unwrap(), None);
    }
}

#[tokio::test]
async fn malformed_establishment_and_duplicate_metadata_are_terminal() {
    for first in [
        EventFrame::End,
        EventFrame::Open(ResponseMetadata::default()),
    ] {
        let (sender, receiver) = mpsc::channel(2);
        sender.send(Ok(first.clone())).await.unwrap();
        sender
            .send(Ok(EventFrame::Open(ResponseMetadata::default())))
            .await
            .unwrap();
        drop(sender);
        let opened =
            ClientStream::<SubscribeEvents>::open(receiver, None, RequestContext::root()).await;
        match first {
            EventFrame::End => assert_eq!(
                opened.unwrap_err(),
                ClientError::transport("subscription event preceded its response metadata", false)
            ),
            EventFrame::Open(_) => {
                let mut stream = opened.unwrap().output;
                assert_eq!(
                    stream.recv().await,
                    Err(ClientError::transport(
                        "subscription sent response metadata twice",
                        false
                    ))
                );
                assert_eq!(stream.recv().await.unwrap(), None);
            }
            EventFrame::Event(_) => unreachable!(),
        }
    }
}

#[tokio::test]
async fn cancellation_preempts_buffered_events_and_is_terminal() {
    let context = RequestContext::root();
    let (sender, receiver) = mpsc::channel(2);
    sender
        .send(Ok(EventFrame::Open(ResponseMetadata::default())))
        .await
        .unwrap();
    sender
        .send(Ok(EventFrame::Event(SampleEvent {
            value: "buffered".into(),
        })))
        .await
        .unwrap();
    let mut stream = ClientStream::<SubscribeEvents>::open(receiver, None, context.clone())
        .await
        .unwrap()
        .output;
    context.cancel();
    assert_eq!(
        stream.recv().await,
        Err(ClientError::Operation(ContextError::Cancelled))
    );
    sender.closed().await;
    assert_eq!(stream.recv().await.unwrap(), None);
}
