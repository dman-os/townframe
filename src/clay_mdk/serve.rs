//! The MDK-owned `serve` main loop (ADR 024 §7a). The host starts `serve` once per incarnation
//! and feeds it every call routed to the micro; [`run`] reads the inbox, runs one handler per
//! request concurrently inside the serve task, honours `responder.cancelled`, and drains
//! in-flight handlers when the host closes the inbox. No task is spawned: wit-bindgen scopes
//! spawned work to the export task that spawned it, so handlers are futures this one task
//! polls itself.
//!
//! Thread-local rule: handlers run in the `serve` task, which does not share thread-local state
//! with the sync presentation exports. State both sides touch must be a `static` or cross
//! through the MDK; this module and [`crate::guest`] keep the handler registry in a `static`.
use crate::lifecycle_host_requests::Request;
use futures::StreamExt as _;
use futures::future::Either;
use futures::future::LocalBoxFuture;
use futures::future::select;
use futures::stream::FuturesUnordered;
use std::future::Future;
use std::future::IntoFuture as _;
use std::pin::Pin;
use wit_bindgen::rt::async_support::StreamReader;

/// What a handler answers: the response bytes, or its own rejection.
pub type Outcome = Result<Vec<u8>, String>;

/// The guest end of the host's request stream.
pub type Inbox = StreamReader<Request>;

async fn read_one(mut inbox: Inbox) -> (Inbox, Option<Request>) {
    let request = inbox.next().await;
    (inbox, request)
}

/// Answer one request with `handler`'s outcome, unless its caller gives up first, in which case
/// the handler future is dropped (cancelling whatever it awaits) and nothing is answered.
async fn answer(request: Request, handler: LocalBoxFuture<'static, Outcome>) {
    let Request {
        publication: _,
        payload: _,
        responder,
    } = request;
    let cancelled = Box::pin(responder.cancelled().into_future());
    match select(handler, cancelled).await {
        Either::Left((outcome, _)) => responder.respond(outcome.as_deref().map_err(String::as_str)),
        Either::Right(_) => drop(responder),
    }
}

/// Run the loop until the host closes the inbox and every in-flight handler has finished.
/// `handle(publication, payload)` builds the handler future for one request.
pub async fn run(inbox: Inbox, handle: impl Fn(&str, Vec<u8>) -> LocalBoxFuture<'static, Outcome>) {
    let mut handlers = FuturesUnordered::new();
    // The read stays pending across handler completions: dropping a pending read would cancel
    // it and could lose a request that arrived in that window.
    let mut reading: Option<Pin<Box<dyn Future<Output = (Inbox, Option<Request>)>>>> =
        Some(Box::pin(read_one(inbox)));
    loop {
        let arrived = match (reading.take(), handlers.is_empty()) {
            (None, true) => break,
            (None, false) => {
                handlers.next().await;
                continue;
            }
            (Some(read), true) => read.await,
            (Some(read), false) => match select(read, handlers.next()).await {
                Either::Left((arrived, _)) => arrived,
                Either::Right((_, read)) => {
                    reading = Some(read);
                    continue;
                }
            },
        };
        let (inbox, request) = arrived;
        // `None`: the host closed the inbox. Stop reading and drain what is in flight.
        if let Some(mut request) = request {
            let handler = handle(&request.publication, std::mem::take(&mut request.payload));
            handlers.push(answer(request, handler));
            reading = Some(Box::pin(read_one(inbox)));
        }
    }
}
