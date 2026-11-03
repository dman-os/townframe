use futures::StreamExt as _;
use futures::future::Either;
use futures::future::select;
use futures::stream::FuturesUnordered;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

wit_bindgen::generate!({
    path: "wit",
    world: "spike",
});

use exports::townframe::inbox_spike::server;
use townframe::inbox_spike::inbox::Request;

static IN_FLIGHT: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static IN_FLIGHT_LOCAL: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Counts a handler as in flight from its creation until it is dropped, finished or not.
struct InFlight;

impl InFlight {
    fn new() -> Self {
        IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        IN_FLIGHT_LOCAL.with(|count| count.set(count.get() + 1));
        Self
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        IN_FLIGHT_LOCAL.with(|count| count.set(count.get() - 1));
    }
}

async fn handle(request: Request) {
    let _counted = InFlight::new();
    let Request {
        publication,
        payload,
        responder,
    } = request;
    match publication.as_str() {
        "echo" => responder.respond(Ok(&payload)),
        "gate" => {
            let name = String::from_utf8(payload).expect("gate names are text");
            let released = townframe::inbox_spike::inbox::gate(name).await;
            responder.respond(Ok(&released));
        }
        "drop" => drop(responder),
        other => responder.respond(Err(&format!("no handler {other:?}"))),
    }
}

type Inbox = wit_bindgen::rt::async_support::StreamReader<Request>;

async fn read_one(mut inbox: Inbox) -> (Inbox, Option<Request>) {
    let request = inbox.next().await;
    (inbox, request)
}

struct Spike;

impl server::Guest for Spike {
    async fn serve(inbox: Inbox) {
        let mut handlers = FuturesUnordered::new();
        // The read stays pending across handler completions: dropping a pending read would
        // cancel it and could lose a request that arrived in that window.
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
            // The host dropped its writable end: stop reading, drain what is in flight.
            if let Some(request) = request {
                handlers.push(handle(request));
                reading = Some(Box::pin(read_one(inbox)));
            }
        }
        townframe::inbox_spike::inbox::finished();
    }

    fn in_flight() -> u32 {
        IN_FLIGHT.load(Ordering::SeqCst)
    }

    fn in_flight_thread_local() -> u32 {
        IN_FLIGHT_LOCAL.with(std::cell::Cell::get)
    }
}

export!(Spike);
