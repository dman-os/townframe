//! Host-side pieces both engines' spike tests share: the request channel that feeds the guest's
//! inbox stream, the responder resource's host state, host-controlled gates, and the scenarios.
//! Each engine module supplies only `run`, which instantiates the fixture, starts `serve` with
//! the inbox as a stream, and hands the scenario an [`Fx`].
use futures::future::LocalBoxFuture;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// What the host learns about a request: the handler's answer, or that its responder was
/// dropped unanswered (the receiver errors).
pub(crate) type Reply = Result<Vec<u8>, String>;

pub(crate) struct HostRequest {
    pub(crate) publication: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) reply: oneshot::Sender<Reply>,
}

/// The host's writable end of the inbox stream. Dropping it (or [`Inbox::close`]) ends the
/// stream the guest reads.
pub(crate) struct Inbox(Option<mpsc::UnboundedSender<HostRequest>>);

pub(crate) fn inbox() -> (Inbox, mpsc::UnboundedReceiver<HostRequest>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (Inbox(Some(sender)), receiver)
}

impl Inbox {
    pub(crate) fn request(&self, publication: &str, payload: &[u8]) -> oneshot::Receiver<Reply> {
        let (reply, response) = oneshot::channel();
        self.0
            .as_ref()
            .expect("the inbox is open")
            .send(HostRequest {
                publication: publication.to_owned(),
                payload: payload.to_vec(),
                reply,
            })
            .expect("the engine holds the receiving end until serve ends");
        response
    }

    pub(crate) fn close(&mut self) {
        self.0 = None;
    }
}

/// The host state behind a guest `responder` resource. Dropping it unanswered drops the
/// sender, which the requester observes as a closed channel.
pub struct Responder(Option<oneshot::Sender<Reply>>);

impl Responder {
    pub(crate) fn new(reply: oneshot::Sender<Reply>) -> Self {
        Self(Some(reply))
    }

    pub(crate) fn respond(&mut self, outcome: Reply) {
        let reply = self.0.take().expect("a responder answers once");
        drop(reply.send(outcome));
    }
}

/// Host imports the guest suspends on until the test releases them, by name.
#[derive(Clone, Default)]
pub(crate) struct Gates(Arc<Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>>);

impl Gates {
    /// The guest's `gate(name)` import: register the gate and wait to be released.
    pub(crate) fn open(&self, name: String) -> oneshot::Receiver<Vec<u8>> {
        let (release, released) = oneshot::channel();
        assert!(self.0.lock().unwrap().insert(name, release).is_none());
        released
    }

    pub(crate) async fn entered(&self, name: &str) {
        while !self.0.lock().unwrap().contains_key(name) {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn release(&self, name: &str, payload: &[u8]) {
        let release = self
            .0
            .lock()
            .unwrap()
            .remove(name)
            .expect("the gate was entered");
        release
            .send(payload.to_vec())
            .expect("the guest still waits");
    }
}

/// What a scenario drives: the inbox, the gates, a sync `in-flight` export call made while the
/// engine runs the serve task, and whether `serve` has returned.
pub(crate) struct Fx<'a> {
    pub(crate) inbox: Inbox,
    pub(crate) gates: Gates,
    pub(crate) served: Arc<AtomicBool>,
    pub(crate) in_flight: Box<dyn Fn() -> LocalBoxFuture<'a, u32> + 'a>,
    /// The same count read from the guest's `thread_local` state.
    pub(crate) in_flight_thread_local: Box<dyn Fn() -> LocalBoxFuture<'a, u32> + 'a>,
}

/// Run `$run(async |fx| ..)` for each scenario, as tests of the engine module invoking it.
macro_rules! scenarios {
    ($run:path) => {
        async fn settle() {
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
        }

        /// Point 1: a `stream<request>` whose items own a resource crosses, and each request
        /// reaches a handler that answers through its responder.
        #[tokio::test]
        async fn stream_of_records_with_resources_crosses() {
            $run(async |fx| {
                let echoed = fx.inbox.request("echo", b"hello");
                assert_eq!(echoed.await.unwrap(), Ok(b"hello".to_vec()));
                let unknown = fx.inbox.request("nope", b"");
                assert!(matches!(unknown.await.unwrap(), Err(message) if message.contains("nope")));
            })
            .await;
        }

        /// Points 2 and 3: two requests are in flight in one instance, each waiting on its own
        /// host gate, a sync export enters meanwhile, and they complete out of order.
        #[tokio::test]
        async fn two_requests_are_in_flight_and_complete_out_of_order() {
            $run(async |fx| {
                assert_eq!((fx.in_flight)().await, 0);
                let mut first = fx.inbox.request("gate", b"a");
                let mut second = fx.inbox.request("gate", b"b");
                fx.gates.entered("a").await;
                fx.gates.entered("b").await;
                assert_eq!((fx.in_flight)().await, 2, "both handlers are in flight");
                fx.gates.release("b", b"B");
                assert_eq!((&mut second).await.unwrap(), Ok(b"B".to_vec()));
                assert!(
                    first.try_recv().is_err(),
                    "the first request is still waiting on its gate"
                );
                assert_eq!((fx.in_flight)().await, 1);
                fx.gates.release("a", b"A");
                assert_eq!((&mut first).await.unwrap(), Ok(b"A".to_vec()));
                assert_eq!((fx.in_flight)().await, 0);
            })
            .await;
        }

        /// Finding: guest `thread_local` state written by the serve task and its handlers is
        /// not what a sync export entered while they are suspended reads (the export gets its
        /// own thread-local block), so guest state shared with sync exports must be a
        /// `static`. Pins today's toolchain behaviour; delete it when that changes.
        #[tokio::test]
        async fn sync_export_does_not_see_serve_task_thread_locals() {
            $run(async |fx| {
                let mut held = fx.inbox.request("gate", b"held");
                fx.gates.entered("held").await;
                assert_eq!((fx.in_flight)().await, 1);
                assert_eq!((fx.in_flight_thread_local)().await, 0);
                fx.gates.release("held", b"done");
                assert_eq!((&mut held).await.unwrap(), Ok(b"done".to_vec()));
            })
            .await;
        }

        /// Point 4: a handler suspended on a host import does not block other requests.
        #[tokio::test]
        async fn handler_awaiting_a_host_import_blocks_nobody() {
            $run(async |fx| {
                let mut slow = fx.inbox.request("gate", b"slow");
                fx.gates.entered("slow").await;
                for round in 0..3u8 {
                    let echoed = fx.inbox.request("echo", &[round]);
                    assert_eq!(echoed.await.unwrap(), Ok(vec![round]));
                }
                assert!(slow.try_recv().is_err());
                fx.gates.release("slow", b"done");
                assert_eq!((&mut slow).await.unwrap(), Ok(b"done".to_vec()));
            })
            .await;
        }

        /// Point 5: dropping the host's writable end ends `serve` only after the in-flight
        /// handlers finish; an unanswered responder is observable as a closed channel.
        #[tokio::test]
        async fn closing_the_inbox_drains_handlers_then_serve_returns() {
            $run(async |mut fx| {
                let dropped = fx.inbox.request("drop", b"");
                assert!(dropped.await.is_err(), "a dropped responder is observable");
                let mut last = fx.inbox.request("gate", b"last");
                fx.gates.entered("last").await;
                fx.inbox.close();
                settle().await;
                assert!(
                    !fx.served.load(Ordering::Acquire),
                    "serve waits for its in-flight handler"
                );
                assert_eq!((fx.in_flight)().await, 1);
                fx.gates.release("last", b"drained");
                assert_eq!((&mut last).await.unwrap(), Ok(b"drained".to_vec()));
                while !fx.served.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await;
        }
    };
}
