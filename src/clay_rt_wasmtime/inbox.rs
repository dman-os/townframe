//! The host half of a guest's `serve` inbox (ADR 024 §7a).
//!
//! Every call into an incarnation, from another micro or from the host, becomes a
//! [`HostRequest`] in this incarnation's channel. The store's `serve` task reads that channel
//! through a [`HostInbox`] stream producer, which gives each request a [`Responder`] resource
//! in the store's table as it is lowered into the guest. The guest answers through the
//! responder, which resolves the caller's oneshot.
//!
//! Nothing here touches the store from outside it: callers only send into the channel, so a
//! caller on another incarnation's store (or a host-side task) never contends for the
//! provider's store lock.
use super::IncarnationWake;
use super::WasmStoreState;
use super::bindings::townframe::dayframe::lifecycle_host_requests as wit;
use super::wasmtime;
use clay_guest_session::capabilities::CallError;
use clay_guest_session::capabilities::CallTarget;
use clay_guest_session::capabilities::CapabilityFailure;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_host::Lease;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake as _;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use wasmtime::StoreContextMut;
use wasmtime::component::Access;
use wasmtime::component::Destination;
use wasmtime::component::FutureReader;
use wasmtime::component::HasSelf;
use wasmtime::component::Resource;
use wasmtime::component::StreamProducer;
use wasmtime::component::StreamResult;

type Reply = Result<Vec<u8>, CallError>;

/// One admitted request on its way to the guest.
pub(crate) struct HostRequest {
    publication: String,
    payload: Vec<u8>,
    reply: oneshot::Sender<Reply>,
    /// Closes when the caller's future is dropped.
    gone: watch::Receiver<()>,
    slot: Slot,
    retain: Option<Lease<CapabilityPublication>>,
}

/// One unit of an incarnation's in-flight bound, held from admission until the handler answers
/// or drops its responder (or the store drops the responder with it).
struct Slot(Arc<AtomicUsize>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Wakes the provider's driver after the caller's future is gone, and only then, so the
/// driver's next burst observes the closed `gone` channel: the guest's `cancelled` future is a
/// host future the store polls only when something wakes the incarnation.
struct CallerGone {
    sender: Option<watch::Sender<()>>,
    wake: Arc<IncarnationWake>,
}

impl Drop for CallerGone {
    fn drop(&mut self) {
        drop(self.sender.take());
        self.wake.wake_by_ref();
    }
}

/// The sending half of one incarnation's inbox, shared by every Call publication it returned.
pub(crate) struct Inbox {
    sender: Mutex<Option<mpsc::UnboundedSender<HostRequest>>>,
    in_flight: Arc<AtomicUsize>,
    limit: usize,
    wake: Arc<IncarnationWake>,
}

impl Inbox {
    pub(crate) fn new(
        limit: usize,
        wake: Arc<IncarnationWake>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<HostRequest>) {
        let (sender, requests) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                sender: Mutex::new(Some(sender)),
                in_flight: Arc::default(),
                limit,
                wake,
            }),
            requests,
        )
    }

    /// Stop admitting requests and report how many are still in flight. The guest reads
    /// everything already sent, then sees the end of its stream. Nothing is admitted after
    /// this returns, so the count can only fall.
    pub(crate) fn close(&self) -> usize {
        self.sender.lock().unwrap().take();
        self.in_flight.load(Ordering::Acquire)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.sender.lock().unwrap().is_none()
    }

    /// Admit and send one request now, and return the future that awaits its answer. Admission
    /// is checked and counted under the sender lock, so [`Self::close`] never races it.
    fn call(
        &self,
        publication: &str,
        payload: Vec<u8>,
        retain: Option<Lease<CapabilityPublication>>,
    ) -> BoxFuture<'static, Reply> {
        let (reply, response) = oneshot::channel();
        let (gone_sender, gone) = watch::channel(());
        let admitted = {
            let sender = self.sender.lock().unwrap();
            match sender.as_ref() {
                None => Err(CapabilityFailure::Revoked),
                Some(_) if self.in_flight.load(Ordering::Acquire) >= self.limit => {
                    Err(CapabilityFailure::LimitExceeded)
                }
                Some(sender) => {
                    self.in_flight.fetch_add(1, Ordering::AcqRel);
                    // The receiving end lives in the store until it is dropped; a failed send
                    // means the incarnation ended, and drops the request with its slot.
                    sender
                        .send(HostRequest {
                            publication: publication.to_owned(),
                            payload,
                            reply,
                            gone,
                            slot: Slot(Arc::clone(&self.in_flight)),
                            retain,
                        })
                        .map_err(|_ended| CapabilityFailure::Revoked)
                }
            }
        };
        let caller_gone = CallerGone {
            sender: Some(gone_sender),
            wake: Arc::clone(&self.wake),
        };
        if admitted.is_ok() {
            self.wake.wake_by_ref();
        }
        async move {
            let _caller_gone = caller_gone;
            admitted?;
            // A reply dropped unanswered by the store (not by the guest, see
            // [`Responder::abandon`]) means the incarnation ended before its handler answered.
            response
                .await
                .unwrap_or_else(|_ended| Err(CapabilityFailure::Revoked.into()))
        }
        .boxed()
    }
}

/// The [`CallTarget`] of one call publication of one Wasmtime incarnation.
pub(crate) struct GuestCallTarget {
    pub(crate) publication: String,
    pub(crate) inbox: Arc<Inbox>,
}

impl CallTarget for GuestCallTarget {
    fn call(
        &self,
        request: Vec<u8>,
        retain: Option<Lease<CapabilityPublication>>,
    ) -> BoxFuture<'static, Reply> {
        self.inbox.call(&self.publication, request, retain)
    }
}

/// The host state behind a guest `responder` resource.
pub struct Responder {
    reply: Option<oneshot::Sender<Reply>>,
    gone: watch::Receiver<()>,
    _slot: Slot,
    _retain: Option<Lease<CapabilityPublication>>,
}

impl Responder {
    fn respond(&mut self, outcome: Result<Vec<u8>, String>) -> wasmtime::Result<()> {
        let reply = self
            .reply
            .take()
            .ok_or_else(|| wasmtime::Error::msg("guest answered one request twice"))?;
        // A caller that gave up meanwhile just loses its answer.
        drop(reply.send(outcome.map_err(CallError::Provider)));
        Ok(())
    }

    /// The guest dropped its handle without answering. Only this path reports a provider
    /// error: a responder the store drops with itself (forced close, trap) leaves its caller
    /// `Revoked`, which is what happened to it.
    fn abandon(mut self) {
        if let Some(reply) = self.reply.take() {
            drop(reply.send(Err(CallError::Provider(
                "the provider's handler dropped its responder without answering".to_owned(),
            ))));
        }
    }
}

impl wit::Host for WasmStoreState {}

impl wit::HostResponder for WasmStoreState {
    fn respond(
        &mut self,
        responder: Resource<Responder>,
        outcome: Result<Vec<u8>, String>,
    ) -> wasmtime::Result<()> {
        self.table.get_mut(&responder)?.respond(outcome)
    }

    fn drop(&mut self, responder: Resource<Responder>) -> wasmtime::Result<()> {
        self.table.delete(responder)?.abandon();
        Ok(())
    }
}

impl<T: 'static> wit::HostResponderWithStore<T> for HasSelf<WasmStoreState> {
    fn cancelled(
        mut access: Access<'_, T, Self>,
        responder: Resource<Responder>,
    ) -> wasmtime::Result<FutureReader<()>> {
        let mut gone = access.get().table.get(&responder)?.gone.clone();
        FutureReader::new(&mut access, async move {
            // The sender is never sent on, so `changed` fails exactly when the caller's
            // future is dropped, and is already failed if that happened before this call.
            let _ = gone.changed().await;
            Ok::<(), wasmtime::Error>(())
        })
    }
}

/// The host's writable end of the guest's inbox stream: requests from the channel, each given
/// a responder in the store's table as it is lowered. A closed channel ends the stream once
/// the requests already sent are read.
pub(crate) struct HostInbox(pub(crate) mpsc::UnboundedReceiver<HostRequest>);

impl StreamProducer<WasmStoreState> for HostInbox {
    type Item = wit::Request;
    type Buffer = Option<wit::Request>;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, WasmStoreState>,
        mut destination: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        if destination.remaining(&mut store) == Some(0) {
            return Poll::Ready(Ok(StreamResult::Completed));
        }
        match self.0.poll_recv(cx) {
            Poll::Ready(Some(request)) => {
                let HostRequest {
                    publication,
                    payload,
                    reply,
                    gone,
                    slot,
                    retain,
                } = request;
                let responder = store.data_mut().table.push(Responder {
                    reply: Some(reply),
                    gone,
                    _slot: slot,
                    _retain: retain,
                })?;
                destination.set_buffer(Some(wit::Request {
                    publication,
                    payload,
                    responder,
                }));
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending if finish => Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}
