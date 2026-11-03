//! The spike on the WCMP polyfill, over its wasmi backend. The polyfill has no `Store::spawn`,
//! so `serve` and the scenarios run inside one `run_concurrent`: the guest's serve task, its
//! handlers and the host's gate releases are all driven by the store's event loop there.
use crate::host::Fx;
use crate::host::Gates;
use crate::host::HostRequest;
use crate::host::Responder;
use crate::host::inbox;
use futures::FutureExt as _;
use std::cell::RefCell;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use tokio::sync::mpsc;
use wcmp::Accessor;
use wcmp::Component;
use wcmp::Destination;
use wcmp::Engine;
use wcmp::HasSelf;
use wcmp::Linker;
use wcmp::Resource;
use wcmp::ResourceTable;
use wcmp::Store;
use wcmp::StoreContext;
use wcmp::StreamProducer;
use wcmp::StreamReader;
use wcmp::StreamResult;

mod bindings {
    wcmp::bindgen!({
        path: "wit",
        world: "spike",
        imports: { default: trappable },
        with: {
            "townframe:inbox-spike/inbox.responder": crate::host::Responder,
        },
    });
}

use bindings::Spike;
use bindings::townframe::inbox_spike::inbox;

struct State {
    table: ResourceTable,
    gates: Gates,
    /// Set by the guest's `finished` import at the end of `serve`.
    finished: Arc<AtomicBool>,
}

impl inbox::Host for State {
    fn finished(&mut self) -> wcmp::Result<()> {
        self.finished.store(true, Ordering::Release);
        Ok(())
    }
}

impl inbox::HostResponder for State {
    fn respond(
        &mut self,
        responder: Resource<Responder>,
        outcome: Result<Vec<u8>, String>,
    ) -> wcmp::Result<()> {
        self.table.get_mut(&responder)?.respond(outcome);
        Ok(())
    }

    fn drop(&mut self, responder: Resource<Responder>) -> wcmp::Result<()> {
        self.table.delete(responder)?;
        Ok(())
    }
}

impl<T: 'static> inbox::HostWithStore<T> for HasSelf<State> {
    async fn gate(accessor: &Accessor<T, Self>, name: String) -> wcmp::Result<Vec<u8>> {
        let released = accessor.with(|mut access| access.get().gates.open(name));
        released.await.map_err(|error| wcmp::Error::Internal {
            message: error.to_string(),
        })
    }
}

/// The host's writable end of the inbox stream, as in the Wasmtime module.
struct HostInbox(mpsc::UnboundedReceiver<HostRequest>);

impl StreamProducer<State> for HostInbox {
    type Item = inbox::Request;

    fn poll_produce(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: &mut StoreContext<'_, State>,
        mut destination: Destination<'_, Self::Item>,
        finish: bool,
    ) -> Poll<wcmp::Result<StreamResult>> {
        if destination.remaining() == Some(0) {
            return Poll::Ready(Ok(StreamResult::Completed));
        }
        match self.0.poll_recv(cx) {
            Poll::Ready(Some(request)) => {
                let responder = store.data_mut().table.push(Responder::new(request.reply))?;
                destination.set_buffer(vec![inbox::Request {
                    publication: request.publication,
                    payload: request.payload,
                    responder,
                }]);
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending if finish => Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn run<R>(body: impl for<'a> AsyncFnOnce(Fx<'a>) -> R) -> R {
    let engine = Engine::with_backend(wcmp_wasm_core_wasmi::Wasmi::new()).unwrap();
    let path = std::env::var("DAYFRAME_INBOX_SPIKE_COMPONENT")
        .expect("run x/check-dayframe.ts --prepare-only and export its DAYFRAME_* lines");
    let component = Component::new(&engine, &std::fs::read(path).unwrap())
        .await
        .unwrap();
    let mut linker = Linker::new(&engine);
    clay_rt_wcmp::test_utils::define_wasi(&mut linker, "inbox-spike").unwrap();
    Spike::add_to_linker::<State, HasSelf<State>>(&mut linker, |state| state).unwrap();
    let gates = Gates::default();
    let served = Arc::new(AtomicBool::new(false));
    let mut store = Store::new(
        &engine,
        State {
            table: ResourceTable::new(),
            gates: gates.clone(),
            finished: Arc::clone(&served),
        },
    )
    .unwrap();
    let spike = Spike::instantiate_async(&mut store, &component, &linker)
        .await
        .unwrap();
    let server = spike.townframe_inbox_spike_server();
    let (writer, receiver) = inbox();
    let stream = StreamReader::new(&mut store.as_context_mut(), HostInbox(receiver)).unwrap();
    // Start `serve` and leave it in the store: poll its call future once inside a driver so the
    // task exists, then leak the future (dropping it would end the call). Later drives of the
    // store run it.
    store
        .run_concurrent(async |accessor| {
            let mut call = Box::pin(server.call_serve(accessor, stream));
            assert!(futures::poll!(call.as_mut()).is_pending());
            std::mem::forget(call);
        })
        .await
        .unwrap();
    let store = RefCell::new(store);
    let pump = async {
        while !served.load(Ordering::Acquire) {
            store.borrow_mut().run_until_stalled().unwrap();
            tokio::task::yield_now().await;
        }
    };
    let scenario = async {
        let fx = Fx {
            inbox: writer,
            gates,
            served: Arc::clone(&served),
            in_flight: Box::new(|| {
                let mut store = store.borrow_mut();
                let count = server
                    .call_in_flight(&mut *store)
                    .now_or_never()
                    .expect("a sync export finishes on its first poll")
                    .unwrap();
                async move { count }.boxed_local()
            }),
            in_flight_thread_local: Box::new(|| {
                let mut store = store.borrow_mut();
                let count = server
                    .call_in_flight_thread_local(&mut *store)
                    .now_or_never()
                    .expect("a sync export finishes on its first poll")
                    .unwrap();
                async move { count }.boxed_local()
            }),
        };
        body(fx).await
    };
    futures::future::join(pump, scenario).await.1
}

scenarios!(run);
