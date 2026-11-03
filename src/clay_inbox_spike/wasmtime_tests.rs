//! The spike on Wasmtime (the pinned fork): `serve` and the scenarios run inside one
//! `run_concurrent`, so the guest's serve task, its handlers and the host's gate releases are
//! all driven by the store's event loop.
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
use wash_runtime::wasmtime;
use wasmtime::Config;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::StoreContextMut;
use wasmtime::component::Accessor;
use wasmtime::component::AccessorTask;
use wasmtime::component::Component;
use wasmtime::component::Destination;
use wasmtime::component::HasSelf;
use wasmtime::component::Linker;
use wasmtime::component::Resource;
use wasmtime::component::ResourceTable;
use wasmtime::component::StreamProducer;
use wasmtime::component::StreamReader;
use wasmtime::component::StreamResult;
use wasmtime_wasi::WasiCtx;
use wasmtime_wasi::WasiCtxView;
use wasmtime_wasi::WasiView;

mod bindings {
    wash_runtime::wasmtime::component::bindgen!({
        path: "wit",
        world: "spike",
        wasmtime_crate: wash_runtime::wasmtime,
        imports: { default: trappable },
        exports: { default: async },
        with: {
            "townframe:inbox-spike/inbox.responder": crate::host::Responder,
        },
    });
}

use bindings::Spike;
use bindings::townframe::inbox_spike::inbox;

struct State {
    wasi: WasiCtx,
    table: ResourceTable,
    gates: Gates,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl inbox::Host for State {
    fn finished(&mut self) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl inbox::HostResponder for State {
    fn respond(
        &mut self,
        responder: Resource<Responder>,
        outcome: Result<Vec<u8>, String>,
    ) -> wasmtime::Result<()> {
        self.table.get_mut(&responder)?.respond(outcome);
        Ok(())
    }

    fn drop(&mut self, responder: Resource<Responder>) -> wasmtime::Result<()> {
        self.table.delete(responder)?;
        Ok(())
    }
}

impl<T: 'static> inbox::HostWithStore<T> for HasSelf<State> {
    async fn gate(accessor: &Accessor<T, Self>, name: String) -> wasmtime::Result<Vec<u8>> {
        let released = accessor.with(|mut access| access.get().gates.open(name));
        Ok(released.await?)
    }
}

/// The host's writable end of the inbox stream: requests from the channel, each given a
/// responder resource in the store's table as it is lowered into the guest. A closed channel
/// ends the stream.
struct HostInbox(mpsc::UnboundedReceiver<HostRequest>);

impl StreamProducer<State> for HostInbox {
    type Item = inbox::Request;
    type Buffer = Option<inbox::Request>;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, State>,
        mut destination: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        if destination.remaining(&mut store) == Some(0) {
            return Poll::Ready(Ok(StreamResult::Completed));
        }
        match self.0.poll_recv(cx) {
            Poll::Ready(Some(request)) => {
                let responder = store.data_mut().table.push(Responder::new(request.reply))?;
                destination.set_buffer(Some(inbox::Request {
                    publication: request.publication,
                    payload: request.payload,
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

/// Start `serve` as a store task, the way the product driver would: it lives in the store, and
/// only a drive of the store's event loop makes it (and every handler it holds) progress.
struct ServeTask {
    server: bindings::exports::townframe::inbox_spike::server::Guest,
    stream: StreamReader<inbox::Request>,
    served: Arc<AtomicBool>,
}

impl AccessorTask<State> for ServeTask {
    async fn run(self, accessor: &Accessor<State>) -> wasmtime::Result<()> {
        self.server.call_serve(accessor, self.stream).await?;
        self.served.store(true, Ordering::Release);
        Ok(())
    }
}

async fn run<R>(body: impl for<'a> AsyncFnOnce(Fx<'a>) -> R) -> R {
    let mut config = Config::new();
    config.wasm_component_model_async(true);
    let engine = Engine::new(&config).unwrap();
    let path = std::env::var("DAYFRAME_INBOX_SPIKE_COMPONENT")
        .expect("run x/check-dayframe.ts --prepare-only and export its DAYFRAME_* lines");
    let component = Component::from_file(&engine, path).unwrap();
    let mut linker = Linker::new(&engine);
    wasmtime_wasi::p3::add_to_linker(&mut linker).unwrap();
    Spike::add_to_linker::<_, HasSelf<State>>(&mut linker, |state| state).unwrap();
    let gates = Gates::default();
    let mut store = Store::new(
        &engine,
        State {
            wasi: WasiCtx::builder().inherit_stderr().build(),
            table: ResourceTable::new(),
            gates: gates.clone(),
        },
    );
    let spike = Spike::instantiate_async(&mut store, &component, &linker)
        .await
        .unwrap();
    let server = spike.townframe_inbox_spike_server().clone();
    let (writer, receiver) = inbox();
    let served = Arc::new(AtomicBool::new(false));
    let stream = StreamReader::new(&mut store, HostInbox(receiver)).unwrap();
    store
        .spawn(ServeTask {
            server: server.clone(),
            stream,
            served: Arc::clone(&served),
        })
        .unwrap();
    let store = RefCell::new(store);
    // Bounded drive bursts, the way the product driver ticks: the store is borrowed only for
    // the burst, so the sync export below can enter between bursts.
    #[expect(
        clippy::await_holding_refcell_ref,
        reason = "a drive burst owns the store; scenario entries run between bursts"
    )]
    let pump = async {
        while !served.load(Ordering::Acquire) {
            store.borrow_mut().drive_until_stalled().await.unwrap();
            tokio::task::yield_now().await;
        }
    };
    let scenario = async {
        let fx = Fx {
            inbox: writer,
            gates,
            served: Arc::clone(&served),
            // A sync export entered between bursts: it must finish on its first poll.
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
