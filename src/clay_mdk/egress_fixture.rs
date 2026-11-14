#![cfg(target_arch = "wasm32")]
//! Egress probe fixture: a guest that tries every host channel the Wasmtime WASI context could
//! grant (TCP connect, UDP bind, name lookup, directory listing, file open, environment,
//! arguments) when it mounts, and renders one text node reporting what each attempt returned.
//! `clay_rt_wasmtime`'s tests pin that every attempt fails and the environment is empty, so adding
//! `inherit_network()`, a preopen or an env var to the guest `WasiCtx` breaks a test.

use clay_mdk::bindings;
use clay_mdk::document_events;
use clay_mdk::documents;
use clay_mdk::micro_lifecycle;
use clay_mdk::presentation;
use clay_mdk::presentation_events::Event;
use clay_mdk::presentation_events::EventResponse;
use clay_mdk::presentation_events::{self};
use clay_mdk::wire::WireMutations;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;
use std::net::ToSocketAddrs;
use std::sync::OnceLock;

/// The probe report, computed once in the async `initialize` (the p3 socket and filesystem
/// calls are async imports, which a sync `mount` export may not suspend on).
static REPORT: OnceLock<String> = OnceLock::new();

fn outcome<T, E: std::fmt::Display>(result: Result<T, E>) -> String {
    match result {
        Ok(_) => "ok".to_owned(),
        Err(error) => format!("err({error})"),
    }
}

/// One line per attempted channel: `name=ok` if the attempt succeeded, `name=err(..)` otherwise.
fn report() -> String {
    let tcp = std::net::TcpStream::connect(("127.0.0.1", 9));
    let udp = std::net::UdpSocket::bind("127.0.0.1:0");
    let dns = "localhost:80"
        .to_socket_addrs()
        .map(|mut addrs| addrs.next());
    let read_dir = std::fs::read_dir("/").map(|entries| entries.count());
    let open = std::fs::File::open("/etc/hostname");
    let cwd = std::fs::File::open("hostname");
    [
        format!("tcp={}", outcome(tcp)),
        format!("udp={}", outcome(udp)),
        format!("dns={}", outcome(dns)),
        format!("read_dir={}", outcome(read_dir)),
        format!("open={}", outcome(open)),
        format!("open_relative={}", outcome(cwd)),
        format!("env={}", std::env::vars().count()),
        format!("args={}", std::env::args().count()),
    ]
    .join(";")
}

struct Probe;

impl micro_lifecycle::Guest for Probe {
    type InstanceRoot = clay_mdk::StatelessRoot;

    async fn initialize(
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, micro_lifecycle::InitializationError> {
        REPORT.set(report()).expect("initialize runs once");
        assert!(context.arguments.is_empty());
        assert!(context.configs.is_empty());
        assert!(context.collections.is_empty());
        assert!(context.registrations.is_empty());
        assert!(context.requirements.is_empty());
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(clay_mdk::StatelessRoot),
            publications: Vec::new(),
        })
    }

    async fn call(publication: String, _request: Vec<u8>) -> Result<Vec<u8>, String> {
        Err(format!("egress fixture serves no call {publication:?}"))
    }
}

impl presentation_events::Guest for Probe {
    fn mount(
        _publication: String,
        placement: &presentation::MountPoint,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        let surface = placement.open()?;
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.create_text(REPORT.get().expect("initialize ran before mount"));
        edits.append_children(1);
        edits.pop();
        surface.apply(&edits.take())?;
        Ok(surface)
    }

    fn unmount(_surface: &presentation::Surface) {}

    fn dispatch(_surface: &presentation::Surface, _event: Event) -> EventResponse {
        EventResponse {
            prevent_default: false,
            stop_propagation: false,
        }
    }
}

impl document_events::Guest for Probe {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("egress fixture owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("egress fixture owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("egress fixture submits no document operations");
    }
}

bindings::export!(Probe with_types_in bindings);
