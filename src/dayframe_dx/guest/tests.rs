//! Recording renderer probe: exercises the actual component ABI, not the Blitz renderer.
use std::{
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use wasmtime::component::{Component, HasSelf, Linker, Resource, ResourceTable};
use wasmtime::{Engine, Store};

mod host_bindings {
    wasmtime::component::bindgen!({
        path: "../app/wit",
        world: "presentation-client",
        imports: { default: trappable },
        additional_derives: [PartialEq],
        with: {
            "townframe:dayframe/presentation.surface": super::RecordedSurface,
            "townframe:dayframe/presentation.mount-point": super::Placement,
        },
    });
}

use host_bindings::exports::townframe::dayframe::presentation_events::Event;
use host_bindings::townframe::dayframe::presentation::{self, *};

pub struct RecordedSurface {
    batches: Vec<Vec<u8>>,
    alive: Arc<AtomicBool>,
}
pub struct Placement {
    alive: Arc<AtomicBool>,
}
#[derive(Default)]
struct RecordingHost {
    table: ResourceTable,
}

impl presentation::Host for RecordingHost {}
impl presentation::HostMountPoint for RecordingHost {
    fn open(
        &mut self,
        placement: Resource<Placement>,
    ) -> wasmtime::Result<Result<Resource<RecordedSurface>, PresentationError>> {
        let alive = self.table.get(&placement)?.alive.clone();
        if !alive.load(Ordering::Acquire) {
            return Ok(Err(PresentationError::Revoked));
        }
        Ok(Ok(self.table.push(RecordedSurface {
            batches: Vec::new(),
            alive,
        })?))
    }
    fn drop(&mut self, placement: Resource<Placement>) -> wasmtime::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}
impl presentation::HostSurface for RecordingHost {
    fn apply(
        &mut self,
        surface: Resource<RecordedSurface>,
        edits: Vec<u8>,
    ) -> wasmtime::Result<Result<u64, PresentationError>> {
        let surface = self.table.get_mut(&surface)?;
        if !surface.alive.load(Ordering::Acquire) {
            return Ok(Err(PresentationError::Revoked));
        }
        // This backend records wire traffic only. Tree validation belongs to the real renderer.
        surface.batches.push(edits);
        Ok(Ok(surface.batches.len().try_into().unwrap()))
    }
    fn drop(&mut self, surface: Resource<RecordedSurface>) -> wasmtime::Result<()> {
        self.table.delete(surface)?;
        Ok(())
    }
}

fn counter_component() -> Vec<u8> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output = Command::new("cargo")
        .current_dir(&directory)
        .args([
            "build",
            "-p",
            "dayframe_guest",
            "--example",
            "counter",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "guest build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Ask Cargo for its configured target directory; never choose a separate target directory.
    let metadata = Command::new("cargo")
        .current_dir(&directory)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .unwrap();
    assert!(
        metadata.status.success(),
        "{}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout).unwrap();
    let core = PathBuf::from(metadata["target_directory"].as_str().unwrap())
        .join("wasm32-unknown-unknown/debug/examples/counter.wasm");
    let output = Command::new("wasm-tools")
        .args(["component", "new"])
        .arg(core)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "component encoding failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

#[test]
fn exported_partial_opcode_word_has_zeroed_padding() {
    let mut edits = dioxus_interpreter_js::Interpreter::default();
    edits.push_id(7);
    let bytes: Vec<u8> = edits.export_memory().collect();
    // In the pinned Dioxus interpreter, metadata word 6 points to the opcode stream.
    // This is an encoding-specific regression: push-id + end leave two unused opcode slots.
    let message_offset = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    assert_eq!(&bytes[message_offset..message_offset + 4], &[0, 19, 0, 0]);
}

fn click_event(revision: u64) -> Event {
    let html = dioxus_html::HtmlEvent {
        element: dioxus_core::ElementId::from_raw(1),
        name: "click".into(),
        bubbles: true,
        data: dioxus_html::EventData::Mouse(dioxus_html::SerializedMouseData::default()),
    };
    Event {
        revision,
        element: html.element.raw().try_into().unwrap(),
        name: html.name,
        bubbles: html.bubbles,
        data_json: serde_json::to_string(&html.data).unwrap(),
    }
}

#[test]
fn event_envelope_roundtrips_through_dioxus_without_a_second_payload_schema() {
    let event = click_event(1);
    let json = serde_json::json!({"element": event.element, "name": event.name,
        "bubbles": event.bubbles, "data": serde_json::from_str::<serde_json::Value>(&event.data_json).unwrap()});
    let html: dioxus_html::HtmlEvent = serde_json::from_value(json).unwrap();
    assert_eq!(
        html,
        dioxus_html::HtmlEvent {
            element: dioxus_core::ElementId::from_raw(1),
            name: "click".into(),
            bubbles: true,
            data: dioxus_html::EventData::Mouse(dioxus_html::SerializedMouseData::default()),
        }
    );
}

fn expected_counter_batches(event_count: u64) -> Vec<Vec<u8>> {
    use dioxus::prelude::*;
    use dioxus_core::{ElementId, RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedHtmlEventConverter, SerializedMouseData};
    use dioxus_interpreter_js::MutationState;

    fn counter() -> Element {
        let mut count = use_signal(|| 0_u64);
        rsx! {
            button {
                onclick: move |_| count += 1,
                "{count}"
            }
        }
    }

    dioxus_html::set_event_converter(Box::new(SerializedHtmlEventConverter));
    let mut dom = VirtualDom::new(counter);
    let mut edits = MutationState::default();
    let mut batches = Vec::with_capacity(event_count as usize + 1);
    dom.rebuild(&mut edits);
    batches.push(edits.export_memory());
    for _ in 0..event_count {
        let event = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            event,
            ElementId::from_raw(1),
        );
        dom.render_immediate(&mut edits);
        batches.push(edits.export_memory());
    }
    batches
}

#[test]
fn shared_store_probe_pins_cross_incarnation_failure_after_guest_trap() -> wasmtime::Result<()> {
    let engine = Engine::default();
    let component = Component::new(&engine, counter_component())?;
    let mut linker = Linker::new(&engine);
    presentation::add_to_linker::<_, HasSelf<RecordingHost>>(&mut linker, |host| host)?;
    let mut store = Store::new(&engine, RecordingHost::default());
    let first = host_bindings::PresentationClient::instantiate(&mut store, &component, &linker)?;
    let events = first.townframe_dayframe_presentation_events();
    let denied = store.data_mut().table.push(Placement {
        alive: Arc::new(AtomicBool::new(false)),
    })?;
    assert!(matches!(
        events.call_mount(&mut store, Resource::new_borrow(denied.rep()))?,
        Err(PresentationError::Revoked)
    ));
    let first_lifetime = Arc::new(AtomicBool::new(true));
    let placement = store.data_mut().table.push(Placement {
        alive: first_lifetime.clone(),
    })?;
    let surface = events
        .call_mount(&mut store, Resource::new_borrow(placement.rep()))?
        .unwrap();
    let expected_batches = expected_counter_batches(4);
    let click = click_event;
    for revision in 1..=2 {
        let response = events.call_dispatch(
            &mut store,
            Resource::new_borrow(surface.rep()),
            &click(revision),
        )?;
        assert!(!response.prevent_default && !response.stop_propagation);
    }
    assert_eq!(
        store.data().table.get(&surface)?.batches,
        expected_batches[..3].to_vec()
    );

    // A second instance of the same compiled component has a separate counter/address space.
    let second = host_bindings::PresentationClient::instantiate(&mut store, &component, &linker)?;
    let second_events = second.townframe_dayframe_presentation_events();
    let second_placement = store.data_mut().table.push(Placement {
        alive: Arc::new(AtomicBool::new(true)),
    })?;
    let second_surface = second_events
        .call_mount(&mut store, Resource::new_borrow(second_placement.rep()))?
        .unwrap();
    second_events.call_dispatch(
        &mut store,
        Resource::new_borrow(second_surface.rep()),
        &click(1),
    )?;
    assert_eq!(
        store.data().table.get(&second_surface)?.batches,
        expected_batches[..2].to_vec()
    );
    events.call_dispatch(&mut store, Resource::new_borrow(surface.rep()), &click(3))?;
    let expected = expected_batches[..4].to_vec();
    assert_eq!(store.data().table.get(&surface)?.batches, expected);

    // Revocation is enforced at the imported operation, not trusted to the guest's event handler.
    first_lifetime.store(false, Ordering::Release);
    let error = events
        .call_dispatch(&mut store, Resource::new_borrow(surface.rep()), &click(4))
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::UnreachableCodeReached)
    );
    assert_eq!(store.data().table.get(&surface)?.batches, expected);
    let error = second_events
        .call_dispatch(
            &mut store,
            Resource::new_borrow(second_surface.rep()),
            &click(2),
        )
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::CannotEnterComponent)
    );
    assert_eq!(
        store.data().table.get(&second_surface)?.batches,
        expected_batches[..2].to_vec()
    );
    Ok(())
}

#[test]
fn separate_stores_isolate_guest_traps_and_revoke_only_their_own_surface() -> wasmtime::Result<()> {
    let engine = Engine::default();
    let component = Component::new(&engine, counter_component())?;
    let mut linker = Linker::new(&engine);
    presentation::add_to_linker::<_, HasSelf<RecordingHost>>(&mut linker, |host| host)?;
    let create = || -> wasmtime::Result<_> {
        let mut store = Store::new(&engine, RecordingHost::default());
        let guest =
            host_bindings::PresentationClient::instantiate(&mut store, &component, &linker)?;
        let alive = Arc::new(AtomicBool::new(true));
        let placement = store.data_mut().table.push(Placement {
            alive: alive.clone(),
        })?;
        let surface = guest
            .townframe_dayframe_presentation_events()
            .call_mount(&mut store, Resource::new_borrow(placement.rep()))?
            .unwrap();
        Ok((store, guest, surface, alive))
    };
    let (mut first_store, first_guest, first_surface, first_lifetime) = create()?;
    let (mut second_store, second_guest, second_surface, _) = create()?;
    let expected_batches = expected_counter_batches(2);
    let click = click_event;
    let first_events = first_guest.townframe_dayframe_presentation_events();
    let second_events = second_guest.townframe_dayframe_presentation_events();
    first_events.call_dispatch(
        &mut first_store,
        Resource::new_borrow(first_surface.rep()),
        &click(1),
    )?;
    second_events.call_dispatch(
        &mut second_store,
        Resource::new_borrow(second_surface.rep()),
        &click(1),
    )?;
    let first_before_close = first_store
        .data()
        .table
        .get(&first_surface)?
        .batches
        .clone();
    assert_eq!(
        first_before_close,
        second_store.data().table.get(&second_surface)?.batches
    );
    first_lifetime.store(false, Ordering::Release);
    let error = first_events
        .call_dispatch(
            &mut first_store,
            Resource::new_borrow(first_surface.rep()),
            &click(2),
        )
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::UnreachableCodeReached)
    );
    second_events.call_dispatch(
        &mut second_store,
        Resource::new_borrow(second_surface.rep()),
        &click(2),
    )?;
    assert_eq!(
        first_store.data().table.get(&first_surface)?.batches,
        first_before_close
    );
    assert_eq!(
        second_store.data().table.get(&second_surface)?.batches,
        expected_batches[..3].to_vec()
    );
    Ok(())
}
