#![cfg(target_arch = "wasm32")]

use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::Mirror;
use clay_collection::Page;
use clay_collection::RowEvidence;
use clay_collection::Stamped;
use clay_collection::Version;
use clay_collection::WriteBasis;
use clay_mdk::bindings;
use clay_mdk::capability_bank;
use clay_mdk::collection_registration;
use clay_mdk::collections;
use clay_mdk::document_events;
use clay_mdk::documents;
use clay_mdk::micro_lifecycle;
use clay_mdk::presentation;
use clay_mdk::presentation_events::Event;
use clay_mdk::presentation_events::EventResponse;
use clay_mdk::presentation_events::{self};
use clay_mdk::registration::RegistrationWriter;
use clay_mdk::wire::WireMutations;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

struct GuestValue {
    body: Vec<u8>,
    capabilities: Vec<capability_bank::CapabilityRef>,
}

/// A registration writer whose default basis reads the probe's own replica of the collection.
type Writer = RegistrationWriter<Box<dyn Fn(&Vec<u8>) -> WriteBasis>>;

type Replica = Mirror<Vec<u8>, Stamped<Option<Rc<GuestValue>>>>;

struct State {
    reader: Rc<collections::CollectionReader>,
    selector: Vec<u8>,
    lease: Option<Rc<collections::ProjectionLease>>,
    /// Shared with the writers' default-basis closures; never borrowed across an await.
    mirror: Rc<RefCell<Replica>>,
    /// Wakers of scenarios waiting for the read loop to apply a page.
    applied: Vec<std::task::Waker>,
    surfaces: BTreeMap<u64, presentation::Surface>,
    status: &'static str,
    pages: u64,
    empty_reads: u64,
    replays: u64,
    resumes: u64,
    resets: u64,
    drop_next: bool,
    resume_requested: bool,
    stop_requested: bool,
    running: bool,
    writer_a: Option<Rc<Writer>>,
    writer_b: Option<Rc<Writer>>,
    cap_writer: Option<Rc<Writer>>,
    cap_publication: Option<capability_bank::PublishedCapability>,
    cap_proxy: Option<capability_bank::PresentationComponent>,
    cap_stamp: Option<collections::Stamp>,
    scenario: &'static str,
    log: Vec<String>,
}

thread_local! {
    static STATE: RefCell<Option<Rc<RefCell<State>>>> = const { RefCell::new(None) };
}

struct CollectionProbe;

impl micro_lifecycle::Guest for CollectionProbe {
    type InstanceRoot = clay_mdk::StatelessRoot;

    async fn initialize(
        context: micro_lifecycle::InitialContext,
    ) -> Result<micro_lifecycle::Initialized, micro_lifecycle::InitializationError> {
        use micro_lifecycle::InitializationError;
        let micro_lifecycle::InitialContext {
            arguments,
            configs,
            collections: collection_bindings,
            registrations,
            requirements,
        } = context;
        assert!(arguments.is_empty());
        assert!(configs.is_empty());
        assert!(requirements.is_empty());
        if collection_bindings.len() != 1 {
            return Err(InitializationError::InvalidConfiguration(
                "collection probe expects exactly one collection binding".into(),
            ));
        }
        let mirror = Rc::new(RefCell::new(Mirror::empty()));
        let writer = |binding: collection_registration::RegistrationBinding| {
            let replica = Rc::clone(&mirror);
            Rc::new(RegistrationWriter::new(
                binding.writer,
                Box::new(move |key: &Vec<u8>| replica.borrow().write_basis(key))
                    as Box<dyn Fn(&Vec<u8>) -> WriteBasis>,
            ))
        };
        let mut registrations = registrations.into_iter();
        let (writer_a, writer_b, cap_writer) = match (
            registrations.next(),
            registrations.next(),
            registrations.next(),
        ) {
            (None, None, None) => (None, None, None),
            (Some(a), Some(b), None) if a.name == "a" && b.name == "b" => {
                (Some(writer(a)), Some(writer(b)), None)
            }
            (Some(cap), None, None) if cap.name == "cap" => (None, None, Some(writer(cap))),
            _ => {
                return Err(InitializationError::InvalidConfiguration(
                    "collection probe expects no registrations, exactly `a` and `b`, or exactly `cap`"
                        .into(),
                ));
            }
        };
        let binding = collection_bindings.into_iter().next().unwrap();
        let mut selector = Vec::new();
        ciborium::into_writer(&"all", &mut selector).expect("CBOR selector encoding failed");
        let lease = binding
            .reader
            .acquire(selector.clone(), None)
            .await
            .map_err(|error| {
                InitializationError::Unavailable(format!("collection acquire failed: {error:?}"))
            })?;
        let state = Rc::new(RefCell::new(State {
            reader: Rc::new(binding.reader),
            selector,
            lease: Some(Rc::new(lease)),
            mirror,
            applied: Vec::new(),
            surfaces: BTreeMap::new(),
            status: "reading",
            pages: 0,
            empty_reads: 0,
            replays: 0,
            resumes: 0,
            resets: 0,
            drop_next: false,
            resume_requested: false,
            stop_requested: false,
            running: true,
            writer_a,
            writer_b,
            cap_writer,
            cap_publication: None,
            cap_proxy: None,
            cap_stamp: None,
            scenario: "idle",
            log: Vec::new(),
        }));
        STATE.with_borrow_mut(|slot| {
            assert!(slot.replace(state.clone()).is_none());
        });
        wit_bindgen::spawn_local(read_loop(state));
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(clay_mdk::StatelessRoot),
            publications: Vec::new(),
        })
    }

    async fn call(publication: String, _request: Vec<u8>) -> Result<Vec<u8>, String> {
        Err(format!("collection probe serves no call {publication:?}"))
    }
}

impl presentation_events::Guest for CollectionProbe {
    fn mount(
        _publication: String,
        placement: &presentation::MountPoint,
        components: Vec<presentation::ComponentRef>,
    ) -> Result<presentation::Surface, presentation::PresentationError> {
        assert!(components.is_empty());
        let surface = placement.open()?;
        let retained = surface.retain()?;
        let identity = surface.identity();
        let (text, writes, cap) = with_state(|state| {
            let state = state.borrow();
            (
                state.text(),
                state.writer_b.is_some(),
                state.cap_writer.is_some(),
            )
        });
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::ROOT);
        edits.create_element("section", None);
        edits.set_id(ElementId::from_raw(1));
        edits.create_text(&text);
        edits.append_children(1);
        let buttons: &[(usize, &str)] = if cap {
            &[
                (2, "drop-next"),
                (3, "resume"),
                (4, "stop"),
                (5, "cap-write"),
                (6, "cap-close"),
                (7, "cap-redeem"),
            ]
        } else if writes {
            &[
                (2, "drop-next"),
                (3, "resume"),
                (4, "stop"),
                (5, "write"),
                (6, "drop-a"),
            ]
        } else {
            &[(2, "drop-next"), (3, "resume"), (4, "stop")]
        };
        for &(id, label) in buttons {
            edits.create_element("button", None);
            edits.set_id(ElementId::from_raw(id));
            edits.add_event_listener("click");
            edits.create_text(label);
            edits.append_children(1);
            edits.append_children(1);
        }
        edits.append_children(1);
        edits.pop();
        surface.apply(&edits.take())?;
        with_state(|state| {
            assert!(
                state
                    .borrow_mut()
                    .surfaces
                    .insert(identity, retained)
                    .is_none()
            );
        });
        Ok(surface)
    }

    fn unmount(surface: &presentation::Surface) {
        with_state(|state| {
            state.borrow_mut().surfaces.remove(&surface.identity());
        });
    }

    fn dispatch(surface: &presentation::Surface, event: Event) -> EventResponse {
        let state = with_state(|state| state.clone());
        let mut write = false;
        let mut cap_write = false;
        let mut cap_close = false;
        let mut redeem = false;
        let restart = {
            let mut state = state.borrow_mut();
            if !state.surfaces.contains_key(&surface.identity()) {
                return EventResponse {
                    prevent_default: false,
                    stop_propagation: false,
                };
            }
            if event.name != "click" {
                return EventResponse {
                    prevent_default: false,
                    stop_propagation: false,
                };
            }
            match event.element {
                2 => {
                    state.drop_next = true;
                    false
                }
                3 if state.status != "revoked" => {
                    state.resume_requested = true;
                    state.stop_requested = false;
                    state.status = "resuming";
                    render(&mut state);
                    !state.running
                }
                4 => {
                    state.stop_requested = true;
                    state.resume_requested = false;
                    state.lease = None;
                    state.status = "stopped";
                    render(&mut state);
                    false
                }
                5 if state.writer_b.is_some() && state.scenario == "idle" => {
                    state.scenario = "running";
                    render(&mut state);
                    write = true;
                    false
                }
                5 if state.cap_writer.is_some() && state.scenario == "idle" => {
                    state.scenario = "running";
                    render(&mut state);
                    cap_write = true;
                    false
                }
                6 if state.cap_writer.is_some() => {
                    cap_close = true;
                    false
                }
                6 => {
                    state.writer_a = None;
                    state.log.push("a-writer-drop=ok".into());
                    render(&mut state);
                    false
                }
                7 if state.cap_writer.is_some() => {
                    redeem = true;
                    false
                }
                _ => false,
            }
        };
        if write {
            wit_bindgen::spawn_local(write_scenario(state.clone()));
        }
        if cap_write {
            wit_bindgen::spawn_local(cap_write_scenario(state.clone()));
        }
        if cap_close {
            wit_bindgen::spawn_local(cap_close_scenario(state.clone()));
        }
        if redeem {
            wit_bindgen::spawn_local(cap_redeem(state.clone(), surface.identity()));
        }
        if restart {
            state.borrow_mut().running = true;
            wit_bindgen::spawn_local(read_loop(state));
        }
        EventResponse {
            prevent_default: false,
            stop_propagation: false,
        }
    }
}

impl document_events::Guest for CollectionProbe {
    fn changed(_snapshot: documents::DocumentSnapshot) {
        unreachable!("collection probe owns no document watches");
    }

    fn lagged(_watch_id: u64) {
        unreachable!("collection probe owns no document watches");
    }

    fn completed(_receipt: documents::OperationReceipt) {
        unreachable!("collection probe submits no document operations");
    }
}

impl State {
    fn text(&self) -> String {
        let mut text = format!(
            "status={}\nscenario={}\npages={}\nempty-reads={}\nreplays={}\nresumes={}\nresets={}\nrows={}",
            self.status,
            self.scenario,
            self.pages,
            self.empty_reads,
            self.replays,
            self.resumes,
            self.resets,
            self.mirror.borrow().live_rows().len()
        );
        for (key, value) in self.mirror.borrow().live_rows() {
            text.push('\n');
            text.push_str(&decode_key(&key));
            text.push('=');
            text.push_str(&decode_value(&value));
        }
        for line in &self.log {
            text.push('\n');
            text.push_str(line);
        }
        text
    }
}

fn with_state<T>(f: impl FnOnce(&Rc<RefCell<State>>) -> T) -> T {
    STATE.with_borrow(|slot| f(slot.as_ref().expect("collection probe is not active")))
}

fn decode(value: &[u8]) -> String {
    ciborium::from_reader(value).expect("collection row is not a CBOR string")
}

fn decode_value(value: &GuestValue) -> String {
    match value.body.as_slice() {
        [0xd9, 0xa7, 0xf8, index] => format!("cap@{index}"),
        body => decode(body),
    }
}

fn cap_tagged_body() -> Vec<u8> {
    vec![0xd9, 0xa7, 0xf8, 0x00]
}

fn cap_failure(error: capability_bank::CapabilityError) -> &'static str {
    match error {
        capability_bank::CapabilityError::ContractMismatch => "contract-mismatch",
        capability_bank::CapabilityError::Revoked => "revoked",
        capability_bank::CapabilityError::LimitExceeded => "limit-exceeded",
        capability_bank::CapabilityError::Unavailable => "unavailable",
    }
}

fn encode(value: &str) -> Vec<u8> {
    let mut encoded = Vec::new();
    ciborium::into_writer(value, &mut encoded).expect("CBOR string encoding failed");
    encoded
}

fn decode_key(key: &[u8]) -> String {
    match ciborium::from_reader(key).expect("collection key is not CBOR") {
        ciborium::Value::Text(key) => key,
        _ => panic!("collection key is not a string"),
    }
}

fn accepted(
    result: Result<collections::Stamp, collection_registration::MutationError>,
) -> (String, Option<collections::Stamp>) {
    match result {
        Ok(stamp) => (format!("ok@{}", stamp.lamport), Some(stamp)),
        Err(error) => (failure(error), None),
    }
}

fn failure(error: collection_registration::MutationError) -> String {
    match error {
        collection_registration::MutationError::Conflict(Some(current)) => {
            format!("conflict@{}", current.lamport)
        }
        collection_registration::MutationError::Conflict(None) => "conflict@none".into(),
        collection_registration::MutationError::Revoked => "revoked".into(),
        collection_registration::MutationError::KeyTooLarge => "key-too-large".into(),
        collection_registration::MutationError::ValueTooLarge => "value-too-large".into(),
        collection_registration::MutationError::RowLimit => "row-limit".into(),
        collection_registration::MutationError::InvalidAttachment => "invalid-attachment".into(),
        collection_registration::MutationError::CapabilityLimit => "capability-limit".into(),
    }
}

/// Resolves once the read loop has applied a page that leaves the replica holding `stamp` for
/// `key`. Writers default their basis from the replica, so a scenario step that depends on a
/// previous write waits for it to arrive instead of racing the read loop.
async fn replica_holds(state: &Rc<RefCell<State>>, key: &[u8], stamp: &collections::Stamp) {
    let held = WriteBasis::Observed(stamp.clone().into());
    std::future::poll_fn(|context| {
        let mut state = state.borrow_mut();
        if state.mirror.borrow().write_basis(&key.to_vec()) == held {
            std::task::Poll::Ready(())
        } else {
            state.applied.push(context.waker().clone());
            std::task::Poll::Pending
        }
    })
    .await;
}

/// Two scopes of one micro write the same keys. Their default bases come from the replica,
/// so each ordinary write supersedes what the probe has seen, deletions included.
async fn write_scenario(state: Rc<RefCell<State>>) {
    let value = |body: &str| collection_registration::WrittenValue {
        body: encode(body),
        attachments: Vec::new(),
    };
    let (a, b) = {
        let state = state.borrow();
        (
            state.writer_a.clone().expect("writer a is bound"),
            state.writer_b.clone().expect("writer b is bound"),
        )
    };
    let log = |line: String| {
        let mut state = state.borrow_mut();
        state.log.push(line);
        render(&mut state);
    };
    let (x, y) = (encode("x"), encode("y"));
    let stamp = |stamp: Option<collections::Stamp>| stamp.expect("the write was accepted");

    let (outcome, a_x) = accepted(a.put(x.clone(), value("a1")).await);
    log(format!("a-put-x={outcome}"));
    let a_x = stamp(a_x);
    replica_holds(&state, &x, &a_x).await;
    let (outcome, b_x) = accepted(b.put(x.clone(), value("b1")).await);
    log(format!("b-put-x={outcome}"));
    replica_holds(&state, &x, &stamp(b_x)).await;
    // Compare-and-set against the superseded stamp reports the visible one.
    let (outcome, _) = accepted(
        a.put_with(
            x.clone(),
            collection_registration::WriteBasis::CompareAndSet(Some(a_x)),
            value("stale"),
        )
        .await,
    );
    log(format!("a-stale-cas-x={outcome}"));
    let (outcome, a_x2) = accepted(a.put(x.clone(), value("a2")).await);
    log(format!("a-put-x-again={outcome}"));
    let a_x2 = stamp(a_x2);
    replica_holds(&state, &x, &a_x2).await;

    // A delete of a key the replica has never held is a concurrent tombstone. A blind write
    // loses to it; the replica now holds the tombstone's stamp, so the default basis wins.
    let (outcome, b_y) = accepted(b.delete(y.clone()).await);
    log(format!("b-delete-y={outcome}"));
    let b_y = stamp(b_y);
    replica_holds(&state, &y, &b_y).await;
    let (outcome, _) = accepted(
        a.put_with(
            y.clone(),
            collection_registration::WriteBasis::Concurrent,
            value("blind"),
        )
        .await,
    );
    log(format!("a-blind-put-y={outcome}"));
    let (outcome, a_y) = accepted(a.put(y.clone(), value("a3")).await);
    log(format!("a-recreate-y={outcome}"));
    replica_holds(&state, &y, &stamp(a_y.clone())).await;

    let (outcome, _) = accepted(a.put(vec![0; 1025], value("big-key")).await);
    log(format!("a-put-big-key={outcome}"));
    let big = collection_registration::WrittenValue {
        body: vec![0; 64 * 1024 + 1],
        attachments: Vec::new(),
    };
    let (outcome, _) = accepted(a.put(y.clone(), big).await);
    log(format!("a-put-big-value-y={outcome}"));

    let (outcome, b_x_del) = accepted(b.delete(x.clone()).await);
    log(format!("b-delete-x={outcome}"));
    replica_holds(&state, &x, &stamp(b_x_del)).await;
    let (outcome, _) = accepted(b.put(x.clone(), value("b2")).await);
    log(format!("b-put-x-again={outcome}"));
    drop((a, b));
    let mut state = state.borrow_mut();
    state.scenario = "done";
    render(&mut state);
}

async fn cap_write_scenario(state: Rc<RefCell<State>>) {
    let writer = {
        let state = state.borrow();
        state.cap_writer.clone().expect("cap writer is bound")
    };
    let log = |line: String| {
        let mut state = state.borrow_mut();
        state.log.push(line);
        render(&mut state);
    };
    let published = match capability_bank::publish_presentation("cap") {
        Ok(published) => published,
        Err(error) => {
            log(format!("cap-publish={}", cap_failure(error)));
            let mut state = state.borrow_mut();
            state.scenario = "done";
            render(&mut state);
            return;
        }
    };
    log("cap-publish=ok".into());
    let (outcome, _) = accepted(
        writer
            .put(
                encode("cap"),
                collection_registration::WrittenValue {
                    body: cap_tagged_body(),
                    attachments: Vec::new(),
                },
            )
            .await,
    );
    log(format!("cap-fabricated-put={outcome}"));
    let reference = match published.reference() {
        Ok(reference) => reference,
        Err(error) => {
            log(format!("cap-reference={}", cap_failure(error)));
            let mut state = state.borrow_mut();
            state.scenario = "done";
            render(&mut state);
            return;
        }
    };
    let (outcome, stamp) = accepted(
        writer
            .put(
                encode("cap"),
                collection_registration::WrittenValue {
                    body: cap_tagged_body(),
                    attachments: vec![collection_registration::RowAttachment::Reference(reference)],
                },
            )
            .await,
    );
    log(format!("cap-put={outcome}"));
    let mut state = state.borrow_mut();
    if stamp.is_some() {
        state.cap_publication = Some(published);
        state.cap_stamp = stamp;
    }
    state.scenario = "done";
    render(&mut state);
}

async fn cap_redeem(state: Rc<RefCell<State>>, identity: u64) {
    let value = {
        let state = state.borrow();
        state
            .mirror
            .borrow()
            .live_rows()
            .into_iter()
            .map(|(_, value)| value)
            .find(|value| !value.capabilities.is_empty())
    };
    let log = |line: String| {
        let mut state = state.borrow_mut();
        state.log.push(line);
        render(&mut state);
    };
    let Some(value) = value else {
        log("cap-redeem=absent".into());
        return;
    };
    let proxy = match value.capabilities[0].redeem_presentation().await {
        Ok(proxy) => proxy,
        Err(error) => {
            log(format!("cap-redeem={}", cap_failure(error)));
            return;
        }
    };
    let component = match proxy.component() {
        Ok(component) => component,
        Err(error) => {
            log(format!("cap-redeem={}", cap_failure(error)));
            return;
        }
    };
    let mut edits = WireMutations::default();
    edits.push_id(ElementId::from_raw(1));
    edits.mount_component(&component);
    edits.append_children(1);
    edits.pop();
    let outcome = {
        let mut state = state.borrow_mut();
        let outcome = match state.surfaces.get(&identity) {
            // The surface was unmounted while redemption was in flight.
            None => "unmounted",
            Some(surface) => match surface.apply(&edits.take()) {
                Ok(_) => "mounted",
                Err(presentation::PresentationError::Revoked) => "revoked",
                Err(presentation::PresentationError::Unsupported(_)) => "unsupported",
                Err(presentation::PresentationError::InvalidBatch(_)) => "invalid-batch",
            },
        };
        state.cap_proxy = Some(proxy);
        outcome
    };
    log(format!("cap-redeem={outcome}"));
}

async fn cap_close_scenario(state: Rc<RefCell<State>>) {
    let (writer, stamp) = {
        let state = state.borrow();
        (
            state.cap_writer.clone().expect("cap writer is bound"),
            state.cap_stamp.clone(),
        )
    };
    // The default basis comes from the replica, so wait until it holds the row being deleted.
    if let Some(stamp) = &stamp {
        replica_holds(&state, &encode("cap"), stamp).await;
    }
    let log = |line: String| {
        let mut state = state.borrow_mut();
        state.log.push(line);
        render(&mut state);
    };
    log(format!(
        "cap-remove={}",
        accepted(writer.delete(encode("cap")).await).0
    ));
    let published = state.borrow_mut().cap_publication.take();
    drop(published);
    log("cap-close=ok".into());
    let proxy = state.borrow_mut().cap_proxy.take();
    match proxy {
        Some(proxy) => {
            let outcome = match proxy.component() {
                Ok(_) => "ok",
                Err(error) => cap_failure(error),
            };
            drop(proxy);
            log(format!("cap-proxy={outcome}"));
            log("cap-proxy-drop=ok".into());
        }
        None => log("cap-proxy=absent".into()),
    }
    let mut state = state.borrow_mut();
    state.cap_stamp = None;
    state.scenario = "done";
    render(&mut state);
}

fn render(state: &mut State) {
    let text = state.text();
    let mut revoked = Vec::new();
    for (&identity, surface) in &state.surfaces {
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::from_raw(1));
        edits.set_text(&text);
        edits.pop();
        match surface.apply(&edits.take()) {
            Ok(_) => {}
            Err(presentation::PresentationError::Revoked) => revoked.push(identity),
            Err(error) => panic!("collection probe surface update failed: {error:?}"),
        }
    }
    for identity in revoked {
        state.surfaces.remove(&identity);
    }
}

async fn read_loop(state: Rc<RefCell<State>>) {
    loop {
        let resume = {
            let mut state = state.borrow_mut();
            if state.stop_requested {
                state.running = false;
                render(&mut state);
                return;
            }
            if state.resume_requested {
                state.resume_requested = false;
                state.lease = None;
                state.status = "resuming";
                let cursor = state.mirror.borrow_mut().cursor().cloned();
                render(&mut state);
                Some(cursor)
            } else {
                None
            }
        };
        if let Some(resume) = resume {
            let Some((lease, acquired_resume)) = acquire(&state, resume).await else {
                state.borrow_mut().running = false;
                return;
            };
            let mut state = state.borrow_mut();
            if state.stop_requested {
                state.running = false;
                render(&mut state);
                return;
            }
            state
                .mirror
                .borrow_mut()
                .begin_lease(acquired_resume.as_ref());
            state.resumes += 1;
            state.lease = Some(Rc::new(lease));
            state.status = "reading";
            render(&mut state);
            continue;
        }

        let (lease, ticket, committed) = {
            let state = state.borrow();
            let lease = state
                .lease
                .as_ref()
                .expect("collection lease missing")
                .clone();
            let ticket = state.mirror.borrow_mut().begin_read();
            let committed = ticket.committed().cloned();
            (lease, ticket, committed)
        };
        let result = lease
            .read(
                committed.map(|cursor| cursor.0),
                collections::ReadLimits {
                    max_transactions: Some(1),
                    max_changes: Some(1),
                    poll_timeout_ms: 10,
                },
            )
            .await;
        {
            let state = state.borrow();
            if state.stop_requested || state.resume_requested {
                assert!(state.mirror.borrow_mut().cancel_read(&ticket));
                continue;
            }
        }

        match result {
            Ok(page) => {
                let nonempty = page
                    .transactions
                    .iter()
                    .any(|transaction| !transaction.changes.is_empty());
                let page = decode_page(page);
                let mut state = state.borrow_mut();
                state.pages += 1;
                if !nonempty {
                    state.empty_reads += 1;
                }
                if nonempty && state.drop_next {
                    state.drop_next = false;
                    assert!(state.mirror.borrow_mut().cancel_read(&ticket));
                    state.replays += 1;
                } else {
                    assert!(state.mirror.borrow_mut().apply(&ticket, &page));
                    for waker in std::mem::take(&mut state.applied) {
                        waker.wake();
                    }
                    state.status = if state.mirror.borrow_mut().bootstrap_complete() {
                        "live"
                    } else {
                        "reading"
                    };
                }
                render(&mut state);
            }
            Err(collections::ReadError::ResetRequired) => {
                {
                    let mut state = state.borrow_mut();
                    state.lease = None;
                    state.mirror.borrow_mut().clear();
                    state.resets += 1;
                    state.status = "reading";
                    render(&mut state);
                }
                let Some((lease, _)) = acquire(&state, None).await else {
                    state.borrow_mut().running = false;
                    return;
                };
                let mut state = state.borrow_mut();
                if state.stop_requested {
                    state.running = false;
                    render(&mut state);
                    return;
                }
                state.lease = Some(Rc::new(lease));
                render(&mut state);
            }
            Err(collections::ReadError::Revoked) => {
                let mut state = state.borrow_mut();
                state.lease = None;
                state.status = "revoked";
                state.running = false;
                render(&mut state);
                return;
            }
            Err(collections::ReadError::SourceUnavailable) => {
                let mut state = state.borrow_mut();
                state.lease = None;
                state.status = "source-unavailable";
                state.running = false;
                render(&mut state);
                return;
            }
        }
    }
}

async fn acquire(
    state: &Rc<RefCell<State>>,
    mut resume: Option<Cursor>,
) -> Option<(collections::ProjectionLease, Option<Cursor>)> {
    loop {
        let (reader, selector) = {
            let state = state.borrow();
            (state.reader.clone(), state.selector.clone())
        };
        let requested = resume.clone();
        match reader
            .acquire(selector, requested.as_ref().map(|cursor| cursor.0.clone()))
            .await
        {
            Ok(lease) => return Some((lease, requested)),
            Err(collections::AcquireError::ResetRequired) => {
                let mut state = state.borrow_mut();
                if state.stop_requested {
                    return None;
                }
                state.mirror.borrow_mut().clear();
                state.resets += 1;
                state.status = "reading";
                resume = None;
                render(&mut state);
            }
            Err(collections::AcquireError::Revoked) => {
                finish_with_status(state, "revoked");
                return None;
            }
            Err(collections::AcquireError::SourceUnavailable) => {
                finish_with_status(state, "source-unavailable");
                return None;
            }
            Err(collections::AcquireError::InvalidSelector) => {
                finish_with_status(state, "invalid-selector");
                return None;
            }
            Err(collections::AcquireError::UnauthorizedSelector) => {
                finish_with_status(state, "unauthorized-selector");
                return None;
            }
        }
    }
}

fn finish_with_status(state: &Rc<RefCell<State>>, status: &'static str) {
    let mut state = state.borrow_mut();
    state.lease = None;
    state.status = status;
    render(&mut state);
}

fn decode_page(page: collections::Page) -> Page<Vec<u8>, Stamped<Option<Rc<GuestValue>>>> {
    Page {
        transactions: page
            .transactions
            .into_iter()
            .map(|transaction| clay_collection::Transaction {
                changes: transaction
                    .changes
                    .into_iter()
                    .map(|change| match change {
                        collections::Change::Upsert(upsert) => {
                            decode_key(&upsert.key.body);
                            let value = upsert.value.map(|value| {
                                let value = Rc::new(GuestValue {
                                    body: value.body,
                                    capabilities: value.capabilities,
                                });
                                decode_value(&value);
                                value
                            });
                            Change::Upsert {
                                key: upsert.key.body,
                                row_version: version(upsert.row_version),
                                value: Stamped {
                                    stamp: upsert.stamp.into(),
                                    value,
                                },
                                evidence: match upsert.evidence {
                                    collections::RowEvidence::Authoritative => {
                                        RowEvidence::Authoritative
                                    }
                                    collections::RowEvidence::Pending => RowEvidence::Pending,
                                },
                                write_context: upsert.write_context,
                            }
                        }
                        collections::Change::Remove(removal) => {
                            decode_key(&removal.key.body);
                            Change::Remove {
                                key: removal.key.body,
                                row_version: version(removal.row_version),
                            }
                        }
                    })
                    .collect(),
            })
            .collect(),
        through: Cursor(page.through),
        bootstrap_complete: page.bootstrap_complete,
    }
}

fn version(version: collections::Version) -> Version {
    Version::new(version.incarnation, version.sequence)
}

bindings::export!(CollectionProbe with_types_in bindings);
