#![cfg(target_arch = "wasm32")]

use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::Mirror;
use clay_collection::Page;
use clay_collection::RowEvidence;
use clay_collection::Version;
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

struct State {
    reader: Rc<collections::CollectionReader>,
    selector: Vec<u8>,
    lease: Option<Rc<collections::ProjectionLease>>,
    mirror: Mirror<Vec<u8>, Rc<GuestValue>>,
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
    writer_a: Option<Rc<collection_registration::RegistrationWriter>>,
    writer_b: Option<Rc<collection_registration::RegistrationWriter>>,
    cap_writer: Option<Rc<collection_registration::RegistrationWriter>>,
    cap_publication: Option<capability_bank::PublishedCapability>,
    cap_proxy: Option<capability_bank::PresentationComponent>,
    cap_version: Option<collections::Version>,
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
        let mut registrations = registrations.into_iter();
        let (writer_a, writer_b, cap_writer) = match (
            registrations.next(),
            registrations.next(),
            registrations.next(),
        ) {
            (None, None, None) => (None, None, None),
            (Some(a), Some(b), None) if a.name == "a" && b.name == "b" => {
                (Some(Rc::new(a.writer)), Some(Rc::new(b.writer)), None)
            }
            (Some(cap), None, None) if cap.name == "cap" => (None, None, Some(Rc::new(cap.writer))),
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
            mirror: Mirror::empty(),
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
            cap_version: None,
            scenario: "idle",
            log: Vec::new(),
        }));
        STATE.with_borrow_mut(|slot| {
            assert!(slot.replace(state.clone()).is_none());
        });
        wit_bindgen::spawn_local(read_loop(state));
        Ok(micro_lifecycle::Initialized {
            root: micro_lifecycle::InstanceRoot::new(clay_mdk::StatelessRoot),
            provisions: Vec::new(),
        })
    }
}

impl presentation_events::Guest for CollectionProbe {
    fn mount(
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
            cap_redeem(&state, surface);
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
            self.mirror.len()
        );
        for (key, value) in self.mirror.rows() {
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
        ciborium::Value::Array(pair) => match &pair[..] {
            [
                ciborium::Value::Integer(writer),
                ciborium::Value::Bytes(local),
            ] => {
                format!("{}:{}", u64::try_from(*writer).unwrap(), decode(local))
            }
            _ => panic!("registration key is not a [writer, local-key] pair"),
        },
        _ => panic!("collection key is neither a string nor a registration pair"),
    }
}

fn accepted(
    result: Result<
        collection_registration::MutationAccepted,
        collection_registration::MutationError,
    >,
) -> (String, Option<collections::Version>) {
    match result {
        Ok(accepted) => (
            format!("ok@{}", accepted.row_version.sequence),
            Some(accepted.row_version),
        ),
        Err(error) => (failure(error), None),
    }
}

fn failure(error: collection_registration::MutationError) -> String {
    match error {
        collection_registration::MutationError::Conflict(Some(current)) => {
            format!("conflict@{}", current.sequence)
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

fn removed(
    result: Result<
        Option<collection_registration::MutationAccepted>,
        collection_registration::MutationError,
    >,
) -> String {
    match result {
        Ok(Some(accepted)) => format!("ok@{}", accepted.row_version.sequence),
        Ok(None) => "absent".into(),
        Err(error) => failure(error),
    }
}

async fn write_scenario(state: Rc<RefCell<State>>) {
    let value = |body: &str| collections::EncodedValue {
        body: encode(body),
        capabilities: Vec::new(),
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

    let (outcome, a_x) = accepted(a.put(encode("x"), None, value("a1")).await);
    log(format!("a-put-x={outcome}"));
    let (outcome, b_x) = accepted(b.put(encode("x"), None, value("b1")).await);
    log(format!("b-put-x={outcome}"));
    let (outcome, _) = accepted(a.put(encode("x"), None, value("blind")).await);
    log(format!("a-blind-put-x={outcome}"));
    let (outcome, a_x2) = accepted(a.put(encode("x"), a_x, value("a2")).await);
    log(format!("a-cas-put-x={outcome}"));
    log(format!(
        "a-stale-remove-x={}",
        removed(a.remove(encode("x"), a_x).await)
    ));
    let (outcome, _) = accepted(b.put(encode("x"), a_x2, value("stolen")).await);
    log(format!("b-put-with-a-version-x={outcome}"));
    let (outcome, a_y) = accepted(a.put(encode("y"), None, value("a3")).await);
    log(format!("a-put-y={outcome}"));
    log(format!(
        "b-remove-y={}",
        removed(b.remove(encode("y"), None).await)
    ));
    log(format!(
        "b-remove-with-a-version-y={}",
        removed(b.remove(encode("y"), a_y).await)
    ));
    let (outcome, _) = accepted(a.put(vec![0; 1025], None, value("big-key")).await);
    log(format!("a-put-big-key={outcome}"));
    let big = collections::EncodedValue {
        body: vec![0; 64 * 1024 + 1],
        capabilities: Vec::new(),
    };
    let (outcome, _) = accepted(a.put(encode("y"), a_y, big).await);
    log(format!("a-put-big-value-y={outcome}"));
    log(format!(
        "b-remove-x={}",
        removed(b.remove(encode("x"), b_x).await)
    ));
    let (outcome, _) = accepted(b.put(encode("x"), None, value("b2")).await);
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
    let published = match capability_bank::publish_presentation_component() {
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
                None,
                collections::EncodedValue {
                    body: cap_tagged_body(),
                    capabilities: Vec::new(),
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
    let (outcome, version) = accepted(
        writer
            .put(
                encode("cap"),
                None,
                collections::EncodedValue {
                    body: cap_tagged_body(),
                    capabilities: vec![reference],
                },
            )
            .await,
    );
    log(format!("cap-put={outcome}"));
    let mut state = state.borrow_mut();
    if version.is_some() {
        state.cap_publication = Some(published);
        state.cap_version = version;
    }
    state.scenario = "done";
    render(&mut state);
}

fn cap_redeem(state: &Rc<RefCell<State>>, surface: &presentation::Surface) {
    let value = {
        let state = state.borrow();
        state
            .mirror
            .rows()
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
    let proxy = match value.capabilities[0].as_presentation_component() {
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
    state.borrow_mut().cap_proxy = Some(proxy);
    let mut edits = WireMutations::default();
    edits.push_id(ElementId::from_raw(1));
    edits.mount_component(&component);
    edits.append_children(1);
    edits.pop();
    let outcome = match surface.apply(&edits.take()) {
        Ok(_) => "mounted",
        Err(presentation::PresentationError::Revoked) => "revoked",
        Err(presentation::PresentationError::Unsupported(_)) => "unsupported",
        Err(presentation::PresentationError::InvalidBatch(_)) => "invalid-batch",
    };
    log(format!("cap-redeem={outcome}"));
}

async fn cap_close_scenario(state: Rc<RefCell<State>>) {
    let (writer, version) = {
        let state = state.borrow();
        (
            state.cap_writer.clone().expect("cap writer is bound"),
            state.cap_version,
        )
    };
    let log = |line: String| {
        let mut state = state.borrow_mut();
        state.log.push(line);
        render(&mut state);
    };
    log(format!(
        "cap-remove={}",
        removed(writer.remove(encode("cap"), version).await)
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
    state.cap_version = None;
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
                let cursor = state.mirror.cursor().cloned();
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
            state.mirror.begin_lease(acquired_resume.as_ref());
            state.resumes += 1;
            state.lease = Some(Rc::new(lease));
            state.status = "reading";
            render(&mut state);
            continue;
        }

        let (lease, ticket, committed) = {
            let mut state = state.borrow_mut();
            let lease = state
                .lease
                .as_ref()
                .expect("collection lease missing")
                .clone();
            let ticket = state.mirror.begin_read();
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
            let mut state = state.borrow_mut();
            if state.stop_requested || state.resume_requested {
                assert!(state.mirror.cancel_read(&ticket));
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
                    assert!(state.mirror.cancel_read(&ticket));
                    state.replays += 1;
                } else {
                    assert!(state.mirror.apply(&ticket, &page));
                    state.status = if state.mirror.bootstrap_complete() {
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
                    state.mirror.clear();
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
                state.mirror.clear();
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

fn decode_page(page: collections::Page) -> Page<Vec<u8>, Rc<GuestValue>> {
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
                            let value = Rc::new(GuestValue {
                                body: upsert.value.body,
                                capabilities: upsert.value.capabilities,
                            });
                            decode_value(&value);
                            Change::Upsert {
                                key: upsert.key.body,
                                row_version: version(upsert.row_version),
                                value,
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
