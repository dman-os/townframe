//! Event storms: drive a component with every event it listens for, in seeded random order, and
//! fail on any panic.
//!
//! Dioxus signals are borrow-checked at run time, so a handler that holds a `peek()` guard while
//! calling something that writes the same signal compiles, passes the happy-path test and
//! panics (`AlreadyBorrowed`) the first time a real pointer produces that event order. A storm
//! finds those without anyone predicting the order: it mounts the component in a [`VirtualDom`],
//! learns which `(element, event)` pairs have a listener from the mutation stream, then for many
//! steps picks a live listener at random, dispatches a synthetic event of that kind at a random
//! position, and lets tasks and timers run for a random span of virtual time before the next
//! step. A release with no press, a wheel mid-drag, a `pointerleave` during a settle animation,
//! a move at the far edge: all of them turn up in some seed.
//!
//! ```ignore
//! #[test]
//! fn the_strip_survives_a_storm() {
//!     Storm::of(app).viewport(1076.0, 860.0).run_seeds(0..64);
//! }
//! ```
//!
//! A failure names its seed and the last events dispatched; [`Storm::run`] with that seed
//! replays it exactly, so a found panic is kept as a one-line regression test.
//!
//! # Determinism
//!
//! The runtime is a current-thread tokio runtime with a paused clock, so `tokio::time::sleep`
//! inside a component (a settle animation) advances only when the storm lets virtual time pass:
//! no real sleeps, no wall-clock flakiness, and a 300 ms animation costs microseconds. Components
//! that sleep through another clock are not covered.
//!
//! # What a storm cannot do
//!
//! It has no retained DOM, so it cannot hit-test: it dispatches to the element that owns the
//! listener, as a browser does for an event whose target is that element (bubbling reaches the
//! ancestors' handlers as usual). It does not check what the component rendered, only that
//! nothing panicked; pair it with behaviour tests for that.
use dioxus_core::BorrowedAttributeValue;
use dioxus_core::ComponentFunction;
use dioxus_core::ElementId;
use dioxus_core::Event;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_core::WriteMutations;
use dioxus_html::Code;
use dioxus_html::EventData;
use dioxus_html::Key;
use dioxus_html::Location;
use dioxus_html::Modifiers;
use dioxus_html::NamedKey;
use dioxus_html::SerializedKeyboardData;
use dioxus_html::SerializedResizeData;
use dioxus_html::geometry::PixelsSize;
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::Duration;

/// How many dispatched events a failure report quotes.
const TRACE_LEN: usize = 12;

/// Virtual time a step lets pass after its event. Zero drains only what is already runnable;
/// the others straddle the frame-sized timers (16 ms) and whole animations (250-300 ms) that
/// components use, so a gesture's next event lands before, in the middle of, or after a settle.
const SETTLE_WINDOWS_MS: [u64; 7] = [0, 0, 1, 5, 16, 100, 300];

/// Events a renderer sends exactly once per element and never repeats, which a storm would only
/// fabricate: skipped rather than synthesized.
const RENDERER_ONLY: [&str; 1] = ["mounted"];

/// A storm over one component: build it with [`Storm::of`] or [`Storm::with_props`], tune it,
/// then [`Storm::run`] a seed or [`Storm::run_seeds`] a range.
pub struct Storm {
    make: Box<dyn Fn() -> VirtualDom>,
    viewport: (f64, f64),
    steps: usize,
}

impl Storm {
    /// A storm over `app`, a root component without props.
    pub fn of(app: fn() -> dioxus_core::Element) -> Self {
        Self::from_factory(Box::new(move || VirtualDom::new(app)))
    }

    /// A storm over `app` rendered with `props` (cloned for every seed).
    pub fn with_props<P: Clone + 'static, M: 'static>(
        app: impl ComponentFunction<P, M> + Clone + 'static,
        props: P,
    ) -> Self {
        Self::from_factory(Box::new(move || {
            VirtualDom::new_with_props(app.clone(), props.clone())
        }))
    }

    fn from_factory(make: Box<dyn Fn() -> VirtualDom>) -> Self {
        Self {
            make,
            viewport: (1000.0, 800.0),
            steps: 250,
        }
    }

    /// The window the pointer, wheel and scroll events land in, in px. Positions fall inside it
    /// and a few px past each edge, with the edges themselves over-represented.
    pub fn viewport(mut self, width: f64, height: f64) -> Self {
        self.viewport = (width, height);
        self
    }

    /// How many events one seed dispatches (default 250).
    pub fn steps(mut self, steps: usize) -> Self {
        self.steps = steps;
        self
    }

    /// Run one seed and return how many events of each kind it dispatched. Panics, naming the
    /// seed and the last events, if the component panics or leaves a panic in a task.
    pub fn run(&self, seed: u64) -> BTreeMap<String, usize> {
        // Handlers read their payload through the converter a renderer installs.
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("a current-thread runtime builds");
        let mut trace = VecDeque::new();
        let mut counts = BTreeMap::new();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(self.drive(seed, &mut trace, &mut counts));
        }));
        if let Err(payload) = outcome {
            let message = payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "a non-string panic".to_owned());
            let events: Vec<_> = trace.iter().map(String::as_str).collect();
            panic!(
                "storm seed {seed} panicked: {message}\nlast events (oldest first):\n  {}",
                events.join("\n  ")
            );
        }
        counts
    }

    /// Run every seed in `seeds`. Panics if the whole run dispatched nothing, which means the
    /// component rendered no listeners and the storm proved nothing.
    pub fn run_seeds(&self, seeds: Range<u64>) -> BTreeMap<String, usize> {
        let mut total = BTreeMap::<String, usize>::new();
        for seed in seeds {
            for (name, count) in self.run(seed) {
                *total.entry(name).or_default() += count;
            }
        }
        assert!(
            !total.is_empty(),
            "the storm dispatched no events: the component rendered no listeners"
        );
        total
    }

    async fn drive(
        &self,
        seed: u64,
        trace: &mut VecDeque<String>,
        counts: &mut BTreeMap<String, usize>,
    ) {
        let mut rng = Rng::new(seed);
        let mut vdom = (self.make)();
        let mut listeners = Listeners::default();
        vdom.rebuild(&mut listeners);
        for step in 0..self.steps {
            let live: Vec<_> = listeners
                .live
                .iter()
                .filter(|(_, name)| !RENDERER_ONLY.contains(&name.as_str()))
                .collect();
            if let Some((element, name)) = rng.pick(&live).copied() {
                let (data, bubbles, summary) = synthesize(name, &mut rng, self.viewport);
                if trace.len() == TRACE_LEN {
                    trace.pop_front();
                }
                trace.push_back(format!("#{step} {name} on element {element}: {summary}"));
                *counts.entry(name.clone()).or_default() += 1;
                vdom.runtime().handle_event_for_target(
                    RenderTargetId::ROOT,
                    name,
                    Event::new(data, bubbles),
                    ElementId::from_raw(*element),
                );
            }
            let window = Duration::from_millis(*rng.pick(&SETTLE_WINDOWS_MS).expect("non-empty"));
            settle(&mut vdom, &mut listeners, window).await;
        }
        // Let every outstanding animation and task finish: a panic in the tail of a settle is
        // still the storm's finding.
        settle(&mut vdom, &mut listeners, Duration::from_secs(2)).await;
    }
}

/// Render what the last event dirtied, then let tasks and timers run for `window` of virtual
/// time, rendering as they wake the component.
async fn settle(vdom: &mut VirtualDom, listeners: &mut Listeners, window: Duration) {
    vdom.render_immediate(listeners);
    tokio::time::timeout(window, async {
        loop {
            vdom.wait_for_work().await;
            vdom.render_immediate(listeners);
        }
    })
    .await
    .unwrap_err();
}

/// The live `(element, event name)` pairs, tracked from the mutation stream.
///
/// A renderer finds an element by walking its tree; this tracks only the ids the stack carries,
/// which is all dispatching needs. A removed subtree's descendants are not forgotten (the stream
/// does not enumerate them), but dispatching to a stale id is a no-op in the runtime, or lands on
/// whatever element reused the id, which is a real element receiving a real event kind.
#[derive(Default)]
struct Listeners {
    /// The element id each stack node was registered under, if it has been.
    stack: Vec<Option<ElementId>>,
    live: BTreeSet<(usize, String)>,
}

impl Listeners {
    fn forget(&mut self, id: Option<ElementId>) {
        if let Some(id) = id {
            self.live.retain(|(element, _)| *element != id.raw());
        }
    }

    fn popped(&mut self, count: usize) {
        let at = self.stack.len() - count;
        self.stack.truncate(at);
    }
}

impl WriteMutations for Listeners {
    fn push_id(&mut self, id: ElementId) {
        self.stack.push(Some(id));
    }

    fn child(&mut self, _index: usize) {
        *self.stack.last_mut().expect("a node to descend from") = None;
    }

    fn pop(&mut self) {
        self.popped(1);
    }

    fn create_element(&mut self, _tag: &str, _ns: Option<&str>) {
        self.stack.push(None);
    }

    fn create_text(&mut self, _value: &str) {
        self.stack.push(None);
    }

    fn clone(&mut self) {
        *self.stack.last_mut().expect("a node to clone") = None;
    }

    fn append_children(&mut self, m: usize) {
        self.popped(m);
    }

    fn replace_with(&mut self, m: usize) {
        self.popped(m);
        let replaced = self.stack.pop().expect("a node to replace");
        self.forget(replaced);
    }

    fn insert_after(&mut self, m: usize) {
        self.popped(m);
    }

    fn insert_before(&mut self, m: usize) {
        self.popped(m);
    }

    fn set_attribute(
        &mut self,
        _name: &str,
        _ns: Option<&str>,
        _value: BorrowedAttributeValue<'_>,
    ) {
    }

    fn set_text(&mut self, _value: &str) {}

    fn add_event_listener(&mut self, name: &str) {
        let id = self
            .stack
            .last()
            .copied()
            .flatten()
            .expect("a listener goes on an element that has been given an id");
        self.live.insert((id.raw(), name.to_owned()));
    }

    fn remove_event_listener(&mut self, name: &str) {
        if let Some(id) = self.stack.last().copied().flatten() {
            self.live.remove(&(id.raw(), name.to_owned()));
        }
    }

    fn remove(&mut self) {
        let removed = self.stack.pop().expect("a node to remove");
        self.forget(removed);
    }

    fn set_id(&mut self, id: ElementId) {
        *self.stack.last_mut().expect("a node to register") = Some(id);
    }
}

/// splitmix64: a few lines, no dependency, and the same sequence on every platform.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn bits(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.bits() % bound as u64) as usize
    }

    fn unit(&mut self) -> f64 {
        (self.bits() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        (!items.is_empty()).then(|| &items[self.below(items.len())])
    }

    /// A position along an axis `extent` long: mostly inside, sometimes just outside, and a
    /// quarter of the time exactly on or next to an edge, where clamping bugs live.
    fn position(&mut self, extent: f64) -> f64 {
        if self.chance(4) {
            let edges = [
                -1.0,
                0.0,
                1.0,
                extent - 1.0,
                extent,
                extent + 1.0,
                extent * 2.0,
            ];
            return edges[self.below(edges.len())];
        }
        (self.unit() * 1.2 - 0.1) * extent
    }
}

/// One synthetic event of kind `name`: its payload, whether it bubbles, and a one-line summary
/// for the failure trace. Panics on a kind it has no payload for, so a component that listens
/// for something new is a visible gap rather than silently unstormed.
fn synthesize(name: &str, rng: &mut Rng, viewport: (f64, f64)) -> (Rc<dyn Any>, bool, String) {
    let (width, height) = viewport;
    let (x, y) = (rng.position(width), rng.position(height));
    // Enter and leave do not bubble, nor do focus and scroll: dispatching them with
    // propagation would reach handlers a browser never would.
    let bubbles = !matches!(
        name,
        "pointerenter" | "pointerleave" | "mouseenter" | "mouseleave" | "focus" | "blur" | "scroll"
    );
    let point = |buttons: u8| {
        serde_json::json!({
            "alt_key": false, "button": 0, "buttons": buttons, "ctrl_key": false,
            "meta_key": false, "shift_key": false,
            "client_x": x, "client_y": y, "offset_x": x, "offset_y": y,
            "page_x": x, "page_y": y, "screen_x": x, "screen_y": y,
        })
    };
    // Whether the primary button is held depends on the event: a press and a drag hold it, a
    // release and a hover do not. A move is a coin flip, because losing the release outside the
    // window is the realistic way for the two to disagree.
    let held: u8 = match name {
        "pointerdown" | "mousedown" => 1,
        "pointermove" | "mousemove" => u8::from(rng.chance(2)),
        _ => 0,
    };
    let (json, summary) = match name {
        "click" | "dblclick" | "auxclick" | "contextmenu" | "mousedown" | "mouseup"
        | "mousemove" | "mouseenter" | "mouseleave" | "mouseover" | "mouseout" => {
            (point(held), format!("({x:.0}, {y:.0}) buttons={held}"))
        }
        "pointerdown" | "pointermove" | "pointerup" | "pointercancel" | "pointerenter"
        | "pointerleave" | "pointerover" | "pointerout" | "gotpointercapture"
        | "lostpointercapture" => {
            let mut json = point(held);
            json.as_object_mut().expect("an object").extend(
                serde_json::json!({
                    "pointer_id": 1, "width": 1.0, "height": 1.0,
                    "pressure": if held == 1 { 0.5 } else { 0.0 },
                    "tangential_pressure": 0.0, "tilt_x": 0, "tilt_y": 0, "twist": 0,
                    "pointer_type": "mouse", "is_primary": true,
                })
                .as_object()
                .expect("an object")
                .clone(),
            );
            (json, format!("({x:.0}, {y:.0}) buttons={held}"))
        }
        "wheel" => {
            let deltas = [-240.0, -120.0, -3.0, 0.0, 3.0, 120.0, 240.0];
            let (dx, dy) = (
                if rng.chance(3) {
                    deltas[rng.below(deltas.len())]
                } else {
                    0.0
                },
                deltas[rng.below(deltas.len())],
            );
            let mut json = point(0);
            json.as_object_mut().expect("an object").extend(
                serde_json::json!({
                    "delta_mode": 0, "delta_x": dx, "delta_y": dy, "delta_z": 0.0,
                })
                .as_object()
                .expect("an object")
                .clone(),
            );
            (json, format!("({x:.0}, {y:.0}) delta=({dx}, {dy})"))
        }
        "keydown" | "keyup" | "keypress" => {
            let keys = [
                Key::Named(NamedKey::ArrowUp),
                Key::Named(NamedKey::ArrowDown),
                Key::Named(NamedKey::ArrowLeft),
                Key::Named(NamedKey::ArrowRight),
                Key::Named(NamedKey::Home),
                Key::Named(NamedKey::End),
                Key::Named(NamedKey::PageUp),
                Key::Named(NamedKey::PageDown),
                Key::Named(NamedKey::Enter),
                Key::Named(NamedKey::Escape),
                Key::Named(NamedKey::Tab),
                Key::Character(" ".to_owned()),
                Key::Character("a".to_owned()),
            ];
            let key = keys[rng.below(keys.len())].clone();
            let summary = format!("{key:?}");
            let data = SerializedKeyboardData::new(
                key,
                Code::Unidentified,
                Location::Standard,
                rng.chance(4),
                Modifiers::empty(),
                false,
            );
            return (EventData::Keyboard(data).into_any(), bubbles, summary);
        }
        "focus" | "blur" | "focusin" | "focusout" => (serde_json::json!({}), String::new()),
        "input" | "change" | "submit" | "reset" => {
            let values = ["", "a", "12", "a longer value"];
            let value = values[rng.below(values.len())];
            (
                serde_json::json!({ "value": value, "values": [], "valid": rng.chance(2) }),
                format!("value={value:?}"),
            )
        }
        "beforeinput" => (
            serde_json::json!({
                "input_type": "insertText", "data": "a", "is_composing": false, "value": "a",
            }),
            String::new(),
        ),
        "scroll" => {
            let top = rng.position(3000.0);
            (
                serde_json::json!({
                    "scroll_top": top, "scroll_left": 0.0, "scroll_width": 1000,
                    "scroll_height": 3000, "client_width": 400, "client_height": 800,
                }),
                format!("top={top:.0}"),
            )
        }
        "resize" => {
            let size = PixelsSize::new(rng.position(width).abs(), rng.position(height).abs());
            let summary = format!("{size:?}");
            let data = SerializedResizeData::new(size, size);
            return (EventData::Resize(data).into_any(), bubbles, summary);
        }
        other => {
            panic!("dioxus_storm has no payload for the `{other}` event: add one to `synthesize`")
        }
    };
    let event: dioxus_html::HtmlEvent = serde_json::from_value(serde_json::json!({
        "element": 0, "name": name, "bubbles": bubbles, "data": json,
    }))
    .unwrap_or_else(|error| panic!("the `{name}` payload does not deserialize: {error}"));
    (event.data.into_any(), bubbles, summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dioxus::prelude::*;

    /// The bug a storm exists for: the `peek` guard lives to the end of the statement while the
    /// callee writes the same signal.
    fn double_borrow() -> Element {
        let mut count = use_signal(|| 0u32);
        let bump = use_callback(move |next: u32| count.set(next));
        rsx! {
            button {
                onclick: move |_| bump.call(*count.peek() + 1),
                "{count}"
            }
        }
    }

    fn fixed() -> Element {
        let mut count = use_signal(|| 0u32);
        let bump = use_callback(move |next: u32| count.set(next));
        rsx! {
            button {
                onclick: move |_| {
                    let current = *count.peek();
                    bump.call(current + 1);
                },
                "{count}"
            }
        }
    }

    #[test]
    fn a_held_guard_is_found_and_names_its_seed() {
        let outcome = std::panic::catch_unwind(|| Storm::of(double_borrow).run_seeds(0..1));
        let payload = outcome.expect_err("the double borrow must panic");
        let message = payload.downcast_ref::<String>().expect("a formatted panic");
        assert!(message.contains("storm seed 0 panicked"), "{message}");
        assert!(message.contains("click on element"), "{message}");
    }

    #[test]
    fn the_copied_out_version_survives_and_the_storm_dispatches() {
        let dispatched = Storm::of(fixed).run_seeds(0..4);
        assert!(dispatched["click"] > 0, "{dispatched:?}");
    }

    /// Timers run in virtual time and every event kind the component listens for is dispatched.
    fn animated() -> Element {
        let mut level = use_signal(|| 0u32);
        let mut moves = use_signal(|| 0u32);
        rsx! {
            div {
                onpointerdown: move |_| {
                    level.set(1);
                    spawn(async move {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        level.set(0);
                    });
                },
                onpointermove: move |event| {
                    let x = event.data().client_coordinates().x;
                    moves.set(x as u32);
                },
                onwheel: move |event| moves.set(event.data().delta().strip_units().y as u32),
                onkeydown: move |event| moves.set(format!("{:?}", event.key()).len() as u32),
                onfocus: move |_| level.set(2),
                oninput: move |event| moves.set(event.value().len() as u32),
                onscroll: move |event| moves.set(event.data().scroll_top() as u32),
                "{level} {moves}"
            }
        }
    }

    #[test]
    fn every_listened_kind_is_dispatched_with_a_payload_its_handler_can_read() {
        let dispatched = Storm::of(animated).steps(400).run_seeds(0..4);
        for kind in [
            "pointerdown",
            "pointermove",
            "wheel",
            "keydown",
            "focus",
            "input",
            "scroll",
        ] {
            assert!(
                dispatched.get(kind).copied().unwrap_or(0) > 0,
                "{kind}: {dispatched:?}"
            );
        }
    }

    #[test]
    fn a_seed_replays_to_the_same_events() {
        let first = Storm::of(animated).run(7);
        let again = Storm::of(animated).run(7);
        assert_eq!(first, again);
    }
}
