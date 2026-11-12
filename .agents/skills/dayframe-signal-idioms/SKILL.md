---
name: dayframe-signal-idioms
description: Crash-proof Dioxus state idioms for Dayframe UI code. Use whenever writing or reviewing a component that holds state, a gesture handler, an async task that writes signals, or a component that takes props it also keeps in a signal. Also covers the event-storm test (dioxus_storm) that finds the panics these idioms prevent.
---

# State and signals: crash-proof idioms

Dioxus signals are borrow-checked at run time (`RefCell` semantics) and nothing in the type system sees it. The crashes it produces compile, pass the happy-path test and appear the first time a real pointer produces an unlucky event order. `ArcIndex` had three in two days: a `peek` guard alive across a `commit` that wrote the same signal (`AlreadyBorrowed`), a pointerup releasing a gesture it never started, and props frozen by `use_signal(|| items)`. The rules below make each structurally impossible. `clippy.toml` already denies a `read`/`write` guard held across `.await`; the rest is on you.

This skill is the state counterpart to `dayframe-design-language` (tokens, layout) and `dayframe-native-dev-loop` (running it).

## 1. Copy values out; never hold a guard across a call

A guard from `peek()`, `read()` or `write()` lives to the end of the *statement*, so an argument expression keeps it alive through the call it feeds.

```rust
// Don't: the `peek` guard lives until the statement ends; `commit` writes `focus`.
commit.call(*focus.peek() + step);
if let Some(d) = drag.write().as_mut() { notify.call(d.delta); }

// Do: take a copy in its own statement. The guard is gone before anything runs.
let current = *focus.peek();
commit.call(current + step);
let delta = drag.peek().map(|d| d.delta);   // or `let v = s();` / `s.cloned()`
```

- `s()` (clone out, subscribes) in render; `*s.peek()` / `s.peek().clone()` in handlers and tasks (does not subscribe).
- Never bind a guard (`let g = s.read();`) in a component body or handler. A body-long guard blocks every write the render's own handlers make later and is invisible in review.
- A guard may be used for a pure expression inside one statement (`s.read().contains(&k)`), never as an argument to, or receiver around, anything that can run user code: `Callback::call`, `EventHandler::call`, `Signal::set`, `spawn`.
- A signal that is never written is not a signal: `use_hook(...)`.

## 2. Never capture props in `use_signal`

`use_signal(|| prop)` runs once. When the parent passes something else (a tab switch, a resize) the component keeps the first value for its lifetime.

```rust
// Don't
let items = use_signal(|| items);

// Do: derive from props on every render.
let count = items.len();
// Do: for an expensive derivation, `use_memo` over a signal you own or `use_reactive`.
// Do: if identity should reset state, key the component: `Strip { key: "{tab}", items }`.
```

State that *starts* from a prop and is then the component's own is fine, and is named for it (`initially_open`). Anything else is a derivation.

## 3. One stateful component = one state struct in one signal + a pure `next`

Several signals that must agree (`focus`, `settle`, `thumb_x`, `pull`, `generation`) are a state machine written one field at a time from helpers, with no place to see an illegal combination. Make it one value and one transition function.

```rust
#[derive(Clone, PartialEq)]
struct State { focus: f64, thumb_x: Option<f64>, generation: u64 /* … */ }
enum Event { Press { x: f64, y: f64 }, Release, Tick { generation: u64, step: u32 } /* … */ }
enum Effect { Select(usize), Settle(u64) }               // what to do, as data

impl State {
    /// Pure: no signals, no spawn, no callbacks. Unit-test it directly.
    fn next(&self, props: Props, event: Event) -> (State, Vec<Effect>) { /* … */ }
}

// In the component: the one place state is written.
let state = use_signal(State::default);
let apply = move |event| {
    let mut state = state;
    let before = state.peek().clone();                    // guard ends here
    let (after, effects) = before.next(props, event);
    if after != before { state.set(after); }
    effects
};
let send = use_callback(move |event| for effect in apply(event) { /* run effects */ });
// Handlers only send:   onpointerup: move |_| send.call(Event::Release)
```

- Effects run *after* the write, with nothing borrowed. Timers and animations are events: an effect spawns a task that sends `Tick { generation, step }`, and a transition drops a tick whose generation is stale. `apply` is `Copy` (it captures `Copy` values), so the task gets its own.
- Layout derived from props is computed inside `next` from the state it is transitioning, not captured by the handler from an earlier render.
- Reference: `src/ui/arc_index.rs` (`ArcState`, `ArcEvent`, `Effect`, `ArcState::next`) and its `transitions` tests.

## 4. Helpers take and return values, not `Signal`s

A function that takes `Signal<T>` and writes it hides a write from its caller, which is how guard-held-across-call bugs get composed. Pass values in and return the new value (or a `Transition`); let the component own the write. Hooks (`use_*`) are the exception, because that is their job.

## 5. Handlers guard on state

An event can arrive without the one before it: a release whose press went to a scrollbar, a move after the pointer left the window, a tick of a cancelled animation, a second click while a write is in flight. The transition decides, from state, whether the event applies.

```rust
// Do: release only what you pressed.
ArcEvent::Release => if state.thumb_x.is_some() { /* start settling */ }
// Do: a tick applies only to the animation that sent it.
ArcEvent::Tick { generation, .. } => if state.generation == generation { /* … */ }
```

The handler may read state only to decide a non-state matter (`prevent_default`), through a copied-out value (`let pressed = view.thumb_x.is_some();`).

## 6. Prove it with an event storm

`dioxus_storm` (`src/dioxus_storm`, a dev-dependency) mounts a component in a `VirtualDom`, learns every `(element, event)` listener from the mutation stream, and dispatches every event kind each one takes (pointer down/move/up/cancel/leave, click, wheel, keydown, focus/blur, input, scroll, resize) in seeded random order, at random positions including the edges, with animation timers running in virtual time between steps. Any panic fails with the seed and the last events.

```rust
#[test]
fn a_sheet_survives_any_order_of_events() {
    Storm::of(owner).run_seeds(0..64);                      // `owner` = the component with its real parent state
    // Storm::with_props(owner, width).viewport(900.0, 600.0).steps(400).run_seeds(0..32);
}
```

- Every stateful component gets one, mounted under an owner that answers its callbacks as the app does (so the props change mid-gesture, too).
- A found panic is fixed at the cause with the idioms above, and its seed stays as `Storm::of(owner).run(SEED);` beside the fix.
- A storm checks that nothing panics, not that anything is right: keep the behaviour tests.
- `run_seeds` fails if nothing was dispatched, so a component with no listeners cannot pass vacuously. A listener kind with no payload in `dioxus_storm::synthesize` panics by name: add it there.
