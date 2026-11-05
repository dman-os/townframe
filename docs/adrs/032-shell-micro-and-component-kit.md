# ADR 032: The shell micro and the component kit

**Status:** Accepted (operator, 2026-11-05).

**Related:** [FDR 006: the shell grammar](../fdrs/006-shell-grammar.md),
[FDR 005: the tiles primitive](../fdrs/005-tiles-primitive.md),
[ADR 030](030-micro-lifecycle-and-manifest-contract.md),
[ADR 031](031-manifest-declared-collections-and-lazy-capabilities.md).

## Context

FDR 006 settles the shell's functional vocabulary in two layers. This record decides how the
first, structural layer is built on Clay: panels and resizable sections, list and details
(with list-to-list stacking), back and forward, the sidebar socket, the sidebar/back chrome
integration, and sheets — on desktop, in the browser build and on Android.

Dayframe is one composition of Clay among several: a dedicated agentic-IDE app is planned on the
same framework, and apps are produced by choosing at build time which micros and plugs an image
carries. The shell vocabulary is therefore shared across compositions, and alternative shells
may implement it.

## Decision

### 1. One native shell micro, plus a vocabulary crate

The shell is a single native micro, `micro_shell`, with its contract in `micro_shell_manifest`
(NSIDs under `org.example.dayframe.shell.*`, row types, call request/response types). The
contract crate is backend-free (the `clay_mdk/contract` feature), so any micro, native or Wasm,
can depend on it. An alternative shell implements the same contract; other compositions reuse
both crates.

Nothing in the shell is inherently native. It is native because it is on every startup path and
owns the frame, not because it needs platform APIs.

### 2. Panels are contributed, the shell places them

Micros contribute **panel rows** to the shell's durable panels collection, as Settings sections
and toolbar buttons are contributed today: static metadata (title, the sockets the panel may
fill, whether it is a list) and a lazy presentation attachment. Reading the rows never starts a
contributor; mounting a panel redeems its attachment through a `PresentationSlot` (ADR 031 §9).

How a panel *instance* is identified — a singleton panel versus "the editor showing document D"
— is internal to the shell and is not part of the contract yet. IDEs draw the same line
(IntelliJ's tool windows are singletons anchored to a side; editor tabs are instances keyed by
what they show). The first slice needs only singletons and list-and-details over row
attachments: a Settings section row already attaches its own detail presentation, so opening a
section is mounting that attachment. Keyed instances arrive with tabs, after querying over
collections and datums is designed.

### 3. The shell owns navigation

The shell keeps the navigation stack (back and forward) for list-and-details, including
list-to-list stacking. Entries name what is shown (a panel row, or a row a list opened), never a
rendered surface. The platform back gesture lands on this stack. Micros request navigation
through a shell call rather than keeping private stacks, so back behaves the same in every micro.
The design borrows from Android's single-activity back stack and from browser-history routers;
the first slice keeps one stack per open context.

### 4. Arrangement and persistence

The shell lays out sidebar, list and details by width (compact: one panel at a time with the
details over the list; wider: side by side), with resizable sections whose weights it keeps.
General configuration — weights, sidebar contents, preferences — lives in the shell's
configuration document and syncs. Open-context state — the navigation stack, the open panels —
is device-local and is never applied from another device.

### 5. The sidebar/back button is a chrome integration

A micro owns its app bar. The shell publishes the sidebar/back button as a presentation that a
micro requests and mounts wherever its app bar wants it; the button shows **back** while the
current context has somewhere to go back to and **open the sidebar** otherwise, and acts on the
shell's state. A micro that never mounts it still gets the platform back gesture.

### 6. Sheets

A panel opens a sheet through a shell call naming a presentation it may mount. The shell presents
it per FDR 006 (bottom sheet on a phone, side sheet in a wide window), puts it on the navigation
stack so back dismisses it, and drops it when its panel goes away.

### 7. The component kit

A new crate, `clay_dioxus_kit`, becomes the shared component library and replaces
`src/ui` over time. It links into native and Wasm micros alike, so it depends on Dioxus and the
design tokens only, never on a backend or a renderer. It is named for its framework: a kit for
another UI framework would be a sibling crate implementing the same components, and Clay
compositions other than Dayframe (the agentic IDE) use it too. It starts with what the shell uses —
resizable sections, the list-and-details scaffold, sheets, the sidebar/back button — and the
tile list lands when home needs it. Its conventions are set before it grows (see below), because
every later component will copy what is already there.

### 8. Settings is the first consumer

Home opens Settings; Settings' sections are the list and each section's attachment is its
details. Settings stops arranging its own sections and becomes a list panel.

### 9. Platforms and tests

- Desktop (Blitz) and the browser build ship with this work; Android follows as a stacked PR
  (`dayframe_mobile` wired to the composition, Wasmtime on Android) whose job is also to find
  what Dioxus on Android allows before the UI commits to anything it cannot do.
- The browser build is the broad integration surface: Playwright flows, and property tests with
  Bombadil (invariants such as "back always returns to the previous list" and "a sheet always
  belongs to a visible panel"). Desktop keeps its in-crate e2e tests for engine parity; platform
  features (camera) get platform tests.

## First slice as built

Recorded here because the contract in `micro_shell_manifest` carries it and a later shell must
match it.

- **Opening a row.** A list panel's row names the panel's `details` attachment, and the shell
  mounts it in the details position keyed by the open row. A presentation takes no parameters
  and the shell, not the list, owns the stack, so the details presentation asks the shell which
  row is open (`opened` call) when it mounts; a different row is a fresh mount and a fresh
  question. Rejected: the list passing a capability to the shell (a call carries data, not
  authority); the shell reading the list's collection (it has no binding to it); a Dioxus
  context for in-process mounts (not available to guests).
- **Stack semantics.** History-like (back, forward, a push discards the forward entries) with
  two exceptions: an overlay (a sheet, the compact sidebar drawer) exists only on top and is
  discarded for good when anything leaves it, so forward never resurrects a dismissed sheet; and
  opening another row of the list whose row is already open replaces that row's entry, so one
  back leaves the list rather than stepping through every row glanced at. The base is implicit
  (nothing pushed shows home); going home is an ordinary push.
- **Sheets are panels.** A sheet is a panel row whose sockets include `sheet`; `open_sheet`
  names it. A sheet goes with its panel: entries naming a retracted panel are pruned.
- **Host inputs.** The native shell receives the width available to its frame and the platform
  back gesture from the composition as a Dioxus context (`micro_shell::HostInputs`); it never
  measures the DOM.
- **Sidebar.** Wide hosts keep it beside the content (a user can hide it, and the button then
  offers to show it); compact hosts show it as a sheet because the kit has no left drawer yet.

## Component kit conventions

To be settled in the kit's first commit and enforced by review:

- **Headless behaviour, styled skin.** Behaviour (state, focus, keyboard, ARIA) is separated from
  appearance, in the manner of Radix or Base UI; the skin uses design-language tokens only
  (COMPOSE-001).
- **Variants are explicit API** (COMPOSE-003), never ad-hoc classes passed through.
- **Accessible by construction:** every interactive component has a role and an accessible name,
  and a keyboard path wherever the renderer allows one (INTERACTION-002/003; Blitz cannot take
  keyboard focus today, per the `blitz-dioxus-native-limits` skill, so the web build carries the
  keyboard tests); tests query by role and name.
- **Renderer limits are documented at the component** (Blitz, per the
  `blitz-dioxus-native-limits` skill), not discovered by callers.
- **Each component has a showcase specimen and a behaviour test**, and the showcase is the place
  new components are reviewed.

## Deferred

Tabs and history (FDR 006 semantic layer), keyed panel instances, the command palette, the
search convention, the arc strip, tables beyond one default, and alternative shells.

## Notes

- Every push goes through the shell's navigation call; the kit's list-and-details scaffold makes
  that call when a row is opened, so there is one path onto the stack.
