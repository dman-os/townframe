# Dayframe micros — living design log

Scratch log for the `src/dayframe_dx` micro-composition PR. It exists so that work can be handed
between agents and turns without re-deriving intent.

Rules for this file:

- The **design discussion is the Bible**: the original transcript plus the ADR 014 amendment
  (`docs/adrs/014-dayframe-dx-application-architecture.md`). Where this file and those disagree,
  those win.
- Record every decision with its rationale and the trade-off it accepted, so a later agent can tell
  a settled decision from an accident.
- Record every open question instead of silently deciding it. Product and design questions go to the
  operator; implementation details do not.
- Append a turn log entry each turn: what was evaluated, what was changed, what was verified.

## Settled ground rules

Carried from the design discussion. Do not relitigate these without new evidence.

- Micros are the unit of composition. Features are micros. Layout, toolbars, Settings, chat, email
  are all micros.
- The host owns lifecycle, capabilities, dependency edges, contributions, mounts and cleanup.
  A micro must never implement those with its own mutex registry.
- Capabilities are opaque permission tokens. Bespoke typed APIs are separate from authority.
- No generic interface service: no enumerate-everything, no lookup-anything, no dynamic interface
  interpretation. Micros compile against known typed contracts.
- Declarations are not authority. A declared provision only satisfies a dependency once a live,
  compatible provision exists.
- Identity is three distinct things: stable configured identity, runtime incarnation, presentation
  mount. No multiplicity barriers; keyed/idempotent creation reuses an existing experience.
- Mandatory provisions must all be published before activation reports success; partial activation
  cleans up after itself.
- Lifecycle is event-driven with bounded failure. No timer retry loops. An explicit stop stays
  stopped until its inputs change. Consumers close before providers are destroyed.
- Host-owned permissioned collections. A micro writes only its own entries; overrides are separately
  owned records merged into the effective presentation. Deterministic tie-breaks, never activation
  order.
- Host versus domain grammar: the host mediates collections, components, mounts, invocations and
  lifetimes. Panels, buttons, toolbars and layout are composition-provider vocabulary, not host
  primitives. Internal DOM structure is never an API.
- Contribution values are semantic data (name, action, icon, component reference). The owner renders
  them, so it keeps markup and styling control.
- Documents: per-context authorized mirrors are the narrow waist. Cache warmth is never authority.
  Watch delivery is atomic (no read/subscribe gap). Optimistic writes are host-owned: apply locally,
  publish to authorized mirrors, then accept or roll back. Config lives in facets, reusing existing
  plug manifest declarations — no second config store, no second ACL model.
- Presentation: native and Wasm micros compose into the **same host tree**, sharing layout, focus and
  accessibility. Not embedded isolated screens.
- Async for data, synchronous for presentation. Guests own their local mirrors and reactive state in
  their own address space.
- Dayframe has its **own** runtime abstraction, independent of `daybook_core::Rt`, Wasmtime and Wash,
  so the browser can supply its own scheduling. The dayframe PR must not need changes outside
  `src/dayframe_dx` (plus root `Cargo.toml` wiring).
- Defer to Dioxus; do not reinvent presentation. No full web compatibility yet — Blitz is a useful
  support bound. WHATWG alignment is a future direction, not a current requirement.
- Guest libraries adapt the host boundary for React, Vue, Dioxus and friends. The north star is
  in-app authoring of JS micros that evaluate inside Wasm.
- Everything must be buildable from real consumers, top-down. No speculative platform code, no
  one-shot demos inline in a launcher.

## Decisions

### D1 — The app crate knows nothing about app variants (no "profiles")

The core takes configuration; it never knows whether the app is malleable or locked down. The end
app hard-codes which micros are linked and supplies the startup configuration. "Dayframe" is
everything linked plus runtime loading enabled; a chat-only APK links chat and a layout micro and
nothing else.

Consequence: `desktop`, `mobile` and `web` become launchers that state their linkage and their
configuration source. `micros::register_native` (which registers every micro from inside the app
crate) is deleted.

### D2 — Configuration source is an abstraction, not facets

The composition machinery consumes a configuration source trait: an initial desired composition plus
change notification. Implementations:

- facet-backed, for the malleable app (fed from the data layer, as designed);
- static/linked-in, for locked-down builds;
- mock, for tests.

Nothing below the trait mentions facets. This replaces the in-memory desired map that
`desktop/src/composition.rs` currently hard-codes, and it removes the last reason for the desktop
crate to know about Sample.

### D3 — Micros live in their own crates

One crate per micro, plus small contract crates for what other micros consume (for example a
Settings sections contract). Chosen over cargo features because the compiler then enforces the rule
that generic infrastructure cannot name a concrete micro, and because a locked-down build simply does
not link the crate. This supersedes the earlier "ordinary feature modules, no feature crates"
convention: that convention predates the multi-app requirement.

### D4 — Wire format is a typed, framework-neutral mutation list

The WIT presentation boundary carries a versioned `list<mutation>` whose variants mirror the
`WriteMutations` operation set (create element/text, set attribute, add/remove listener, append,
insert before/after, replace, remove, push/pop, set id), plus events as serialized HTML event
payloads and listener identities. Guest element ids stay guest-local; the host maps them per surface.

Rationale: the operator's constraint is that React, Vue or anything else must be adaptable to this
boundary later. A macro-generated binary opcode stream is Dioxus-internal, has no Rust decoder, and
would force the host to reverse-engineer a format that changes with the pinned revision.

Trade-off accepted: we lose byte-for-byte parity with upstream's JS interpreter, and the previous
decision to carry Dioxus's sledgehammer stream is reversed. The vendored
`sledgehammer-bindgen-macro` patch is expected to become unnecessary and should be removed with it.

**Implemented (turn 8, commit `4c88c77`).** `presentation.wit` now carries `list<mutation>` with
`attribute-value = text | float | int | boolean | none` (`bool` is a WIT keyword, hence `boolean`;
Dioxus's `Any` is excluded — a process-local Rust object cannot cross, and the guest adapter panics
on it as an adapter bug). Guest side: `dayframe_guest::wire::WireMutations` implements
`WriteMutations` and records a batch. Host side: a `replay(&[Mutation], &mut impl WriteMutations)`
drives any host writer — the test backend records through a `RecordingWriter`, the native adapter
will use native-dom's `DioxusState::writer`. Tests prove the guest's batches are identical to a
natively driven Dioxus `VirtualDom` rendering the same counter, so the wire is lossless.
`dioxus-interpreter-js`, the `sledgehammer` feature and the whole `vendor/sledgehammer-bindgen-macro`
patch are gone.

### D5 — One virtual DOM per micro instance, one render target per granted mount

A micro that presents in several places (a Settings detail, a toolbar item, its own window) runs one
virtual DOM and maps each granted mount to a render target. Upstream Core already models this with
`RenderTargetId`/`MultiWriter`. Settled without operator input; revisit only on evidence.

**Implemented (turn 12).** `GuestDom` owns one `VirtualDom` whose root renders a keyed `Portal` per
live mount, plus a `BTreeMap<RenderTargetId, WireMutations>` so each surface only receives its own
edits. `GuestDom::new`/`new_with_props` build the DOM once; `mount(&MountPoint)`, `unmount(&Surface)`,
`render()` and `update(change)` drive it. `update` runs the change inside `VirtualDom::in_runtime` so
a document callback arriving outside event dispatch still schedules its signal subscribers — writing
a signal with no runtime context silently does nothing.

This required two contract additions, because the original `mount` handed the *only* surface handle
back to the host and left the guest nothing to update a sibling mount with: `surface.retain()`
returns a second handle over the same placement, `surface.identity()` is a stable per-mount id (not a
resource rep), and `presentation-events.unmount(borrow<surface>)` lets the host tell the guest to
release a target before the surface is withdrawn. `retain` clones do not create a new writer or
target, and a revoked placement stays revoked through every handle: the engine's `WasmSurface`
withdraws only when its last owner drops.

Consequence for the earlier defect: switching Settings sections, or closing and reopening Settings,
remounts the *same* incarnation, which previously asserted "mounted twice" and trapped. It now
unmounts and remounts one target of one DOM, keeping guest state.

### D6 — Node seam borrows `service_utils_rs` vocabulary but does not depend on it

PR #66 (`stack/cabinet-01-services`) fixes the shape of node services: typed `Operation`s, one
`Client<Proto>` across direct/IRPC/XRPC, `ClientError::Transport { outcome_unknown }`,
`RequestContext` deadlines and cancellation, and the rule that a lagged subscriber receives
`SubscriptionLagged` and must reread.

That vocabulary maps directly onto document mirrors: an optimistic write is a procedure whose outcome
can be unknown; a watch is a subscription that can lag and force a reread. The dayframe host seam
will use the same names and semantics so a `Client<Proto>`-backed adapter can be dropped in later.

It will **not** depend on the crate: #66 is unmerged, and it pulls `tokio/net`, `axum`, `hyper` and
`reqwest`, none of which belong in a browser guest.

**Implemented (turn 12).** `dayframe_app::documents` uses that vocabulary without the dependency:
typed `Operation`/`OperationReceipt`/`OperationStatus`, `OperationError::Transport { outcome_unknown
}`, and `DocumentError::SubscriptionLagged` forcing a reread. There is still no production
`daybook_core` source adapter — the only source is `MockDocuments`, whose privileged controls
(`seed`, `complete`, `settle`, `external_change`, `hold_refreshes`) are deliberately absent from
`DocumentAccess`/`MicroContext`. A successful completion commits its own intent into the source and
rolls the overlay back on failure, so acceptance never waits for a later event watermark.

### D7 — Dioxus fork scope

Fork the **dioxus monorepo**; that covers every package we touch (`core`, `hooks`, `signals`, `html`,
`interpreter`, `native`, `native-dom`, `subsecond`, `devtools`, the `dioxus` meta crate).

Blitz is a separate repo and dioxus pins it from crates.io (`blitz-* =0.3.0-beta.4`), so it needs
forking only if a change must land inside Blitz itself. Nothing decided so far requires that.

Changes we expect to carry in the fork:

- Subsecond: browser hot-patch loader behind a `web` feature so `dioxus-core` componentizes for
  `wasm32-unknown-unknown`. Still required upstream at main; good upstream PR candidate.
- `native-dom`: expose applying mutations into the subtree under a granted node, so a Wasm surface can
  mount into the composed host tree. Upstream keeps `DioxusState`/`MutationWriter` `pub(crate)`.
- Pre-existing local fixes preserved separately: Skia/Vello feature interaction, Android
  `buildToolsVersion`.

Upstream state to rebase onto: dioxus main is at `0.8.0-alpha.3` and contains two changes that affect
us — `WriteMutations::set_attribute` now takes a borrowed value (`245b113`), and `native-dom` buffers
mutations and replays them into Blitz after the virtual DOM runs (`c92e43e`,
`MutationBuffer::replay`). The replay shape is exactly what our host adapter needs.

`SubDocumentAttr` exists upstream but creates an isolated document (iframe-like). It breaks the
same-host-tree requirement, so it is a fallback only, not the plan.

### D8 — Dioxus work is done in a per-turn worktree, never in the shared checkout

Other agents may be working on `/home/asdf/repos/rust/dioxus` at the same time. The harness gives each
turn a fresh townframe worktree, which prevents clashes on townframe but not on other repos. So:

1. **Immediately before every dioxus edit** — not just at turn start — check whether the harness
   moved us to a new townframe worktree (`pwd` / `git rev-parse --show-toplevel`). The harness can
   rotate the worktree mid-turn. If it moved, create a **new** dioxus workspace named after the new
   generation and repoint the `../dioxus` symlink; never keep editing one created under an earlier
   generation. Turn 9 violated this: the workspace was created under `5-a`, the harness rotated to
   `8-a`/`9-a` mid-turn, and edits continued in the stale workspace — which is how someone else's
   uncommitted `mutation_buffer.rs` test module ended up in "my" working copy.
2. Give it a unique location and a unique workspace name (the default name is the destination
   basename, which would collide between agents).
3. Do all dioxus edits there. Commit in the worktree.
4. Remove the worktree afterwards: `jj workspace forget <name>` and delete the directory. jj 0.45 has
   no `jj workspace remove`.
5. The recorded SHA must resolve in the real dioxus repo, not in the worktree. Workspaces share one
   object store, so a committed revision survives removal; keep a namespaced bookmark on it so it
   cannot be collected.
6. Record the SHA in this file and reference it from townframe.

Dioxus uses **jj** (colocated: `.jj` and `.git`). Townframe uses jj in the operator's checkout, but the
arena worktrees are plain git — check before choosing commands. Never use git mutation commands in a
jj repo.

Current fork state (verify before relying on it — other agents move it):

- Rebased onto upstream main `7a817390` = `0.8.0-alpha.3` (resolves `blitz-* 0.3.0-beta.4`). The
  earlier base was `f420ca3d` (`blitz-* 0.3.0-beta.3`).
- Patch chain (both **uncommitted**, held for operator authorization): parent `xssyymun` carries the
  subsecond `web`-feature patch + native Skia/Vello + Android fixes; my working copy `f77ca86f`
  carries the native-dom exposure. Conflicts on rebase (Cargo.toml alpha.1→alpha.3 +
  `default-features = false`; mutation_writer.rs vs upstream's buffered-replay rewrite) are resolved
  in the working copy. Verified `cargo check -p dioxus-native-dom --all-features` clean
  (alpha.3 / beta.4).
- **Correction to the turn-7 record:** commit `340719ec` did *not* add `DioxusState::writer`. My
  rebase conflict resolution replaced upstream's `apply_buffered_mutations` signature instead of
  adding a new method, so the exposure was `apply_buffered_mutations` made `pub` — and the
  follow-up `cargo check` was a 0.81s cache hit that never recompiled, so it did not catch it.
  Lesson recorded: after a conflict resolution, force a real rebuild (touch the file or
  `cargo check` with a changed feature) before claiming verification.
- Fork head is now `c47a76bd8097` (bookmark `dayframe-fork-c47a76bd8097`), a child of `340719ec`
  rather than a rewrite of it, so the earlier SHA stays valid. It re-exports `DioxusState` from the
  native-dom crate root and adds `DioxusState::writer(&mut BaseDocument) -> impl WriteMutations`,
  which drives the Blitz stack machine from an external producer without reading or draining the
  buffered mutations. `apply_buffered_mutations` (upstream's own vdom flow) is untouched.
  `MutationBuffer` remains public from `340719ec`. `StackWriter`/`BlitzBackend`/`RealDom` stay private.
- **Host-adapter recipe, now proven:** `DioxusState::create(granted_node_id)` per surface, then
  `state.writer(&mut base_doc)` and `replay(&batch, &mut writer)`. A guest's typed batches build a
  real Blitz subtree under the granted node, and clicks update that same node in place.
- **Constraint discovered: `DioxusState` is not `Send`** (its `MutationBuffer` holds
  `Rc<dyn AnyValue>`), so it cannot be stored in Wasmtime's `ResourceTable`, which requires
  `Send + 'static`. The host must keep per-surface tree state in a side map keyed by the resource
  rep, and store only `Send` data in the table. This matters for D10: any engine-neutral host
  resource model has to separate "opaque handle the guest holds" from "non-Send renderer state the
  backend owns".
- Isolated workspace `dayframe-gen-aa578c2c49e78545-5-a` at
  `/home/asdf/repos/rust/dioxus/tmp/dayframe-gen-aa578c2c49e78545-5-a`. The `5-b` contestant has a
  sibling workspace in the same checkout; never touch it.
- **Not ours:** that workspace's working copy carries an uncommitted ~100-line `#[cfg(test)] mod
  tests` in `packages/native-dom/src/mutation_buffer.rs` (a `Recording` sink asserting `replay`
  fidelity). It was not written by this workstream. It was backed up, excluded from `c47a76bd`, and
  restored untouched. Whoever owns it should commit or drop it.


### D9 — Keep the existing dioxus path dependency; isolate only the temporary worktree

Townframe keeps its existing `path = "../dioxus/packages/..."` dependencies. There are no dioxus
submodules and no `vendor/dioxus` directory: jj does not provide the submodule workflow we want.

When a turn needs to edit dioxus, create a unique jj workspace under the real checkout's already
ignored `tmp/` directory, for example:

```text
/home/asdf/repos/rust/dioxus/tmp/dayframe-micros-<harness-id>
```

The townframe turn can temporarily build against that workspace when needed. Commit the dioxus change
**only when the operator authorizes it** — the operator is currently holding dioxus commits, so the
isolated workspace stays alive across turns (do not forget/delete it yet). Once authorized to commit:
record the SHA, `jj workspace forget <name>`, and delete the temporary directory. The SHA must resolve
from `/home/asdf/repos/rust/dioxus`; never leave the worktree as the only reference. Do not fork or
modify Blitz in this slice.


### D10 — Two neutral seams: the component engine and the renderer

Running micros must work on native and in the browser, and must not be tied to Blitz. So there are
two independent seams, and neither may leak into the other:

- **Component engine**: instantiate a component from bytes, own its store/instance, call its exports,
  schedule its async work, cancel and clean up per incarnation. Native is Wasmtime; the browser is
  `jco` (or equivalent) driving the same component from JS. This is the runtime abstraction the
  settled rules already require — independent of `daybook_core::Rt`, Wasmtime and Wash, and not
  assuming a Tokio executor.
- **Renderer backend**: replay a typed `list<mutation>` batch into a real tree at a granted
  placement, route events back as WIT `event`s, and withdraw the subtree on close. Blitz/native-dom
  is one backend (via the fork's `DioxusState::writer`); the non-Blitz dioxus desktop backend and a
  webview/JS-DOM backend are others. The batch format is the contract, so a backend is an
  implementation of `WriteMutations` plus event translation — nothing else.

Consequences:

- The wire types and `replay` are renderer-neutral and must not live in a crate that depends on
  Blitz or Wasmtime. Backends are separate crates; `desktop`/`web` pick one.
- A browser host cannot use Wasmtime's `Store`/`ResourceTable`, so host resources (placement,
  surface) must be expressible as opaque ids in anything shared across engines.
- Incremental order: prove the Blitz backend headlessly (real `BaseDocument`, no window), then
  extract the neutral seam, then add the engine seam when the second engine (browser) is real. Do not
  build an engine abstraction before a second engine exists to check it against.

**Step 1 is done (turn 9).** A headless `BaseDocument`, a granted `<div>` root, and the real counter
component: the guest's typed batches build `<button click=… data-dioxus-id="1">0</button>` in the real
tree, a dispatched click updates that same node to `1` then `2` without rebuilding it, and two
incarnations mounted into two placements in one document both address their button as element 1 and
stay independent. That is the per-surface element-id scoping D5 depends on, proven in a real retained
tree rather than a recording. Still missing: Blitz DOM event → WIT `event` translation, subtree
withdrawal on surface drop, and incarnation cleanup.

Because `DioxusState` is not `Send`, the proof keeps tree state in a host side map keyed by resource
rep rather than in the Wasmtime resource table. Whatever shape the engine-neutral seam takes, it has
to allow for renderer state that is neither `Send` nor storable in the engine's own handle table.

### D11 — Guests get documents as named, already-granted WIT resources

`presentation/wit/documents.wit` adds a `documents` import interface, a `document-events` export and
a `micro-lifecycle` export, and a `micro-client` world that includes `presentation-client`. The guest
never looks up a global config store and never mints authority: the host calls
`micro-lifecycle.activate(list<document-binding>)` once per incarnation, and each binding carries an
opaque `document-access` resource for a facet the host already approved. From there the guest can
`watch(selection)` and `mutate(route, mutation)`, both of which return host-owned resources
(`document-watch`, `operation`) rather than callbacks or renderer state.

Values cross as `facet-raw` strings (the existing `doc-id`/`facet-raw` vocabulary), so the guest owns
its own decoding and the host keeps no schema. Watch updates arrive as `changed(snapshot)`;
`subscription-lagged` is a distinct error and also an explicit `lagged(watch-id)` callback, so a
guest that falls behind must reread instead of silently missing state. Writes return an `operation`
whose receipt distinguishes `locally-applied`, `accepted` and
`rolled-back(rejected | transport { outcome-unknown })` — the ADR 014 acceptance boundary, carried
across the address space without collapsing transport uncertainty into rejection.

Trade-off accepted: activation is now a guest export the engine must call before any mount, so a
presentation-only component is no longer a valid `micro-client`. That is deliberate; `micro-client`
is the feature world, and `presentation-client` remains the renderer-only contract.

### D12 — The engine bounds a guest's memory, execution and ingress

A guest is untrusted code inside the UI process, so the Wasmtime store is limited rather than
assumed cooperative: a 64 MiB limit per linear memory and 8192 elements per Wasm table, and a fresh fuel
budget (`WasmIncarnation::FUEL_PER_CALL`) granted immediately before each host-initiated entry
(`activate`, `mount`, `dispatch`, `unmount`, and every document callback). A guest that loops forever
therefore traps with `OutOfFuel` inside that one call, which takes the existing trap path: discard
the store, close the instance, withdraw its contributions, leave siblings running.
`WasmMicro::exhaust_fuel` is the test hook for that path and replaces the old `revoke_surfaces`.

`surface.apply` also checks the batch against `SurfaceTree`'s budgets *before* converting it into a
second owned `Vec`, so an oversized batch is rejected once, not copied and then rejected.
`SurfaceTree` itself enforces mutation/byte/string/node-slot/stack/tree-depth/retained-byte limits,
recycles vacant node slots (so the quota bounds live nodes, not lifetime churn), stamps every node
with a generation so a recycled id cannot be addressed by a stale handle, and rejects an explicit
tag/attribute/listener vocabulary instead of silently falling back to `div`.

Surface resource handles have a separate 1024-handle cap per incarnation; dropping a retained
handle restores capacity without withdrawing a surface that still has owners. The Wasm table limit
does not itself constrain Wasmtime's host-side resource table.

Document delivery has one 1024-deep channel per incarnation, a matching cap on in-flight waits,
and `try_reserve_owned` so registration beyond the queue budget returns `denied` before applying a
mutation. A receipt or snapshot whose guest handle was dropped before
delivery is suppressed (`Work::released`), so the guest never sees a callback for a resource it no
longer holds.

**Remaining hardening boundary:** these checks run after canonical ABI lifting. A malicious component
can alias guest string buffers across list entries, so a small guest memory does not bound aggregate
host allocations while lifting `list<mutation>` or document selections. Host import execution and
cumulative node-owned pending writes are not bounded by Wasm fuel either. The current caps are
specific defenses, not proof of process-wide memory or latency isolation for hostile components.
A bounded/lazy lifting strategy or process isolation remains required before making that claim.

### D13 — Incarnation-owned local work is driven by the composition driver

An activation that needs to keep running after it returns (a guest's document watch/receipt pump, a
background preload) registers it with `MicroActivation::own_future(impl Future<Output = ()>)`. The
future is owned by the incarnation, not by a mount and not by a detached task:
`ApplicationRuntime`'s driver polls every owned future alongside reconciliation, drops the ones whose
`Instance` closed, and reaps them on cancel. `MicroRegistry::drive_owned_futures` is the same driver
for callers that drive the registry directly.

This is what makes event-driven guest callbacks possible without a Tokio task per watch and without
tying delivery to a mounted widget: the Wasm engine registers one `documents::drive` loop per
incarnation, and that loop waits on watches and operations created later. Two rules fall out of it:
never hold the Wasmtime `Store` lock across an await, and never deliver a callback for a resource the
guest already dropped.

Rejected alternative: a `use_future` per mounted surface. It would cancel guest delivery whenever the
widget is unmounted (Settings closed, section switched) even though the incarnation is alive and its
writes are still in flight.

## Settled operator answers

- **Rebase:** fork rebased onto latest upstream main (`0.8.0-alpha.3`, Blitz `0.3.0-beta.4`).
- **Per-micro crates:** required, especially for wasm-based micros.
- **Locked semantics:** linkage and startup set are fixed and there is no runtime loading; a micro's
  own configuration stays editable through the normal data/facet facilities.
- **Configuration source:** lives in the core crate that houses the main abstractions (`dayframe_app`),
  behind the D2 trait.
- **Arena `../dioxus` resolution:** a symlink at the shared
  `…/aa578c2c49e78545/dioxus -> <my dioxus workspace>` works — the harness does not block it. It is
  shared by both contestants, so only the one needing it should (re)point it and I must not rely on it
  persisting. dioxus-side edits are verified in the dioxus workspace regardless.

## Open questions for the operator

1. (Resolved above.)

## Arena and build constraints

Recorded because they shape what a turn can attempt.

- `cargo`/`rustc` on `PATH` are rustup shims that cannot execute here. Build through the flake:
  `nix develop --command cargo …` (nightly 2026-08-16, cargo 1.100.0-nightly).
- Use Cargo's configured default target directory; do not override `CARGO_TARGET_DIR`.
  Query `cargo metadata --no-deps --format-version 1` for artifact locations. Budget for cold builds,
  prefer scoped checks, and never run the full workspace suite for this slice.
- **WIT deps are gitignored and must be fetched per fresh worktree — and the harness can rotate the
  worktree mid-turn**, which silently deletes them again (observed in turn 9: a check that passed
  earlier in the same turn failed later with the same `api_utils_rs` WIT error). Re-run the fetch
  whenever that error appears; tracked edits survive the rotation, gitignored files do not.
- **WIT deps are gitignored and must be fetched per fresh worktree.** The workspace graph pulls
  `api_utils_rs` (via `daybook_types`), whose `wit_bindgen::generate!` needs `wasi:clocks`,
  `wasmcloud:postgres`, … under the gitignored `wit/deps/`. Run `deno run --allow-all x/wit-fetch.ts`
  (it runs `wash wit fetch` for every crate with a `wkg.toml`) **once per new worktree** before any
  dayframe build. There is no network restriction in the arena — do not assume one. After the fetch,
  `cargo check -p dayframe_app -p desktop -p dayframe_guest --all-targets` works directly.
- **`../dioxus` symlink is per-generation.** The harness creates a fresh worktree per generation and
  the shared `…/aa578c2c49e78545/dioxus` symlink may be absent; recreate it to the dioxus workspace at
  the start of any turn that builds townframe. Verified build matrix (turn 8): `dayframe_app`,
  `desktop`, `dayframe_guest` all check clean (`--all-targets`) at dioxus alpha.3 / Blitz beta.4;
  `cargo nextest run -p dayframe_guest` 4/4 pass; clippy clean on everything touched.
- Disk: the worktree filesystem has ~71G free; the root filesystem (`/home`, `/tmp`) has ~6G. Keep
  dioxus worktrees and build artifacts on the worktree filesystem.
- **RAM is the tighter limit.** The dayframe graph compiles `wgpu`, `stylo`, `blitz`, `wasmtime` and
  the whole dioxus stack. The operator reported a suspected OOM on the 14 GiB host; kernel evidence
  was not captured. Run the scoped suite as
  `CARGO_BUILD_JOBS=2 … cargo nextest run --test-threads 2 --no-fail-fast -p …`, with its output
  redirected to a log under the arena tmp dir, and poll the log instead of blocking a tool call on it.
- The arena tmp log directory is per-session: a log path that worked earlier in the turn can stop
  existing after the harness rotates the worktree. `mkdir -p` the current session's directory before
  redirecting into it.
- **Every worktree rotation deletes the gitignored state again**, and it does so mid-turn. Turn 12 hit
  both halves of this after a `generation-14-a` → `generation-15-a` rotation: `../dioxus` was gone
  (`failed to read …/dioxus/packages/core/Cargo.toml`) and `src/*/wit/deps` was gone
  (`package 'wasi:clocks@0.2.6' not found`). Tracked edits survive. On any such error, re-point the
  symlink and re-run `x/wit-fetch.ts` before concluding anything about the code.
- The arena allows bash writes to `/home/asdf/repos/rust/dioxus`, but the file tools are restricted to
  the worktree, `TMPDIR`, attachments and skill dirs.

## Known defects in the current tree

To be fixed as the slices land, not all at once.

1. (Resolved, turn 10) `micros::register_native` deleted; concrete micros extracted into per-micro
   crates (`dayframe_settings_contract`, `dayframe_settings`, `dayframe_sample`, `dayframe_counter`).
2. (Resolved, turn 10) `desktop/src/composition.rs` no longer imports `SampleConfig` or hardcodes
   Sample-only enable/disable controls; it consumes `ConfigurationSource` generically.
3. (Resolved, turn 10) `MicroProvider::activate_async`, `AsyncMicroFactory`, and
   `MicroRegistry::activate_async` added and tested.
4. (Resolved, turn 12) The two parallel data stacks collapsed into one. `backend.rs`/`backend/` and
   `data.rs`/`data/` are empty, unlinked source files; their `dayframe_app` exports are removed.
   `documents.rs` is the only data runtime, and it carries the D6 node-seam vocabulary: a per-write `Operation`
   with a stable id, `OperationReceipt { id, revision, status }`,
   `OperationStatus::{LocallyApplied, Accepted, RolledBack(OperationError)}`, and
   `OperationError::{Rejected, Transport { outcome_unknown }}`. Gap-free continuation is
   `DocumentWatch::next(after_revision)` / `Operation::next(after_revision)`, both of which return
   `DocumentError::SubscriptionLagged` for an invalid continuation so the caller must reread. The
   projection/overlay primitives moved to a private `documents/collection.rs`.
5. (Resolved, turn 11) `ProvisionKind` removed. It was never read; contract identity is the provision id and version.
6. (Resolved, turn 11) A missing mandatory provision is `MicroRegistryError::MissingProvision(id)`, not `AccessError::Denied`.
7. (Resolved, turn 8) Guest ships the typed `list<mutation>` wire; sledgehammer and its vendored
   patch are gone.
8. (Resolved) `../dioxus` resolves through the shared symlink and WIT deps are fetched with
   `x/wit-fetch.ts` — all dayframe crates build in the arena. See "Arena and build constraints".


## Turn log

- **Turn 1** — Recapped the 3148-entry pi transcript (5 compactions) into the settled rules above;
  located the session's work in the operator's checkout and confirmed the arena base did not contain
  it. No edits.
- **Turn 2** — Evaluated the landed tree, PR #66, and 29 new upstream dioxus commits plus Blitz
  `0.3.0-beta.4`. Raised the multi-app requirement, the configuration source, per-micro crates, the
  wire format and the fork location. No edits.
- **Turn 3** — Wrote this log. Recorded D1–D9 from the operator's answers, the dioxus worktree
  protocol, and the arena build constraints. No code edits.
- **Turn 4** — Corrected D9: no submodules or vendor path; keep `../dioxus`, use a unique workspace
  below the real dioxus checkout's ignored `tmp/` directory, then forget and delete it after commit.
  Recorded the persistent cross-generation target directory. No code edits.
- **Turn 5** — Dioxus edits (operator-authorized; commits held). Created isolated workspace
  `dayframe-gen-aa578c2c49e78545-5-a` on `8a4a0b3a`; confirmed the subsecond `web` patch present.
  Made the remaining D7 change: `native-dom`'s `DioxusState::writer` → `pub … -> impl WriteMutations`
  (dropped the unused `MutationWriter` alias). Verified `cargo check -p dioxus-native-dom
  --all-features` clean. Left uncommitted in working copy `512bf92c`. No townframe code edits.
- **Turn 6** — Operator settled rebase/per-micro-crates/locked-semantics/config-source and confirmed
  the arena symlink works. Rebased the fork chain onto upstream main `7a817390` (`0.8.0-alpha.3`,
  Blitz `0.3.0-beta.4`), resolving Cargo.toml (alpha.1→alpha.3 + `default-features = false`) and
  mutation_writer.rs conflicts; exposure reworked to match upstream's buffered-replay shape
  (`DioxusState::writer -> pub impl WriteMutations` + public `MutationBuffer`). Verified
  `cargo check -p dioxus-native-dom --all-features` clean. Proved the `../dioxus` symlink mechanism:
  cargo rebuilt ~20 dioxus crates at alpha.3/beta.4 through it. Discovered arena dayframe builds are
  blocked by the pre-existing `api_utils_rs` WIT-deps gap (unrelated). Both dioxus patches remain
  uncommitted in working copy `f77ca86f`. No townframe code edits.
- **Turn 7** — Operator authorized the dioxus commit and directed per-crate checks (not the whole
  workspace). **Committed** the dioxus fork: `340719ec` on upstream main `7a817390` (subsecond
  non-browser Wasm + native-dom `DioxusState::writer`/`MutationBuffer` exposure + the alpha.3
  rebase resolutions), bookmark `dayframe-fork-340719ec`; re-verified `cargo check -p
  dioxus-native-dom --all-features` clean (0.8s). Committed townframe `f068aa6` (Cargo.lock alpha.3
  resolution + this log). Freed disk (removed the native-dom debug incremental dir) after a
  disk-full. Verified the build matrix above: `dayframe_guest` checks clean at alpha.3 via the
  symlink; `dayframe_app`/`desktop`/`mobile`/`web` are blocked only by the `api_utils_rs` WIT-deps
  gap. The dioxus workspace (`…/tmp/dayframe-gen-aa578c2c49e78545-5-a`) is kept alive because the
  operator will push the fork later; the `…/aa578c2c49e78545/dioxus` symlink still points at it.
- **Turn 8** — Operator: no sandbox/network restriction; fetch WIT with the repo script. Ran
  `x/wit-fetch.ts` (10 crates), recreated the per-generation `../dioxus` symlink, and got
  `dayframe_app`/`desktop`/`dayframe_guest` checking clean for the first time in the arena. Found and
  fixed one real alpha.3 rebase fallout: `ui/src/arc_index.rs` used removed `Key::ArrowDown` etc.
  (keyboard-types 0.8 → `Key::Named(NamedKey::…)`), commit `fa830b6`. Implemented D4 — the typed
  presentation wire — commit `4c88c77`: `presentation.wit` `list<mutation>` mirroring
  `WriteMutations`; `guest/wire.rs` `WireMutations`; host `replay` into any `WriteMutations`; counter
  guest and tests ported; `dioxus-interpreter-js`/sledgehammer/vendored patch removed; dioxus bumped
  to alpha.3; guest README rewritten. Verified: 3 crates `cargo check --all-targets` clean,
  `nextest -p dayframe_guest` 4/4 pass (guest batches identical to native Dioxus), clippy clean on
  all touched files (remaining warnings are pre-existing `ui` and upstream `dioxus-html`).
- **Turn 9** — Operator constraints recorded as D10 (engine seam vs renderer seam; jco for the
  browser; must not be tied to Blitz, since the non-Blitz dioxus desktop backend also exists). Took
  the highest-risk unknown first: does a typed batch actually drive a real retained tree? It does.
  Restructured the test host into one bindgen with two backends (recording + Blitz) so there is a
  single `Mutation` type and a single `replay`; added `grant_blitz_placement`, structural assertions
  via `query_selector_all_in`/`text_content`/`attr` (string-matching `outer_html` broke on `&gt;`
  inside an attribute value), and two new tests. Found and fixed two real problems: `DioxusState` is
  not `Send` so it cannot live in Wasmtime's resource table (moved to a host side map keyed by rep),
  and dioxus `340719ec` never actually exposed `writer` (my rebase resolution had renamed upstream's
  method; a cached re-check hid it) — fixed in dioxus `c47a76bd8097`. Verified: `nextest -p
  dayframe_guest` 6/6 pass, `clippy -p dayframe_guest -p dayframe_app -p desktop --all-targets
  --all-features` clean (only the 5 pre-existing `ui` warnings), `clippy -p dioxus-native-dom
  --all-features` clean. Excluded and preserved an uncommitted `mutation_buffer.rs` test module in the
  dioxus workspace that is not ours.
- **Turn 10 (Steps 1–4)** —
  - Step 1: Extracted reusable `GuestDom` runtime helper in `dayframe_guest::lib` (owns single guest
    `VirtualDom`, multi-surface `RenderTargetId` tracking, initial `rebuild`, event decoding via
    `Runtime::handle_event` + `render_immediate`), and simplified `examples/counter.rs`. Added real
    DOM hit-resolution (`TestHost::resolve_click` + `BlitzRig::click`) and subtree withdrawal on
    surface drop in `dayframe_guest/tests.rs` (7/7 `dayframe_guest` nextest + clippy clean, `d45e0fc`).
  - Step 2: Added engine-neutral `presentation` module in `dayframe_app` (`replay`, `SurfaceTree`
    implementing `WriteMutations` and rendering into any host Dioxus `VirtualDom` with macro-generated
    event listener bridges over `with_html_event_groups!`) and `NativeComponents::publish_surface` /
    `SurfaceWriter` / `SurfaceSession` in `native.rs` (`d45e0fc`).
  - Steps 3 & 4: Added `MicroProvider::activate_async`, `AsyncMicroFactory`, and
    `MicroRegistry::activate_async` in `micros.rs`. Built `dayframe_app::wasm` (`WasmPackage`,
    `WasmMicro`, `wasm::provider`) with Wasmtime `presentation-client` host bindings, per-incarnation
    `Store` isolation, and automatic `instance.close()` + surface/Settings withdrawal on guest trap
    or disablement (53/53 `dayframe_app` nextest + clippy `-D warnings` clean, `c4ad751`).
  - Steps 5 & 6: Implemented `ConfigurationSource` (`StaticConfiguration`, `MockConfiguration`,
    `FacetConfiguration` with locked boot entry enforcement and `ApplicationRuntime` integration);
    deleted `micros::register_native`; extracted `dayframe_settings_contract`, `dayframe_settings`,
    `dayframe_sample`, and Wasm `dayframe_counter` into per-micro crates under `src/dayframe_dx/micros/`;
    updated `desktop` to link `dayframe_settings`, `dayframe_sample`, and `dayframe_counter` and drive
    generic lifecycle controls via `ConfigurationSource` (65/65 nextest across all 7 dayframe crates +
    clippy `-D warnings` clean).
- **Turn 11** — Wired composition through `MicroRegistry::activate_async` so an `AsyncMicroFactory`
  actually starts from configuration reconciliation, not only from a direct registry call. The
  composition lock is a `tokio::sync::Mutex` so activation can await without holding a `RefCell`
  guard. Removed unread `ProvisionKind`. Missing mandatory provisions report
  `MicroRegistryError::MissingProvision`. Desktop installs its configuration source and lets the
  observer reconcile, so the UI thread does not block on activation. Verified: 59 nextest across
  `dayframe_app`, settings/sample/counter crates, and `desktop`, plus clippy `-D warnings`.
- **Turn 12** — Implemented D5/D6/D11–D13: one guest VDOM with retained multi-mount surfaces;
  a single permissioned document runtime with exact-intent acceptance and observable receipts;
  named WIT grants and incarnation-owned document delivery; lifecycle close/activation ownership;
  neutral presentation and separate Wasmtime crates. Added presentation validation, recycled node
  identities, stale-event suppression, fuel/memory/ingress limits and bounded surface handles.
  Extended composed DOM coverage through rejection, transport uncertainty, remount, persisted
  reactivation and guest-trap isolation. Counter fixtures are explicitly prepared by
  `x/check-dayframe.ts`, not built by runtime code or tests. Generation 15 verified all 13 guest
  tests; subsequent generation-16 edits require fresh verification. Preparation and Deno checks
  passed in generation 16, but the first scoped native run failed compiling the surface-limit test:
  Dioxus and Wasmtime both glob-imported `Resource`. No test/clippy success is claimed for that run.
- **Turn 13** — Resumed the existing generation-16 build rather than starting a duplicate.
  Fixed the ambiguous test import and moved test-only `AccessError` out of production imports.
  Found the unfinished `FacetConfiguration::{set_enablement,rebind}` seam still returning bare IDs;
  both now return `Operation`. Added a regression exercising accepted enablement, transport-uncertain
  rebind rollback, and receipt completion after the configuration authority closes.
  The first rerun exposed a presentation-test defect: it returned one keyed node directly as a
  component root, bypassing the production keyed sibling list. The same replacement assertion passes
  through `SurfaceTree::render`; the test now uses that path and additionally proves text updates
  preserve the host node while clear/recreate replaces it. No renderer implementation change was
  needed. All 131 scoped tests now pass (zero skipped), including the actual Wasm ABI/remount tests,
  surface-handle accounting and new configuration-receipt regression. Nine-package formatting and
  `deno check`, `deno lint`, `deno fmt --check x/check-dayframe.ts` pass.
  Desktop's cold build exposed a stale caller of `ConfigurationSource::current`: the mock-backed
  launcher now extracts `ConfigurationSnapshot::Ready` explicitly. Three desktop composition tests
  then passed, while the composed DOM test exposed two harness defects: synthetic clicks requested
  layout data from text nodes (now resolved through the mapped element ancestor), and the transport
  predicate expected Rust's `outcome_unknown` rather than the generated WIT debug field
  `outcome-unknown`. Instrumentation showed the actual rolled-back value `41` and transport receipt
  with uncertainty `true`; no delivery timeout or uncertainty loss occurred. Temporary logging was
  removed after pinning the assertion defect.
  Final verification in generation 16: **135 tests passed, zero skipped** (131 across the eight core/
  micro packages, then all four desktop tests, including `composed_settings_wasm_dom_flow`).
  `cargo clippy --locked --all-targets --all-features --no-deps` passed without compiler/lint warnings
  for all nine packages; final nine-package `cargo fmt -- --check` passed. Commands ran through
  `nix develop --command`, with `CARGO_BUILD_JOBS=2`, nextest `--locked --no-fail-fast --test-threads 2`,
  and the prepared absolute `DAYFRAME_COUNTER_COMPONENT`. The nine packages were `dayframe_app`,
  `dayframe_presentation`, `dayframe_engine_wasmtime`, `dayframe_guest`, `dayframe_counter`,
  `dayframe_sample`, `dayframe_settings`, `dayframe_settings_contract`, and `desktop`.
  Native window, screenshot and accessibility verification remain outstanding; headless DOM coverage
  is not proof of those capabilities. The path-dependency CLI bootstrap gap, production document
  adapter, browser engine and post-lifting hostile-component isolation gap remain open; no feature-
  foundation readiness claim includes those unsupported capabilities. No commits or fork edits.
- **Turn 14 (generation 17)** — Fixed the native CLI bootstrap to follow either a local
  Dioxus patch or an explicitly pinned Git patch. Local discovery uses `cargo metadata --locked
  --no-deps` to find the workspace's `dioxus-cli` package instead of assuming its directory or choosing
  a registry CLI. Installation keeps `--locked`, `--features no-downloads`, the repository-local
  install root and post-install `--version` validation. Added four inline Deno regressions, including
  a real minimal Cargo workspace with spaced paths and a CLI in a non-assumed directory. All four
  pass through `nix develop --command deno test --allow-all x/install-dx.ts`; Deno typechecking, lint
  and formatting pass for `x/install-dx.ts` and `x/dx.ts` (the latter only received formatting).
  Updated the native dev-loop instructions with the verified installer and explicit counter-fixture
  preparation. No application Rust or fork source was edited.
  The initial `../dioxus` lookup failed because the dependency was absent; it became available during
  this turn without a source change by this agent. Built `dioxus-cli` and prepared `counter.component.wasm`,
  then launched `dx serve --desktop -p desktop --renderer native` in a private tmux session and began
  compiling the `desktop-dev` binary.
- **Turn 15 (generation 18)** — Identified why `./target` was still being created inside the worktree
  and copied into `/nix/store` despite `.env` setting `CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target`:
  1) `x/dx.ts` and `x/install-dx.ts` hardcoded `root.join("target", "dioxus-cli")` when `DIOXUS_CLI_ROOT`
     was unset, ignoring `CARGO_TARGET_DIR`, so `cargo install --root ./target/dioxus-cli` created a
     388 MB `./target` directory inside the worktree.
  2) Running `x/dx.ts`, `x/install-dx.ts`, or `x/check-dayframe.ts` outside `nix develop` (or before
     `.env` was sourced) left `CARGO_TARGET_DIR` unset in `Deno.env`.
  Fixed `x/install-dx.ts`, `x/dx.ts`, and `x/check-dayframe.ts` to read `CARGO_TARGET_DIR` from the
  environment or `.env`, export it into `Deno.env` for child `cargo`/`dx` processes, and default the
  Dioxus CLI install root to `<CARGO_TARGET_DIR>/dioxus-cli` (`/run/media/asdf/p3N/tmp/townframe-target/dioxus-cli`).
  Verified with 5/5 Deno tests (`nix develop --command deno test --allow-all x/install-dx.ts`),
  `deno check`, `deno lint`, `deno fmt --check`, and `test ! -e target` in the worktree.
  Built `accessibility-cli` into `/run/media/asdf/p3N/tmp/townframe-target/debug/accessibility-cli`,
  launched `desktop` natively via `x/dx.ts serve --desktop -p desktop --renderer native --hot-patch false --watch false`,
  verified the Wayland window (`title: "Dayframe"`) on niri, captured a window screenshot confirming
  `Settings` renders both `Counter (Wasm)` (`0`, `Pending counter writes: 0`, `Refresh counter`) and
  `Sample micro` in the same native host tree, and inspected the AT-SPI tree via `a11y.sh`.
  Confirmed and recorded in `.agents/skills/blitz-dioxus-native-limits/SKILL.md` that `blitz-dom 0.3.0-beta.4`
  has an inverted `BaseDocument::has_changes(&self)` (`self.changed_nodes.is_empty()` at
  `blitz-dom-0.3.0-beta.4/src/document.rs:1028`), so `blitz-shell 0.3.0-beta.4` only publishes the
  initial `InitialTreeRequested` snapshot from `dom.rebuild()` (`Heading`, `Button[title="Close Settings"]`,
  `Switch`) and never pushes later `use_future` micro-reconciliation or interaction updates to AT-SPI.
   Stopped the temporary native window after verification.
- **Turn 16 (generation 20)** — Pre-review self-audit of the full change against ADR 014 and the
   settled rules, then re-verification. Removed the dead empty legacy modules left on disk
   (`app/src/backend.rs`, `app/src/backend/mock.rs`, `app/src/data.rs`, `app/src/data/{collection,refresh,transaction}.rs`).
   Replaced the engine's magic numbers with named limits beside `MAX_SURFACE_HANDLES`
   (`WasmStoreState::MAX_DOCUMENT_HANDLES = 1024`, `MAX_PENDING_DELIVERIES = 1024`, wired into the
   work-channel bound and the drive/watch/mutate gates) and gave `Facet::write`'s serialization an
   explicit `expect` message. Read every remaining unaudited file to completion (`native.rs`,
   `application.rs` driver half, `micros.rs` registry half, `micros/composition.rs`,
   `micros/provisions.rs`, `documents/collection.rs`, `presentation` limits/validation surface,
   `guest/lib.rs`, `guest/wire.rs`, all four micro crates, `desktop/src/{main,composition,e2e}.rs`):
   no abstraction drift from the settled design, no TODO/FIXME, error handling matches the
   crash-on-invariant policy, and web/mobile/showcase still depend only on `dioxus`/`ui`.
   Post-rotation recovery was required before verification: `x/wit-fetch.ts` re-fetch (the missing
   `api_utils_rs` WIT bindings broke the workspace build) and `x/check-dayframe.ts --prepare-only`
   to rebuild `counter.component.wasm` from current guest sources. Final state: 131/131 core tests
   and 4/4 desktop tests passed (nextest `--locked --no-fail-fast --test-threads 2`,
   `CARGO_BUILD_JOBS=2`, prepared `DAYFRAME_COUNTER_COMPONENT`); nine-package clippy reported zero
   warnings from the change (the only clippy warnings came from `ui/src/tiles/mod.rs`, byte-identical
   to the base checkout and outside this PR's scope); nine-package `cargo fmt -- --check` clean.
   No commits, no fork edits.



