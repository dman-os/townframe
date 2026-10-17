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

### D5 — One virtual DOM per micro instance, one render target per granted mount

A micro that presents in several places (a Settings detail, a toolbar item, its own window) runs one
virtual DOM and maps each granted mount to a render target. Upstream Core already models this with
`RenderTargetId`/`MultiWriter`. Settled without operator input; revisit only on evidence.

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

1. At the start of a turn that will edit dioxus code, check whether the harness moved us to a new
   townframe worktree. If it did, create a **new** dioxus worktree; never reuse one from an earlier
   turn.
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
- Native-dom exposure after rebase: `DioxusState::writer` is `pub … -> impl WriteMutations + '_`, and
  upstream's `MutationBuffer` is exposed (`pub mod mutation_buffer`, `pub struct`, `pub fn replay`).
  Host-adapter recipe: for a guest subtree create a fresh `DioxusState::create(granted_node_id)`
  (its internal buffer is empty, so the buffered-replay in `writer()` is a no-op) and feed the guest's
  typed mutation stream via `.writer(&mut base_doc)` — or write into a fresh `MutationBuffer` and
  `replay` into `state.writer(doc)`. `StackWriter`/`BlitzBackend`/`RealDom` stay private. Upstream
  alpha.3's own flow is `vdom.render_immediate(&mut state.buffered_mutations)` then
  `state.apply_buffered_mutations(doc)`; we expose the underlying pieces because a guest has no host
  vdom.
- Isolated workspace `dayframe-gen-aa578c2c49e78545-5-a` at
  `/home/asdf/repos/rust/dioxus/tmp/dayframe-gen-aa578c2c49e78545-5-a`, working copy `f77ca86f`.
  The `5-b` contestant has a sibling workspace in the same checkout; never touch it.


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
- Build through the flake with a persistent target shared by generations:
  `CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-dayframe-target nix develop --command cargo …`.
  Do not create a per-generation target directory. Budget for the cold first build, prefer
  `-p dayframe_app`-scoped checks, run long builds in the background, and never run the full
  workspace test suite.
- **Arena dayframe build matrix (turn 6, verified):** `cargo check --manifest-path <crate>/Cargo.toml`
  builds only that crate's dependency closure (never `-p`, which resolves the whole workspace graph).
  `dayframe_guest` builds **clean** at dioxus alpha.3/beta.4 through the `../dioxus` symlink (37s;
  only the pre-existing `sledgehammer_bindgen_macro` dead-code warning). `dayframe_app`, `desktop`,
  `mobile`, `web` all fail — not on dayframe code, but because `daybook_types`
  (`src/daybook_types/Cargo.toml:24`) unconditionally depends on `api_utils_rs`, whose
  `wit_bindgen::generate!` fails: the vendored WIT deps (`wasi:clocks@0.2.6`, `wasmcloud:postgres`,
  …) are not present in the gitignored `wit/deps/` (fetch via `wkg`, which needs network the arena
  blocks). This is **pre-existing and unrelated to dayframe/dioxus**. `dayframe_guest` escapes it
  only because it has no `daybook_types` dependency. To make app/desktop/etc. checkable here the
  operator must populate `wit/deps` (or drop `daybook_types`'s hard dep on `api_utils_rs`). Dioxus
  itself is verified independently in its own workspace.
- Disk: the worktree filesystem has ~71G free; the root filesystem (`/home`, `/tmp`) has ~6G. Keep
  dioxus worktrees and build artifacts on the worktree filesystem.
- The arena allows bash writes to `/home/asdf/repos/rust/dioxus`, but the file tools are restricted to
  the worktree, `TMPDIR`, attachments and skill dirs.

## Known defects in the current tree

To be fixed as the slices land, not all at once.

1. `micros::register_native` registers every micro from the app crate (D1 deletes it).
2. `desktop/src/composition.rs` still knows Sample: it seeds Sample's config document, imports
   `SampleConfig`, and hosts the enable/disable plus mock-node controls (D1/D2 remove this).
3. Activation is synchronous in `MicroProvider::activate` although day one agreed on async activation
   and Wasm instantiation needs it. `Host::activate_async` exists but the registry never uses it.
4. Two parallel data stacks: the older `backend.rs`/`data/` from ADR 014's accepted half, and the
   session's `documents.rs`. They should collapse into one node seam using the D6 vocabulary.
5. `ProvisionKind` is speculative — only `Context` is ever constructed.
6. A missing mandatory provision is reported as `AccessError::Denied`, which tells the operator
   nothing.
7. The guest still ships the sledgehammer binary stream (D4 replaces it).
8. (Resolved) `../dioxus` resolves through the shared symlink — see "Settled operator answers". The
   remaining arena build blocker is the `api_utils_rs` WIT-deps issue described in "Arena and build
   constraints", which is pre-existing and unrelated to dayframe.


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
