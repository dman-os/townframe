# Clay / Dayframe architecture critique — tracking doc


> [!IMPORTANT]
> ADRs 017, 023, and 024 supersede this document where they conflict. In particular, the current collection design uses projection leases with transactional inline row pages and `bootstrap_complete`, not listing/hydration, `Subscription`, or `ReplayComplete`; ADR 024 supersedes the open lifecycle/configuration questions below. This file remains an audit trail and workstream index, not an implementation specification. The presentation targets are Dioxus Native and the browser DOM; React Native is not a target. A direct DOM backend is a later measured optimization, not a prerequisite for the collection/lifecycle work.
Status: findings reviewed with the operator; W2 (sync/async boundary) landed as code + ADR 015 R5 /
ADR 018 §3 rewrites, with its four follow-up gaps closed (§3.1); W1 (collection waist) redesigned in
ADR 017, code not built; §5 status corrections applied. Remaining workstreams not yet redesigned.
Owner: operator + dayframe track
Updated: 2026-10-31
Scope: ADRs 014, 015, 017, 018 and the `clay_*`, `micro_*`, `dayframe_*` crates. Findings are a
read-only audit verified against the source; W2 (§3.1/§3.2) landed as code + ADR rewrites
2026-10-30. Every finding below has a file reference. The §4 workstreams are the agreed next
steps; each one should become its own ADR amendment or ADR before implementation.

Related: `docs/scratch/dayframe-micros.md` (original design log), `docs/scratch/dayframe-crate-map.md`,
`docs/scratch/dayframe-app-model.md`, `src/big_cabinet_core/doc.rs` (the node doc service as landed).

## 0. One-paragraph verdict

The implementation is substantial plugin-runtime machinery documented as a finished application
architecture. The accepted ADRs claim guarantees — demand-scoped delivery, one collection waist,
portable micro contracts, inspectable failure states, a no-suspension synchronous presentation
boundary — that the integrated system does not provide. The strongest parts (explicit lifecycle
states, segregated optimistic overlays, engine-neutral session code) are real; they have been
generalized and labelled "Accepted/Implemented" well beyond what runs.

## 1. Findings

Severity: **S1** breaks a stated guarantee or is a correctness/safety hole; **S2** is a design
defect that blocks the stated goal; **S3** is scaffolding/limitation that is honestly deferred but
mis-labelled or under-controlled.

### F1 (S1) Demand does not control delivery; everything is eager broadcast — addressed by the ADR 017 rewrite

- `View::attach` registers the relay immediately and every later batch is forwarded to every relay
  regardless of demand: `src/clay_collection/view.rs:883-896`, `:734-739`.
- `release` only clears bookkeeping; it never detaches the relay or stops delivery:
  `src/clay_collection/view.rs:500-513`.
- An authority ignores the `DemandToken` and bootstraps its whole replica:
  `src/clay_collection/view.rs:468-493`, `:515-538`.
- Host collections send a full snapshot on subscribe and treat `release` as a no-op:
  `src/clay_host/collection.rs:180-205`, `:485-503`.
- `View::query` clones the entire visible map, then filters/sorts/limits:
  `src/clay_collection/view.rs:232-242`, `:402-412`.
- Consumer hops forward every demand upstream even when locally answerable:
  `src/clay_collection/view.rs:482-495`.

Consequence: "demand" today means "ask for a coverage receipt", not "establish selective upstream
delivery/retention". The old ADR 017 §5 ("demand is a narrowing token") and §1 ("each hop answers
from its own replica and forwards what it cannot cover") are not implemented.

**Addressed 2026-10-31:** ADR 017 was rewritten. The relay waist (`attach`/`demand`/`release`,
eager `Batch` broadcast, `DemandToken`, `Coverage`) is withdrawn; the replacement is a reader
resource (`open` → subscription, bounded pages, `ReplayComplete`, hydration by reference) plus a
separate writer and a shared overlay plane. The finding remains a true description of the code
until that waist is built; it is no longer the decided design.

Operator: confirmed serious; the scan "seems very bad". See §2.1 for how the node side now shapes
what a mounted doc query must resolve to.

### F2 (S2) "One collection waist" is shared containers, not one protocol — addressed by the ADR 017 rewrite

Shared: `Replica`, `Ledger`. Not shared — three live data paths:

- Documents: bespoke `watch<DocumentSnapshot>` per selection, `WatchSelection`, `Freshness`,
  refresh dedup, retention, receipts: `src/clay_docs/lib.rs:81-88`, `:196-230`.
- Contribution collections: `CollectionWriter`/`Entry` protocol bypassing `View::write`:
  `src/clay_host/collection.rs:236-325`.
- Generic views: `Batch`/`DemandToken`/`Coverage`/`OperationId`.
- WIT document transport is a fourth bespoke protocol (`clay_guest_session/wit`).
- Collection WIT transport (needed for guest→guest collections) is explicitly unbuilt:
  `docs/adrs/017-clay-collection-substrate.md:241-248`.

ADR 017 §1 "no second mechanism exists for guest-provided collections" is false as built.

**Addressed 2026-10-31:** ADR 017 now defines one reader interface (subscriptions over a selector)
for documents, contribution collections, host stores, and computed guest collections, with the
writer and shared overlay plane as separate capabilities. The WIT resources are specified; the
in-process code still exposes the old shapes until the waist is implemented.

Operator: the waist must be designed properly, expressed in WIT and in Rust, and must be able to
carry docs, cross-micro collections, and host-side collection services uniformly.

### F3 (S1) The lifecycle failure model is defeated by the shells — fixed 2026-10-30

- `CompositionRuntime::apply_states` asserts no instance is `Failed`:
  `src/dayframe_composition/lib.rs:120-129`.
- Web shell `expect`s on fetch and on compile/instantiate:
  `src/dayframe_web/shell.rs:41-48`.
- Contradicts ADR 014 amendment ("activation failures are retained … unrelated edits do not
  repeatedly invoke a failing provider", `docs/adrs/014-dayframe-dx-application-architecture.md:518-520`).

The reconciler has a `Failed` state; the shipped composition refuses to tolerate it.

**Fixed:** `apply_states` no longer asserts; `Failed`/`Faulted` are rendered in the status row.
Package factories for `clay_mdk::{wasm,wcmp}::provider` return `Result<Package, String>`, so a
web fetch/HTTP/compile failure becomes `ActivationError::Initialization` → `Failed` on that
instance (the web shell's `expect`s are gone). `InstanceState::Stopped` is renamed `Faulted`
with the model's meaning: ended while still desired, retained, retry = configuration revision;
there is no intentional stop other than disablement. Pinned by
`dayframe_composition::tests::unfetchable_guest_fails_its_own_instance_and_leaves_the_rest_running`.

### F4 (S1) The "no suspension point" invariant behind `first_poll` is false — fixed 2026-10-30 (W2)

- ADR 018 §3 (as originally written) justified synchronous presentation calls by "our WIT has no
  suspension point": `docs/adrs/018-browser-guest-engine.md`.
- The host links async imports, `wasi:clocks/monotonic-clock`'s `wait-for` and `wait-until`
  (`func_wrap_concurrent`; the world imports the whole interface, so both must be registered —
  a guest using `std::thread::sleep` pulls in `wait-until`): `src/clay_rt_wcmp/wasi/clocks.rs`.
- Both engines **used to** panic the host if a guest call returned `Pending`.

A guest that awaits `wait-for` inside `mount`/`dispatch` suspends the export; the old code then
panicked the host. Synchronous *exports* do not imply non-suspending *execution* when async
imports are linked.

**Fixed (W2, 2026-10-30).** `first_poll` in both engines returns `Result<Output, Suspended>`;
`Suspended` and an inner `Err` (including the spec's `CannotBlock`) both take the trap path —
withdraw surface, close incarnation — never a host panic. Pinned by
`clay_rt_wasmtime::tests::guest_blocking_in_dispatch_fails_as_trap_not_panic_or_pending` (sleeper
fixture) and `first_poll_maps_pending_to_suspended_and_ready_to_output`. ADR 015 R5 and ADR 018 §3
rewritten; document deliveries use the same rule.

Operator: does not understand why sync-first was chosen. WASI async (component-model async) and
Dioxus's async model are separate things from "the guest uses async Rust"; a sync WIT function can
be exposed through an async bindgen helper. Revised direction after §3.1/§3.2 investigation:
**presentation exports stay sync-typed+sync lift** (the DOM `preventDefault` requirement, matching
Dioxus); everything that may wait — data, collections, queries — is **async-typed**, serialized by
the component model's entry gate. "Async by default" applies to the data boundary, not the
presentation boundary.

### F5 (S1) The composition root still ships demo controls and lacks per-instance configuration

- `dayframe_composition` no longer imports concrete micros: launchers pass an
  `InstalledMicro` catalog (manifest + provider closure) plus locked instances to
  `CompositionRuntime::new`, which binds one mock config document per manifest config
  grant, registers every provider, and seeds one enabled on-demand instance per
  installed micro the locked set does not pin. Desktop scans a package directory
  (`DAYFRAME_PACKAGE_DIR`, else `$XDG_DATA_HOME/dayframe/packages`); web reads
  `packages/index.json`. Package identity is the SHA-256 of the exact component bytes
  and the manifest is read statically from the `dayframe.manifest` custom section
  (`clay_mdk::package`), never by running the guest. Unreadable/manifest-less entries
  land in `installation_errors` in the lifecycle panel, never fatal.
- Mock lifecycle and settle buttons are in the product shell: `src/dayframe_composition/lib.rs:215-274`.
- The remaining violation is configuration ownership, not the micro list: the shared crate's
  concrete-micro dependencies are dev-only, and launchers linking native built-ins is what
  ADR 014's amendment permits (`docs/adrs/014-…:583-585`).
- `FeatureRegistry`/`FeatureContext` exist only in `src/clay_micro_core/flags.rs` and its tests;
  nothing gates on them.
- `ConfiguredMicro` now includes `eager` (default false); enabled instances only activate when
  eager or reachable through demand. Settings owns its hidden/order preferences in a facet.
  This supplies document-backed startup/selection, not persistent storage. Package
  discovery is now catalog-driven (above); OCI transport remains future work.
- No per-instance document configuration: `grant_config` resolves a *global* named config owner
  (`src/clay_docs/lib.rs:264-305`), so two configured instances of one provider share config.
- `dayframe_mobile` is a themed `h1`: `src/dayframe_mobile/main.rs:16-25`.
- There is no `dayframe_app` crate; ADR 014 §1's shared application runtime (routes, shell,
  backend contract, feature modules) does not exist.

Operator: "why the heck is the composition crate importing the micro crates" — design to fix now.
Does not care about the `FeatureRegistry` name/design; what is needed is a real model of how
micros are configured (datums/docs in the real app, persistence abstracted) and how that drives
boot-time composition. Micro lifecycle to be designed properly, taking cues from Android/iOS and
other application platforms. Mobile is WIP. `dayframe_app` to be reintroduced for shared
web/desktop/mobile code.

### F6 (S2) The "portable" Settings contract contains native host authority

- Claim: "no host or composition crate types cross this boundary": `src/micro_settings_manifest/lib.rs:7-9`.
- Imports `clay_mdk::native::{AccessError, Collection, CollectionWriter, Contributable, Entry, Instance}`:
  `src/micro_settings_manifest/lib.rs:15-20`; native provision impl `:41-95`.
- Native micro views depend directly on Dioxus `Element` (ADR 015 §5 acknowledges "not yet one
  source").
- `clay_mdk::native` vs `clay_mdk::wasm`/`wcmp` authoring APIs differ substantially; "same MDK
  API" is mostly shape/branding.

A guest cannot implement or consume the typed `SettingsSections` provision. This crate boundary
actively prevents portability rather than leaving it unfinished.

Operator: micros are either platform-specific+native (compiled into the app, may import platform
crates as an escape hatch) or portable (native *or* wasm, isomorphic). Most micro support code must
be isomorphic; platform-specific micros do their own imports. **Settings must be portable.**

### F7 (S2) Browser package delivery: no cache, no artifact identity, redundant hops

- Every re-activation refetches and recompiles the package; web still has no compiled-package
  cache. Artifact identity now exists: `x/dayframe-web.ts` writes `packages/index.json`
  (location, SHA-256 digest, statically-read manifest), the shell fetches a package only on
  activation, and verifies digest + manifest before `wcmp` compiles it — so disable/enable
  can no longer run different bytes under one configured name without an activation failure.
  provider/version.
- `clay_mdk::wasm` and `clay_mdk::wcmp` duplicate provider/controller logic
  (`src/clay_mdk/wasm.rs`, `src/clay_mdk/wcmp.rs`); `clay_rt_wasmtime` and `clay_rt_wcmp`
  duplicate activation/mount/dispatch/unmount and mutation conversion.
- Browser engine has no fuel and no established memory cap; a runaway guest hangs the tab
  (`docs/adrs/018-…:106-117`, accepted limitation).
- Browser evidence is one Chromium counter happy path: `src/dayframe_web/e2e/counter.spec.ts`.

Operator question (open): on the web the call path is host-wasm (dioxus-web via wasm-bindgen) → JS
glue → guest wasm → JS glue → host-wasm for every import. Should some presentation helper code be
JS, or should more of clay's browser runtime live in wasm to avoid the extra hop? Are we leaning
directly on Dioxus here? **Needs measurement before deciding** — see §3 Q7.

### F10 (S1) The native wasm runtime has no real package layer — shared engine/cache built, instance layer open

- `WasmPackage::from_bytes` builds a fresh `Config` + `Engine` **per package** and re-registers
  the whole WASI p3 + `MicroClient` surface on a fresh `Linker` per package:
  `src/clay_rt_wasmtime/lib.rs:346-364`. Wasmtime's model is one `Engine` per process. Measured
  (§3.3): an extra Engine + Linker costs ~40 µs and no measurable RSS, so the defect is
  structural, not raw cost. With one engine per package there is nowhere to put process-wide
  policy: the memory reservation (4 GiB of address space per instance today, §3.3), the pooling
  allocator, epoch interruption, and the compilation cache. Stores and components from different
  packages also can't interoperate.
- The two engines disagree: `WcmpPackage::load(engine, bytes)` takes a shared engine
  (`src/clay_rt_wcmp/lib.rs:278`); the wasmtime adapter does not.
- The only loading path is `from_env` (`DAYFRAME_<NAME>_COMPONENT`), and the "package cache" in
  `clay_mdk::wasm::component_package` is a static map keyed by that `&'static str` name. It is a
  test fixture shipped as the runtime's package layer: no content identity, no precompiled
  (`Component::serialize`/`deserialize`) cache, no `InstancePre`.

Operator: look at **`daybook_core::rt`** (`src/daybook_core/rt.rs`, `rt/wash_plugin.rs`) and
**`wash-runtime`** (`crates/wash-runtime/src/engine/` at the pinned rev — one shared `Engine`
built via `EngineBuilder`, pooling allocator with read-back limits, per-workload `InstancePre`,
warm instance pools, trap remediation) for how composition on top of wasmtime should work; the
previous passes were pointed there and did not adopt it. Fix shape: one runtime-owned engine
passed in (matching wcmp), one linker per engine, per-package `Component` + `InstancePre`,
packages keyed by content digest with a precompiled-artifact cache. Folds into W6.

**Addressed 2026-10-30 (`b67d5e68`+):** `clay_rt_wasmtime::engine()` is one process-wide engine
(one `Config`, one compatibility hash) and `linker()` one process-wide linker; `WasmPackage` holds
only the compiled component plus clones of both. Linear-memory reservation is set to the 64 MiB
store limit with a 64 KiB guard, so per-instance address space is about 64 MiB rather than 4 GiB.
`compile_component` caches precompiled artifacts under `$DAYFRAME_COMPONENT_CACHE` (default
`$XDG_CACHE_HOME/dayframe/components`), keyed by blake3(wasm bytes + `precompile_compatibility_hash`),
written via temp-file + rename and repaired by recompiling when loading fails. Tests pin shared
engine/linker identity and that a warm load does not rewrite the artifact. **Open:** content
identity for packages (the `component_package` name cache is still the only lookup), `InstancePre`,
pooling allocator, and the same cache on wcmp (F7).

### F8 (S2) Cross-hop lifecycle and identity are incomplete

- Consumer `dispose_scope` withdraws only local overlays; there is no upstream scope-disposal
  operation on `Upstream`: `src/clay_collection/view.rs:578-603`, `:42-51`.
- `OperationId`/`DemandId` are process-wide counters; cross-address-space namespacing unresolved:
  `src/clay_collection/view.rs:29-33`, `docs/adrs/017-…:245-248`.
- Ordering of writes across address spaces is unspecified.

The chained optimistic-write protocol is not ready to cross the boundary that motivated it.

### F9 (S3) Browser guests are trusted-code-only today

No instruction bound, no verified memory ceiling (ADR 018 §5 states this honestly). "Fetched
micro" currently means trusted delivery, not third-party extensibility. Operator: controls
expected soon; tracked, not blocking the redesign.

## 1b. Why the implementation went this way (root causes, from the design log)

Nothing external forced these outcomes. Each was a locally reasonable increment whose premise was
never re-checked. Evidence is in `docs/scratch/dayframe-micros.md` (DM) and
`docs/scratch/dayframe-crate-map.md` (CM).

| Finding | Proximate cause | What should have caught it |
|---|---|---|
| F4 `first_poll` | "Sync presentation" was a ground rule from the start (DM:56, ADR 015 R5), justified only by `preventDefault`. ADR 015 §1 deferred the alternatives "until a second real engine forces the question". The second engine arrived (ADR 018) and the question was not reopened; `first_poll` encoded the rule. A day later guests moved to `wasm32-wasip3` (CM:740-754), whose WASI imports are async — this silently invalidated "our WIT has no suspension point" (`src/clay_rt_wcmp/lib.rs:118-120`), and nobody re-read ADR 018 §3. | A deferral clause with a trigger that nobody checks is not a decision. A test of a guest that sleeps inside `dispatch`. |
| F5 composition names micros | DM D1 is legitimate: the *end app* hard-codes linked micros and supplies startup config. ADR 018 §7 then moved desktop's "engine-agnostic" half — including `linked_providers` and the config maps — into a shared *library* so parallel micro PRs would not edit two copies (CM:583-588). Launcher-only linkage became library-level. DM D2's datum-backed config source was never built, so `MockConfiguration` was the only implementation and got shipped as the product's. CM §3 item 9 already flagged "the composition root names micro symbols"; the fix only removed contribution types (CM:556-558), not providers or config. | No `dayframe_app` to own config; a dependency rule in `guardrails.toml` forbidding `micro_*` deps outside launcher bins. |
| F1 demand ≠ delivery | The substrate was built test-first from overlay semantics (DM turn 31, 18 tests) with no consumer that needed narrowing: the only live collections are tiny settings-section lists where broadcast is harmless. Filters are closures, so no token can be derived from a query (DM:1067-1070) and demand became a separate opaque JSON side-channel that `HostUpstream` covers unconditionally. Demand-on-mount was deferred "until a UI renders pending state" (DM:1071-1074); the node adapter was deferred. The design log states "host streams deliberately overfetch" (DM:1207) but ADR 017 kept the narrowing language as decided-and-implemented. | Building against the node doc service (now `big_cabinet_core::doc`). A test that `release` stops delivery and that an unrelated row is not sent. |
| F2 not one waist | Docs predate the substrate; slice 3 swapped their containers onto `Replica`/`Ledger` (DM:1017-1020) but not their protocol, because a protocol migration needed collection WIT, which was deferred. The old ADR 017 §9 ("documents are one bespoke layering") contradicted its own §1; the 2026-10-31 rewrite makes documents one reader source. | Reading the ADR for internal consistency. |
| F3 failure panics | The `assert!` comment says "do not conceal one behind an empty panel" — the AGENTS.md crash-on-invariant rule applied to something that is *not* an invariant (activation of a fetched/configured micro is external input). No UI existed for `Failed` and adding one needed the design-language skill, so a panic was the path of least resistance. | The ADR 014 amendment requirement, turned into a test that a failing provider renders a failed slot. |
| F6 native types in manifest | Operator asked for the `micro_xxx_manifest` pattern; the crate was renamed but its contents were not moved (DM:497 records it as "partial"). Typed provisions only exist as the native `Contributable` trait, so there was nowhere portable to put them. | A `check-matrix` contract that manifest crates build for `wasm32-wasip3` with no `clay_mdk::native`. |
| F7 web fetch/recompile | The browser shell was promoted from the deleted `clay_wcmp_spike` (CM:758-770) with one guest; cache and identity never had a second consumer to motivate them. | — |

Meta-causes, which matter more than any single row:

1. **ADRs were written "as built" after the code** ("ADRs locked in before merge", CM:710). They
   mix intended semantics, as-built inventory, and open items under one "Accepted. Implemented"
   status, so a gap reads like a feature.
2. **Bottom-up substrate without a real consumer.** DM's own ground rule "everything must be
   buildable from real consumers, top-down" (DM:65) was broken by the collection work.
3. **Turn gating on green tests.** Every increment closes on "N/N nextest + clippy clean", which
   rewards local consistency. Tests pin mechanisms (overlay ordering) but not claims (release
   stops delivery; a sleeping guest does not panic the host; a failed micro renders).
4. **Deferral clauses with triggers** ("until a second engine forces the question") that nothing
   checks when the trigger fires.

## 1c. ADR hygiene (applies to every ADR rewrite below)

An ADR keeps: context, requirements, decisions, rationale, rejected alternatives, and each
invariant **with the test that pins it**. Per-section status: *decided* / *built* / *pinned*.

An ADR drops (move to scratch or delete): crate-by-crate as-built inventories, migration step
lists, dependency pins and fork hashes (018 §6), CI lane lists (018 §9), verification counts,
FIXME disposition tables (015 §11), crate renaming history. These go stale and drown the
decisions.

Deferrals must name a trigger *and* the check that fires on it (a test, a guardrail, or a grep in
a CI lane), or they are written as open questions.

## 2. Constraints that are now known (and were not when the ADRs were written)

### 2.1 The node doc service has a shape: `src/big_cabinet_core/doc.rs`

Not saying the collection must mirror it, but a micro mounting a doc query now resolves on the
node to a known surface. Relevant properties the waist must be able to carry or adapt to:

- **Capability-scoped**: every op carries a `CapId`; scope is `DocScope { read, write, create, drawers }`
  (`doc.rs:201-216`). Authority is a handle minted by the service, matching ADR 015 R2.
- **Reads are selective by construction**: `ReadDatums { doc_id, at, datums: Option<BTreeSet<DatumKey>>, optimistic }`
  (`doc.rs:261-271`); `QueryDatums { drawer_id, selector: All | Doc | Tag | Route | Key }`
  (`doc.rs:280-296`) returns `(doc_id, datum_key, doc_heads, version)` *listings*, not values.
- **Observation is a cursored stream**: `ObserveDatums { scope: Doc | Tag | Drawer{tag?}, after: u64 }`
  → `DatumObserved::Entry { revision, doc_id, datum_key, datum: Present | Removed | AccessLost } | ReplayComplete { through }`
  (`doc.rs:338-399`). Again versions/presence only, not values — values are fetched by
  `ReadDatums`. This is a **listing + targeted hydrate** model, the opposite of F1's eager
  full-replica broadcast.
- **Versions**: `DatumVersion { heads: Vec<ChangeHash>, clock: u64 }` (`doc.rs:91-96`). ADR 017 §3
  says causal positions never enter `Version`; the node now *does* expose heads alongside a clock.
  The waist's `Version` design must decide whether `clock` is the ordering and heads are opaque
  payload, or something else.
- **Write conditions and idempotency**: `WriteMode::{IfDocUnchanged, IfDatumsUnchanged, AtHeads, AtCurrent}`
  and `CommandId` (1..=256 bytes, reuse/in-flight errors) (`doc.rs:37-89`, `:165-170`). Optimistic
  writes through the waist must be able to carry a condition and a command id, and surface
  `Conflict { heads, datums }` as a rejection class.
- **Read verdicts**: `DatumRead::{Accepted, Absent, Pending, Rejected, Undecodable}` (`doc.rs:98-120`).
  `FacetValue::{Loading, Absent, Present}` is too narrow; the row payload needs to carry the
  verdict, or the waist needs a verdict-aware row type.
- `ReplayComplete` is explicitly **not** global source completeness (ADR 017 §3) — a replay-boundary
  definition for the node adapter is still owed.

### 2.2 Other fixed points

- Browser host is itself wasm (Shape W); guests are same-realm components via `wcmp`.
- Guests build on `wasm32-wasip3`; WASI p3 imports are component-model async.
- Dioxus fork pinned for native components; `dioxus-web` renders guest surfaces via `rsx`.
- Micro configuration will be datum/doc-backed in the real app (persistence abstractable).

## 3. Open design questions (to be answered in the ADR work, not here)

- **Q1 Waist shape.** Answered by the ADR 017 rewrite (2026-10-31): one reader interface —
  `open(selector) → Subscription` with bounded pages and `ReplayComplete`, listing plus targeted
  `hydrate`, a separate writer with source-defined commands and outcomes, and a shared optimistic
  overlay plane. It serves node docs, host contribution collections, guest-provided computed
  collections, and host-side collection services; Rust and WIT are two renderings of one spec.
- **Q2 Demand semantics.** Answered in ADR 017 §2: dropping a subscription stops delivery and
  cancels a pending `next`; retention is the mirror's policy, so a subscription releases only its
  own retention claim and never evicts rows another live subscription retains.
- **Q3 Listing vs values.** Answered in ADR 017 §2: the waist adopts the node's listing+hydrate
  split — a page carries row presence and stamp, and bodies are fetched by `RowRef` without
  substituting a newer value.
- **Q4 Async boundary.** Answered in §3.1 (2026-10-30 investigation) and §3.2 (Component Model
  alignment); the reduced wasmtime spike is **done** — see §3.2 "Reduced spike — done". Remaining
  open: whether the pinned `wcmp` fork's *web* backend can resume a guest export that suspends on
  a host import (its tests cover host-function suspension, not this exact shape); not blocking the
  ADR rewrite, which landed with the spike.
- **Q5 Micro configuration model.** Datum shape for a configured instance: stable identity,
  provider id+version/artifact digest, enablement, bindings, *activation conditions / demand
  triggers*, per-instance config document binding. How boot composition projects from it.
- **Q6 Micro lifecycle.** States and transitions borrowed from Android (`Activity`/`Service`
  lifecycles, process death and state restoration, bound vs started services, WorkManager for
  durable work) and iOS (scene lifecycle, background modes, state restoration): created /
  started / resumed / paused / stopped / destroyed, demand-driven activation, memory pressure,
  restoration after host restart. Which of these apply to a micro vs a mount vs an integration.
- **Q7 Browser hop cost.** Measure host-wasm→JS→guest-wasm round trips for `mount`/`dispatch`
  before deciding whether presentation helpers move to JS or more clay code moves into one wasm
  module. Also decide how much of the browser renderer path depends on Dioxus specifics.
- **Q8 Package identity.** Content-addressed artifact ids in configuration; compiled-package cache
  on both engines; what "same version" means for a fetched micro.
- **Q9 Portability tiers.** Exact crate/feature layout for isomorphic micro support code vs
  platform-specific escape hatches; what a portable manifest crate may import (answer today: only
  wire types).

## 3.1 W2 investigation: sync DOM, async data (2026-10-30)

The instinct (keep the DOM boundary synchronous, do our own async behind it) is what Dioxus
itself does, and it is implementable here. The earlier "async by default" direction is withdrawn
for presentation calls; it stands for everything else.

### What Dioxus does

Dioxus has exactly one synchronous requirement, and it solves it by *bounding* the sync work, not
by forbidding async.

- **Web** (`packages/web/src/dom.rs:68-97`): the JS event listener is a synchronous closure. It
  calls `runtime.handle_event(...)`, which runs listeners inline, then reads the flag the handler
  set and calls `web_sys_event.prevent_default()` before returning. The browser applies the
  default action the moment the listener returns, so the flag must be set before that return.
- **Core** (`packages/core/src/events.rs:340-363`, `SpawnIfAsync`): an event handler may return an
  async block. It is polled exactly once (`now_or_never`); if it is still pending it is spawned
  and runs later. The comment says why: "Quick poll once to deal with things like prevent_default
  in the same tick." So `prevent_default()` called before the first `.await` takes effect, and one
  called after the first `.await` does not. Async is allowed; only the *decision* is synchronous.
- **Desktop** (`packages/interpreter/src/js/native.js`, `handleEvent`): the webview's JS posts the
  event with a synchronous `XMLHttpRequest` (`xhr.open(endpoint, false)`) and applies the returned
  `preventDefault`/`stopPropagation` before the JS handler returns. Same shape, different boundary.

Our renderer already copies the web shape (`src/clay_surface/render.rs:183-201`): the listener
closure calls `dispatch(event)`, gets `EventResponse`, and calls `evt.prevent_default()` before
returning. The guest side (`src/clay_mdk/guest/dom.rs:153-186`) runs the guest's own Dioxus
runtime synchronously and reports back whether its handler set the flag. Nothing about this needs
to change.

### The four boundaries, and which one is actually broken

1. **Browser → host (web).** Dioxus-web's JS listener runs our Rust directly. Synchronous by
   construction; no choice to make. (Desktop's webview adds the sync-XHR hop; that is Dioxus's
   problem, not ours.)
2. **Host renderer → micro.** `render.rs`'s listener closure calls the micro and must return the
   `prevent_default` decision before the closure returns. This is the one sync requirement.
3. **Micro → guest component.** `presentation-events.dispatch` is a sync WIT function
   (`src/clay_guest_session/wit/presentation.wit:72`). The engines call it through `first_poll`
   (`src/clay_rt_wcmp/lib.rs:483`, `src/clay_rt_wasmtime/lib.rs:488`).
4. **Guest → host imports.** WASI `monotonic-clock`'s `wait-for` and `wait-until` are `async func`
   imports linked on both engines (`src/clay_rt_wcmp/wasi/clocks.rs`, `src/clay_rt_wasmtime/lib.rs`).

The bug is the interaction of 3 and 4: a guest that awaits an async import during `dispatch`
suspends the export, `first_poll` sees `Pending`, and panics the host. Boundary 2 never required
that boundary 3 be unable to suspend — it only requires that the *decision* come back before the
listener closure returns.

### Proposed answers

**A1. Presentation calls stay synchronous functions; suspension becomes a guest failure, not a
host panic.** `mount`/`dispatch`/`unmount` remain sync WIT functions, and the host still drives
them with one poll. The change is the failure mode: a `Pending` result is a guest contract
violation — the guest suspended inside a call that promises a synchronous decision — and is
handled exactly like a trap: withdraw the surface, close the instance, report
`PresentationError::Revoked` / `EventResponse::default()`. Before W2 landed the code panicked
instead (`src/clay_rt_wcmp/lib.rs:120-127`, `src/clay_rt_wasmtime/lib.rs:52-60` at the time). This
matches the existing rule "errors are values, traps isolate the incarnation" (ADR 015 context).
*Landed* — see F4 and §3.1 "Follow-up gaps closed".

**A2. Guests may use async internally, with the same rule Dioxus gives its own handlers.** A
guest's event handler may spawn work; only the code before its first `.await` contributes to
`prevent_default`/`stop_propagation`. The MDK should do what `SpawnIfAsync` does: poll the
handler's future once, and spawn the remainder inside the guest's own runtime. That is guest-side
async and needs no WIT change. *Status: decided, not built* — recorded in ADR 015 R5 as such. The
MDK has no `SpawnIfAsync` mirror, and the two tests that would pin the rule (a handler setting the
flag before its first await is honoured; one setting it after is not) do not exist. Building them is
guest-side work independent of the host changes W2 landed; until then the rule is a documented
contract with no enforcement.

**A3. No concurrent calls into one instance.** `wcmp`'s `TypedFunc::call` refuses a second driver
while one is in a turn (`RecursiveDriver`, `rust/wcmp/src/instance/func.rs:261-266`), and our
`dispatch` holds the incarnation's `RefCell`/`Mutex` guard across the call, so a re-entrant call
would panic on the borrow anyway. Events are delivered one at a time per incarnation, in order.
A handler that needs concurrency spawns inside the guest (A2); it does not overlap calls. This
also answers the "event storms" worry: `pointermove`-class events are the guest's to coalesce
inside its own runtime, exactly as a normal Dioxus app does.

**A4. Async imports are not callable during presentation calls, and that must be a link-time
fact rather than a runtime failure.** Today the WASI imports are linked into the same instance that
serves presentation calls, so the contract is only enforceable by A1's trap-equivalent handling (a
guest that blocks loses its surface). The durable fix is to give the presentation world no async
imports: clocks, if a guest needs them, come through an async world the guest calls outside
presentation calls, or not at all. Until that split exists, A1 is the safety net and the MDK must
not expose an async host API that is reachable from a handler's synchronous prefix.
*Deferral is checked, not just declared* (per §1c): the sleeping guest is pinned to fail closed on
both engines (`clay_rt_wasmtime::tests::guest_blocking_in_dispatch_fails_as_trap_not_panic_or_pending`,
`clay_rt_wcmp_wasmi::tests::guest_blocking_in_dispatch_fails_as_trap_on_the_wcmp_engine`), and
`clay_rt_wcmp::wasi::tests::async_linked_wasi_imports_are_enumerated_and_pinned` enumerates the async-linked
WASI imports so widening the exception list fails a test and forces the split. Wasmtime has no
enumeration equivalent — its WASI surface arrives wholesale from
`wasmtime_wasi::p3::add_to_linker`, not from our source — so the sleeper fixture is that engine's
only net.

**A5. Native micros are unaffected.** They are called as plain Rust functions inside the listener
closure, so they are synchronous by construction and can set `prevent_default` directly. The async
rule is a property of the WIT boundary, not of the micro trait.

**A6. What is shown while work is pending does not change.** `mount` returns a surface only when
the first render is committed, so there is no "mount pending" state to render. Data that is not
yet loaded is `FacetValue::Loading`, which is already async and separate (ADR 015 R5's second
half, which was right and stays).

### What this withdraws

The earlier proposal to make `mount`/`dispatch`/`unmount` async WIT functions is rejected. It
would move the sync requirement from the WIT to a side channel (a prevent-default state the host
reads synchronously), which is more machinery for the same result, and it is not what Dioxus does.
`first_poll` stays; its panic becomes a trap-equivalent failure.

### Open item before the ADR rewrite — closed (2026-10-30)

The wasmtime reduced spike landed (§3.2 "Reduced spike — done") and ADR 015 R5 / ADR 018 §3 were
rewritten in the same workstream. The remaining web-backend question (whether `wcmp`'s web
backend can resume a guest export that suspends on an async host import) is tracked under Q4 and
is not blocking: A1's trap-equivalent path handles `Pending` on either backend without a host
panic.

### Follow-up gaps closed before committing W2 (2026-10-30)

Reviewing "are we actually settled on W2" surfaced four gaps between what the rewritten ADRs claim
and what the code and the rest of the ADRs say. All four are closed in the same PR:

1. **A2 claimed as built.** R5 read as if the MDK mirrored `SpawnIfAsync`; it does not. R5 and §3.1
   A2 now mark it *decided, not built*, and R5's overall status line carries the exception.
2. **A4's deferral had no check** (§1c requires one). Added
   `clay_rt_wcmp::wasi::tests::async_linked_wasi_imports_are_enumerated_and_pinned`, which enumerates
   `func_wrap_concurrent` registrations in `src/clay_rt_wcmp/wasi/` and fails if the set grows —
   the trigger for doing the link-time split instead of quietly widening the exception list.
3. **The wcmp engine was unpinned for the sleeping guest.** The wasmtime test proved the trap
   path on one engine only, which is exactly the "two engines keep the engine-neutral crate
   honest" claim ADR 018 §Consequences makes. Added
   `clay_rt_wcmp_wasmi::tests::guest_blocking_in_dispatch_fails_as_trap_on_the_wcmp_engine` over
   the same `sleeper` fixture.
4. **Stale clauses contradicting the new R5.** ADR 015 §1 deferred declarative prevent-default
   "until a second real engine forces the question" — that trigger has fired (wcmp) and R5 rejects
   the alternative, so §1, §Consequences, and §Deferred now record a rejection rather than a
   deferral. ADR 018's open question still cited a "panic-on-pending rule" that no longer exists;
   it now states the real stake (a conforming guest wrongly failed closed, per browser). ADR 018
   §Migration listed only `counter`/`slots` as prepared components; `sleeper` is prepared too.

Out of W2's scope and recorded for W4/F3: `CloseReason` is
`{Requested, DependencyWithdrawn, HostShutdown}` (`src/clay_host/instance.rs:19-23`), so a guest
that traps or blocks closes as `Requested` — indistinguishable from a user disabling it. F3's
failed-slot rendering needs a distinguishable reason; that is a lifecycle-model change, not a
boundary change.

## 3.2 Component Model alignment: how sync and async coexist (2026-10-30)

Research into the Component Model reference as implemented by `wcmp` (which follows Wasmtime and
the spec). Answers Q4's remaining uncertainty and the follow-up: what does the runtime *expect*
when a host makes a synchronous call, and what happens if something else comes in. Sources:
`rust/wcmp/src/instance/func.rs`, `rust/wcmp/src/executor/start_call_intrinsic.rs`,
`rust/wcmp/src/concurrency/{scheduler,suspend_seam,blocking_builtin}.rs`,
`rust/wcmp/src/store/store_data.rs`, `rust/wcmp/src/baseline/sync_lower.rs` (tests).

### The two axes are separate in the spec

A function's **type** (WIT `func` vs `func async`) and its **lift mode** (`canon lift` with or
without `async`) are independent, with one constraint: `canon lift async` requires an async
function type; a *sync* lift can carry either. So the spec itself anticipates a component with
mixed sync and async exports — that is the designed-for case, not an extension.

| | sync lift | async lift |
|---|---|---|
| **sync func type** | runs to completion; instance carries *may-not-suspend* for the call's length | not allowed |
| **async func type** | waits at the entry gate while another task holds the instance; can suspend | task-based; results via `task.return`/callback; waits at the gate |

### What a synchronous host call means (our `mount`/`dispatch`)

- The instance record gets **may-not-suspend** set for the length of the call.
- Guest code inside may invoke blocking builtins (including the *synchronous lower* of an async
  host import like `wait-for`) — but the rule is lazy: the block first runs all ready work of its
  own instance; if the condition is still unmet, the block **fails with the structured
  `CannotBlock` cause** ("cannot block a synchronous task before returning"). It does not
  suspend, and it does not hang. Proven by wcmp test
  `it_fails_a_future_that_stays_pending_with_the_cannot_block_cause`
  (`baseline/sync_lower.rs:667`): sync-typed export + never-resolving host future →
  `Err(CannotBlock)`, not `Pending`, not a host panic.
- An idle store with work left also resolves through this rule: `idle_cause` returns
  `CannotBlock` when any instance must not suspend, `Deadlock` otherwise
  (`store/store_data.rs:404-441`) — mirroring Wasmtime's `any_may_not_suspend`.

**Consequence for F4/A1:** for a spec-conforming guest, `first_poll` should receive
`Ready(Err(CannotBlock))` on the first poll — the failure surfaces inside the same turn the guest
blocks in, before any waker is needed. Our trap-equivalent handling (withdraw surface, close
instance) already covers it. The `expect("… completes on its first poll")` panic is therefore a
defensive assert that should be unreachable for conforming guests; converting it to trap-handling
(A1) is still right, but F4's severity as a *host-crash vector* drops: a guest that blocks inside
a sync call is killed by the spec's own machinery, exactly as a trap would kill it. What remains
unverified: empirically confirm on the **wasmtime** backend that the first poll yields `Err` and
not `Pending` (wcmp proves it for its own backend via the test above; wasmtime's async entrypoints
are driven differently). That is the reduced spike.

### What "something else comes in" means: the entry gate

Per component **instance** (not per export — all exports of one micro share one instance record):

- The scheduler is cooperative and single-driver: one poll = one turn, guest code runs only
  inside a turn, a second driver of the same store fails with `RecursiveDriver`
  (`instance/func.rs:261-266`). There is never parallel guest execution.
- **Async-typed tasks** enter through the instance's **entry gate** and wait there while another
  task holds the instance exclusively. Exclusivity is held by a sync task *until return*, and by
  a callback task *during each event* (released between events). Arrival order.
  (`start_call_intrinsic.rs:56-70`, `typed_func.rs:140-146`.)
- **Sync-typed tasks ignore the gate** — they run to completion and cannot block (may-not-suspend
  makes blocking a `CannotBlock` failure instead), so they can only occupy a turn where the
  running task has already yielded/suspended, which for callback tasks is exactly when
  exclusivity is released. (`func.rs:338-347`.)
- `call_concurrent` (from `run_concurrent` or a host async body) queues tasks; several may be
  awaited together; they serialize through the gate. Dropping the future cancels nothing.

So the spec's answer to "a micro is one instance also serving non-presentation calls" is: **they
queue per instance and interleave at suspension points; exclusivity prevents overlapping guest
execution; sync calls are bounded because they legally cannot block.** No host-side invention
needed beyond what the engine already provides.

### Alignment decision for our boundary

1. Presentation exports stay **sync-typed + sync lift** (`mount`/`dispatch`/`unmount` as WIT
   `func`). This is precisely the shape the spec's may-not-suspend machinery is built for, and it
   gives Dioxus's synchronous `prevent_default` its guarantee: the call legally returns (or fails
   with `CannotBlock`) before the listener closure must.
2. Future data/service exports (collections, long queries, doc reads) should be **async-typed**
   when they need to suspend — the entry gate already serializes them against presentation calls
   on the same instance. This is the principled version of "async by default": async is the type
   of anything that may wait; sync is the type of the calls that form the synchronous DOM
   boundary, and may-not-suspend is the spec-level enforcement of that promise.
3. Our host policy of one-event-at-a-time per incarnation (A3) is *stricter* than the gate and
   stays: it is a delivery policy, not a correctness need. The gate is the floor.
4. The remaining question this answers negatively: we do **not** need to redesign around
   "async calls into a mixed instance" — the component model already specifies it, both engines
   implement it, and our presentation sync calls coexist with future async service calls through
   the gate with no change to the presentation WIT.

### Reduced spike — done (2026-10-30)

One test per backend: a guest whose sync-typed `dispatch` blocks via `std::thread::sleep` (which
lifts to the async `monotonic-clock.wait-until` import); assert the host sees `Err` (treated as
trap: surface withdrawn, instance closed) on the **first** poll, not `Pending`, and the host
process does not panic. **Implemented and passing** as
`clay_rt_wasmtime::tests::guest_blocking_in_dispatch_fails_as_trap_not_panic_or_pending`, pinned
by the `sleeper` fixture (`src/clay_mdk/sleeper_fixture.rs`, prepared by
`x/check-dayframe.ts`). The wasmtime backend yields `Ready(Err(CannotBlock))` on the first poll —
never `Pending` — confirming the Component Model's may-not-suspend machinery; the wcmp engine was
confirmed the same way (a temporary probe in `first_poll`'s `Pending` arm never fired under
`--no-capture`). Linking the sleeper on wcmp also exposed that the world imports the *whole*
monotonic-clock interface: `wait-until` had to be registered alongside `wait-for` (both
`func_wrap_concurrent`, count pinned by the enumeration test). `first_poll` in both
engines now returns `Result<Output, Suspended>`; `Suspended` and an inner `Err` both take the
trap path (ADR 018 §3 as rewritten). Document deliveries (`changed`/`lagged`/`completed`) use the
same rule. The ADR rewrites (015 R5, 018 §3) and §5 status corrections landed in the same
workstream, followed by the four gap closures in §3.1 "Follow-up gaps closed" — including the same
sleeping guest pinned on the wcmp engine
(`clay_rt_wcmp_wasmi::tests::guest_blocking_in_dispatch_fails_as_trap_on_the_wcmp_engine`) and the
async-import enumeration test (`clay_rt_wcmp::wasi::tests::async_linked_wasi_imports_are_enumerated_and_pinned`).

## 3.3 Activation cost model, measured (2026-10-30)

Question: can a few thousand enabled wasm micros be started eagerly (Cordis-style), or does
activation have to be demand-driven? Measured with a throwaway probe on `clay_rt_wasmtime`. Setup:
release profile, x86-64 Linux, current `WasmPackage` config (fuel + component-model async,
default allocator), release guests. Each instance was activated, mounted into a host-side
`SurfaceTree` mirror, and sent one real click. Drivers were not spawned, so per-instance tokio
tasks are excluded.

| | `micro_counter` (Dioxus guest) | `slots` (no Dioxus) |
|---|---|---|
| artifact | 1.23 MiB | 0.10 MiB |
| cranelift compile (`Component::new`) | 160 ms | 20 ms |
| precompiled (`serialize`) size | 3.57 MiB | 0.35 MiB |
| precompiled load (`deserialize_file`) | 0.47 ms, +0.89 MiB RSS | 0.26 ms, +0.42 MiB RSS |
| precompiled load from memory (`deserialize`) | +3.57 MiB RSS | +0.35 MiB RSS |
| activate (instantiate + config) per instance | 0.12 ms, +0.18 MiB RSS | 0.06 ms, +0.04 MiB RSS |
| mount per instance | 0.04 ms, +0.016 MiB | 0.02 ms, +0.018 MiB |
| dispatch (click → re-render) | 0.04 ms | — |
| 3000 instances of one package, mounted | 605 MiB, ~0.5 s | 162 MiB, ~0.2 s |
| address space reserved per instance | 4162 MiB | 4162 MiB |

Reading:
- **Instances are cheap; distinct code is what costs.** The 3000-instance rows share one compiled
  component. Distinct micros each bring their own code. For 3000 Dioxus-sized micros:
  - cold compile ≈ 8 core-minutes;
  - precompiled cache on disk ≈ 10.5 GiB, plus ≈ 3.6 GB of `.wasm`;
  - code RSS when mapped from file ≈ 2.6 GiB (≈ 10.5 GiB when deserialized from memory);
  - instance state on top ≈ 0.5 GiB.
  Each Dioxus micro statically links its own copy of the Dioxus runtime, which accounts for most
  of the 1.2 MiB.
- **Eager start is affordable only under all three conditions:**
  - micros share code: one package exporting many micros (Cordis §6.5's granularity-by-package),
    or micros are small (slots-sized: ≈ 0.4 MiB code + 0.06 MiB instance each, so ≈ 1.4 GiB for
    3000 distinct ones);
  - a content-addressed precompiled cache exists (F10/W6); without it, every boot recompiles;
  - the memory reservation is tuned. The default 4 GiB + guard reservation per linear memory caps
    a 47-bit address space at ~30k instances, and a kernel with 39-bit user VA (256 GiB) at ~60.
    `Config::memory_reservation` sized to the store limit, or a sized pooling allocator, removes
    the cap at the cost of explicit bounds checks. This is one-engine policy (F10).
- **Lazy activation saves code loading, not instance cost.** A demand-driven micro that is never
  needed never has its code deserialized or mapped. That is the only large saving lazy activation
  buys at this scale. Shared-code packaging buys most of the same for eager start.
- Browser (napkin, not measured): wcmp compiles with `WebAssembly.compile` from bytes on every page
  load, with no streaming compile and no code cache (F7). Instance cost there is again small, and
  the dominant term is download + compile of distinct code. That makes the same packaging and
  caching levers apply even more strongly.
- An extra Engine + Linker per package (F10) measured at ~40 µs and no visible RSS.

### Native test compilation under contention (2026-10-31)

The release measurements above do not describe the unoptimized host test binaries under shared
CPU load. With prepared guests, a fresh `DAYFRAME_COMPONENT_CACHE`, and normal nextest
concurrency, the `clay_mdk` / `dayframe_desktop` suite reproduced nine 120-second timeouts
(51 passed). An external `strace -f -tt -T -e trace=openat,rename` run showed first cache
publication taking roughly 79–96 seconds. A separate untraced reproduction measured about
80% CPU `some` pressure in both the build scope and the host, no CPU quota throttling, and
zero memory-limit or OOM events; scope memory usage was about 918 MiB against a 6 GiB high
watermark. Limiting compiler workers with `RAYON_NUM_THREADS=1` alone did not eliminate the
timeouts.

There is also a concrete redundant compile in `src/clay_rt_wasmtime/lib.rs:374-412`: on a cache
miss, `compile_component` calls `Engine::precompile_component`, publishes its output, then calls
`Component::new` on the original bytes rather than loading the compiled output. Some direct
MDK tests compile independently of this cache. These observations identify cold compilation
and CPU contention as investigation targets; they do not prove that the redundant compile alone
accounts for every timeout. Removing it remains package-cache work (F10), not a lifecycle fix.

For bounded verification, the same 60 tests passed with a fresh cache and
`cargo nextest run --all-features --no-fail-fast --test-threads 2 -p clay_mdk -p dayframe_desktop`
in 70.191 seconds (selected feature resolution, prepared guest paths, no compiler-worker override).
A prior serial run passed all 60 in 29.750 seconds with a warm cache. No tests were skipped and
no timeout limits were increased; unrestricted concurrent cold-cache execution remains unresolved.

### 3.4 W4 design: binding-derived liveness (core mechanism implemented 2026-10-30)

Operator decisions, superseding the eager-start / Cordis-availability reading of ADR 014's
"exact demand triggers unresolved":

- **Not a process model.** Micros are singletons. No `spawn`, no `close`, no explicit stop.
  Disable (config datum) is the only intentional stop, per §3.1.
- **Alive iff reachable from an eager root through held bindings.** A *binding* is any
  host-routed edge the host can count: a mounted surface, a live subscription, an in-flight
  invoke. The host already owns all of them (separate address spaces). First binding appears →
  activate; last one dropped → idle grace, then evict. Reachability (tracing, not refcount) so a
  cycle of bindings cannot keep itself alive.
- **Eager roots are the only explicit start.** The shell's composition datum names them.
- **Contributions become static registry rows the host holds without an instance** (VS Code
  `contributes`, OSGi whiteboard registry). Today `contributions` is applied only *after* the
  micro runs, so the toolbar can only discover a button by booting it. This is the change that
  makes laziness possible at all.
- **"Who may write a registry row" = the micro's own manifest.** No arbitrary writes into
  another micro's clients collection; a row names its own micro and is dropped on
  disable/uninstall. The toolbar's own config datum decides which rows it accepts and their
  order (override records), and mounting an accepted row is the binding that activates it.
  The host knows nothing about toolbars; it knows "row targets T" and "T holds a mount on X".
- **Enable-time init** (`InitManifest`/`InitRunMode::PerInstall`, `daybook_types/manifest.rs:567`,
  `daybook_core/rt.rs:1181`) stays for genuinely dynamic registration (Chrome `onInstalled`),
  writing to the same registry under the same self-only rule. Not needed for the toolbar case.
- **Freezing is an optimization only.** Eviction must always be correct as a cold start from
  documents; a later Evicted-with-snapshot state may make resume fast. Blocked on wasmtime not
  exposing component memory/globals, and on resource-handle tables; wcmp equivalent unproven.
- Prior art naming: systemd `WantedBy` for reverse edges, socket activation for bind-starts;
  Android bound services (alive while any client bound, `startService` for the explicit root);
  OSGi delayed/immediate components + whiteboard; React/Compose presence-derived lifetime;
  Cloudflare Durable Object eviction criteria (no in-flight call, timer, or open stream);
  Fuchsia bind-starts + stop-idle escrow.

Open within W4 (not blocking): what happens to a visible surface whose micro is evicted —
(a) keep the binding, so data changes re-activate it; or (b) show the host's last mirrored
`SurfaceTree` (OLE presentation cache) until interaction. Default is (a) until the cost probe
says otherwise.

**Hardened (2026-10-30):** found and fixed a real hole from the first W4 pass — withdrawal of the
last binding was *reported* as `Idle` but the incarnation was never closed (`update` only retired
on configuration change). Eviction now closes and revokes. Also: `EagerRoots::{AllEnabled,Named}`
replaces the hidden "explicit policy" flag; the registry owns the single policy cell;
`retain_acceptance` drops every acceptance without an incarnation before liveness is computed
(failed-activation rollback, dead-consumer release, transitive within the reconcile loop);
declarations/`demand`/`accept_all` are addressed through the contributor's configured binding
(`contributes_to`), so two consumers of one contract never see each other's rows; broker handles
check `host.check(&instance)` and return `Revoked` after close. `ApplicationRuntime::set_demand`
and `set_policy` are gone: a root cannot pre-accept for a micro. Pinned by
`composition::tests::{accepted_static_contributions_activate_and_withdrawal_closes_the_incarnation,
a_demand_row_must_name_a_declared_contribution_addressed_to_the_consumer,
consumers_of_the_same_contract_each_own_only_their_bound_contributors,
a_consumers_acceptance_dies_with_its_incarnation, evicting_a_consumer_releases_what_it_accepted}`
and `application::tests::{a_broker_handle_is_revoked_with_its_incarnation,
a_failed_activation_rolls_back_its_acceptance}`.

**Built (broker pass):** `CompositionBroker` on every activation (`MicroActivation::broker`,
`Context::{declarations,accepted,accept,accept_all}`); the registry owns the shared policy cell and
the desired-projection, and a micro's acceptance wakes the driver like a shell edit. `accept_all`
is a standing rule over a contract, so late-installed contributors join without re-activating the
consumer. `micro_settings` now accepts `settings.sections` itself, so `dayframe_composition` no
longer names the counter anywhere — the desktop e2e still exercises activation, disable, re-enable
and trap through that path. Pinned by
`application::tests::a_consumer_micro_activates_declared_contributors_without_the_root_naming_them`,
`composition::tests::accept_all_*`.

**Current configuration pass (2026-10-31):** startup is `ConfiguredMicro::eager`, defaulting to
false; `EagerRoots` and the root startup setter are removed. Settings is the locked eager boot
entry, while sample and counter start through accepted declarations. `dayframe_composition`
uses `FacetConfiguration` over a granted document facet. Settings owns hidden/order preferences
in another facet and follows config plus `DeclarationWatch` updates in an incarnation-owned
future. Hide/show and startup edits expose optimistic operation receipts and rollback. Unknown
or unaddressed new acceptance is denied immediately; previously accepted rows removed or rebound
by configuration no longer obstruct reconciliation. Eager changes retain an otherwise demanded
incarnation. The document backend is still in-memory: none of this adds restart persistence.
Provider linkage for native built-ins is still explicit in the launchers; this pass does not
finish F5, and per-instance configuration ownership is unchanged (the config owner is still
global).

`CompositionPolicy` now contains only incarnation-owned `demand` and `accept_all`; root
reachability and `InstanceState::Idle` remain the lifecycle mechanism. **Not built:** automatic
demand edges from host mounts/subscriptions/invokes (a micro accepts explicitly today), idle
grace, guest-side (wasm) access to the broker, freezing.

## 4. Workstreams (agreed direction; each becomes an ADR or amendment before code)

| # | Workstream | Resolves | Depends on |
|---|---|---|---|
| W1 | Collection waist redesign: spec, WIT package, Rust rendering; subscription = reader resource; listing+hydrate; command/outcome; scope disposal; id namespacing; node adapter replay-boundary definition — **ADR 017 rewritten 2026-10-31**, code not built | F1, F2, F8 | Q1–Q3 (answered) |
| W2 | Sync presentation exports stay; suspension is trap-equivalent (not host panic); entry-gate alignment; guest-internal async under Dioxus once-then-spawn | F4 | Q4 — **done 2026-10-30**, except A2's MDK half (decided, not built) |
| W3 | Micro configuration + boot composition model (datum-backed, persistence abstracted); composition root names no micro | F5 | Q5 |
| W4 | Micro lifecycle = binding-derived liveness; static contribution registry; micro-owned acceptance (`accept`/`accept_all`); eager roots; eviction; failure states honoured by shells — §3.4, done except host-edge auto-edges and idle grace | F3, F5 | Q6 |
| W5 | Portability tiers: isomorphic MDK, portable manifest crates, platform escape hatch; port Settings | F6 | Q9, W1 |
| W6 | Package identity + caching on both engines; `InstancePre` on wasmtime (model on `daybook_core::rt` / `wash-runtime`); dedupe `clay_mdk::{wasm,wcmp}` and the two `clay_rt_*` adapters — shared engine/linker + precompiled cache **done**, package identity/`InstancePre`/wcmp cache open | F7, F10 | Q8 |
| W7 | Reintroduce `dayframe_app`; mobile shell | F5 | W3, W4 |
| W8 | Browser isolation controls (fuel instrumentation or equivalent, memory cap) | F9 | — |
| W9 | Measure browser hop cost; decide JS vs wasm placement of presentation helpers | F7 | Q7 |

Ordering suggestion: W1 and W3/W4 are the load-bearing designs and can proceed in parallel; W2 is
done (its WIT outcome feeds W1); W5–W7 follow; W8/W9 are independent. W4 before W3 in practice:
F3's failure states and the missing trap-distinguishable `CloseReason` (§3.1) are inputs to how a
composition root names and boots micros, not the reverse.

## 5. ADR status corrections — applied 2026-10-30

Independent of the redesign, these labels were misleading to readers; the status lines now carry
the corrections:

- ADR 017 "Accepted. Implemented in …": §1 (demand-scoped answering), §5 (narrowing demand),
  §1 ("no second mechanism") are not implemented. **Rewritten 2026-10-31:** those sections and
  the relay `Batch` waist are withdrawn, replaced by reader subscriptions, source-defined
  commands, and a shared overlay plane; the status line says the rewrite is the target, not
  evidence the waist exists.
- ADR 018 §3: the "no suspension point" premise was wrong while async WASI imports are linked.
  §3 rewritten (W2): trap-equivalent handling, `CannotBlock`, entry-gate alignment, pinning tests.
- ADR 014 "Accepted": §1 (`dayframe_app`), §7 (feature modules), §8 (gates controlling anything)
  have no implementation; the amendment's "Initial micro provider seam" is contradicted by
  `dayframe_composition`. Status line now marks these *decided, not built*.
- ADR 015 §9 / `micro_settings_manifest` doc comment: the portability claim is false as built.
  Both status lines now state the claim is not yet true (finding F6, workstream W5).
