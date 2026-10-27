# ADR 015: Clay — address-space-first boundaries for the composition runtime

**Status:** Accepted. Migration stages 1–8 and the crate-map phases 1–4
(`docs/scratch/dayframe-crate-map.md` §7–§8) are implemented; crate names below are as built.
**Amended by:** ADR 017 (collection substrate: §4 optimistic visibility, §6 contribution
collections), ADR 018 (browser engine, Proposed: §1 Shape W, §8, §Deferred).
**Amends:** ADR 014 (crate layout and seam placement; its data-runtime and micro-amendment
semantics are retained and quoted, not replaced). Builds on the settled rules in
`docs/scratch/dayframe-micros.md` (D1–D13) and the original design transcript
(`play.artifacts/2026-10-12T01-27-35-880Z_01a12db8-eac6-704a-bc16-2554dbd08db7.jsonl`,
cited below as `transcript:N` by JSONL line).

## Context

The `dayframe_dx` micro slice proved the composition semantics (lifecycle, contributions,
reconciliation, document mirrors, wasm guests) but grew them as one `dayframe_app` crate with
the boundaries placed by accretion. Review surfaced the structural defects:

1. **Two half-micros.** A wasm micro exists only as a guest component *plus* a hand-written
   native host-side adapter crate (`dayframe_counter` knows the Wasmtime engine, grants the
   facet, inserts the Settings section). A wasm guest cannot contribute to a collection, mount
   another component, or publish a provision; every capability except surface+documents is
   native-only. `guest/lib.rs:7` names this "the major violation of the design".
2. **The boundary types assume one address space.** `Instance` is an `Arc`-bump on the whole
   `Host`; `MicroActivation` hands feature code the full host authority surface; contribution
   entries carry typed Rust values behind `Rc`/`Arc`; the host kernel uses `Condvar` and
   `ThreadId`. None of that crosses to a browser, and `Condvar::wait` does not even *compile
   usefully* on `wasm32-unknown-unknown` (it panics on contention paths).
3. **The data layer was placed, not designed.** Document mirrors live in `host.rs`/`documents.rs`
   interleaved with lifecycle code (`host.rs:542`: "THIS STINKS!"), `documents.wit` sits inside
   the presentation crate, and the transcript's data-layer spec (three layers: host document
   service / per-context mirror / authoring API, `transcript:328`) was only partially expressed.
4. **Browser shipping was never priced in.** The host VDOM itself is wasm32 on web; "native"
   micros become code linked into that module; guests are same-realm components. The current
   sync `dispatch -> event-response` contract requires `preventDefault()` during the DOM event
   handler, which silently rules out Web-Worker guests — that decision had never been made
   explicitly.

wasmCloud was studied as the reference component-model host (`docs/scratch/dayframe-micros.md`
D-research, turn 17 brief). What we take: **deny-by-default capability linkage** (an undeclared
import fails at link time; the manifest *is* the allowlist), **WIT as the only boundary type
system including for native providers** (a workload never learns whether a capability is native
or a component), **one engine with cheaply-cloneable pre-compiled packages**, and **errors as
values, traps isolate the incarnation**. What we deliberately do not take: wRPC/NATS lattice
(no cross-process guests yet), the pooling allocator, epoch interruption (our guest calls are
synchronous exports bounded per call by fuel, D12; wasmCloud needs epochs because its guest
invocations are long-running async tasks).

## Decision

### 1. Runtime shapes, stated concretely

**Shape N — native application process.** One UI thread owns the host Dioxus `VirtualDom` and
all native micros (one address space, one heap). Each wasm guest incarnation is a separate
Wasmtime store with its own linear memory and handle tables; all guest calls are synchronous
exports bounded by per-call fuel (D12). Document delivery and watch pumps are polled by the
composition driver (D13), never by a detached task per watch.

**Shape W — browser.** The host (VDOM, composition, native micros) is itself one wasm32 module
in the JS realm. A guest is a component instantiated in the *same realm* (through the `wcmp`
component-model polyfill, ADR 018; jco transpilation was the original candidate), with its own
linear memory, called synchronously. Native micros are simply linked into the host module; nothing about their API changes. The data layer's node seam is a network
client behind the same `DocumentService` boundary; `outcome_unknown` is normal there, not an
edge case.

**Rejected for now: Web-Worker guests.** `presentation-events.dispatch` returns
`EventResponse { prevent_default, stop_propagation }` synchronously because the DOM requires
`preventDefault()` during the handler. A worker boundary forces everything async. Alternatives
(Dioxus-liveview-style declarative prevent-default, or async dispatch with default-always-runs)
are deferred until a second real engine forces the question (D10).

**Cross-cutting rule:** a type may rely on process-local affordances (`Rc`, closures, Dioxus
values, `Any`) only if it stays on one side of an address-space boundary *and* the same
capability is expressible across the boundary through the wire types. Convenience is allowed to
be local; authority and semantics are not.

### 2. The boundary rules

These are the runtime-shape constitution. Every FIXME disposition and every crate boundary
below is derived from them.

- **R1 — Plain data and opaque ids cross boundaries; nothing else.** No Rust references,
  closures, `Rc`/`Arc`, Dioxus components, signals, or `Any` values define a portable contract
  (`transcript:2408`, ADR 014:472-475). Local adapters may use all of those *behind* the
  boundary.
- **R2 — Authority is a handle resolved by a service, and a bare id is never authority.**
  Handles are cheap `Copy`/clone data (`InstanceId`, `ComponentRef`, `SurfaceId`, entry ids,
  watch ids). A handle grants exactly what its issuing service checks on use; obtaining an id
  through another channel grants nothing (`transcript:322`: "A warm cache is never authority";
  ADR 014:494: "A document ID obtained through another service is not authority").
- **R3 — Host services are plain Rust objects with methods; values travel between them.**
  No god-contexts: `MicroActivation`'s bag-of-everything is replaced by a small activation
  surface (§6). Feature code never receives `Host`. The privileged composition root (launcher,
  tests, engine) holds `Host` directly.
- **R4 — Shared values are plain, clone-cheap data.** Collections carry descriptors and handles
  (name, action reference, icon, component ref), not native objects (`transcript:60`). If a
  value could not be serialized for a wasm guest without changing meaning, it does not belong
  in a shared collection.
- **R5 — Synchronous presentation, asynchronous data** (settled ground rule,
  `dayframe-micros.md:50-51`). Rendering and event dispatch are sync; watches, mutations,
  receipts, configuration are async with explicit receipts.
- **R6 — Engine-neutral resources are opaque ids plus host side maps.** Engine handle tables
  (Wasmtime's `ResourceTable` or the browser engine's equivalent) store only `Send` data;
  non-`Send` renderer state lives in host-owned side maps keyed by those ids (D10's discovered
  constraint).
- **R7 — The core crates are wasm32-safe.** `clay_collection`, `clay_host`, `clay_docs`,
  `clay_micro_core`, `clay_surface` use no `std::thread`, no `Condvar`, no OS handles, and no
  executor assumption.
  Observation uses `tokio::sync` channels (executor-independent, compiles for wasm32). The native
  engine and the Dioxus adapters are the only platform-specific crates, and they are
  replaceable per platform.
- **R8 — Revocation is honest.** Revocation removes delivered values from cooperative mirrors
  and ends their watches; it does not claim to erase what malicious native code already copied
  (`transcript:328` §6).
- **R9 — We do not break microspace.** A host change must not force a micro to rebuild. Every
  micro-facing contract — the MDK authoring API, the guest WIT worlds, manifests, and payloads
  carried by the collection waist (ADR 017) — is designed for additive evolution. Payloads that
  cross to micros as domain data are self-describing (JSON today; CBOR acceptable), not
  host-version-specific binary layouts; `DemandToken` and contribution payloads already are.
  How an old guest keeps running against a newer host is open between two shapes: the host
  serves several versions of its WIT and adapts the old ones, or a versioned, fully typed
  adapter component lives in guest space between the micro and the host (a libc-style shim
  that can be composed ahead of time, i.e. deferred static linking). Neither is built; the rule
  binds API design now so either remains possible.

### 3. Crate layout

`src/` is the crate root: every direct child of `src/` is a crate, named to show its family.
Crates keep their sources at the crate root (`lib.rs`, no inner `src/` nesting). "Dayframe" is
the shipped composition; the framework is **clay**.

```text
src/
  clay_collection/   the shared collection substrate (ADR 017): Replica, Ledger, View, the
                     Batch waist, demand and coverage. No host, document, engine, or Dioxus
                     concepts.
  clay_guest_session/  engine-neutral document host sessions: admission, resource lifetimes,
                     cancellation and neutral deliveries. Owns the WIT sources; consumers
                     generate their own bindings. No Wasmtime or Dioxus dependency. Grows into
                     the shared engine layer when a second engine lands (ADR 018 §2).
  clay_surface/      the typed Mutation wire, replay, SurfaceTree, and the private `render`
                     projection into a host VirtualDom. No Blitz, no Wasmtime, no host concepts.
  clay_host/         the authority kernel: Host, plain Instance identity, lifecycle and
                     dependency edges, capability tokens, contribution collections over
                     clay_collection, component refs and mount records, root window/mount
                     collections, and the `RendererRegistry` seam. No Dioxus, no documents,
                     no engine.
  clay_docs/         the document data layer: facet model (FacetRoute, FacetValue, Freshness),
                     DocumentService (node seam) over clay_collection, sparse authorized
                     mirrors, watches, optimistic operations and receipts, the mock source.
  clay_micro_core/   micro descriptors, the provider registry keyed by MicroId, typed and
                     erased provisions, activation, composition reconciliation, configuration
                     sources, feature flags, and `ApplicationRuntime<R: RendererRegistry>`.
                     No Dioxus, no engine.
  clay_dioxus/       the Dioxus renderer adapter only: NativeComponents (the RendererRegistry
                     implementation), ComponentMount, SurfaceWriter/SurfaceSession,
                     MountedSurface, use_component_recovery.
  clay_rt_wasmtime/  the native component engine: store/instance per incarnation, fuel and
                     memory bounds, WIT host functions, document delivery pump, WasmPackage.
                     Depends on clay_guest_session, clay_surface, clay_host, clay_docs — never
                     on clay_micro_core. ResourceTable and generated ABI types remain here.
  clay_mdk/          the Micro Development Kit (§5). Features: `in_process` (authoring over
                     clay_host/clay_docs/clay_micro_core, renderer-agnostic), `view` (its Dioxus
                     half: ComponentSlot, use_collection), `wasmtime` (engine-backed providers
                     for composition roots), `wit` (guest bindings and the guest Context), and
                     `dioxus` (the guest VirtualDom adapter, GuestDom, export_micro!).
  micro_<name>_manifest/  a micro family's contract crate: id, provisions, requirements, facet
                     access, contribution payload types (§9).
  micro_settings/, micro_sample/, micro_counter/  the micros; micro_counter is a wasm32 guest.
  dayframe_desktop/  dayframe composition roots per platform: explicit linkage, configuration
  dayframe_web/      source selection, engine/renderer selection, launch. "Dayframe" names
  dayframe_mobile/   the composition we ship, not the framework.
  dayframe_showcase/
  ui/                design-system tokens and presentational components (unchanged scope).
```

The host/data dependency chain is `clay_collection <- clay_host <- clay_docs <-
clay_guest_session`; the shared session layer must not depend on an engine or renderer. Engine
adapters consume that layer and `clay_surface`. Guest MDK bindings read the shared WIT sources
without linking the host implementation. Micro crates depend on `clay_mdk` and portable contract
crates only — never on `clay_micro_core`, `clay_host`, `clay_dioxus`, or an engine directly; the
MDK `in_process` backend is the only path to them. Composition roots depend on everything they
link and name micros only through `linked_providers`. No crate above a micro may name a concrete
micro (D3).

### 4. The data layer (clay_docs), per the original discussion

The transcript settled this design (`transcript:322`, `transcript:328`). clay_docs is where it
lives, in three layers with different runtime shapes; its replica and optimistic ledger are the
shared `clay_collection` substrate (ADR 017 §9):

```text
DocumentService (host, one per application)
    node adapter / shared sparse cache / refresh dedup / retention+eviction /
    optimistic transactions / permissioned mirror delivery
DocumentMirror (one per granted context)
    authorized facet projections / local reactive updates / pending mutation state
Facet authoring API (clay_mdk)
    typed decoding / observable values / mutation intents / UI-framework adapters
```

Semantics that are **requirements**, quoted or paraphrased from the transcript:

- A mirror contains only the projections its context requested and retained; it is not a copy
  of the drawer. Two contexts may observe the same facet through separate mirrors while the
  host shares one fetch/cache entry (`transcript:328` §1).
- A **watch** is a resource: it delivers the current authorized state and retains a targeted
  refresh/live projection until released; dropping it ends delivery and reduces retention
  (`transcript:328` §2, §7). The initial snapshot and subsequent updates form one subscription —
  no read-then-subscribe gap; `mirror-revision` orders delivery to that mirror and is not a
  source cursor (ADR 014:89-95).
- Watching an authorized key that does **not yet exist** must be expressible; a lagged
  subscriber receives an explicit lag/reset signal and must reread (D6's `SubscriptionLagged`).
- Facet states distinguish **not-loaded / known-absent / present / refreshing-stale /
  denied-or-revoked** so an uncached facet is never mistaken for a nonexistent one
  (`transcript:322`). Availability/freshness is evidence for rendering; a refresh being pending
  never hides an existing local value (ADR 014 §4).
- Optimistic writes are host-owned: authorize → apply to the retained projection → publish to
  all authorized mirrors of that projection → submit → durable acceptance resolves the receipt
  without waiting for an event watermark; rejection or transport failure rolls the overlay back.
  Receipts distinguish `LocallyApplied`, `Accepted`,
  `RolledBack(Rejected | Transport { outcome_unknown })` (ADR 014 §5, D6, D11). Closing a
  mirror or ticket never undoes a submitted write (`transcript:328` §5). *Amended by ADR 017
  §4:* the accepted overlay stays visible until the base reaches the version the write landed
  at, so the row never flashes back to its pre-write value; it is not committed locally.
- Filtering happens **before data enters the mirror**: document/branch eligibility, facet tag
  and key id, read vs mutate rights, revocation. No unauthorized values, keys, or pending-write
  details arrive and get filtered later (`transcript:328` §6).
- Reads and subscriptions are **targeted**: no whole-map clone on a single-facet read and no
  full-collection broadcast to a watcher of one route (`transcript:322`, "the existing
  collection is not ready to be the whole document cache"). Watch-scoped snapshots (today's
  `DocumentSnapshot` per selection) satisfy this while selections stay small; the API must not
  grow an operation that clones the host's whole cache.
- Pending mutations and actively watched projections are never evicted (`transcript:322`).
- Blob bytes are not facet values; blob facets and the bytes they reference have separate
  retention and access treatment (`transcript:322`). Out of scope for this slice; recorded so
  the API does not preclude it.

**Contribution collections are a separate mechanism that stays separate.** Settings sections,
toolbar entries, root mounts are host-owned *transient registration* collections: owned entries
and separately owned overrides, disposal on owner closure, deterministic ordering, no
optimistic persistence and no source cursors (ADR 014:398-403; `transcript:36`, `transcript:58-60`;
Cordis §3.4.2's commutativity boundary — owned registrations commute, arbitrary shared mutation
does not). They share the reactive-read shape and, since ADR 017, the replication substrate
(`clay_collection`) with data mirrors — never the ownership or persistence semantics.
They live in **clay_host** (they are host bookkeeping, `transcript:225-226`), parameterized by
plain-data entry types (R4), with lease handles (§6). Configuration is not a third store: micro
configuration lives in document facets reusing plug-manifest declarations (settled rule,
`dayframe-micros.md:46-47`); *composition* configuration is a `ConfigurationSource` trait with
static/mock/facet backends and nothing below the trait mentions facets (D2).

### 5. The Micro Development Kit (clay_mdk)

Micros — native or guest — author against **simple functions and values, never the waist
protocol and never host authority** (`transcript:328` §4: "This API adds ergonomics, not
authority"). Sketch of the surface:

```rust
// in a micro's activate:
let config = ctx.config_document();                 // already-authorized binding
let count = config.watch_facet::<u64>(CONFIG_TAG, "count")?;
let current = count.current();                      // decoded snapshot, availability evidence
let op = count.write(next)?;                        // optimistic; receipt is an Operation
let entry = ctx.sections()?.insert(SettingsSection { title, detail })?;  // owned entry lease
let view = GuestDom::new_with_props(App, props);    // dioxus feature: guest VDOM adapter
```

The MDK is one crate with backends: `in_process` (in-process handles over clay_host/clay_docs)
and `wit` (guest-generated bindings from clay_guest_session/wit). The same micro source is
intended to compile to the host module or to a guest component. As built, both backends share
the entry shape and vocabulary but not yet one source: a native micro exports
`activate<R: RendererRegistry<Render = Element>>(&Context<R>)`, a guest exports
`activate(&Context)` through `export_micro!`, and both declare one non-generic `MicroManifest`.
Contributions travel as manifest data routed through the erased `Contributable` contract, so a
composition root names no contribution type. Typed decoding, Dioxus signal adaptation, and
closure-based mutation conveniences are MDK-local: a closure-based `update(|v| ...)` must lower to a defined patch or intent, never
serialize a callback (`transcript:328` §4). Mediated values (an action a contributor hands to
another micro) are **invocable handles**: serializing an action id must not manufacture
permission to invoke it (`transcript:153`). `GuestDom`/`WireMutations` move here unchanged in
semantics (D5).

### 6. clay_host shape: tokens, leases, collections

- **`Instance` names an incarnation; it does not own or operate one.** It carries plain host and
  incarnation identity plus descriptive metadata, not `Host` or watch channels. Services perform
  liveness checks and closure. A terminal observer is explicitly acquired before closure and
  receives its reason only after cleanup; retaining a reference alone neither retains service
  state nor creates a historical-reason obligation. This supersedes the earlier `Host`-holding
  token sketch (`host.rs:133-136` FIXME; R2, R3).
- **Leases are once-only release handles** (`transcript:222`): `EntryLease<T>`,
  `WatchLease`, `MountLease`. Explicit release, scope closure, and `Drop` converge on the same
  once-only operation; `Drop` does not promise completed asynchronous cleanup. A lease that
  outlives its owner is inert, never a stale-cleanup hazard: cleanup compares runtime
  incarnation identities so stale cleanup cannot withdraw a replacement (ADR 014:456-458).
- **Contribution collections** are host-owned stores of plain-data entries tagged by owner
  incarnation. Writers touch only their own entries; readers hold a read-only
  `clay_collection` view fed by per-subscription delta batches (ADR 017 §6–§8), not whole-map
  snapshots. Overrides are separately owned records, kept in a sibling collection and merged by
  the provider into the effective presentation;
  disabling an override owner reveals the current base entry — no stale snapshot is restored
  (`transcript:59-60`). Root windows and root mounts are built-in collections of this kind;
  layout micros consume them and may expose further mount collections (`transcript:225-226`).
- **Lifecycle recovery** drops the `Condvar`/`ThreadId` gate (R7): dependent-close ordering is
  synchronous on the calling thread (the close cascade already is); the gate only served
  multi-threaded close callers, which the wasm32 shape forbids anyway.

### 7. clay_micro_core shape: activation without god-objects

- `MicroProvider` is one async trait; the sync `MicroFactory` is a convenience wrapper. The
  sync/async split with `AsyncMicroFactory::activate -> Denied` (`micros.rs:255`) disappears.
- Activation receives a small value surface, not a host grab-bag: instance token, declared
  requirements resolved to typed provisions, a staging area for publications, and an
  `own_future` registration point (D13). `MicroActivation` keeps that shape on both MDK
  backends; the registry-facing internals (`PendingActivation`, provisions storage) are
  crate-private.
- Incarnation-owned futures are polled with `futures::stream::FuturesUnordered`-style
  combinators, each wrapped in a select-on-owner-close — no hand-rolled poll loop
  (`micros.rs:59`), still no `tokio::spawn` (R7: no executor assumption).
- Composition reconciliation, configuration sources, and `InstanceState` vocabulary are
  unchanged in semantics (ADR 014 amendment §"Initial micro provider seam"); they move crates
  unchanged in this stage.
- Feature flags (ADR 014 §8) move to `clay_micro_core::flags` unchanged.

### 8. The engine seam (clay_rt_wasmtime)

- One wasmtime `Engine` per process; `WasmPackage` is the cheaply-cloneable pre-compiled
  component handle (wasmCloud's `InstancePre` shape); one store per incarnation with fuel,
  memory, table, and handle caps (D12). Store context carries per-incarnation host state as
  plain data; renderer state stays in host side maps (R6).
- **Deny by default:** the linker satisfies exactly the imports of the `micro-client` world;
  anything else fails at instantiation. A micro's manifest (§9) declares its facet access and
  contracts; undeclared access is a link/grant error, mirroring wasmCloud's
  manifest-as-allowlist.
- Traps close the incarnation and withdraw its contributions; siblings are unaffected;
  `exhaust_fuel` remains the test hook (D12).
- The generic wasm provider (`clay_mdk::wasm`, feature `wasmtime`) takes a manifest + package
  and performs the ceremony dayframe_counter used to perform: grant the declared facets,
  activate the component, route declared contributions. A wasm micro needs no bespoke native crate.
- The browser engine is `clay_rt_wcmp` (ADR 018, Proposed), a future crate against the same
  clay_guest_session/clay_surface contracts (D10); nothing in this ADR builds the abstraction
  early, but R1/R6/R7 are what make it possible.

### 9. Micro manifests

Following the plug pattern (`micros/settings_contract:2` FIXME): each micro family gets a
`micro_<name>_manifest` crate holding the descriptor data — id, version, declared requirements
and provisions, facet access declarations (tag, key id, rights), and shared contract types —
written in Rust so compatibility is type-checked at build time. The runtime consumes manifests;
it never enumerates the world (settled ground rule). A `cargo x` packaging step (build-oci
equivalent) is future work; manifests today are compile-time data.

The contract crate is required when another crate names the micro's contract types, or when a
binary needs the manifest without the micro's code (the Wasm case: a composition root links
`micro_counter_manifest`, never `micro_counter`). A purely native, self-contained micro may keep
`manifest()` inline (`micro_sample`). The provider registry is keyed by `MicroId`, and the
composition root registers providers in one appendable `linked_providers` list. A manifest's
`version` must change whenever its wire shape changes incompatibly; the enforcement of that
discipline is open (R9).

### 10. Presentation slots (the mount primitive)

A guest that renders composition (a layout micro, a canvas micro) must place *another*
micro's presentation inside its own tree. The wire gains a slot: a reserved element or a
`Mutation::MountComponent(component-ref)` whose subtree the host fills with the referenced
component's render (recursively through the same machinery). `ComponentMount` becomes the
native backing of a slot; `NativeComponents`' renderer registry is the native realization of
"host fills slots". Internal DOM structure is never an API (settled rule); slots reference
components by handle, not DOM nodes (`desktop/composition.rs:189` FIXME). Native code may
obtain `NativeComponents` through Dioxus context instead of threading props
(`composition.rs:193` FIXME); the prop remains the explicit form.

### 11. FIXME disposition table

| FIXME | Disposition |
| --- | --- |
| `guest/lib.rs:7` (native guests bypass the boundary) | §5 clay_mdk dual backends |
| `host.rs:2` (is this wasmcloud?) | §8 engine seam; wasm-composition hosting ≠ micro-composition hosting, separate crates |
| `host.rs:133/136` (Instance naming, owns Host) | §6 token semantics, `InstanceId`, authority removal |
| `host.rs:542/549` (data layer placement, narrow waist) | §3 clay_docs, §4 spec, clay_guest_session interfaces |
| `documents.rs:5` (bespoke data plane?) | §4: document mirrors and contribution collections are distinct mechanisms, both specced |
| `documents/collection.rs:1` (understand the origin) | §4 quotes the transcript design it must grow into |
| `documents.wit:1` (WIT placement) | §3 clay_guest_session |
| `presentation/lib.rs:1` (file too large) | clay_surface modules: `wire.rs`, `tree.rs`, `render.rs`, `replay.rs` |
| `app/lib.rs:3` (crate/dir naming) | §3 layout |
| `desktop/main.rs:1` (crate `src/` nesting) | §3 layout |
| `micros.rs:59` (don't we have tokio) | §7 combinator-based driver |
| `micros.rs:118` (glossary) | §Vocabulary below |
| `micros.rs:123` (MicroActivation OOP) | §7 activation value surface |
| `micros.rs:255` (AsyncMicroFactory) | §7 single async trait |
| `settings/lib.rs:31` (who builds SettingsContext) | §5: the MDK activation context; contract crates declare, MDK constructs |
| `settings_contract:2` (manifest naming) | §3, §9 |
| `counter/lib.rs:5` (micro knows the engine) | §8 generic wasm provider; engine knowledge moves to the composition root |
| `composition.rs:189/193` (native export in UI, ComponentMount plumbing) | §10 slots + context |

### Vocabulary

| Term | Meaning |
| --- | --- |
| Host | The clay_host service object owning lifecycle, tokens, collections, mounts. |
| Incarnation | Host state for one running micro instance; never a handle. |
| Instance | A cloneable reference naming an incarnation by plain identity; services check it, it grants nothing by itself. |
| Lease | A once-only release handle for one owned effect (entry, watch, mount). |
| Provision | A live published implementation of a declared contract, owned by an incarnation. |
| Contribution | An owned entry (or override) in a host-owned transient collection. |
| Mirror | A context's authorized, sparse, reactive projection of document facets. |
| Hop | One `clay_collection::View`: replica, overlay ledger, local query, upstream link (ADR 017). |
| Overlay | A segregated whole-row pending write, retired by outcome, base version, or scope disposal. |
| Demand | An opaque narrowing token sent upstream; it retains a subscription, never rows. |
| Coverage | The upstream's evidence that a demand is complete, so absence is distinguishable from not-loaded. |
| Watch | A retention + delivery resource over a mirror selection. |
| Operation | A submitted mutation intent with a stable id and an explicit receipt. |
| Engine | The component runtime (wasmtime today); instantiates and bounds guests. |
| MDK | The micro authoring API; ergonomic surface over the wire or in-process backends. |
| Manifest | A micro's compile-time declaration of identity, contracts, and data access. |
| Slot | A wire-level placement a host fills with a referenced component's render. |
| Composition root | The launcher that links micros, engines, and the configuration source. |

## Migration

Ordered so every stage keeps the scoped suite green (the 8 core packages + desktop):

1. **Crate split, no behavior change.** Extract `clay_surface` (presentation), `clay_guest_session` (WIT),
   `clay_host` (host.rs + contribution collections), `clay_docs` (documents.rs + mock),
   `clay_micro_core` (micros + application + features), `clay_dioxus` (native.rs). Rename
   `dayframe_*` crates and micro crates per §3; flatten crate roots to `src/<crate>/`.
   Re-home `documents.wit` out of the presentation package.
2. **Runtime-shape fixes.** Instance token docs + `InstanceId`; remove public `host()` from the
   token; slim `MicroActivation`; replace the Condvar recovery gate with synchronous cascade;
   combinator-based owned-futures driver; one async `MicroProvider` trait.
3. **clay_docs semantics.** Facet availability states, mirror vocabulary, targeted-read
   guarantees documented and enforced by API shape; mock keeps its privileged controls.
4. **Contribution collections in clay_host** with lease handles; root mounts and Settings
   sections consume them through leases instead of bare `Entry` handles.
5. **clay_mdk** with `native` and `wit` backends; `GuestDom`/`WireMutations` move in behind
   `dioxus`; micro_sample and micro_settings author against it.
6. **Generic wasm provider + counter collapse.** `micro_counter` becomes the guest crate +
   manifest; `dayframe_counter`'s wiring moves into the generic provider; engine knowledge
   leaves micro crates.
7. **Slots.** `MountComponent` on the wire; `ComponentMount` as its native backing; context-based
   component lookup for native hosts.
8. **Launcher renames** (`dayframe_desktop`, …) and final scoped verification.

Stages 1–2 are mechanical refactors guarded by the existing 135 tests. Stages 3–7 each add
focused tests; no stage may weaken a settled semantic (D1–D13, ADR 014).

## Consequences

- The browser host becomes a link target, not a redesign: core crates compile to wasm32, the
  only platform crates are engine + renderer adapters, and guests use one API.
- Wasm micros become first-class: any capability native micros have is a capability the wire
  has, because native micros use the same MDK.
- The host kernel shrinks to lifecycle + authority + collections; documents, composition, and
  rendering are replaceable layers around it.
- The sync-dispatch decision excludes Web-Worker guests until declarative prevent-default is
  designed; this is recorded, not discovered.
- We accept a large mechanical rename as one PR stage rather than shimming old paths; the
  interface break is intentional (breaking changes are preferred over compatibility shims).

## Boundary correction after implementation review

The original migration checklist is not evidence of completion. The operator's recovered
comments in `docs/scratch/dayframe-micros.md` and the following corrections supersede conflicting
API sketches above:

- An instance reference carries plain identity, not a `Host`, channel, or lifecycle methods.
  Services validate and operate on it. Terminal observation is an explicitly acquired resource;
  preserving an arbitrary old id must not require retaining all closed incarnations forever.
- Micro source and portable contract crates must not depend on `clay_micro_core`, `clay_dioxus`,
  or their privileged implementation types. Re-exporting those types from an MDK `native`
  module does not satisfy this boundary. Native and WIT backends implement one authoring API.
- The engine-neutral host implementation shared by Wasmtime and a second engine
  (`clay_rt_wcmp`, ADR 018) owns the WIT sources. Engine-specific generated bindings and
  resource tables stay in the engine adapters; putting Wasmtime types in their common dependency
  would reverse the separation.
- Node → Clay → guest uses query-driven partial replication. A query both requests hydration
  and retains a stream of authorized changes. Consumer reads use their local reactive replica,
  never an implicit synchronous remote read. Not-yet-loaded, known-absent, present, and revoked
  remain distinguishable even when the convenient value accessor returns no value.
- Snapshot and continuation belong to one subscription. Changes are filtered before delivery;
  overlapping queries must not duplicate effects or withdraw data still retained by another
  query. Releasing a query ends its upstream demand; it does not evict replicated rows —
  retention is the replica's own policy (ADR 017 §5 supersedes the earlier "ends its delivery
  and retention" wording). Late updates from a replaced query cannot repopulate its successor.
- Transient contributions and durable documents may share replication mechanics, not ownership
  or persistence semantics. Optimistic operations have explicit acceptance/rollback outcomes;
  closing a replica does not cancel a submitted durable write. IDs-only/host-backed value views
  are deferred optimizations, not the initial contract.
- A boundary is evaluated through actual Settings/Sample consumption and native/WIT parity
  tests. Compiling host code for wasm32 alone proves neither guest portability nor browser
  support. Each increment must state which consumers and engines it actually exercised.

## Deferred

- Cross-process guests and wRPC-style transports; Web-Worker guests; declarative
  prevent-default.
- The browser engine (ADR 018); hot-patch/reload of guests; OCI packaging of micro manifests.
- Collection WIT transport and cross-guest id namespacing (ADR 017 §Not yet implemented).
- One micro source compiled to both backends (today native and guest micros share the entry
  shape and vocabulary, not a source file).
- Old-guest compatibility mechanism under R9 (multi-version host WIT or guest-space adapter).
- Production `DocumentService` adapter over `index/facet_set.rs` + drawer (ADR 014:500-504).
- Blob retention treatment; document listing/discovery API (requires separately granted
  capability, `transcript:329`).
- Action invocation handles across micros (ADR 014 amendment Q5).
