# C6 proposal: one `clay_mdk::Context` for native and guest micros

Status: C6b (in-process backend) and C6c (guest parity, isomorphic `export_micro!`) implemented; see "C6b notes" and "C6c notes". Grounded in `clay_mdk/{native,guest,engine,wasm,wcmp,mirror}.rs`,
`clay_guest_session/wit/*.wit`, `micro_{settings,sample,counter}`.
Owner: Claude (C6 subagent); manager reviews.
Scope: `clay_mdk` authoring surface, `micro_*` crates and their composition call sites.
Updated: 2026-11-04 (C6c: guest backend for the shared `Context` landed)

## 1. Author-facing API

One concrete, non-generic, cheap-clone handle. Methods are written once in `clay_mdk/context.rs`
and delegate to a private backend, so the two backends cannot drift in signature.

```rust
pub struct Context(/* Rc<backend::Inner> */);                 // Clone

pub async fn activate(cx: Context) -> Result<(), Error>;       // shape of every micro's entry (by value:
                                                               // no borrow held across .await)
impl Context {
  pub fn config(&self, name: &str) -> Option<&Config>;         // None = not granted (standalone)
  pub fn collection<K,V>(&self, binding: &str) -> Option<Collection<K,V>>;       // None = optional+unbound
  pub fn registration<K,V>(&self, binding: &str) -> Option<Registration<K,V>>;   // None = unbound / read-only
  pub fn require(&self, binding: &str) -> Option<Capability>;  // None = optional+unbound
  pub fn publish_component(&self, name: &str, render: impl Fn() -> Element + 'static) -> Publication;
  pub fn publish_call<Req,Resp,F,Fut>(&self, name: &str, handler: F) -> Publication
      where F: Fn(Req) -> Fut + 'static, Fut: Future<Output = Result<Resp, Error>>;
  pub fn spawn(&self, fut: impl Future<Output = ()> + 'static);  // incarnation-owned (own_future)
  pub fn arguments<A: DeserializeOwned>(&self) -> A;             // guest has initial-context.arguments; native: ()/config
}
```

- **Sync vs async.** Everything that only hands out an already-granted binding stays sync
  (`config`, `collection`, `registration`, `require`, `publish_*`): both backends receive grants before
  `activate` (native `grant_*`, guest `initial-context`). The only async surfaces are `activate`
  itself, handle operations (`Registration::put`, `Capability::call`, `Collection::changed`) and
  handlers. Undeclared binding names panic (manifest invariant); only *unresolved optional* is `None`.
  `Result` is dropped from the sync accessors: failure modes there were all invariant breaks.
- **Entry macro (both):** `clay_mdk::export_micro!(activate)`. On the guest it expands to the
  `micro-client` exports as today (but `initialize` awaits `activate`). On native it is *not* needed:
  `clay_mdk::native::factory(resources, manifest, bindings, activate)` takes the same `async fn`. Making
  the macro also emit a `pub const ACTIVATE: fn(Context) -> LocalFuture<..>` is the only thing a
  dual-target crate needs (open Q3).
- **Error.** One `clay_mdk::Error`: `Denied | Revoked | Lagged | InvalidValue(String) | Conflict(Option<Stamp>) |
  Rejected(String) | Limit`. `From<AccessError>`, `From<DocumentError>`, `From<WriteFailure>`,
  `From<MutationError>`, `From<CapabilityError/CallError>`, `From<CapabilityFailure>`. `AccessError`/`DocumentError` stop
  being author-visible.
- **Config.** One `Config` (the mirror's `ConfigDocument` shape, which is already sync-read +
  event-fed and fits native `clay_docs::DocumentWatch::current()`): `config.facet::<T>(tag, key) ->
  Result<Facet<T>, Error>`; `Facet<T>`: `current() -> Snapshot` (status/revision/pending), `state(&snap) ->
  FacetState<T>` (Loading|Absent|Present), `write(&T) -> Result<Operation, Error>`,
  `resync()/refresh()`. Native settings' `FacetValue`/`Operation` map onto `FacetState`/`Operation`.
  Native keeps `clay_docs` types behind the facade; `DocumentRef`, `Freshness`, `FacetRoute` leave the micro-facing path
  (shells that seed mock docs keep `clay_docs`/`DocumentService` directly via `Resources`).
- **Collections.** `Collection<K,V>` (typed CBOR: key/value codec = serde CBOR via ciborium, panic on a
  row violating its contract, as today): `rows() -> Vec<Row<K,V>>` (value, stamp, `Vec<Capability>`
  attachments minted for this incarnation), `is_bootstrapped()`, `async bootstrapped()`, `async changed()`
  (replaces exposing `tokio::sync::watch`). `Registration<K,V>` derefs to its `Collection` and adds
  `async put(&K,&V, attachments: Vec<RowAttachment>) -> Result<Stamp, Error>`, `async delete(&K)`, and
  `put_with/delete_with(.., WriteBasis)`. **Default basis** = what the mirror holds for the key
  (`Observed(stamp)`, `Concurrent` if absent) as today; explicit `CompareAndSet` via `*_with`.
  Rows are retracted on incarnation close (host-side, unchanged).
- **Requirements.** `Capability` (unified `CapabilityRef`): `status()`, `async call_raw(Vec<u8>)`,
  typed `async call::<Req,Resp>(&Req)`, `async retain() -> CallBinding`, `async redeem_presentation() ->
  PresentationComponent`. A presentation is shown with the `PresentationSlot { capability }` /
  `ComponentSlot` Dioxus components (move from `view.rs`, now backend-neutral).
- **Publishing.** `publish_component(name, render)` any number of times, one per declared presentation
  name; returns `Publication` (attachable to rows as `RowAttachment::Bound(pub)`). `publish_call` takes a
  typed async handler over CBOR `Req/Resp` (decode/encode in the facade; undecodable request =
  `Err(Error::InvalidValue)` returned to the caller as `call-error.provider`). Raw bytes handler:
  `publish_call_raw(name, F: Fn(Vec<u8>) -> Fut)`. The contract comes from the manifest declaration
  of `name`, not a parameter (today's `ContractIdentity` argument disappears: no second source of truth).
- **Tasks.** `cx.spawn(fut)`: native `tokio::task::spawn_local` owned by the incarnation (aborted +
  joined on close, panic resurfaced); guest `wit_bindgen::spawn` (features `async-spawn` already on),
  dropped at `instance-root.close`. Panics crash (AGENTS.md).
- **Components read the context** by capturing handles (`Facet`, `Collection`, `Capability`) in the
  `render` closure; no ambient context. Hooks: `use_facet(&Facet<T>) -> Snapshot`, `use_rows(&Collection<K,V>)
  -> Vec<Row<K,V>>`, `use_changes()`. They subscribe through each handle's internal `Notifier`
  (backend sets how wakeups reach the VirtualDom: native tokio watch/scheduler, guest mirror notifier
  -> Signal bump inside `in_presentation`). Hooks sit in the root scope of `render` as today (state shared by all surfaces).

## 2. Backends

**Native** (`in_process`): `Inner` holds today's `Context` fields (instance, `configs`, `requirements`,
`CapabilitySession`, `CollectionSession`, `bindings`, staged publications, `Tasks`). `Collection` = current
`CollectionWatch` (feed task over `CollectionSession` reader); `Registration` = current one with
`put` made `async` (immediate-ready future; keeps one signature). `publish_call` wraps the typed handler
into `Arc<dyn CallTarget>`. `activate` future is run inside the existing `initialize` and awaited before
`admit_publications`. `Resources` drops the `R` parameter (see 3).

**Guest** (WIT lowering):
| method | WIT |
|---|---|
| `config` / `Facet::*` | `initial-context.configs`; `document-access.watch/mutate`, `document-watch.current/select/refresh`, `document-events.changed/lagged/completed` (mirror, unchanged) |
| `collection` | `initial-context.collections` -> `collection-reader.acquire(selector=[], resume)` -> loop `projection-lease.read(committed, limits)` in a `cx.spawn`ed feed task feeding `clay_collection::Mirror` (same type native uses); `Collection::rows` reads the mirror |
| `registration` | `initial-context.registrations` -> `registration-writer.put/delete` (`write-basis`: concurrent/observed/compare-and-set map 1:1 to `WriteBasis`) + a read mirror of the same collection |
| `RowAttachment` | `encoded-value.capabilities: list<capability-ref>` from `published-capability.reference()` |
| `require` / `Capability` | `initial-context.requirements`; `capability-ref.status/call/retain-call/redeem-presentation`, `call-binding.call`, `presentation-component.component` |
| `publish_component` | `capability-bank.publish-presentation(name)`; host calls `presentation-events.mount(publication, ..)` |
| `publish_call` | `capability-bank.publish-call(name, contract)`; host calls `micro-lifecycle.call(publication, request)` |
| `spawn` | `wit_bindgen::spawn` |

Runtime changes: `Runtime` keeps a `BTreeMap<name, GuestDom>` and a `BTreeMap<name, CallHandler>`;
`mount/unmount/dispatch` route by publication name (a surface remembers its dom: `surface.identity()` -> dom map);
`call` dispatches the handler; `initialize` awaits `activate` and no longer rejects collections/registrations/requirements.
Feed-task mirror updates and call handlers run through `in_presentation` so the Dioxus signal bump flushes edits.

**WIT gaps / changes** (small):
1. `initial-context` binds read and write bindings in two lists; the contract that a registration's *name*
   also has a collection binding for the mirror is unstated. Propose: a registration binding always
   comes with a reader of the same collection under the same name (host already has it: native does
   `grant_writer` + `collection`). Verify in `clay_rt_*`; if absent add it host-side, no WIT change.
2. Slots: `presentation.mutation.mount-component(borrow<component-ref>)` exists, and
   `presentation-component.component()` yields a `component-ref`, so guest `PresentationSlot` is expressible; `GuestDom`
   needs a Dioxus-side element that emits `mount-component` (today `mount` asserts `components.is_empty()`).
   No WIT change; it is guest-runtime work.
3. `micro-lifecycle.call` has no way to say *which caller*; irrelevant now.
4. `arguments` is opaque CBOR in WIT; native has no equivalent (`Option` from config) - see Q3.
5. Async handler concurrency: the `call` export is async but the store runs one task at a time with
   cooperative yields; a handler that awaits an outbound `capability-ref.call` must not deadlock on the provider store
   (the `GuestCall` queue is designed for this; the callee/caller same-store reentrancy case needs a conformance test).

## 3. Rendering

`RendererRegistry` implementors: `NativeComponents` (`Render = Element`) and a test-only `StubRegistry`
(`Render = ()`). Nothing else. So: author API is `dioxus_core::Element` on both sides. `Resources` loses `R`;
it stores a type-erased publisher built once at composition: `Resources::new(registry: impl RendererRegistry<Render=Element> + 'static, ..)`
keeps `Rc<dyn Fn(&Instance, Rc<dyn Fn()->Element>) -> Result<ComponentRef, AccessError>>`. `RendererRegistry` stays in
`clay_host` as the composition seam (not author-visible; maybe keep `Render` assoc type until a second renderer exists).
The stub test registry returns a never-rendered `Element` (`VNode::empty()`). **No escape hatch now**: a non-Dioxus
renderer would add a native-only `clay_mdk::native::RawPublisher` extension (a `Context` extension trait behind a
feature) and would not touch `Context`; YAGNI until a second renderer exists. Cost: `in_process` needs `dioxus-core`
(already a dep in the `dioxus` feature; `Element` is light).

## 4. Module layout (`clay_mdk`)

Public top level after C6: `Context`, `Config`, `Facet`, `Collection`, `Registration`, `Capability`, `Publication`,
`Error`, `Row`, `RowAttachment`, `WriteBasis`, `Stamp`, `Attachment`, `export_micro!`, hooks (`use_*`) + slot components,
manifest vocabulary (unchanged), `ComponentRef`. Everything else is `pub(crate)`.
- `context.rs` (new): `Context` and its handle types' public methods (the single source of signatures); `Error`.
- `backend/in_process.rs` <- `native.rs` + `native/collections.rs` (Inner, feed task, factory assembly; `native::factory`/`Resources`/`CollectionBindings`/`BoundCollection` stay public under `clay_mdk::host` for composition roots only).
- `backend/guest.rs` <- `guest.rs` runtime + `mirror.rs` + `registration.rs`/`wire.rs` pieces; `guest/dom.rs` stays.
- `engine.rs`, `wasm.rs`, `wcmp.rs`: unchanged roles (engine drives *host-side* guests); fold `wasm`/`wcmp` into `engine/{wasmtime,wcmp}.rs`, public `factory` re-exported as one `clay_mdk::host::wasm_factory`.
- `view.rs` -> `hooks.rs` (backend-neutral slots/hooks).
- Selection: cargo features, not target_arch (wasm32-unknown-unknown hosts native micros in the browser shell): `in_process` => native backend; `guest` (rename of `dioxus`+`wit`) => guest backend. Both may compile on a host for tests; `Inner` is `cfg`-gated enum when both are on, otherwise the single type. Raw-WIT fixtures keep `wit`/`bindings` only.

## 5. Migration

- `micro_settings`: `activate<R>(&Context<R>) -> Result<(), AccessError>` -> `async fn activate(cx: Context) -> Result<(), Error>`; `config.facet`, `collection(binding)` (now `Option`, required => `.expect`), `Facet<SettingsConfig>` from the facade; view uses `use_facet`/`use_rows`.
- `micro_sample`: same; `registration(..).unwrap().put(...).await`; `RowAttachment::Bound(detail)` from `Publication`.
- `micro_counter`: `activate(&Context)->Result<(),DocumentError>` -> async/`Error`; `use_document_changes` -> `use_facet`; gets a second presentation + a call (C6c).
- Shells/compositions: `dayframe_composition/{lib,bindings,tests}.rs`, `dayframe_desktop/composition{,/tests}.rs`, `dayframe_web/shell.rs`: `Resources<R>` -> `Resources`, `factory` call sites unchanged otherwise; `micro_*_manifest` crates: drop `ContractIdentity` args if any.
- `micro_collection_probe`, lifecycle/slot/sleeper fixtures: raw WIT, untouched (`mirror` stays reachable inside `clay_mdk` only).
- Tests: `clay_mdk/native/tests.rs` (StubRegistry), `micro_sample/tests.rs`, `micro_settings` tests, `clay_mdk/tests.rs` (guest: add collection/registration/require/second-presentation/call fixtures through `export_micro!`).
- Deleted: `guest::Context`, `native::Context<R>`, `RendererRegistry` from the author path, `AccessError`/`DocumentError`/`ConfigDocument`/`DocumentRef` from micro-facing paths, `Runtime::call` stub, `initialize` rejection of collections/registrations/requirements, single-presentation assert, `use_document_changes` public name, the `ContractIdentity` parameter of `publish_call`.

## 6. Decisions (2026-11-04, manager)

1. Sync accessors drop `Result`: undeclared binding panics, unresolved optional is `None`. Runtime-granted (powerbox) bindings arrive later through a separate async request API.
2. A call's contract comes from the manifest declaration only.
3. Isomorphic MDK is in scope (ledger F6): `export_micro!(activate)` emits the guest exports under the guest backend and a native `ACTIVATE` entry under `in_process`. `arguments` leaves the author API.
4. Call handlers are serde CBOR generic; Lexicon typing waits for big_plugs. `publish_call_raw` only if a real caller needs it.
5. One `Context` struct with a cfg'd inner.
6. `async changed()` replaces the tokio `watch` in the public API.
7. Decoded rows are cached per revision, not decoded on every `rows()`/render.
8. `RendererRegistry` stays a `clay_host` seam behind an `Element`-erased `Resources`; no `clay_dioxus` break now.
9. Composition-only items live in `clay_mdk::host`.

## C6b notes (2026-11-04): where the code differs from the proposal

- `publish_component` / `publish_call` return `Result<Publication, Error>`, not `Publication`: the incarnation can
  close while an async `activate` is suspended, so `Revoked` is a runtime condition, not an invariant break.
- Collection and registration grants are taken before `activate` runs (the accessors cannot fail); a bound collection
  that is not declared fails initialization like the guest engine path. Feed tasks start when `collection()` is called.
- `Facet::write` takes `T` by value (the document service takes ownership); `Facet::refresh` returns the post-request
  snapshot. `FacetState` is one shared enum (crate root) for the guest mirror and the in-process facade.
- `Capability` is a wrapper (typed `call`, `redeem_presentation`, `status`); `AttachedRef` is gone, `PresentationSlot`
  takes a `Capability`. `Error::Unavailable` keeps a disabled provider distinct from a revoked one.
- Handlers run on the incarnation's executor through the same `GuestCall` queue an engine's driver serves; no
  `publish_call_raw` (no caller).
- `Resources` is non-generic; `wasm_factory`/`wcmp_factory` take the `NativeComponents` the surfaces attach to as an
  explicit argument, because the erased publisher cannot attach wire surfaces.
- Not done in C6b: `export_micro!` still emits only the guest exports; the native `ACTIVATE` entry and the guest
  `Context` come with C6c, which is why `micro_counter` still uses `clay_mdk::guest`.

## C6c notes (2026-11-04): guest parity

- **Shape.** `Context`, `Config`, `Facet`, `Operation`, `Capability`, `Publication` are single façade types
  in `context.rs`; each method delegates to the arm of a private enum for the enabled backend feature
  (`in_process`, `guest`, or both: a build holds either in a `Context`). `Collection`/`Registration`/`Row`/
  `Attachment` moved to `rows.rs` and are backend-neutral: only the page feed, attachment minting
  (`Minter`/`Stored`) and the write crossing (`Writer`) differ per backend. `Collection::changed` no longer
  uses tokio `watch`; an own change counter (`Observer`) serves both. Feature `dioxus` became `guest`.
- **Manifest in the macro.** `export_micro!(activate, manifest_fn)`. A guest has no manifest at runtime and
  the WIT `publish-call` takes a contract, so the macro is given the same `fn() -> MicroManifest` the host
  admits against; the guest takes call contracts and checks binding names (collection access, requirements)
  from it. One source of truth, no WIT change. Under `in_process` the macro also emits
  `pub const ACTIVATE` (boxed-future `fn(Context)`), which `host::factory` takes directly.
- **Registration needs a reader.** `engine.rs` already grants a collection reader for every bound binding and
  a writer for each `write` one, so a registration always has its mirror source; nothing host-side changed.
- **Guest runtime.** `Runtime` holds one `GuestDom` per published presentation (by name), the surface ->
  publication map, call handlers by name, staged publications and owned tasks. Every mirror/feed change sets a
  dirty flag; `mutate` then bumps each presentation's tick signal inside its Dioxus runtime (hooks read it via
  `ChangeTick`: nothing polls Dioxus tasks in a guest). Feeds long-poll `projection-lease.read` with
  `poll_timeout_ms = u32::MAX`; the host clamps it (30 s), and an empty page neither bumps the revision nor
  re-renders. `spawn` is `wit_bindgen::spawn_local` wrapped so `instance-root.close` cancels each task at its
  next poll (dropping the future cancels the host call it was parked in) and waits for them; close then clears
  views, handlers and bindings, which also breaks the runtime -> view -> context -> runtime cycle. The new
  conformance scenario asserts `GuestResidue::NONE` after close.
- **Deviations / limits.**
  - A guest `Publication` could attach to rows only while `activate` ran (fixed in C6e below).
  - Guest `Capability` has `status` and `call`; `redeem_presentation`/`PresentationSlot` stay in-process
    (guest slots need the `mount-component` GuestDom work in section 2.2). `Context::instance()` is in-process
    only.
  - `Facet::write` in a guest also resyncs the mirror (the host sends no event for the guest's own write); the
    counter's "mirror lagged / released" texts are gone (lag auto-resyncs; revoked watches make `use_facet`
    panic like in-process).
  - Added `Error::Lagged`; `OperationReceipt/Status/Error` and `RefStatus` are now crate types (converted from
    `clay_docs`/WIT), so both backends return the same values.
  - A standalone counter's `badge` shows the in-memory count as of its last render (only `view` is notified).
- **Proof.** `clay_mdk/context_fixture.rs` (example `context`, built via `export_micro!`) reads a host-written
  row and registers one recording it, serves a typed `rows` call from its own mirror;
  `lifecycle_conformance::context_collections_and_calls` runs it on Wasmtime and wcmp/Wasmi.
  `micro_counter` (view + badge presentations, `count` call) builds as the guest and links natively (unit test
  through `ACTIVATE`).

## C6e notes (2026-11-04): rows attach own publications by name

- **Change.** `registration-writer.put` takes `written-value { body, attachments: list<row-attachment> }`,
  `row-attachment = reference(capability-ref) | own-publication(string)`; reads keep `encoded-value`. The host
  resolves the name through `CapabilitySession::own_endpoint`, fed by `attach_own(micro, declared)` from
  `InitialContext.micro` + `.publications` (engines) or the factory (in-process), so it needs no live handle and
  works after `initialize`. An undeclared name is `invalid-attachment`.
- **Author surface.** `clay_mdk::RowAttachment::own("detail")` replaces `RowAttachment::Bound(Publication)`;
  `Publication` is gone (`publish_component`/`publish_call` return `()`). The guest `Staged` wrapper and
  `reference()` use are gone from the MDK; `micro_sample` writes its row with `RowAttachment::own(DETAIL)`, so the
  stored row holds an endpoint like a manifest row (it used to hold a live publication).
- **Why not drop `Bound`/`published-capability.reference`.** Host `RowAttachment::Bound` and `reference` still
  serve forwarding a held reference (a requirement's reference, a bound mint); only the own-publication use is
  subsumed. Candidate for removal if nothing forwards references.
- **Proof.** `clay_mdk` native test `a_row_written_after_activate_attaches_the_writers_own_declared_publication`
  (call handler writes the row, reader redeems it, undeclared name rejected); the `context` fixture gains
  `attach`/`detail`, and `context_collections_and_calls` asserts the stored endpoint and the rejection on
  Wasmtime and wcmp/Wasmi. The engine scenario stops at the stored endpoint (a conformance engine has no
  supervisor to redeem through); native redemption covers the activation path.

## C6d-1 notes (2026-11-04): manifest vocabulary aligned to ADR 028

- **NSID.** `clay_collection::Nsid` (re-exported by `clay_mdk`) replaces `ContractIdentity` everywhere: manifest, capability
  bank (`presentation_component()` is `org.example.clay.presentationComponent`), WIT `publish-call(name, contract: string)`
  (kept in package 0.4.0, like earlier unreleased WIT edits), durable endpoint rows (`[micro, publication, contract]`),
  collection ids and schemas. Runtime-defined: `org.example.clay.*` (`shell.roots`, `shell.rootKey`, `shell.root`);
  dayframe micros: authority and prefix `org.example.dayframe.<micro>` (`settings.sections`, `counter.count`,
  facet tags `*.config`); tests and fixtures: `org.example.clay.test.*` / `org.example.test.*`.
- **Request.** One `Request` (camelCase wire, `access` absent for `requires`, present for `collection_requests`,
  `minRevision`), mirroring `big_plugs_core::request::Request`; swap to theirs after the merge.
- **Sidecar.** `Sidecar { id, package_version, authority?, manifest }` is the flat on-disk form; factories take the
  `MicroId` separately, `InstalledMicro::new(sidecar, ..)`, manifest crates expose `manifest()` (guests) and `sidecar()`.
  `Sidecar::validate` = `MicroManifest::validate` + the authority rule; the composition drops a refused package whole.
- **Contributions** name a write collection request by `binding`; `declared_rows(&Sidecar)` groups rows by binding and the
  composition installs them into the collection the micro's resolution bound (an unbound binding skips its rows).
- **Not done (C6d-2):** micro id = installed-as PlugId, writer = (replica, micro id), BLAKE3, grant re-check.
- **Persisted-data breaks (pre-release, not migrated):** durable collections are keyed by the NSID text now, endpoint
  rows have a new CBOR shape, and a stored binding datum must hold NSIDs.
