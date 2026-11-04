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
  - Guest `Capability` has `status` and `call`; `redeem_presentation`/`PresentationSlot` followed in C7a
    below. `Context::instance()` is in-process only.
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

## C6d-2 notes (2026-11-04): runtime identity aligned to ADR 028

- **Request defaults.** `necessity` defaults to `required`, `resolution` to `configured`, both skipped on serialize at the
  default (`skip_serializing_if = is_default`), matching `big_plugs_core::request::Request`. Fixture JSON and snapshots omit them.
- **Micro id = installed-as plug id.** `clay_mdk::MicroId` is `<did>/<name>`, validated at construction and
  deserialization (DID syntax and plug-name grammar copied from `big_plugs_core::PlugId`: dots allowed, never leading, trailing
  or doubled; the unit tests pin it). `MicroId::parse/installed/local`, `.namespace()/.name()`. `Sidecar.id` is now the bare plug
  *name* (a `String`, checked in `Sidecar::validate`), so the sidecar JSON still reads as `big_plugs`' importer expects.
  `Package::new(namespace, sidecar, bytes)` / `from_json(namespace, ..)` build the id (`Package::id()`).
  Stand-in namespace `clay_mdk::LOCAL_NAMESPACE = "did:example:clay-local"`: `did:example` is the W3C documentation method, valid
  DID syntax that can never be a real publisher, so a stand-in id cannot be mistaken for a real one. Replaced by the node DID once
  Clay runs on the node.
- **Plumbing.** `InstalledMicro::new(id, sidecar, factory)` and the factory closure receives the id first
  (`|id, resources, bindings|`), so a factory cannot be built under another id than the catalog's. The supervisor keeps its opaque
  `clay_host::MicroId` (`From<&clay_mdk::MicroId>`, text identical); `CompositionRuntime`'s public API uses the validated one.
  `declared_rows(micro, sidecar)` takes the id. UI labels use `MicroId::name()` ("Enable dayframe.counter micro"), never the DID;
  the Playwright spec needs no change. Row keys a manifest chooses (the counter's settings row key `dayframe.counter`) are not
  micro ids and stay as they were.
- **Writer identity.** `clay_collection::ReplicaId` (16 bytes) and `WriterId::new(&replica, micro)`: bytes are the replica then the
  micro id's UTF-8, so bytewise order is (replica, micro) and `Stamp` orders (lamport, replica, micro). `WriterId::from_encoded`
  reads a stored/wire identity. `CollectionRegistry::new(replica)` (no `Default`); `Grant` carries the `writer`;
  `registry.writer(&micro)` for the durable install. The replica is generated once (getrandom) and stored by the composition in the
  `replica` subspace under key `id`; `CompositionRuntime::replica()`. Persisted-data break (pre-release, not migrated): stored
  rows' writers are the old micro-id bytes, a store from before this change keeps stale stamps, and a pre-existing binding datum
  (`binding -> collection`) no longer decodes.
- **BLAKE3.** `Package::digest` is hex BLAKE3 (`blake3` replaces `sha2` in `clay_mdk`'s `package` feature); `x/dayframe-web.ts` computes
  it with `jsr:@std/crypto@1.0.5` (Deno's WebCrypto lacks BLAKE3; verified equal to `b3sum`).
- **Grants.** The bindings datum maps a binding to `{collection, contract, access}` (`BindingGrant`) or `null`. `resolve` honors a grant
  only while it covers the current request (same contract; same access or write covering read). A widened request is unresolved
  (`Unconfigured`), never changing enablement; no recorded grant still takes the default. `set_binding` records the request's current
  contract and access (the runtime keeps each micro's collection requests for it).
- **Schemas.** `Sidecar::validate` lets a declared contract's selector/key/value/write-context schema lie outside the authority when
  some `requires` or `collection_requests` entry's `contract` names it.
- **Environment hazard seen.** The root filesystem filled during this item (the wasmtime component cache at `~/.cache/dayframe` is
  ~870 MB and grows); runs here used `XDG_CACHE_HOME` and `TMPDIR` on the big drive.

## C7a notes (2026-11-04): presentation slots inside guest micros

- **WIT.** Two async waits, both host-side level-triggered on the supervisor's status tick (`Activator::changes`):
  `capability-ref.status-changed(seen: ref-status)` completes once `status() != seen` (receiver taken before the
  status is read, so nothing is lost), and `presentation-component.revoked()` completes once the proxy's publication
  is revoked (the proxy keeps the tick receiver it was redeemed with: `PresentationComponent::revocation().wait()`).
  Bound references have no tick source and never complete (same as native). Implemented in `clay_guest_session`
  (`CapabilityRef::status_changed`, `Revocation`), `clay_rt_wasmtime` and `clay_rt_wcmp` (shared by wasmi).
- **One author surface.** `Capability::redeem_presentation` now returns the façade `PresentationComponent` (`component()`
  -> opaque `Mount`, `revoked()`), `Capability::status_changed(seen)` replaces the tokio-watch `status_changes()`.
  `PresentationSlotProps` lost `owner`: the in-process mount owner is the redeeming instance (taken from the proxy).
  The follow loop (`hooks::follow_provider`) is written once over those; only the driver and the render of `Mount`
  differ. In-process it is a `use_future`; in a guest nothing polls Dioxus tasks, so it is a runtime task
  (`Runtime::spawn_for_view`, cancelled when the slot component drops, which drops the proxy and releases the provider)
  that stores state in a cell and ticks the presentations (`Runtime::notify_views`). The loop reads the status *before*
  redeeming and waits for it to move off that, which (unlike reading after the failure) cannot miss a provider enabled
  between the failure and the read; a failure that itself changes the status costs one extra redemption attempt.
- **Wire.** Dioxus cannot write `mount-component`, so a slot is `div { style: "display: contents", data-clay-slot: id }`:
  `WireMutations::set_attribute` records the attribute as a slot edit and `take_resolving` expands it to
  `mount-component(ref)` + `append-children(1)`, i.e. the component is a child of the slot `div` (the element is the stack
  top when its attribute is written, so the stack stays balanced). The `div` is a wrapper, not the bare component the
  in-process mount produces; `clay_surface` admits only a fixed tag list, so a custom `clay-slot` tag is rejected.
  `GuestDom` holds `Slots` (id -> `Rc<component-ref>`) provided as Dioxus context; ids are retired when the slot element
  goes and released after the next flush (the flush may still carry its creation). The slot element is keyed by the
  component, so a new redemption is a new element.
- **Not done / question.** `mount`'s granted `components` list in a guest is still refused (assert) with its old message:
  nothing in the author API names a granted component (layout micros, ADR 015 §10), and which surface's list a Dioxus
  component would index is a product question.
- **Proof.** `lifecycle_conformance::context_follows_a_provider` (Wasmtime and wcmp/Wasmi): the `context` fixture's `follow`
  call parks on `status-changed` while the host-side provider is disabled, redeems when it is enabled, and ends on
  `revoked` when it is disabled; each step is a `progress/n` row the host waits on (no polling).
  `clay_mdk` `wasm::tests::a_guest_slot_mounts_another_guests_presentation_and_follows_it_through_disable_and_enable`: the
  `context` fixture's `slot` presentation shows the counter guest through the real renderer, clickable, then
  unavailable on disable and mounted again on enable. No new fixture crate; `x/check-dayframe.ts` unchanged.

## C7b notes (2026-11-04): toolbar micro and completion validation

- **Toolbar.** `micro_toolbar_manifest` (contract: `org.example.dayframe.toolbar.buttons`, rows `Button { label, icon,
  action: Attachment }`, key = contributor's chosen id) and `micro_toolbar` (MDK-only, mirrors settings). The toolbar
  reads the buttons collection (`use_rows`) and a press is `Capability::call::<(), ()>` on the row's attachment, which is
  the lazy endpoint the counter's manifest declared. Clay learns nothing about toolbars. The press disables only that
  button while its call is in flight; a failed call (disabled or faulted provider) shows in a `role="alert"` line.
  `status()` is not read by the toolbar (a disabled provider's button stays enabled and shows the failure on press).
- **Counter.** New call `increment` (contract `org.example.dayframe.counter.increment`, under the counter's authority:
  `Sidecar::validate` requires a declared call contract to lie under it, so it is not a toolbar-owned `action@1` NSID) and
  a `toolbar-button` contribution (key `increment`) through the optional write binding `toolbar`. `Counter::increment` is
  shared by the call and the view's click. The call returns once the write is *submitted*, not settled: the mock document
  source never completes an operation by itself (only `MockDocuments::settle/complete` do), so awaiting settlement hangs;
  the overlay outlives the incarnation (the native test reads the count back from a later incarnation).
- **Mounting (decision to confirm).** The toolbar registers a shell root like Settings, but `RootRow` gained
  `pinned: bool` (serde default false) and `Application` shows pinned roots even while Settings is closed; the toggle
  covers the unpinned roots. Before, the toggle gated every root. Alternative if "pinned" is unwanted vocabulary: a
  second shell collection, or the toolbar rendered by the shell outside the roots loop.
- **Icon.** `Button.icon` is carried as `data-icon` only; there is no icon vocabulary or glyph rendering yet.
- **Volatile button state not done.** The composition declares only durable collections (`declare` uses
  `durable_bytes_registrations`) and has no volatile collection declaration, scope-owned writer path or per-incarnation
  scope for manifest-declared collections; badge/enabled state would need that first, so it was left out per the brief.
- **Tests.** `dayframe_composition::tests::the_toolbar_renders_the_counters_button_while_it_is_stopped_and_a_press_starts_it_once`
  (native counter through `ACTIVATE`: the pinned root renders "Increment" through a `PresentationSlot`, the button row is read
  and `status()` is `Dormant`, counter `Idle` with 0 activations; one call = 1 activation, idle after, the count is 1 from a
  later incarnation). `dayframe_desktop::e2e::the_toolbar_button_increments_a_stopped_wasm_counter` (Wasmtime guest, real
  Application: Settings closed, guest idle, button rendered, press, reopen shows 1). Playwright
  `the toolbar button increments a stopped wcmp counter` does the same on wcmp. Desktop `desktop_runtime` and the web shell
  link the toolbar (on demand, enabled on admission, not locked: it has an enable/disable toggle in the lifecycle panel).
- **Completion validation.** Test-scoped pair in `dayframe_composition::tests` (`editor`, `provider`, test
  `the_editor_calls_only_the_providers_whose_trigger_matches`): the editor declares a durable completion-source collection
  and a `complete` call; two providers each contribute one row `{ trigger, provider: attachment }` to a lazy `complete`
  call. The editor filters rows locally and calls only the matching provider (non-matching providers stay `Idle` with 0
  activations, text matching nothing starts nobody). **`retain-call` is not reachable from `clay_mdk::Context`**:
  `Capability` has `call`/`redeem_presentation`/`status` but nothing wrapping `CapabilityRef::retain_call` (which exists in
  `clay_guest_session` and is covered by `lazy_tests::retained_proxies_keep_the_provider_live_until_dropped`). So the
  "one incarnation across keystrokes while the editor holds a retain-call binding" half is not proven through the MDK;
  the test instead pins the present behaviour (each call is its own incarnation, 2 activations for 2 calls). Adding
  `Capability::retain_call` (a `CallBinding` wrapper for both backends and a WIT resource for guests) is the open item.
- **Dead end seen.** An awaited operation in a guest call (`Operation::next` from inside a call handler) was tried
  and hangs, but only because the mock source never settles; the guest runtime itself was not shown to mis-wake (the
  `clay_rt_wasmtime` driver change tried while chasing it was reverted).

## C7b-2 notes (2026-11-04): follow-ups from the C7b review

- **Implementing is not owning.** `Sidecar::validate` no longer holds a published call's contract to the authority: only
  the collection contracts a package *declares* (and their schemas, unless requested) must lie under it. Implementing a
  contract grants no access, so it needs no request. The toolbar now owns `org.example.dayframe.toolbar.action`
  (`micro_toolbar_manifest::ACTION_CONTRACT`, empty request and response); the counter's `increment` publication
  implements it (`INCREMENT_CONTRACT` is gone). The toolbar manifest declares nothing for the contract: a call contract is
  only an NSID a provider and a caller agree on, there is no call-contract declaration kind in the manifest. Test
  `a_package_declares_only_nsids_under_its_authority` covers both directions, and the composition test that used a foreign
  call contract as its authority violation now declares a foreign collection contract.
- **Durable mock operations settle on persistence.** In `DocumentService::durable` a mutation writes the source (and the persist
  queue) when it is made, keeps its overlay, and the persist future accepts it (`State::complete_stored`) once the store
  has applied that change, before it wakes `persisted()` waiters. The persist future holds the state weakly so it does not
  keep the queue open. A durable operation cannot be rejected (`complete(.., Err)` on one asserts) and a mutation after
  `close_persistence` panics like every other source write. Volatile services keep manual `settle`/`complete`. The old durable
  test that rejected an operation was dropped for that reason; `a_durable_operation_is_accepted_once_its_write_reaches_the_store`
  pins the new behaviour. The counter's `increment` call awaits `Operation::next(0)` and maps a rollback to
  `Error::Rejected`; the native and wasm toolbar tests and the Playwright toolbar test still hold.
- **`Capability::retain_call`.** Returns `clay_mdk::CallBinding` (`call` over the retained binding, drop releases it) for
  both backends; the guest wraps the generated `capability_bank::CallBinding` (already generated, no WIT change). The completion test
  now has the editor `focus` (retain the matching providers' calls while the editor itself is held live by a lease, since the
  bindings live in its incarnation), complete three times (one provider activation), and `blur` (provider goes idle, the next
  completion activates again). Guest proof: the `context` fixture's `hold`/`release` calls and
  `lifecycle_conformance::context_retains_a_call` (Wasmtime and wcmp/Wasmi): two calls through the binding are one provider
  incarnation, the provider is still `Active` after `hold` returned, and `release` makes it `Idle` (awaited on
  `Supervisor::changes`). The `context_*` scenarios moved to `lifecycle_conformance/context.rs` with a `ContextRig` for the
  shared boot, to stay inside the module-item budget.
- **Pushed component list removed.** `presentation.mount` takes `(publication, placement)`; both engines, the guest runtime, the
  `export_micro!` macro, the fixtures and `ComponentSlot`/`ComponentSlotProps` are gone. `grant_components` stays only as a
  `test-utils` helper (the presentation-import tests still need component handles in a table). The `slots` fixture and
  `wasm_layout_slots_compose_interactive_wasm_children_and_isolate_withdrawal` went with it: the only pushed-component consumer,
  superseded by the C7a guest-slot test (a guest mounts another guest's presentation through a capability). Not covered any
  more: a mount refusing a component grant from another host (there are no grants). ADRs 029-031 never described pushing; the
  stale `ComponentSlot` mention in ADR 015's crate map and the `slots` fixture in ADR 018's migration list were updated.

## C7c notes (2026-11-04): the Wasm layout micro

- **Crates.** `micro_layout_manifest` (contract: `org.example.dayframe.layout.panels`, rows `Panel { title, content: Attachment }`,
  key = the contributor's registration id; `LayoutConfig { visible: Vec<String> }`, facet `org.example.dayframe.layout.config` /
  `main` of the `layout` configuration binding; manifest declares the collection, the config request and a pinned `shell.roots`
  row; `examples/emit_manifest.rs` writes the sidecar) and `micro_layout` (MDK only, `cdylib + rlib` like the counter: a
  wasm32-wasip3 guest through `export_micro!`, native `ACTIVATE` elsewhere; it links nowhere natively today, so there is no
  workspace dependency entry for it). Clay learns nothing about layouts.
- **View.** One toggle per installed panel row (`Show/Hide <title> panel`, writes the config facet), then one
  `section[aria-label="<title> panel"]` per configured id, in configured order, each a `PresentationSlot` over the row's attachment.
  An id with no row is `Panel <id> is not installed.` (`role="status"`); a provider that is disabled/faulted is the slot's own
  `role="alert"` ("Section unavailable ..."). Only a mounted slot redeems, so hidden panels never activate their providers. The
  layout never edits the configured list except through the viewer's toggles, so unregistered/unavailable ids stay put (and have no
  remove control: see questions).
- **Default arrangement = nothing visible** (`FacetState::Absent` reads as `LayoutConfig::default()`), not "all known panels":
  with all panels shown by default the layout would hold every contributor live from boot, which breaks the idle-while-unheld
  contract the existing desktop/Playwright scenarios assert (counter idle with Settings closed, toolbar press = one activation).
  The viewer opts panels in with the toggles.
- **Mounting.** Pinned root (like the toolbar): the arrangement is the main surface and should not disappear with the Settings
  toggle. The package is a discovered one, so it is admitted disabled like the counter; the lifecycle panel enables it.
- **Contributors.** The counter declares a `layout-panel` row (attachment = its `view`) through an optional write binding
  `panels`; the sample declares one too (attachment = `detail`). The sample is eager, so the "hidden panel never activates" proof
  uses the counter (guest, on demand) as the hidden one.
- **`SHELL_ROOTS` moved** to `clay_mdk::manifest` (crate root export, gone from `clay_mdk::host`): a guest manifest cannot link
  the `in_process`-only `host` module, and the layout's manifest names `shell.roots` in a request.
- **Guest runtime bug found and fixed.** `wit_bindgen::spawn_local` only pushes onto a global list that the executor of the *next
  polled component-model task* adopts. A task spawned from a synchronous entry (`mount`, `dispatch`, a document event: a view
  creating a `PresentationSlot` while it renders, which the layout does at its first render) was not polled until an unrelated wake;
  the only one a layout has is its collection feed's long poll, clamped to 30 s, so each first slot took 30 s (a 30 s clamp of
  `CollectionLimits::max_poll_timeout` to 3 s made it 3 s: that is how it was confirmed). `Tasks` now keeps a doorbell task parked
  in `initialize`'s scope and every spawn rings its waker. Pinned by `e2e::layout::a_hidden_panel_never_activates_its_provider_until_it_is_shown`,
  which runs under `start_paused` and asserts the virtual clock did not advance 5 s (without the ring it reaches 60 s; verified by
  disabling the ring). The existing clay_mdk guest-slot test did not catch it because the feed there had a change in flight.
- **Replaced `slots` coverage.** Interactive children: `a_mounted_panel_is_interactive...` clicks the counter inside the layout and
  the sample's flag; withdrawal isolation: disabling the counter turns only its panel into the placeholder while the sample's panel
  keeps handling clicks, and enabling remounts in place on a new incarnation. Unregistered/unavailable placeholders and unchanged
  arrangement: `an_unregistered_or_unavailable_panel_is_a_placeholder_and_the_arrangement_is_untouched`.
- **Tests.** Desktop (`dayframe_desktop::e2e::layout`, Wasmtime, real `Application`, Settings folded so the counter has one holder);
  Playwright `e2e/layout.spec.ts` (wcmp; the counter's runtime row reads `Idle` until its panel is shown, `Active` after, `Idle` after
  hiding). `x/dayframe-web.ts` now indexes every `<name>.manifest.json` package of the prepared package directory;
  `x/check-dayframe.ts` builds `micro_layout`, joins it to the wit-bindgen `std` feature check and emits its sidecar with the counter's.
- **Open questions for the owner.** (1) Default arrangement: empty (chosen, see above) vs all panels in key order; the latter needs
  the existing idle assertions reworked. (2) A configured id nothing registers has no remove control; adding one would be the only
  way an id leaves the arrangement besides re-registering it. (3) Arrangement is order-of-show only (toggle appends); reordering
  and removing unknown ids are not designed. (4) The slot's wording is "Section unavailable" for every presentation, including a
  panel. (5) Whether the layout package should be enabled on admission like the native built-ins.

## C7d notes (2026-11-04): validation matrix and test hygiene

- **Matrix.** `clay-pr-ledger.md` "Validation matrix": 21 items, evidence read from test bodies. Covered: ADR 030 #1-7, #9; ADR 031 #1-4, #6-8, #10.
  Partial: ADR 030 #8 (dynamic half), #10; ADR 031 #5, #9, #11. ADR 030/031 Status prose and ADR 030's "Implementation status" were stale and
  are corrected (still Proposed; acceptance is the operator's).
- **Bug found and fixed (`761cb9ca`).** `Supervisor::close` moved a node to `Closing` without `refresh` (mirror + `changes` tick). Any
  `changes` waiter on `Closing`, and `Activator::status` during a guest close, was wrong. Regression test hangs without the fix.
- **Gaps filled.** ADR 031 #1 (composition known-empty), #2 (`context_collections_and_calls` over volatile and durable collections),
  #7 (three-recipient concurrent call + redemption).
- **Deferred (not built), needs a decision.** ADR 030 #8 dynamic half: `Lease` has no holder, so two incarnations retaining each other
  never idle, and a lazy initialization cycle (A's initialize demands B whose initialize demands A) deadlocks on A's staged outcome rather
  than failing with `Cycle`. ADR 031 #5: no Lexicon validation of keys/values (only limits, attachment indices, selector ceilings);
  depends on the schema registry (big_plugs P7). ADR 030 #10 / 031 #11: no native `Incarnations`; the lifecycle fixture is wasm32-only.
  ADR 031 #9 guest-side `status()` of a bound reference after closure through the WIT: no fixture call exposes it (adding one needs a
  guest rebuild and a fixture change; the mapping is a one-line enum match per engine).
- **Hygiene.** Spin-waits replaced by event waits: composition `settle` (supervisor `changes().wait_for`), clay_host supervisor tests
  (`settle` over a change receiver; `Controls::logged` over a stored-permit `Notify`), `lazy_tests` (`entered_init`/`entered_call` permits,
  `settle`), `lifecycle_conformance` (`idle`, `leased_provider_closes_when_idle`, `until`; guest call handlers via a new
  `InFlightCalls` watch replacing `Arc<AtomicUsize>`, `Incarnations::calls_in_flight`), `clay_mdk` hooks test (`rendered`: mirror
  `changed()` then `wait_for_work`), desktop composition test. Kept on purpose: bounded `yield_now` loops (4-32 iterations) that assert
  something does NOT happen (nothing can be awaited for an absence): supervisor tests, `lazy_tests::stay_live`, conformance `stays_up`,
  composition slot-churn loops; `registration_tests_durable::InterleavingStore::apply` and `native::tests` handler `yield_now` are
  deliberate interleaving points, not waits; `sleeper_fixture` burns guest time on purpose. No `waitForTimeout`/`setTimeout` in the
  Playwright specs.
- **Env note.** Per-crate nextest runs need the `DAYFRAME_*_COMPONENT` variables `x/check-dayframe.ts` prints (without them three
  `clay_mdk` blitz/store tests fail in 0.03 s); the full gate sets them itself.

## C7e notes (2026-11-04): retained cycles and activation cycles (ADR 030 #8 dynamic half)

- **Holder.** `Lease` carries `Holder::{External, Incarnation(scope id)}` and counts per holder in `Leases` (a `Mutex<BTreeMap>`, one
  `Arc` per incarnation). `Activator` carries the holder; `Supervisor::activator()` is External, the factory's activator is minted in
  `start_visiting` per incarnation. No WIT change, no capability-layer change: the session's activator is the one the factory was handed,
  so `retain_call`/`redeem_presentation` leases are incarnation-held already.
- **Reachability.** `Supervisor::unreachable_group(start)` searches backwards from the provider over retainers (lease holders and
  configured consumers). Roots: eager, an External lease, a retainer that is Starting/Initializing/Closing or no longer supervised.
  Result is `start` plus the unreachable nodes it reaches forward (its SCC); `close_unreachable` closes them with `join_all`, so
  members' guest closes overlap and see peers revoked. Triggered only by a lease edge hitting zero (`Idle` queue) and by `release`
  after a close; never on reads. `Node::is_idle` is gone; `Host::is_bound` stays (tests and the conformance harness use it).
- **Behaviour change to know.** A non-eager micro that nothing leases and no reachable micro requires is now unreachable. In production
  non-eager micros start only through `acquire` (a lease) or as providers, so this is only visible to tests that called the private
  `Supervisor::start`; two supervisor tests now make their consumers eager. Starting consumers now count as retaining their configured
  providers (previously only bindings, which exist after initialization began).
- **Activation cycles.** `Inner::waits` (waiter -> targets, RAII `Waiting` guard) records configured provider starts and demands made
  by an initializing incarnation. `start_visiting` takes the waiter and, before awaiting a pending activation, fails with
  `StartError::Cycle(target)` if the target (through pending activations) waits on the waiter. Edges are recorded before any await,
  so of two demands racing into each other the second sees the first. The demanding side gets the error (`Unavailable` in the guest);
  the supervisor state is restored by the existing Cycle path (node back to Idle).
- **Tests.** supervisor: mutual retention (stays live while an external lease reaches either; one close each), three-member group
  (close gate on `a` makes `b`, `c` finish first; `a` sees `b` Closed, no fault), A->B->A init cycle (+ restart afterwards), self
  demand, configured provider demanding its starting consumer. `lazy_tests`: two real `CapabilitySession`s retaining each other with
  `retain_call`. Fake factory gained `demands`, `retain`, held leases (cleared in root close). Note the Script holds a handle on held
  leases, so the root must clear them explicitly.
- **Not built.** Guest-level conformance scenario (see ledger gap detail): needs a supervised two-guest rig and a fixture rebuild.

## C7f notes (2026-11-04): guest bound reference, native conformance run, guest retained cycles

- **Guest-held bound reference (ADR 031 #9).** The `context` micro gained `probe(key)`: it reads the status of the row's first
  attachment, then makes a call and a `retain_call` through it and answers all three as text. `context_sees_a_closed_provider_revoked`
  attaches a `RowAttachment::Bound` publication of a host-side provider (taken from a lease), probes (`Live`, both calls succeed), drops the
  lease so the provider idle-closes, and probes again (`Revoked`, both fail `Revoked`, provider still `Idle`, one activation). Wasmtime, Wasmi
  and native. `ContextRig` lost its row arguments: `boot` starts the guest, `seed`/`seed_endpoint` write rows with a stored host writer.
- **Native conformance run: design.** The scenarios are generic over `Incarnations`, whose methods were `initialize` (from the guest-session
  `InitialContext`), `publications`, `mount`, `inspect`, `calls_in_flight`, `close`. Two ways to get a native run: (a) a native fixture
  with equivalent behaviour, which is a third copy of the fixture logic, or (b) run the fixture source itself in-process. The `context`
  micro is already written against `clay_mdk::Context`, which has an in-process backend, so (b): `clay_mdk/context_micro.rs` is one file,
  `#[path]`-included by the `context` example (guest, `export_micro!`) and by `clay_mdk`'s tests (`context_micro` module, with
  `extern crate self as clay_mdk` so the file names the crate the same way in both). The wasm slot test's hand-copied manifest is now
  `context_micro::manifest()`, which also fixed a drift hazard (adding `probe` had silently broken it; found by running clay_mdk's tests,
  which the first commit of C7f had skipped).
- **Native engine.** `native::Granted` is what an incarnation is granted before `activate`; `Granted::activate` builds the `Activation` and
  `Context`, runs `activate`, and admits the staged publications. The supervisor's `factory` now ends in it (behaviour unchanged), and
  `resolve_requirements` is shared too. `InProcess` (`native/conformance_tests.rs`) maps the scenario's `InitialContext` (activator, micro
  id, collection projections, registration grants, requirements, configs) to a `Granted`, uses the manifest for declared publications (the
  guest engines are told the same through `context.publications`), keeps the capability and collection sessions to count what survives
  `Root::close` (must be `GuestResidue::NONE`; it is, so the in-process close leaves no handle behind), and has no driver (native tasks are
  the incarnation's own local tasks, joined by the root).
- **Trait split.** `mount` and `calls_in_flight` moved to `GuestIncarnations: Incarnations`; the 15 scenarios over the raw `lifecycle`
  fixture (and `Harness`, `Fixture`) require it, the `context_*` scenarios require only `Incarnations`. So the type system states which
  scenarios an in-process micro can run. Wasmtime and Wasmi implement both.
- **Native scenarios that run:** all 6 `context_*` (collections and calls over a volatile and a durable collection; follows a provider;
  retains a call; sees a closed provider revoked; the two peer scenarios). **That do not (the 15 `lifecycle` ones), and why:**
  `provider_consumer_graph`, `lazy_references_activate_retain_and_release`, `leased_provider_closes_when_idle`,
  `failing_provider_publishes_nothing`, `registration_scope_ownership`, `failed_registration_initialization_publishes_nothing`,
  `undeclared_publications_fail_initialization` and the call-concurrency ones that drive `relay` (`overlapping_*`, `many_*`,
  `calls_reenter_*`, `disable_*`/`shutdown_with_calls_in_flight_*`) depend on the raw fixture: role by CBOR `arguments`, calls over raw bytes
  (`echo:` + request), a consumer root whose `close` redeems a proxy, registration bindings as raw `WriteBasis` writers, host-gated peers.
  `guest_trap_fails_outstanding_calls`, `sync_exports_interleave_with_suspended_calls` and `dropped_call_finishes_in_the_guest_without_leaks`
  are guest-engine properties (a trap, sync WIT exports, a guest store's residue) with no in-process analogue.
- **Plan for the other 12 (not built; larger than C7f).** (1) `Context::publish_raw_call(name, handler: Fn(Vec<u8>) -> Future<Result<Vec<u8>,Error>>)`
  on both backends (the native one is `RawHandler` already; the guest one is the `call` export dispatch). (2) A close hook on `Context`
  (`Context::on_close(async fn)`, run before the incarnation's handles drop; native: `NativeRoot::close`, guest: `InstanceRoot.close`).
  (3) Role by closure or manifest-declared configuration instead of CBOR arguments (the native engine can pass the role as a captured value).
  (4) Port `lifecycle_fixture.rs` roles to one `lifecycle_micro.rs` over `Context`, include it in the `lifecycle` example and in `clay_mdk`'s
  tests the same way as the `context` micro, and give `Harness` a native `Incarnations`. (5) `registration_scope_ownership` needs the typed
  `Registration` writer's `WriteBasis` surface, or stays guest-only. Estimated two to three days with the surface changes; it changes the
  authoring API, so it needs the owner's go-ahead.
- **Peer rig (ADR 030 #8 guest level).** `PeerRig` in `lifecycle_conformance/context.rs`: two supervised `context` micros `a` and `b`, each
  with its own `feed` holding one row (`peer` or `init`) that attaches the other's `echo` as a lazy reference; a `Peer<E>` factory hands each
  incarnation its own activator (so the leases it takes are incarnation-held) and a root that closes the engine incarnation, asserts
  `GuestResidue::NONE`, and joins the driver. The micro gained `echo` and the `init` row (a call `activate` makes and waits for).
  `context_peers_retaining_each_other_close_together`: `hold` on each through an external lease; dropping `a`'s lease keeps both up (b's binding reaches a)
  (32 yields, log unchanged), dropping the other closes both once. `context_peers_demanding_each_other_fail_as_unavailable`: acquiring `a`
  fails `Faulted`, both micros `Faulted` with an "unavailable" message and no instance, log `initialize a, initialize b`. All on native,
  Wasmtime and Wasmi.
- **Also:** the dead-code warning in `lazy_tests::PeerRoot` (C7e) is now an `expect` with a reason.

## C7g notes (2026-11-04): the portable lifecycle scenarios run natively, with no close hook

- **Ruling followed.** A micro never closes itself and author code gets no close callback (operator, 2026-11-04). The WIT
  `instance-root.close()` stays as the host to guest mechanism; the MDK answers it by dropping what the incarnation owns. ADR 030 §6
  gained the sentence saying so. Nothing was added to `Context` (no `on_close`, no raw call API, no stop token); C7f's plan items (1)
  and (2) are dropped, and (3) became grants.
- **What closing does, at the sites.** Native (`NativeRoot::close`): abort and join the incarnation's tasks (which owns every call
  handler loop, feed, view task and any value a task holds), then drop the publication leases; the supervisor calls it while the
  incarnation's providers and leases are still held and revokes afterwards. Guest (`Runtime::close`): stop and join tasks (each drops its
  future), then drop views, handlers and bindings. Both are commented with that mechanism. A value whose `Drop` uses a dependency is
  therefore held by a task or handler, not by something the registry outlives the close with.
- **The consumer scenario without a close hook.** The `context` micro keeps its redeemed `presentation` proxy in `Held`, owned by a task
  it spawns; `Held::drop` calls the proxy's synchronous `component()` and crashes if the provider was already withdrawn (a drop cannot
  return an error, so a violation is a panic: a close error on a guest, a task panic raised at the join natively). `provider_consumer_graph`
  passes only if that drop ran with the provider live; flipping the assertion makes it fail on native and on Wasmtime (done once by hand,
  not kept as a test). `GuestResidue::NONE` after the close shows the proxy was dropped once. The effect is synchronous on purpose: a
  `Drop` cannot await a host call, and the only async host-visible effects (rows, calls) would need the close to wait for them.
- **Roles from grants.** The micro gained optional requirements `presentation`, `service`, `peer`; a `relay` call (`relay:` plus the
  `peer`'s answer); `feed` rows `reject` (fail `activate` after publishing everything) and `name` (prefix of the row the incarnation
  writes). `ContextWorld::granted` (conformance side) is the one `InitialContext` builder; `ContextWorld::declared` the one declared set,
  used by every harness and by the raw fixture's sidecar. Calls are typed CBOR text (the wire is what the raw fixture also speaks, so
  `Gate`/`Gated` serve both).
- **Native additions.** `Granted` carries `calls: InFlightCalls` (counted per call handler from the moment the call is queued; the
  supervisor factory never reads it) and `Granted::activate` takes the declarations to admit against separately from the ones the
  micro publishes under (the factory passes the same map; `undeclared_publications_*` makes them differ, which is what a guest's
  sidecar disagreeing with its component is). `calls_in_flight` moved from `GuestIncarnations` into `Incarnations`; `GuestIncarnations`
  now has only `mount`, for `sync_exports_*`.
- **Raw fixture.** `lifecycle_fixture.rs` is a provider only (presentation `detail`, `echo`, `relay`, trap on the request `trap`), with
  the micro's wire and contracts; the consumer, failing and registration roles, the CBOR role arguments and the root that redeemed a proxy
  are gone. Its sidecar and the package test follow. `x/check-dayframe.ts` and the CI export needed no change (same two example names).
- **Not carried over (listed in the ledger gap detail):** the explicit `WriteBasis` sequence of `registration_scope_ownership` (the
  `Registration` writer derives bases from its mirror; covered at the writer's layer), the mount inside `lazy_references_*`, "the consumer
  publishes nothing", exact refusal wording.
- **Harness.** `lifecycle_conformance.rs` split into `calls.rs` (call concurrency, the engine-only trio) and `registrations.rs`
  beside `context.rs`, which also keeps the shared `seeder`/`cbor_text`. Two function-length limits (`activate`, the ownership
  scenario) were met by extracting `publish_follow`, `publish_retention`, `assert_owns_one_row`, `versions_of`.
- **Design questions for the owner.** (1) A `Drop` that must report failure can only panic; if micros need an observable
  cleanup result (a row written as the incarnation closes), that needs a design (a host-side effect on drop, not a hook). (2) Unchecked: native
  component renderers (`publish_component` closures) are handed to the component registry, so they may drop with the instance after
  `Root::close` rather than inside it as a guest's presentations do; values whose `Drop` needs a live dependency are held by tasks or
  handlers here (documented at `NativeRoot::close`), and a view-held one needs checking before anyone relies on it. (3) Whether to keep a mutation check of the
  `Held` drop order as a test.

## C7h notes (2026-11-04): native views are dropped inside close

- **The defect.** A native micro's `publish_component` closure lived in `NativeComponents.renderers` until `use_component_recovery`
  noticed the host withdrew the reference, i.e. after `NativeRoot::close` returned and the scope was torn down. A proxy or `Drop` guard a
  view captured therefore dropped after its providers could be revoked; the guest runtime drops its views inside `Runtime::close`.
- **Ownership fix.** `RendererRegistry::publish` and `NativeComponents::publish_surface` return `(ComponentRef, RenderGuard)`;
  `attach_surface` returns the guard. Dropping a `RenderGuard` removes that renderer (the removed closure is dropped after the map borrow
  ends). Native: `Activation.views` stages the guards, `NativeRoot` holds them and `close` drops tasks, then views, then leases. Guest:
  `GuestRoot` holds the surface guards and drops them before `E::close(runtime)` (each factory holds a runtime handle, so the engine's
  close now owns the runtime alone). One owner each: the host owns the reference's lifetime (withdrawn at supplier teardown), the guard
  owns the renderer.
- **Deleted.** `use_component_recovery`, `NativeComponents::recover_withdrawn`, `Host::component_lifetimes` and the `live_components`
  watch (it also rebuilt a full key set on every publish and teardown), plus the host test of it. Nothing needs host-driven cleanup now.
- **Behavior change to know.** `ComponentMount` used to panic ("live component has no native renderer") when a live mount found no
  renderer. The owner now drops its renderer as its close begins and the host withdraws the reference only after the close returns, so
  a render in that window legitimately finds the mount live and the renderer gone; it renders "Component withdrawn." like a withdrawn
  component. Slot recovery tests are unchanged and green.
- **Tests.** `native::tests::a_value_captured_by_a_view_is_dropped_inside_the_close` (the drop is observed with the instance
  `Closing`, exactly once; verified to fail with the guard's removal disabled). The `context` micro's `detail` view also captures a
  second `Held` proxy (a guard-order check on Wasmtime/Wasmi, where the guest drops views in `Runtime::close`); natively it does not
  discriminate by itself, the native test above does.
