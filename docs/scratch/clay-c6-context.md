# C6 proposal: one `clay_mdk::Context` for native and guest micros

Status: proposal (phase 1), no code changed. Grounded in `clay_mdk/{native,guest,engine,wasm,wcmp,mirror}.rs`,
`clay_guest_session/wit/*.wit`, `micro_{settings,sample,counter}`.

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

## 6. Open questions for the manager

1. Is dropping `Result` from sync accessors (panic on undeclared binding, `None` on unresolved optional) acceptable, or keep `Result` for forward-compat with runtime (powerbox) grants?
2. Contract for `publish_call` taken from the manifest declaration only (proposed) vs also stated in code?
3. Dual-target crates: should one crate be buildable as both native micro and Wasm guest (needs `export_micro!` to also emit a native `ACTIVATE`)? And `arguments`: add native equivalent, or drop from the author API?
4. Typed vs raw call handlers: CBOR serde generic as the default, or require Lexicon-typed (daybook_types) request/response?
5. `Context` as one struct with cfg'd inner (proposed) vs a `Backend` trait that micros are generic over (rejected: reintroduces `Context<B>`); OK?
6. Collection read handle exposes `async changed()` instead of tokio `watch`; OK to lose `revisions()`?
7. Guest hot paths: per-row mirror in the guest costs CBOR decode per `rows()` call; accept, or cache decoded rows per revision?
8. Is `RendererRegistry` kept as a `clay_host` seam with an `Element`-erased `Resources`, or collapse it to a plain `ComponentPublisher` trait now (breaking `clay_dioxus`)?
9. Public module naming: `clay_mdk::host` for composition-only items, replacing `native::`/`wasm::`/`wcmp::` - fine?
