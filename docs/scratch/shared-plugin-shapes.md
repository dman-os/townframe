# Shared shapes for micros and nodules

Status: Draft
Owner: nodules lane
Updated: 2026-11-18
Scope: access declaration, the refusal error, and the `Cx` / MDK `Context` surface for fetch, blobs, runtime grant requests and settings. Refs read: `stack/node-59-adr-d56` (S), `origin/feat/desktop-base` (D). No code changes proposed here; Rust types come first and schemas are derived from them.

Principle: the shared parts are types and method names in one place; a method differs between the two sides only where the runtime differs (a micro awaits and watches, a plain routine fails fast and reads a snapshot).

Precedent used: WASI (`wasi:http` request/response/body-stream, `wasi:blobstore`), Deno (`fetch`, permission prompt with a pending-request state), Android (`ActivityResultContracts.RequestPermission` returns a future, a plain service fails with `SecurityException`), Fuchsia (a refusal names the component lacking the capability).

## 1. Access declaration

Today
- Nodule: routines declare `uses` against the cartridge's `permbinds` (ADR 049 §3). The micro already has the same on D: `MicroContribution.uses: BTreeMap<String, RoutineUse>` (D `src/big_cartridges_core/lib.rs:367`), documented as "a micro with no `uses` can call nothing".
- Micro-only: `MicroManifest.requires: Vec<Request>` (`src/clay_mdk/manifest.rs:437`) with `Request { binding, contract, access?, necessity, resolution }` (`manifest.rs:342`). It does two jobs: (a) names the contract a micro calls or mounts, so configuration can bind a supplier; (b) implies the grant ("may call" or "may mount", `manifest.rs:345`). `Context::require(binding)` returns the `Capability` (`context.rs:775`).

Proposed
- Job (b) moves out. Whether a micro may reach something is answered by `MicroContribution.uses` over cartridge permbinds, narrowed by the permbind rules and intersected with grants (as the field doc already says). Nothing in the micro manifest grants authority; `Request` docs already say "declaring a request grants no authority", so this makes the type match its doc.
- Job (a) stays, renamed in intent: `requires` becomes pure supplier resolution (binding + contract + necessity + resolution), with `access` dropped from it. It is the micro-to-micro wiring, not a permission, and has no nodule analogue.
- Reuse the nodule type, no new one:

```rust
// big_cartridges_core, unchanged
pub struct MicroContribution { pub uses: BTreeMap<String, RoutineUse>, /* .. */ }
// clay_mdk::manifest, access removed
pub struct Request { pub binding: String, pub contract: Nsid, pub necessity: Necessity, pub resolution: Resolution }
```

Mapping
- Nodule side: nothing changes.
- Micro side: admission checks that each `requires` contract is covered by a `#method` permbind named in `uses`; the host mints handles from `uses`, not from `requires.access`. `collection_requests` keeps `access` (read/write) for now.

Open points for claytxt
- Does every `requires` contract correspond to a `#method` permbind the cartridge can author, or do micro publications need a permbind kind of their own (a `#publication`)? I could not confirm that from the code read.
- `configuration_requests` and `collection_requests` are untouched. They would meet `uses` at clay_docs "query mounts": the host streams matching datums to the micro under the permbind that covers the query, so the mount's request becomes a `uses` entry and the request keeps only its binding and shape.

## 2. One refusal shape

Today
- Nodule: `ContextError::MissingAuthority { cartridge: String, permbind: String, request: Option<String> }` in `method_core` (S `src/method_core/lib.rs:18`, variant at about :37). Wire form: HTTP 403, `{"error":"MissingAuthority","message":..,"operationError":{"Context":{"MissingAuthority":{"cartridge","permbind","request"}}}}` (S `src/big_dispatch/xrpc.rs:177`, test `src/big_ndk/tests.rs:323-350`). `Denied` is `DispatchError::Denied`, 403 `{"error":"Forbidden"}` (`xrpc.rs:193`), and `#net` fetch returns the bare `{ "error": "Denied" }` (`cx.rs:327`, `cx.rs:208`). Drawers and docs services still answer `Denied` (ADR 049 §4.3 interim).
- Micro: `clay_mdk::Error { Denied, Revoked, Unavailable, Limit, Lagged, InvalidValue, Conflict, Rejected(String) }` (D `src/clay_mdk/context.rs:42-65`), and `Context::call` returns `ClientError<M::Error>` (`context.rs:903`), so a call's refusal arrives as the method's error, not as `Error`.

Proposed: one `Refusal`, in `method_core` next to `ContextError`.
- Why there: it is the existing foundation crate, `allowed_dependencies = []`, builds native (schemars), `wasm32-unknown-unknown` and `wasm32-wasip3` (S `src/method_core/guardrails.toml`). `big_ndk` already allows it and `clay_mdk` already allows it (D `src/clay_mdk/guardrails.toml`). `big_cartridges_core` would also work, but it is the catalog, and a guest SDK should not link it.

```rust
// method_core
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum Refusal {
    /// The rules forbid it; no answer to a request lifts this.
    #[error("denied")]
    Denied,
    /// Declared by `permbind` but not granted to `cartridge` (not necessarily the caller).
    #[error("cartridge `{cartridge}` is not granted what its permbind `{permbind}` declares")]
    MissingAuthority { cartridge: String, permbind: String, request: Option<GrantRequestId> },
}
/// A UUID's text; transparent on the wire, so existing `request` strings stay valid.
#[serde(transparent)]
pub struct GrantRequestId(String);

pub enum ContextError { /* .. */ Refused(Refusal) }   // replaces the MissingAuthority variant
```

- Wire form changes by one level: `operationError.Context.Refused.MissingAuthority{..}` and `Refused: "Denied"`. HTTP status stays 403; the `error` string stays `MissingAuthority` / `Forbidden`. This breaks the fixtures in `big_ndk/tests.rs`, `big_nodule/e2e/executor/routines.rs` and `big_node/e2e/nodule_wasm.rs`; per house rule I would change them, not shim.
- The micro shows "Y needs permission for this" from `Refusal::MissingAuthority { cartridge: Y, permbind, request }`, with `request` handed to the shell prompt (ADR 049 §5, "Another cartridge's missing authority").

Mapping
- Nodule side: every service that answers `Denied` today moves to `Refusal::Denied` or `MissingAuthority` once the handle scope carries the permbind (the §4.3 interim). `Cx::fetch` stops returning a bare `{error: Denied}` body and returns `FetchError::Refused(Refusal)` (item 3). `ClientError<E>` gains `fn refusal(&self) -> Option<&Refusal>` where `E: AsRef<ContextError>`; one accessor, not per-service matching.
- Micro side: `Error::Denied` becomes `Error::Refused(Refusal)`; `Error::Rejected(String)` stays (a provider rejecting a request is not a refusal of authority). `Context::call` keeps `ClientError<M::Error>`; its `refusal()` gives the same type. The in-process backend maps its capability check to `Denied`, or to `MissingAuthority` when it knows the permbind; the guest backend decodes the 403 envelope with the same `ContextError`.

Open points for claytxt
- Is `Revoked` (incarnation ended) a refusal? Proposed: no, it is lifecycle, so it stays in `Error`.
- Is the in-process host able to name the permbind a refused handle came from? That is the same "scope carries the permbind" work the nodules lane owes the drawers service.

## 3. Same names and shapes on `Cx` and `Context`

Handles stay internal. Today `Cx` returns raw ids through `caller_capability`, `nodule_capability`, `config_capability`, `drawer_capability`, `blobs_capability` (S `cx.rs:169-197`) and `config_doc_id` (`:180`). Proposal: those getters are removed; the fields stay private and each method below presents the right one itself. The one legitimate use of an id, a nested call on the caller's behalf (the confused-deputy case in `caller_capability`'s doc), becomes `cx.call_as_caller(M)` beside `call` (`cx.rs:376`). The raw `transport()` escape (`cx.rs:321`) goes with them. The MDK already hides ids (`Capability` is opaque, `context.rs:121`), so this converges `Cx` toward it.

Shared types live in `big_ndk` (clay_mdk already depends on it) and the MDK re-exports them, so each name exists once. Only `Refusal` and `GrantRequestId` go lower, to `method_core`, because dispatch needs them.

### 3a. Network fetch (`#net`)

Today
- Cx: `fetch(&self, method: http::Method, url: &str, headers: http::HeaderMap, body: Vec<u8>) -> Result<(StatusCode, HeaderMap, Vec<u8>), HttpTransportError>` (S `cx.rs:333`); the response is read whole into a `Vec`, a refusal is a 403 the caller must parse.
- MDK: none (D `context.rs` has no network entry point; `grep fetch` finds only package loading).

Proposed (precedent: `wasi:http` outgoing handler, Deno `fetch`; the `http` crate is already used):

```rust
pub type Body = BoxStream<'static, Result<Bytes, FetchError>>;
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error(transparent)] Refused(Refusal),
    #[error("transport failed: {message}")] Transport { message: String, outcome_unknown: bool },
}
// on Cx and on Context, identical
pub async fn fetch(&self, request: http::Request<Bytes>) -> Result<http::Response<Body>, FetchError>;
```

Request body is `Bytes` for now (a streaming upload waits on a host need). The response body streams; a caller wanting it whole uses a `Body::collect()` helper (an inherent method, not a top-level function). `Transport` mirrors `ClientError::Transport`, so `outcome_unknown` is kept.

Mapping: Cx stops buffering and stops stringly reading the 403; the host's egress already writes `MissingAuthority`/`Denied` envelopes (`src/big_nodule/egress.rs:224`), which `fetch` decodes into `Refusal`. The micro host exposes the same through its `#net` permbind, answered by the same egress logic, as a new MDK host service.

Open points: does a micro's `fetch` go through the host egress (as I assume) or the webview's own `fetch` on the web shell? If the latter, `#net` is not enforceable there; flag for claytxt.

### 3b. Blobs (put/read)

Today
- Cx: `blobs_capability()` only (S `cx.rs:196`); an author hand-builds `PutBlob` / `ReadBlob` calls (`big_blobs_core/protocol.rs:169,172`: `org.example.daybook.blobs.put`, `.read`) and presents the id. MDK: none.

Proposed (precedent: `wasi:blobstore` container `get_data`/`write_data`):

```rust
// Cx and Context, identical
pub fn blobs(&self) -> Blobs;                       // cheap handle, no id exposed
impl Blobs {
    pub async fn put(&self, bytes: impl Into<Bytes>) -> Result<BlobInfo, BlobError>;
    pub async fn read(&self, hash: &BlobHash, range: Option<Range<u64>>) -> Result<Body, BlobError>;
}
pub enum BlobError { Refused(Refusal), Blobs(BlobsError) }
```

`Blobs` is the only new top-level symbol and holds both operations; there is no `put_blob`/`read_blob` pair. `BlobInfo`, `BlobsError`, `BlobHash` come from `big_blobs_core`. Open: is `big_blobs_core` wasm32 buildable and may `big_ndk` allow it in guardrails.toml (its current allowed list is `method_core`, `service_client_core_rs`, `service_core_rs`)? If not, a thin re-export crate is not worth it; move the three types to `method_core`.

Mapping: Cx builds the `needs_handle` capability internally from the routine's `blobs` handle; the micro's host does the same under the micro's `uses`. Both sides therefore need `#method` permbinds naming `blobs.put`/`blobs.read`.

### 3c. Requesting a grant at runtime

Today
- Cx: nothing typed; docs say "a routine may then ask with `requestCartridgeGrant`" (S `cx.rs:330`, method `big_cartridges_core/protocol/requests.rs:249`, returns `FiledGrantRequest`), so the author builds that call by hand, and every routine always holds the handle for it (ADR 049 §4.2 as-built).
- MDK: ADR 049 §5 specifies `ctx.request(permbind)` and `ctx.await_request(r)`; neither exists on D (`grep` finds no `request` method on `Context`).

Proposed: one filing method, one id type, and an answer future only where it can be awaited.

```rust
// Cx and Context
pub async fn request(&self, permbind: &str) -> Result<GrantRequest, Error>;   // files; never implicit
pub struct GrantRequest { pub id: GrantRequestId, pub cartridge: String, pub permbind: String }
// Context only (a micro awaits, a plain routine does not)
impl GrantRequest { pub async fn answered(&self) -> Result<GrantAnswer, Error>; }
pub enum GrantAnswer { Granted, Declined }
// Context only
pub async fn await_request(&self, id: &GrantRequestId) -> Result<GrantAnswer, Error>;
```

The deliberate asymmetry is `answered` and `await_request` (ADR 049 §4.2 "Who waits"; precedent: Android `RequestPermission` result vs a service's `SecurityException`). A plain routine calls `request`, gets the `GrantRequest`, and its own failing call already returns `MissingAuthority` carrying the same id; it ends there. A workflow can add `answered` later without a signature change. A drawer-set permbind uses `request(permbind)` too: the shell prompt is the powerbox, so `request_drawer` is not a separate method here (ADR 049 §5 names it; I propose folding it, since the permbind itself says it is a drawer set). "Allow never narrows" and "declined stays declined for the boot" are host rules, not signatures.

Open points for claytxt: confirm folding `request_drawer` into `request`; confirm the micro host re-mints the handle before `answered` resolves (§5) so the next call succeeds.

### 3d. Cartridge settings / config

Today
- Cx: `read_config(datums: Option<BTreeSet<String>>) -> Result<Value, ClientError<Value>>` and `write_config(set: BTreeMap<String, Value>, remove: BTreeSet<String>) -> Result<Value, ClientError<Value>>` (S `cx.rs:201`, `:228`): untyped JSON, a hand-built `docs.readDatums` body, error type `Value`, and a hard-coded `{ "error": "Denied" }` when nothing was delivered.
- MDK: `Context::config::<C: ConfigDecl>() -> Result<Option<Facet<C::Value>>, Error>` (D `context.rs:763`), a typed watched `Facet` with `current`/`write`/`refresh` (`context.rs:504-532`). The cartridge declares `settings: BTreeSet<Nsid>` (D `big_cartridges_core/lib.rs`, `Cartridge.settings`).

Proposed: the typed side wins; the micro's `ConfigDecl` becomes the shared declaration.

```rust
// big_ndk, re-exported by clay_mdk
pub trait Setting { const NSID: &'static str; type Value: Serialize + DeserializeOwned; }
// Cx: a snapshot at call time (a routine is one call long)
pub async fn setting<S: Setting>(&self) -> Result<Option<S::Value>, Error>;
pub async fn write_setting<S: Setting>(&self, value: S::Value) -> Result<(), Error>;
// Context: a watched facet (a micro lives across many renders)
pub fn setting<S: Setting>(&self) -> Result<Option<Facet<S::Value>>, Error>;
```

`Error` here is each SDK's own, both holding `Refused(Refusal)`. Same name, shared trait; the asymmetry (snapshot vs `Facet`) is deliberate, since a routine has no lifetime to watch over. The `BINDING`/`KEY` constants on the MDK's `ConfigDecl` are plug-local and stay MDK-only.

Open points for claytxt: the settings NSID in `Cartridge.settings` replaces the manifest's `configuration_requests` for the document ACL in the end state; that is the query-mounts question and is not decided here.

## Not covered

- `configuration_requests` / `collection_requests` redesign (waits on clay_docs query mounts).
- Streaming request bodies for `fetch`; the web shell's own `fetch` enforcement.
- Drawers/docs services moving from `Denied` to `MissingAuthority` (nodules lane work behind the handle-scope change).
- The shell prompt UI and its pending list (ADR 049 §5); workflow waiting.
- Migration of existing wire fixtures; this draft names them but does not enumerate.

## Manager review (nodules, 2026-11-18)

Accepted as the direction: access through `uses` only (`requires` keeps supplier resolution), one `Refusal` in `method_core` replacing `ContextError::MissingAuthority`, capability ids gone from `Cx`, and the same names on both sides with the snapshot/`Facet` and fail-fast/await asymmetries kept deliberate. Corrections before any code:

1. **Where the shared author types live.** §3 says `clay_mdk` already depends on `big_ndk`; it does only under its `guest` feature (`clay_mdk/Cargo.toml`, `dep:big_ndk`), so the in-process micro host never links it. Wire types (`Refusal`, `GrantRequestId`) go to `method_core` as proposed. The author-facing types (`FetchError`, `GrantRequest`, `GrantAnswer`, `Setting`, `Blobs`) need a home both runtimes always link: `method_core` if they stay plain types, or a small shared crate if they carry behaviour. To settle with claytxt; not `big_ndk`.
2. **Streams must not require `Send`.** A wasm guest is single-threaded, and the guest SDK's streams are `!Send` (ledger: "guest subscriptions need a `!Send` stream"). `Body` is a `LocalBoxStream` on the guest side; the native in-process host may need `Send`. Pick one type that works for both (e.g. generic over the backend, or `!Send` everywhere since micros and routines don't move bodies across threads), and say which.
3. **`fetch` is being built now** by the `#net` lane (streaming `Cx::fetch`, node-57). It should land with this signature, `fetch(http::Request<Bytes>) -> Result<http::Response<Body>, FetchError>`, so it doesn't change twice.
4. **`request(permbind: &str)` is stringly typed.** Fine for now; once the manifest builder exists, permbinds become typed values the builder hands out, and `request` takes one of those.
5. **The `Refusal` wire change** is a breaking change to fixtures in three crates, done outright (house rule), as one layer on the stack after `#net` lands.
