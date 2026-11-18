# Unified cartridge manifest: micros and nodules, WASI-first

Sources: S = `stack/node-59-adr-d56`, D = `origin/feat/desktop-base`, W = `/run/media/asdf/p3N/repos/wasmCloud/crates/wash-runtime` (our fork; paths relative to it unless prefixed). Nothing built or run. Part A is research, Part B is the proposal. Claims about code I did not read are marked "unverified".

---

# Part A: what wash-runtime does

## A1. Caller attribution

- **Outgoing `wasi:http` is attributed per workload, with a per-component allowlist.** `CtxHttpHooks::send_request` (`src/engine/ctx.rs:484-503`) calls `HostHandler::outgoing_request(workload_id, request, options, fut, allowed_hosts)` (`src/host/http.rs:1386-1393`, impl `:2809-2823`). The hook struct holds `workload_id` and that component's `allowed_hosts` (`ctx.rs:475-479`), and the policy decision is `check_allowed_hosts` (`http.rs:4008`). So wash itself attributes the call, and a stock `wasi:http` client needs no header. The hook does **not** pass `component_id`, an invocation id, or a routine.
- **Component identity exists in the store, not in the hook.** Each component has a `Ctx { id, store_id, component_id, workload_id, .. }` (`ctx.rs:309-340`), but a store shared by linked components has one `active_ctx` slot swapped by `AccessorActiveCtxGuard` (`ctx.rs:200-232`). The code's own TODO says this is wrong under concurrency: concurrent linked calls "interleave" and `active_ctx` "need not match the running call" (`ctx.rs:190-204`). So with WASIp3 async concurrency inside one shared store, per-component attribution is not reliable today.
- **Per-invocation attribution exists, but only for host component plugins.** `JobRegistry` maps the running guest task (`GuestTaskId`) to a job and `CallerIdentity { workload_id, component_id, binding }` (`src/host/job_registry.rs:35-47, 108-124`; `ctx.rs:121-141`). The module doc says a plugin can partition state by caller "attributed exactly under concurrency via the caller's root guest task" (`src/plugin/component_host/mod.rs:~40-48`). This is the right mechanism; the `wasi:http` hook just does not use it.
- **Default stores are one-invocation stores.** `InstancePolicy::Ephemeral` is the default: a store is built, instantiated, invoked and dropped per call, on HTTP dispatch and linked calls alike (`src/engine/instance_pool.rs:1-12`; `types.rs:65-100`). With Ephemeral, or `Warm` with `max_concurrency = 1` (the default, `types.rs:81-90`), "which invocation is this store serving" is exact, because the store serves one at a time.

**Conclusion.** The host can attribute an outgoing call to a workload and a component today. To attribute it to the *routine* (export invocation), the fork needs one small change: put a host-minted `Invocation { routine, caller_capability, .. }` on the per-call `Ctx` when the host dispatches the inbound call (the host knows it: it chose the routine from the NSID), and pass `&Ctx`-derived identity into `HostHandler::outgoing_request`. That is exact for Ephemeral and `max_concurrency = 1`. For `max_concurrency > 1` (or shared linked stores) it needs the `GuestTaskId` route that `JobRegistry` already uses. Recommendation: nodules run Ephemeral/concurrency 1 (they are durable via the host actor, not via warm guest state), so the first change suffices, and `x-nodule-capability` becomes host-set state that the egress handler overwrites instead of a guest-supplied header.

## A2. Wiring and configuration

A workload is plain structs (`src/types.rs:29-37`; wire form `proto/wasmcloud/runtime/v2/workload.proto`):

```rust
pub struct Workload { namespace, name, annotations, service: Option<Service>,
                      components: Vec<Component>, host_interfaces: Vec<WitInterface>, volumes: Vec<Volume> }
pub struct Component { name, bytes, digest, local_resources: LocalResources,
                       pool_size, max_invocations, max_concurrency,
                       reclaim_window_seconds, reclaim_min_instances }
pub struct LocalResources { memory_limit_mb, cpu_limit, config: HashMap<String,String>, environment,
                            volume_mounts, allowed_hosts: Arc<[AllowedHost]> /* empty = deny all */,
                            allowed_ip_name_lookups, allowed_host_loopback_ports }
pub struct WitInterface { namespace, package, interfaces: HashSet<String>, version: Option<Version>,
                          config: HashMap<String,String>, name: Option<String> }   // src/wit.rs:194-219
```

Operator YAML (`runtime-operator/config/samples/deployment.yaml`):

```yaml
spec: { template: { spec: {
  hostInterfaces:
    - { namespace: wasi, package: http, interfaces: [incoming-handler], config: { host: hello.localhost.direct } }
  components:
    - { name: hello-world, image: ghcr.io/wasmcloud/components/http-hello-world-rust:0.1.0 } } } }
```

How an import is bound:
1. **Host capability.** `hostInterfaces` entries are matched against what the component *imports* and the registered `HostPlugin`s serve (`bind_plugins`, `includes_bidirectional`). The component's own WIT is the source of truth: `ComponentHostPlugin::world` is "derived from the component's exported interfaces" (`plugin/component_host/mod.rs` header), not restated in a manifest. `WitInterface.name` is a routing key: the identifier a multiplexing plugin opens with, an operator binding name, or an intra-workload component name (`wit.rs:209-218`).
2. **Another component in the same workload.** Matched by interface name; a unique exporter wins over a plugin (`workload.rs:3980-4000`); ambiguous exports error only if actually imported (`workload.rs:1404-1413`). A labelled import `(implements "iface") label` routes to the exporter *named* `label` (`workload.rs:3913-3965`, `labelled_exporter`). Instantiation order is a topological sort of those import edges, cycles rejected (`workload.rs:1421-1504, 4211-4264`).
3. **Config.** `wasi:config/store` is a per-component view: workload/interface `config` layered with that component's `LocalResources.config` (`plugin/wasi_config/mod.rs:~38-48`). `LocalResources` is also the egress/volume policy (`allowed_hosts` deny-by-default, `types.rs:121-135`).

So wash glues by **WIT interface names derived from the component binary**, with config and policy as side data. Nothing in wash is an NSID or a consent model; those are ours.

## A3. Lifecycle

- **Unit: the workload** (`WorkloadState` Starting/Running/Completed/Stopping/Error, `types.rs:42-51`; `HostApi::workload_start/stop`, `host/mod.rs:107-152`). Components inside it are instantiated together, in dependency order; a `Service` is a long-lived component with `max_restarts` (`types.rs:55-60`).
- **Instances:** Ephemeral per call by default (so an idle component costs no memory), or `Warm { pool_size, max_invocations, max_concurrency, reclaim }` (`instance_pool.rs:207-228`). Idle shrinkage is `reclaim_window_seconds`/`reclaim_min_instances`: it retires warm instances, never the workload (`instance_pool.rs:232-240`).
- **Not present:** lazy workload start, dependency-driven workload start, scale-to-zero of a workload, idle teardown of a workload. I grepped for them; the only hits are comments. Ordering across workloads is the operator's (Kubernetes).
- Host plugins can hear `wasmcloud:host/workload-lifecycle` bind/unbind (`component_host/mod.rs` header), which is a notification, not a lifecycle manager.

**Consequence.** wash provides the *mechanism* a micro supervisor needs (cheap ephemeral stores, a start/stop unit, an in-workload dependency order) but not the *policy* (demand start, retention, reverse close). That policy stays in Clay's supervisor, as ADR 030 has it. A micro incarnation maps to one wash workload with a Service or single warm instance; `workload_start` is "start incarnation", `workload_stop` is `Closing`.

---

# Part B: the proposal

## B1. Plain answers

**`entrypoints` and why one object.** It names the WIT interfaces the host *drives* on the micro's component: `initialize` (the start hook, mandatory), `presentation` (sync mount/dispatch/unmount), `installer` (reserved, never run). It is an object, not a list, because the roles are a closed set the host defines: each role is a slot with exactly one interface, and unknown slots are carried in `extra`. It is the micro's "world", written as roles. Its WASI-friendliness is that it says "I export *these interfaces*" and lets the host pick the driver from the interface, rather than per-operation names.

**A publication** is a named thing the micro will offer to others once `initialize` has returned: a `call` (request/response bytes typed by an NSID contract, e.g. `org.example.test.counter.increment`) or a `presentation` (a mountable UI component). `initialize` returns publications by name and the host admits what it returns against the declaration (ADR 030 §5). **To a routine:** a publication of kind `call` is exactly a routine: the nodule's `implements: Nsid` is "I serve this contract". Only `presentation` has no nodule analogue.

The micro's other members, from D `canonical_manifest()`:

```jsonc
"configuration_requests": [{            // a config document it wants, with a facet ACL
  "binding": "counter",                 // local name code uses
  "facets": [{ "tag": "org.example.test.counter.config",  // the datum kind (NSID), never the plug that holds it
               "key_id": "count", "read": true, "write": false, "create": false, "delete": false }] }],
"collection_contracts": [{              // an extension point THIS micro defines for others
  "id": "org.example.test.counter.tallies",
  "selector_schema": "org.example.test.counter.tallySelector",   // schema NSIDs: how to query,
  "key_schema": "org.example.test.counter.tallyKey",             // how a row is keyed,
  "value_schema": "org.example.test.counter.tally",              // what a row holds
  "write_context_schema": null,
  "offered": ["read", "register"] }],   // what a bound micro may do: read rows, register rows
"collection_requests": [                // another micro's collection it wants
  { "binding": "tally-book", "contract": "org.example.test.counter.tallies", "access": "write" }],
"contributions": [{                     // a static row it plants in a target's collection at install
  "id": "toolbar-button", "binding": "tally-book",   // lands where its own write request resolves
  "key": "counter",                     // row key in the target's collection
  "payload": { "slot": "toolbar", "order": 2, "label": { "en": "Counter" }, "enabled": true },  // target-owned vocabulary, opaque to host
  "attachments": [{ "index": 0, "publication": "view" }] }]  // row slot 0 becomes a lazy endpoint to this micro's "view"
```

## B2. One entrypoint shape, WASI-first

Three descriptions of the same fact today:

| Side | Says | Host knows the driver from |
|---|---|---|
| Micro | `entrypoints.{initialize,presentation}` = WIT interface per role; publications by name | the role |
| Nodule | `routines[k].impl = componentExport{component, export}` + `implements` | `export` (in practice always `handle`; D/S `big_ndk/wit/world.wit` exports only `wasi:http/handler@0.3.0` and the SDK routes by NSID) |
| wash | components export/import interfaces; host matches by interface name | the interface name, derived from the binary |

The nodule's `export` is a component-export name already (an interface or a function), so it needs no new field; it just never named the real thing. The micro's roles are really "which interface drives me".

**Proposed single shape.** A component's WIT surface is **derived from its binary at admission** (wash does the same; the manifest restates nothing). The manifest then declares **provisions**: named, contract-typed things, each reached **through one export name**, where the *interface* selects the host driver (precedent: wash `HostPlugin` interface matching; `wasi:http/handler` is already how nodules are served).

```text
Provision { component: key in `components`,
            export: component export name, e.g. "wasi:http/handler@0.3.0"
                    or "townframe:dayframe/presentation-events@0.4.0",
            implements: Option<Nsid>,   // the contract served; absent where the driver fixes it (presentation)
            uses: { permbindKey: Use } }
```

Driver table (host knowledge, not manifest data): `wasi:http/handler` = XRPC call; `townframe:dayframe/presentation-events` = mount/dispatch/unmount; `townframe:dayframe/micro-lifecycle` = start hook that publishes named calls. `kind` stops being a stored field: it is a function of `export`, so a new kind arrives with a new WIT interface, not a manifest vocabulary change (the forward-compat the micro manifest wanted from `PublicationKind::Other`).

Caveat I must be honest about: today a micro `call` is not a wasm export. It is a handle the guest registers during `initialize` and callers reach through `capability-ref.call(list<u8>)` (D `clay_guest_session/wit/capabilities.wit`). So a micro call provision's `export` is the interface that publishes it (`micro-lifecycle`), bound late by name. The cleaner end state, which I recommend as a follow-up not a blocker, is for micro calls to be served by `wasi:http/handler` exactly as nodule routines are: `capability-ref.call` becomes an in-process XRPC request, and "call" is one thing on both sides. That also makes outgoing-call attribution (A1) identical for both.

**What changes for nodules:** `nodule.routines` becomes the cartridge-level `provides` map (same fields minus nothing: `impl.componentExport{component, export}` flattens to `component` + `export`; `implements` and `uses` unchanged). `export` becomes validated against the real component exports at admission and should say `wasi:http/handler@0.3.0` instead of `handle`. Which runtime serves a provision is derived: its `component` is the micro's component or it is the nodule's.

## B3. A better noun than "publication"

| Candidate | Precedent | Fit |
|---|---|---|
| `provides` (item: a *provision*, prose: "a provided contract") | Debian/RPM `Provides:`, OSGi `Provide-Capability`, ADR 030 §11's own earlier draft (`provides: [ProvidedContract]`); mirrors `requires` | Names what is true: this component *implements an NSID contract*. Matches routines' `implements`. |
| `exposes` | Fuchsia CML `expose`/`offer`/`use` | Good verb, but Fuchsia's `expose` is routing to a parent, which is not our meaning. |
| `exports` | WIT/component model | Collides with the component's literal export (`Provision.export` is that). |
| `service` | Android `<service exported>`, systemd unit | Collides with wash `Service` and `service_core`. |

**Recommend `provides`.** Micro publications and nodule routines **should be the same concept**; `call` vs `presentation` is determined by the export interface (B2), not a stored kind. The consuming side is then symmetric: a cartridge that *uses* a provision of someone else's declares a permbind for it (B4).

## B4. The unified manifest

Types, in `big_cartridges_core` (changes marked; everything else as on S):

```rust
pub struct Cartridge {
    pub id: CartridgeId, pub version: Version, pub authority: Option<String>, // identity: its only home
    pub lexicons: BTreeMap<Nsid, DatumPin>,   // now also holds collection contracts (def `type: "collection"`)
    pub sidecars: BTreeMap<Nsid, DatumPin>,
    pub components: BTreeMap<String, DatumPin>,           // wasm bytes; WIT surface derived at admission
    pub permbinds: BTreeMap<String, Permbind>,            // the one list of what the cartridge reaches
    pub settings: BTreeSet<Nsid>,
    pub provides: BTreeMap<String, Provision>,            // NEW: replaces nodule.routines AND micro publications
    pub contributions: BTreeMap<String, Contribution>,    // NEW at this level: replaces micro contributions
    pub nodule: Option<Nodule>,                           // routines removed: revisioned_stores, local_state, initialization
    pub micro: Option<Micro>,                             // typed, replaces MicroContribution{schema, description}
    /* name, icons, dependencies, signature, extra as today */
}

pub struct Provision {
    pub component: String,            // key in `components`
    pub export: String,               // component export name: WIT interface (or function); selects the host driver
    pub implements: Option<Nsid>,     // contract served; None only where the driver fixes it
    pub uses: BTreeMap<String, Use>,  // this provision's ACL over permbinds (was RoutineUse)
}
pub struct Use { pub read: bool, pub write: bool, pub create: bool, pub delete: bool }  // renamed from RoutineUse

pub enum Permbind {                   // kinds are named by what you do
    Datum(PermbindDatum),             // read/write/create/delete a datum kind or a collection contract; per-access necessity
    Method(PermbindMethod),           // call another provision (XRPC contract); necessity, minRevision
    Mount(PermbindMount),             // NEW: embed another provision's presentation; necessity, minRevision
    Contribute(PermbindContribute),   // NEW: add rows to another cartridge's collection contract, own rows only
    Drawers(PermbindDrawers),         // `resolution` removed (ADR 049 item 4)
    Net(PermbindNet),
}
pub struct PermbindMount      { pub nsid: Nsid, pub description: String, pub necessity: Necessity, pub min_revision: Option<u32> }
pub struct PermbindContribute { pub nsid: Nsid /* the target's collection contract */, pub description: String, pub necessity: Necessity }

pub struct Contribution {             // a static row; authority comes from the #contribute permbind, not from `uses`
    pub into: String,                 // key of a #contribute permbind
    pub key: String,                  // row key in the target collection; stored under (this cartridge, key), so it can never replace another's row
    pub payload: serde_json::Value,   // target-owned vocabulary; validated against the contract's value schema at admission
    pub attachments: Vec<Attachment>, // lazy endpoints: slot -> one of this cartridge's own `provides`
}
pub struct Attachment { pub index: u32, pub provides: String }

pub struct Micro {
    pub component: String,            // the micro's component
    pub initialize: String,           // export name of the start hook; absent-hook micros do not exist
    pub activation: Activation,       // default enablement hint: Lazy (default) | Eager; open vocabulary
}
```

`Nodule` keeps `initialization` (a revision seed, unrelated to the micro hook; keep the two names apart as on S). `Micro.activation` is a default the enablement service copies into `ConfiguredMicro.eager` (ADR 030 §§1, 15: configuration owns the live value).

Example: one cartridge with a nodule and a micro.

```jsonc
{
  "$type": "org.example.daybook.cartridge",
  "id": "did:plc:abc/tallies", "version": "1.0.0", "authority": "org.example.tallies",
  "name": "Tallies",
  "lexicons": {
    "org.example.tallies.record":  { /* #datumPin: procedure */ },
    "org.example.tallies.increment": { /* #datumPin: procedure */ },
    "org.example.tallies.tally":   { /* #datumPin: record shape */ },
    "org.example.tallies.tallies": { /* #datumPin: { "type": "collection", "selector": "...", "key": "...", "value": "org.example.tallies.tally", "offered": ["read", "register"] } */ }
  },
  "components": { "handler": { /* #datumPin */ }, "ui": { /* #datumPin */ } },
  "settings": ["org.example.tallies.config"],

  "permbinds": {
    "tallies":      { "$type": "…#datum", "nsid": "org.example.tallies.tallies", "description": "Your tallies",
                      "read": "required", "write": "required", "create": "optional" },     // per-access necessity
    "counterConfig":{ "$type": "…#datum", "nsid": "org.example.tallies.config", "description": "Counter settings",
                      "read": "required", "write": "optional" },
    "rates":        { "$type": "…#net", "host": "api.example.com", "methods": ["GET"], "description": "Fetch rates" },
    "shelfAdd":     { "$type": "…#method", "nsid": "org.example.shelf.add", "description": "File tallies on the shelf",
                      "necessity": "optional", "minRevision": 2 },                          // micro-to-micro call
    "shelfView":    { "$type": "…#mount", "nsid": "org.example.shelf.view", "description": "Show your shelf inside Tallies",
                      "necessity": "optional" },                                            // micro-to-micro presentation
    "toolbar":      { "$type": "…#contribute", "nsid": "org.example.toolbar.items",
                      "description": "Adds a Tallies button to your toolbar", "necessity": "optional" }
  },

  "provides": {
    "record": {                                       // was a nodule routine
      "component": "handler", "export": "wasi:http/handler@0.3.0",
      "implements": "org.example.tallies.record",
      "uses": { "tallies": { "read": true, "write": true, "create": true }, "rates": {} } },
    "increment": {                                    // was a micro `call` publication
      "component": "ui", "export": "townframe:dayframe/micro-lifecycle@0.4.0",
      "implements": "org.example.tallies.increment",
      "uses": { "tallies": { "write": true }, "shelfAdd": {} } },
    "view": {                                         // was a micro `presentation` publication
      "component": "ui", "export": "townframe:dayframe/presentation-events@0.4.0",
      "uses": { "tallies": { "read": true }, "counterConfig": { "read": true }, "shelfView": {} } }
  },

  "contributions": {
    "toolbarButton": {
      "into": "toolbar", "key": "tallies",
      "payload": { "slot": "toolbar", "order": 2, "label": { "en": "Tallies" }, "enabled": true },
      "attachments": [{ "index": 0, "provides": "view" }]
    }
  },

  "nodule": { "initialization": [{ "key": "seed", "target": "org.example.tallies.reindex", "mode": "once" }] },
  "micro":  { "component": "ui", "initialize": "townframe:dayframe/micro-lifecycle@0.4.0", "activation": "lazy" }
}
```

How the old micro members landed:

| Old | Now |
|---|---|
| `entrypoints.initialize` | `micro.initialize` |
| `entrypoints.presentation` | the `export` of each presentation provision |
| `entrypoints.installer` | dropped from the typed shape (never executed); carried through `extra` |
| `publications[]` | `provides` |
| `requires[]` (no `access`) | `#method` / `#mount` permbinds + `uses`; no `binding`, supplier chosen by routes and the lockfile, as for nodules |
| `configuration_requests` | `#datum` on a `settings` NSID (facet `key_id` narrowing becomes a `#datum` narrowing) |
| `collection_contracts` | a `type: "collection"` lexicon in `lexicons` under the cartridge's authority |
| `collection_requests` | `#datum` on that NSID (read/write), or `#contribute` for planting rows |
| `contributions[]` | `contributions` at cartridge level |
| `resolution` | gone; runtime requests are a host service (ADR 049) |
| sidecar `id`, `package_version`, `authority` | the cartridge's `id`, `version`, `authority` |

Authority rules: a `#contribute` write is scoped to rows keyed under the contributor's own cartridge (storage key `(contributor, key)`), which closes ADR 030 §13's "any Register-bound micro can supersede any key". A collection contract is *declared* under the author's authority and *used* by others through `#datum`/`#contribute`; consent reads "adds a Tallies button to your toolbar".

**Manifest versus host behaviour (lifecycle).**
- Manifest data: `micro.initialize`; `micro.activation` default; `provides` (what `initialize` may return, checked at admission); each permbind's `necessity` (it decides Requires versus Wants edges); `contributions` and their attachments (the durable reverse edges).
- Host behaviour, not manifest: `enabled` and live `eager` (ConfiguredMicro); retention class of a held resource (declared by the *contract's* metadata, ADR 030 line ~569, and a holder cannot promote it); incarnations and their states; demand activation; closing, reverse order, retry generations; idle grace.

## B5. Lifecycle model

**Confirmed:** the systemd-like design fits wash's mechanisms and the operator's requirements.
- Lazy start, nothing at boot: unit enabled, not started. Lazy endpoint = socket activation. A micro's `activation: eager` is the `WantedBy=default.target` default.
- Dependencies first: a micro's required `#method`/`#mount` permbinds named in its provisions' `uses` are `Requires=` edges; optional ones are `Wants=`. Start order is the topological order wash already computes inside a workload (`workload.rs:4211`), generalized by Clay across incarnations; shutdown is its reverse.
- Reverse edges: `contributions` are `WantedBy=` with data, weak (keep nothing alive).
- Memory: a micro incarnation is one wash workload; `workload_stop` frees it; idle micros are not instantiated at all. Ephemeral-store components (the nodule default) never accumulate.

**Two improvements** (under 300 words total):
1. **Compute the start graph at admission, not at first start.** With `uses`, permbind `necessity` and `dependencies` all in the typed cartridge, the registry can build the required-edge graph and reject a cycle (and a required edge to an NSID no catalogued cartridge provides) when the cartridge is installed. ADR 030 §6 detects cycles only when an activation waits on itself. Trade-off: admission needs a view of the catalog, and an edge that depends on route configuration can still change later, so keep the runtime check as backstop.
2. **Retention is per contract, plus a host idle grace.** Closing the instant the last edge drops thrashes a toolbar that remounts. Add a host-policy idle grace (seconds, plus eviction under memory pressure, as Android caches stopped processes in an LRU and Fuchsia leaves it to the realm) so "responsive" and "no idle memory" are tuned in one place. Trade-off: slightly more resident memory for a quiet window; it is host config, not manifest, so it does not touch this proposal. ADR 030 line ~697 already lists intent prewarm; this is its mirror.

Nodules stay as they are: durable host actor, no incarnations, no reachability close. `initialization` stays a nodule revision seed. I would not move any of the micro lifecycle onto nodules; the one shared piece is that both obtain `provides`, `uses`, and `contributions` from the same cartridge.

## B6. What changes for whom

| Area | Micro side (claytxt's crates: `clay_mdk`, loaders, supervisor) | Nodule side (`big_cartridges_core`, `big_nodule`, `big_ndk`) |
|---|---|---|
| Identity | Delete `Sidecar` id/version/authority; read them from the cartridge | none |
| Manifest type | `MicroManifest` shrinks to `Micro{component, initialize, activation}`; admitted typed, no opaque `description`/`schema` | add `Micro`, `Provision`, `Contribution`, `Attachment`; `MicroContribution` removed |
| Provisions | `publications` becomes `provides`; `PublicationKind` goes (driver from `export`); `validate` moves to admission | `nodule.routines` moves to `provides`; `RoutineImpl::ComponentExport{component, export}` flattens; `export` checked against the real component exports (`wasi:http/handler@0.3.0`) |
| Requests | `requires`/`collection_requests`/`configuration_requests` become permbinds; `binding` and `Request` type removed; suppliers from routes and lockfile; `Context::require` becomes handle minting from `uses` | add `Permbind::Mount`, `Permbind::Contribute`; drop `PermbindDrawers.resolution`; rename `RoutineUse` to `Use` |
| Collections | `collection_contracts` become `collection` lexicons; `CollectionContract` struct goes | new lexicon def type `collection`; admission checks contract schemas against authority |
| Contributions | `ContributionDeclaration` becomes cartridge-level `Contribution`; payload validated against the contract; writes keyed `(contributor, key)` | contributions usable by nodules too (event-router handlers); host owns the registration scope |
| Necessity | per-access on data requests | already so |
| Lifecycle | unchanged; edges computed from `uses` + permbind necessity; `eager` stays config, manifest carries `activation` default only | none; `initialization` unchanged |
| Attribution | `fetch` host service attributes by invocation (A1) | stop requiring `x-nodule-capability` from the guest; host sets it from per-call `Ctx`; one fork change to wash (`Invocation` on `Ctx`, passed to `outgoing_request`) |
| Follow-up | optional: serve micro calls via `wasi:http/handler` so `call` is one thing | optional: SDK stops hard-coding `handle` |

Open questions for the operator: (1) accept `#mount` and `#contribute` as new permbind kinds, or fold mount into `#method`? (consent text differs, so I kept them separate); (2) a `collection` lexicon def type needs the cabinet/lexicon owners' agreement; (3) whether `activation` belongs in the manifest at all, since ADR 030 puts the live value in configuration (I kept it as a default only, as ADR 030 §15 describes "manifest defaults").

## Manager review (nodules, 2026-11-18)

Agreed: `provides` as the one noun for routines and micro publications, with the driver chosen by the export's WIT interface; identity only on the cartridge; `#mount` and `#contribute` as the new permbind kinds (`#method` already covers micro-to-micro calls once they are XRPC contracts, so the earlier `#publication` idea is not needed); contributions at cartridge level, keyed `(contributor, key)`; `resolution` gone; the attribution fix in the wash fork instead of a guest header; the lifecycle kept as ADR 030 has it, with admission-time cycle checks and a host idle grace. Three changes:
1. **No new Lexicon def type.** atproto Lexicon has a fixed set of def types, and inventing `collection` breaks compatibility with every Lexicon tool. A collection's rows stay an ordinary `record` Lexicon; the cartridge declares which records are collections (and what a bound cartridge may do with them) in a cartridge-level `collections` map keyed by NSID.
2. **The permbind key replaces `binding`.** Micro code that names a requirement (`require("shelf")` today) names the permbind key instead, so the local name lives in one place.
3. **Micro calls move to `wasi:http/handler`** as the target, not a follow-up afterthought: a call is then the same export, the same XRPC envelope and the same caller attribution on both sides, which is the WASI-first direction the operator set. Until then a micro call provision's `export` is `micro-lifecycle`, as the proposal says.
