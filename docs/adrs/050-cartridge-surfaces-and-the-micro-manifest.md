# ADR 050: Cartridge surfaces and the micro manifest

**Status:** Accepted (operator, 2026-11-18; ledger D57 and D58, "keep it as it is and improve it as we go"). The endpoint
vocabulary and the requirement resolver are built (claytxt, `feat/daytxt`); the surfaces list, `uses` on micros,
`initialization`, collections as Lexicon tokens and several micros per cartridge are not. Each section says which.

**Supersedes in part:** ADR 030 §§10-14 (the installer, the manifest and its member vocabulary) and ADR 031 §§3, 5 and
7 (collection contracts, publications, the stored link). **Amends:** ADR 047 §2 (contributions are a list of surfaces,
not optional fields) and ADR 049 §3 (`uses` sits on the micro, as 049 already says, and never on an endpoint).

Related: ADR 022 (actors and grants), ADR 027 (cartridges and nodules), ADR 028 (identity, the request shape),
ADR 030 and 031 (micro lifecycle, collections and lazy links, whose runtime model is unchanged), ADR 044
(configuration documents), ADR 045 (method routes), ADR 047 (the cartridge manifest), ADR 049 (one permission model).

Where the built code and a scratch draft disagree, this ADR follows the code and D58. The drafts put `uses` on each
endpoint and had one `#call`; the code and D58 have `uses` on the micro and two endpoint kinds.

## Context

Until now a cartridge was a nodule with an optional micro bolted on: ADR 047 gives the micro one sentence ("designed by
Clay's lane") and ADR 030/031 describe a standalone micro package whose manifest travels in the catalog as an opaque
`description`. Three things made that untenable.

1. **Two private vocabularies for one authority.** The micro manifest had its own request list (`requires`,
   `collection_requests`, `configuration_requests`) beside the cartridge's permbinds. A micro's `requires` entry said
   three things at once: may call Y (authority), Y is supplied by X (composition) and start Y first (lifecycle).
   Authority is now granted per cartridge (ADR 049), so it could not stay in a second list.
2. **Several words doing several jobs.** "Capability" meant both an authority handle (`CapId`) and a lazy link. "Contract"
   meant a collection shape, a call's request/response pair and a requirement. `publications`, `Request` and `facet` were
   flagged by the operator as the worst names in the manifest.
3. **Wasm memory.** A wasm instance's linear memory never shrinks, so only ending an instance returns memory. Laziness
   inside one singleton saves startup, not memory once the code has used it. A cartridge with a small always-present part
   (a toolbar badge) and a heavy part (a chat view) must be able to start and stop them separately.

## Decision

### 1. A cartridge is shared infrastructure plus a list of surfaces

**Shared by every surface and owned only by the cartridge:** identity (`id`, `version`, `authority`), `lexicons`,
`components`, `permbinds`, `settings`, and the grants made against them (ADR 047 §2, ADR 049 §4). Nothing a surface
declares repeats these, so a micro manifest no longer carries an `id`, `package_version` or `authority` of its own (the
sidecar form that did so was a stand-in and goes away).

**Surfaces are a list, not optional fields.** The cartridge carries `surfaces: [{ "$type": "<cartridge>#nodule", ... },
{ "$type": "<cartridge>#micro", ... }]`, an open union discriminated by `$type`. The same forward-compatibility rule as
everywhere in the manifest applies: a host that does not know a `$type` (a later editor, tiles) carries the member
unread and never runs it, so a new surface is a new member and never a change to the shared part. A known `$type` with
malformed members is an error, never "unknown". Admission checks what is shared across surfaces in one pass without
knowing any surface's vocabulary: every `uses` key names a permbind, every component key exists, every named export
exists in the wasm. It also enforces **per-kind counts**: at most one nodule (ADR 047 §2), any number of micros.

**Several micros per cartridge.** The reason is the memory one above, so the split follows lifecycle lines: a live
presence that must keep counting (a badge) versus a heavy view loaded on demand. Rules:

- A micro has a `name`, and its id is `<cartridge id>/micro/<name>`; it is a singleton with its own incarnations, state
  and idle shutdown (ADR 030), independent of its siblings.
- There is **no special same-cartridge micro API**. Two micros of one cartridge share data the way any two micros do:
  through the node (datums, collections, the cartridge's nodule). Shared memory between them would defeat the point.
- A purely static button needs no micro at all: a contribution row whose link points at the heavy micro starts it only on
  click (§3).

Not built yet: the `surfaces` list and several micros; the code holds one micro per package.

### 2. Identifiers: NSIDs only for what others name without knowing the provider

An NSID names a Lexicon, always (the NSID audit found collection contracts, runtime kinds and host tags sharing the
space, which collides with atproto's meaning). The rule that replaces it is **substitution**: a thing gets an NSID only
if a different cartridge could provide the same thing, because NSIDs plus routes (ADR 045) are the mechanism of
substitution.

| What | Named by | Why |
|---|---|---|
| A method (nodule routine or micro method endpoint) | NSID of its procedure/query Lexicon | callers target the operation, never the provider |
| A collection definition | `nsid#def` of a `token` def in the owner's Lexicon, with the row key and value as sibling `object` defs | other cartridges write into it without knowing who owns it |
| The contract a callback or view implements | NSID of a Lexicon | many providers implement the same one |
| A permbind, endpoint, initializer, micro or component | a local key in the manifest | only this manifest refers to it |
| Micro, endpoint, row owner | ids the host forms under the cartridge: `<cartridge id>/micro/<name>`, `<micro id>/endpoint/<name>` | owned things need no global namespace |
| Runtime ids (collection instance, incarnation, `CapId`) | the host, never authored | runtime objects are not names |

A collection definition is a `token` def because atproto Lexicon's def types are fixed (`record`, `query`, `procedure`,
`subscription`, `object`, `token`, primitives); inventing a `collection` type would break every Lexicon tool, while a
`token` is the standard way to give a concept a name inside a Lexicon. The wiring (key def, value def, `verbs`) lives in
the owning micro's `collections` map, keyed by the token, which is ours to define. A reference to a def is a `LexRef
{ nsid, def }`, `def` defaulting to `main`. The collection **instance** a binding resolves to is a host-minted
`CollectionId`, a different type that no manifest names. Presentation components are identified by their WIT interface,
so no NSID stands for "presentation". Collections stay in the micro section until a nodule needs one.

### 3. The micro surface

A micro is one component, one instance per incarnation:

```text
Micro { name, component, entrypoints, activation, endpoints, uses, collections,
        contributes, initialization }
```

**Component and entrypoints.** `component` is a key into the cartridge's `components`. `entrypoints` names the WIT
interface the host drives for each role: `initialize` (the one start hook, which returns the served endpoints by name),
`presentation` (mount and events) and `installer` (§5). `installer` was reserved by ADR 030 §10 and never executed; it is
now real, and lives in **its own WIT world** so that adding the export does not break every guest already built against
the `micro-client` world.

**Activation.** `activation` is an open string, `lazy` (the default) or `eager`, a hint about default enablement
(VS Code's `activationEvents`, with the 1.74 simplification that activation derives from the contribution, which is our
lazy link). Whether a micro is live-eager on this machine is configuration and stays out of the manifest.

**Endpoints.** What a micro serves is a map `name -> endpoint`, an open `$type` union (`publications` is retired:
the word suggested publish/subscribe, and "capability" collides with the authority sense). Hosts carry an unknown member
verbatim and never serve it. The members, as built in `clay_mdk::manifest`:

- **`#methodEndpoint { nsid }`**: a request/response operation **registered and routed by NSID** to its one provider, the
  same word as a nodule's method. Because it is registered, a micro and a nodule supply each other under one rule (ADR
  045: one claimant is the provider, several are `Ambiguous` until a node route picks one).
- **`#callbackEndpoint { implements }`**: a request/response operation that is **never registered**. It is reachable only
  through a link someone holds, and `implements` (required) names the Lexicon contract of its request and response. It
  exists because many micros implement one contract: every toolbar action implements the toolbar's action contract, and
  the row's link, not the NSID, picks the implementation. Registering it would make every pair of toolbar contributors
  `Ambiguous`. A callback therefore cannot also be a method. It is addressed by its endpoint-ref (cartridge, micro,
  endpoint name), a link is data (storable and passable while the provider is stopped), invoking one activates the
  provider and holds it for the call, and reading its status never activates anything. A runtime closure a live
  incarnation registers as a callback is revoked when the incarnation closes.
- **`#viewEndpoint { implements? }`**: a mountable presentation. Every view has the one presentation-component contract,
  so `implements` only names the contract another micro requires it by, and a view nobody requires omits it. A view
  resolves through the same route table as a method but through a **Clay-side resolver, never dispatch**: a mount is not
  request/response. It needs redeem-and-hold while mounted, a status with a blocked reason readable without activating,
  and `revoked` followed by re-redeem (the capability-bank semantics of ADR 031 §6), which dispatch's Ready/NotReady
  provider contract lacks.

The nodule-side permbind is also called `#method`; in the cartridge Lexicon the endpoint defs are `#methodEndpoint`,
`#callbackEndpoint` and `#viewEndpoint` so the two unions do not collide. In the code the full `$type` is
`org.example.clay.micro.manifest#methodEndpoint` and so on; the JSON reads the same.

**`uses`, once per micro.** A micro's authority is the `uses` map on the micro, over the cartridge's permbinds, exactly
the routine rule of ADR 047 §3 and the asymmetry list of ADR 049 §3: one handle is minted per incarnation and shared by
every mount of it, so a per-endpoint ACL could not be enforced and the runtime authority is the union anyway. Endpoints
carry only their contract. An author whose endpoints need very different authority splits the micro, which is the other
reason several micros are allowed.

**Collections** a micro defines for others to fill are the `collections` map of §2: `nsid#def` of the token ->
`{ key, value, selector?, writeContext?, verbs }`. `verbs` replaces `offered` and is an open string vocabulary
(`read`, `register`, `causalWrite`, `domainCommands`), which frees "capability" for authority handles.

**Contributions** are the rows a micro plants in another cartridge's collection (a toolbar button, a menu item). A row is
`key -> { value, links }`: `value` is opaque to the host and validated against the target's value def; `links` maps a
slot index in the value, written `{"$link": n}`, to one of this micro's own endpoints, stored as a lazy link (ADR 031
§7). Both forms of declaring a row exist and write the same rows:

- **Static `contributes`**, first-class, keyed by the `#contribute` permbind and then the row key (VS Code's shape,
  keyed by contribution point). Static rows are visible at consent and previewable without linking any micro code, and
  the composition re-evaluates them at every boot after the target resolves, so a row whose target appears later is not
  lost. The code reads them with `declared_rows` and tests each manifest crate's rows.
- **`initialization`**, which runs the installer (§5) and so can write computed or many rows (one per saved place, one per
  locale).

A row has one of **three lifetimes**, all authorized by the same `#contribute` permbind: *installation* (written at
install or upgrade, retracted at uninstall), *durable at runtime* (written by a running micro, survives its close), and
*incarnation* (written by a live incarnation, retracted by RAII when it closes). A badge value is the third kind and
never lives in a static row (ADR 031 §13).

**No authority over contributions yet.** The operator's decision (D58): any micro may write any row, and there is no
ownership rule around contributions. "Own rows only", proposed earlier, is withdrawn: it was a key-ownership write policy
inside collections, and collections are plain maps whose write authority arrives later as query-scoped grants. Per-contributor
**registration scopes stay** (ADR 031 §4) but only as bookkeeping (who wrote what, so a retraction is atomic); they are
not a write rule. The manifest keeps room for the better design.

### 4. `requires` becomes one declaration: a permbind, plus `uses`

A micro's `requires` carried three concerns. The author now writes **one** thing, a permbind with a necessity (a
`#method` or `#view`-shaped permbind on the supplied NSID) named in the micro's `uses`, and each concern is derived:

| Concern | Where it comes from |
|---|---|
| **Authority** | the permbind exists (consent text, per-cartridge grant) and `uses` names it, ADR 049 |
| **Supply** | one resolution rule, below |
| **Lifecycle** | from the permbind's necessity, below |

**Supply: one rule, two transports.** A request names the NSID of what it wants and never a supplier. The supplier is
whichever installed endpoint serves it, resolved as built in `daytxt_core::requirements`:

- a method is found by its `nsid`; a view by its `implements`; a callback **never** resolves by NSID;
- exactly one provider supplies it; none leaves the requirement **unsupplied** (not an install error: the micro installs
  and its start is blocked); several is an install **error** naming each candidate and supplying none, because choosing
  silently would let installation order decide, and choosing is a route (ADR 045);
- a micro never supplies its own requirement, since the supervisor would wait on itself at start.

The rule picks the provider; two transports then reach it. A supplier that is a micro answered **inside Clay** takes a
Clay-local fast path (the supervisor binds the supplier's endpoint of that name), because Clay's supervisor is
single-threaded and dispatch providers are `Send + Sync`: bridging needs a channel hop and a held lease, or cycle
detection and strong edges are lost, and in-process micros have no node transport. A supplier that is a **nodule or
remote** goes through dispatch (ADR 045). Both are one table of routes. Today only the Clay-local transport is built;
dispatch as a supplier for micros is D57 step 9. A client-scoped endpoint (the shell's per-window `navigate`) needs a session scope
to say which client answers; that is open.

**Lifecycle, from necessity.** A required requirement means the supplier is started first and held (a strong edge) while
the micro lives. An optional one starts the micro without it, unbound. A required requirement with no supplier blocks the
micro (`Unconfigured`, not a fault). An unrecognized necessity is read as required, since blocking is the conservative
reading of a newer format. Because an incarnation serves every endpoint, the lifecycle takes the union of the micro's
required permbinds.

### 5. One `Initializer`, one `RunMode`, one input record

Nodule `initialization` (ADR 047 §3) and the micro's become the same noun:

```text
Initializer { key, mode }          RunMode = once | everyStart | everyUpgrade
input   = { key, trigger: install | upgrade | start | target-changed, previous, current }
```

`everyUpgrade` runs at install and at each version change; `previous` and `current` are the versions, which closes the
nodule to-do that initializers received `{}`. A nodule initializer targets one of its own routines (ADR 047 §3); a micro
initializer calls the micro's `installer` export with the record. The installer runs as a short-lived instance, not an
incarnation: no UI, nothing served, and its only reach is the entry's `uses` (normally a `#contribute` handle). Its rows
go to the installation scope as a set, replaced atomically on each run so an upgrade cannot leave stale rows, and
retracted on uninstall.

The **`target-changed` trigger** re-runs an initializer when the collection it contributes to appears or changes. A
once-only installer would lose a contribution whose target resolves later, which is why static `contributes` also stays.
`target-changed` is new vocabulary on the input record's `trigger` and is a known value, not an enum break.

### 6. Built-in micros are synthesized host cartridges

The shell, settings, toolbar, sample and lifecycle micros have no wasm component and no publisher. They are installed as
**synthesized host cartridges under the node's local DID** (`LOCAL_NAMESPACE` is the stand-in today) whose component kind
is a native one (the lifecycle micro needs `CompositionRuntime` from the Dioxus context, which no wasm guest has). This
keeps one identity rule for every micro and lets built-ins hold collection tokens like any cartridge: `org.example.clay.*`
NSIDs belong to a host-cartridge authority and are declared, no longer "only ever requested". ADR 049 item 10 anticipated
it ("host services and cartridges keep isomorphic shapes").

### 7. Where the manifest types live, and the catalog

`big_cartridges_core` builds native only, but the per-micro manifest crates must build for `wasm32` and depend only on the
micro SDK. The manifest types (surfaces, micro, endpoints, `LexRef`, `Initializer`) therefore live in **a small neutral,
wasm-buildable crate** that both depend on; today they are `clay_mdk::manifest`, which compiles without backend features.
**This reverses the catalog's opaque micro `description`**: `big_cartridges_core` documented that it "does not depend on
Clay's crates" and carried the micro manifest as an uninterpreted JSON object. The cartridge now carries typed surfaces
and admission audits them. The reversal is deliberate: the shared audit of §1 and a consent screen that names what a micro
adds both need the typed shape.

### 8. Vocabulary: old to new

| Old | New | Notes and precedent |
|---|---|---|
| `publications` | `endpoints`, a `name -> endpoint` map | ADR 031 already said "lazy endpoint"; no authority sense. Rejected: `offers`, `services`, `commands` |
| `kind: call \| presentation` | `$type`: `#methodEndpoint \| #callbackEndpoint \| #viewEndpoint` | members differ in data, so a union; `view` is VS Code's `views` |
| `requires`, `Request` | a permbind plus a `uses` key | §4; `binding` disappears as a second local name |
| `resolution: configured \| atRuntime` | removed | ADR 049 item 4: runtime requests are a host service |
| `facet`, `configuration_requests` | `#datum` permbind on a settings Lexicon | datum is the noun of ADR 049; `key_id` narrowing becomes `key` on the permbind |
| `collection_requests` | `#datum` permbind whose target is a collection token's `LexRef` | one permbind list |
| `collection_contracts` | `collections` map keyed by the token | VS Code's `viewsContainers` pattern: an id others key by |
| "contract" | retired as a word | method Lexicon (`implements`), collection definition, `LexRef` |
| `offered` | `verbs` | frees "capability" |
| `contributions[]` with `binding` | `contributes`, keyed by permbind key then row key | VS Code's `contributes`, keyed by contribution point |
| `payload` | `value` | built; it is validated against the target's `value` def |
| `attachments{index, publication}` | `links{index, endpoint}`, `{"$link": n}` in the value | built; one slot-to-endpoint link |
| `entrypoints.installer` (reserved) | real, with `initialization` driving it | §5 |
| `capability-ref`, `published-capability`, `capability-bank` (WIT) | `endpoint-ref`, `published-endpoint`, `endpoint-bank` | WIT ABI break, paid once at the 0.5.0 bump |
| sidecar `id`, `package_version`, `authority` | cartridge identity | §1 |

Taken from VS Code: `contributes` as an object keyed by contribution point; contribution and contribution point; `views`;
`activation` derived from the contribution; defining a point by an id others key by. Not taken: `commands` (collides with
cabinet commands) and `extensionDependencies` (our `dependencies` are delivery hints only; suppliers are routes).

### 9. Deferred and open

- **Contribution authority.** Who may write which rows: "authority comes later", with query-scoped grants (§3).
- **Host-side cache with preload hints** (a Clay-host to-do). The host warms a cache between the node and the micros, and
  streams rows into a micro only when it needs them, so a micro's `initialize` can mount queries cheaply without data
  landing in its linear memory. This is a cache, so it waits on the fixes it would hide.
- **Direct micro-to-micro WIT bindings.** Micro-to-micro stays host-mediated today. Direct typed bindings are to be explored.
- **WIT `implements` labelled exports** (one named `wasi:http/handler` per routine). Not usable for Rust guests: wit-bindgen
  has no labelled exports (only Go does, #1657; Rust is the unmerged draft #1551) and wasmtime-wasi-http's `ProxyPre`
  cannot serve one (#14859). Routines keep one `wasi:http/handler` export routed by NSID inside it; the manifest's `export`
  field already holds a label, so adopting it later changes values, not shape.
- **Collections at cartridge level** (a nodule that owns a collection).
- **WIT 0.5.0 renames** of §8, in one bump together with the separate installer world.
- Link-redemption authority (no permbind yet says "redeem links in collection X"), `props` on views, collection backing
  and retention, and TS/Solid micros (manifest-first authoring with codegen).

### 10. Crate names

Per the rename on `feat/daytxt` (`67ac0667`): `dayframe_composition` is `daytxt_core` (it holds the requirement
resolver), `dayframe_native` is `daytxt_native`, `dayframe_{desktop,mobile,web}` are `daytxt_*`, `clay_node` takes the
generic micro hosting of `dayframe_node` plus `clay_node_egress`, and `ui` folds into `clay_dioxus_kit`. `day_node` is
unchanged; environment variables are `DAYTXT_*`.

## Example: the Weather cartridge

A nodule fetches forecasts and a micro shows them. Annotations say what is **built** (on `feat/daytxt`) and what is
**target** (decided here, not yet built).

```jsonc
{
  "$type": "org.example.daybook.cartridge",
  "id": "did:plc:weatherco/weather", "version": "1.0.0",
  "authority": "org.example.weather",   // the NSID prefix this cartridge may DECLARE
  "lexicons": { /* getForecast (procedure), current (procedure), forecast (record),
                   settings (record), alerts (a token #items with #itemKey and #item) */ },
  "components": { "fetcher": {}, "panel": {} },
  "permbinds": {
    "forecasts": { "$type": "…#datum", "target": "org.example.weather.forecast", "read": "required", "write": "required" },
    "api":       { "$type": "…#net", "host": "api.weather.example", "methods": ["GET"] },
    "refresh":   { "$type": "…#method", "target": "org.example.weather.getForecast", "necessity": "required" },
                  // the one declaration of §4: authority, supply and lifecycle all derive from it
    "toolbar":   { "$type": "…#contribute", "target": "org.example.shell.toolbar#items", "necessity": "optional" }
  },
  "surfaces": [                         // target: an open `$type` list; carried unread when unknown
    { "$type": "…#nodule",              // at most one
      "routines": { "getForecast": {
          "impl": { "component": "fetcher", "export": "wasi:http/handler@0.3.0" },
          "implements": "org.example.weather.getForecast",
          "uses": { "forecasts": { "write": true }, "api": {} } } } },
    { "$type": "…#micro",
      "name": "panel",                  // target: id <cartridge>/micro/panel; a "badge" micro would be a second entry
      "component": "panel",
      "entrypoints": {                  // built; `installer` is its own world (target)
        "initialize": "townframe:dayframe/micro-lifecycle@0.4.0",
        "presentation": "townframe:dayframe/presentation-events@0.4.0" },
      "activation": "lazy",
      "uses": { "forecasts": { "read": true }, "refresh": {} },   // target: per micro, over the permbinds
      "endpoints": {                    // built
        "view":    { "$type": "…#viewEndpoint" },                  // mount; nobody requires it, so no `implements`
        "current": { "$type": "…#methodEndpoint", "nsid": "org.example.weather.current" },
                                        // registered, routed by NSID to this one provider
        "open":    { "$type": "…#callbackEndpoint", "implements": "org.example.shell.toolbar.action" }
                                        // never registered: reached only through a link; implements is required
      },
      "contributes": { "toolbar": { "weather": {          // target: permbind key, then row key
          "value": { "label": { "en": "Weather" }, "slot": "toolbar", "order": 2,
                     "action": { "$link": 0 } },          // built: `{"$link": n}` marks the slot
          "links": [ { "index": 0, "endpoint": "open" } ] } } } }  // built: slot 0 is a lazy link to "open"
    }
  ]
}
```

What the host does. At admission: every `uses` key is a permbind, the components and the micro's exports exist, and the
`value` is validated against `org.example.shell.toolbar#item`. Consent shows "adds a button to the toolbar" from the
`toolbar` permbind. The shell draws the button from `value` while the micro is stopped. Pressing it redeems the link,
which starts the micro; `refresh` is a required `#method` whose sole provider is the cartridge's own nodule (a durable
actor, already ready), so nothing starts first and the supplier is held while the micro lives. Closing the last strong
edge closes the micro and leaves the row. In the code today `refresh` would be the `requires` entry
`{ "binding": "refresh", "contract": "org.example.weather.getForecast" }`, and because only the Clay-local transport is
built, a supplier that is a nodule is not yet reachable (it would be reported unsupplied); the supplier-through-dispatch
step closes that.

## Consequences

- One permission model reads one list: a person grants a cartridge, and its micro and nodule cannot ask for different
  things. ADR 049 §9a's risk analysis sees every reach through `uses`.
- A host without a surface runtime round-trips the cartridge intact, since unknown `$type` members are carried unread.
- An author whose endpoints differ in authority or weight must split the micro: the same pressure memory applies.
- The WIT 0.5.0 bump breaks every built guest; collecting the renames into one bump, with the installer as a separate
  world, pays that once.

## Implementation order

Each step ships alone. Done by claytxt: step 1 (the endpoint, `links` and `value` vocabulary in `clay_mdk`, with the manifest
snapshots re-blessed) and step 2 (necessity on `Requirement`; the NSID resolver over installed micros replacing the hard-wired
shell wiring, transport still Clay-local). Remaining: `facet` to `#datum` once the node's per-cartridge configuration
document exists; collections as Lexicon tokens with `#contribute` over three lifetimes; the WIT 0.5.0 bump; the
installer with static `contributes` and the `target-changed` trigger; the `surfaces` list with one micro and cartridge
identity; built-in cartridges; several micros; dispatch as a supplier for micros.
