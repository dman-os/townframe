# Cartridge = shared infrastructure + N surfaces (revision of unified-manifest)

Supersedes `unified-manifest.md`. `provides` is withdrawn. Read-only research; nothing built. S = `stack/node-59-adr-d56`, D = `origin/feat/desktop-base`.

## 1. The shape in one paragraph

A cartridge has a **shared part** every surface reads (identity, `lexicons`, `components`, `permbinds`, `settings`, grants) and **surface sections**, each with its own vocabulary, runtime and lifecycle: `nodule`, `micro`, later `editor`, `tiles`. The only thing every surface must do the same way is the **contract with the cartridge**: an entry says what it may reach with `uses` (keys of the cartridge's `permbinds`) and names its wasm as *component key + WIT export name*. That contract is one small shared type, so admission can audit every surface in one pass (every `uses` key exists, every component exists, every export exists in the wasm) without knowing any surface's vocabulary. Unknown surface sections ride in the cartridge's open `extra` members and are carried, not run, by a host that lacks that runtime (the same forward-compatibility rule the micro manifest already has).

## 2. Decisions on the operator's points

**NSIDs name Lexicons only (audit).** What the micro manifest used NSIDs for, and what each becomes:

| Today (NSID-shaped) | Becomes |
|---|---|
| Collection contract id (`org.example.test.counter.tallies`) with selector/key/value schema ids | A **def inside a real Lexicon**, referenced `nsid#def`: `org.example.shell.toolbar#items`. The Lexicon document has a `collection` def whose `key`, `value`, `selector` are sibling defs (`#itemKey`, `#item`, `#itemSelector`), so the schemas are real Lexicon defs too. Precedent: atproto `ref` (`nsid#def`) already names defs, and the `record` def already carries a `key`. |
| Collection **instance** (`BindingGrant.collection`) | `CollectionId`, host-minted, its own type, never in a manifest. Only configuration and grants name one. |
| Runtime kind `org.example.clay.presentationComponent`, `org.example.clay.shell.roots` | Presentation: identified by the **WIT interface** (`townframe:dayframe/presentation-events@0.4.0`), no contract NSID at all. `shell.roots` is a collection contract of the *shell's own cartridge*, so it becomes a real Lexicon def `org.example.shell.roots#collection` that the shell owns. Nothing is "requested but never declared" any more. |
| Host tags (`clay.micro.bindings`, `...cartridges.grant.*`) | A `HostTag` type outside the Lexicon space, or a published Lexicon. Not a manifest concern. |

I chose **def-in-a-Lexicon** over a separate id space for contracts because a contract *is* a schema bundle (key, value, selector), the registry, admission and `minRevision` already work per Lexicon, and it adds no new id space. A separate space is right for instances, which are runtime objects. One new validated type, `LexRef { nsid, def }` (default def `main`), replaces `Nsid` wherever a permbind or request may name a def.

**Micro = one component, one instance.** `micro.component` is a single key; no capability carries a `component`. The micro's `entrypoints` stays exactly as the operator liked it (WIT interface per role).

**WIT `implements` as the target shape for routines (ledger, 2026-11-18).** Component-model `export get-forecast: wasi:http/handler;` (encoded `(implements "wasi:http/handler")`) makes "a routine is this export" literal: one named handler export per routine, `Routine.export` = `"get-forecast"`. Imports are supported in wasm-tools and wasmtime; **exports are spec'd but unconfirmed in wit-bindgen Rust and our wasmtime 48.0.7** (spike pending). So the types do not change: `export` is a component export name either way.
- Target: `export: "get-forecast"`.
- Fallback today: `export: "wasi:http/handler@0.3.0"` (the one handler every nodule has, `big_ndk/wit/world.wit`), with the SDK routing by NSID inside it. Several routines may share that export; admission allows it only while the export is the plain interface.

**Noun for "publication"** (now the micro surface's own word):

| Option | Precedent | For / against |
|---|---|---|
| `capabilities` | Fuchsia (components declare `capabilities`, then `expose`/`offer`); and the micro's own WIT already says `capability-bank`, `capability-ref`, `published-capability` | Zero new vocabulary: the manifest finally matches the guest API. Against: "capability" also names dispatch capability ids on the nodule side, but that is a different surface. |
| `services` | Android bound `Service` (started on first bind, stopped on last unbind: this micro lifecycle) | Matches behaviour. Against: collides with wash `Service` and `service_core`. |
| `endpoints` | "lazy endpoint" already in ADR 031 | Fine for calls, odd for a UI presentation. |

**Recommend `capabilities`.** An entry is "a capability the micro offers", of kind `call` or `presentation`.

## 3. Rust types

Shared part, in `big_cartridges_core` (identity fields are the only home of `id`, `version`, `authority`; nothing in a surface repeats them):

```rust
pub struct Cartridge {
    pub id: CartridgeId, pub version: Version, pub authority: Option<String>,
    pub lexicons: BTreeMap<Nsid, DatumPin>,    // every schema, procedure and collection contract lives here
    pub sidecars: BTreeMap<Nsid, DatumPin>,
    pub components: BTreeMap<String, DatumPin>,// wasm bytes by key; each component's WIT surface is read from the binary at admission
    pub permbinds: BTreeMap<String, Permbind>, // the one list of what the cartridge may reach
    pub settings: BTreeSet<Nsid>,              // config-document Lexicons, read by any surface
    pub nodule: Option<Nodule>,                // surface sections: each its own datum type
    pub micro: Option<Micro>,
    /* name, icons, dependencies, signature */
    #[serde(flatten)] pub extra: Extra,        // editor, tiles, ... carried until a host knows them
}

/// The contract with the cartridge: what every surface entry shares. Flattened into entries.
pub struct Reach {
    pub uses: BTreeMap<String, Use>,           // permbind key -> verbs used (was RoutineUse)
}
pub struct ExportRef { pub component: String, pub export: String } // WASI-first: component + WIT export name

pub struct LexRef { pub nsid: Nsid, pub def: String }  // `nsid#def`, def defaults to "main"

pub enum Permbind {
    Datum(PermbindDatum),         // target: LexRef (a record, settings, or a collection def); per-access necessity
    Method(PermbindMethod),       // call a method Lexicon; necessity, minRevision
    Mount(PermbindMount),         // NEW: embed a micro's presentation; necessity, minRevision. nsid names the capability's contract Lexicon
    Contribute(PermbindContribute), // NEW: add rows to a collection def, own rows only; target: LexRef
    Drawers(PermbindDrawers),     // `resolution` removed (ADR 049 item 4)
    Net(PermbindNet),
}
```

Nodule section, unchanged except where noted (routines stay a map keyed by routine key):

```rust
pub struct Nodule { pub routines: BTreeMap<String, Routine>, pub revisioned_stores, pub local_state, pub initialization: Vec<Initializer> }
pub struct Routine { pub r#impl: ExportRef /* today componentExport{component, export} */, pub implements: Nsid, #[flatten] pub reach: Reach }
```

Micro section (what changes):

```rust
pub struct Micro {
    pub component: String,                      // the one component; the micro is one instance
    pub entrypoints: Entrypoints,               // WIT interface per host-driven role: initialize, presentation, installer (reserved)
    pub activation: Activation,                 // default enablement hint: Lazy | Eager (open); live `eager` is configuration
    pub capabilities: BTreeMap<String, Capability>, // renamed from publications; keyed by name; `initialize` returns these by name
    pub contributions: BTreeMap<String, Contribution>,
}
pub enum CapabilityKind {                       // open vocabulary, unknown kinds carried
    Call { implements: Nsid },                  // request/response; the NSID is a method Lexicon
    Presentation,                               // a mountable UI; no contract NSID, the WIT interface identifies it
}
pub struct Capability { pub kind: CapabilityKind, #[flatten] pub reach: Reach }
pub struct Contribution {
    pub into: String,                           // key of a #contribute permbind (which names the target collection def)
    pub key: String,                            // row key; stored under (this cartridge, key): cannot replace another cartridge's row
    pub payload: serde_json::Value,             // target-owned vocabulary, validated against the collection's value def
    pub attachments: Vec<Attachment>,           // slot index -> a capability of THIS micro, as a lazy endpoint
}
pub struct Attachment { pub index: u32, pub capability: String }
```

Removed from the micro manifest: `requires`, `collection_requests`, `configuration_requests`, `collection_contracts`, `resolution`, and the sidecar identity fields. Their replacements are permbinds and Lexicon defs (table in section 6).

## 4. The Weather cartridge, annotated

A **Weather** cartridge: a nodule that fetches forecasts from a weather service and stores them; a micro that shows them in a panel and adds a button to the shell's toolbar.

```jsonc
{
  "$type": "org.example.daybook.cartridge",
  // IDENTITY, owned only here. The micro and nodule never repeat it.
  "id": "did:plc:weatherco/weather",       // <publisher DID>/<package name>
  "version": "1.0.0",
  "authority": "org.example.weather",      // the NSID prefix this cartridge may DECLARE; everything outside it must be requested

  // SHARED: Lexicons (schemas). Every NSID below names a Lexicon, nothing else.
  "lexicons": {
    "org.example.weather.getForecast": {}, // procedure: input { place }, output { forecast }. The nodule serves it
    "org.example.weather.forecast": {},    // record: one saved forecast
    "org.example.weather.current": {},     // procedure: "what is the weather now?". The micro serves it to other micros
    "org.example.weather.settings": {}     // record: the user's places and units
  },
  "components": {                          // wasm by key; WIT surface read from the binary, not restated here
    "fetcher": {},                         // the nodule's code
    "panel": {}                            // the micro's code
  },
  "settings": ["org.example.weather.settings"], // config documents this cartridge reads/writes; surfaces reach them via a #datum permbind

  // SHARED: the one list of everything this cartridge may reach. Grants are asked per cartridge, once.
  "permbinds": {
    "forecasts": { "$type": "…#datum", "target": "org.example.weather.forecast",
                   "description": "Saved forecasts",
                   "read": "required", "write": "required", "create": "required" }, // necessity is per access: it can read without being allowed to write
    "prefs":     { "$type": "…#datum", "target": "org.example.weather.settings",
                   "description": "Your places and units", "read": "required", "write": "optional" },
    "api":       { "$type": "…#net", "host": "api.weather.example", "methods": ["GET"],
                   "description": "Fetch forecasts" },                           // outgoing HTTP: only this host, only GET
    "refresh":   { "$type": "…#method", "target": "org.example.weather.getForecast", // may call a method Lexicon. The micro calls its own nodule
                   "description": "Refresh forecasts", "necessity": "required" },
    "toolbar":   { "$type": "…#contribute", "target": "org.example.shell.toolbar#items", // a collection def in the SHELL's Lexicon
                   "description": "Adds a Weather button to the toolbar", "necessity": "optional" }
                                           // may add rows to that collection; only rows it owns, never edit anyone else's
  },

  // SURFACE 1: the nodule (durable background actor on the node; no UI).
  "nodule": {
    "routines": {                          // a routine = one operation this nodule serves
      "getForecast": {
        // TARGET shape, once WIT labelled exports are confirmed: one named export per routine
        //   "impl": { "component": "fetcher", "export": "get-forecast" }
        // TODAY: the one handler every nodule exports; the SDK routes by the NSID below
        "impl": { "component": "fetcher", "export": "wasi:http/handler@0.3.0" },
        "implements": "org.example.weather.getForecast", // the Lexicon this routine answers; callers target this, not the routine
        "uses": { "forecasts": { "write": true, "create": true }, // what THIS routine touches: permbind key -> verbs
                  "prefs": { "read": true },
                  "api": {} }                                   // net access has no verbs, listing it is enough
      }
    },
    "initialization": [ { "key": "firstFetch", "target": "org.example.weather.getForecast", "mode": "once" } ]
                                           // on install, call our own routine once. A seed, unrelated to the micro's `initialize`
  },

  // SURFACE 2: the micro (UI part, one component, one instance, started on demand).
  "micro": {
    "component": "panel",                  // the only component; there is exactly one instance
    "entrypoints": {                       // which WIT interfaces the host drives on that component, by role
      "initialize": "townframe:dayframe/micro-lifecycle@0.4.0",       // the start hook; returns the capabilities below
      "presentation": "townframe:dayframe/presentation-events@0.4.0"  // how the host mounts and updates its UI
    },
    "activation": "lazy",                  // default: not started at app launch, only when something asks. Config can force "eager"
    "capabilities": {                      // what this micro offers once started (was "publications"), by name
      "view": {                            // a mountable UI panel; no contract NSID: the presentation interface identifies it
        "kind": "presentation",
        "uses": { "forecasts": { "read": true }, "prefs": { "read": true, "write": true }, "refresh": {} }
      },
      "current": {                         // a request/response other micros may call
        "kind": "call", "implements": "org.example.weather.current",
        "uses": { "forecasts": { "read": true } }
      }
    },
    "contributions": {                     // rows planted in ANOTHER cartridge's collection at install, visible before this micro ever runs
      "toolbarButton": {
        "into": "toolbar",                 // the #contribute permbind above, which fixes the target collection
        "key": "weather",                  // row key; stored under (this cartridge, "weather")
        "payload": { "slot": "toolbar", "order": 2, "label": { "en": "Weather" } },
                                           // the toolbar's own vocabulary; the host does not read it
        "attachments": [ { "index": 0, "capability": "view" } ]
                                           // slot 0 of the row is a lazy link to our "view": clicking starts the micro, nothing runs before
      }
    }
  }
}
```

Start and stop, with this example: pressing the toolbar button redeems the lazy link, which starts the micro. Its `view` uses `refresh` (required, a `#method`), whose provider is the same cartridge's nodule (a durable actor, already ready), so nothing else starts first. Closing the panel drops the last edge and the micro closes in reverse order. Which facts are host behaviour and not manifest data: the live `eager` flag, retention of held links (declared by the contract's Lexicon metadata, not the micro), incarnations, retry and idle policy.

## 5. How a new surface (tiles) slots in

A tile is a simple visual surface portable across Clay implementations: stateless, rendered per request, no instance to keep. It needs a section, a vocabulary, a runtime, and the shared contract; nothing in the shared part changes.

```rust
pub struct Tiles { pub tiles: BTreeMap<String, Tile> }   // datum type with its own authority NSID, like Nodule and Micro
pub struct Tile {
    pub at: ExportRef,                // component + export, the same shared type
    pub sizes: Vec<TileSize>,         // tile-only vocabulary: which grid sizes it renders at
    #[flatten] pub reach: Reach,      // the same `uses` over cartridge permbinds
}
```

```jsonc
"tiles": { "tiles": { "now": {
  "at": { "component": "panel", "export": "townframe:tiles/render@0.1.0" },
  "sizes": ["1x1", "2x1"],
  "uses": { "forecasts": { "read": true } } } } }
```

What the host does: admission runs the shared audit (the `uses` keys exist in `permbinds`, the component exists, the export exists in the wasm); a Clay implementation that has a tile runtime renders it, one that does not carries it unread; the lifecycle is the tile runtime's own (here: an ephemeral store per render, which is wash's default, `instance_pool.rs`, so nothing idles in memory). The micro and nodule sections are untouched, and a tile cannot reach anything its cartridge did not declare as a permbind. Reverse edges (contributions) are not part of the shared contract: tiles get them only if the tile design wants them.

## 6. What changes for micros versus today

| Today (D) | After |
|---|---|
| Sidecar `id`, `package_version`, `authority` | the cartridge's `id`, `version`, `authority`; `Sidecar` goes |
| `MicroManifest` opaque in `MicroContribution{schema, description}` | typed `Micro`, admitted by `big_cartridges_core` |
| `entrypoints{initialize, presentation, installer}` | kept, inside `micro` (`installer` stays reserved) |
| `publications[]`, kind `call`/`presentation`, `contract` | `capabilities` map, `kind` enum; each carries `uses` |
| `requires[]` (+`binding`, supplier from Clay config) | `#method` / `#mount` permbinds named in `uses`; supplier by routes and lockfile, as for nodules |
| `collection_requests`, `configuration_requests` | `#datum` permbinds on `LexRef` (collection def, or a `settings` Lexicon); facet `key_id` narrowing becomes a `#datum` narrowing |
| `collection_contracts` (selector/key/value schema NSIDs) | a `collection` def in a real Lexicon, referenced `nsid#def`; schemas are sibling defs |
| `contributions[]` with `binding` to a request | `contributions` map with `into` a `#contribute` permbind; rows keyed `(contributor, key)` |
| `resolution: configured/atRuntime` | removed; runtime grant requests are a host service |
| `org.example.clay.presentationComponent`, `...shell.roots` as NSIDs | WIT interface; shell-owned Lexicon def |
| `BindingGrant.collection` typed `Nsid` | `CollectionId` |
| One `uses` ACL per micro | `uses` per capability |

Nodule side: nothing changes today; the target is `export: "get-forecast"` once the labelled-export spike passes.

Open points: (1) `LexRef` in `#datum` changes its field from `nsid: Nsid` to `target: LexRef` (a breaking change to S fixtures; I would do it, not shim); (2) the `collection` def type needs the Lexicon owners' agreement; (3) `#mount` stays a separate permbind from `#method` only because its consent text differs ("shows Y's UI inside this one").

## Manager review (nodules, 2026-11-18)

Accepted: shared infrastructure plus surface sections, `Reach { uses }` and `ExportRef` as the only shared entry types, identity only on the cartridge, `LexRef` (`nsid#def`) wherever a permbind targets something inside a Lexicon, `CollectionId` for instances, WIT interfaces instead of runtime-kind NSIDs, the tiles sketch as proof of extensibility. Two corrections:

1. **No `collection` def type.** atproto Lexicon's def types are fixed (`record`, `query`, `procedure`, `subscription`, `object`, `token`, primitives); inventing one breaks every Lexicon tool. The standard tool for naming a concept inside a Lexicon is a **`token`** def: a named identifier with no data. So `org.example.shell.toolbar#items` is a `token` in the shell's Lexicon, the row key and value are ordinary `object` defs beside it (`#itemKey`, `#item`), and the contract's wiring lives in the owning cartridge's micro section, which is ours to define:
   ```jsonc
   "micro": { "collections": {
     "org.example.shell.toolbar#items": {              // the token names the contract
       "key": "org.example.shell.toolbar#itemKey",     // object defs, real Lexicon schemas
       "value": "org.example.shell.toolbar#item",
       "offered": ["read", "register"] } } }
   ```
2. **Not `capabilities`.** In this system "capability" already means an authority handle (`CapId`, capability ids across the node and ADR 037), so a micro's "capabilities" would read as its permissions. Recommend **`endpoints`**: the micro side already calls the lazy link to one of these a "lazy endpoint" (ADR 031 §7), contributions attach endpoints, and it names what other parts call or mount without implying authority. Alternatives: `offers` (Fuchsia `offer`, close to its meaning), `services` (collides with node services).

## Operator review, second pass (2026-11-18)

- **Surfaces are a list, not optional fields.** `nodule: Option<…>` / `micro: Option<…>` is out. The cartridge carries a list of surface entries, an open union discriminated by `$type` (the earlier "contributions" list shape: `[{ "$type": "…#nodule", … }, { "$type": "…#micro", … }]`). A new surface is a new member, carried unread by hosts that don't know it; admission enforces per-kind counts (at most one nodule today).
- **Several micros per cartridge are worth allowing.** A micro is a singleton with its own lifecycle, so one cartridge may want a heavy micro (a chat with lots of loaded state) and a light one (a toolbar button with a live unread badge) that start and stop independently, instead of one micro whose lifecycle keeps everything resident. A purely static button needs no micro at all: a contribution row with a lazy link starts the heavy micro only when clicked. Sharing state between a cartridge's micros goes through the node (datums, the cartridge's nodule), not shared memory; the MDK may offer conveniences above that.
- **Identifiers: substitution decides.** Names stay NSIDs only where something else could provide the same thing (methods, and contracts others implement), because that is what NSIDs plus routes buy: substitution. Things a specific cartridge owns (a collection instance, an endpoint, a host tag, a surface entry) are named hierarchically under the cartridge's id, `<cartridge id>/<kind>/<name>`, needing no global namespace. Direction to settle per item from the NSID audit.
- **WIT `implements`:** research continues on the Rust toolchain (wit-bindgen, wasmtime) for labelled exports, since one labelled `wasi:http/handler` export per routine would replace path routing inside one handler.
