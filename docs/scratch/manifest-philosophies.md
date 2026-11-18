# Micro manifest vs nodule/cartridge manifest: where the philosophies meet and collide

Sources: `origin/feat/desktop-base` ("DB") and `stack/node-59-adr-d56` ("S"). All ADR paths are `docs/adrs/`. Nothing was built or run; all JSON below is copied from a snapshot test or transcribed, and says which.

Short version: the micro manifest is *not* a different animal at the request level. It already uses the plug request shape (`{binding, contract, access?, necessity, resolution, minRevision?}`, ADR 028 §4, ADR 030 §12) and says "required unresolved means not startable", exactly like permbinds. The collisions are about *where* the declaration lives (a private manifest vs the cartridge's one permbind list), *who is named* (contracts vs concrete providers/collections), and *what is audited* (typed ACL vs opaque payload rows). The lifecycle (incarnations, lazy activation, idle shutdown) is genuinely micro-only.

---

## 1. The two raw manifests

### 1a. Micro manifest (DB)

Source: `canonical_manifest()` in `src/clay_mdk/manifest/tests.rs`, pinned by `src/clay_mdk/manifest/snapshots/clay_mdk__manifest__tests__manifest_canonical_wire_shape.snap`. It is the one real JSON that exercises every member (the small `src/clay_mdk/lifecycle_fixture.manifest.json` has only publications and one optional `requires`). The shipped micros (`src/micro_counter_manifest`, `micro_layout_manifest`, `micro_lab_manifest`) are YAML snapshots of the same shape. On the wire the manifest is flat beside three envelope fields the "sidecar" adds: `id`, `package_version`, `authority` (ADR 030 §11).

```jsonc
{
  "entrypoints": {                         // which WIT worlds the component exports; fixed set, not per-operation
    "initialize": "townframe:dayframe/micro-lifecycle@0.4.0",   // the one start hook; returns published names
    "presentation": "townframe:dayframe/presentation-events@0.4.0", // sync mount/dispatch/unmount (UI)
    "installer": null                       // reserved, never executed (ADR 030 §10)
  },
  "publications": [                         // what this micro can serve to others (named, finite kinds)
    { "name": "view",      "kind": "presentation", "contract": null },  // a mountable component; contract implied
    { "name": "increment", "kind": "call", "contract": "org.example.test.counter.increment" } // request/response op
  ],
  "requires": [                             // what it needs from other micros: a request with no `access`
    { "binding": "shelf",                   // local name code uses: initial-context.require("shelf")
      "contract": "org.example.test.shelf", // NSID of the call/presentation wanted, never a supplier
      "necessity": "optional",              // omitted on the wire = required
      "minRevision": 2 }                    // least Lexicon revision (same as permbind #method)
  ],                                        // (`resolution` omitted = "configured")
  "configuration_requests": [               // config-document facets, per binding, with an r/w/c/d ACL
    { "binding": "counter",
      "facets": [ { "tag": "org.example.test.counter.config", "key_id": "count",
                    "read": true, "write": false, "create": false, "delete": false } ] }
  ],
  "collection_contracts": [                 // extension points THIS micro defines for others (it is the "target")
    { "id": "org.example.test.counter.tallies",
      "selector_schema": "org.example.test.counter.tallySelector",
      "key_schema": "org.example.test.counter.tallyKey",
      "value_schema": "org.example.test.counter.tally",
      "write_context_schema": null,
      "offered": ["read", "register"] }     // what a bound micro may do: read rows / register rows
  ],
  "collection_requests": [                  // collections of OTHER micros it wants to read or write
    { "binding": "tally-book", "contract": "org.example.test.counter.tallies", "access": "write" }
  ],
  "contributions": [                        // static rows it writes into a target's collection at install,
    { "id": "toolbar-button",               // visible before this micro ever runs (the reverse edge)
      "binding": "tally-book",              // names one of its own write collection_requests
      "key": "counter",                     // row key in the target's collection
      "payload": { "slot": "toolbar", "order": 2, "label": { "en": "Counter" }, "enabled": true },
                                            // opaque to the host: the TARGET micro's vocabulary
      "attachments": [ { "index": 0, "publication": "view" } ] }
                                            // lazy endpoint: row slot 0 -> this micro's publication "view"
  ]
}
```

The cartridge envelope carries this inside `MicroContribution { component, schema, description, uses }` (DB `src/big_cartridges_core/lib.rs:367-382`). `component` is the artifact key, `schema` is the manifest-format NSID (`org.example.clay.micro.manifest`), `description` is the manifest above as an opaque JSON object the catalog does not interpret, and `uses` (ADR 049 §3) is the micro's ACL over the cartridge's permbinds. Admission from the envelope is "not built" (ADR 030 Implementation status); the sidecar JSON stands in.

### 1b. Nodule cartridge manifest (S)

There is no JSON rendering or snapshot of a cartridge. `src/big_ndk_testing/smoke.rs:91-200` builds one as Rust literals (routines with `ComponentExport`, `implements`, `uses`, and `Permbind::Method` with `necessity: Optional`). Below is ADR 047 §2's own worked example (the ADR's normative JSON, the faithful shape of `Cartridge` in `big_cartridges_core`), not a test output.

```jsonc
{
  "$type": "org.example.daybook.cartridge",
  "id": "did:plc:…/photos",                 // <namespace DID>/<name>; identity owned by the envelope
  "version": "1.2.0",
  "authority": "org.example.photos",        // the NSID prefix it may DECLARE under
  "name": "Photos",
  "dependencies": { "did:plc:…/blobs": "^1" },  // delivery hints only; never decide who provides an NSID
  "lexicons":   { "org.example.photos.album": { /* #datumPin */ } },   // own + carried copies
  "sidecars":   { "org.example.photos.album": { /* #datumPin */ } },   // how a record is stored in Automerge
  "components": { "main": { /* #datumPin */ } },                       // wasm bytes, OCI-layer-like
  "permbinds": {                            // THE one list of what the cartridge reaches (audit + grant read this)
    "albums":      { "$type": "…#datum", "nsid": "org.example.photos.album", "description": "Your albums",
                     "read": "required", "write": "required", "create": "required" },   // per-access necessity
    "blobsPut":    { "$type": "…#method", "nsid": "org.example.daybook.blobs.put",
                     "description": "Store photo bytes", "necessity": "required", "minRevision": 2 },
    "workDrawers": { "$type": "…#drawers", "description": "Drawers to import photos from",
                     "resolution": "atRuntime", "read": "required", "write": "optional" }
  },
  "settings": ["org.example.photos.settings"],   // config-document datum kinds, shared by nodule and micro
  "nodule": {
    "routines": {
      "createAlbum": {
        "impl": { "$type": "…#componentExport", "component": "main", "export": "create-album" },
        "implements": "org.example.photos.createAlbum",     // the operation NSID served, not the routine key
        "uses": { "albums": { "write": true, "create": true },   // the routine's ACL: keys name permbinds
                  "blobsPut": {}, "workDrawers": { "read": true } }
      }
    },
    "revisionedStores": { },                // ADR 027 §7
    "localState": { },                      // ADR 027 §6
    "initialization": [ { "key": "seed", "target": "org.example.photos.reindex", "mode": "once" } ]
                                            // a LIST: declaration order is the lifecycle contract (ADR 047 §8)
  },
  "micro": { }                              // the other contribution (see 1a): at most one nodule, a micro, ...
}
```

---

## 2. Vocabulary map

| Concept | Micro manifest | Nodule / cartridge | Same? |
|---|---|---|---|
| Request shape | `Request {binding, contract, access?, necessity, resolution, minRevision?}`, used by `requires` and `collection_requests` (ADR 030 §12) | `Permbind` = same request "split by kind" (`#datum`, `#method`, `#drawers`, `#net`), plus `description` (ADR 047 §5) | Same rules, different packaging. Micro has two flat lists; cartridge has one keyed map. |
| Where requests live | Inside the micro's manifest, per micro | Cartridge level, shared by nodule and micro (ADR 047 §2) | **No.** See collision 1. |
| What may be touched, per code unit | Nothing per publication; `MicroContribution.uses` over permbinds (ADR 049 §3) | Per-routine `uses` (ADR 047 §3) | Same idea, but a micro has one ACL for all its publications. |
| Offer / implement | `publications[name, kind, contract]` | `routines[key].implements: Nsid` | Close. Publication name is micro-local; routines also hide their key behind the NSID. Presentation publications have no per-publication NSID; nodules have no equivalent (ADR 049 Q2 asks if it is a `#method`). |
| Depend on a service | `requires` (no `access`): "may call / may mount" | `#method {nsid, necessity, minRevision}` + `uses` | Same meaning. |
| Necessity | `required`/`optional`, per request; default required | `necessity`, or per-access `required`/`optional` for `#datum`/`#drawers` | Same field. Granularity differs (per request vs per access). |
| Resolution | `configured` / `atRuntime`, on every request (ADR 031 §11) | `resolution` only on `#drawers`; ADR 049 review item 4 deletes `atRuntime` entirely in favor of runtime requests | **Diverging.** |
| Provider selection | `requires.binding` resolved by Clay config (`clay.micro.bindings` datum) to a supplier | `#method` resolved by routes (ADR 045) and the lockfile (ADR 047 §7); re-consent on provider change | Same goal (NSID not provider), two mechanisms. |
| Config / settings | `configuration_requests[binding, facets[tag,key_id,r/w/c/d]]` | `settings: [nsid]` + `#datum` permbind + `uses` | Same data, different surface. Facet ACL is the old plug-manifest `RoutineFacetAccess` that ADR 047 says it "restores" for routines. |
| Collections | `collection_contracts` (declare) + `collection_requests` (use) | `#datum` for kinds, `#drawers` for sets; `revisionedStores` (ADR 027 §7) for owned stores | **No clean equivalent.** ADR 049 item 8: collections are "data from cartridge Y", granted via `grant/cartridge/Y`. I did not verify whether revisionedStores can serve as a contract-declaration vehicle. |
| Reverse dependency | `contributions[id, binding, key, payload, attachments]` (durable rows + lazy endpoints) | Commands/processors/initializers target an operation NSID (ADR 027 §2); no "write into a target's data" | **No equivalent.** |
| Entry points | `entrypoints{initialize, presentation, installer}` (WIT worlds) | `impl: componentExport{component, export}` per routine; `components` map | Different: micro has one fixed world; routine picks an export per operation. |
| Identity | sidecar `id`, `package_version`, `authority`, later the envelope's | envelope `id`, `version`, `authority` | Same (ADR 030 §11 says so); micro manifest is opaque `description` in the envelope. |
| Start-time hook | `initialize` (returns publications; staged visibility) | `initialization[{key,target,mode}]` (invokes an own operation on revision R) | Different: micro init is a constructor, nodule init is a migration/seed. |
| Grants | `clay.micro.bindings` datum per micro: binding -> `{collection, contract, access}` (ADR 031 §11) | `grant/drawer|cartridge|net|method|authority/...`, one set per cartridge, in the person's nouns (ADR 049 §4.1) | **No.** |

---

## 3. Lifecycle and dependency model of micros

Think systemd where the unit is a singleton service. This is from ADR 030 §§1-6 and ADR 031 §§5-9.

- **Unit.** A micro is a configured singleton; the micro id (`did:…/dayframe.counter`) is its package, config and runtime identity. At most one live "incarnation" exists, created fresh per successful init (ADR 030 §1). Observable states: `Disabled, Idle, Blocked(missing requirements), Initializing, Active, Closing, Faulted, UnavailableProvider`. (ADR 031 §11 also names `Unconfigured`.)
- **Enabled vs started** is `systemctl enable` vs `start`. Enabled micros are *dormant* until an eager root (`eager` flag, e.g. the shell's `shell.roots`) or demand starts them (ADR 030 intro, §2).
- **Activation on demand** is the socket-activation analogue. A durable row (written at install) carries a *lazy endpoint* `{micro_id, publication_name, contract}` (ADR 031 §7). Reading the row activates nothing. *Redeeming* it, invoking a lazy call, or mounting a lazy presentation does (`Demand = ExplicitRoot | RedeemLazyEndpoint | InvokeLazyService | MountLazyPresentation | RetainLiveBinding`, ADR 030 §4). Concurrent demand joins one pending incarnation.
- **Reverse dependencies (`WantedBy=`).** A contribution is the reverse edge: the *contributor* declares "I belong in your toolbar" into the *target's* collection, so the target need not know contributors and the contributor need not run to announce itself (ADR 030 §3). Stored in an installation-owned registration scope, removed atomically on uninstall (ADR 031 §4). Differences from `WantedBy`: it carries data (payload), it is targeted at a collection, and it is weak: it keeps nothing alive.
- **Forward dependencies (`Requires=` / `Wants=`).** `requires` with `necessity`. Required and unresolved: the micro is `Unconfigured`, not a fault, and neither boot nor a redeem starts it. Optional and unresolved: starts with the binding absent (ADR 031 §11 "Lifecycle"). Resolved required requirements are *strong*: the dependency is kept alive while the dependent lives. Mandatory configured cycles are rejected; activation that would wait on itself fails as a cycle (ADR 030 §6, status).
- **Ordering.** There is no `After=`/`Before=` in the manifest. Start order is implied by strong requirements (a micro's `initialize` can acquire a required capability, which activates the supplier first). Shutdown order is the reverse: dependents close before providers, because the incarnation scope is dropped while its dependencies are still retained (ADR 030 §6). "Spatial" ordering (toolbar slot, order) is *not* host semantics: it is target-owned vocabulary in the payload and in the target's own config, which "may refer to stable registration ids for ordering, hiding, or placement" (ADR 030 §3).
- **Liveness.** A micro stays live only while an eager root or an external *strong* resource retains it (lifecycle classes `NoMicroRetention/Lazy/Weak/Strong/OwnedEffect`, ADR 030 §2). Losing the last edge starts orderly `Closing`: reject new publications, `instance-root.close()` once, drop scope, recompute reachability (ADR 030 §6, ADR 031 §9). Unreachable strongly-connected groups close together.
- **Incarnations and faults.** Staged outward visibility: publications, rows and mounts appear atomically only after `initialize` succeeds; failure unwinds by RAII (ADR 030 §5). A `Faulted` micro is not retried by demand churn, only by a retry generation or relevant config change (ADR 030 §1). Changing a binding restarts the micro; no hot rebinding (ADR 031 §11).

**Do nodules have an equivalent?** Partially.
- *Enabled but not startable* ("needs configuration") for missing required permbinds: same as `Unconfigured` (ADR 047 §5, §11).
- *Initializers* (`initialization`, ADR 047 §3, §8; ADR 027 §4): run an own-cartridge operation on a revision with a run mode, via dispatch "once a provider is ready"; an ordered list is the only temporal ordering in the nodule manifest. Operator decided initializers target only their own cartridge's operations (ADR 047 Amendment A).
- *Readiness*: dispatch has `NotReady {reason}` (retryable) and per-provider `readiness` in the routing table (ADR 027 §3/§8). Retry is the caller's, not a graph walk.
- *Differences:* a nodule has a durable actor and is not torn down for being idle; no incarnations; no reachability-based liveness; no lazy-activation graph (nodule calls dispatch to a provider that is just "ready" or not); no reverse edges; no start order between nodules (cross-nodule dependencies "are delivery hints only", ADR 028 §4). Required methods are checked at start, not woven into a start order.

---

## 4. Collisions

Which side matches ADR 028's plug sensibility: requests by what you use (NSIDs), necessity, resolution, authority by contract not by guest, the manifest as the admitted/audited artifact.

1. **Two declaration homes for "what I reach."**
   - Micro: `requires`, `collection_requests`, `configuration_requests` live in the micro's own manifest; grants are `clay.micro.bindings` per micro.
   - Nodule: one cartridge-level `permbinds` map plus per-routine `uses`; ADR 049 §3 already says a micro "declares `uses` exactly as a routine does" and shares the cartridge's grants.
   - Matches 028/047: the nodule side ("one list ... which is what an audit and a grant read", ADR 047 §2). DB's micro manifest still holds a parallel list and `uses` points into a permbind list the micro does not populate.
   - Unified: micro requests become permbinds; the micro manifest keeps only what is micro-specific and a micro's `uses` names permbinds.

2. **Contributions: audited ACL vs opaque rows.**
   - Micro: a contribution is a data row written into a target's collection; the `payload` is opaque to the host and the write authority is "any micro bound to a collection with Register may write any key" (ADR 030 §13), so a micro can supersede another's row.
   - Nodule: reverse edges do not exist; a consumer calls an operation NSID and routes pick the provider (ADR 027 §2, ADR 045). What a routine may reach is a closed typed list (`uses`).
   - Nodule side matches 028 ("statically answerable whether a cartridge can ever reach a resource"). The contribution edge is not statically audited for authority, only for schema (and schema validation is "not built", ADR 031 status).
   - Unified: a contribution is a `#datum`-shaped write permbind on the target's contract (key-scoped), shown at consent as "this adds to Y's toolbar".

3. **Collections vs datums/drawers.**
   - Micro: a micro declares collection contracts and others request them (read/write); grants name a concrete collection id and access.
   - Nodule: `#datum`/`#drawers`; ADR 049 item 8 withdraws "Clay-only collections" in favor of "data from cartridge Y" via `grant/cartridge/Y`.
   - ADR 049 matches 028's one-answer-per-question and audit. The micro side has not yet been reconciled, and "declare a contract" has no cartridge-level home (a collection contract is an authority-declared NSID, a permbind is a request).
   - Unified: collection contract = a cartridge-level declared NSID in `lexicons`; requests are permbinds; grant is per cartridge.

4. **`resolution: atRuntime`.**
   - Micro (ADR 031 §11, 030 §12): a property on every request, with a powerbox "later".
   - Nodule (ADR 049 review item 4): the property goes away; any permbind can be requested at runtime through one queue, explicit only, with boot-scoped "only this time".
   - Nodule newer and operator-reviewed. Micro manifest still serializes `resolution`.
   - Unified: drop `resolution` from the manifest; runtime requests are a host service on both.

5. **Grant granularity and nouns.**
   - Micro: grant is per binding to a concrete collection/contract/access (a stored grant holds only while the request covers it, ADR 031 §11).
   - Nodule: grants per cartridge in the person's nouns (`grant/drawer|cartridge|net|method`), one answer satisfies every equal-or-narrower permbind (ADR 049 §4.1).
   - Both honor "grants never widen silently" (028 §3). Nodule matches "the user faces the cartridge, never its parts."
   - Unified: micro bindings are derived from cartridge grants; no micro-only grant datum.

6. **Who is named: contract vs supplier.**
   - Micro: a lazy endpoint stores `{micro_id, publication_name, contract}` (a specific provider; ADR 031 §7) and the requirement binding is configured to a supplier.
   - Nodule: consumers target operation NSIDs "so a consumer survives substitution" (ADR 027 §2, 047 §3); substitution is a route.
   - Mixed. Endpoints naming the contributor are natural for a static row (the contributor *is* the provider). Requirements already follow 028. The gap is that micro substitution (a fork in the slot keeps the micro id, ADR 030 §1) is slot-based, not route-based.
   - Unified: requirements resolve by NSID + route/lock for both; a contribution's endpoint is the contributor by construction.

7. **Typed admitted artifact vs opaque `description`.**
   - Micro: the catalog carries the manifest as an open JSON map under a `schema` NSID; "Clay's runtime owns that manifest's interpretation" (DB `big_cartridges_core/lib.rs:355-366`); admission from the envelope is not built.
   - Nodule: typed `Cartridge`/`Nodule`/`Permbind`, admitted and audited at the registry (ADR 047 Consequences).
   - Nodule matches "the manifest as the admitted artifact." Authority checks ("a package may declare only NSIDs under its authority", ADR 030 §11) are specified but not yet enforced through the envelope.
   - Unified: the micro manifest members become typed fields on `MicroContribution`, admitted by `big_cartridges_core`.

8. **Necessity granularity.**
   - Micro: one `necessity` per request; `access` read|write.
   - Nodule: per-access (`read:"required", write:"optional"`).
   - Nodule is finer and what the grant UI shows. Unified: adopt per-access necessity for data-like requests.

9. **Configuration facets.**
   - Micro: `configuration_requests` with a per-binding r/w/c/d ACL on `(tag, key_id)`.
   - Nodule: `settings` NSIDs shared by both runtimes plus a `#datum` permbind (ADR 049 §2 narrows by datum kind).
   - Unified: one config-datum permbind; `key_id` narrowing becomes a `#datum` narrowing.

10. **Start-time semantics.**
    - Micro: constructor (`initialize`) returning publications; lazy.
    - Nodule: `initialization` seeds/migrations on revision, "enabled" is a switch not a start.
    - Not a collision in intent; just do not reuse the word "initialize" for both in a unified manifest. Unified: keep both; a cartridge may have both an `initialization` list and a micro `initialize` entrypoint.

---

## 5. What must stay micro-only

- **Presentation.** `presentation` entrypoint, `kind: presentation` publications, mount/dispatch/unmount sync exports, `$attachment` slots in payloads, redeem-then-mount (ADR 031 §6). Nodules have stateless views only (ADR 027).
- **Incarnation lifecycle.** Singleton incarnations, strong/weak/lazy retention classes, reachability, idle `Closing`, retry generation, `eager` (ADR 030 §§1-6). Nodules have durable actors; do not push this onto them.
- **Clay-host-local wiring.** `shell.roots`, the runtime-defined `org.example.clay.*` contracts (requested by micros, satisfied by the runtime, ADR 030 §11), nav buttons, panel slots, layout payload vocabulary (target-owned, host-opaque).
- **Contribution payload vocabulary** (`slot`, `order`, `label`): owned by each target micro's collection contract; the host must stay generic (ADR 030 §4).
- **UI sinks** (`<img>` of a blob, CSS `url()`) as reads and external sends for the permission analysis (ADR 049 §3 asymmetry 3).
- **Node-side only:** routines, `implements`, `revisionedStores`, `localState`, `initialization`, wflow durability, routes/lock.
- **Lazy endpoint semantics.** "Weak, redemption-activates" has no meaning for a nodule call, which dispatches to a ready provider or fails `NotReady`.

---

## Top five collisions

1. Two homes for the request list: micro-private `requires`/`collection_requests`/`configuration_requests` vs the cartridge's single `permbinds` + `uses`.
2. Contributions are opaque data rows with unscoped write authority vs the nodule's closed, statically auditable `uses`.
3. Micro-declared collection contracts and per-binding concrete-collection grants vs ADR 049's "collections are data from cartridge Y", one cartridge-level grant.
4. `resolution: atRuntime` still on every micro request vs ADR 049's removal in favor of explicit runtime requests.
5. The micro manifest is an opaque `description` map under a schema NSID (admission not built) vs the typed, admitted `Cartridge`/`Permbind`.

## Manager's position (nodules, 2026-11-18), for the operator and claytxt

The request *fields* already agree (both inherit ADR 028's request shape); what differs is where requests live, what a grant names, and what gets audited. In each case the plug-manifest sensibility the operator wants kept is the cartridge side's: one list of what the cartridge reaches, requests by contract, grants in the person's nouns, the manifest as the typed artifact that is admitted and audited.

1. **One home for requests.** A micro's `requires` (the authority part), `collection_requests` and `configuration_requests` become cartridge permbinds, and the micro names them in `uses`, as ADR 049 §3 already says. *Agreed with claytxt for `requires`; the other two wait on clay_docs query mounts, and should be designed to land as permbinds.*
2. **Contributions stay, and become auditable.** The reverse edge is a real feature (a micro adds itself to another's toolbar without either running), and nodules will want it too (ADR 031's event-router example registers handlers). So it moves to the cartridge level, usable by both runtimes, and writing into a target's collection becomes a permbind shown at consent ("adds itself to Y's toolbar"). Authority is scoped to the contributor's own rows, closing "any micro with Register may write any key".
3. **Collection contracts are declared NSIDs** in the cartridge's `lexicons`; using one is a permbind; the grant is per cartridge ("data from cartridge Y", ADR 049 item 8). Coordinate with the query-mounts design before building.
4. **`resolution: atRuntime` goes** from the micro manifest, as ADR 049 already removed it: any permbind can be requested at runtime.
5. **The micro manifest becomes typed fields on `MicroContribution`**, admitted by `big_cartridges_core` like routines, instead of an opaque `description` map. This is also what makes micro admission ("a package declares only NSIDs under its authority") enforceable.
6. **Requirements name contracts, resolved by routes and the lockfile, for both.** A contribution's endpoint naming the contributor stays, since the contributor *is* the provider.
7. **Per-access necessity** (`read: required, write: optional`) for data-like requests on both sides.
8. **Kept separate on purpose:** the incarnation lifecycle (lazy activation, retention, idle closing) is micro-only, since nodules have durable actors; the two start hooks keep different names (micro `initialize` is a constructor, nodule `initialization` is a revision migration/seed).
