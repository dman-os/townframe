# ADR 047: The cartridge manifest: routines, bindings and components

**Status:** Proposed (manager, 2026-11-12, from the operator's review of the cartridge and nodule manifests). Nothing here is implemented.

**Related:**
- [ADR 027: cartridges, nodules and operation contracts](027-cartridges-nodules-and-operation-contracts.md) (the nodule description this replaces)
- [ADR 028: cartridge identity, naming and dependencies](028-cartridge-identity-naming-and-dependencies.md) §1 (registry layout), §2 (identity, kept), §4 (the request shape this replaces)
- [ADR 042: byte bodies and local-only methods](042-byte-bodies-and-local-only-methods.md) (method declarations)
- [ADR 044: cartridge configuration documents](044-cartridge-configuration-documents.md) (installs, grants, settings)
- The daybook plug manifest in `src/daybook_types/manifest.rs` (`RoutineManifest`, `RoutineFacetAccess`, `RoutineLocalStateAccess`, `LocalStateManifest`, `WflowBundleManifest`), which this ADR follows wherever the two differ.

## Context

The cartridge envelope (`big_cartridges_core::Cartridge`) and the nodule description (`big_nodule_core::Nodule`) drifted away from the plug manifest daybook had, and lost things the operator relied on:

- **No routines.** A nodule has one `handler` (an artifact key) and a bare `implements` list of method NSIDs. Nothing declares a method on its own, so nothing can say what one method may touch, which runtime runs it, or let a command or an initializer point at it. The plug manifest had `routines`, each with its implementation (`RoutineImpl::Wflow { bundle, key }`) and its own ACL, and commands and inits named a routine.
- **Access is declared per nodule, and only loosely.** `requires: Vec<Request>` mixes "call this operation" (where `access` must be absent) and "use this datum kind" (where `access` is a free string) in one struct. It cannot say which method of which dependency, cannot name local state, and confers nothing (ADR 028 §4: "a manifest describes needs, not granted authority"). The plug manifest said, per routine and per tag, read/write/create/delete (`RoutineFacetAccess`), and which local states it reached (`RoutineLocalStateAccess`).
- **Not a Lexicon.** The cartridge is meant to be published over atproto and stored as datums, but it is a Rust struct with a hand-rolled open union (`Contribution` tagged by a `schema` string instead of `$type`).
- **Lexicons and sidecars are inside the nodule**, although a micro uses the same Lexicons. The sidecars sit in a map parallel to the Lexicons they annotate.
- **"Artifacts" say nothing about the code.** An `Artifact` is a key, a digest, a size and a media type; that it is a wasm component, and what it exports, is implied by whoever reads it.
- **Unused declarations.** `revisionedStores` and `privateState` are validated at admission and read by nothing else. `privateState` also has the wrong name: the plug manifest called it *local state*, which says what matters (it is not synced to other nodes) without claiming it is private (it can be offered to other cartridges).
- **Opaque `bindings` sections** (commands, views, processors) are carried and never validated.
- **Keyed lists and plain strings.** Most sections are lists of objects with a `key`, so uniqueness is checked by hand, and every NSID is a `String` checked by `is_nsid` at 16 call sites.

## Decision

### 1. A cartridge version is a set of datums in its registry document

The registry document (ADR 028 §1) already holds one cartridge as datums: the identity, one envelope per version, and one `org.example.daybook.blob` datum per artifact so big_blobs retains the bytes. This ADR keeps that and splits the envelope's contents into datums of their own, each one a record of a real Lexicon:

- `org.example.daybook.cartridge/<version>`: the envelope. Identity (`<did>/<name>`, ADR 028 §2, unchanged), version, `authority`, dependencies, the contributions, and the digest of every other datum of this version (below), so the publisher's signature over the envelope covers them.
- `com.atproto.lexicon.schema/<version>/<nsid>`: one per Lexicon the cartridge publishes, as the record atproto itself publishes Lexicons with. They are cartridge-level: the nodule, the micro and every routine refer to them by NSID.
- `org.example.daybook.cartridge.component/<version>/<key>`: one per component (§3).
- `org.example.daybook.blob/cartridge.artifact/<version>/<key>`: the bytes, as today.

Every record is an atproto record, and every open union is a `$type` union. `Contribution`'s `schema` string goes.

### 2. The nodule contribution

```jsonc
{
  "$type": "org.example.daybook.cartridge.nodule",
  "routines": {
    "createAlbum": {
      "impl": { "$type": "org.example.daybook.cartridge.nodule#componentExport",
                "component": "main", "export": "create-album" },
      "implements": "org.example.photos.createAlbum",
      "uses": ["albums", "blobsPut", "drawer"]
    },
    "reindex": {
      "impl": { "$type": "org.example.daybook.cartridge.nodule#componentExport",
                "component": "main", "export": "reindex" },
      "uses": ["albums", "index"]
    }
  },
  "bindings": {
    "albums":   { "$type": "…#datum",  "nsid": "org.example.photos.album",
                  "read": true, "write": true, "create": true, "delete": false },
    "blobsPut": { "$type": "…#method", "nsid": "org.example.daybook.blobs.put" },
    "index":    { "$type": "…#localState", "key": "index" },
    "drawer":   { "$type": "…#drawer", "access": "write" }
  },
  "localState": { "index": { "kind": "sqlite" } },
  "settings": ["org.example.photos.settings"],
  "initialization": { "seed": { "routine": "reindex", "mode": "once" } }
}
```

- **`routines`**, keyed by name. A routine has an `impl`, a `$type` union whose only member today is `#componentExport` (a component and an export of it; the place a wflow or another runtime's member goes later, as `RoutineImpl::Wflow` did). It `implements` at most one method NSID: a routine with none is reachable only from the manifest (an initializer). It `uses` binding keys, and nothing else.
- **`bindings`**, keyed by name, are everything the cartridge reaches outside its own code (§4).
- **`localState`**, keyed by name: the plug manifest's `local_states`. The kinds are today's (`sqlite`, `kv`).
- **`settings`**: ADR 044's, unchanged.
- **`initialization`**, keyed by name, names a routine instead of a method.
- **Removed:** `handler`, `implements`, `requires`, `revisionedStores`, `privateState`, the opaque `bindings` sections, and the nodule's `lexicons` and `sidecar` (moved to §1). Commands, views and processors return as typed sections that name routines when something implements them; until then a manifest that carries one is refused, not silently passed through.

### 3. Components

The chain is routine → component → bytes, with nothing in between. A component datum describes one unit of code and points at its bytes; a routine's `impl` points at a component and an export of it. There is no grouping layer (the plug manifest's `wflow_bundles` said which keys a component served; here each routine says it). A component's kind is its `$type`, so each kind states its own fields, and its bytes are a blob datum (§1) described the way OCI describes a layer, by digest, size and media type:

```jsonc
{ "$type": "org.example.daybook.cartridge.component#wasm",
  "blob": "cartridge.artifact/1.2.0/main",
  "digest": "z…", "size": 812345, "mediaType": "application/wasm" }
```

`#wasm` (a WASI component) is the only kind today. Another runtime's code is a new member when a runtime for it exists.

### 4. Bindings: one way to reach anything, whoever provides it

A binding names what it reaches by contract, never by provider (ADR 028 §4 keeps that rule), so a node-native service and another cartridge's nodule are reached and granted the same way. The members are the four kinds the node has today:

- `#method { nsid, from? }`: one method. `from` names the dependency expected to provide it when the author wants to pin that; without it any enabled provider serves.
- `#datum { nsid, read, write, create, delete }`: one datum kind, as `RoutineFacetAccess` had.
- `#localState { key, cartridge? }`: a local state of this cartridge, or one another cartridge offers (as `RoutineLocalStateAccess` named a plug).
- `#drawer { access }`: a drawer chosen when access is granted, not by the manifest. This is the "slot" a later grant fills: the manifest says a drawer goes here and with what access; the install or the user names which.

A binding is resolved either by the manifest (a method, a datum kind, a local state) or by a grant (a drawer), and either way the grant is to the **cartridge**, keyed by binding: ADR 044's grant datums become `nodule.grant/<binding key>`. Granting reasons about one list, the cartridge's bindings.

### 5. Each call carries only its routine's access

How the access is partitioned among routines is the author's, and it is enforced. When dispatch invokes a routine, it mints a capability for that call covering the routine's `uses`, intersected with what the cartridge was granted, and the capability ends with the call. A write the routine did not declare is refused at the docs service; a call to a method it did not bind is refused at dispatch; a response that does not match the method's output Lexicon is a failed call.

So "this routine reads the sensitive kind and binds no outbound method" is stated in the manifest and holds on every call, and a validator can check a cartridge for a routine that holds both. The limit is stated rather than hidden: two calls served concurrently by one component instance could exchange handles inside it, so against a deliberately hostile component the wall is the union of the routines' access, enforced per cartridge. Per routine, the partition holds against bugs and makes each routine's reach auditable, as the plug manifest's per-routine ACLs did.

### 6. Typed keys and maps

Every keyed section is a map. NSIDs are a typed `Nsid` parsed on deserialize (one exists in `clay_collection::nsid`; it moves to `big_cabinet_core`), so a manifest with a bad NSID is refused at the edge and never constructed; the scattered `is_nsid` checks go.

## Consequences

- `big_cartridges_core::Cartridge`, `Request`, `Artifact` and `big_nodule_core::Nodule` are replaced, along with admission and the registry projection. There is no migration: pre-release, and no shipped node holds cartridges.
- ADR 044's grant datums are keyed by binding (§4).
- Dispatch mints a per-call capability (§5), and the docs service checks datum-kind access: a capability's scope gains datum kinds per document, beside today's per-document read and write (`DocScope`). ADR 044 §6 (a nodule writes its settings, not its grants) becomes one case of it.
- Clay's micro contribution uses the cartridge-level Lexicons and the same bindings, so claytxt's manifest changes with it.

## Open questions

1. **Where a sidecar lives.** A sidecar annotates one Lexicon. Either as a datum at the same slot under its own NSID (`…lexicon.sidecar/<version>/<nsid>`), tied to its Lexicon by key, or inside the Lexicon record if extra members there remain a valid Lexicon.
2. **Offering local state to other cartridges**: whether a `#localState` binding to another cartridge needs that cartridge to declare the state offered.
3. **The micro's routines**: whether a micro's handlers are routines in the same sense, or only consume bindings.
