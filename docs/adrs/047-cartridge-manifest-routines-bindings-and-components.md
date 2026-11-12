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
      "uses": { "albums": { "write": true, "create": true }, "blobsPut": {}, "workDrawers": { "read": true } }
    },
    "reindex": {
      "impl": { "$type": "org.example.daybook.cartridge.nodule#componentExport",
                "component": "main", "export": "reindex" },
      "uses": { "albums": { "read": true } }
    }
  },
  "bindings": {
    "albums":      { "$type": "…#datum", "nsid": "org.example.photos.album",
                     "read": "required", "write": "required", "create": "required" },
    "blobsPut":    { "$type": "…#method", "nsid": "org.example.daybook.blobs.put" },
    "workDrawers": { "$type": "…#drawers", "read": "required", "write": "optional" }
  },
  "settings": ["org.example.photos.settings"],
  "initialization": { "seed": { "routine": "reindex", "mode": "once" } }
}
```

- **Contributions are keyed by their datum slot**, like everything else in the registry document, so a cartridge could carry more than one nodule. Today a cartridge carries at most one: nothing separates two nodules of one cartridge (they share its bindings and grants), so a second one buys nothing yet.
- **`routines`**, keyed by name. A routine has an `impl`, a `$type` union whose only member today is `#componentExport` (a component and an export of it; the place a wflow or another runtime's member goes later, as `RoutineImpl::Wflow` did). It `implements` at most one method NSID: a routine with none is reachable only from the manifest (an initializer).
- **`uses` is the routine's ACL**: a map from binding key to the part of that binding's access the routine takes. `reindex` above takes `albums` read-only, so the routine that rebuilds the index cannot write an album even though the cartridge may. A routine reaches nothing it does not list, and never more of a binding than the binding declares.
- **`bindings`**, keyed by name, are everything the cartridge reaches outside its own code (§4).
- **`settings`**: ADR 044's, unchanged.
- **`initialization`**, keyed by name, names a routine instead of a method.
- **Local state is deferred.** When it comes, it is the plug manifest's `local_states`: a named SQLite database in the nodule's address space, because table semantics are wanted there directly, not through a method. Nothing declares or binds one until a nodule needs it.
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

A binding names what it reaches by contract, never by provider (ADR 028 §4 keeps that rule), so a node-native service and another cartridge's nodule are reached and granted the same way. **The binding's kind decides how it is resolved**, and that is the whole difference between them:

- `#datum { nsid, read, write, create, delete }`: one datum kind, as `RoutineFacetAccess` had. **Fixed by the manifest**: the routine depends on that shape, so nothing substitutes it. The grant only says whether the access is given.
- `#method { nsid, from? }`: one method. **Resolved to a provider**: any enabled provider of the NSID serves, which is where substitution happens (another cartridge, or the node's own service, behind the same contract). `from` pins it to one dependency when the author needs that one.
- `#drawers { read, write, … }`: **a set filled by grants.** The manifest says "the drawers I work on, with this access"; the user adds or removes drawers over time, and a routine that uses the binding sees the set as it is at the call. "Every drawer this cartridge has been granted" is this binding with no further narrowing. A single drawer is the set's one-member case, not another kind.

Each access in a binding is `required` or `optional`. A required one must be granted for the cartridge to be enabled; an optional one may be granted later or never, and the routine sees at the call whether it has it (the `workDrawers` above can be read-only and the nodule degrades). The same holds for a whole binding: a method binding marked optional is a feature that works only when some provider is enabled.

Bindings are declared up front, because they are what the user grants and what a validator reads: a binding added later is a new cartridge version, and its new required access must be granted before that version is enabled (an upgrade shows the difference). What grows at runtime is the *contents* of a set binding, never the list of bindings.

Grants are to the **cartridge**, keyed by binding: ADR 044's grant datums become `nodule.grant/<binding key>` (and the members of a set binding, `nodule.grant/<binding key>/<drawer id>`). Granting reasons about one list, the cartridge's bindings; how its routines split that access is the author's partition (§5).

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

### 7. Sidecars sit next to their Lexicon

A sidecar is its own datum at the same slot as the Lexicon it annotates: `org.example.daybook.lexicon.sidecar/<version>/<nsid>` beside `com.atproto.lexicon.schema/<version>/<nsid>`. The slot ties the two, so a Lexicon's sidecar is always found next to it, and the Lexicon record stays a plain atproto Lexicon.

### 8. What the prior art has that this must not miss

The design follows snapd's plugs (package-level declarations, each app inside listing the ones it uses), Fuchsia's component `use` declarations (whose `availability: optional` is the precedent for optional access), Sandstorm's powerbox (a grant chosen by the user, kept as a durable reference) and Cloudflare Workers bindings (named, typed handles the deployment fills). Each of them also settles something this ADR has to:

- **Confused deputy.** When routine A calls another cartridge's method, the callee acts with *its own* bindings, never with A's, and A's capability is not forwarded. A callee that should act on the caller's data takes it as a datum reference or a capability passed explicitly, so authority moves only by being handed over (ocap; Sandstorm's rule).
- **Revocation.** Removing a grant takes effect at the next call, since every call's capability is minted afresh and ends with the call (§5). A long-running call keeps what it started with until it ends.
- **Upgrades.** A new version whose required access exceeds what was granted is not enabled until that difference is granted (snapd and Android ask again on upgrade; §4).
- **Which grants need a person.** A `#datum` binding to the cartridge's own authority and a `#method` binding to a declared dependency can be granted at install without asking (snapd's auto-connection); a `#drawers` binding always names drawers someone chose. The policy itself is the install flow's, not this ADR's.

## Open questions

1. **The micro's routines**: whether a micro's handlers are routines in the same sense, or only consume bindings.
2. **Who may grant**: whether the install flow's auto-grant policy (§8) is fixed by the node or configurable per user.
