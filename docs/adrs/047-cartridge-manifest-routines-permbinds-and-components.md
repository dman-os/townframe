# ADR 047: The cartridge manifest: routines, permbinds and components

**Status:** Proposed (manager, 2026-11-13, from the operator's review of the cartridge and nodule manifests). Nothing here is implemented.

**Related:**
- [ADR 027: cartridges, nodules and operation contracts](027-cartridges-nodules-and-operation-contracts.md) §2 (the nodule description whose serialized shape this replaces; its responsibilities stay)
- [ADR 028: cartridge identity, naming, dependencies and malleability](028-cartridge-identity-naming-and-dependencies.md) (identity, upgrades, the request shape, the lockfile; all kept except where §9 says)
- [ADR 035: references and URLs](035-references-and-urls.md) (`#datumPin`)
- [ADR 044: cartridge configuration documents](044-cartridge-configuration-documents.md) (installs, grants, settings)
- [ADR 045: method routes and the dev takeover](045-method-routes-and-dev-takeover.md) (substitution)
- The daybook plug manifest in `src/daybook_types/manifest.rs` (`RoutineManifest`, `RoutineFacetAccess`, `RoutineLocalStateAccess`, `LocalStateManifest`), whose per-routine access this restores.

## Context

ADR 027 §2 keeps the word **routine** for "an executable implementation inside a nodule", identifying "the operation NSID it implements, artifact/export references, private state requirements, and requested host facilities", and ADR 028 §1 states the goal that it be "statically answerable whether a cartridge can ever reach a given resource". The serialized description never carried that. It has one `handler` (an artifact key) serving a bare `implements` list, and one `requires` list for the whole nodule, so what any one method may touch cannot be read off the manifest. The daybook plug manifest could: each routine had its own ACL per datum kind (`RoutineFacetAccess`) and per local state (`RoutineLocalStateAccess`).

The rest of what this ADR changes is shape, not meaning:
- the cartridge is not a Lexicon record, though it is published over atproto and stored as datums: `Contribution` is an open union tagged by a `schema` string instead of `$type`;
- Lexicons and sidecars sit inside the nodule, though a micro uses the same Lexicons, and the sidecars are a map parallel to the Lexicons they annotate;
- an `Artifact` is a key, digest, size and media type, and what kind of code it is is implied by whoever reads it;
- keyed sections are lists with a `key` member, and NSIDs are `String`s checked by `is_nsid` at 16 call sites;
- the request shape (ADR 028 §4) is one struct for an operation call and a datum access, told apart by whether `access` is set.

**What stays.** Everything ADR 027, 028, 044 and 045 decided that this ADR does not name in §9. In particular: the cartridge id `<namespace DID>/<name>` (028 §2); upgrades, forks and "grants never widen silently" (028 §3); dependencies name NSIDs, not providers, and cartridge-to-cartridge dependencies are delivery hints only (028 §4); a cartridge carries copies of the Lexicons it depends on (028 §4); the resolution lockfile (028 §4); routes and the dev takeover (045); installs and settings (044); commands, processors and initialization target operation NSIDs, never a provider's private routine names (027 §2), so a consumer survives substitution.

## Decision

### 1. A cartridge version is a set of datums in its registry document, pinned by its envelope

The registry document (ADR 028 §1) already holds a cartridge as datums: its identity, one envelope per version, and a `Blob` datum per artifact. This keeps that and moves the envelope's large parts into datums of their own, each a record of a real Lexicon:

- `org.example.daybook.cartridge/<version>`: the envelope (§2).
- `com.atproto.lexicon.schema/<version>/<nsid>`: each Lexicon the cartridge carries, its own and the copies it depends on (028 §4), as atproto itself publishes Lexicons.
- `org.example.daybook.lexicon.sidecar/<version>/<nsid>`: the sidecar of a record it holds (028 §4), at the same slot as its Lexicon, so a Lexicon's sidecar is always found next to it and the Lexicon record stays a plain Lexicon.
- `org.example.daybook.cartridge.component/<version>/<key>`: each component (§3).
- `org.example.daybook.blob/cartridge.artifact/<version>/<key>`: the bytes, as today.

The envelope names the other datums of its version by `#datumPin` (ADR 035): the document and heads it was published at. Heads are hashes of the document's history, so the publisher's signature over the envelope covers every pinned datum, and no separate digest list is needed. A Lexicon from *another* cartridge is never referenced by URL: a cartridge carries its own copy (028 §4), and the NSID is the reference. Cartridge identity is the DID-based id, never a keyhive document, and delivering a cartridge some other way (OCI, ADR 028 §2) is a later field of the envelope.

In atproto, `$type` names the Lexicon definition an object follows: a record carries its own, and an object in a field that can hold different kinds carries the one it is (a routine's implementation, a permbind, a component). A field that always holds one kind has none.

### 2. What belongs to the cartridge, and what to a contribution

A cartridge has **at most one nodule and at most one micro** (ADR 028's open question settled): nothing separates two nodules of one cartridge, since they would share its permbinds and settings. The contributions are two optional fields of the envelope, not a list; a later kind is another optional field.

```jsonc
{
  "$type": "org.example.daybook.cartridge",
  "id": "did:plc:…/photos",
  "version": "1.2.0",
  "authority": "org.example.photos",
  "dependencies": { "did:plc:…/blobs": "^1" },          // delivery hints (028 §4)
  "lexicons":   { "org.example.photos.album": { /* #datumPin */ }, … },
  "components": { "main": { /* #datumPin */ }, "ui": { /* #datumPin */ } },

  "permbinds": {
    "albums":      { "$type": "…#datum", "nsid": "org.example.photos.album",
                     "description": "Your albums",
                     "read": "required", "write": "required", "create": "required" },
    "blobsPut":    { "$type": "…#method", "nsid": "org.example.daybook.blobs.put",
                     "description": "Store photo bytes", "necessity": "required", "minRevision": 2 },
    "workDrawers": { "$type": "…#drawers", "description": "Drawers to import photos from",
                     "resolution": "atRuntime", "read": "required", "write": "optional" }
  },
  "settings": ["org.example.photos.settings"],

  "nodule": {
    "routines": {
      "createAlbum": {
        "impl": { "$type": "…#componentExport", "component": "main", "export": "create-album" },
        "implements": "org.example.photos.createAlbum",
        "uses": { "albums": { "write": true, "create": true }, "blobsPut": {}, "workDrawers": { "read": true } }
      },
      "reindex": {
        "impl": { "$type": "…#componentExport", "component": "main", "export": "reindex" },
        "implements": "org.example.photos.reindex",
        "uses": { "albums": { "read": true } }
      }
    },
    "revisionedStores": { /* ADR 027 §7, unchanged in meaning */ },
    "localState": { /* ADR 027 §6, the plug manifest's local_states */ },
    "initialization": { "seed": { "target": "org.example.photos.reindex", "mode": "once" } }
  },

  "micro": {
    "components": ["ui"],
    "uses": { "albums": { "read": true, "write": true }, "workDrawers": { "read": true } }
  }
}
```

**The cartridge's:** identity, version, authority, dependencies, Lexicons, components, **permbinds** (one list of what the cartridge reaches, whichever part reaches it, which is what an audit and a grant read) and **settings** (ADR 044 already makes them shared by the nodule and the micro).

**The nodule's:** routines, revisioned stores, local state and initialization.

**The micro's:** the components it runs and one `uses`. A micro is a UI: one long-lived instance per cartridge, not a call per request, so it has no routines, and its capability lasts for the instance's life. It may run several components (how they compose into one instance is Clay's runtime's). Its `uses` narrows the permbinds as a routine's does, so the UI can be kept from what only the nodule should touch.

### 3. Routines

- A routine has an `impl`, a `$type` union whose only member today is `#componentExport`: a component and an export of it. Durable execution stays the wflow service, not a routine kind (ADR 027 §2); another runtime's code is a new member when one exists.
- A routine `implements` one operation NSID. Commands, processors and initialization name that **operation**, never the routine (ADR 027 §2), so a consumer keeps working when another provider serves the operation.
- **`uses` is the routine's ACL**: a map from permbind key to the part of the permbind the routine takes. `reindex` takes `albums` read-only, so it cannot write an album though the cartridge may. A routine reaches nothing it does not list, and never more of a permbind than the permbind declares. ADR 027 §2's "private state requirements and requested host facilities" are these entries.

### 4. Components

The chain is routine → component → bytes. A component datum states the kind of code by `$type` and pins its bytes, described as OCI describes a layer (digest, size, media type). `#wasm` (a WASI component) is the only kind today.

### 5. Permbinds

A permbind is ADR 028 §4's request, keyed by its name (the request's `binding`) and split by kind, so each kind states only the fields it has. It keeps the request's rules: it names a contract by NSID, never a provider; `necessity` (or per-access `required`/`optional`) and `resolution` (`configured`, or `atRuntime` through a picker) mean what they mean there; `minRevision` is the Lexicon revision it needs. Every permbind carries a `description`, shown when it is granted. The kind decides how it is satisfied:

- `#datum { nsid, read, write, create, delete }`: a datum kind, as `RoutineFacetAccess` had. The routine depends on its shape, so nothing substitutes it; a grant only gives or withholds the access.
- `#method { nsid, necessity, minRevision? }`: an operation. Any enabled provider serves it, chosen among several by a route (ADR 045); this is where substitution happens.
- `#drawers { read, write, …, resolution }`: a **set** of drawers filled by grants, configured or picked at runtime. The set changes over time; a routine sees it as it is at the call. "Every drawer the cartridge was granted" is this permbind with no narrowing, and one drawer is the set's one-member case.

Local state and revisioned stores are reached through `uses` like a permbind, when they are implemented.

**Required and optional** keep ADR 028 §3's rule: a cartridge whose required access is not all granted (a required set with no member, a required method with no provider) stays **enabled but not startable** until configured, Clay's `needs configuration` state, and nothing is disabled behind the user's back. Optional access may be granted later or never, and a routine sees at the call whether it has it. The prior art agrees: Fuchsia's `use` has `availability: required | optional`, Android's `uses-feature` has `required="false"`, and a snap app checks whether an optional plug is connected.

**Grants stay each runtime's** (ADR 028 §1, ADR 044): `nodule.grant/<permbind>` and `micro.grant/<permbind>` (and `…/<permbind>/<drawer id>` for a set's members). A grant is keyed by the permbind it satisfies, so granting reasons about the cartridge's one list of permbinds.

### 6. Each call carries only its routine's access

When dispatch invokes a routine, it mints a capability for that call covering the routine's `uses`, intersected with what was granted, and the capability ends with the call. A write the routine did not declare is refused at the docs service, a call to an operation it did not bind is refused at dispatch, and a response that does not match the operation's output Lexicon is a failed call. This needs the docs capability scope to name datum kinds per document beside today's per-document read and write (`DocScope`); ADR 044 §6 (a nodule writes its settings, not its grants) becomes one case of it.

"This routine reads the sensitive kind and binds no outbound operation" is then stated in the manifest and holds on every call. The limit is stated: two calls served concurrently by one component instance could exchange handles inside it, so against a hostile component the wall is the union of the routines' access, enforced per cartridge. Per routine, the partition holds against bugs and makes each routine's reach auditable.

Also settled with the prior art (snapd, Fuchsia, Sandstorm, Android):
- **Confused deputy.** A routine calling another cartridge's operation passes none of its own access; the callee acts with its own. Authority moves only when handed over explicitly, as a datum reference or a capability in the call.
- **Revocation** takes effect at the next call, since each call's capability is minted afresh.
- **Upgrades** that widen required access leave the cartridge not startable until granted (028 §3).

### 7. The lockfile and substitution

Unchanged, named here because the manifest is only half of it:
- **The lockfile** is ADR 028 §4's resolution record: when a cartridge is enabled, each method permbind is pinned to the provider cartridge id, version and schema revision that serves it, in node-local SQLite (`big_nodule::store`, `resolution_locks`). It is per node, not synced. A change of provider is a change of grant and is re-consented (`ProviderChanged`).
- **Substitution** is a route (ADR 045): the operator maps an NSID or authority prefix to a provider in the node config document, and every dependent's lock moves with it, as Cargo's `[patch]` does. Running as another cartridge is only the dev takeover.
- **Which version runs** is the install's pin (ADR 044 §2): a `#datumPin` to the envelope in the registry document.

### 8. Typed keys and maps

Every keyed section is a map. NSIDs are a typed `Nsid` parsed on deserialize (one exists in `clay_collection::nsid`; it moves to `big_cabinet_core`), so a manifest with a bad NSID is refused at the edge and never constructed.

### 9. What this changes in earlier ADRs

- ADR 027 §2's serialized description: `handler` + `implements` become routines; `requires` becomes cartridge-level permbinds; `private_state` is renamed `localState`; Lexicons and sidecars move to the cartridge (§1). Revisioned stores, initialization and the wflow boundary are unchanged.
- ADR 028 §4's request shape becomes `permbinds` split by kind, with `description` added; its rules are unchanged.
- ADR 028's open question (more than one micro) is settled: at most one nodule and one micro.
- ADR 044's grant datums are keyed by permbind.
- Commands, views and processors stay targets of operation NSIDs (027 §2) but become typed sections when something implements them, instead of opaque carried maps.

## Consequences

- `big_cartridges_core::Cartridge`, `Request`, `Artifact` and `big_nodule_core::Nodule` are replaced, with admission and the registry projection. No migration: pre-release, and no shipped node holds cartridges.
- Clay's micro contribution uses the cartridge's Lexicons and permbinds, so claytxt's manifest changes with it.
- The docs capability scope gains per-document datum kinds (§6).

### 10. The name, and who grants

- **Permbinds** (operator, 2026-11-13). A permbind is the named hole in a cartridge's authority; in capability-security terms it is filled by an *endowment* (a grant at install or configuration) or an *introduction* (authority passed later, as the at-runtime picker does), and the hole itself keeps one home-grown name so it is never confused with a capability handle.
- **Candidates are chosen automatically; a person accepts them.** For most permbinds exactly one thing can satisfy them (a datum kind, an operation with one provider), so the install flow proposes that candidate and the person accepts or declines it. Where several candidates exist (a drawer set, an operation with several providers), the flow proposes one and the person may change it. Nothing is granted without being shown. The data layer is this ADR's and ADR 044's; the screens are a UX design of their own.

### 11. The lifecycle, as ADR 027 §4 and ADR 044 define it

Kept, and restated because the words are easy to blur:

- **Catalogued (known):** the cartridge's versions are in the registry drawer. Nothing runs; no actor or grant exists. Syncing the registry never goes further than this (ADR 028 §1).
- **Installed:** a node pins one version, `cartridges.install/<cartridgeId>` `{ cartridge: #datumPin, configDoc, nodule?, micro? }` (ADR 044 §2), and the runtime admits it: validates the description, provisions the nodule's durable actor, and finds or creates the cartridge config document that holds its settings and grants. An install starts **disabled**, with nothing granted. Installing answers "this node has this cartridge, at this version".
- **Configured:** permbinds are granted (§10) and settings written. Grants and settings live in the cartridge config document, so they follow the user to sister nodes; the install and enablement do not (ADR 044 §1).
- **Enabled:** a per-node switch, per contribution (`nodule.enabled`, `micro.enabled`): whether this node runs the installed version. Enabled is the user's choice; it does not imply startable. An enabled cartridge whose required permbinds are not all granted is **not startable** ("needs configuration", §5) until they are. Disabling stops new calls and live waits and keeps the actor, grants, settings and local state.
- **Upgraded:** the install's pin moves to a newer version of the same id; required access the new version widens must be granted before it starts (ADR 028 §3).
- **Uninstalled:** the install goes; the cartridge config document stays, so a reinstall or a sister finds its settings and grants (ADR 044 §7). Clearing it is the explicit `clearCartridgeConfig`.

In one line: **install** is "have it here, at this version", **configure** is "what it may reach", **enable** is "run it now", and only enable is a plain switch.
