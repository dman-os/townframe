# ADR 007: Plug Manifests as Drawer Docs

**Status:** Proposed.

## Context

Today, plug manifests live in a single AmStore (`PlugsStore`) on the **app doc**
(`doc_app`), under the `plugs` prop. There are exactly two ways a plug enters
the repo: `import_from_oci_layout` / `import_from_oci_registry`, which pull an
OCI artifact, store its layers as blobs, rewrite `oci://sha256:` component URLs
to `db+blob://`, and reconcile the parsed manifest into the store; and
`ensure_system_plugs()`, which seeds `@daybook/core` + `@daybook/wip` on first
boot.

Being *known* (manifest in the store) and being *enabled* (loaded in rt) are the
same thing today, with one exception: wasm bundle workloads are loaded lazily,
driven by the per-checkout dispatch backlog (local sqlite). Everything else is
eager and all-or-nothing:

* boot queues **inits for every known plug** (`rt::ensure_plug_init_dispatches`);
* `triage.rs::refresh_processors` registers **every plug's processors**;
* the drawer accepts writes to **any tag owned by a known plug**
  (`validate_facets`, called from `drawer::add` and `drawer::update`);
* commands of any known plug are invocable.

There is no delete API and no enable/disable API anywhere (FFI surface confirms:
load / import / inspect / list only).

This ADR separates the two concerns:

* **known** — a manifest doc the repo has seen (derived, local, cheap);
* **enabled** — a manifest the user wants loaded in the runtime (durable,
  synced, pinned).

The motivation is distribution and authoring. Manifests should flow through the
hive itself — drawer sync for learning, relays for pulling — instead of an
out-of-band OCI registry. And users should be able to author plugs *inside*
daybook (the goal is a plug that can create plugs), with the drawer's facet
validation and dmeta author signatures keeping the marketplace honest, and
fork/branch giving version recovery.

### Related work

* ADR 005 — relays retain sponsored docs; parts carry `Access::Relay` (bytes
  retrievable, not readable) — the pull path this ADR relies on.
* ADR 006 — recovery principals; not directly involved here.
* KeyHive notebook (`https://www.inkandswitch.com/keyhive/notebook/`) — pull as
  an access effect weaker than read; trust-minimized sync servers.

## Terminology

* **Manifest doc** — a drawer doc carrying the plug-manifest facet.
* **Plug ref** — a full reference to a manifest: `db+facet:///<doc_id>/<tag>/<id>`
  plus branch and heads (see §3).
* **Known plug** — a manifest doc the local facet-set index has catalogued.
* **Enabled plug** — an entry in the plugs config facet: a pinned ref.
* **Repo config doc** — the third core doc (with app doc and drawer doc) that
  holds synced config facets, including the plugg config facet.

## Decision

### 1. Facet shapes in `daybook_types::doc.rs`

Two new well-known facets, shaped in `src/daybook_types/doc.rs` via the
`define_enum_and_tag!` macro:

```rust
PlugManifest type (crate::manifest::PlugManifest),  // tag: org.example.daybook.plugManifest
PlugConfig  struct {
    /// full ref: db+facet:///<doc-id>/org.example.daybook.plugManifest/main
    ///            ?branch=<branch>&at=<head1>|<head2>
    pub enabled: HashMap<String, Url>,
    pub plug_config_doc_ids: HashMap<String, String>, // unchanged semantics (§6)
},
// tag: org.example.daybook.plugConfig
```

**Naming note.** Plug ids are `@namespace/name` with camelCase names
(`@daybook/core`, `@daybook/wip`, `@example/vibeCoder`). Facet names follow the
same convention: `plugManifest`, `plugConfig`. The `define_enum_and_tag!` macro
currently lowercases identifiers to build tag strings (`PlugManifest` →
`org.example.daybook.plugmanifest`); a separate branch switches the macro to
camelCase, at which point the emitted tag matches the name exactly. The ADR
names the facets `plugManifest`/`plugConfig` and treats the macro casing as an
implementation detail.

The manifest facet's value schema is `schemars::schema_for!(PlugManifest)`; the
manifest types in `manifest.rs` need `schemars::JsonSchema` derived (currently
absent).

### 2. The repo config doc

A **third core doc**, `doc_config`, created at repo init alongside `doc_app` and
`doc_drawer`, with the same parents: `create_doc_with_parents([core_docs_group,
drawer_group])`. It is the official home of durable, synced config facets
(`ConfigRepo` currently sits on the app doc and moves here as part of the
in-flight config-to-drawer-doc migration; this ADR only requires the plugg
config facet to live there).

The **plug config facet** (`org.example.daybook.plugConfig/main`) holds the
enablement map and the per-plug config-doc ids (today `plug_config_doc_ids` in
`PlugsStore` — cross-checkout state, so it belongs in synced config, not in the
local index). Writes to the plug config facet validate against the
`plugConfig` facet manifest declared by `@daybook/core` like any other facet.

### 3. Enablement: full refs, pinned heads, no auto-repin

The enablement value for a plug is a **full ref**: doc id + branch + heads,
serialized as the existing facet-ref URL form from `dict.md`
(`db+facet:///self/...?at=hash1|hash2`) extended with a branch query param:

```text
db+facet:///<doc-id>/org.example.daybook.plugManifest/main?branch=main&at=<head1>|<head2>
```

(Note: `daybook_types::url::parse_facet_ref` ignores query params today; it
gains `branch`/`at` parsing — the codebase otherwise pairs a URL with a
separate `ref_heads` field, e.g. `ImageMetadata`, and the URL form is chosen
here to make refs self-contained.)

Consequences:

* **Changing the manifest facet does not change enablement.** The pin points at
  specific automerge heads; because automerge keeps full history, pinned heads
  are always materializable once the doc's bytes are present — regardless of how
  many manifest updates happened since.
* **No waiting on the index.** Enablement carries its own doc ref, so a fresh
  checkout can resolve enabled plugs from config alone as their docs arrive;
  it never waits for the index to catalogue manifests first.
* **The race is a state: pending, not unknown.** When config references a
  doc/heads that are not yet local (sync pending, or a relay-only doc not
  yet materialized), the plug is `enabled/pending` — the ref is known, the
  manifest bytes are simply not readable yet. How pending plugs resolve is
  specified in §6.
* **`@daybook/core` cannot be disabled.** The one guard in the system:
  the plugg facet mutation rejects disabling or removing the core entry. It
  lives in the config write, not in the runtime — everything else about core
  (including manifest updates via re-pin) is ordinary. Any other plug,
  including one that breaks manifest authoring, can be disabled freely.

### 4. `@daybook/core` and repo init

No special-casing in the runtime or validation. The core manifest is a plain
manifest doc authored at repo init, like every other plug. Two bootstrap
details:

1. The init dance creates `doc_config` and the core manifest doc
   (`drawer::add` with the `plugManifest` facet), alongside the existing system
   plods, and repeats idempotently on open (the `ensure_system_plugs`
   replacement: ensure the core manifest doc exists, create if missing).
2. The *first* manifest write is the single write in the system that must skip
   facet validation — `validate_facets` requires a registered manifest for the
   facet tag, and none exists before the first manifest doc. That one write
   goes through an unchecked internal doc-create. Everything after validates
   normally (the `plugManifest` facet is declared by core itself, so manifest
   authoring is just normal drawer writes).

### 5. Known plugs: derive from the existing facet index

No new store. Known plugs are a query over the existing facet-set index
(`DocFacetSetIndexRepo::list_docs_for_tag(plugManifestTag)`) which already
tracks `(doc_id, heads)` per tag, kept fresh by drawer events. The `PlugsRepo`
keeps its shape (load / get / list / events / FFI) but its backing changes:

* `manifests` map: derived read cache over manifest docs (doc → manifest at
  heads), *not* a replicated AmStore;
* `tag_to_plug` / `facet_manifests`: in-memory/sqlite indices rebuilt from the
  same set;
* the app-doc `PlugsStore` is retired.

### 6. Enablement lifecycle and downstreams

**Startup.** rt derives the enabled set from the plugg config facet and
resolves each entry to its ref. A plug whose manifest is readable at the
pinned heads is **active**; an entry whose doc is absent or unmaterialized
is **pending**. Every downstream that lists enabled plugs at startup (init
queueing, triage registration, drawer validation) operates on the active set
only.

**Resolution.** A pending entry resolves when its pinned doc arrives. This
is a switch sink (`PlugEnablementSink`, consuming drawer + config events):

1. the sink sees the doc land (`DrawerEvent::DocAdded`/`DocUpdated` on the
   pinned doc), loads the manifest at pinned heads;
2. it emits `PlugsEvent::PlugEnabled` — identical to a fresh manual
   enablement;
3. downstreams react to the event.

Design principle: **pending resolution is exactly a new manual enablement** —
only the trigger differs (doc arrival vs user action). Nobody polls a
pending list; anyone who cares must subscribe to `PlugsEvent`. The config
entry is the only state: removing it, even while pending, wins (resolution
reads the current config, so a removed entry never fires) — no tombstone
needed.

**Downstreams** — each enabled-plug consumer reacts to enablement events as
follows:

* **InitRepo** — queues the plug's inits on `PlugEnabled` (and at boot for
  the active set). Init identity stays `init_id(plug_id, version, init_key)`,
  so a re-pin that bumps the manifest version re-runs `PerInstall` inits;
  `PerNode`/`PerBoot` semantics are unchanged. Disabling does not un-run
  inits — it just stops new queueing until re-enabled.
* **Triage (processors)** — `refresh_processors` re-runs on
  `PlugEnabled`/`PlugDisabled`/`PlugUpdated`; only active plugs' processors
  are registered, from the pinned manifest.
* **Drawer facet validation** — the facet-manifest lookup
  (`get_facet_manifest_by_tag`) consults active plugs only. Existing facets
  of a disabled or unknown plug stay readable; writes fail with a distinct
  "plug disabled" vs "unknown tag" error.
* **Commands** — command URLs resolve through active plugs' manifests only.
* **Wasm bundles** — unchanged: dispatch-driven, loaded on demand from the
  pinned manifest.
### 7. Events rework

Today `PlugsEvent` is consumed by exactly two sites: `rt/switch.rs`
(`SwitchEvent::Plugs`, `events_for_init` replay) and the FFI listener bridge.
Rework to be **enabled-only**:

```rust
PlugsEvent {
    PlugEnabled   { id, heads, origin },   // config entry added or pending→active
    PlugDisabled  { id, origin },          // config entry removed
    PlugUpdated   { id, heads, origin },   // explicit re-pin only
    PlugsConfigChanged { heads, origin },  // the plugg config facet moved
}
```

Known-but-disabled manifest changes are invisible to the switch — they only
move the facet index. `events_for_init` synthesizes from the plugg config
facet, emitting `PlugEnabled` for the active set only (pending plugs emit
when they resolve, §6), instead of replaying the app-doc store.

### 8. Import paths

* **`import_from_oci_registry` / `import_from_oci_layout` stays**, re-purposed
  as *authoring*: pull artifact → store layers as blobs (`db+blob://` component
  URLs) → **write a manifest doc** (drawer `add`) → optionally enable. OCI
  remains the out-of-band authoring channel for third-party tools; distribution
  thereafter happens through the hive, not the registry.
* **`import_from_doc_id(doc_id, heads)`** — the new native path: read the
  manifest facet at the given heads (`drawer::get_doc_with_facets_at_branch_heads`),
  `validate_incoming_plug`, grant core-docs-group access
  (`grant_docs_admin(&core_docs, [doc_id])`) so the doc replicates in the core
  partition, and enable at those heads.

### 9. `daybook_cli` plug commands

New top-level `Plugs` subcommand (mirroring the `Devices` nested-command
pattern in `StaticCommands`):

```text
daybook plugs list                  # table of known plugs: id, version, status
                                    #   (disabled | enabled | pending), pinned
                                    #   heads vs latest heads, config doc id
daybook plugs show <plug-id|ref>    # full manifest summary + facets + refs
daybook plugs enable <facet-ref|doc-id> [--heads H]   # validate, grant core-group
                                    #   access, pin (default: current main
                                    #   branch heads)
daybook plugs disable <plug-id>     # remove from the enablement map (rejected
                                    #   for @daybook/core)
daybook plugs update <plug-id>      # explicit re-pin to latest main-heads
                                    #   (fails if re-validation fails)
daybook plugs pending               # enabled entries whose doc/heads are not
                                    #   locally readable (config/manifest race,
                                    #   §6)
daybook plugs import <target> [--no-enable] [--heads H]
                                    # one command, target resolved by shape:
                                    #   db+facet://<doc>/<tag>/<id> or bare doc
                                    #     id → import from an existing doc
                                    #     (validate, grant core-group access,
                                    #     enable by default; --no-enable skips)
                                    #   oci://<registry-ref> or a local OCI
                                    #     layout path → authoring import (pull
                                    #     artifact, write a manifest doc;
                                    #     --no-enable leaves it known only)
```

Notes:

* list / show read the facet index (known) + the plugg facet (enabled) and
  resolve each enabled ref at pinned heads; pending entries need no local doc.
* `enable`/`disable`/`update` write the plugg config facet through the drawer
  (validated as any facet write); `disable` on `@daybook/core` is rejected by
  the config mutation (§3).
* `import` disambiguates by shape: `db+facet://…` / bare doc id → doc
  import; `oci://…` / local OCI layout path → authoring import. Ambiguous
  targets (a local path that could be a doc id) are resolved by explicit
  flags, not heuristics.
* Authoring a full plug with a wasm bundle is the OCI import path; a future
  plug can do the same in-app by writing manifest facets (§8).

### 10. The app doc and future direction

The app doc shrinks: `PlugsStore` retires; `ConfigStore` and `TablesStore`
follow to drawer docs (in flight, "halfway done"). This ADR does not move
config wholesale — it only pins the repo config doc as the official home of
synced config facets and puts the plugg facet there.

## Consequences

### Positive

* Manifests are drawer docs: learned via drawer sync, pullable via relays
  (KeyHive pull semantics); no out-of-band registry needed for distribution.
* User authoring becomes ordinary drawer writes with facet validation, dmeta
  author signatures, and fork/branch history for recovery; a plug that creates
  plugs is just a plug writing manifest facets.
* Enablement is declarative config with pinned refs: robust against manifest
  churn and sync races, no index dependency for enablement, explicit updates.
* Runtime gating matches "known ≠ loaded": processors, inits, commands, and
  facet validation all enable-driven.
* Event rework is cheap: two use sites, both thin.

### Costs & Trade-offs

* The initial manifest write (core doc at repo init) is a system-bootstrapped
  write, not a normal validated drawer write.
* `PlugsRepo`'s read path becomes derived (drawer + facet index) instead of a
  single automerge map: more moving parts, at-heads reads for every enabled
  plug.
* One config rule exists: `@daybook/core` cannot be disabled (enforced in
  the plugg facet mutation, §3). Everything else — including core manifest
  updates — is ordinary; no runtime special cases.
* URLs carry heads/branch; `daybook_types::url` needs `at`/`branch` parsing
  that does not exist yet.
* No migration: existing repos keep their app-doc store (frozen) or are
  re-imported manually via OCI authoring.

### Deferred

* The bigger switch rework (upcoming PR) — this ADR only redirects events to
  enabled plugs.
* Full `ConfigStore`/`TablesStore` move to drawer docs.
* Relay-side behavior: manifest docs rely on existing sponsorship/relay access;
  no relay protocol change here.

## Open Questions

1. `plug_config_doc_ids` location — kept in the plugg facet (§2) vs a ref facet
   on the manifest doc itself. Kept simple here; revisit when config docs gain
   ref semantics.
2. Whether `PlugUpdated` carries only the new heads or also a diff against the
   old pin — the switch only needs heads today.
