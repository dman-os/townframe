# ADR 053: The core cartridge: the node's built-in surfaces ship as a cartridge

**Status:** Accepted (2026-11-24). Proposed by the nodules lane from the operator's decision of 2026-11-24 ("let us indeed try
to make the micros layer itself a cartridge", ADR 052); the operator accepted it as described. §9 records which items the
operator settled. Decisions still marked **To confirm** in the body and not settled in §9 remain open.

**Amends:** nothing yet; it is the home ADR 052 §2 and the ledger's "core cartridge" to-do point at.

Related: ADR 052 (the micros nodule that lives here), ADR 050 §2 (the NSID rule), ADR 047 (manifest, permbinds,
initializers), ADR 044 (installs and pins), ADR 028 (identity, monotone upgrades), ADR 049 §6.1 (runtimes grant
themselves first), ADR 021 (wire surfaces and their evolution check), `docs/scratch/nsid-audit.md` (item 6).

## 1. Problem

The node's own vocabulary is a list in the binary, not a cartridge.

- `node_type_libraries()` (`big_cabinet_core/reference/defs.rs:25`) returns the type libraries the node itself provides:
  today one, `org.example.daybook.defs` (`datumRef`, `datumLocation`, `datumPin` and the rest of the reference defs). Every
  consumer that resolves a cartridge's references reads this one list. The readers (`git grep node_type_libraries`):
  - `big_cabinet/schema.rs:83`, `:271` and `:314`: the built-in datum schemas carry it when they are compiled; admission of
    a registration group refuses a vendored copy that differs (`ConflictingDefs`) and carries the node's copy for every
    registration, so any datum Lexicon can refer to it.
  - `big_nodule_core/lib.rs:877` (`provided_library`, used at `:517` and `:788`): a manifest's Lexicon files and
    references are validated against it, so a nodule's reference into the defs resolves without being requested and a
    vendored copy must equal the node's.
  - `big_dispatch/router.rs:394`: dispatch compiles each operation's input and output contract with it carried, and
    replaces a provider's copy with the node's.
  - `wire_surfaces/snapshot.rs:126`: the checked-in wire-surface snapshot lists it as owned by `big_cabinet_core`.
- The other built-in surfaces are special cases in the same way: datum tags the node's services declare in Rust
  (`big_node::schema_groups()`), host-internal tags with no Lexicon behind them (`docs/scratch/nsid-audit.md` item 6), and
  methods the nodules host serves. They obey a different set of rules than a cartridge's contracts do: no
  version, no install record, no evolution check except the snapshot's, no way to see them from `day carts`.
- ADR 052 wants the micros layer to be a nodule. A nodule needs a cartridge to live in, and the node's own is the natural
  one, but there is none.

## 2. Decision: one preinstalled cartridge

The node's built-in surfaces that *can* be a cartridge ship as one: the **core cartridge**, id `<the node's publisher DID>/core`
(the id's authority is the node's own, ADR 028). The id differs per node. Nothing references the core cartridge by id: other
cartridges reference its Lexicons by NSID, so the difference is harmless. There is no build-time signing key; the node
publishes its core cartridge like any publisher (§3 step 3). It carries a manifest like any other (ADR 047 and ADR 050): shared
infrastructure (Lexicons, components, permbinds, settings, grants) and a list of surfaces, which today is one nodule, the
`micros` nodule of ADR 052. Its Lexicons are the source `node_type_libraries()` reads. The rest of this ADR says how that
works without a loop, and which special cases move.

## 3. Bootstrap

The defs are needed to admit any cartridge's Lexicons, the core cartridge's own included, and a cartridge is admitted
through the registry whose datums are themselves admitted against Lexicons. So the first revision cannot be read from
anything the node has to admit. The rule that breaks the loop is: **admission never reads the core cartridge from the
store; it reads the copy compiled into the binary.**

1. **The core cartridge is built with the node.** Its manifest, Lexicon files, sidecars and wasm component are produced at
   build time and embedded (`include_bytes!` for the component and a generated Lexicon set; the same Rust declarations
   that produce `defs_lexicon()` today are what generate the files, so the source of truth stays one definition). The
   embedded package has one revision, the node build's.
2. **`node_type_libraries()` stops being a free function over a static and becomes a view of that embedded package.** It
   is a value (`TypeLibraries`) built once from the embedded Lexicons and passed to the readers listed in §1: the
   `SchemaRepo` carries it for admission and compiling (`big_cabinet/schema.rs`), `big_nodule_core` validation takes it for
   `provided_library`, the dispatch router takes it for contract compiling, the wire-surface snapshot walks it. Nothing
   consults the store. (Today a global is read from four crates; that the readers are many is why they receive the value,
   not why it stays global.)
3. **At node creation** (`day node init`, or the first boot of a directory with no node state), the node installs the
   core cartridge: it writes the install datum (`org.example.daybook.cartridges.install/<core id>`, ADR 044) pinning the
   embedded revision, publishes the revision to the registry from the embedded package (the same envelope and artifact
   blobs a person's `installCartridge` would write), and writes the grants the manifest declares. This is the one install
   the node performs with no operator present: it is signed by the node's own identity (the seed keyring), the way the
   service actors' durable grants are made at boot. Finding from implementation: the id's namespace is the node's publisher
   DID, so no node-build DID or build-time key exists. The node already signs unsigned envelopes in its own namespace with
   its own key (`src/big_cartridges/lib.rs`, the `publish` handler calls `envelope.sign_with(...)` when
   `envelope.id.namespace == self.publisher`), so publishing the core cartridge is an ordinary publish by that publisher.
4. **Admission of the core cartridge's own Lexicons** is the bootstrap case, and it is the only one: `SchemaRepo` is seeded
   from the embedded package before the cabinet or the registry exists, as `big_node::schema_groups()` seeds the built-in
   datums today. The registry then admits the core cartridge's *published* Lexicons (step 3) against the same embedded
   libraries, which is trivially consistent because they are the same bytes. After step 3 the store holds a record of what
   the node provides; admission still reads the binary. The record is for `day carts`, the wire-surface snapshot, other
   nodes' audit and the person's view, not an input.
5. **Pinned to the node's build, never uninstalled, never disabled.** The core cartridge's install is
   not a person's choice: `uninstallCartridge`, `disableCartridge` and `setCartridgeGrant` on it are refused with a typed
   error, and the install datum is rewritten to the embedded revision whenever it differs (§4). A person who wants a
   node without it has a different node build.

Everything in step 3 is also what makes the core cartridge's grants honest: the runtime permbind ADR 052 §2.2 item 2 needs
is declared in its manifest and written by the node at install, not by a person, so it never appears as a question.

## 4. Upgrades

A new node build embeds a new core cartridge revision. At boot, before any other cartridge is reconciled:

- **The embedded revision is newer than the install's pin** (the normal case after an upgrade): the node publishes the
  new revision and rewrites the install datum, under ADR 028's monotone rule (same id, higher version, grants never
  widen silently). The node is both the proposer and the only party that can accept, so "never widen silently" becomes
  "widening is stated in the release's notes and the wire-surface snapshot diff", reviewed in the PR that bumps the
  revision. The core cartridge's nodule is re-hosted from the new component by the normal reconcile, and dependent
  state (the micros nodule's handle revision) is rebuilt lazily by the next `micros.awaitHandles`.
- **The embedded revision equals the pin**: nothing.
- **The embedded revision is older than the pin** (a downgrade of the binary): the node refuses to boot with a typed error
  naming both revisions. The refusal falls out of the registry's existing `Downgrade` rule at publish
  (`CartridgesError::Downgrade` in `src/big_cartridges_core/protocol.rs`), so no separate check is written. The content of
  a version is pinned by a unit test, so a change to the embedded content forces a version bump. A downgrade cannot un-admit Lexicons other cartridges already depend on (ADR 028: a breaking
  change is a new NSID), and the Lexicon evolution rules (ADR 021) only run forward.
- **Existing nodes, the first time**: a node created before this ADR has no core install. It takes the "no pin" path of
  §3 step 3 at its next boot. There is no migration of data, because the core cartridge's Lexicons are the defs the
  node already admits; the install datum and registry entry are new records. Nodes that sync (a sister's drawers) see the
  install datum arrive as an ordinary config document write, which is node-local (ADR 044: config is node-local, syncing it
  is backup and never activates anything), so a sister's different core revision never changes this node's.

## 5. The micros nodule lives in it

ADR 052 §2's `micros` nodule is the core cartridge's one nodule surface. It gets from this ADR a home with an actor, a
manifest to declare its permbinds in (§2.2's runtime permbind among them), and a version the node's build pins. The
nodule's wasm component is built by the same pipeline as other nodules (`cargo x plug build-oci`) and embedded.
Building it as a prerequisite of building the node, rather than as a test fixture, is a build-order change the guardrails
and CI have to learn (the wasm guests live in the `src/wasm_utils_rs` workspace). Settled by the operator: the
component is **embedded** in the node binary, because the GUI must work standalone with no network at startup. Fetching it
from a pinned OCI digest at first boot is rejected for that reason: it avoids a build dependency but makes node creation
depend on the network.

## 6. What else could move in: the catalogue

For each special case: whether it moves into the core cartridge, and why. "Moves" means its definition is published as the
core cartridge's Lexicon or surface; admission and bootstrapping of whatever the cartridge system itself stands on stays
in the binary (§3).

| Special case | Verdict | Why |
|---|---|---|
| `org.example.daybook.defs` (the reference defs) | **Moves** (first) | The reason for this ADR. Its definition is the core cartridge's Lexicon; `node_type_libraries()` becomes its view. |
| Cabinet core datums (`Dmeta`, `BranchPath`, `Drawer`, the drawer roster and child entries, meta doc, known drawer, successor: `big_cabinet/schema.rs` `CORE`) | **Published in the core cartridge, admitted from the binary** | The cabinet cannot start without them: they define the drawers the registry itself is stored in. Their Lexicons are generated from the same Rust declarations and appear in the cartridge for other cartridges to reference and for the evolution check, but the bootstrap path of §3 stays Rust. **To confirm.** |
| Registry and cartridge identity datums (`REGISTRY_UNIVERSE`, `big_cartridges::schema_groups()`) | **Published in, admitted from the binary** | Same loop: the registry cannot admit the datums it stores installs in from a cartridge installed through it. |
| Host-internal datum tags: `org.example.daybook.cartridges.install/<id>`, `.grant.{drawer,cartridge,net,method,authority}/<key>` (`big_cartridges_core/config.rs:57-201`), the sidecar tag `org.example.daybook.lexicon.sidecar` (`big_cartridges_core/registry.rs:60`), `LEXICON_TAG`, `COMPONENT_TAG` | **Moves, as real Lexicons, under ADR 050 §2's rule** | `nsid-audit.md` item 6: record-kind names with no Lexicon behind them. ADR 050 §2 says an NSID names a Lexicon and a thing gets one only if a different cartridge could provide it. These are *schemas of records the node reads* (substitutable records other tools write: a grant is authored by `day`, the GUI or another client). So they publish Lexicons in the core cartridge. The tags that are only a registry-internal kind with no second author (the sidecar payload, the component tag) may instead leave the NSID space for their own id type, as the audit's direction says: **To confirm** per tag. |
| The core cartridge's own manifest datums | **Stays** | A manifest is the cartridge system's schema; making it a Lexicon *of* a cartridge is the loop again. |
| Built-in methods of the node's native services (authority, dispatch, cabinet drawers/docs/blobs, `nodules`, `cartridges`) | **Stays native; Lexicons published** | They are the node itself. Dispatch already emits each method's Lexicon (D41); publishing those in the core cartridge puts them under the same evolution check and lets a cartridge reference a method's types without a vendored copy. The implementations are not guests. |
| `micros.*` | **Moves** (ADR 052) | The first non-native surface. |
| `listCartridgeGrants`, `awaitCartridgeGrants`, `installCartridge` and the other cartridge-management methods | **Stays native** | The registry and the enable/disable/uninstall decisions are the node's authority: a guest cannot be the thing that grants guests. They are what the micros nodule's runtime permbind reaches. |
| `nodules.setMethodRoute` and the other privileged routing methods (ADR 045) | **Stays native** | Routing is dispatch's. |
| The sidecar overlay mechanism and the `x-lexicon-ref` annotation | **Stays** | A rule of the Lexicon system, not a surface. |
| Host-side collection store (Clay) | **Stays in the GUI** | Clay state (ADR 052 §9). |
| The daytxt cartridge's built-in micros | **Separate cartridge** | They are an app's, not the node's; ADR 052 §6. The core cartridge is what the node can run without an app. |
| Future built-in nodules (a node-side scheduler, a settings surface) | **Move when they exist** | The core cartridge is where the node's non-native, always-present surfaces go, for the same reasons as `micros`. |

The split's rule, in one sentence: **a thing moves in when it is a schema others reference or a surface that is not what
the cartridge system stands on; what the cartridge system stands on stays native but publishes its Lexicons here.**

## 7. Costs and risks

- **Build order.** The node binary depends on a wasm build (§5) and on a generated Lexicon set. Both are build-time
  products of the same sources; the guardrails matrix and CI must build them before the node.
- **A second copy of the truth.** After §3 step 4 the store and the binary both hold the core cartridge. The binary wins,
  and the store's copy is rewritten whenever they differ, so a divergence is repaired and not argued over. An e2e test
  pins that a hand-edited install is rewritten.
- **A cartridge people can see but not change.** `day carts ls` lists it; its grants are fixed. The person's consent and
  audit view (ADR 049 item 6) shows it as "the node itself", with its permbinds (the runtime read of installs and
  grants) stated, since a cartridge that can read every other cartridge's grants is the one a person should be able to
  audit.
- **Wire-surface snapshot ownership.** The snapshot attributes the defs to `big_cabinet_core` today
  (`wire_surfaces/snapshot.rs:126`). It becomes the core cartridge's, and the evolution check (ADR 021) is what reviews a
  change to it.

## 8. Order of work

Each layer is one small PR. Nodules lane.

1. `TypeLibraries` as a value the readers receive instead of the free function (no behaviour change). Four crates change
   together because the interface breaks; that is intended.
2. The embedded core package: generated Lexicon set, manifest, install at creation, the refusal rules, the upgrade path.
3. The runtime permbind and descendant-scoped authority (ADR 052 §2.2).
4. The micros nodule (ADR 052).
5. Publish the host-internal tags as Lexicons, and the cabinet/registry datums into the cartridge (§6).

## 9. To confirm

1. **Bootstrap rule** (§3): admission reads the binary's embedded copy, never the store; the store's record is for audit
   and is rewritten when it differs.
2. **Who signs the preinstall** (§3 step 3): settled, the node's own identity. The core cartridge's id is
   `<the node's publisher DID>/core` and the node signs it like any publisher.
3. **Downgrade refusal** (§4): a binary older than the installed core revision refuses to boot, by the registry's existing
   `Downgrade` rule at publish.
4. **Embed the micros component, or fetch a pinned digest** (§5): settled by the operator, embed (the GUI must work
   standalone with no network at startup).
5. **Cabinet and registry datums are published in the core cartridge but admitted from the binary** (§6).
6. **The host-internal tags**: real Lexicons for the ones other tools author (the install and grant records), a separate id
   type for the ones with no second author (§6, `nsid-audit.md` item 6).
