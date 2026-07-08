# FDR 001: Dpaths — Paths, Tags, and Filesystem Projection of Daybook Docs

**Status:** Draft. Resolved-in-review decisions are folded into the body;
remaining open questions are collected in [Open Questions](#open-questions)
and tagged inline as `Q<n>`.

Companion documents (planned): FDR 002 (vocabulary — nodes, drawers,
checkouts), FDR 003 (version-control primitives across CLI and GUI), FDR 004
(workspace CLI experience), ADR 010+ (pauperfuse virtual tree store,
checkouts & reconciliation, lenses, blob strategy).
ADR 003 and ADR 001 cover cipherblobs and blob inventory/pins respectively;
this document is the logical addressing layer those physical layers slot
under. Note: throughout, the library is called **pauperfuse** (full name).

---

## Context

Daybook documents live in a CRDT-based node with **no global arbiter**:

- Documents are identified by unique doc ids, but the docs themselves are made
  of unordered facets. Two replicas can concurrently assign _any_ metadata to a
  doc; the CRDT guarantees convergence, not uniqueness.
- There is no write gate that could enforce "only one document may live at
  `/notes/foo.md`". Any peer may write anything at any time, including
  concurrently with another peer.
- Causal consistency means path-assignment facts arrive in arbitrary order and
  may be temporarily invisible on a partitioned device.

Yet every downstream consumer of daybook — the CLI, an Obsidian vault import, a
DCIM photo folder, a wasi sandbox, an LLM agent, mobile app file pickers —
speaks the language of **paths**. The path is the universal interop surface
("interop is the main usecase"), so we need a path system that:

1. survives concurrency without needing uniqueness;
2. is user-facing (non-technical users, mobile apps, GUI and CLI alike);
3. works as a tagging/filtering system (e.g. "everything under `/inbox`");
4. maps deterministically onto real filesystems;
5. stays out of pauperfuse's way: pauperfuse is daybook-object-model-agnostic,
   so dpaths must be expressible as ordinary opaque object references to it.

Prior art consulted: Ink & Switch **Patchwork** — its drafts plugin
(`patchwork-base/drafts`) demonstrates branch bookkeeping as clone-docs + heads
(`clonedAt`/`mergedAt`), frozen checkouts as url→heads maps (`DraftCheckpoint`),
and cross-doc derived timeline caches with heads cursors
(`ChangeGroupDoc.computedThrough`). That prior art informs FDR 003; dpaths here
are daybook's analog of its _pointers_: stable, app-agnostic addresses of
content.

---

## Decision

### 1. Dpaths are a label set with path syntax

A dpath is a UTF-8 string that looks like a Unix path:

```
/inbox/hello.md
/DCIM/2026/06/IMG_1234.jpg
/projects/daybook/design.md
```

but it is a **label**, not a filesystem position. Specifically:

- **No uniqueness constraint.** Any number of documents (or facets) may claim
  the same dpath, concurrently and from different devices.
- Labels form a **derived tree**. `/a/b` may exist without `/a`; when it does,
  `/a` materializes as a bare (implicit) directory.
- A dpath is simultaneously a path-like address and a tag. Because the namespace
  is a label set, "tag assignment" is the degenerate case: a tag is just a dpath
  that happens to have no file under it (see [Tags](#tags)). No separate tag
  mechanism is needed for v1.

Syntax rules:

- Segments are separated by a single `/`; no empty segments (no `//`, no leading
  or trailing slash **inside** the dpath _value_).
- Segments MUST NOT be `.` or `..`.
- Segments are UTF-8 strings. No percent-encoding inside daybook.
- **Comparison is byte-exact** (no case folding, no Unicode normalization — see
  note on normalization below). Everything downstream treats dpath strings as
  opaque UTF-8 byte sequences.
- **No reserved namespaces in daybook.** Dpath labels are not restricted. The
  special surfaces a checkout creates (`/by-id`, the checkout metadata
  directory, etc.) are instead addressed at the **materialization layer**, by
  collision resolution: a dpath claim on a reserved name loses its literal real
  path to the reserved surface and spills into the suffix rules, e.g.
  `/by-id.d/…` (see §5). Nothing in daybook itself forbids or rewrites such
  claims.
- **Per-platform materialization adjustments are in scope.** Case-insensitive
  filesystems and NFC/NFD normalization (macOS is NFD) are materializer
  concerns: daybook stays byte-exact UTF-8, and the FS materializer adapts
  explicitly per platform, consistently. The materialized tree must remain
  honestly usable *as a filesystem* — copyable, deletable, consumable by any
  other program (a user deletes a file, it is gone from disk; another app owns
  the directory, daybook coexists). That interop requirement is why the
  materializer layer — not the dpath namespace — carries normalization and
  case-degradation behavior.

### 2. The dpath facet

A dpath is declared by a facet in the claiming document:

```jsonc
{
  // Full-doc claim (document IS the thing at this path):
  "org.example.daybook.dpath//inbox/hello.md": {
    // empty object = whole-document reference is implied
  },

  // Selective claim (only these facets materialize at this path):
  "org.example.daybook.dpath//photos/beach.jpg": {
    "targets": [
      { "facetRef": "db+facet://self/org.example.daybook.blob/main" },
      {
        "facetRef": "db+facet://self/org.example.daybook.imagemetadata/main",
        "refHeads": []
      }
    ]
  },

  // Cross-doc claim: assign a dpath to a doc we may not have write
  // access to (read-only adoption). The dpath facet lives in OUR doc;
  // the target may be any doc in the node.
  "org.example.daybook.dpath//DCIM/other.jpg": {
    "facetRef": "db+facet://<doc-id>/org.example.daybook.blob/main",
    "refHeads": ["<hash>"]
  }
}
```

Decisions locked:

- **Key-id = the dpath string itself** (with a leading `/` to disambiguate from
  other facet key-ids, since real dpaths always start with `/`).
  - This gives **one dpath facet per exact dpath string per doc**. The
    consequence the design leans on: concurrent assignment of the same dpath to
    the same doc converges into a single merged facet rather than creating
    duplicates. This also makes **import idempotent by construction**: importing
    the same camera folder on two devices concurrently produces
    map-key-identical facets that merge cleanly.
- **Multiple dpaths per document**: allowed and first-class. A photo can live at
  `/DCIM/2026/06/IMG_1.jpg` and `/favorites/IMG_1.jpg` simultaneously. Each is
  an independent facet under its own key; adding one never disturbs the others.
- **Default scope = whole document.** If the facet value carries no targets (or
  no value at all), the lens set materializes every user-visible facet of the
  doc at that path.
- **Selective scope = target list.** Each entry references facets by the
  existing `db+facet` URL machinery, including the empty-heads same-transaction
  convention from dict.md.
- **Cross-doc targets are allowed.** A dpath facet may reference facets of _any_
  document. This deliberately enables "quick and dirty" assignment of dpaths to
  docs the assigning replica cannot write to. The cost: path resolvers must
  model **"target not found"** as a normal state (see Materialization states).
  Expected to be rare; kept because it makes read-only adoption trivial.
- **Dpath facets are plain facets.** They receive the same sync, merge,
  conflict, and history treatment as everything else, and are themselves
  addressable in the facet-reference graph.
- **The dpath facet stays a pure address.** No lens hints, no mime expectations,
  no lens parameters ride inside it. Lens customization, when needed, is a
  **separate facet** that the lens layer resolves against the dpath — see
  [Lens interaction](#9-lens-interaction-pointer-to-adr-012) and ADR 012.

### 3. Materialization: the dpath tree maps onto a real FS deterministically

The mapping from dpath labels to concrete filesystem paths must be
**deterministic as a function of (claimant set, checkout binding state)** — a
checkout can always be rebuilt and reconciled from materialized content *plus its
recorded bindings* (three-way detection needs reproducibility, scoped to the
checkout that owns the binding state). Fresh checkouts with no binding history
converge across devices via shared tiebreakers — see [Binding
stability](#binding-stability) below.

Three rules define the mapping. They compose by recursion: apply the rules
segment by segment down each materialized directory.

**Rule 1 — many-claimants (two or more objects claim the same dpath).** The
conflicting dpath materializes as a directory. Each claimant gets a
deterministically-unique entry name inside it. The unique name is derived from
the claimant's object identity (doc id, plus facet key-id for selective claims —
naming scheme is open: Q1). Two notes both claiming `/inbox/hello.md`
materialize as `inbox/hello.md/<name-a>.<ext>` and `inbox/hello.md/<name-b>.<ext>`;
the dpath's extension is preserved in the derived names (see Q1).

**Rule 2 — file/dir collision (one object claims dpath `P` as a file, another
object's children live under `P`).** The file keeps the accurate name `P`; the
divergent children materialize under `P + .d` (e.g. file at `/a`, file at
`/a/b` → `/a` and `/a.d/b`).

**Rule 3 — `.d` stacking (the total order).** Rule 2 can itself produce a new
claimant collision (a third object claims the literal dpath `/a.d`). Conflicts
resurface as more suffixes: the mapping keeps appending `.d` until every
claimant has a unique real path; **the claimant with the most `.d` suffixes
loses its name first** — concretely, the real-FS path `/a.d` is awarded to the
_collision directory_ of `/a`'s rule-2 spill, and the object claiming literal
dpath `/a.d` is demoted to a deeper suffixed name. The invariant to hold is:
the procedure terminates and is order-independent (Q2: proof + worked examples;
Appendix A).

Two consequences worth restating because they are load-bearing:

- **Prefix-only paths exist.** `/a/b` may be claimed with no claimant for `/a`;
  the materialized tree contains a bare `a/` directory.
- **No rename atomicity.** Moving an object = adding a new dpath facet and
  (separately) deleting the old one. Between the two, the object materializes in
  _both_ places. The checkout's reconciliation layer (FDR 002/ADR 011) may
  choose to collapse add+remove of an identical object into a rename when both
  arrive in one detected change, but the CRDT layer provides no such atomicity
  and nothing may assume it.

#### Binding stability

Path stability over time — the interop property ("the file at this path today is
at this path tomorrow") — is a per-checkout property, and bindings are sticky:

- **A materialized binding is never rewritten.** Once a claimant is bound to a
  real path in a checkout, later arrivals (lexicographically smaller,
  earlier-dated, whatever) must NOT rename it. New claimants take the clean name
  only if it is free; otherwise they follow the suffix rules. Deletion of the
  current holder promotes the next claimant into the path — the one unavoidable
  churn event, caused by an actual delete.
- **Tiebreaker chain for assigning a _new_ binding:**

  1. already bound in this checkout → keep (stickiness; effectively
     first-come-first-served for the checkout);
  2. dpath-assignment timestamp (the dpath facet's change metadata /
     `dmeta.createdAt` — shared state, so fresh checkouts on any device agree;
     clock skew only degrades intuition, never correctness — every replica
     computes the same function of the same bytes);
  3. object id, lexicographic (final fallback).

- **Cross-device divergence is presentation-only.** Two checkouts may bind the
  same dpath's claimants differently (different stickiness histories, clock
  skew, holder deletions at different times). Content never flows through
  filesystem paths: edits sync by object identity and dedupe by digest.
  Documented consequence: rsync-ing the raw tree between devices can observe
  renames that daybook sync itself never performs.
- **Checkout specs may pin the policy** (default: sticky + timestamp) — e.g. an
  Obsidian-vault checkout may forbid clean-name stealing entirely, a diff view
  may sort purely lexicographically. One knob, narrow first; per-projection rule
  DSLs are explicitly out of scope for v1.

### 4. Conflicts are opinions, not errors

Automerge can merge anything the JSON schema permits — including values whose
_combination_ is semantically broken or surprising. Per the project's
error-handling philosophy, we do **not** gate merges. Adopted rule (agreed in
design discussion):

- A CRDT merge always proceeds. If the merged facet state is semantically wrong
  (two conflicting values automerge keeps as a conflict, a shape a lens can't
  parse, a broken intra-doc reference), the **facet is flagged as in-conflict
  and the document keeps moving** — other facets remain fully usable, exactly
  like jj keeps conflicts inside working-copy content without blocking other
  operations.
- The conflict is _an opinion about the data, not a low-level error_. The
  materializer renders conflicted state in-place (conflict markers for text
  lenses, e.g. `<<<`/`>>>` blocks in markdown, per-lens), and the CLI offers
  adjustment tools to repair. Resolution writes new changes; it is never a
  special protocol event.
- For dpath _assignment_ specifically: the CLI's reconciliation resolves
  claimant collisions deterministically per
  [Rule 3](#3-materialization-the-dpath-tree-maps-onto-a-real-fs-deterministically)
  at the next transaction, so dpath-level conflicts never require user
  arbitration — only _facet-content_ conflicts can surface as visible conflict
  state.

### 5. Reserved materialization surfaces (`.dnode`, `.dtree`, `/by-id`)

The checkout creates special filesystem surfaces that are *not* dpath claims:

- the checkout state directory (`.dtree` — per FDR 002) and, where the tree
  roots a node, the node directory (`.dnode`), holding the pauperfuse
  transactional state;
- `/by-id/` — materialization of **dpathless documents** (docs with no dpath
  facets) as full JSON reprs. Primary consumer: the **wasi virtual filesystem**,
  giving LLM agents raw JSON access to documents. Documents with dpaths do not
  appear under `/by-id` in v1.

Handling of the interaction with user dpath claims (locked in review):

- **No namespace reservation.** A dpath claim on `/by-id/…` (or on the checkout
  directory's name) is a perfectly legal dpath.
- **Reserved surfaces win their literal name via Rule 2.** The reserved surface
  acts as the winning "file" at that real-FS position; dpath claimants of the
  same name spill into the suffix rules and materialize under the `.d` suffixed
  path (e.g. `/by-id.d/…`). The exact name of the metadata directory is an FDR
  002 decision; the rule is fixed regardless of the name chosen.
- **Degraded/absent states need not be representable on the filesystem.** The
  placeholder states below exist in the pauperfuse *transactional store*; what
  the filesystem itself shows may be coarser (e.g. an empty placeholder file).
  The **driver** — CLI, watch daemon, wasi bridge, app — is responsible for
  surfacing these states via status queries, not by inventing filesystem
  encodings.
- **Nested checkouts** (a dpath-materialized subtree inside a checkout looking
  like an importable folder, `.dnode`/`.dtree` dirs inside adopted trees) must not be
  recursively re-adopted/imported: by-id materialization is skipped beneath
  dpath-materialized subtrees, and import/adopt must detect and refuse or
  re-bind nested checkout metadata. Spec owned by the pauperfuse ADRs (008/009).

### 6. Degraded availability states are first-class

Because dpath facets and the content they reference are separate facets that
sync independently, a materialized path may exist while its content is missing
(blob not yet synced — see ADR 001's inventory model — target facet not found
for cross-doc refs, or facet in conflict). The checkout represents these as
**normal, first-class states**, never errors:

- `missing-content` — path materialized as a placeholder; recovery (re-sync,
  restore from blob backup) fills it in later. Mirrors the "totally tolerant
  about missing blobs" requirement.
- `target-not-found` — cross-doc dpath pointing at a facet that doesn't resolve
  (yet or ever). Materialized as placeholder + status entry.
- `in-conflict` — per §4.

These states live in the local transactional store and surface through the
driver's status surface (CLI, app UI), not through special filesystem gadgets —
see §5.

### 7. Adoption and import

Interop case studies adopted (from the smooth-transition discussion):

- **Obsidian vault**: `db import .` / `db adopt .` treats an existing directory
  as a materialized checkout, assigns dpaths from origin paths, and wires
  bidirectional sync. Existing files keep their paths; daybook object identity
  is created behind the scenes.
- **DCIM / media libraries**: import assigns dpaths from origin paths
  (`/DCIM/...`) and — critically — **adopts blobs in place** (external tracked
  blobs, hardlinks/reflinks; no byte duplication). Detail in the blob-strategy ADR 013; this
  document only fixes the addressing consequences.
- **Assignment**: each adopted file becomes a **new document** (new doc id)
  claiming its origin-path dpath. The default is one-doc-per-file; the "adopt
  into a single document" mode (one CRDT history for a whole directory tree —
  the code-source-control case) is supported by the same primitives: one doc,
  one dpath facet per file, each targeting the specific facets that represent
  that file. Facet-level granularity for this mode needs its own spec (Q4).
- **Adoption collisions follow the generic rules, not a special policy.**
  Adoption first creates the candidate docs, then hands resolution to the
  transactional layer, which materializes under the Rule 1–3 suffix procedures
  and records resolvable status entries. The expected common shape —
  "adopt this folder under the dpath subtree `/hello/`" — makes collisions
  rare, so we start with this uniform semantic and relax later only if the UX
  demands it (no hash-compare takeover special case in v1).

### 8. Deletion and garbage collection (assumption)

Deletion of a dpath facet unassigns an address. What happens to
no-longer-referenced documents, branch docs, and unreachable bookkeeping CRDTs
is a **garbage collection system** to be specified (Patchwork's lesson: their
`onDeleteDraft` unlinks bookkeeping from the parent's `drafts` list and leaves
the CRDTs in place — deletion = unreachability, not physical removal). This FDR
assumes a GC layer exists and defines dpath deletion as "stop materializing
here"; the GC's scope, triggers, and guarantees are owned by FDR 003 and
ADR 011.

### 9. Lens interaction (pointer to ADR 012)

- The dpath **extension is a lens-selection hint**; MIME types from
  `blob`/metadata facets are the other input; plugs register lenses and may
  inspect both. Which hint wins for which format is ADR 012 material.
- A full-JSON materialization lens (rendering the entire doc as JSON, used by
  the by-id view and available under dpaths as a sidecar for power users) is one
  of the first lenses.
- Selective dpaths referencing facets across docs make the lens resolution step
  potentially multi-document; lenses must accept a target set, not a single doc.
- **One dpath → an entry *set*.** A single lens's output over one object can be
  multiple files (note + sidecars, thread + attachments). All naming
  *within* one lens's output — including lens customization via separate facets
  (Q: how, exactly — ADR 012) — is lens territory. Only collisions *between*
  entry-sets of different claimants follow the dpath rules here; whether Rule 1
  also covers intra-doc entry-set clashes is an ADR 012 open problem. The dpath
  layer fixes only the boundary: dpath → object → entry-set, and the collision
  order between distinct claimants.
- **Multiple lenses may target the same dpath source**; constraints here are
  expected to emerge from real use rather than being bounded now.

---

## Tags

Tags are the superset case: a dpath label whose tree position is conventional
rather than structural (`/inbox`, `/tagged/urgent/2026` or any agreed
convention). Nothing in the addressing scheme treats any subtree specially;
filtering by prefix is the tag query. The design consequence is deliberate:
**tagging tools can be built entirely on dpath assignment + prefix queries**
with no additional mechanism. Separately, plugs can define their own tag-like
facets (e.g. automatic hashtag extraction for Obsidian markdown is already
planned); when a full **query system** lands, materialization selection
(checkout filtering) rides **on top of it**. Dpath prefix-queries remain the
zero-infrastructure path for v1 filtering.

---

## Open Questions

Remaining after the first review round; each tagged with what it blocks.

1. **Unique-name derivation inside Rule 1 directories.** Locked lean: preserve
   the dpath's extension in derived names; give the lens some control (since
   multiple lenses may emit for one dpath source); stay unbounded about the
   exact scheme until constraints emerge from real lens behavior. Must be
   stable, collision-free, user-legible (`ls` output) and adoption-round-trip
   safe. _Blocks: FS materializer, import UX._
2. **Rule 3 totality proof.** The `.d`-stacking procedure must terminate for
   adversarial inputs (documents claiming `a.d`, `a.d.d`, … simultaneously,
   interacting with rules 1 and 2). Appendix A keeps growing with the worked
   cases; if the "most `.d` wins" order turns out ambiguous at some depth,
   define tie-breaks. _Blocks: FS materializer spec._
3. **Reserved-surface mechanics.** Directory names are settled (`.dnode`,
   `.dtree` — FDR 002); the *rule* (reserved surface wins the literal name,
   dpath claims spill to `<name>.d`) stands. Open: is `/by-id`
   materialization skipped under dpath-materialized subtrees only, or does
   the driver get finer controls? _Blocks: pauperfuse ADRs._
4. **Single-doc adoption granularity.** For code-source-control adoptions (one
   CRDT history per directory), confirm facet-per-file mapping and that the
   dpath targeting scheme can address per-file facets within the single doc.
   _Blocks: FDR 003 (multi-CRDT-history representation), ADR 010._
5. **Body-facet primacy for display.** Resolution for "which path shows" is
   lexicographic-first (see below), but whether the Body facet should be able
   to override it for UX primacy is undecided. _Blocks: FDRs 002/003 display._

Resolved in this review (kept here so the history is explicit):

- ~~Reserved namespaces~~ — none; collision rules handle reserved surfaces (§5).
- ~~Tags: real tag facets?~~ — plugs may add their own facets (hashtag
  extraction planned); materialization queries ride atop the future query
  system (Tags section).
- ~~Dpath value metadata~~ — facet stays lean; customization is a separate
  facet resolved by the lens layer (ADR 012).
- ~~Import collision policy~~ — uniform Rule 1–3 handling post-adoption; the
  common "adopt under `/subpath/`" shape makes this rare (§7).
- ~~Primary dpath / path display~~ — pauperfuse materializes **all** dpaths
  (or the query-filtered subset, once that exists). For deterministic display
  when one path must be shown: Body-facet primacy if applicable, else
  lexicographically-first dpath.
- ~~Dpaths in URLs~~ — moved to Appendix B; a dedicated URL FDR is planned.

---

## Appendix A: worked materialization examples

Cases 1–5 are the base rules; 6–8 are the adversarial compositions that case
2's totality proof (Q: Rule 3 totality) must cover.

| # | Dpath claims (objects)                                                                                | Materialized FS                                                                   |
| - | ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| 1 | doc A → `/inbox/hello.md`                                                                             | `/inbox/hello.md` (A)                                                             |
| 2 | A → `/inbox/hello.md`; B → `/inbox/hello.md`                                                          | `/inbox/hello.md/` dir; children `<A-name>.…`, `<B-name>.…`                       |
| 3 | A → `/hi/hello`; C → `/hi/hello/child`                                                                | file and dir collide: `/hi/hello` (A) + `/hi/hello.d/child` (C)                   |
| 4 | A → `/a`; D claims `/a.d`                                                                             | rule-3 spill: `/a.d/` belongs to `/a`'s collision dir; D demoted to deeper suffix |
| 5 | A → `/a/b` only                                                                                       | bare `a/` dir materialized (implicit), `a/b` (A)                                  |
| 6 | A → `/a`; C → `/a/b`; D → `/a.d`                                                                      | compose cases 3 and 4                                                             |
| 7 | A → `/a`; E → `/a.d/x`; F → `/a.d.d`                                                                  | deep suffix stacking                                                              |
| 8 | G → `/inbox`; H → `/inbox` (both docs are _directories_: each has file-children via their own dpaths) | rule 1 on the dir: `/inbox/` with `<G-name>/…` and `<H-name>/…` subtrees          |
| 9 | doc A claims `/by-id/x` while the checkout materializes its own `/by-id/` surface                     | reserved surface wins literal name; A spills to `/by-id.d/…` (Rule 2/3)            |
| 10 | A binds `/x.png` (holder); claimant B arrives later with an earlier dpath-assignment timestamp        | binding is sticky: A keeps `/x.png`; B lands at `/x~<id-b>.png` (the tiebreaker chain only applies to unbound claimants) |

## Appendix B: deferred to other documents

- **Dpaths in URLs** (`db+dpath:…`, inter-doc references, dict.md's open
  branches-in-URLs TODO): belongs with a dedicated URL FDR; not addressed
  here. townframe-2 ADR 007 §7 already fixes the base address grammar
  (`db:<id>`, `db://<drawer-id>/<id>`, `db+iroh://…`); dpath addressing must
  extend, not redefine, it.
- **Nested checkout / re-adoption detection**: spec owned by the pauperfuse
  ADRs (008/009); principle noted in §5.
- **Lens customization facets and intra-doc entry-set collision semantics**:
  owned by ADR 012 (§9).
- **Checkout metadata directory naming**: owned by FDR 002.