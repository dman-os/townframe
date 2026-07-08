# FDR 003: Version-Control Primitives — Diffs, Branches, and History Across CLI and GUI

**Status:** Draft. Open questions are numbered and tagged with what they block.
Scope note (locked in review): this FDR covers the **common grammar of VCS
primitives** — how diffs, branches, and histories are represented so CLI and
GUI speak one language — with focus on the design work needed to implement the
CLI pauperfuse surface. Out of scope: node metadata contents (FDR 004/ADR 009),
GUI layouts, blob diffing/patching (ADR 011, deferred by design).

Companion documents: FDR 001 (dpaths — conflicts-as-opinions, §4), FDR 002
(vocabulary — node, drawer, checkout, agent), **townframe-2 ADR 007**
(logical document/branch/drawer identity — adopted throughout below), ADR 010+
(pauperfuse tree store, reconciliation), dict.md (doc branches, `/tmp` branch
namespace, heads). Note: townframe-2 ADRs already claim 007–009; this repo's
pauperfuse ADRs begin at **010** (vtree store), then 011 (checkouts &
reconciliation), 012 (lenses), 013 (blob strategy).

Prior art consulted: Ink & Switch **Patchwork** — lab notebook entries 03–09
(dynamic history, diff visualizations, edit groups, simple branching, bots,
history-as-chat) plus the actual implementation in `patchwork-base/drafts`
(clone-doc branches + heads bookkeeping, heads-pinned checkpoint views,
cross-doc change groups with cursors). Appendix A has the code-level details;
§3 and §7 lean on them directly.

---

## Context

Daybook is CRDT-based; merging always succeeds, so "conflict" never blocks
work (FDR 001 §4). What remains genuinely hard — and what this FDR must
settle — is the **grammar**: what the user sees, what the CLI verbs do, and
what the GUI renders, such that both surfaces express the same five things
about a document collection under concurrent multi-device editing:

1. *what changed* (diffs);
2. *where work diverges* (branches);
3. *what happened* (history);
4. *how the FS checkout relates to the node's docs* (checkouts, three-way state);
5. *what it means to remove things* (deletion).

Two asymmetries shape every decision below:

- **Most documents have exactly one human reader/writer.** Multi-user
  contention exists (shared drawers) but is not the common case, so the
  default flow must be zero-ceremony.
- **Agents and plugs are *also* writers** — of a different character: bulk,
  mechanical, sometimes untrusted. They need a staging surface that humans
  can review before it reaches main.

---

## Decision

### 1. Main-by-default (write path)

Changes are written **directly to the branch of record — `main` — by
default**. There is no hidden working branch, no eager tmp-branch snapshots.
Data safety does not depend on user action: Automerge retains full history
and every write is an ordinary synced change the moment connectivity allows
(Patchwork's identical conclusion: edits on main, drafts opt-in).

Consequences:

- **The CLI's "uncommitted state" is purely local ingest lag** — bytes on
  disk or in a plug's buffer that have not yet been ingested into facets.
  `db status` reveals exactly this: the three-way state (last-applied tree ↔
  real tree ↔ branch of record) from the pauperfuse reconciliation model
  (ADR 009).
- **Watch mode** auto-ingests detected edits into main after its debounce
  window. Nothing "commits" in git's durability sense; ingest *is* the save.
- **Staging is opt-in and for non-human writers**: agent/plug workflows write
  into a `/tmp`-namespace branch (device-local per dict.md) — e.g. an agent
  checkout configured `--stage` — and a human promotes (`db merge`) or
  discards. This is the entire role of tmp branches: stage wflow and AI
  writes. They are not load-bearing for normal users and get pruned
  aggressively (§8).

### 2. The CLI porcelain (v1 surface)

Verbs are git-shaped because git grammar is the interop target, but they map
to different machinery underneath:

| Verb | Meaning | Machinery beneath |
|---|---|---|
| `db status [paths]` | three-way: un-ingested local edits, upstream changes, conflicts, missing content | pauperfuse reconciliation diff (ADR 009) |
| `db diff [paths] [--at <version>] [--branch <b>]` | diff between any two of: real tree / last-applied / any heads | tree diff + content diff (§7) |
| `db commit [paths] [-m]` | apply detected local edits into main (or `--branch`) now, naming the change range since the last checkpoint | semantic ingest via lens → automerge transaction |

| `db log [paths] [--doc …] [--author …]` | history of the tree/doc(s), grouped per §5, with named checkpoint ranges | automerge `getChangesMeta` + pauperfuse op log |
| `db branch <name>` / `list` / `merge` / `delete` / `rename` | branch management on the branch of record | branch bookkeeping (§3) |
| `db fork [--at <version>]` | fork from a historical point (§4) | clone-at-heads |
| `db stage`-style flags on checkout creation (`--stage`, agent mode) | writes land in a tmp branch | tmp branch + later `merge` |
| `db checkout --at <version>` / pin | materialize a historical version read-only | heads-pinned views (§6) |


`commit` vs `ingest`: the verbs are aliases for the same operation —
"apply the working set to the branch of record" — kept git-shaped for
familiarity; `-m` attaches a message to the change.


### 3. Branches (per-doc, git-simple at the surface)

Per dict.md, branches are per-doc CRDTs sharing genesis. Locked UX posture
(Patchwork's simple-branching lessons, verified in `drafts`):

- **One branch of record per doc: main.** All other branches are drafts,
  agent staging, or experimental forks. UX constraint, not engine constraint
  (the CRDT model permits branching from any branch; the *surface* defaults
  to simple branching: create-from-main, merge-back, delete).
- **Branch bookkeeping follows townframe-2 ADR 007** (logical document,
  branch, and drawer identity), not a Patchwork-style ad-hoc bookkeeping doc:

  - `DocumentId = main BranchId = main physical document ID`; each branch is
    its own physical object, addressed by `BranchId`.
  - Branch identity lives in a `daybook.branch` system facet on every branch
    (`document_id`, `branch_id`, `created_from: optional BranchVersion` — the
    fork point = the source's heads at fork time, which is Patchwork's
    `CloneEntry.clonedAt` made durable in a facet).
  - The main branch carries a `daybook.branches` directory **keyed by stable
    `BranchId`, never by mutable name**; a declaration carries an optional
    name, a `publication` of `Shared | Archived`, an `authority` scope, and
    `created_from`. Names are relationships, not identifiers; renaming never
    touches `BranchId`.
  - Ordinary facet writes cannot mutate these system facets; only
    branch-management operations author them (with authority validation
    before authoring).
  - **Discovery is authority-scoped**: shared branches via the main-branch
    directory; private branches via authorized group feeds; **local
    (`/tmp`-namespace) branches are discovered only through checkout-local
    state** — which is exactly where our conflict/staging branches live and
    the mechanism by which they never replicate.
  - A **merge stamps the merged heads** onto the bookkeeping (the
    `mergedAt` of Patchwork's CloneEntry, as a declaration field; exact
    schema belongs to the facet-schema work ADR 007 defers).
  - Deleting a branch = removing its declaration (and unlinking any shared
    directory entry); the CRDT remains as data per §8.
- **Unnamed drafts by default** ("Draft N" counter, renameable later), **merge
  deletes the draft** (bookkeeping unlink; the CRDT remains as data — §8), and
  **fork-at-version** is the retroactive affordance: scrub/pin to a past
  version and `db fork --at`. *Nothing is ever moved off main*: history
  contains main's edits and always will (§4).

### 4. History is immutable; relocation never happens

- **The one-never-moves rule:** once changes are on a replicated branch,
  they can be forked-from, merged, partially applied, or reverted — never
  relocated, truncated, or rewritten. Sync has no "retract" op and that is a
  feature: every replica keeps the audit trail.
- **Redaction requires a new doc id.** If content must disappear from
  history (legal, privacy, spam), redaction = create a redacted successor
  doc, re-assign dpaths/labels to it, and abandon the old doc's drawer
  membership (old CRDT becomes unreachable; GC handles reaping per ADR 009).
  There is no in-place history surgery.
- **Scrubbing/pinned views are heads-based reads, not rewrites.** Loading a
  past version renders the doc *at those heads* (read-only by construction —
  the heads-pinned view has no write surface). "Continue from this point"
  = fork-at-version (§3): a new branch cloned at those heads; main keeps its
  history untouched. No content is replaced wholesale anywhere in the
  system; the only "broad replacement" that exists is redaction via new doc
  id (§4 first bullet).

### 5. Multi-doc history: groups are derived, not authored

"Session" was proposed as a first-class authored unit and **rejected for
v1** — it has no owner in the porcelain, and inventing one now would bake in
guesses. Instead:

- The **change group** is a *derived* view: changes across a drawer's docs
  grouped by (author, time-burst) — Patchwork's `ChangeGroup` model
  (inactivity-gap grouping, debounced incremental recomputation with heads
  cursors). `db log`, the GUI timeline, and `db status` summaries all render
  from this one grouping.
- **Commits are not a useful name-able unit in general** — Automerge emits
  one change per non-debounced keystroke from live editors, so in practice a
  busy doc produces many small changes; the message-bearing "commit" exists
  where the tooling creates one explicitly (`db commit -m`), and raw changes
  must still all be represented in history. The ChangeGroup view exists
  precisely to make that mass legible.
- What *does* exist as an authored unit: **the commit** (a message on a
  change — §2) and **the branch** (a named sink for changes). Both are
  optional ceremony on top of history, never required.
- Cross-doc atomicity does not exist at the CRDT layer and nothing pretends
  it does: a multi-doc operation is N changes with a shared op-log record in
  the checkout (pauperfuse op log, ADR 009), displayed as one group because
  the records share an op id.
- Grouping is **rendering-local** (resolved): inactivity-gap grouping is a
  projection; tune parameters once we have an implementation and real
  samples. No store-level commitment.

### 6. Branch-on-conflict (the auto-merge policy)

Default policy for every write path (CLI, watch, GUI), both for ingest of
local edits and merge of a draft:

1. **Attempt against main**: compute the semantic facet updates (lens ingest)
   against main's current heads and *dry-run validate* — each affected facet
   must still validate against its plug schema, and each lens must be able to
   render the would-be merged state.
2. **Validate → land on main** (the overwhelmingly common case: single-editor
   docs, CRDT merges already resolved the content).
3. **Fail-validate → auto-branch**: the write lands on a conflict branch at
   the standard local path **`/tmp/conflicts/<conflicting-facet-id>`**, and
   the checkout thereafter tracks that branch for further writes coming from
   it until the user resolves.

**Trigger discipline:** this is *not* a contention mechanism. Concurrent
multi-editor edits of sound schemas merge fine (that is what the CRDT is
for). Auto-branch fires only for **poorly designed schemas that break under
concurrent changes** — schema or lens validation can fail even though
automerge merged happily. The resolution UX is therefore **"pick a version of
the document"** — of the document *as rendered by its lens*, never of raw
facets; users are not shown the guts. The picker offers the rendered
candidate states (main-as-was, the tmp branch's render); picking one either
merges the chosen tmp branch forward or re-lands the chosen version on main;
the other is abandoned.

**General rule: merges are explicit and refusable.** The system never
reattempts a failed merge on its own; resolving a conflict branch (or
picking a rendered version) is manual, and the same generalization applies
everywhere: **`db merge` of anything — draft branch, agent branch, checkout
ingest — is refused when it would break a facet** (validation failure =
refusal with the rendered-pick escape hatch). In most cases the refusal is
telling you the facet schema is badly designed; fix the schema rather than
loosening the rule.

**Conflict branches are `/tmp`-only, by design.** They never replicate:
creating a branch is expensive (a whole new branch CRDT and its sync
metadata — ADR 007's doc-identity machinery), and unattended/broken tooling
must not be able to flood sync with them. Consequence, documented as a
caveat: conflict branches are **visible only on the node (and checkout) that
created them** — other nodes will just see the conflicted state on main and
their own local conflict branch when they touch the same data.

This keeps branch management a **burden you only carry when a schema is
actually sick**, and even then nothing is lost or blocked. After-the-fact,
history always allows re-derivation (any past state is renderable at heads).

### 7. The diff object (one primitive, three renderers)

Every diff the user sees is derived from one shape so CLI and GUI cannot
drift:

```
Diff {
  // tree-level, from pauperfuse reconciliation + tree diffs
  entries: [ EntryChange {
    path_binding,          // where it landed in the checkout (FDR 001),
    object,                // object ref (facet URL / blob digest)
    from: {heads?, content_ref?}, to: {heads?, content_ref?},
    kind: added | removed | changed | moved | missing-content | in-conflict,
  } ],
  // content-level, per changed object, from automerge ops (per-char) or
  // lens-appropriate granularity; blob objects diff by ref only (ADR 011)
  changes: per-object,
}
```

Renderers: (a) git-style path+line text for CLI; (b) summary stats
(+chars/−chars by author/section — Patchwork's minibar findings word better
than char-counts for prose); (c) GUI inline visual per datattype. Diffs are
compute-on-demand from (heads A, heads B, tree bindings) — nothing persists
except the op log's own record.

### 8. Deletion and GC (multi-doc reality)

**Deletion is a lens operation, not a data-layer operation.** Content
deletion semantics belong to each lens (a multi-file lens that sees a file
removed adjusts its specific entries; a whole-doc lens reacts to its source
disappearing). The user-facing unit of deletion — and the thing checkouts
force us to reckon with, since checkouts are multi-doc by nature — is:

- **The trash can is a dpath tag** (`/trash/…`, a reserved top-level
  namespace in FDR 001 §5 terms). A deletion in a checkout (or via any
  interop surface, `rm` in an Obsidian vault included) **moves the claimed
  dpath into `/trash/…`**. Resolved per review:

  1. **Checkouts are query driven, and all checkouts default to excluding
     the `/trash/` clause** — so trashed objects vanish from every
     materialized tree by default without any per-checkout or per-drawer
     special casing. Trash is just a tag.
  2. **Drawers need no trash logic at all**: with the drawer-as-descriptor
     semantics (townframe-2 ADR 007 §6), a trashed doc simply **no longer
     makes itself part of any drawer group** — no membership surgery, no
     per-drawer bookkeeping.
  3. Emptying trash (GC, ADR 011) unassigns the `/trash/` dpath and drops
     any remaining drawer association. Until emptied, trash is reversible —
     objects restore to their previous dpath (`db restore`).
  4. **New requirement surfaced by this design: a node-local tracking
     surface for "all locally existing sedimentrees and their docs" that is
     not drawer-based.** Trash (and checkouts, and GC, and recovery)
     otherwise have nothing to enumerate what a node actually holds, since
     drawer membership is no longer the container of record. Spec lands in
     the reconciliation ADR 011 (local inventory), consumed by FDR 004's
     status surfaces.
- **In-doc content deletions** (text, facets) remain ordinary CRDT changes;
  concurrent edit-vs-delete merges per automerge semantics — the trash model
  deliberately sidesteps this class by deleting *claims and membership*, not
  content bytes.
- **Deleting a branch** = removing its `daybook.branches` declaration (ADR
  007); the branch CRDT remains as locally prunable / unreachable data.
- **GC (owned by ADR 011)**: emptying trash removes dpath claims + drawer
  memberships and reaps unreachable CRDTs; `/tmp` branches are aggressively
  prunable on device (never replicated; prune on merge, abandon, or TTL);
  unreachable agents/docs/drawer-links are reaped only when unreferenced by
  any live bookkeeping — the Patchwork lesson: *deletion = unlinking the
  reference; physical reaping is a separate, later, local decision.*

---

## Open Questions



3. **Redaction flow ownership** — new-doc-id redaction touches dpath
   re-assignment, drawer membership, and blob pinning (ADR 001).
   Out of scope here, confirmed by review; tracked. _Blocks: future FDR._

### Resolved in this review

- ~~`ingest` alias~~ — porcelain verb is `db commit` only.
- ~~Change-group parameters~~ — rendering-local projection; inactivity-gap
  grouping; tune with real samples.
- ~~Ingest granularity~~ — pauperfuse transactional-semantics ADR (011)
  territory, not FDR.
- ~~Conflict-branch location/visibility~~ — `/tmp/conflicts/<conflicting-facet-id>`,
  `/tmp`-only (never replicated; sync-flooding guard); the originating
  checkout tracks it for subsequent writes. Cost rationale: branch creation
  = new branch CRDT + sync metadata; broken tooling must not flood sync.
- ~~Fork for multi-doc checkouts~~ — mirror jj: the **checkout switches** to
  the newly forked branch state; any pending working-set state is eagerly
  captured into that branch at fork time so nothing is lost (this is the one
  deliberate eager-write exception to §1's main-by-default — it exists only
  at fork/branch-switch time).
- ~~Resurrection policy~~ — dropped with the redesign: deletion is lens/trash
  territory (§8), not a content-level CRDT question; no resurrection status
  exists.
- ~~Redaction scope~~ — confirmed out of scope here.

## Backlog landing in other documents

- **Watch-mode debounce/ingest policy**: FDR 004 + ADR 011.
- **Pauperfuse transaction semantics** (commit/ingest granularity, per-doc
  change batching vs per-file with shared op record): ADR 011.
- **Blob diff/replacement semantics** (content-addressed; reference-link LWW
  on the blob facet): ADR 013.
- **GC scope/triggers/guarantees** (incl. trash emptying): ADR 011.
- **Doc-level forking** — "fork this doc into a *new doc* in my drawer"
  (hypermedia edit-friendliness; differs from branch forks: new DocumentId,
  fresh drawer membership): its own future FDR, per review.
- **GUI grammar**: the primitives here are the surface contract; GUI-specific
  renderings inherit them. Next FDR or per-app design.

---

## Appendix A: Patchwork prior art (code-verified)

From `~/repos/ecma/patchwork-base/drafts/` — the shipped drafts plugin, not
the essay prototypes:

- **A branch is a clone doc + heads bookkeeping** — `DraftDoc.clones:
  Record<originUrl, CloneEntry{cloneUrl, clonedAt: UrlHeads, mergedAt?:
  UrlHeads}>`. Fork = `repo.clone(originHandle)` + record `clonedAt`; merge =
  `target.merge(clone)` + stamp `mergedAt`. No history mutation anywhere.
- **Edits on main by default** — the overlay forks docs resolved beneath a
  draft only ("On 'main': no clone"; main clones are identity mappings). Our
  main-by-default matches the shipped system.
- **Checkpoints = url→heads maps** — `DraftCheckpoint = Record<url,
  DocCheckpoint{from?, to?}>` renders each member doc at fixed heads. This is
  how their timeline scrubber "loads an old version": pinned heads-pinned
  read-views (`withHeads(url, to)`), *not* content replacement. Continuing
  from a pinned point = fork-at-version (`cloneAtVersion` in
  `DraftsSidebar.tsx`), whose clones branch off the pinned heads with
  `clonedAt` recorded.
- **Cross-doc timeline rows** — `ChangeGroup` spans member docs (inactivity
  gap 10 min, 250ms debounce), persisted incrementally in a `ChangeGroupDoc`
  with per-member `computedThrough` heads cursors, invalidated by
  late-syncing old-timestamp changes. Direct precedent for §5's derived
  groups and §7's diff plumbing (Automerge `getChangesMetaSince`).
- **Deletion = unlinking bookkeeping** — `onDeleteDraft` removes the entry
  from the parent's `drafts` list; CRDTs stay in place; unreachable docs
  accumulate until GC. Our §8 takes the same stance and hands reaping to
  ADR 009.
- **What Patchwork gave up, we keep** — their shipped UI has *no*
  "move my recent edits off main into a branch" (the essay's single-user
  prototype had it; live sync killed it). Our `/tmp` staging for agents'
  bulk writes plus `fork --at` covers the honest retroactive cases.
