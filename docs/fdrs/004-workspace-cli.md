# FDR 004: Workspace CLI Experience — Nodes, Checkouts, Import, Watch, Sync

**Status:** Draft. The FDR of the **CLI checkout experience**: what an
engineer types to put daybook content on disk, keep it live, and send it to
peers. GUI notes are one paragraph — the GUI drives these same verbs, so the
CLI grammar is the contract; deeper GUI design (drawer-grouped browsing,
progress panels' layout) is not locked here.

Companion documents: FDR 002 (vocabulary — node/`.dnode`, checkout/`.dtree`),
FDR 003 (VC primitives — main-by-default, commit, branch-on-conflict, trash,
porcelain verbs), FDR 001 (dpaths — reserved surfaces, binding stability,
adoption assignment), ADR 010–013 (pauperfuse internals).

Day-1 use cases this FDR must serve (locked):

1. **Media library** — a photo/video/music folder stays where it is, blobs
   are adopted without byte duplication (hardlink-first per ADR 013), files
   keep working in every existing app.
2. **Obsidian vault sync** — an existing vault becomes node-synced; edits
   flow both ways; interoperability with Obsidian is preserved *as an Obsidian
   vault*.
3. **Agents as task-tracker collaborators** — agents get daybook checkouts as
   working surfaces for shared task/collab state, write into staging branches
   (FDR 003), and humans review/merge. This locks the techie use case: a
   multi-agent workflow where daybook *is* the task tracker, not a file dump.

---

## Decision

### 1. The lifecycle: ambient ingest, explicit sync

Two independent clocks control when state moves. Misreading this is the
classic daybook confusion, so it is stated first:

- **Ingest (local) is ambient.** Every `.dtree`-related CLI invocation acts
  jj-style: it first **auto-commits the working set** — applying detected
  local edits into the branch of record (main, per FDR 003 §1) — and then
  performs its operation. The CLI is a pseudo-watch: walk away from a
  checkout and return any time; the last command already captured state.
  There is no "remember to commit before `db log`".
- **Watch mode is an explicit daemon** (`db watch` / GUI runs one watcher
  aggregate). It exists for *idle-time* ingest — a normie editing a vault
  who never runs commands deserves the same auto-commit behavior. Watch =
  debounce → semantic ingest (FDR 003 §1, §6 policy). The scanner is always
  authoritative; events only mark dirty scopes (pauperfuse reconciliation,
  ADR 011).
- **Remote sync is always explicit**: nodes advance locally from checkouts
  (and incoming sync traffic while a sync node runs), but **nothing talks to
  remote nodes until a sync node is up** (`db sync`, §7; apps run one by
  default per FDR 002). A one-shot CLI command on a plane commits locally and
  syncs later. This is also why the CLI needs no login and no default node
  (FDR 002 §7).

### 2. Node lifecycle: `init` and `mirror` are node-level

- **`db init [name]`** creates a **node**, not a checkout: a fresh named
  directory in the **cwd**, containing its `.dnode` (node stores, secrets,
  node metadata) plus the root-drawer checkout's `.dtree`. Git's
  `git init <dir>` shape. The node's display name defaults to the directory
  name; GUI-created nodes default to device-name-style names (FDR 002
  open call).
- **`db mirror <source> [dir]`** is the remote-flavored node creation:
  bootstrap a new node that **mirrors** an existing one (relay account or
  p2p peer) — the add-device/mirror flow of FDR 002 §8, expressed for the
  CLI. Mirror = new node + own agent + hydrate through sync; never a
  `.dnode` copy (FDR 002 §2). **`clone` is retired as a verb everywhere** —
  it implies a repo-like, data-first relationship that isn't real here; the
  relationship is *mirroring* and that is the word, everywhere the concept
  appears (FDR 002 §1/§8 included).

### 3. Checkout lifecycle: `checkout` and `adopt`

Two distinct entry points, finally separated (the old "adopt vs import" blur
resolved):

- **`db checkout <dir> [spec]`** creates a NEW tracked materialization from
  the current node: fresh directory, fresh `.dtree`. The spec (§4) selects
  content by **dpath prefixes and reserved clauses** (v1 grammar; the full
  query system is future). Agent staging: `--stage` puts checkout writes on a
  `/tmp` branch per FDR 003 (agents-as-task-tracker use case: shared task
  drawers, one staged checkout per agent, human merges after review; CLI
  specifics fleshed out once the pauperfuse ADRs exist, per review).
- **`db adopt <dir> [--under <dpath>] [--drawer <drawer>]`** is the missing
  interop verb: **bind an EXISTING directory as a live tracked checkout of
  the EXISTING node.** Files stay where they are; daybook object identity is
  created behind the scenes:

  1. bind the directory (create `.dtree`, bind to the contextual node);
  2. assign dpaths from origin paths (`--under /DCIM` prefixes every file;
     default root: files land at `/`, drawer files under the chosen
     subtree — the common shape from FDR 001 §7);
  3. track every file in the pauperfuse checkout tree **without creating
     any docs** (default, locked per review): adopt only creates the
     `.dtree` and records what the conversion *would* do; `db status` then
     shows it (`~312 new notes via obsidian lens, 44 blobs adopted in
     place, 3 unknown formats`), and `--import-now` (or a plain
     `db import .`) performs the conversion. The dry-run preview is part
     of adopt's plan output and status remains the living preview.
  4. **the non-importing default is a design-shaping rule**: import/creation
     of docs is network-expensive once a sync node is up, so **defaults do
     the cheap local-tracking thing and the expensive thing is always
     explicit**. Watch-ingest of *later* edits is exempt from this (the
     OneDrive reasoning: once you track a directory you've accepted its
     sync cost — including compiler trash — which is the normie
     optimization); but bulk *initial* conversion must never happen as a
     side effect of binding.
  5. content handling at import time: blob-like files are adopted in place
     (external-tracked, hardlink-first — ADR 013; no copy), text-like
     files become dpathed docs via the import flow (§5).

- **`db detach <dir>`** releases a checkout without touching repo data:
  removes `.dtree`, keeps docs/dpaths/blobs. (Uninstall story.)

### 4. The checkout spec (what `.dtree` binds)

- **Selection grammar (v1): dpath prefixes + reserved clauses.** `--under
  /DCIM`, `--drawer <drawer>`, the reserved `/trash/` default-exclusion (FDR
  003 §8), `--include/--exclude` prefix lists. The *mechanism* is
  query-shaped from day one (all checkouts are "queries over dpath space"),
  but v1 speaks only clauses this simple; the query system FDR replaces
  clauses wholesale later.
- **Binding-stability knob** (FDR 001 Binding stability): sticky+timestamp by
  default; a checkout spec may pin alternatives (e.g. forbidding clean-name
  stealing for vaults).
- **Lens selection** comes from dpath extension + content hints (FDR 001
  §9); per-checkout lens overrides are **not** v1 (lens customization facet
  is ADR 012 territory).
- **Ephemeral checkouts** (`--ephemeral`): own throwaway `.dtree`, auto-pruned
  (FDR 002 resolved call).
- Checkout state is *per-checkout, always*: bindings, transactional store,
  watch cursor, staged-branch pointers (`.dtree` is the whole story; FDR 002).

### 5. `db import` — the touch-like converter

- **`db import <path>`** converts an external file or directory into
  **dpathed docs in the node**, using the same lens machinery that
  materialization runs in reverse. One-shot; **no binding, no checkout
  state**. Re-running is content-idempotent (same digest → same dpathed doc
  facets merge into themselves; FDR 001 import idempotency).
- Import is also *adopt's* bulk engine (§3, step 5; opt-in at adopt, never
  the default) — one convert path, two surfaces. **Import is the most
  network-expensive ambient operation in the system**; when `db sync` is
  running, every imported doc fan-outs to peers, which is why adopt does
  not import by default and why import stays an explicit, reportable,
  resumable stream (progress events for the GUI panel).
- **How lenses drive imports is owned by ADR 012** (flagged in review): the
  lens codec, ingest direction, per-format behaviors (.doc atomization, epub
  as blob, mime/extension hints), and what "convert to a daybook facet" means
  per format live there. This FDR only fixes the CLI contract: import speaks
  paths and emits dpathed docs, and progress is reportable (the GUI's
  import/sync progress panel consumes these events).
- Default target drawer/dpath root is the contextual node's root drawer at
  the path-derived dpath; flags override (`--under`, `--drawer`, `--doc-id`
  for the single-doc code-SCM mode later).

### 6. Auto-commit semantics on commands

- **Which commands auto-commit**: anything that consults a `.dtree` working
  set. The working set is the pending local ingest (un-ingested file edits
  vs. last-applied tree); auto-commit = FDR 003's `db commit` (branch-on-
  conflict policy applies exactly as for watch). **Ordering rule, applied
  everywhere:** local writes first, upstream materialization second — a
  `db commit` ingests the working set, and ambient materialization of
  upstream changes comes after, so the checkpoint the commit names is the
  local write frontier: the last change id of the local ingest across all
  touched docs.
- **Messages**: auto-commits are **anonymous changes** — no message, no
  ceremony. Naming is **checkpoint framing, not git commit framing** (locked
  per review): since the sync protocol has no truncation, a git-style
  "message names one commit" can't fit a stream of anonymous auto-changes.
  Instead, Patchwork-style: a **name is a mutable annotation covering the
  range of changes since the previous name** ("last N changes as a group",
  until a previous named marker). Mechanism: mutable replicable annotation
  mapping `(branch, from_heads…to_heads) → name`, rendered on top of the
  change-group timeline (FDR 003 §5). Consequence: **names are renameable
  after the fact** — the earlier immutability concern dissolves at the
  naming layer; raw automerge changes underneath stay unnamed and immutable
  (change-metadata messages remain an implementation detail for the newest
  change, if used at all).
- **Name changes after the fact** (locked here; the remaining hard case):
  - **branch names**: mutable — ADR 007 makes names optional relationships in
    the `daybook.branches` directory; renames never touch `BranchId`.
  - **change-level messages**: immutable — they ride an already-synced
    change. The user-facing fix for labeling mistakes is therefore the
    checkpoint annotation above (mutable, range-covering), not change
    rewriting; the nuclear path remains redaction (new doc id, FDR 003 §4).
  - **node display names**: mutable, local facet territory (app/config docs;
    FDR 002 open call).
- A command that can neither auto-commit (e.g. pure reads like `db diff --at
  <past>`) nor find issues reports clearly; reads never fail silently because
  state was stale — they commit first when the working set is non-empty
  (except `--no-commit` for scripting).

### 7. Sync lifecycle

- **`db sync`** stands up the node's sync engine: foreground (default),
  daemonized, or emitted as a **systemd unit** (`--systemd`). This is the
  only CLI surface that talks to remote nodes (FDR 002 §7).
- While up: peer sync over the existing stack (iroh-backed transports),
  drawer-scoped discovery, relay attach (`db relay add` — account flows are
  ADR 005/006 territory and referenced, not re-specced).
- **Device mirroring on by default** among a home's nodes: my-devices list
  (node metadata) means `db sync` on both laptops converges them; for
  relayless users this is the whole story (FDR 002 §8, FDR 003).
- **Progress surfaces**: sync state (per-node/per-drawer backlog) and import
  progress (§5) are the two event streams the GUI's progress panel consumes.
  Emitted as structured status, readable via `db status --sync`/`--import`;
  the GUI subscribes. (The GUI panel layout itself is not this FDR.)

### 8. Status surfaces (normie and techie sharing one engine)

- `db status [paths]` — the three-way (FDR 003 §1) plus checkout-level
  decorations: degraded states (FDR 001 §6: `missing-content`,
  `target-not-found`, `in-conflict`), conflict-branch presence
  (`/tmp/conflicts/…`), trash hits, staged-branch summaries.
- Trash is **not a special surface** in v1: `/trash/…` is query-invisible in
  checkouts by default and inspectable via `db status --trash` / restore via
  `db restore` (FDR 003 §8).
- The node's **local inventory** (what this node actually holds — ADR 011)
  backs "everything synced?" answers; its user-facing surface is minimal in
  v1 (a `db status --node` summary at most).

### 9. GUI (one paragraph, on purpose)

The GUI runs watch for **all checkouts combined**, renders the drawer-grouped
browser of the identity-context discussion, and surfaces the two progress
streams (import, sync). It exercises this FDR's verbs as its implementation
contract — no GUI-specific data flows exist; anything the GUI needs that the
CLI lacks is a missing verb, filed here.

---

## Open Questions

1. **`db init` literal shape** — resolved: `db init [name]` always creates
   `./<name>/` in the cwd and never inits an existing dir; `--adopt-here`
   can bundle "new node + adopt cwd" for the rare case, but existing-dir
   workflows own `db adopt` (per review). _Blocks: porcelain only._
2. ~~Verb rename: `db commit` → ?~~ — **resolved: keep `db commit`**, with
   ordered semantics per review: a commit performs **local writes first,
   then remote pulls, always** — the ambient materialization of upstream
   changes is ordered *after* the local ingest. The commit then **names the
   last change id of the local write across docs** (the anchor of the
   checkpoint range). No `db pull` verb exists; upstream application is
   ambient in every operation, after the local step. FDR 003 §2 wording
   updated to match.
3. **Agent task-tracker conventions** — resolved to plug-territory: agent
   layouts/facets are plug-defined (markdown dirs + occasional flushes is
   the expected shape); 004 locks only `--stage` mechanics. No dedicated
   FDR for now.
4. **`--import-now` ordering** — when converting an adopted dir: import is
   always local-first (docs are local automerge changes; sync catches up on
   the next `db sync`), no queueing layer. _Blocks: §3/§5 detail._
   Also: no `db pull` unless the rename is rejected.
4. **Per-checkout import policy** — confirmed per review: import behavior
   (auto-import on ingest? ignore patterns? lens parameters?) is a
   **per-checkout config knob** in the checkout spec, not a global. Schema
   owner: ADR 012 (lens definitions) + this FDR's spec shape.

## Resolved in this review

- ~~adopt vs import~~ — no `db adopt` under an import umbrella: **adopt =
  bind existing dir as tracked checkout** (the missing interop verb),
  **import = touch-like one-shot lens conversion**, and **checkout = fresh
  materialization**. Adopt reuses import as its bulk engine.
- ~~Watch default~~ — watch is a CLI command / GUI daemon; the CLI itself
  auto-commits the working set on every `.dtree`-related invocation
  (jj-style pseudo-watch; `--no-commit` for scripting).
- ~~Node-init vs checkout-init conflation~~ — `init`/`mirror` are node-level;
  `checkout`/`adopt` are checkout-level; existing directories never belong
  to `db init`.
- ~~Ingest vs sync lifecycle~~ — commands auto-commit locally; remote sync
  happens only while `db sync` (or the app) runs.
- ~~`clone` naming~~ — retired everywhere: the verb is **`db mirror`**
  (mirroring is the actual relationship; `clone` implies a repo-like,
  data-first vision that is false — hydrate-through-sync is not a copy).
- **Import cost principle** — *defaults never trigger network-expensive
  work*: adopt tracks without converting (`--import-now` / `db import` to
  convert); ambient incremental ingest stays (OneDrive normie reasoning);
  per-checkout import policy configurable.
- **Naming = checkpoint framing** — names are mutable range annotations
  ("last N changes since the previous name"), not per-change commit
  messages; follows the no-truncation sync posture. Adopted into FDR 003
  §5's follow-up (see below).
- **Default-node config** — the app global config (`app.rs` semantics) +
  XDG config may define a default node, consulted last; no implicit
  default.
