# ADR 011: Checkouts & reconciliation

- **Status:** Draft
- **Supersedes:** none
- **Depends on:** ADR 010 (bridge & vtree), FDR 003 (VC primitives), FDR 004 (workspace CLI), townframe-2 ADR 007 (doc/branch identity)
- **Depended on by:** ADR 012 (lenses), ADR 013 (blob strategy)

## 1. Scope & posture

This ADR locks the **expectations and load-bearing designs** for checkouts:
the daybook deployment's instantiation of the bridge core (ADR 010). It is
deliberately not an exhaustive flow catalog — obvious details fall out of
implementing the rest (per review); what is written here is what future
decisions depend on.

- **Generic stays in 010, daybook lives here.** The three-way *hook*,
  transfer brokering, reps, stubs — all 010. Branch-on-conflict, `/tmp`
  conflict branches, trash mapping, lenses as the ingest/render boundary —
  here.
- **No daemons for v1** (per review): correctness never depends on a
  long-running process. Watchers are *reactive observers* of backend change
  sources — fs notify events, daybook heads movement — and they **do not
  open the vtree store until they observe a change**. A detected change
  wakes one reconcile cycle; an idle system holds nothing open.
- **Single writer, arbitrated by the store**: whoever holds the per-checkout
  store lock (CLI command or a woken watcher) runs the cycle. No IPC
  protocol; sqlite's lock is the arbiter.
- **Ordering rule (locked FDR 004, restated in bridge terms)**: local
  writes first, upstream materialization second — every cycle ingests the
  local backend's report before brokered transfers apply upstream state.

## 2. Checkout lifecycle (expectations)

- **create** (`db checkout`): bind the directory as the fs backend, assert
  the target is empty (FDR 004 §3), set `rep:fs` to the empty tree, run one
  reconcile cycle — materialization flows through brokered transfers.
- **adopt** (`db adopt`): bind + dpath assignment + tracking **without
  creating docs by default** (the network-expense principle, FDR 004 §3).
  `--import-now` / `db import` performs conversion; until then the checkout
  is track-only — deletions of unconverted files are rep-level only (§4).
- **detach** (`db detach`): files stay on disk exactly as they are; the
  store is archived/deleted per detach policy. No deletion ever happens at
  detach.
- **Per-checkout spec** (`.dtree`): import policy knobs (ignore patterns,
  lens parameters, auto-import behavior) — schema owned with ADR 012's
  lens definitions; 011 only requires that the knobs exist per-checkout,
  not globally.
- Many checkouts may bind one node; each has its own store and writer.
  Cross-checkout coherence is the node's sync problem (automerge), never a
  vtree concern.

## 3. Reconcile triggering (expectations)

A reconcile cycle runs when:

1. **a CLI command that consults the working set executes** — local-first
   per the ordering rule; or
2. **a watcher observes a backend change** — fs event, daybook heads
   movement — and wakes the cycle for that checkout (store opened lazily at
   this point, not before); or
3. **a user asks** (`db sync` foreground mode: run cycles until quiescent).

There is no required background process. Watchers are an optimization for
latency, not a liveness requirement (per review: the watcher only reacts;
if it is not running, the next command's cycle catches up — scanner
authority, FDR 003).

Upstream wakeup for v1 is **poll-based and cheap**: the watcher's daybook
side checks heads movement (O(changed) for the daybook backend's own
tracking, ADR 010 §2.1); push notifications are a later optimization, not a
design dependency.

## 4. Deletion semantics (expectations)

- **The vtree's entire role**: removal from the rep. A deleted path stops
  being an entry; the rep folds the removal like any other delta. What the
  deletion *means* downstream is not vtree business (per review: `/trash/`
  is daybook semantics).
- **Daybook semantics layer**: the lens maps fs-side removal of a
  doc-backed file to the trash dpath operation (FDR 003 §6: move doc to
  `/trash/…`; checkouts default-exclude `/trash/`); permanent deletion
  happens only at empty-trash → GC. Doc-side deletion materializes as fs
  removal through the same transfers.
- **Track-only (adopted, unimported) files**: removal is recorded in the
  rep and nothing else — there is no doc to trash until import creates one.
- Deletions are **never destructive beyond the lens policy**: the vtree
  itself has no permanent-delete operation; recovery of doc content is the
  daybook backend's business (trash, history).

## 5. Conflict handling (the load-bearing design)

**What conflicts even are** (locked per review): automerge is a CRDT —
concurrent edits *merge*; there are **no git-style merge conflicts** in
this system. The only conflict a daybook checkout can have is a **facet
validation failure**: a change (local or upstream) that makes a facet fail
to parse or validate — a genuinely broken case, arising from bugs or from
facets *deliberately designed to bounce* (refuse invalid states). This is
exactly FDR 003 §3's branch-on-conflict scope: validation-failure-only.

Policy for now — simple, refine later:

```rust
fn on_validation_bounce(cx: &mut Cx, bounce: Bounce) -> Res<()> {
    // the bounced change lives on a device-local /tmp/conflicts/<facet-id>
    // branch (FDR 003 §3); the checkout keeps rendering the last-good state
    let br = cx.daybook.create_conflict_branch(&bounce, "/tmp/conflicts/")?;
    cx.record(br, bounce.heads);        // tf2 ADR 007 bookkeeping
    Ok(())
}
```

Expectations locked:

1. **No on-disk conflict markers, ever.** We do not render conflict state
   into the real files — inline conflict resolution would demand advanced,
   fragile lens machinery (per review). **The disk always shows the
   last-good render.**
2. **Bounces never block the checkout**: the bounced change lives on its
   device-local conflict branch (`/tmp/conflicts/<facet-id>`, never
   replicated eagerly); the checkout stays live on the last-good state.
3. **Bounced content is always reachable** on its branch; nothing is
   silently dropped.
4. **Resolution is a pick, not a merge**: the conflict surface is a
   **CLI conflict viewer** — list bounced facets, show diffs to stdout
   (last-good state vs each candidate version, rendered by the lens's
   diff view), ask the user to pick. Picking applies the chosen version as
   a normal change. No inline editing; merge-style tooling may come later.
   _CLI consequence: viewer verbs belong in the FDR 004 revision._

## 6. Bulk operations: ordered walks (crash-safe by shape)

Any bulk pass over a checkout — initial materialization, `db import`,
large transfer plans, full re-scans — is an **ordered walk** of the vtree
in the store's canonical order (sorted entries, lexicographic paths), with
a **persisted cursor** (current path position) per running operation. The
vtree is ordered-walk-shaped anyway whenever one side reconciles against
another tree (per review), so bulk ops reuse that shape:

- a crashed bulk op **resumes at its cursor** — completed entries are never
  re-done (per-entry work is idempotent *and* atomic, §8);
- progress reporting rides the cursor (the GUI's import/stream panels
  consume it — FDR 004 §7);
- cursor + per-entry atomicity = crash safety with **no journal**.

From the daybook side, the same walk drives ingest: doc set → rendered
entries in canonical order, one cursor, resumable.

## 7. GC (expectations)

- **vtree node GC**: unreferenced nodes (no rep points at them or their
  subtree) are dropped opportunistically — after rep updates, or on store
  open. No reference counting, no urgency: nodes are small, and correctness
  never depends on GC timing.
- **Trash GC**: emptying trash is a daybook-backend operation (FDR 003 §6)
  that *produces* deletions the vtree folds like any other delta.
- **Blob GC**: ADR 013's problem entirely — 011 only requires that the
  vtree's `ContentRef`s stay resolvable for as long as 013 says they will
  be, and that stubs are the honest representation when they are not.

## 8. Crash & concurrency expectations

- **Per-file atomic materialization** is the backend's export contract
  (ADR 010 §4.3): a crash mid-transfer leaves a stub (or a dirty entry),
  the next cycle finishes it. No journal, no recovery protocol.
- **One writer per checkout** at a time (store lock). Two CLI commands
  serialize; a watcher's cycle waits or merges into the winner's cycle.
- **Backend truths, rep caches** (ADR 010 §3.3): any store corruption or
  stale rep heals via a fresh change report. Worst case is re-hashing, never
  data loss — blob loss is 013's failure mode (ADR 010 §5.1), not a
  checkout one.

## 9. Open questions

1. **Symlink-vs-file races in three-way staging** — symlink target changes
   and kind flips (file↔dir↔symlink) need explicit merge rules; carried
   from ADR 010 OQ4. _Blocks: lens kind-flip handling (012)._
2. **Watcher event coalescing** — fs event storms (builds, checkouts of
   dependency trees) must collapse into one cycle's report; debounce window
   policy is an impl detail, but the *expectation* — a storm never produces
   per-event cycles — is locked here. _Blocks: nothing._
3. ~~`db sync` ↔ checkout latency~~ — resolved per review: **no**; no
   checkout needs sub-second upstream wakeup without its own watcher
   (heads-poll in §3 suffices for v1).