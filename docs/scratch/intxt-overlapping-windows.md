# intxt: ops when tool calls overlap (proposal for ADR 040 §3)

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-12
Scope: How the recorder turns observations into worktree ops when tool-call windows overlap (parallel tool calls in Claude Code, concurrent ACP tool calls). Applies to hooks and ACP alike. Builds on D59 (one op per tool-call window).

## Today

A window opens with a pre-snapshot and closes with an observation against it (`intxt_capture::observe`): `earlier` = last observation → pre, `changes` = pre → now. That is right when windows don't overlap. With two calls A and B:

```text
t0 A opens (pre_A)   t1 B opens (pre_B)   t2 A closes   t3 B closes
```

- At t2: `earlier` = last → pre_A, `changes` = pre_A → t2. B's edits between t1 and t2 are attributed to A.
- At t3: the last observation is t2, but `earlier` = t2 → pre_B, which runs **backwards** and restores the content of t1. Then `changes` = pre_B → t3 re-applies t1–t2's edits, and attributes them to B as well.

**Reproduced (ledger F28):** driving `observe` with exactly that sequence (a.txt `0` → `1` while both windows are open, then b.txt added before B closes) gives A's close `changes: a.txt=1`, then B's close `earlier: a.txt=0` (the revert) and `changes: a.txt=1, b.txt=2` (the replay).

So the op log, which ADR 040 §3 makes linear, gets a revert-and-replay pair, and the content at op N (the latest blob for a path at or before N) is wrong between those ops. The end state is right, which is why tests haven't caught it. Parallel tool calls are common: Claude Code runs independent reads and edits in parallel.

## Proposal

**Segment the time since the last observation at every window boundary, and record one op per segment.**

- The recorder already stores each open window's pre-snapshot in the worktree state. At any observation (a window closing, a turn end), it takes the stored pre-snapshots newer than the last observation, sorts them by time, and diffs consecutively: last → pre₁ → pre₂ → … → now. Each diff is one op, so the log stays linear and every change is recorded once.
- A segment's **cause** is the set of calls whose window covers it, known from the stored opens and closes:
  - one call open: that call (the common case, unchanged);
  - none open: no cause (as today's between-window edits);
  - several open: the edit can't be attributed by time alone. If exactly one of them reported touching the path (Edit/Write/MultiEdit report paths; ACP `locations`), that call; otherwise the op lists all of them.
- The pre-hook stays as cheap as today (a snapshot, ~25 ms, no node call): segments are cut at close time, not recorded at open time, so nothing new sits on the agent's critical path.

**Datum change:** `WorktreeOp.cause: Option<Location>` becomes `causes: Vec<Location>` (empty = unknown). A reader shows a multi-cause op as "during A and B". The alternative, keeping one `cause` and dropping attribution when several calls overlap, loses information the recorder has.

## What it costs

- One more datum shape change in ADR 040 §3 (cause becomes causes).
- More ops when windows overlap: one per segment instead of one per call.
- A closing window may leave a segment open for a call still running; its edits so far are recorded under it now, and its own close records only what follows.

## Open

1. `causes` as a list (proposed), or one `cause` with attribution dropped when ambiguous?
2. Narrowing by tool-reported paths: proposed as above; it relies on tool reports, which F26 showed miss most edits, so it only ever narrows, never widens.
