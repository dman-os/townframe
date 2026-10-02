# PR #51 — my comments and the responses

Companion to `bigsync-reconciliation-open-items.md` §M (which tracks the *reviewer's* 40 cubic comments).
This file maps **your** inline comments on PR #51 to a response: what I found, what I plan, or what needs a
decision from you. Read this one first; §M is the long-form record.

Original comments live on the PR. To see one: `gh api repos/dman-os/townframe/pulls/comments/<id> --jq .body`

Threads: [#1 dirt count](#1--decide_peer_stratrs228--4057265519) · [#10 replay_page clone](#10--replay_pagers114--4058442902--4058444320)
[#11 WorkerUnavailable retry](#11--typesrs82--4058530255)
Removed as closed: #4, #5, #7, #8, #9, #12, #13, #14 (numbers are PR thread ids, so they are not contiguous).

Also tracked here: [the decisions you made in this pass](#decisions-taken-2026-09-21) and
[the reviewer's open code items](#the-reviewers-open-code-items).

---

## 1 — `decide_peer_strat.rs:228` — 4057265519

> if there are 1m unseen cursor increases on the peer and we have a dirt count of 3, that means cursor replay
> will only see 3 events right? i.e. dirt count equals to events we get during cursor replay? i.e. it excludes
> object we can't see?

**Response: not yet answered — it is in the "answer from code" batch, and the answer decides whether the
site's comment is wrong or the code is.** Partial evidence already on record (§L, last bullets):
`part_dirty_count` counts part-level changes the *local* store can see for that part from the cursor, and it
**does not count an access revocation** (the revocation deletes the `(part, principal)` row, so there is no
`changed_at` left to compare; both stores document this, and the peer summary refuses the part on the same
state anyway). That is consistent with your reading — dirt is the visible-change count, not "everything the
peer is ahead by" — but I want the trace from `dirt_count` to the replay event list before writing it down.


## 10 — `replay_page.rs:114` — 4058442902 + 4058444320

> issue: ouchie clone, this could contain hundreds of objects keys no??
> reply: the whole replay_page task design and how it's handled from the machine is weird and inefficient and messy

**Response: the clone is avoidable and worth fixing; the "design is weird" part belongs to the cursor/replay
redesign, not this PR.** At `replay_page.rs:112-116` the task clones `subscription.request` before handing it
to `replay_subscription`. The request carries the subscription's targets (part keys, and for object targets a
key plus a cursor), so its size is bounded by the subscription, not by the store — but it is still a per-page
clone of owned key vectors on a hot path, and it can be borrowed or carried as an `Arc` without changing any
protocol. I will measure the request size at the site first (so the fix is justified by a number, not a
feeling), then do the borrow. The task-shape complaint I agree with and am keeping in §M as a design item for
the replay/cursor redesign rather than smuggling a refactor into this PR.

!> confirm

## 11 — `types.rs:82` — 4058530255

> issue: uhh, does any one actually implement "retry" on this?? is this an unforutnate timing result or a hub
> shutting down result? because if it's the latter, why can't the hub start another doc_worker??

**Response: not yet answered — "answer from code" batch, and if no retry exists the doc comment is simply
wrong.** `WorkerUnavailable`'s comment says "Retryable: the next round spawns a fresh worker", so either a
matcher re-spawns (and the question is what triggers "the next round"), or nothing does and the failure is
terminal. Finding the matcher answers both halves of your question: whether this is a timing artifact or a hub
shutdown, and whether the hub can start another doc worker. I will not change the doc comment until the
matcher is read.

---

## Decisions taken (2026-09-21)
- **No ALPN bump.** No production users, so the `/0` version stays and reviewer item A7 is closed as won't-fix
  for this PR (it becomes "bump at the next protocol change").
- **Doc/ADR findings ignored.** The cubic claims against ADRs 010/011/012 and the demo (C4–C12, D4, D5) are
  out of scope for this PR; they stay recorded in §M as documentation/decision items, not as work.
- **`BlobId::to_bytes32` is out of scope** — it is total by type (`blobs.rs:109`), a different conversion from
  the fallible `ByteKey` one, and nothing about it should change.

## The reviewer's open code items

From §M.1, still open: **A8** (`spawn_rpc_handler` takes its inflight permit *inside* the spawned task, so a peer
can park unbounded tasks) and **A9** (`is_idle()` omits `task_counts.live` and `active_machine_tasks`, so a parked
live long-poll is not in-flight work). **A1/A2 is closed as unreachable**: no production minter creates an
`o:`-prefixed part key — `object_part_key` / `materialize_object_parts` have zero occurrences and the `o:` scheme
survives only as key bytes plus `ids.rs` test vectors. A7 (ALPN) is closed: no production users, so the version
stays until the next protocol change.


---

## Answers to the `!>` comments (2026-09-21, late)

### `!>` on thread #10 — "confirm"

Confirmed and queued: measure the `subscription.request` size at `replay_page.rs:112-116`, then remove the
per-page clone by borrowing or carrying the request in an `Arc`, with no protocol change. The task-shape complaint
stays with the replay/cursor redesign.

## part_store / keyed-frontier audit (read-only lane, verified by me)

Ranked instances, worst first (`part_store*` refs re-checked after the #12 page-reader split; the `keyed_frontier`
ones are as the lane reported them). #1, #4 and the seam I re-read myself; the rest are lane-reported with `file:line`.

| # | instance | cost | fix |
|---|---|---|---|
| 1 | per-candidate authorization: `read_denied` resolves each candidate with `find_part_ref` + a `big_sync_syncable` access query (`part_store/sqlite.rs:1604` `permitted_parts`, big_repo twin `store/sqlite.rs:484`), uncached `SELECT` (`sqlite_core.rs:285`), and it runs **per event** while building a page (the default `page_event_is_readable`, `part_store.rs:458`, called at `:687`) | up to 2·E·P round-trips per page, plus per target per round and per requested part on the RPC edge | two set-based statements (`part_id IN (…)`, then `part_ref IN (…)`), or reuse `summarize_parts` as the RPC edge already does (`big_sync/rpc.rs:1050-1070`) — **needs the delivery-then-denial / `AnyOf` ordering question answered first** |
| 2 | per-transition write fan-out: ~6 statements per transition (`store/sqlite/sedimentree.rs:434-545`) | the write side of the AFW republish amplification already measured | batch the inserts, hoist the two per-batch updates out of the loop; measure per-transition-vs-per-(doc,part) first |
| 3 | per-part page statement with four correlated subselects + a `has_more` `EXISTS` (`parts_cursors.rs:724` `list_events_with_policy`) | per requested part | structural; not yet traced to an RPC call |
| 4-7 | observed-store N+1 (`part_store/sqlite.rs:1816` `observed_snapshot`); the clone set on the page round (`part_store.rs:539`, `:553`, and `subscriber.clone()` **inside** the per-event loop at `:687`); memory's per-bucket-page member scan (`memory.rs:273` `bucket_items_for_path`, the documented price of `obj_id` order); statement-per-item inside write transactions (`keyed_frontier/sqlite_generic.rs:137-147`, `:435-446`) | | as listed; the per-event `subscriber.clone()` is real but bounded by the subscription |

No result divergence found between the memory and sqlite twins on these paths; both are under the shared contract
harness, so a batch fix wants one contract case rather than a bespoke test per store.

Open questions from the lane, none decided: whether the candidate check can reuse `summarize_parts` without
perturbing delivery-then-denial and `AnyOf` ordering; whether `find_part_ref` is cacheable (no `DELETE FROM
big_sync_parts` found in the two files grepped — not an exhaustive sweep); and whether #2's loop is per transition
or per (doc, part) pair, which decides whether batching it is a 2× or a 50× win.

## Schema / key-denormalization audit (thread #7 `!>`, read-only lane, verified by me)

**Headline: the schema is already ref-based where it matters.** The tables that would blow up worst go through
integer refs — `big_sync_parts`/`big_sync_objs` allocate via `ensure_part_ref`/`ensure_obj_ref`
(`sqlite_core.rs:246-283`), and the high-row-count tables (`big_sync_members`, `big_sync_buckets`,
`big_sync_pending_members`) store **zero key bytes**, only refs. Refs are grow-only (no `DELETE FROM
big_sync_parts|objs|scopes` anywhere), so a ref is stable and swapping an inline key for one can never be
invalidated. The exposure is bloat, not a violated constraint: no `VARCHAR`, no `CHECK(length(...))`, no
truncation in either migration set — every table is `STRICT` (type, not length).

The one must-fix this audit found — `part_members_in` in `permission_writer.rs` joining `big_sync_parts` on `part_id` with no scope — has since been fixed: the helper takes the scope (`part_members_in(sql, scope_id, part)`, `WHERE p.scope_id = ?1 AND p.part_id = ?2`) and has a test that fails against the old predicate.

### worth-it

- `WITHOUT ROWID` on the tables whose PK contains a long key (syncable, peer_cursors, commits, fragments,
  event_log, admissions, tombstones, causal_ciphertext_index, watermarks, secret_blobs, deks,
  admission_readers) removes the separate PK b-tree, i.e. one full copy of the key set per row. Safe: the only
  `rowid`/`last_insert_rowid` use in `src` is a plugin's own table (`plug_plabels/wflows/label_engine.rs:223,285`).
  Keep `big_sync_parts`/`big_sync_objs` as rowid-alias tables — their ref *is* the rowid.
- Repeated-key inventory: `syncable.principal_id` 3 copies/row, `peer_cursors.peer_id` 2, `objs.obj_id` 3
  (deliberate — the bucket index needs it trailing for page order), `causal_ciphertext_index` 5 key copies and
  `sedimentree_id` 3×, `subduction_commits`/`_fragments` ~192 B of keys/row, `keyhive_event_log` `event_hash` 3×.

### Ordering must not be ref-ified

`ORDER BY part_id`/`obj_id` is load-bearing (`part_store/sqlite.rs:805,1023,1051,1389,1649,1824`;
`big_repo/store/sqlite.rs:137,166,188,205,219,564`): the page cursor is defined over key bytes and refs are
allocated in first-sight order, so ordering by ref would silently change page semantics.


Still unproven, marked as such: whether a path-shaped reserved part key (`/…`) can reach `ensure_part_ref` from
production. The audit found only `/seds`-shaped and 32/35-byte minters, and since every key now renders as base58,
that question is about minters, not about text.
