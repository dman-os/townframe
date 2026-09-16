# BigSync reconciliation — open items

Tracker for the PR that implements
[ADR-012](./adrs/012-bigsync-reconciliation-strategy.md). It exists so that
review findings, unresolved design calls, and half-finished investigations are
not lost between sessions.

Statuses: `open` (nobody owns it), `needs-decision` (blocked on an operator
call), `decided` (answer recorded, work not done), `in-progress`, `done`,
`dropped`. Provenance: `me` = verified by reading the code in this repo,
`fork`/`lane` = reported by a subagent and not yet independently verified. Every
review item below is `lane` provenance: the review lanes had no shell tool, so
they could **not** run `jj diff -r main..@` and could not attribute a finding to
the diff. Treat every item as "this is true of the current tree", not "this PR
broke it".

## A. Decisions

| id | item                                                                                            | status              |
| -- | ----------------------------------------------------------------------------------------------- | ------------------- |
| A1 | Blob inventory part access must mirror the inventory document's readers                         | decided (see below) |
| A2 | Per-document rule vs. `blob_inventories` group rule                                             | decided (see below) |
| A3 | Materialized access rows vs. resolving membership on the serving path                           | decided (see below) |
| A4 | Backfill for stores that already have the parts but no access rows                              | decided (see below) |
| A5 | Object cursor: keep the `object_replays` side path, or key bookkeeping by object | decided (implemented) — keep the generalized book, adopt `main`'s semantics |
| A6 | Host for the permission writer: pin worker + derived permission stream, vs. `group_part_worker` | decided (see below) |
| A7 | Part-store subscription tasks: stop token shape                                                 | decided (see below) |

### A1 — decided

The two derived blob inventory parts
(`blake3_derive_key("daybook.blob_inventory_partition.v1", inventory_doc_id)`,
one per `core_inventory_doc_id` / `docs_inventory_doc_id`;
`src/daybook_core/blobs.rs:16-20`) must carry access rows equal to "whoever can
read that inventory document". This is a _set_ rule, so a full rewrite
(`set_part_members`) is the natural write, and it cannot drift.

Why it matters today: the blob store is opened as deliberately policy-free
(`src/daybook_core/repo.rs:160-170`), but the new part-scoped check consults
that store's own `big_sync_syncable` rows for the scope id
(`src/big_sync/part_store/sqlite.rs:1626-1645` via
`permitted_parts`/`page_denied`), which nobody writes for these two parts.
Result: `Part` routes to them are denied for every peer, permanently (7 785
denials in the investigated run). Blob objects reach a part in exactly one
place, `pins_part_worker.rs:424-434`, which writes _membership_ only — no access
rows anywhere in the codebase.

### A2 — decided

Per-document rule: the part's access set is the readers of its inventory
document, as stated. Note that both inventory docs are administered by the
single `blob_inventories` group (`src/daybook_core/authority.rs:27-34,120-127`,
granted in `repo.rs:685-695`) and that group has its own part id
(`authority.rs:64-66`), so a group rule would resolve to the same set today. The
per-document rule is chosen because the two inventory docs need not stay in
lockstep.

### A3 — decided

Materialized rows in the blob store's table, written by the pin-part worker from
its derived permission input (A6), through the blob store handle. The serving
path (`permitted_parts`, `part_store/sqlite.rs:1626-1645`) must not run a
keyhive traversal per part request. `set_part_members` rewrites the whole set in
one transaction (`part_store/sqlite.rs:1456-1492`), so drift is not possible.

### A4 — decided

No backward compatibility: dev stores are recreated, so no migration and no
version bump. Seed the two parts unconditionally at boot (`repo.rs:999-1013`)
and let the derived permission input keep them current. Worth remembering: the
first-build path is guarded by `cursor == 0` (`group_part_worker.rs:56-105`), so
a store that already has a non-zero cursor would otherwise never get rows for
these parts.

### A5 — decided (implemented)

The object-route bookkeeping (`object_replays { acknowledged, in_flight }`,
`src/big_sync_core/cursor.rs:51-60`) was my recommendation, on the stated
grounds that "an object target has no part cursor to advance, so the object's
own cursor orders the replay". Two consequences of that shape are now visible:

- the object route **drops its resume point**: `refreshed_replay_target`
  re-issues `Object { obj_id }` with no cursor while the part arm re-issues
  `Part { part_id, cursor }` (`lib.rs:1443-1460`, verified `me`). A page that
  hits `LIMIT` re-requests from 0, and `next_cursor: None` doubles as the
  caught-up verdict (`lib.rs:1597-1622`), so a busy object route loops on its
  first page.
- the acknowledgment conflates completion kinds: `on_obj_sync_job_evt` mutates
  `acknowledged`/`in_flight` for _any_ kind while the doc comment justifies it
  only for content replays (`cursor.rs:251-266`, verified `me`).

The shape I now favour: give the object route a real cursor like the part route
(one watermark slot keyed by object, not by part), so ack/dedup/abandon are the
same rules as everywhere else, and drop the second bookkeeping system.

**Decided and implemented (2)**: keep the generalized `JobBoard`/`object_replays`
book rather than restoring `main`'s `ObjectJobState` verbatim, because a `JobBoard`
waiter's lanes ARE main's two per-waiter booleans and its streams are the parts — the
shape is the same thing generalized, and B1's fix just made the completion path
read the owed halves accurately. What was adopted from `main` is what was actually
wrong: the route carries its resume position (`SubscriptionTarget::Object { cursor }`,
with `obj_resume_cursor` filling it), a membership completion no longer counts as a
content acknowledgement, dropping a worker releases the claim while keeping the
position, and the multi-part supersede rule is pinned by a test against main's
semantics.

Residual, accepted: the route resumes from the **acknowledged** position, so an
object whose replays have not been acknowledged yet keeps being handed its current
page (bounded by the page hold and the round trip) until the backend reports.
Nothing is skipped — that is the price of not resuming from the last *emitted*
cursor, which is what let a dead job strand the object in the first place.

### A6 — decided

The pin-part worker must consume a **second, durable input**: a derived stream
of keyhive permission changes, filtered to the groups/documents/principals the
worker cares about, carrying enough history to say who had access before and
what changed (the analogue of how the AFW turns the doc delta stream into
durable frontier updates). The oracle lane's recommendation to write these rows
from `group_part_worker` is rejected: that worker is about keyhive group→part
mapping and should not take on permission mirroring for a store created
elsewhere. Machines are allowed more than one input.

The raw source is `big_repo_keyhive_event_log`
(`migrations/002_big_repo_init.sql:31`), which stores raw signed keyhive events
keyed by `seq`, with durable per-reader cursors in
`big_repo_keyhive_admission_readers` (`store/sqlite/events.rs:269-306`);
retention is pinned to the minimum registered reader (`events.rs:360-382`).
`group_part_worker` is the existing translator from that log to group membership
→ doc parts (`group_part_worker.rs:126-131,520-547,781-800`). Nothing in
`keyhive_admission` is reachable from daybook today (`pub(crate)`, no public
Resolved shape (oracle-lane study, `doc_delta_store.rs` as the analogue): a
big_repo-owned `RevisionedStore` projection over the admission log, consumed by
the pin worker through its own `ConcurrentDeltaWalker` plus state repo. Not a
facet (keyhive events are bincode `StaticEvent`s with no automerge branch), not
a part-store event log (permission rows would then be replicated content needing
their own policy rows, which is circular), not a materialized repo-wide table
(writes O(docs x principals) rows for one consumer). See the design block below.

Revocation semantics that must hold either way: a revocation takes effect only
once the keyhive pull lands, exactly like the doc store
(`group_part_worker.rs:56-127`), so the blob part is never looser than the doc
part it derives from.

Design from the study, with my calls on the questions it raised:

- Delta entry: `{ doc_id, groups, agents_after (absolute reader set), added,
  removed, changed }`. The absolute set is load-bearing: the effect is
  idempotent and latest-state, so a replay after a crash or a lost consumer
  memory is safe without the doc-delta one-transaction contract, and the
  consumer can skip the write when the set already matches the part.
- Selection: `KeyhivePermissionSelector { docs, interesting_groups }` compiled
  into the reader, like `AutomergeFrontierSelector`. SQL cannot filter opaque
  `event_bytes`, so selection suppresses entry production, never decode. The
  "skip events whose issuer group is not interesting" shortcut is **not sound**
  (an event can move a watched doc into a new group, and the containing-group
  set is itself derived state) — dropped. The filter may only skip events that
  name no watched doc, and must fall back to the full decode plus containing
  group query when it cannot classify.
- Owner: big_repo owns the projection, the public selector/entry types (beside
  `AutomergeFrontierSelector`), and a new retention reader constant registered
  lazily on first open. daybook owns the selector instance, the walker state
  repo in the blob store's SQLite DB, the `set_part_members` effect, and the
  keyed machine. `group_part_worker` is untouched.
- Vocabulary: reuse the already-public grant/revoke shapes
  (`BigRepoDomainNotification`, `changes.rs:157-200`), not a parallel one.
- Reader predicate: exactly the predicate the doc scope uses for its own part
  access (`Access::is_reader()`, `group_part_worker.rs:666-673`), not a fresh
  choice. The rule is "mirror the doc part".
- Boot and caught-up proof, no back compat: seed unconditionally (no
  `cursor == 0` guard). Read the admission head H *before* the closures, compute
  both docs' reader sets from keyhive, `set_part_members` both parts, commit the
  consumer memory and the walker progress at H, then register the retention
  cursor at H and open the walker after H. The retention cursor may LAG and must
  never lead: a reader that leads the walker lets `prune_admitted_events` delete
  rows the walker has not read yet, silently dropping wake-ups. (Corrected by the
  ADR study: the effect, the memory/cursor and the retention row live in three
  different SQLite databases, so a single transaction is not achievable; the safe
  order is effect (idempotent, full replacement) -> memory+cursor in one tx ->
  retention last.)
- When this node cannot read or has no content for the inventory doc, mirror
  the doc part exactly (empty set = deny, i.e. wipe). Denying is the
  conservative side of "never looser than the doc part", and it is what the doc
  scope itself does (`document_has_content` gate, `group_part_worker.rs:632-662`).
- Cost: about zero new durable bytes in steady state (the projection is
  on-read). The real costs are one more tailer on the log (4 read-pool queries
  per second at `IDLE_POLL` 250 ms), one bincode decode per admitted event, and
  a new repo-wide retention floor of (floor minus reader seq) x (event_bytes +
  ~80 B) while the consumer lags. Events/s and bytes/event must be measured
  from a real store, not guessed.
- Consumption: directly, through a second `ConcurrentDeltaWalker` in the pin
  worker's existing `select!`, with its own `(namespace, consumer_id)` pair, the
  same keyed scheduler and the same per-document reconcile mutex. The key
  spaces differ (the facet input keys by branch, permissions key by doc), so the
  pending map becomes an enum or stays per input.
- Retention and pruning: pruning is driven by the keyhive admission reader
  cursors (`events.rs:350-382`), so this adds one reader to the cursor count.
  Accepted: a filtered consumer settles the revisions it produces no entries
  for, so it acks past most of the log quickly (the doc-delta precedent settles
  entry-less revisions directly, `doc_delta_store.rs:243-248`), and the floor
  only grows while the consumer is stalled.
- Sparseness carries through from the doc delta: no full keyhive mirror of
  who-can-access-what. Only interest-scoped sparse state keyed by the watched
  doc, kept for diffing later. The delta entry and the consumer memory must be
  restated under that constraint; whether an absolute `agents_after` full reader
  set violates it is open. The answer that landed: keep one sparse row per watched
  subject (its closure and agent map) in the consumer's own walker-state
  namespace, and never a hive-wide mirror. See `docs/adrs/013-keyhive-access-delta-stream.md`
  §3-§4, and ADR-008 for the walker contract.
- The store question is answered: `keyhive_admission::Store` is ALREADY a
  `RevisionedStore` over the admission log and `group_part_worker` already drives
  it with `ConcurrentDeltaWalker`, so there is no new projection to build - just a
  mapping layer to export and a consumer to attach. The log is a wake-up and
  ordering signal, not a replay source: the payload is read-time keyhive state
  (`agents_for_membered`), which is always >= the revision being reported, so a
  crash replays into an unchanged closure and emits nothing.
- ADR written: `docs/adrs/013-keyhive-access-delta-stream.md` (Proposed). My calls
  on the three questions it raised: (1) the mapping stays in big_repo and is
  exported in the frontier store's shape (selector carries the consumer's memory),
  rather than widening `keyhive_admission`/`AdmittedRow` to `pub` for a
  daybook-side mapping; (2) the mechanism is a general watched-subject set
(documents and groups), wired to exactly the two repository inventory documents
  today - no shard configuration surface until a shard exists; (3) the entry-cost
  numbers stand as labelled model bounds, with row/entry counters added during
  implementation and the ADR updated with real ratios.
- Provenance correction from the study: the blob mirror must be the DOC-LEVEL
  closure (`agents_for_membered(inventory_doc)`), which is a deliberate divergence
  from the doc parts' rows (those come from per-containing-group `part_agents`,
  because a part is a one-way digest of a group id). Access levels are mirrored
  verbatim, `Relay` included: filtering to readers would withhold exactly what
  ADR 001 grants relays. The affected-event test is O(k) against cached closures
  rather than a group/document containment scan, and an unchanged closure emits no
  entry, which also avoids the no-op-write wart recorded in ADR 012 decision 5.
- Drift recorded in ADR 013: my earlier pointer to a "doc-delta section of ADR
  012" was wrong - there is none; the doc-delta design is ADR 008 §5 plus the
  code, and the code (`DocDeltaRevisionStore` owns no state) differs from §5.
- Scope of the stream: NOT documents only. The same mechanism must cover keyhive
  groups and everything else that carries access (principals, group
  membership, delegation), with names that do not read as "documents only".
  What the general watched-entity set is, and how selection is expressed over
  it, belongs in the ADR.
- Transitive versus direct access is an open question with a working preference
  for transitive (the effective resulting set for a watched entity). It is only
  acceptable if the cost is understood: quantify storage and computation for
  both, what each buys a consumer, and where each one's staleness risk lies.
  The baseline to explain against is what `group_part_worker` does today
  (`reconcile_doc`, `agents_for_membered`, the per-containing-group `part_agents`
  sets, the memo, and the access-level predicate), because the blob parts must
  mirror the doc parts.
- ADR-013 review of the operator's `!>` comments landed; decisions taken:
  (a) memory-optional acking is sound ONLY on the `All` subject set (where the
  affected test is vacuous); on `Watched` the memory row is required because it
  is §6's index, not a payload cache — no sink can answer "which groups are
  reachable from this subject" (the part store exposes only `member_count`/
  `part_dirty_count`). (b) The retention reader id is derived from the same
  `(namespace, consumer_id)` that owns the walker cursor, so `note_retention`
  takes no raw reader string. (c) `computed_at_seq` stays but is diagnostic,
  never an ordering or skip key (a closure read may lead its revision). (d) Two
  machines, one per input, each with its own budget — the `pin_worker` precedent
  does not split one budget. (e) `All` is built now; `Watched` stays the path for
  the first consumer. (f) The event subject must be `Delegation::subject_id()`
  (proof-chain root), never the immediate signer — see B19 for the existing
  code that uses the signer.

### A7 — decided

Subscription pumps in the part stores get a **stop token** instead of relying on
receiver-drop, and the abortable join set needs a companion stop-token type (not
just a timeout-based `stop`). Sites: `src/big_sync/part_store.rs:445` (trait
default), `part_store/sqlite.rs:1249` (holds `self.clone()`, i.e. an Arc cycle),
`memory.rs:1138`. Existing precedent for owner-held task sets stopped by a
token: `BigRepoRpcStopToken.subscription_tasks` (`src/big_repo/rpc.rs:153-170`).

## B. Review backlog

Dense on purpose: each row is a claim to _check_, not a fact to trust. Evidence
is `file:line` in the current tree.

| id  | area            | claim                                                                                                                                                                                                                                                                                                                      | evidence                                                                                                                                                                                                                                         | sev (lane) | status                                                                                                                                                                |
| --- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| B1  | `big_sync_core` | The cursor set collected from a cancelled removal worker was handed to both the removal worker and the sync worker, while each completion settled every cursor with a fixed lane; a lane never owed panicked, and a lane the other worker owed was left pending forever (frozen part watermark / `cursor_active` stuck). | FIXED: cursors are split by owed lane (`lib.rs` `resume_pending_removal`), each completion consults `JobBoard::owes_lane` before settling (`watermark.rs`, guards in `handle_remove_completed`/`handle_sync_completed`), and the exhausted case settles membership lanes only when the zombie applied the removal. Test `readd_settles_each_lane_from_the_worker_that_owes_it` drives the real `Removed` then `Changed` path. | high | done — verified by me: `cargo clippy -p big_sync_core --all-targets --all-features` exit 0, `cargo nextest run -p big_sync_core` 82/82 pass; the fork also reproduced the old panic by reverting the fix |
| B2  | `big_repo`      | A `Deferred` publish was parked with no re-drive, so a document whose next event never arrived left its `pending_admission`/`pending_part_sources` cursors unacked forever, and the walker's contiguous-prefix ack stalled every document behind it. | FIXED: the `Deferred` arm now re-arms via `TokioKeyedScheduler::retry` (new `rearm_deferred_publish` + `retry_publish`, with `publish_task`/`task_future` extracted so the retry rebuilds the command from the newest pending state), using the retry bookkeeping the completion already carries; the backoff doubles per attempt and the loop's existing `next_deadline`/`tick` arm drives it. Cursors are still not acked on that path. Delayed retries do not consume physical budget (`active_count` counts started handles), so a stuck document cannot starve admission. A wake arriving while a retry is pending replaces it (cancel-and-replace) — pinned by a test. | high | done — verified by me: clippy clean, `nextest -p big_repo -E 'test(automerge_frontier_worker)'` 5/5 pass; negative check (revert `rearm` to `park`) fails 2 of the 3 new tests |
| B2a | `big_repo`      | Optimization, not a defect: `BigRepoLocalNotification::DocMaterializationReady` (`changes.rs:77-80`) is the exact signal the deferred paths wait on, and the AFW ignores it. Wiring it as a `wake_docs` insert would make the re-drive immediate instead of up to one backoff late. | `on_local_notifications`; the retry above is the durable mechanism and must stay (in-memory notifications are lossy by design). | low | open — my call: do not wire now; if latency matters, restrict the wake to documents that currently have a pending re-arm |
| B3  | `big_sync_core` | An object's `acknowledged` was advanced by a membership (removal) completion, i.e. a removal counted as the backend having observed the object's content replay, and it cleared the in-flight content claim. | FIXED: `on_obj_sync_job_evt` touches the replay position only for `kind == Sync` (a part-scoped *sync* of the same object still counts). Test `a_membership_completion_does_not_acknowledge_the_object_replay`. | med | done — verified by me: `nextest -p big_sync_core -p big_sync` 170/170 pass, clippy clean |
| B4  | `big_sync_core` | Three sites dropped a sync worker without `abandon_obj_sync`, so the object's content claim could never be settled and a later delivery of the same cursor read as "already in flight" — the object's content was never fetched again. | FIXED: all the drop sites call `abandon_obj_sync`, which now releases the **claim** (`in_flight = None`) while keeping the **position** (`acknowledged`) — clearing the whole entry also threw the position away and would send the route back to its first page. Test `an_abandoned_object_replay_is_owed_again` extended to pin that the position survives. | med | done — verified by me: 170/170 pass, clippy clean |
| B5  | `big_sync_core` | The multi-part supersede rule was untested against the vetted `main` semantics. | RESOLVED, no behaviour change needed: the fork compared it to `main` and the current split is equivalent — `Retain` (a waiter owing Membership whose last stream is superseded: keep the membership lane, drop the sync need) matches main's `pending_membership && parts.len() == 1`, and `Release` (drop the stream, free that stream's cursor, leave the waiter's lanes) matches main's "remove the part from the waiter and mark that part's cursor ready". The lane book IS main's two per-waiter booleans generalized. Test `a_supersede_leaves_a_two_lane_waiter_gating_its_other_part_on_both_halves` pins the hardest case. | med | done — verified by me: 170/170 pass, clippy clean |
| B6  | part store      | Bucket endpoints were served to any authenticated peer with no access check, so a peer that knows a scope key and part id could walk leaf pages and learn every `obj_id` plus a guessed-payload fingerprint oracle. The other two peer-facing read arms already took the authenticated peer; the bucket arms destructured `WithChannels { inner, tx, .. }` and dropped it. | FIXED, denied and unified: one policy entry point (`HostPartStore::read_denied(ReadTarget::{Part,Object}, subscriber)`, `big_sync/part_store.rs:354-366`, defaulting to `permitted_parts`) is now asked by every peer-facing read arm — `PeerSummary` (`rpc.rs:472`), `ReplayPage` (trait default `replay_page`, `:398`), both bucket methods in both stores (`part_store/sqlite.rs:800,904`, `memory.rs:551,592`, doc-scope twin `parts_cursors.rs:243,341`). `get_changed_buckets`/`leaf_buckets`/`replay_page`/`subscribe` take the subscriber (interface change, no shim); a refusal is answered in the shape the arm already uses for a part it does not know (`UnkownParts`/`UnkownPart`/`UnknownPart`), never as an empty or partial page, and an unauthenticated caller is refused too. Test `every_read_arm_refuses_what_the_asker_may_not_read` (`rpc.rs:925-1095`) is a table over all four arms x {granted, authenticated-without-access, unauthenticated}, so "unified" is a property of a test. | high | done — verified by me: clippy clean on `big_sync`/`big_sync_core`, `nextest -p big_sync` 91/91, `-p big_sync_core` 86/86, `cargo check -p big_repo --all-features --all-targets` exit 0; negative check by me: disabling the `read_denied` guard in `MemoryPartStore::get_changed_buckets` makes the table test fail on the real leak (a refused peer gets the full bucket walk), restored and re-run green |
| B6a | part store      | Residual, reported for a decision: the *decision* is unified but the *shape* of a part-scoped refusal is not. `ReplayPageOutcome::Unauthorized` (`big_sync_core/rpc.rs:438-439`, "may not read the target's part") is distinguishable from `UnknownPart`, so a page refusal confirms the part exists — the one non-disclosure gap left on this surface. It cannot simply be collapsed: the signal is load-bearing for revocation (`big_sync_core/lib.rs:1628`, `big_repo/backend.rs:150-221` does keyhive reconciliation on it, `test2/revocation.rs` asserts a stale reader is rejected with it, and the cursor is settled from it). | `rpc.rs:519-545` vs `:563-605`; `big_sync_core/rpc.rs:429-440`. | med | open — my recommendation: keep the distinct `Unauthorized` (a real workflow consumes it; the leak is the existence of a part id the caller already named), and record it rather than reworking revocation. Collapsing it would need another revocation channel first |
| B6b | part store      | `SqlitePartStore`'s page/subscription path never consulted `hidden_parts` (only `summarize_parts` did), while the config documents the field as "invisible to remote part access". | RESOLVED, no new gate needed: the peer-facing page already resolves the part through `summarize_parts`, which gates `hidden_parts`, so `replay_page` answers `UnknownPart` for a hidden part while the trusted local reader (`open_local_revision_reader*`) still sees it. Pinned by `hidden_parts_are_invisible_to_the_page_path` in both stores (control: a visible part pages, the hidden one does not, and the local reader still reads it). The responder did not need a check, and none was added. | `part_store/sqlite.rs:377` (gate), `:1975-2030` (test); `memory.rs:1797` | med | done — verified by me: 91/91 |
| B7  | part store      | `SqlitePartStore`'s bucket queries omitted the `hidden_parts` gate that `summarize_parts` and the sibling doc store apply, so the same RPC had different visibility per scope. | FIXED for both stores: `get_changed_buckets`/`leaf_buckets` now answer `UnkownParts`/`UnkownPart` for a hidden part, checked before the limit check as `parts_cursors.rs` does. Tests `hidden_parts_are_invisible_to_bucket_walks` in each store (visible part as the control, then both endpoints must call the hidden one unknown). | med | done — verified by me: clippy clean, `nextest -p big_sync` 88/88; negative check (guards disabled) leaks the hidden part's summaries |
| B8  | part store      | Page limit was compared with `>=` _after_ pushing, so `limit == 0` returned one event when one was waiting and zero once the hold expired: a nondeterministic violation of the wire contract. | FIXED in the trait's default body (both stores use it): a zero limit returns an empty page before anything is drained, and `next_cursor` stays the caller's own position because nothing was drained and an empty page may not claim caught-up. The push-then-check loop stays exact for `limit >= 1`. Shared harness assertion `assert_zero_page_limit_carries_no_events`. | med | done — verified by me: 88/88; negative check reproduces `a zero limit must carry no events, attempt 0 got 1` |
| B9  | part store      | The responder capped `hold_ms` but never capped `limit`; a peer could ask for `u32::MAX` and make the responder buffer a whole part in one reply. | FIXED at the untrusted boundary: `MAX_PAGE_LIMIT = 1024` + `page_limit()`, applied to the `ReplayPage` request. It silently lowers an over-large request (no error variant exists, and asking for more is not a protocol violation) and can never raise one. The store is deliberately not clamped: `replay_page` is also reachable in-process and the field is the caller's own bound. Test `an_untrusted_page_limit_is_capped` pins the boundary plus the no-raise property. | med | done — verified by me: clippy clean, 88/88 |
| B9a | part store      | Same class as B9 on the other two peer-facing read arms: `GetChangedBucketsRequest`/`LeafBucketsRequest` carried an unbounded `u32` page hint, so a peer asking `u32::MAX` could make the responder buffer a whole part's bucket summaries or leaf entries. | FIXED: `MAX_BUCKET_LIMIT = 1024` + `bucket_limit()`, applied to both requests at the responder before the store call (`rpc.rs:56-62,570,601`). The cap bounds the *hint* only, never the answer: the stores add `BuckId::ARITY` headroom for the last bucket's changed siblings after the cap, so the sibling-group guarantee survives. Same rule as B9 — it may only lower a request, never raise one. Test `an_untrusted_bucket_limit_is_capped`. | med | done — verified by me: clippy clean, 91/91 |
| B10 | part store      | An `Object` target replayed from cursor 0 (the store hard-coded `0` as the page's lower bound) and `refreshed_replay_target` re-issued the target with no position, so an object route with more than one page of events returned its first page forever. | FIXED, with an interface change: `SubscriptionTarget::Object` gained `cursor: CursorIndex` (no shim — the wire enum changed, and the 18 pattern sites became `{ obj_id, .. }` plus 20 constructions). The machine fills it from the new `obj_resume_cursor` (the acknowledged position) and the store uses it as the page's exclusive lower bound. Tests: `an_object_route_is_refreshed_with_the_cursor_its_replay_acknowledged` (machine) and `assert_object_route_resumes_from_its_cursor` in the shared store contract harness (both stores). | med | done — verified by me: 170/170 pass, clippy clean, `cargo check -p big_repo --all-features --all-targets` exit 0 |
| B11 | part store      | `limit_hint == 0` at the bucket endpoints. | RESOLVED, premise corrected: the two stores and the doc-scope sibling already agreed, and the readings differ per field by design. `GetChangedBucketsRequest::limit_hint` is a documented page bound (zero = zero buckets); `LeafBucketsRequest::limit_hint` is an undocumented hint (zero = no preference -> the smallest useful page of one, which is load-bearing: memory's `buckets[limit - 1]` would underflow without it). Behaviour unchanged; both readings are now stated at each site and pinned by the harness assertion `assert_bucket_limit_hints_agree_across_endpoints`. | low | done — verified by me: 88/88; negative checks: disabling memory's zero guard panics at `memory.rs:293`, removing leaf's `max(1)` fails the one-entry assertion. Follow-up (folded into the B6 lane): the wire type `LeafBucketsRequest::limit_hint` still has no doc comment in `big_sync_core/rpc.rs` |
| B12 | `big_repo`      | `publish_heads` writes frontier part membership directly while publish tasks are abortable, so an aborted/racing publish can re-add a membership the worker just removed.                                                                                                                                                  | `automerge_frontier_worker.rs:370-382` vs `:595-598`, `:999-1005`.                                                                                                                                                                               | med        | open                                                                                                                                                                  |
| B13 | `big_repo`      | `is_broken()` is checked once, then never re-checked before the heads are read under the doc lock and the payload written, so an invalidation in that window still advertises heads from a broken bundle.                                                                                                                  | `automerge_frontier_worker.rs:341-358` (comment promises otherwise at `:336-340`).                                                                                                                                                               | med        | fixed - see section F |
| B14 | `big_repo`      | With `let walk = pending;`, a live doc with non-empty `blocked_refs` reports `SyncDocOutcome::Ready` while the same blocked set on a cold doc reports `Pending`, so the reported outcome is a function of liveness rather than of resolution state.                                                                        | `doc_worker.rs:1661,1700-1705`.                                                                                                                                                                                                                  | med        | open - needs a decision, see section F |
| B15 | harness         | `Node::restart` hard-codes `keyhive_change_notifs: true`, so a node booted via `boot_without_keyhive_notifs` comes back with the subscription wired, destroying the "stale membership view by construction" property for any restarting test.                                                                              | `test2/harness/topo.rs:287-305` (esp. `:303`), `:140`.                                                                                                                                                                                           | low        | open                                                                                                                                                                  |
| B16 | harness         | The two new notification-driven restart tests pre-grant `/seds` pull before `connect`, while every sibling restart test reaches the same documents with no grant — either the grant is load-bearing (siblings pass for another reason) or the tests bypass the product's grant path.                                       | `test2/harness/topo.rs:354`, `test2/restart.rs:748-751`.                                                                                                                                                                                         | med        | open                                                                                                                                                                  |
| B17 | `big_sync_core` | A cancelled removal whose zombie then *fails* with every hint already cancelled kept its membership lanes owed with no worker left that could settle them, because the settle was gated on the zombie having applied. | FIXED: with no re-removal left, the membership lanes now finish regardless of the zombie's outcome, and `ZombieRemovalOutcome` is gone (it was the only user). This is not an acknowledgement of a mutation that never ran: a Membership completion carries no acknowledgement — the object's replay position advances only on a Sync completion (B3) — and the only canceller of a hint is the re-add path (`cancel_obj_removal_hint` is called from `Changed`), so every cancelled hint means the object belongs in that part again. Test `a_failed_cancelled_removal_still_finishes_its_membership_lanes` (negative-checked: it fails with the old gate). | med | done — verified by me: clippy clean, `nextest -p big_sync_core -p big_sync` 171/171 pass |
| B18 | `big_sync_core` | If every `re_added_parts` entry is filtered out at resume (the part is gone from the peer), no sync worker is spawned and the sync lanes stay owed; the fork traced `remove_part` retiring the part's book, which frees those waiters, but that is a reading and not a test. | `lib.rs` `resume_pending_removal` + `remove_part`. | low | open — needs a test |
| B19 | `big_repo` | `group_part_worker` takes the event's *immediate signer* (`delegation.issuer`) as the group id when deriving a part (`group_part_worker.rs:734-741,773`), but the id keyhive dispatched the operation to is `Delegation::subject_id()`, the proof chain's root issuer (`../keyhive/keyhive_core/src/crypto/signed_ext.rs:28-46`, consumed at `keyhive.rs:1980,2066`). When a non-root group member re-delegates the two differ, so a part can be derived for an id that is not a membered graph. Latent, pre-existing, out of ADR 013's scope — recorded, not fixed. | evidence above; found by the ADR review while checking the subject derivation ADR 013 must use | med | fixed - see section F |

Known-correct items from the same review (recorded so they are not
re-litigated): the sqlite frontier reader enables its notification before
reading the committed revision (`keyed_frontier/sqlite_read.rs:154-160`);
memory's zero-limit `list_events` resume point matches sqlite's documented rule
(`part_store/memory.rs:1050-1051`); and all three subscription pumps exit on
send error, so none is an unbounded leak _by itself_ (`part_store.rs:445`,
`part_store/sqlite.rs:1249`, `memory.rs:1138`,
`store/sqlite/sedimentree.rs:171`).

## C. `tier10_4` stress timeout (the failure that started this)

Test:
`big_repo::test2::stress::tests::long_test_big_repo_tier10_stress_4_editor_converges`,
`TIMEOUT 240.033s`; neighbours pass. Status: **open**.

Pinned (`me`, from the log):

- Two docs diverge; only `zEdmxWPmx2WH` (editor-4) differs, holding one extra
  local commit per doc (`D1 b2686aac` @34.06s, `D2 9f308780` @34.19s) against
  the other three nodes' `d33b1ac0` / `30e5ac76`.
- After those two commits there is no activity for either doc for ~200s on any
  node: no sync task spawned, no part events.
- The other three nodes each spawned a sync for these docs at 33.5s / 34.2s,
  i.e. for the older content.

Debunked leads (do not re-chase):

- "The frontier worker never published because a keyhive scope lookup blocked."
  The stress boot uses `WorkerGroupScope::disabled()` = `Groups(∅)`
  (`test2/harness/topo.rs`), so every publish ending as `OutOfScope` is the
  expected path and that worker is not the notification path this test depends
  on. The 8 lookups that hung >150s belong to 7 unrelated docs inside a worker
  that could not act on them.
- "`try_send` failure tears down a live subscriber." The subscription channel is
  `mpsc::unbounded` (`store/sqlite/sedimentree.rs:146`), so `try_send` can only
  fail `Closed`; the 4 939 teardowns are genuine drops.

Missing instrumentation that blocks the next step: sync-scheduling log lines
carry no node identity, so the 521 publish entries cannot be attributed to a
node, and the `OutOfScope` outcome is silent. Add both, then re-run.

**C-critical, found after the fact: the central negative ("no sync task spawned for
editor-4's post-34.2s writes on any node for ~200s") rests on a log line that the
runs could not have emitted.** `spawn_sync_task`'s only positive event —
`"spawn sync task"` — was `trace!` (`big_sync/worker.rs:953`) while both of its skip
verdicts are `debug!` (`:927` "skipping sync task for removed peer", `:940` "sync task
has no resolvable backend; skipping"), and `setup_tracing`'s default filter is `info`
(`utils_rs/testing.rs:19-29`). The investigation ran `RUST_LOG_TEST=debug` — 103
occurrences of exactly that in the session log and zero of `trace` — so no run at that
level can distinguish *never scheduled* from *scheduled, then skipped or run to no
effect*. The line is now `debug!`, so the question is answerable at the level these
tests run. Re-verify before drawing any further conclusion from the absence: if the
`spawn` line simply was not visible, the stall may be "tasks spawned and did nothing",
which is a different hunt from "nothing was scheduled". The two `debug!` skip paths
are the specific mechanisms that would drop a scheduled task, so a re-run should grep
for both of them by name as well as for the now-visible spawn.

**And the reproduction, one run later.** The test reproduces on demand — `RUST_LOG_TEST=debug cargo nextest run -p
big_repo -E 'test(...tier10_stress_4_editor_converges)'` timed out at 240s on the first attempt, exactly as
`/tmp/flake.log` did (log kept at `/tmp/tier10-rerun.log`, 42 MB). What it shows, in order:

1. all three mutation phases completed **and settled** (`stress phase3 post-mutations settled`);
2. the final spanning-topology reconnect completed and the cluster settled — `stress final cluster settle
   complete` at **27.576s**;
3. `BigRepoStressFixture::assert_cluster_alignment` (`test2/stress.rs:510`) was then entered and **never
   returned**: `stress cluster alignment complete` never appears, so that one call consumed the remaining
   212 seconds until the kill. Its first step is a freeze/reopen barrier with **no timeout** —
   `wait_for_quiescence_freeze(None)` → `unfreeze()` → `wait_for_quiescence(None)` over *every* node
   concurrently (`stress.rs:526-544`) — so one node that never quiesces hangs the barrier indefinitely;
4. the barrier's own convergence poll (20 samples, every ~10.3s from 39.5s to 235.7s) reports the **same
   mismatch every time**: `converged=false per_node="…all four at 16/16…" mismatch="z8SuPgcTV4Dj:ref=1
   [zEdmxWPmx2WH:1]"`. All head/commit activity in the log stops at **27.9s** (`heads:` max 27.8988s,
   `ColdSedimentreeHeadsUpdated` max 27.8588s), so nothing changes for the rest of the run.

**Correction to an earlier reading of this same log (mine, in this document's previous revision): I claimed
750 `spawn sync task` lines kept firing until 239.97s and that "no sync task was spawned" was therefore an
artifact of the `trace!` level. That is wrong, and the timestamps — which I did not check before making the
claim — refute it: all 750 `spawn sync task` lines occur before 28s and **zero** occur after. I had read the
tail of the log as sync-task spawns when those lines are `spawn machine task`.** So the original
"no sync task spawned for editor-4's writes" observation stands as *fact about the log*; what was wrong was
only the level it was inferred from, which is now fixed. For completeness, the post-28s churn is 47 919
`spawn machine task` lines in periodic bursts every ~53 ms — but machine-task spawning also runs at ~180/s
during the healthy phases (4 988 before 28s), so the *rate* is not by itself the anomaly and the burst
pattern needs a code-level explanation rather than a log-level one.

**So the shape of the bug is: a doc's locally persisted content has no carrier.** `zEdmxWPmx2WH` (editor-4)
logs `persisting local Automerge commits` for doc `z8SuPgcTV4Dj` at 18.88s, and from 28s on no sync task is
ever scheduled for anything, so that content never crosses and the alignment barrier waits forever on a
cluster that is one commit away from agreeing. That is the *no re-drive* family (B1/B2 territory), not the
"nothing was scheduled because nothing was owed" family — note that B2's deferred-publish re-arm is already in
this working copy and this run still reproduces, so B2's fix does not cover this path.

**The "node identity" half of the instrumentation was never missing either**: every one of those 750 spawn
lines carries its `machine_loop{worker=editor-N}` span, so sync-scheduling *is* attributable to a node. What
remains genuinely silent is the AFW's `OutOfScope` verdict (`automerge_frontier_worker.rs:1082`), which tears
the frontier mirror down without a log line of its own.

The log-triage worker (`ef65d7d6`) hit its 30-minute limit before writing its report, but its transcript
tail shows it had reached this same shape (the convergence-poll mismatch and the `heads` cutoff); the facts
above were all re-derived and verified here rather than taken from it.

## D. Landed / uncommitted in the working copy

| id | item                                                                                                                                                                                                      | status                                                                       |
| -- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| D1 | `WorkerGroupScope::admits_nothing()` + AFW skips the keyhive membership walk when the scope is `Groups(∅)` (its answer is `false` by construction; the walk is a shared-lock traversal). Unit test added. | done (uncommitted) — `check`, `clippy --all-targets`, targeted test all pass |
| D2 | Remove `SCOPE_LOOKUP_TIMEOUT` from the AFW scope lookup. A timeout there converts a hang into a `Deferred` publish, i.e. a stall dressed up as progress: it hides a deadlock instead of surfacing it. `MATERIALIZATION_WAIT_TIMEOUT` stays (its worst case is holding one AFW budget slot). | dropped (removed) |
| D3 | `// TEMP-HUNT:` marker in `daybook_core/sync/tests.rs` with `DAYB_KEYHIVE_DIAG`; keyhive scope-lookup instrumentation.                                                                                    | open — remove once C is closed                                               |

House rules for this work: never edit the nextest config; never run `git` (jj
only); bounded log reads via
`.agents/skills/stress-sync-investigation/scripts/bounded-log.py`.

## E. Why the AFW is live with an empty scope

`WorkerGroupScope::disabled()` = `Groups(∅)` is a runtime value, not "do not start the
worker". The stress harness and read-only/relay boots use it, and the worker is still the
consumer that acknowledges the admission/part-source cursors for the events it sees: its
`OutOfScope` arm tears down stale mirror membership and acks both sources so the walker's
durable cursor advances. That is why it must stay live (skipping the events entirely would
leave their cursors unacked and stall the cursor), and why `admits_nothing()` — keeping the
worker but skipping a membership walk whose answer is `false` by construction — is the
right short-circuit, while a timeout on that walk was not.

## F. Keyhive access delta stream (ADR 013) — lane 1 map

Lane 1 is the stream and its reader contract; the blob-inventory permission writer is lane 2.
Plan (read-only recon from a forked-context oracle): `/tmp/delta-stream-plan.md`. Reuse, do not
reinvent: `keyhive_admission::Store` is already a `RevisionedStore` over `ConcurrentDeltaWalker`;
the durable sparse state is `SqliteDeltaWalkerStateRepo` (`delta_walker_key_state`), which is what
makes memory-optional acking sound; the shape to copy is `DocDeltaRevisionStore`;
`FacetSetRevisionStore` is the precedent for constructing the input state repo with an explicit
`(namespace, consumer_id)`. The stream owns no cursor and no ack path.

Settled while producing the map (so the lane is not blocked on them):

- Subject-less events produce no entry, as a stated classification with a test, not a silent drop:
  `PrekeysExpanded`/`PrekeyRotated` cannot change a closure (they change reachability), and a
  `CgkaOperation` whose id is not a keyhive document has no closure to read. Recorded in ADR §6.
- The read limits bound *source entries read*, not entries emitted — the walker's own documented
  reading; `Watched` may emit several entries from one source entry. New open question 8.
- `All` ships first and is the supported shape for lane 2 (not a stopgap for `Watched`); `Watched`
  stays an explicit `todo!()` with no half-built cache behind it. ADR §2 wording fixed.
- The closure helper changes type rather than being wrapped: `agents_for_membered` returns
  `BTreeMap<Identifier, Access>` (4 call sites), because the row codec and the walker key need the
  identifier, and byte keys are the caller's choice. `KeyhiveAccessMemory` stays the name but is now
  stated to be a *row value*, not a repo.
- The sink takes `HashMap<PeerKey, Access>` while the closure is keyed by `Identifier`, so the
  conversion happens at write time in the consumer, never inside the stream (ADR §5).
- Retention: the reader id derives from `(namespace, consumer_id)` beside the pair that owns the
  walker cursor (`big_sync/delta_walker_state.rs`), while the single consumer-facing op
  (`note_retention`) stays on the exported store. Note for the pruner: a reader registered at 0
  pins all pruning in the scope (`events.rs:364-366`), and `archived_through` is
  `#[cfg(test)]`-gated today and must be un-gated for the §9 recovery signal.
- ADR citations corrected: `Access::is_fetcher` is `../keyhive/.../access.rs:42-45` (not
  `access.rs:47-49`), the sink is `big_sync/part_store.rs:519-524` / impl `:1476` (not
  `:500-509`, which is `open_local_revision_reader_all`), the `big_sync_syncable` rows are written
  at `parts_cursors.rs:866-889` and read as the fetch predicate at `store/sqlite.rs:836-841`, and
  the group-part worker's signer bug bites at `group_part_worker.rs:759,786` (B19).

Not lane 1: the O(k) affected test belongs to the `Watched` lane (under `All` there is no affected
test to assert); lane 1's honest assertion is that no closure cache is consulted, checked with a
spy walker-state repo. The boot-order property (§8) is lane 2's test; lane 1 must not add a seed
method.

### Lane 1 status: DONE (uncommitted)

`src/big_repo/runtime2/keyhive_access_stream.rs` (955 lines) implements the stream and its reader
contract, re-exported from `big_repo::lib.rs`. `Watched` is a `todo!()` with no cache behind it;
`All` is implemented as §2/§5 define. 11 in-crate tests pass, including the `RevisionedStore`
contract harness against a real sqlite admission store.

The lane's own worker was killed twice by the 30-minute child limit, once mid-implementation and once
mid-verification, and it died the second time after reporting "all 11 pass" when 3 were red. I
finished and verified it by hand: fixed its two compile errors (the reader's `open` shadowed
`revision` in a match arm; the spy state repo's `begin_with_context` needed `Ok(..)` because the
concrete repo's method is sync while the trait's is async), two clippy errors (unused imports; a
`let _ = future` in the `Watched` test), kept the reader's `memory` field with a reasoned
`#[expect(dead_code)]` (only the `Watched` path will read it, and `expect` forces that lane to
remove the annotation), and **fixed the one real defect the red tests were pointing at**:
`event_subject_id` derived a delegation's subject from `delegation.issuer` — the immediate signer —
which is precisely the B19 hazard this stream exists to avoid. `big_repo/keyhive.rs` now calls
`SignedSubjectId::subject_id()`, which walks the proof chain to its head, matching Keyhive's own
dispatch. That single change turned the three remaining failures green, because the signer id is not
a membered graph and every entry for such a delegation was being dropped.

Verified by me: `nextest -p big_repo -E 'test(keyhive_access_stream)'` 11/11; `clippy -p big_repo
--all-targets --all-features` exit 0 with no warnings; 17/17 on the `keyhive_admission`/
`agents_for_membered`/`group_part` filters (the closure helper's signature changed to
`BTreeMap<Identifier, Access>`); `clippy -p big_sync_core` clean and 86/86; `cargo check -p
daybook_core --all-features` exit 0. The negative check for the subject fix was observed rather than
contrived: with `delegation.issuer` restored, `delegation_subject_is_the_proof_chain_root_not_the_signer`
fails with `derived == signer`, and the two entry-count tests fail with zero entries.

Deliberately left for later, recorded not done: `archived_through` stays `#[cfg(test)]`-gated because
un-gating it now would create a `pub(crate)` fn with no caller (a dead-code warning); its consumer is
lane 2's boot-time §9 recovery. `note_retention` takes the concrete `SqliteDeltaWalkerStateRepo`
because `retention_reader_id()` is an inherent method on it and it is the only state repo in-tree;
if a second backend appears, that moves to the trait.

### Lane 2a status: DONE (uncommitted)

`src/daybook_core/blobs/permission_writer.rs` (728 lines, declared from `blobs.rs`) is the consumer
machine: `spawn_blob_inventory_permission_writer` (its own durable identity
`@daybook/core/blob-inventory-permissions` + consumer `keyhive-access`, one supervisor task ending in
`.unwrap()`), `run_permission_machine` (registers retention at the durable progress *before* opening
the reader, `All` selector, `ConcurrentDeltaWalker::open` keyed by subject, keyed task budget, cancel
token), `on_delta` (the no-op path acks, the newest cursor wins, `tasks.replace`),
`run_permission_task` (`Identifier::to_bytes()` -> `PeerKey`, `set_part_members` as a full
replacement) and `on_task_completion` (a cursor moves only for a task that reported `Written`).

Verified by me: 3/3 in-crate tests (`nextest -p daybook_core -E 'test(permission_writer)'`), clippy
clean on `daybook_core` both `--all-targets --all-features` and lib-only. The tests are the ones that
matter for the 7,785 denials: an inventory document's part carries that document's closure and a
grant on one inventory document does not reach the other's part; a smaller second closure leaves no
stale member; and the serving predicate now refuses a stranger while admitting a closure member
(`read_denied`).

The worker was killed by the 30-minute limit before its report (third time in this work). I finished
it: two compile errors (`Send` — the boxed repo teardown is not `Send`, so it is now resolved into a
future before the async block that captures it; and a missing `M` annotation on the stream in the
test). The third test then failed with `the walker never settled revision 14; it is stuck at 0`, which
was **not** a machine bug: `ensure_sqlite_ctx` opens one sqlite file per local-state id, so the
harness's test-local namespace was reading a different database from the machine's. The harness now
observes the machine's own `(namespace, consumer_id)` identity, which is what makes a durable-progress
assertion meaningful at all.

Not in 2a, recorded for 2b: the machine has **no production caller** yet (the pin-worker host wiring
and the unconditional boot seed from Keyhive), so nothing in a real boot writes the derived
blob-inventory parts yet — the denials are cleared at the machine level, not in a running app. Also
2b: `archived_through` un-gating (its consumer is the boot-time §9 recovery) and the e2e/stress
assertion.

### Lane 2b status: steps 1-2 done (uncommitted); steps 3-4 open

**Step 1, the unconditional boot seed** is in `spawn_blob_inventory_permission_writer`: before the
reader can open (ADR 013 §8), every derived part is written from the current Keyhive closure via
`seed_inventory_parts` -> `set_part_members`. The `Identifier` -> `PeerKey` hop was extracted into
`peer_access_map` and is now shared with `run_permission_task`, so there is one conversion, not two.
Test `the_boot_seed_writes_a_fresh_part_from_keyhive`: on a store nothing has written to, the seed
alone populates the part with exactly the document's closure. Negative check by me: with the seed's
loop forced to zero iterations, that test fails with `left: {}` against the real closure.

**Step 2, the host wiring**: `Rt::boot` spawns the writer beside `spawn_blob_pin_worker` (blob part
store, local state repo, big repo, the two inventory documents, a child cancel token), `RtStopToken`
gained `blob_inventory_permission_stop`, and it is stopped *before* the blob-pin machines because
shutdown order is the reverse of construction. `crate::blobs` re-exports the entry point the way the
pin workers are re-exported, and the `dead_code` allowance it needed while it had no caller is gone.
Verified: `clippy -p daybook_core --all-targets --all-features` exit 0 and 4/4 on
`nextest -p daybook_core -E 'test(permission_writer)'`.

**Step 3 is done, and the design question resolved itself once the walker's contract was read.** §9's
recovery signal is `archived_through` (`store/sqlite/events.rs:243-251`, `#[cfg(test)]` un-gated for
this), exposed on the stream as `KeyhiveAccessRevisionStore::archived_through`. What decided it is
`ConcurrentDeltaWalker::open`'s own doc comment (`concurrent_delta_walker.rs:88-96`): *"The reader was
opened by the caller at the state repo's durable progress … so the caller's `after` and the progress
read here agree."* Opening at the floor while the state repo sits below it breaks that, which is why the
recovery is a progress **advance** (`SqliteDeltaWalkerStateRepo`'s transaction `advance_from`, a
compare-and-swap on `delta_walker_progress` that rejects a stale expected value) and only then an open
at the floor — the invariant holds by construction and the first readable entry is contiguous with the
walker's durable prefix, so the watermark machine never sees an unanchored gap. That is sound here for
the reason the whole design rests on: the parts are a function of *current* Keyhive state, so a
discarded wake-up range cannot leave the rows wrong. Test
`a_cursor_below_the_archive_floor_resumes_at_the_floor` prunes the admitted log out from under a
consumer that has never run, and asserts it settles at the floor with the seed holding the rows.
Negative check by me: with the advance disabled the test fails `the walker never settled revision 4;
it is stuck at 0` — which is the pre-fix behaviour ADR 013 §9 predicted.

**Step 4 is done** (worker `c33c5242`): `a_boot_makes_the_inventory_parts_read_by_their_document_closure`
boots a real app context, takes both inventory documents from the booted runtime's own ids (nothing
hardcoded) and asserts each derived part holds exactly that document's closure and answers `read_denied`
accordingly — closure member admitted, stranger refused. Verified by me: clippy exit 0 and 6/6 on
`test(permission_writer)`. The worker's honest limit, which I accept: on an unpruned log the machine's
own replay also writes those rows, so this test pins the user-facing property but does not isolate the
seed from replay — the seed's *distinct* observable effect is the pruned boot, and that is now step 3's
test.

**Two readings that corrected the backlog while landing this:**

- **B14 is a diagnostic lie, not a scheduler bug.** The receipt is logged and then ignored:
  `backend.rs:268-281` reads `receipt.outcome` only for the `debug!` at `:270` and derives the lane's
  outcome from `doc_payload_heads` (`Noop` when the heads did not move). So a live doc whose
  `blocked_refs` are non-empty reporting `Ready` (while the same worker's `reconcile_causal_coverage`
  treats that set as "blocked content" and bails, `doc_worker.rs:1384`) changes no lane behaviour. It is
  still worth fixing because we are hunting with exactly these logs, but it is not the tier10_4 cause.
- **The retention floor does advance.** `on_task_completion` already calls `stream.note_retention(state,
  through)` on every `DeltaAck::Accepted { through: Some(..) }`, so the admission-log floor tracks
  settlement instead of pinning at the boot cursor; it only lags when the machine stalls, which is the
  documented cost in §9. Registering at boot and advancing on settlement is therefore the whole
  retention story, and the `MAX(seq, excluded.seq)` upsert is what makes the re-registration monotone.
  (Verified by reading the ack path — no change was needed.)

## G. Defect batch landed after the delta-stream lanes (B13, B19, B14)

Each of these was implemented by a fresh worker against a hoisted brief (`/tmp/b13-brief.md`,
`/tmp/b19-brief.md`) on the rule that only one writer touches the working copy at a time, and each was
verified by the operator before being recorded here: read the hunk, re-run clippy and the narrow filters
myself, and check the worker's own negative check was real.

**B13 — AFW re-checks `is_broken()` before it writes (fixed).** The publish path checked the bundle once
before the heads read, the payload build *and* an `await` on `set_obj_payload`, so eviction — which
`mark_broken` (`runtime2/types.rs:233`, an `AtomicBool` store plus `notify_waiters`, taking no lock) is
allowed to win — could still land in that window and the payload would advertise a bundle that cannot be
served. It now re-checks immediately before the write with no `await` between the check and the write, and
defers (`Deferred`, not an error, for the reason the surrounding comment already gives). The worker
verified the placement question against the code rather than assuming: because the break path takes no
lock, sitting inside the heads `lock_scope` would buy no ordering. Clippy exit 0 with zero code warnings;
5/5 on `test(automerge_frontier_worker)` and 4/4 on the tier9/contract/payload filters, re-run by me.

*No test, and the reason is sound rather than lazy:* there is no suspension point between the two checks
(synchronous lock read, synchronous payload build), so any test can only break the bundle before the
*first* check — where the pre-existing check catches it — and would therefore pass with the fix removed.
The worker proposed the seam it would need (a `test-support` callback between read and write, or hoisting
read-then-re-check into a callable helper) and stopped there instead of inventing one.

*Deliberately out of scope, per the worker's report and my agreement:* a break *during* the awaited
`set_obj_payload` is still unsuppressed. Closing it needs a post-write re-check plus compensating part
removal, and the argument for not needing it is that re-acquisition re-materializes and re-publishes via
the keyed replacement publish that overwrites stale heads. If that argument is ever doubted, it is a
separate item.

**B19 — `group_part_worker` names the delegation's subject (fixed).** `affected_event` derived both the
group part and the `document_ids_containing_group` argument from `delegation.issuer` — the *immediate
signer* — so a re-delegation signed by a non-root member inserted a part for a member id and asked about
that member, while the group's own part went stale for the event. Both arms now resolve the subject through
`BigKeyhiveHandle::event_subject_id`, the repo's settled reading (the same function the delta stream uses
for every admitted event), rather than deriving a second way. The subtlety that explains the original bug
is worth keeping: the value in hand is a `StaticEvent` whose proof is only *digests*, so the subject is not
derivable from the payload — it must be resolved through the hive's graph, and `issuer` was the cheap
shortcut that skipped the walk. Test
`runtime2::group_part_worker::tests::delegation_group_part_names_the_proof_chain_root_not_the_signer`
builds a re-delegation and first asserts `signer != subject`, so it cannot pass vacuously, then asserts the
part is the subject's. Negative check observed: with both arms back on `issuer` it fails
(`left: {signer's part}` vs `right: {subject's part}`). Clippy exit 0, zero warnings, 15/15 on
`test(group_part)`, re-run by me. Cost, measured by the worker with a temporary probe rather than
guessed: ~6 µs marginal per Delegated/Revoked row (46.8 µs vs 40.8 µs per call, debug build, 20k
iterations), pure in-memory graph lookups inside the spawned Decode task, not on the machine loop.

Two things the worker reported instead of deciding, with my decisions:

- *`event_subject_id` can now propagate an error into a path that could not fail before, and
  `on_task_completion` panics on a task error (`group_part_worker.rs:331`).* **Keep the panic.** This is
  not a new failure class: the delta stream already resolves *every* admitted event through the same
  function with `?` (`big_repo/runtime2/keyhive_access_stream.rs:314`), and admission rows exist only for
  hashes `KeyhiveIncorporationSink::admit` saw as newly admitted — i.e. after the local projection
  incorporated the event and its proof links. A resolution failure is therefore an invariant violation,
  and this repo's convention is that invariant violations crash rather than get papered over. Residual
  risk to keep in mind: if Keyhive can ever drop a non-member delegation from the graph such that a
  previously admitted event stops resolving, this becomes crash-on-stale-graph; the fix then is to
  resolve at admission time and carry the subject in the admission row, which is an interface change.
- *No `Revoked` test.* Accepted as a residual gap: the two arms are symmetric and share the resolution
  call, and the worker could not find or build a non-root re-signed revocation fixture within budget. It
  reported that rather than writing a variant that would pass either way, which is the right call. Worth
  a fixture when someone next touches revocations.

**B14 — refined, still open, and deliberately not fixed by guess.** The defect is real: with
`let walk = pending;`, a *live* doc with non-empty `blocked_refs` reports `SyncDocOutcome::Ready` while the
same set on a cold doc reports `Pending(blockers)` (`doc_worker.rs:1657,1695-1706`), and the same worker
refuses to run `reconcile_causal_coverage` while blocked (`:1384`). But two readings changed the picture:

1. Its impact today is *diagnostic, not behavioural*: the receipt is logged and then ignored —
   `backend.rs:270` reads `receipt.outcome` only for the `debug!`, and the lane's outcome comes from
   `doc_payload_heads` (`Noop` when the heads did not move). So fixing it changes no scheduling. It still
   matters because we are hunting with exactly those logs.
2. The obvious mechanical fix is *not honest*: the live path holds `blocked_refs` as
   `HashSet<(BigRepoCiphertextKind, CommitId)>`, which carries no reason tag, so any
   `Vec<MaterializationBlocker>` it reports would mislabel (it cannot tell
   `MissingDocumentKeys` from `MissingAutomergeDependencies`). Two honest options, needing a decision:
   **(a)** tag `blocked_refs` at insertion with the reason so a live doc can report real blockers, or
   **(b)** keep the outcome but carry the blocked count/refs on `SyncDocReceipt` so the *log* tells the
   truth without claiming a precise blocker. Both are interface changes, so neither was made unilaterally.
