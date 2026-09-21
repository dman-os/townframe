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
| B20 | `big_sync_core` | A `Sync` completion was **dropped** whenever a removal's membership lane sat at the same cursor as the object's in-flight content replay: the caller's gate (`owes_obj_sync_completion`, `cursor.rs:143-176` in the working copy; the parent spelled it `owes_obj_job_lane(.., Sync)`) answered false for that cursor, `handle_sync_completed` `continue`d, and `on_obj_sync_job_evt(.., Sync)` never ran — so `acknowledged` did not move and `in_flight` stayed set (only `abandon_obj_sync` clears it), stranding the position the object route resumes from, while `cursor.rs` states a sync completion is exactly the acknowledgement that advances it. | FIXED: the acknowledgement is no longer entangled with the board's settlement. `CursorSyncMachine::on_obj_sync_job_evt` releases the object's claim for every `Sync` completion, then settles the board only where `JobBoard::owes_lane` says that lane is owed (an object-target replay registers no part job at all; a shared cursor's membership lane stays owed and keeps gating its part), and `handle_sync_completed` (`lib.rs:2738-2749`) no longer guards before calling. `owes_obj_sync_completion` deleted (no references left). Test `a_sync_completion_acknowledges_the_object_beside_a_membership_lane` (`cursor.rs`) drives an object-only `Changed` and a `Removed` at one cursor and asserts the route resumes from 7 while the membership lane is still owed. | med | done — verified by me: `cargo clippy -p big_sync_core --all-targets --all-features` exit 0, zero warnings; `cargo nextest run -p big_sync_core -E 'test(cursor) \| test(sync)' --retries 0` 32/32 pass; extra narrow run `-E 'test(removal) \| test(readd) \| test(object) \| test(completion)'` 28/28 pass, which is what covers the `handle_sync_completed`/`handle_remove_completed` call sites (the required filter alone skips them); negative check: reinstating the drop (early return in the `Sync` branch when a Membership lane is owed at that cursor) fails the new test on `left: 0, right: 7` — "the acknowledged replay is what the object route resumes from" — then the fixed file was restored byte-identical (sha256 match) and re-run green |

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
`TIMEOUT 240.033s`; neighbours pass. Status: **open, and its shape changed on 2026-09-18** —
the concurrency question now dominates; see the re-measurement block immediately below.

### C re-measurement (2026-09-18, on the post-batch working copy)

Two measurements that change what this section is about:

1. **The test passes in isolation.** `cargo nextest run -p big_repo -E
   'test(long_test_big_repo_tier10_stress_4_editor_converges)'` → `PASS [29.141s]` (log
   `/tmp/tier10-alone.log`). The previous revision of this section recorded it timing out at 240s on
   the first isolated attempt, so either that attempt predated the H-A/H-B/H-C/B2 work now in the
   tree, or the hang is intermittent. Treat "reproduces on demand" as unproven until it is shown
   again at this revision.
2. **The hang signature is absent from the current full-suite log.** In `/tmp/flake.log` (a 764-test
   parallel run, 589s) the whole file contains **zero** occurrences of `converged=false`, `mismatch=`,
   any `phase3` marker, `stress final cluster settle complete`, and `stress cluster alignment
   complete`. The `3_editor_1_relay` variant (stdout block 345459-449606) *did* progress: `phase1
   mutations complete` 42.19s, `phase1 post-mutations settled` 56.26s, `phase2 connect complete`
   88.30s, `phase2 post-connect settled` 126.67s, `phase2 mutations complete` 216.22s. So it was killed
   at 240s inside phase3/alignment, before the barrier's convergence poll ever ran. (An earlier
   revision of this block read the absent `phase3` marker as "killed before the mutations"; that was
   wrong — the markers are phase1/phase2 and they are present.)
   unbounded `wait_for_quiescence_freeze(None)` this section blames — that run never reached the
   no-carrier stall either. But do not read the timeouts as "the tests are merely slow": the same
   block re-syncs **30 objects 700 times** while `phase2` grinds (below), which is what keeps phase3
   from settling.

So the failure set of that run decomposes as: one real deterministic failure
(`test2::topologies::tier3_line_private_reader_keyhive_propagates_through_relay`, reproducible alone in
1.9s, `Unknown agent` — the race class AGENTS.md names) and four **non-settlement** failures that must
not be filed as scheduling. Measured inside that log's two tier10 blocks (lines 345459-449606): 30
distinct `obj_id`s, **700 `spawn sync task`, 395 `big sync object task completed`, 63 failures**, spread
from 13s to 226s across all four peers — up to 51 spawns and 28 completions for a single object, i.e.
roughly two spawns per completion, sustained for 200 seconds. A sync task that *completes and reports
`ObjectSynced`* therefore does not settle the object it synced: the machine re-arms and re-syncs the
same 30 objects until the kill. `sparse_dirt` shows the same shape (`saw 19997 objects` while sync
tasks keep completing at 214s). One sub-signature is already visible: two tasks for the *same*
`obj_id`+peer spawned 95ms apart (editor-2, task 478 then 487 at 70.52s/70.61s) with only 487
completing — the skill's "multiple concurrent tasks for one object" note, where a completion that does
not settle the claim leaves the machine re-arming. This is the live hunt; the `tier3` fix (retry
versus panic) is a separate, already-assigned item.

Attribution note: `/tmp/flake.log` is a symlink to `/run/media/asdf/p3N/tmp/hunt-logs/flake1.log`, so
there is only one full-suite log and pre/post-batch log comparison is impossible; attribution was
done from `jj diff -r d7284beffd73..@`, which touches in these paths only the two H-D width
conversions (`keyhive_doc_id`, `group_part_worker.rs:626`) — neither is on the `Unknown agent` path.

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

**Step 2, the host wiring**: `IrohSyncRepo::boot` spawns the writer (blob part store, local state
repo, big repo, the two inventory documents, a child of the repo's cancel token) and
`IrohSyncRepoStopToken` gained `blob_inventory_permission_stop`, stopped after the blob worker and the
RPC server that serve those parts. It used to live in `Rt::boot`; that was wrong for the same reason
the parts are wrong without it — the clone path and any headless sync boot the serving boundary
without an `Rt`, so the parts were served with no access rows at all. A peer then sees them as unknown
parts and `wait_for_full_sync` never resolves (this is what timed out `cli_clone_and_wait_until_synced_smoke`
4/4 and `long_test_iroh_clone_sync_batch_100_docs_with_blobs` in CI). An `Rt` does not serve parts, so
it does not own the writer. `crate::blobs` re-exports the entry point the way the
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

## H. External reviewer batch — triage (35 claims, ~25 files)

An external LLM reviewer produced 35 findings. They were triaged **read-only** by four reviewers (workflow
`4f63e4b8`), one brief per file group (`/tmp/triage-{a,b,c,d}.md`), each required to return per claim:
verdict, `file:line` evidence with the call path that makes it reachable, one-sentence root cause, whether it is
introduced by this working copy, and the smallest correct fix — with **no implementation**. The verdicts below
are theirs; **each fix still gets verified at the site by the operator before it lands**, and the two claims the
operator had already formed an opinion on (A6, C12) are noted with the disagreement.

The reviewer's own framing was adversarial and it matters: 13 of the 35 claims were refuted by the code, and the
refutations are recorded below so they are not re-litigated. Several refutations rest on the same fact —
`ObjKey`/`PeerKey` are variable-length *by design* (ADR 012 decision 1) while their consumers assume 32-byte
Keyhive identities, which `ids.rs:136-148` documents as an invariant ("Every key that reaches them is a 32-byte
digest by construction") that is *asserted at the point of use* rather than carried by the type. Where a
non-32-byte value is genuinely reachable (A5, C1-C3) that is a real defect; where it can only be reached by
violating the documented contract from local code (B4-B11), calling it a defect would be reopening that
invariant, and the reviewer said so.

### H.1 Valid and ours — to fix

| ID | Site | Defect | Fix | Batch |
|----|------|--------|-----|-------|
| A3 | `big_sync_core/cursor.rs:143` (+ guard at `lib.rs:2674-2686`) | An object-target replay is tracked only in `object_replays`, but the B1 `owes_obj_job_lane` guard consults only the part JobBoard, so the completion is dropped: `acknowledged`/`in_flight` never move, the route stays at cursor 0. **This is the stall-shaped one.** | Let an object replay's own claim (`in_flight == cursor`) satisfy the guard when no part job owes that cursor. | H-A |
| A4 | `big_sync_core/cursor.rs:300-312` | `object_replays.get_mut(&obj_id)` never creates the entry, so a part-scoped `Sync` completion cannot record its cursor; the route later reads 0 and re-reads the object's first page (the reviewer notes the harm today is a redundant re-read, not the stall — A3 is the stall). | Seed the entry on a `Sync` completion (`entry().or_default()`). | H-A |
| B2 | `store/sqlite/sedimentree.rs:633` | `leaving` is derived from the document's *occupied* parts, not the reconciler's managed set, and the delete is part-wide (`:640`), so a document materialized into a foreign part deletes that part's access rows while its membership stays. | Derive `leaving` from `stale` (`stale.contains(&part_id)`). | H-B |
| B3 | `store/sqlite/sedimentree.rs:635-646` | When `agents.is_none()` and not leaving, the branch still runs a part-wide `DELETE ... WHERE part_ref = ?` with no re-insert, so a document leaving `/seds` deletes *every* principal's `/seds` row — the embedder's mirror grant. | Skip the delete when `agents.is_none()`; the part-level access reconciler owns those rows. | H-B |
| C1 | `daybook_core/blobs.rs:479-494` | Path built from `blob_hash_from_id` (Display) unencoded; a peer-authored facet digest with a reserved `/…` spelling makes `Path::join` replace `BlobsRepo::root` — a path escape. | One fix covers C1-C3: keep `BlobId` a distinct 32-byte-digest type (or validate at the parse boundary). | H-C |
| C2 | `daybook_core/blobs.rs:750-756` | `blob_id_to_iroh_hash`/`blob_id_to_digest_str` call the panicking `to_bytes32()`, reachable from `sync.rs:61` for any parsed non-32-byte key. | Same typed-`BlobId` fix; reject non-32-byte ids at the blob boundary. | H-C |
| C3 | `daybook_core/blobs.rs:751` / `ids.rs:209-224` | Our own writers emit the canonical 34-byte multihash form, but `BlobId::from_str` decodes multibase only, so `pin_worker.rs:230` accepts it as a 34-byte key and blob sync panics. | Decode multihash digests to the 32-byte id; reject every other length before the key enters the inventory. | H-C |
| A5 | `big_sync_core/ids.rs:144-147` | The fixed-width conversion panics, and the sync path's `obj_id` can come from a peer-delivered event or a parsed key, aborting the process instead of dropping the object. | Make the conversion fallible at these edges and answer the sync with an error for a non-32-byte key. | H-D |
| A1 | `big_sync/part_store/memory.rs:184-189` | Materialization mutates membership rows only; this store's object route reads the keyed frontier, so a later explicit share is invisible where sqlite (which records at the current revision) re-tells the peer. | Record the derived-part touch as a frontier mutation at the stamping revision — note `MemoryKeyedFrontierTable::apply_at` rejects a non-advancing revision, so this needs the operator's call against the "subscribing allocates no revision" invariant. | H-E |
| A2 | `big_sync/part_store.rs:397-401`, `memory.rs:405-412` | The object route's authorization runs *before* the `subscribe` that would materialize the derived part, and candidates resolve from `objs[obj].parts`, so a peer holding only a share on `o:{obj}` is `Unauthorized` forever (and is re-asked forever by that arm). | Add `obj_id.object_part_key()` to the `FromObject` candidates in both stores. | H-E |
| A7 | `big_sync/rpc.rs:14` | ALPN is still `townframe/big-sync/0` while this change set altered the wire enum it gates (`SubscriptionTarget::Object` gained `cursor`), so a stale peer completes `/0` and then fails mid-protocol. | Bump the ALPN version. | H-F |
| A8 | `big_sync/rpc.rs:346-350` | The permit is acquired *inside* the spawned task while dispatch spawns unconditionally, so `MAX_INFLIGHT_RPC_HANDLERS` bounds running handlers, not the waiters — a peer can pile up unbounded parked tasks. | Acquire the permit before spawning, or use a bounded pending-request queue. | H-F |
| A9 | `big_sync/worker.rs:116-121` | `wait_for_idle`'s predicate omits `task_counts.live` and `active_machine_tasks`, so it can report idle while an in-flight machine task still owes the events it will feed the cursor machine. | Use the scheduler's idle predicate plus `active_machine_tasks == 0`. **Not the `tier10_4` cause**: that path settles via `wait_for_quiescence`/`assert_cluster_alignment`, so this does not explain the timeout. | H-F |
| B12 | `test2/harness/topo.rs:255,728-739,624-647` | `Pair::boot`/`boot_persistent` register `/seds` routes without authorizing either peer, while only `boot_without_keyhive_notifs` grants both directions first; the denial backs off rather than retrying, so the tests still pass over document parts and silently lose `/seds` replication coverage. **This is the tracker's existing B16.** | Call `allow_part_pull(remote, &[global_part_id()])` both ways in `boot`/`boot_persistent` before connect. | H-G |
| B13 | `test2/harness/topo.rs:373-390` | Same root cause at the connection seam: route registration (`set_peer_parts`) and per-part authorization (`allow_part_pull` → `add_part_member`) are separate steps with no ordering rule. | Grant each subscribed part inside `connect_with_keyhive_notifications` before `set_peer_parts`. | H-G |
| C11 | `docs/adrs/012-...:400` | Object-partness is a bare byte-prefix test (`ids.rs:93-97`) over keys that may be arbitrary bytes (hashed part keys), so ≈2⁻¹⁶ of them begin `o:` and would resolve through the object-part authorization path — contrary to the ADR's own "it cannot collide" claim. | Make object-partness decidable without a prefix test (stored kind / membership), or record the collision probability as accepted. | H-H |
| C4 | `docs/adrs/010-...:455` | The slot schema has no writer sequence or observed-lane frontier, so the causal merge the prose asserts has no representation (`011:306-309` has the fields ADR 010 lacks). | Add `writer_seq`/`observed`, or define sibling supersession via source causality the adapter holds. | H-H |
| C6 | `docs/adrs/010-...:498-508` | ADR 010 redeclares ADR 011's `TaskDeclarationV1` with a narrower field set while demanding byte-equal canonicalization, so two adopters cannot canonicalize the same `TaskId`. | Reuse ADR 011's declaration verbatim, or define one producer-independent canonical declaration (the reviewer notes "required" is the wrong word for the two optional timestamps). | H-H |
| C9 | `docs/adrs/011-...:463` | `retain_until` and `not_after` are both optional, so a producer can set a retention horizon with no deadline, permitting an old replica to reintroduce and execute a pending ticket after terminal evidence is discarded — the very invariant `011:470` states. | Pin `not_after` whenever a retention horizon is set, at the producer boundary. | H-H |
| C12 | `docs/bigsync-reconciliation-open-items.md:167-169` | The bullet tells future permission-writer changes to filter to readers, contradicting the later correction (`:226-233`) and the shipped behaviour (mirror the closure verbatim). | Replace it with the final rule: copy the document closure, `Relay` included; point at ADR 013 §4. | H-H |
| D4 | `x/task-coordination-demo.ts:389` | The demo publishes the router heartbeat exactly once, modelling the ADR's *periodic* lossy discovery channel as a fire-once broadcast — and "no periodic republication" is not among the simplifications its header declares. | Re-publish the heartbeat each step. | H-I |
| D5 | `x/task-coordination-demo.ts:404` | The router's only memory of an allocation is the registration snapshot taken at discovery time, so a second `route()` for an in-flight task passes the `activeAttempts` guard and calls `dispatch.start` again. | Give the router live allocation memory (record the accepted id in `route`, or an `AttemptChanged` update back from `accept`). | H-I |

### H.2 Refuted by the code — recorded so they are not re-litigated

| ID | Claim | Why it is not a defect |
|----|-------|------------------------|
| B1 | `publish` leaks unreadable part ids | Every delivered id came from `bus.by_part`, which is populated only from the subscriber's own requested targets (`sedimentree.rs:157-163`), so the list is always ⊆ requested; object subscribers get an empty list. Delivery-then-denial is the revocation-settling path by design (`sqlite.rs:634-644`), and filtering would be exactly the omission shape tracker decision B6 rejects. **Conflicts with the decision; not a distinct gap.** |
| B4 | `inspect_stored_doc_blobs` panics on a non-32-byte `DocumentId` | A test-support inspection API; every constructor feeds 32 bytes; local only; non-32-byte is an invariant break by contract, not an input shape. |
| B5 | `keyhive_doc_id` panics instead of using its `Res` | The `Err` its callers consume is "32 bytes but not a curve point"; the length panic precedes it. Widening the non-document classifier to key shapes is an ids/ADR decision, not a panic to convert. |
| B6, B7, B8, B9, B10, B11 | `to_bytes32()` panics in `NativeBigRepoIo`, `mod.rs::connect/close`, `open_connection_iroh`, `OpenConn`, `register_peer`, `backend.rs` error paths | In each case the reviewer established the identity is 32 bytes wherever it is minted (handshake-authenticated peer ids, local `Runtime2Cmd`s whose payload is a `Box<dyn Any>` and cannot cross the wire, local contact-card identities), i.e. not remote-reachable. |
| A6 | Retention-reader id aliases two walkers when a component contains `/` | The `format!("{ns}/{consumer}")` mapping is indeed non-injective, but every call site passes literal constants and no namespace is another namespace plus `/` plus a suffix, so no two pairs can currently collide. Nothing to fix now; if a caller ever embeds a key in a component, length-prefix the namespace. *(The operator had called this real on inspection — the reviewer's call-site sweep is the better evidence and refutes reachability.)* |
| D1, D2, D3, D6 | Demo overwrites per-writer terminal facts; `accept` passes the router's mutable ticket; split-brain claims allocate twice; all nodes share one `TriageDomain` | `x/task-coordination-demo.ts` is an unreferenced, unbuilt Deno "executable thought experiment" (its own header) outside every check/build pipeline; each of these is either a declared boundary stand-in or the ADR's accepted behaviour (011:331 explicitly allows duplicate allocation across partitions). D1's requested signed-sequence machinery is ADR 011 design territory. |

### H.3 Decisions needed (spec questions, not fixes)

- **C5** ADR 010's bounded settlement set has no retention/compaction rule, so "stale-task rejection after
  pruning" cannot be judged — ADR 010 already carries this as its open question 1 (`:711`). Decide the settlement
  representation first.
- **C7** ADR 011's router slot is grow-only and the document *asserts* bounded lanes while its own open question 5
  (`:603`) asks what those bounds are. Decide lane expiry/compaction, or restate "bounded" as bounded-by-current
  candidate set.
- **C8** `UNCLEAR` rather than valid: nothing in ADR 011 states that an empty slot blocks takeover, so "the
  election cannot start" is an inference. Evidence needed is a statement that step 1 presupposes an existing
  claim; if it does, the document needs an empty-slot bootstrap rule.
- **C10** Where a task-authoritative terminal record lives when `archive_part` is absent is ADR 011's open
  question 9 (`:607`) — the reviewer's loss/resurrection is the known consequence of that open decision.
- **B14** (from §G) still needs the operator's choice between tagging `blocked_refs` at insertion and carrying the
  blocked count on `SyncDocReceipt`.

### H.4 Fix batches (one writer at a time, each verified by the operator)

- **H-A** object-route cursor machine (A3, A4) — `big_sync_core/cursor.rs` (+ `lib.rs` guard): the stall-shaped
  pair, and the tests from the earlier object-path lane are the place to pin them.
- **H-B** access-row reconciliation (B2, B3) — `store/sqlite/sedimentree.rs`: revoking rows a part-level owner
  owns is the denial class, so these come before any cosmetic work.
- **H-C** blob identity (C1-C3) — one typed-`BlobId` fix covering path escape, panics and the multihash mismatch.
- **H-D** fallible fixed-width conversion at the sync edge (A5) — smallest first step of the larger key-width
  question; the type-level answer (carry the invariant in the type) is a broader refactor to decide separately.
- **H-E** part-store materialization/authorization asymmetry (A1, A2) — memory vs sqlite, and the ordering of
  authorization before materialization.
- **H-F** RPC hygiene (A7 ALPN bump, A8 permit-before-spawn, A9 idle predicate).
- **H-G** harness `/seds` authorization (B12, B13) — the existing tracker B16, now with the exact constructors.
- **H-H** documentation truth (C11 as-built vs claim, C4, C6, C9, C12).
- **H-I** demo fidelity (D4, D5) — demo-only, lowest priority.

### H.5 Batch status

**H-A — object-route cursor settlement (A3, A4): DONE, verified by the operator.** Two files:
`big_sync_core/cursor.rs` (new `owes_obj_sync_completion`, and `on_obj_sync_job_evt` now *seeds* the
`object_replays` entry with `entry().or_default()` instead of `get_mut`) and `big_sync_core/lib.rs`
(`handle_sync_completed`'s per-cursor guard calls the new predicate; the refusal path, its message and its
`continue` are untouched, so the B1 fix stands). New tests:
`an_object_only_replay_completion_advances_the_route_it_acknowledged` (asserts the *observable* route target,
`Object { cursor: 7 }`) and `a_part_scoped_sync_records_the_object_cursor_of_an_object_with_no_replay_yet`.

Operator verification, on my own runs: clippy exit 0 with zero warnings on both crates; `big_sync_core` 88/88;
`big_sync` 91 passed (the 1 skip is pre-existing, present before this change). **Both negative checks reproduced
independently**: reverting the guard to `owes_obj_job_lane` fails A3's test with
`left: Some(Object { …, cursor: 0 })` vs `right: … cursor: 7` (exactly the worker's reported text), and removing
the A4 seed fails its test with `left: []` against the expected `SetPartCursor { cursor: 5 }`/`PartIdle` pair.
Both probes were reverted through tracked edits and the suite re-run green.

**A subtlety the worker found and reported instead of papering over:** the naive form of A3 ("an object replay's
claim satisfies the guard") would panic the JobBoard, because a `Membership`-only waiter can sit at the same
cursor as an object-only `Sync` claim and the board's `settle` panics on a lane its waiter does not hold. The
predicate therefore lets the claim stand in only when the board tracks *nothing* for that cursor
(`owes_obj_sync_completion`, `cursor.rs:163-171`), which is the tracker's own wording. Reachability of the panic
is low — that combination also cancels the object-only sync worker and abandons the claim — so this is insurance
against a process-fatal panic rather than a behaviour change. Worth keeping in mind when reading that predicate:
it deliberately asks the board twice.

### H.6 A deterministic contract failure in the `big_repo` store's object route

Found because the H-B worker reported a red test instead of working around it. It is **not** H-B's and not H-A's:

- `cargo nextest run -p big_repo -E 'test(sqlite_big_repo_host_part_store_contract)'` fails deterministically at
  `big_sync/part_store.rs:1273` — `an object route with buffered events produced no page`.
- **H-A exonerated by me**: reverting H-A's guard to `owes_obj_job_lane` reproduces the failure byte-identically
  (the cursor machine is not on this path at all — the contract drives `HostPartStore::replay_page` directly).
  **H-B exonerated by its own revert** (and the harness never calls `reconcile_group_part_batch`).
- **It is `SqliteBigRepoStore`-specific**: the *same* contract suite passes for the plain `SqlitePartStore` and for
  the memory store — `memory_host_part_store_contract` (`big_sync/part_store/memory.rs:1695`) and
  `sqlite_host_part_store_contract` (`big_sync/part_store/sqlite.rs:2269`) are both inside the 91-test `big_sync`
  run that is green. Only `SqliteBigRepoHarness` (`big_repo/store/sqlite/tests.rs:10-25`, a bare
  `SqliteBigRepoStore::new(sql, "…", BuckId::MAX_LEVEL)`) fails.

Instrumented evidence (mine, temporary and removed): the failing call is the **second** page, resuming from
`cursor=184`; and a page from `cursor=0` with limit 8 returns exactly **one** event —

```
Changed(ObjChanged { cursor: 184, part_ids: [], payload: {"idx": 1, "tag": "object-route"} })
```

The harness's `seed_live_obj` writes a payload (`idx 0`) *and then* adds the object to the part, so that first
change should be visible on the object route; it is not. A 3-second hold on the resume page returns empty, so this
is not a hold-too-short race. The object route in `SqliteBigRepoStore` therefore omits (or coalesces away) the
change that was written before the object's part membership existed, where the plain store keeps it.

**Verification gap, owned:** when the object-path lane landed, I verified `big_sync` + `big_sync_core` (170/170) and
only `cargo check`ed `big_repo` — so this assertion's `big_repo` instantiation was never *run*. A shared contract
suite has to be run at **every** instantiation, not just the crate that owns it. That is why this slipped, and it is
the reason the H-B worker's report was worth taking seriously rather than filing as "unrelated".

### H.7 H.6 fixed — the object row was stamped by every payload write, and the object route read only that row

**Root cause (two halves of one bug, both in `big_repo/store/sqlite/sedimentree.rs`):**

1. `set_obj_payload_in_tx` upserted the object-level member row `(obj_ref, maybe_part_ref = 0)` on *every* payload
   write, overwriting its `txid`. Instrumented: `set_obj_payload(idx 0)` → `(obj,0)@182`; `add_obj_to_parts` →
   `(obj,part)@183`; then `set_obj_payload(idx 1)` re-stamped **both** rows to `184`, so the first change's position
   ceased to exist anywhere. The plain store stamps that row only when the object has no live parts; the memory
   store gates its object lane the same way.
2. `replay_candidates`' object arm filtered `m.maybe_part_ref = 0 AND m.obj_ref IN (…)`, i.e. it read only that one
   (now overwritten) row. The plain store, the memory store, *and this store's own* `open_local_revision_reader`
   resolve an object route as every frontier row of that object.

Outcome: **the store was wrong** (not the contract). The fix is the two hunks — the object row is stamped only when
the object has no parts (with a comment saying why), and the object arm resolves `m.obj_ref IN (SELECT obj_ref FROM
big_sync_objs …)`. No test file changed: the existing shared assertion *is* the regression test, since it failed
before and passes now.

**Verified by the operator, on my own runs:** `big_repo` contract 1 passed (was exit 100); both `big_sync`
instantiations pass; `clippy -p big_repo --all-features --all-targets` and `clippy -p big_sync --all-targets
--all-features` exit 0 with zero warnings; `big_repo store::sqlite` 67/67. **My own negative check**: with the guard
disabled but the widened arm present (`if true || parts.is_empty()`), the contract fails with the original
`an object route with buffered events produced no page`, so that half is independently necessary; the worker
reported the mirror check for the other half (`assert_subscribe_live_filtering_contract`, `payload None` vs
`{"idx":1}`), which my probe could not reach because the suite aborts at the first failing assertion. Probe
reverted through a tracked edit and the suite re-run green.

**Flagged, not fixed (deliberate, narrow diff):** `ReplayCandidate._maybe_part_id` and its `LEFT JOIN` are now dead;
the pre-existing FIXME at `sedimentree.rs:283` (linear merge scan, now fed the object's part rows too) is not
addressed; and consecutive *partless* payload writes still coalesce into a single object-route page in all three
stores, unpinned by any assertion. Each is a follow-up, not a regression.

### H.8 Two cross-store divergences around `add_obj_to_parts` (unpinned, found while answering a design question)

An object joining a part **does** publish a change even when its payload is unchanged: the trigger is the membership
transition to `Live`. All three stores agree on that, and on the two deliberate exemptions — an object with no payload
is recorded as *pending* membership with no event (the event appears when the payload lands), and re-adding to a part
the member is already live in is a no-op. The mechanisms:

- `big_repo` store, `parts_cursors.rs:510-567`: per part, skip if already `Live`, else a cursor +
  `big_sync_members(…, EVENT_CHANGED, cursor)` + bucket transition + `latest_cursor` bump + `SubEvent::Changed`.
- `big_sync` sqlite store, `part_store/sqlite.rs:1069-1128`: same, plus an explicit frontier row per part.
- memory store, `part_store/memory.rs:856-918`: same, via `queue_evt(PartEvent::Changed(…))`.

Two divergences are *not* pinned by any assertion:

1. **Cursor granularity.** The memory store allocates one cursor for the whole call (`global_cursor.next()` before the
   loop) and the plain sqlite store likewise computes `cursor` before its `for part_id in parts`, while the `big_repo`
   store allocates a cursor **per part inside the loop** (`parts_cursors.rs:549`). Adding one object to two parts in a
   single call therefore yields two events sharing one cursor in two stores and two distinct cursors in the third.
   Because resume-from-cursor paging uses the cursor as an exclusive lower bound, a client resuming from a shared
   cursor can skip the sibling event — a plausible store-specific flake.
2. **Frontier visibility of a membership add.** In the `big_repo` store the frontier *is* `big_sync_members`, so the
   add writes the object route's row directly (and the H-J fix widened the object arm to read every row of the
   object, so the route now sees it). The plain sqlite store writes an explicit frontier row per part. The **memory**
   store writes **no** frontier entry (`add_obj_to_parts` has zero `frontier` references), so on that store a
   membership add is invisible on the *object* route — which is A1's asymmetry seen from the explicit-add side rather
   than the subscribe-materialization side.

Both are candidates for the shared contract suite (`assert_host_part_store_contract`), which is exactly where a
cross-store semantic like this belongs.

### H.9 H-C done — the blob digest became a type, so the escape is unrepresentable

**Shape chosen: (a), the 32-byte invariant carried by the type.** `pub type BlobId = ObjKey` became
`pub struct BlobId([u8; 32])` (`daybook_core/blobs.rs:91`), with:

- `TryFrom<&ObjKey> -> Result<BlobId, BlobIdDecodeError>` as the only way in from a foreign key, so a key that is not
  a digest is *rejected* rather than panicking or reaching the path layout;
- `From<BlobId> for ObjKey` for the part-store object key (the digest bytes, so the two spellings of one digest name
  one object);
- `Display`/`Debug` = base58btc of the 32 bytes, which is what `object_paths` (`:629`) is built from — the doc there
  now states the property: *base58 of 32 bytes by construction, so no part of it can read as an absolute path or a
  parent*;
- `to_bytes32()` total (the width is the invariant), so `blob_id_to_iroh_hash`/`blob_id_to_digest_str` can no longer
  panic on a length that cannot vary;
- `FromStr` accepting exactly two spellings — the blake3 multihash text our writers emit, and the plain multibase
  text of a `db+blob` URL — both decoding to the same 32 bytes.

**A second defect the survey found, worse than the review's framing:** the canonical multihash text did not merely
panic downstream, it named a **different blob**. `BlobId::from_str` (multibase-only) turned the 34-byte multihash text
into a 34-byte key, i.e. a different identity *and* a different on-disk name than `put()` writes. Two decoders for one
text form.

**A trap avoided:** `utils_rs::hash::decode_base58_multibase` **panics on the empty string** (it indexes the first
byte), so `FromStr` checks emptiness explicitly rather than relying on the decoder to reject it.

**Verified by the operator** (the worker timed out mid-verification and wrote no results section; all of this was
re-derived from the working copy): `clippy -p daybook_core --all-targets --all-features` exit 0;
`cargo check -p daybook_ffi --all-features` exit 0 (the one other crate that names `BlobId`); 44/44 on
`test(blob) | test(pin_worker) | test(inventory)`; the two new tests pass on their own; and mechanically, every
`BlobId::new` site passes exactly 32 bytes with **no** `BlobId::new(<bytes from a peer>)` left anywhere. The two
tests are substantive, not shallow: `blob_id_rejects_reserved_and_non_digest_spellings` rejects
`/etc/daybook-escape`, `o:/object/path`, `../daybook-escape`, arbitrary text and the empty string, and
`blob_digest_spelling_names_the_same_blob_on_disk` asserts both spellings resolve to the **same on-disk path** via
`repo.get_path`.

**No revert-probe applies here, and that is the point**: the fix is type-enforced, so there is no small edit that
reintroduces a non-32-byte blob id — which is why the negative check is the mechanical absence of arbitrary
construction above rather than a failing-test observation. The brief asked for a third test (a non-32-byte key
reaching the previously-panicking paths); it is unnecessary because that value is now unrepresentable.

Not done, deliberately: no wider `daybook_core` test sweep was run. The change is compile-enforced across the crate
and `clippy --all-targets` compiled every test target, with the affected filters green, so the remaining risk is
runtime behaviour of unrelated suites rather than of the type.

## I. Derived object parts: case 10 dropped, `o:` is a name (decided)

**Decision (operator, 2026-09-18): drop case 10 outright, and stop storing, enumerating or
interpreting derived object parts at all.** `o:{object_key}` stays the reserved *name* from ADR 012
decision 1; nothing branches on it, so a part whose arbitrary key begins `o:` is simply an ordinary
part. The object lane therefore carries **content only** — a membership transition, removal
included, is a part-lane fact delivered to peers that may read that part, and a peer that may no
longer read any containing part is refused rather than told about the removal (long-poll, not an
event bus). ADR 012 is updated: decision 3 rewritten, decision 9's vocabulary extended, the case
table's rows 9-11 and two deferred bullets closed.

### Why (all code-verified, 2026-09-18)

1. **Materialization was a write on a read, and in sqlite it was observable.** `subscribe` on an
   `Object` target called `materialize_object_parts` (`memory.rs:176-215`, sqlite ~`:195-215`), which
   wrote a part row plus the object's single live membership row at the current revision. In sqlite
   `big_sync_members` *is* the keyed frontier, so a subscriber behind that revision was handed a
   fabricated content touch — a read emitting a sync event to a third party.
2. **`obj_parts()` returned the derived row**, so the frontier reconciler removed it as "not
   desired" (`automerge_frontier_worker.rs:388-400`; scope teardown `:1076-1082` — `desired_parts`
   comes from keyhive scope and can never contain a derived part) and then *consumed its own
   removal* (`:811-830` → `Cmd::RemoveFrontierMembership`). **Latent, not the `tier10_4` cause**:
   `AFW mapped removed part revision` had **0** occurrences in `/tmp/tier10-rerun.log` against 277
   `mapped changed` — so the loop exists in code and did not fire in that run.
3. **The inheritance fallback made `o:{O}` pageable as an ordinary part** by any peer that could read
   a containing part (`big_repo/store/sqlite.rs:766-780`, sqlite twin `part_store/sqlite.rs:1610-1640`
   — `find_part_ref` miss, then `part_id.object_key()` → `readable_parts_of_object`). That is the
   hack that made `o:` observable to non-`o:` paging peers, and it is what routed `Removed{part_id:
   o:{doc}}` to them.
4. **Materialization never bought case 11 anything.** Access to `o:{O}` is only ever *inherited*, and
   no production code writes an access row for a derived part (`set_part_members(o:{obj})` has test
   callers only: `memory.rs:1891`, `sqlite.rs:2552,2611`). So an object in no part stayed refused
   with the row present — §3's "fail-closed until object parts exist" was still fail-closed after
   they existed.
5. **Case 10 did not need it either**: `set_part_members` creates the part row itself via
   `ensure_part_ref` (`part_store/sqlite.rs:1496-1500`), and an access row FKs `part_ref →
   big_sync_parts` (`migrations/001_init.sql:91-96`), so a stored row and the direct-share feature
   stand or fall together — which is why dropping the case drops the mechanism.

### What the cleanup touches

- delete `materialize_object_parts` and its call in `subscribe` (both stores);
- delete the inheritance fallback in `permitted_parts`, and with it `PartKey::object_key` (its only
  caller is that fallback) and `ObjKey::object_part_key` (3 non-test callers: the two materializers
  plus the contract harness);
- `obj_parts` needs no exemption once nothing stores derived parts, but assert it (both stores + the
  `parts_cursors` twin) so a future writer cannot reintroduce one;
- object-lane projection: a part-level deletion must not synthesize `Changed{part_ids: [], payload:
  Null}` (`big_sync/part_store.rs:96-110`, `part_store/sqlite.rs:1334-1355`, `memory.rs:375-388`), and
  the machine must not let a membership fact occupy `object_replays` (`cursor.rs:238-275`);
- replace `assert_subscribing_allocates_no_revision_contract` (`part_store.rs:1150-1200`) with "a
  subscribe writes nothing and emits nothing to any subscriber" — the phantom touch was the
  materialization, not the derivation.

Tracker rows this closes or moots: **H.1 A1 and A2** (moot), **C11** (closed by construction: nothing
interprets the prefix), **H-E**'s scope (shrinks to the tombstone/payload asymmetry in §J3), and the
ADR 012 items named above.

### Decisions taken on the earlier list (delegated: "as long as it falls out of the existing design")

- **B6a**: keep the distinguishable `ReplayPageOutcome::Unauthorized`; revocation reconciliation
  consumes it and the leak is the existence of a part id the caller already named.
- **B2a**: do not wire the materialization wake; the durable re-arm stays the mechanism.
- **B14**: option (a) — tag `blocked_refs` at insertion so the log carries real blockers. Rides with
  §C; not a blocker for it.
- **A6**: leave the non-injective retention reader id (unreachable); length-prefix the namespace if a
  caller ever embeds a key in a component.
- **B16**: an experiment before a decision — connect *without* the `/seds` pre-grant and see whether
  the siblings' path still converges; the answer decides harness versus product fix.
- **H-D**: direction fixed — a `big_repo`-local extension trait over the key types whose conversion
  is fallible and returns `eyre`, used at the sync edge; identities that are minted local and
  fixed-width keep the infallible path, where the panic *is* the invariant assertion.

## J. ADR 012 revocation/removal gaps (found 2026-09-18, to address)

- **J1 — the object lane's removal shape contradicted the ADR. DONE (ADR).** The code synthesized a
  payload-less `Changed` for a part deletion while §2/§9 define an empty part list as *resolve the
  membership* and the vocabulary as touch/`Removed`. Fixed in §3 + §9 + case rows; the code half is
  §I.
- **J2 — what a refusal does to a route is unstated. NEEDS A CALL.** §2 says the next page discloses
  a revocation; §9 says denial backs off (`UNAUTHORIZED_BACKOFF`, 30s) rather than dropping the route.
  Missing: the page's resume point is the last *delivered* event (`part_store.rs:436-483`:
  `resume = Some(evt_cursor)` per received event, `next_cursor: if drained { None } else { resume }`),
  so a revoked-then-regranted peer re-reads what it missed *if retention still holds it* — retention,
  not the cursor, is the risk; plus what the route/worker state does while denied, and that the
  distinguishable `Unauthorized` (B6a) is the one non-disclosure gap on this surface.
- **J3 — "content cleared" has no event and the stores disagree about what remains. NEEDS A CALL.**
  The payload is nulled in the same transaction as the last removal with no event
  (`parts_cursors.rs:601-621`, `sqlite.rs:1192-1210`); memory instead tombstones the object and drops
  the entry (`memory.rs:968-975`). §9 says `Removed` "keeps its tombstone" but never defines it.
  Decide whether content-cleared needs a kind, and require the three stores to agree on the tombstone
  shape (contract assertion).
- **J4 — re-add is not symmetric with add. NEEDS A CALL.** `add_obj_to_parts` on an object with no
  payload records *pending* membership and emits nothing (`parts_cursors.rs:511-526`;
  `big_sync_pending_members`), so "removed then re-added" leaves a peer with silence where it expects
  the object to come back; the event appears only once a payload lands. State the rule, or change it.
- **J5 — ordering between an access change and a membership change is unstated. NEEDS A CALL.** Two
  transactions, two cursors, either intermediate state observable. Both orders are safe for a reader
  (it either sees the `Removed` or is refused), but a producer's ordering is part of the contract if a
  revocation must never be observable as a removal for a part the peer cannot read.
- **J6 — recovery after a long revocation is unspecified. RECORDED.** If the log pruned past a
  revoked peer's cursor, no band is specified for re-deriving what it missed: tie it to the pruning
  floor (registered readers, `events.rs:350-382`) and name the recovery (enumeration/RIBLT band or a
  fresh cursor).
- **J7 — ADR 010's removal dependence. DEFERRED with ADR 010.** Its lifecycle is expressed as
  "removed from the active task part" plus a settlement in `S`, and reviewer C10 (no `archive_part`
  → a stale active copy can resurrect execution) is a removal-semantics question. Deferred, but the
  model decided here is what it will build on.

### J resolutions (operator, 2026-09-18) — ADR 012 §2 and §9 amended

- **J2 — refusal is trivial; the real question was tombstone exclusion. Verified, narrowed to one
  open decision.** A page is a list operation over one part: no authorization to list it, refused.
  Access is a property of the part and not of an interval, so a peer that keeps or regains access
  reads the whole retained history behind its cursor — retention is the bound, not authorization.
  The stamps that would let a page *exclude* `Removed` for a never-seen object went with `Added`
  (`big_sync/migrations/001_init.sql:55-57`, ADR 012 §9), and the membership row's `txid` is
  overwritten per transition (`ON CONFLICT … DO UPDATE SET txid`, `parts_cursors.rs:539,589-591`),
  so exclusion **cannot be recomputed** for an arbitrary cursor today. It is exact for `cursor = 0`:
  a reader that acked nothing in a part can hold no membership of it. Cost today: the page's row
  budget is spent on `event_type = 2` rows (cutoff CTE and row query in `list_events_with_policy`),
  which a fresh subscriber on a long-lived part pays in round trips before it reaches content.
  **Not a correctness gap** — an uninformative removal is a no-op on both sides
  (`remove_obj_from_part` early-returns on an unknown `obj_ref`, `parts_cursors.rs:574-577`; the
  peer's replica knows its own membership). **Decided: restore the add stamp as a *server-side
  predicate*, not as a wire kind.** One `added_at` per `(obj, part)` membership row, holding the
  cursor at which that row most recently became *present* (set on absent→present, preserved by
  present→present touches): a deleted row at `T` is delivered to a reader at `c` iff
  `added_at <= c < T`, and `cursor = 0` falls out of the same predicate (no stamp is zero). It must
  be applied in both the cutoff CTE and the row query so excluded tombstones do not consume the
  limit. The event-kind set stays two, so §9's "two subscribers can be shown different kinds"
  objection does not return: only which rows a page is drawn from depends on the reader's cursor.
  Dead-row pruning is a separate question (§K3) and is not safe without epochs.
- **J3 — no content-cleared API exists and none is needed. Closed.** Confirmed: the `PartStore`
  trait exposes `set_obj_payload`/`obj_payload`/`add_obj_to_parts`/`remove_obj_from_part` and
  nothing else, and the only nulling is the implicit `UPDATE big_sync_objs SET payload_json = NULL`
  at `parts_cursors.rs:605-608`. With J4's rule, content-gone is the last `Removed` a reader can
  read and the replica drops content when its containing set empties — an inference, not an event
  kind. The store divergence this item recorded disappears with the rule (nothing nulls a payload
  implicitly), and the contract becomes: the dead membership row stays in every store.
- **J4 — decided: keep the asymmetry; payload retention removes the pathology.** A payload-less add
  is not an event (pending membership), symmetrically on re-add, and with retention a re-added
  object emits `Changed` because its payload is still there. The "sedimentree exists but the payload
  was dropped" case is gone, and the new invariant is **live membership implies a payload**.
- **J5 — dissolved; only the rule is written down.** The question was transactional ordering between
  the access change and the membership change (two writes, two cursors). With J2's answer it does
  not arise: the filter is *current* access, no epoch is carried, both orders are correct, and a
  reader either sees the removal or is refused the part that names it.
- **J6 — recorded, and now the same knob as J2's tail.** The pruning floor (registered readers)
  bounds both the long-revocation gap and the tombstone page tail.
- **J7 — deferred with ADR 010** (unchanged), now noted as building on the payload rule above.

## K. GC, counters, cursor epochs (decided shape; implementation ordered)

Decided with J2-J4 (operator, 2026-09-18); ADR 012 §9 carries the prose.

- **K1 — explicit payload removal is the only way to clear content.** `remove_obj_payload` on the
  store: in one transaction, remove the object from every containing part (emitting those
  `Removed`s) and then drop the payload. Stop the implicit null in `remove_obj_from_part` (both
  stores; memory additionally drops the object entry today at `memory.rs:968-975`). Invariant: live
  membership implies a payload, and a payload-less object cannot be in a part (payload-less adds
  stay in `big_sync_pending_members` and emit nothing).
- **K2 — the janitorial loop is the embedder's.** Store side: a part-less object listing plus
  counters (live/dead rows, payload bytes, tombstone bytes, dead-to-live ratio per part). Backend
  side: a loop that reads them, decides what to keep, and calls K1. This replaces "membership
  removal drops the payload" as the collection policy, and the ratio is also what says which parts
  are paying bucket-sync cost for slots nobody asked about.
- **K3 — cursor epochs: withdrawn; tombstone vacuuming blocked on an authority rule.** A per-part
  epoch carrying rotation state in the cursor's identity is *wrong specialization*: it makes the
  bucket strategy's meaning depend on a flag two partitioned peers cannot agree on. It also does not
  answer the case that matters — A syncing from B, where B's stale presence resurrects what A
  pruned — so it cannot be the thing pruning rests on. Pruning needs "whose membership set wins on
  disagreement": an authority for the part. big_repo can compare authority for a partition;
  keyhive's authority is over object *existence*, not membership, so it cannot settle a member row.
  Detection already exists (`live_fp`/`dead_fp`, `live_count`/`dead_count` separate dead from live),
  so the gap is the direction only. Until then, dead rows are kept and paid for locally (storage,
  dead fingerprint, comparisons); `added_at` removes the wire cost, which is what it was for.
- **K4 — an epoch is neither an event kind nor `added_at`.** `added_at` excludes tombstones
  *within* an epoch; a rotation drops them *between* epochs. Both keep the wire vocabulary at two
  kinds.
- **K6 — leaf page byte budget.** `limit_hint` counts entries, not bytes: at the cap a page of
  32-byte keys is ~42 KB and a longer key is worse, so the per-object cost is no longer bounded.
  Decision: add a byte bound beside the entry cap (recorded in the ADR's deferred list too).
- **K7 — key-hash addressing: not now, on purpose.** Replacing the leaf entry's `obj_id` with a hash
  of it does not work as a local tweak: the leaf page is *paired by id* (the asker looks the object
  up locally and computes its own fingerprint), and the action on a difference is keyed by id, so a
  hash means a resolution step per difference — i.e. difference-proportional set reconciliation
  (RIBLT), which is already the deferred band with its own item-width question. It would also make
  the hash an *identity* if it ever addressed an object, which needs a scope-stable keyed hash and a
  durable hash-to-key index to be ungrindable (ADR decision 9's case 12). Ignored for now.
- **K8 — the replay loop's shared cursor (fixed with this work).** `subscribe_with_policy` advanced
  one high-water cursor across parts, so when one part's page truncated at `REPLAY_RAW_BATCH_SIZE`
  (256) while a sibling's page returned a higher cursor, the truncated part's remaining rows between
  the two were skipped — silent event loss on catch-up. Fixed: each part advances from its own page
  (its bound moves only from that part's returned cursors/`next_cursor`, advanced before the access
  filter so a filtered row is not re-read forever), grouped by bound so shared positions still
  collapse into one store call. The object route got its own cursor in a mixed subscription —
  strictly fewer dropped events, and **not covered by any test** (`replay_page` cannot reach
  multi-part: it takes one target by value, so it never had the shape).
- **K5 — where `added_at` lives (storage, not wire).** It is per-key metadata on the frontier's own
  row: a column beside `event_type`/`txid` in `big_sync_members` (both sqlite stores — the
  big_sync store's frontier *is* that row, `part_store/sqlite_frontier.rs` decodes it into
  `FrontierEntry<PartFrontierKey, PartEvent>` and already reads `event_type`/`revision` off it),
  and a side map keyed `(obj, part)` in the memory store, whose frontier *value* is the `PartEvent`
  itself (`MemoryKeyedFrontierTable<PartFrontierKey, PartEvent>`, with `tombstoned_objs` already
  holding a removal cursor per object). It must **not** go on `PartEvent`: `PartPage.events:
  Vec<PartEvent>` is the postcard wire type, so a field there would be sent to every peer. The
  page SQL uses it purely as a predicate; the memory store needs the same stamp retained for
  tombstoned keys, which is one more place the two stores must be pinned to agree.
- **K6 — the authority vacuuming would rest on already exists in both deployments.** Big_repo
  derives partitions from keyhive groups, and keyhive tracks the causal relation for object removal
  with permanent revocations, so a removal is derivable from keyhive state rather than from a
  peer's replay event — the honest reason removals were modelled loosely: the primary consumer of
  the replay stream does not act on remote removal events for content. Triage's authority is the
  router: a removal traced to a router is respected, and a healed partition re-derives from the
  historical routers, so validity is a function of which router it came from. Both are "whose
  membership set wins" rules; what is missing is plumbing one into a part store's pruning decision.

## L. Post-batch review lane, diff-attributed (2026-09-18)

Unlike section B, this lane had a shell: it ran `jj diff -r main..@ -- src/big_sync`, paged the
whole diff, and re-checked every claim below against the working copy, so these items are
attributed to the diff (in the one case that leaves `src/big_sync`, the file it leaves is the
symlink documented in L1). No test suite was run for the comment/doc-only part of this lane;
`cargo clippy -p big_sync --all-targets --all-features` was.

| id | area | claim | evidence | sev (lane) | status |
| -- | ---- | ----- | -------- | ---------- | ------ |
| L1 | part store (schema) | `big_sync/migrations/001_init.sql` is edited in place — `buck_index` added to `CREATE TABLE IF NOT EXISTS big_sync_objs`, `added_at` to `big_sync_members`, and `big_sync_syncable` re-keyed from `obj_ref` to `part_ref` with a new `changed_at` — so any database that already applied version 1 can no longer open: sqlx records a checksum per applied migration and refuses a changed file, `CREATE TABLE IF NOT EXISTS` would not add the columns to it anyway, and no `ALTER` exists. Two migrators run this same physical file, so the break is not confined to big_sync's own schema: `SqliteCore::init_schema` runs it under `_sqlx_migrations`, and the big_repo store runs it as its version 1 through the symlink `big_repo/migrations/001_init.sql` under `_big_repo_migrations`. `set_ignore_missing(true)` on the big_sync migrator only covers versions recorded in the DB but absent from the directory; it is not a checksum escape. | `migrations/001_init.sql:1-8,35,81,110-116`; `big_sync/part_store/sqlite_core.rs:353-357` (migrator statics + `set_ignore_missing`), `:374-375` (`init_schema` -> `MIGRATOR.run`); `big_repo/store/sqlite.rs:48-52` (`dangerous_set_table_name("_big_repo_migrations")`), `:456-457` (`init_schema` then `MIGRATOR.run`); `ls -la src/big_repo/migrations/001_init.sql` -> symlink to `../../big_sync/migrations/001_init.sql`. | high | decided — accepted for now, no shim and no new migration file (operator call: break rather than shim). Revisit before release. The file is now internally honest instead: its header states that 001 creates fresh databases only and that no pre-existing scope DB is supported, and the `added_at` comment no longer claims rows can "predate the column" (verified: every insert either stamps a cursor from the bumped-before-use global cursor, so >= 1, or is a guarded upsert whose insert arm cannot fire — `sedimentree.rs:846`, `parts_cursors.rs:1074`). Note the comment edit changes the checksum too, which is the same accepted break. |
| L2 | part store (bucket wire) | The leaf page's byte budget skips a row that does not fit and keeps scanning (`continue`), while the page is keyset-paginated (`o.obj_id > r.after_id`, `next_after` = the last entry returned): if a later, shorter key fits, that entry is returned and the resume point moves past the skipped one, which no later page can then reach. The memory store stops the page instead (`break`), and the budget's own doc bounds the page ("The encoded size one bucket's leaf page is bounded by, whatever `limit_hint` asks for"), with the shared contract test stating the intent as "A leaf page stops at the byte budget before it stops at the entry hint". Secondary: a page whose rows all fail the budget reports `done=false` with `next_after=None`, which the machine reads as "re-ask from the head" (`leaf_after = None`). Uniform key widths make `continue` and `break` identical, which is why that contract test passes (it seeds 4 KiB and 80 KiB keys, but all one width per bucket). | `part_store/sqlite.rs:956-958` vs `part_store/memory.rs:597-599`; `part_store.rs:707-717` (const doc), `:3028` (test doc); `big_sync_core/bucket.rs:174` (`state.leaf_after = page.next_after`), `:177` (`leaf_exhausted`). | high | in-progress — owned by the replay-page wire fork, which is doing `continue -> break` in `part_store/sqlite.rs`. Two things for them while they are in the file: (1) the pin that would have caught this is one wide-then-narrow key pair in the same bucket; (2) the const doc's parenthetical ("a page with no entries reads as `done` while entries remain") does not match either store — memory computes `done = end == items.len()` and sqlite `done = entries.len() == total_count`, so an empty page yields `done = false` with `next_after = None`, i.e. no position at all, which is strictly worse than the doc says. |
| L3 | harness (RPC fidelity) | `MemoryRpcClient::peer_summary` calls `target_part_store.summarize_parts` directly, so the in-process harness answers a summary for every named part, while the production responder refuses a part the asker may not read before summarizing (`read_denied` -> `UnkownParts`, folded into the same answer an unknown part gets). The other three arms of the same client pass `source_peer_id` and let the store refuse, so the summary arm is the odd one out: a test that reasons about policy through the summary path exercises a more permissive responder than production. | `test.rs:215-241` vs `rpc.rs:431-483` (refusal at `:472`); the three arms that do pass the peer: `test.rs:243` (`replay_page`), `:269` (`get_changed_buckets`), `:299` (`leaf_buckets`). | med | open |
| L4 | formatting | Two committed spots are not rustfmt-shaped: the tail of `collect_stats_until`'s doc comment and the whole function (through its closing brace) are indented four spaces past module level, and the `pub use part_store::{..}` list in `lib.rs` is neither sorted nor laid out the way rustfmt emits a vertical list. Cosmetic only — no behaviour depends on either. | `test.rs:1162-1165` (fn at `:1164`); `lib.rs:43-47`. | low | open |

Recorded so they are not re-litigated (deliberate, from the same lane):

- **The hold-expiry "caught up" page.** `replay_page` returns `next_cursor: None` when the hold
expires with nothing drained and no `ReplayComplete`, which a caller reads as caught-up. This is
deliberate and pinned (`assert_page_outcome_contract`), and the client asks for `HOLD_MS` equal to
the responder's `MAX_PAGE_HOLD`, so reaching it needs the responder to fail to deliver within 15 s.
- **`part_dirty_count` does not count an access revocation.** The revocation deletes the
`(part, principal)` row, so there is no `changed_at` left to compare; documented in both stores,
and the peer summary path refuses the part on the same state anyway.
- **memory's `bucket_items_for_path` scans a part's members per bucket page.** The price of keeping
the `obj_id` order the page cursor needs after the index became a hash; stated at the site, and the
sqlite store keeps the range predicate in a column instead.
- **"The first entry is always taken" is load-bearing at both bucket endpoints.** `done` is computed from
where the page ended (`done = end == items.len()` in memory, `entries.len() == total_count` in sqlite), so
a page that returns nothing while rows still match reports `done = false` with `next_after = None`: the
walk loses its position and re-asks from the head rather than terminating. That is why the byte budget
takes the first entry even when it alone exceeds the budget.
