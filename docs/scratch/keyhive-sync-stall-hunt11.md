# HUNT11 — keyhive peer-sync stall diagnosis (4-node stress, 960s timeout)

Log: `/tmp/hunt11-60m.log` (single test run, all node processes interleaved).
Peer map (verified: each `runtime2_hub` local_peer_id observes exactly the 3 endpoint ids that are not itself — `SyncSessionObserved` counts):
left = `cda13a681af2…` (base64 `zaE6aBry…`, hub `zEqhAodnY…`) = **A**;
right = `92bdc2021dff…` (`kr3CAh3…`, hub `zAspM7D5…`) = **B**;
`a6e8f0ba…` (`pujwuuT+…`, hub `zCEYh74Rz…`) = **C**; `896a51eb…` (`iWpR66…`, hub `zAFQtJbrw…`) = **D** (reopened node).
Endpoint hex ⇄ base64 verified byte-exact; hub ids are 33-byte `0xc0`-prefixed multikeys (different namespace, mapped via sessions).

## 1. Serving-side send-set computation (`subduction_keyhive::protocol`)

- Responder: `handle sync request` → `found_ops = local_pair_events − (peer_found ∪ peer_pending)`, `requested = peer_found − (local ∪ our_pending)` (`../subduction/subduction_keyhive/src/protocol.rs:917-930`). INSTR `sending/send_prefixes/our_pending` is logged in `log_sync_response_plan` (protocol.rs:964-1000).
- `local_pair_events = get_events_for_peer_pair` (protocol.rs:1372-1429): walks **only three agents** — `get_agent(our_id)`, `get_agent(their_id)`, `get_agent(Public.id())` (protocol.rs:1387-1398) → `keyhive.static_events_for_agent` (protocol.rs:2094-2104, `keyhive_core/src/keyhive.rs:1430`). Result = public events ∪ (our_events ∩ their_events) (protocol.rs:1404-1425).
- `our_pending = keyhive.pending_event_hashes()` (protocol.rs:1431-1437) — events that failed admission and sit in `pending_events` (`keyhive_core/src/keyhive.rs:3035-3106`); they are **not** in any per-agent event set until deps arrive (keyhive.rs:3041-3099).
- The requester advertises with the **same** pair view: `sync_keyhive_for_request` builds `found` from `get_events_for_peer_pair`/cache (`protocol.rs:597-610, 1930-1938`), plus its own pending.

How a serving side holding events answers `sending=0`:
- **(b) pair-view classification miss (the one that fired here).** Events attributed to a *group/document agent* (e.g. the `BigKeyhiveGroup(0x29259f1f…)` of doc z8tuaGWr, log 127270) are attributed to none of {our, their, Public}, so they are absent from the pair view **on both sides** — the holder never sends them and the peer never requests them. Proof: B created doc z8tuaGWr at **452.68s** (log 127268-127280, `FinalizeAllocatedDoc`, access deltas emitted 452.688) and still served `sending=0` to A's request at **458.163s** (log 134093) and in every later round. 5.5 s old, admitted, pending=0 — not a race.
- **(a) ingest race**: a responder can serve a request before its own just-received events are ingested/admitted (ingest then persist, protocol.rs:1558-1668; cache currency gated on generation, protocol.rs:2044-2092). Concrete window in log: B asked C for `98a6f25f` at 457.416 (log 133560); C answered `sending=0` at 457.557 (log 133716) because C had only *requested* that hash from A at 457.260 (log 133074). Transient, self-heals on a later round — not the stall here.
- **(c) pending advanced**: stuck pending events stay out of `static_events_for_agent` but do show up as `our_pending > 0` (protocol.rs:1431). All stall-window INSTR lines say `our_pending=0`, so (c) is ruled out by the log.

## 2. townframe hub: what re-arms `start_keyhive_sync`

- `start_keyhive_sync` (`src/big_repo/runtime2/hub.rs:2605`) fires from: (1) `ConnEstablished` establishment handler (hub.rs:1936 → 2539; also waiter path 971-972); (2) `WaitForKeyhiveReconciliation` waiters queued on a live conn (hub.rs:959-972, 2528-2539); (3) `handle_keyhive_change_notif` (hub.rs:2725) fed by the per-peer `subscribe_keyhive_changes` RPC subscription (native.rs:1589-1650), wired whenever `keyhive_change_notifs` is on — always in non-test builds (`src/big_repo/lib.rs:349`); (4) post-round latched follow-ups (hub.rs:2696, 2854).
- **No periodic keyhive reconciliation loop exists** (only `keyhive_maintenance_loop` compaction and `keyhive_cache_refresh_loop` in the log). The push path *does* exist: `keyhive_dispatcher` tails the durable admission log and wakes subscribers (`src/big_repo/runtime2/keyhive_dispatcher.rs:20-31, 164-265`), and a notification starts a round (hub.rs:2725).
- Observed non-re-arm: B's `OpenConn` to C at 671.97s (log 239196) produced no round — an open to an already-connected peer emits no fresh `ConnEstablished`, so nothing re-arms. With connections never lost after 462s, **the only remaining trigger is the dispatcher notification**, which fires once, at admission time, on the gaining node (keyhive_dispatcher.rs:179 "Boot at the current head: pre-boot incorporations are covered by each subscriber's initial pull"). If that one round's pair-view misses the events (case b), the divergence is permanent.

## 3. Who withheld what

- Doc **z8tuaGWr** was created by **B** during stage-6 at 452.68s (log 127268+: hub `zAspM7D5` `FinalizeAllocatedDoc`, pending group `0x29259f1f…`; access deltas `DocumentAccessChanged` Admin for 4 member principals, log 127279-127281). Its keyhive membership events are B's extra `kh_events` (+3) and its `big_sync_members` rows (right-only, log 307500-307510).
- A pulled obj z8tuaGWr from B three times; B served **Unauthorized** every time (log 135434 @458.24, 147114 @461.58, 148461 @461.95: "classified remote Unauthorized after Keyhive reconciliation … before=NotAuthorized after=NotAuthorized", `src/big_repo/backend.rs:235`). B's serving-side fetch policy (`src/big_repo/store/sqlite.rs:533-594`, `big_sync_syncable` rows) and/or A's own keyhive lacked the group membership that the pair-view sync never delivered.
- The final keyhive rounds 462.2-462.6 are all empty both directions (log 149716, 149993 with `sync_responder_total=378 = sync_requester_total`, 462.268) — consistent totals, so both sides *believe* they agree; the group-scoped events are invisible to the 3-agent pair view, so neither side can detect the divergence. Events were created **before** the last round (452.68 < 462.2), by B (stage-6 mutations run on active nodes; the reopened node D created only `342dd044`/`809dde08` at ~460s, which did propagate — log 142051-142253).
- Kill switch: after reconciliation A's keyhive still says NotAuthorized → the big_sync backend **settles the object's cursor as a Completion** ("Re-grants publish a fresh frontier object… this obsolete cursor may be settled", `src/big_repo/backend.rs:247-275`) → A never retries z8tuaGWr; parity frozen from 511.3s (log 184273; poll diff 307467-307510).
- Caveats: `kh_events=140 vs 143` is weak evidence by itself — `big_repo_keyhive_event_log` is compaction-trimmed (`src/big_repo/store/sqlite/events.rs:388` DELETE below watermark), so counts skew with compaction timing. The reliable signals are the object payload/membership rows and the three Unauthorized classifications. The `z4BtTqMK`/zA7RxidiMb left-only membership row (log 307504) with equal object heads is an unexplained secondary asymmetry — flagged, not diagnosed.

## 4. Fix shape (matching existing mechanisms)

1. **Fix the advertisement basis (root cause, in subduction).** The sync request/response set must not be derived from the 3-agent pair view alone. The protocol already has a full enumeration: `all_agent_events` (`protocol.rs:420-545`, used by `snapshot_probe` and by the dispatcher's diag) and the durable admission log. Serve/advertise from that (or include the group/document agents of groups shared with the peer); keep the pair view as a filter optimization. Seam: `get_events_for_peer_pair` (protocol.rs:1372) + `cached_events_for_pair_with_peer`/`ensure_cache_current` (protocol.rs:1930, 2044). This is an upstream subduction interface change — flag to operator.
2. **Keep the push path, it's already the right shape.** `keyhive_dispatcher` + `handle_keyhive_change_notif` is serving-side notify-on-admission; once (1) lands, the notification-triggered round can actually deliver the events. No periodic blind polling needed; a cheap divergence check (event-count-per-prefix summary in the existing `sync_responder_total`/`sync_requester_total` fields, protocol.rs:904-907) would make a silent pair-view miss *detectable* — today `378=378` looks converged while the groups diverge.
3. **Stop settling on self-reported NotAuthorized (townframe).** `backend.rs:247-275` treats "my keyhive grants nothing after one pair-view reconciliation" as proof of revocation and settles the cursor. Local absence after a sync that *cannot carry group events* is not evidence. Correct form: settle only when the serving side affirms revocation (extend the Unauthorized response with the reason, `../subduction/subduction_core/src/handler/sync.rs:877-902`), otherwise park the object and re-drive on the next keyhive change notification for that doc (the push path of §2). No retry storm: the existing latch (hub.rs:2711-2723) coalesces.
4. Product decision needed from the operator (choose one):
   - (A) Make document/group-scoped keyhive events first-class in the peer sync advertisement (subduction protocol change), accepting larger request/response sets; or
   - (B) Keep pair-view sync and treat group membership as *pull-on-demand*: on `Unauthorized`, the requester issues an explicit "send me membership for doc X" exchange before any settlement decision.
   - Related: should `after=NotAuthorized` (backend.rs:247) ever settle a cursor without server-side confirmation? Current behavior turns any membership-delivery lag into a permanent, silent content divergence — that is what froze this test.

## Current-source qualification (2026-10-02 greening)

The log observations above remain historical evidence; the proposed attribution is not established
for the current branch. Current `static_events_for_agent` calls `events_for_agent`, which includes
reachable document CGKA operations and `membership_ops_for_agent`; the latter traverses
`membered_handles_reachable_by(who)` and includes delegation and revocation heads. Therefore three
root identifiers in the peer-pair computation do not by themselves imply that group/document events
are excluded. Prove a missing event against those traversals and the pair intersection/cache before
changing advertisement policy. Current `backend.rs` also already distinguishes Unknown and checks
document membership history before settling NotAuthorized, unlike the historical quoted path.
Use a fresh failing trace to determine whether either mechanism remains defective; do not implement
the earlier fix proposal solely from this document.