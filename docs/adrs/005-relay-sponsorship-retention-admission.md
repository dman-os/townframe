# ADR 005: Relay Sponsorship, Retention, and Free-Tier Admission

**Status:** Proposed.

## Context

The relay is a hosted multi-tenant sync server built on `big_repo` (Subduction +
Sedimentree + Keyhive). It retains documents on behalf of accounts and serves
them to the account's agents. For the first offering the relay supports
**free-tier users only**, with limits configured locally.

The relay must answer, at every interesting point in the sync path:

* *may this peer connect / fetch / put?* — authority;
* *which documents do we retain, and for whom?* — sponsorship;
* *is this within the account's free-tier budget?* — admission;
* *is this peer flooding us with metadata?* — keyhive/part growth abuse.

These are separate questions, decided in separate seams. The payer of any
future metering is the **sponsor** of a document — an account that asked the
relay to retain it — not the transport peer that happened to sync it.

### Existing mechanics we build on

* `big_sync` maintains **one cursor store** for all parts and backends:
  `big_sync_peer_cursors (scope_id, peer_id, part_id, cursor)` with
  `CursorIndex = u64`. Per-peer, per-part cursors already exist; every
  backend advances them through the same store.
* The relay's own control RPC is `RepoSyncRpc` on the
  `townframe/repo-sync/0` ALPN — a direct node-to-node stream surface,
  deliberately separate from Subduction and big_sync.
* `subduction_keyhive`'s keyhive handler **auto-registers unknown peers**
  on sync-check. That is the metadata-flood surface this ADR must close.
* Subduction's policy layer already has `classify_put_rejection`, mapping a
  policy's error into stable `SyncPolicyRejectionKind`s.

## Terminology

* **Repo agent**: a normal Keyhive principal bound to a device, able to sync
  and author content. Repos exist and sync peer-to-peer with repo agents and
  no relay in the picture.
* **Recovery principal**: a separately-created, stable Keyhive principal with
  recovery/admin authority. Detailed in ADR 006; this ADR only depends on its
  existence as the relay's anchor identity.
* **Sponsorship group** (`G_sponsor`): the account's Keyhive group listing
  the documents the account wants the relay to retain.
* **Adopted set**: the relay's own Keyhive group mirroring which documents it
  has accepted and retained. The account can always observe this set.
* **Adoption**: the relay's acceptance of a document into the adopted set.
* **Lease**: an in-memory claim on a document while the relay syncs it.
  Released on success or failure; re-derived from the adopted set on restart.
* **Adoption worker**: the relay-side worker, one per sponsorship part.
* **Cursor ack**: a cursor-based confirmation of how far the relay has
  progressed on a part/backend — *not* an adoption confirmation.

---

## Decision

### 1. Account onboarding and the account document

Repo creation **precedes** relay involvement and does not require it. Only
when the account wants relay retention does it introduce the relay.

1. The user creates their repo (existing flow): mints the repo authority,
   repo agents. A recovery principal is created **only when the repo is
   first uploaded to a relay** — not upfront — and there may be a separate
   recovery principal per relay (see [ADR 006]).
2. The recovery principal is given **Admin** on the account's authority
   surfaces, including the account document.
3. The **initiating repo agent** (the agent for the daybook repo starting
   this) contacts the relay. The relay receives the agent's contact card
   and the account document's identity out-of-band.
4. The **repo agent creates the account metadata document** and grants the
   relay **read access** to it. The relay does not write it.
5. **The relay verifies the recovery principal has Admin access on the
   account document** before accepting the account. The relay does not accept
   a document it cannot attribute to an account with a verified recovery
   principal.
6. The account document declares the account's structure:

   ```text
   account meta doc (Keyhive document, repo agent writes, relay reads)
     recovery_principal:  <Keyhive principal id>
     sponsorship_group:   S_docs    documents to retain
     repo_agents_group:   G_agents  agents considered members of this account
   ```

7. The relay watches a **private metadata group** it owns, into which it
   discovers account documents, and reacts to changes through the existing
   group-part machinery.

**How the relay first accesses the account document.**

1. The repo agent creates the account document locally and grants the relay
   read access to it.
2. The repo agent tells the relay, out-of-band, that it is the initiating
   agent for that account document (`repo agent, account doc`).
3. The repo agent opens a connection to the relay. Because the relay now
   knows the repo agent, it accepts `sync_keyhive` from it.
4. The relay performs a one-shot `sync_doc(account_doc)` to pull the
   account document's contents; its local Keyhive accepts it because the
   relay was granted read access in step 1.
5. **Watching for changes**: the relay subscribes to the account document's
   obj id for the connected peer, exactly as it subscribes to the
   sponsorship group part (§3) — so subsequent edits to the account
   document (new sponsorship group, new repo agents group, changed
   recovery principal) reach the relay through the same part machinery.

The relay is never an Admin anywhere in the account graph. It is granted
**Relay** access to the documents inside `S_docs`, which is all retention
requires.

The recovery-principal onboarding detail — where the recovery key is created
(a separate place, not the daybook app), how daybook_core adopts the contact
card and grants Admin, and how recovery clones open the repo with a throwaway
identity — is [ADR 006]. This ADR assumes only that the relay can verify the
anchor.

**Keyhive read-only semantics.** Keyhive deliberately permits
sub-delegation — *"Restricting sub-delegation of an Agent's capabilities
MUST NOT be permitted"* — and `compute_add_proof` enforces it: a direct
member can add another member at `can <= their own level`, and anything
higher is an `AccessEscalation` error. So "granting the relay read access"
does **not** produce a read-only member that cannot add: a Read member
could delegate Read/Relay onward. What it does guarantee is *non-
escalation*: the relay can never reach Edit/Admin on the account document,
so it cannot modify the document, its membership at or above Edit, or the
sponsorship group's contents. The relay is trusted not to sub-delegate;
the account can audit the document's membership for unexpected Read-level
delegations.

### 2. Keyhive abuse: refuse unknown peers, bound graph growth

Two independent measures:

1. **Refuse keyhive sync with unknown peers.** On a relay, a peer that has
   not been introduced (no contact card exchanged, not a member of any known
   account graph) must not be able to stream keyhive events at the relay.
   The `subduction_keyhive` handler's auto-registration is wired so that the
   relay only registers peers it already knows (or that present a valid
   account anchor). Unknown peers get keyhive sync refused at the connection
   level, not the event level.

   (Contact-card exchange is not automatic at connection today: the
   protocol sends `RequestContactCard` on demand when a sync needs a peer's
   card. The relay must not honor card requests from unknown peers — a
   known account agent's first contact card arrives out-of-band during
   onboarding, §1.)
2. **Bound per-peer keyhive graph entry growth.** The relay meters how many
   keyhive event entries it accepts per peer per session (and a hard cap on
   total keyhive graph growth per peer). This is a relay-local counter
   surfaced through the same admission seam as usage; exceeding it rejects
   further keyhive sync events for the session. (Exact implementation
   options — a per-session event budget in the keyhive sync handler, or a
   cap in `keyhive_storage` — is a subduction_keyhive change tracked
   separately; the relay policy declares the limits.)

### 3. The adoption worker: sponsorship parts to the adopted set

The relay does **not** mirror `S_docs` wholesale. One **adoption worker** per
sponsorship part subscribes to the peer's sponsorship group part and turns its
membership into the adopted set.

**State.** The adopted set is a **Keyhive group** owned by the relay (a mirror
of accepted documents). It is not a table: group membership is the state, the
account can observe it, and restart recovery is the same as for any other
group. Leases are in-memory only — they are re-derived from the adopted group
+ current membership after a restart.

**Event handling.**

```text
Add(D):
  if D in adopted:          # dupe event — still sync, don't skip, don't panic
    sync_doc_from_peer(D)
    return
  lease = take_lease(D)             # in-memory; None if already leased
  if lease is None: return
  if adopted_count >= max_docs:     # free-tier doc budget
    release_lease(D); return        # stays out of the adopted set
  match sync_doc_from_peer(D):
    Ok:    add D to adopted group; release_lease(D)
    Err:   release_lease(D)

Change(D):
    if D in adopted group: sync_doc_from_peer(D)   # incremental
    else:                  return                  # not adopted, ignore

Remove(D):
    if D in adopted group:
        remove D from adopted group
        delete sedimentree content of D (storage layer, graceful)
    # slot freed: pick another D' from S_docs not in the adopted set
    # and try_add it (lease-based, same as Add)
```

`sync_doc_from_peer` is the existing big_sync/Subduction sync of the
document's Sedimentree from the connected peer, initiated by the worker. The
relay pulls at its own pace; it never mirrors the sponsorship group
wholesale.

**Adoption budget.** At adoption time the relay checks only the **document
count** (`max_docs`) against the adopted group. Byte budgets (`max_total_bytes`,
`max_sedimentree_bytes`) are **not** computed at adoption — the relay cannot
trust remote payloads, and totals are not known up front. They are enforced
inside the storage layer (§5), where true sizes are known.

### 4. Cursor acks: the relay's fetch confirmation

The relay's progress is observable via **cursor acks** — not as a new sync
backend, and not as a relay-specific adoption protocol (the account can always
observe the adopted set). A client that needs to know the relay has pulled
its content — e.g. before logging off — opens an irpc stream on the peer
(`RepoSyncRpc`, the `repo-sync/0` control surface) and receives **every
cursor advance the remote maintains for it, per part**:

```rust
// stream subscription on RepoSyncRpc
SubscribeCursorAcks { }

// streamed events
CursorAck {
    part_id:  PartId,
    backend:  BackendId,
    cursor:   CursorIndex,      // the relay's new cursor for (part, backend)
    payload:  Option<serde_json::Value>,  // opaque backend-specific detail
    at:       Timestamp,
}
```

**Ack payloads** are generic — an opaque JSON payload alongside the cursor
position, so backends can attach whatever detail they need (e.g. heads,
doc ids, rejection reasons) without new wire types.

Because the stream is inert when there are no events, a **separate RPC
call** fetches the latest cursor acks on demand:

```rust
GetLatestCursorAcks { part_id: PartId } -> Vec<CursorAck>
```

The cursor is the same store big_sync already maintains
(`big_sync_peer_cursors`, per peer+part). Advancing it means the relay has
processed the event stream up to that position — including the exact heads
the client wants to know the relay holds. Cursor acks are **generic**, not
relay-specific: any peer can subscribe to any peer's cursor advances over the
same control surface.

### 5. Per-sedimentree disk usage

The relay must know, for each adopted document, how many bytes it stores.
The current schema (`big_repo_subduction_commits` / `big_repo_subduction_fragments`)
stores payloads in `blob` columns but has no per-tree aggregate. Summing
`LENGTH(blob)` per check is O(n) and too slow for the hot path, so usage is
maintained **incrementally**, in the same transaction as the writes.

**Schema addition:**

```sql
CREATE TABLE big_repo_sedimentree_usage (
    scope_id        INTEGER NOT NULL,
    sedimentree_id  BLOB NOT NULL,
    bytes           INTEGER NOT NULL DEFAULT 0,   -- stored payload bytes
    objects         INTEGER NOT NULL DEFAULT 0,   -- stored item count
    PRIMARY KEY (scope_id, sedimentree_id)
) STRICT;
```

**Maintenance** (transactional with the write):

* On `insert_commit_rows` / `insert_fragment_rows`: for each row actually
  inserted (`rows_affected() == 1`), add `LENGTH(signed) + LENGTH(blob)` to
  `bytes` and `1` to `objects`.
* On delete (compaction, GC, adoption removal via the worker): subtract the
  same deltas.
* Fragmentation/rollup is naturally accounted: creating a fragment adds its
  bytes; deleting the covered loose commits subtracts theirs.

### 6. Per-sedimentree size limits

A single document must not balloon. Defaults:

```text
max_sedimentree_bytes     = 40 MiB
max_sedimentree_objects   = 4_000 total commits
```

Fragmentation should keep trees small, but subduction has known cases where
fragmentation breaks, and an attacker can create new **root commits that never
fragment**. Limits are therefore enforced **at storage time**, inside the save
transaction, per tree:

```text
if usage.bytes   + delta_bytes   > max_sedimentree_bytes:   roll back, reject
if usage.objects + delta_objects > max_sedimentree_objects:  roll back, reject
```

**De-adoption on policy failure.** Policy failures (size-limit rejections)
de-adopt the document: the relay removes it from the adopted set and records
the reason durably:

```sql
CREATE TABLE relay_deadoptions (
    account_id  BLOB NOT NULL,
    doc_id      BLOB NOT NULL,
    reason      TEXT NOT NULL,        -- 'size_limit' | 'commit_limit'
    occurred_at INTEGER NOT NULL,
    PRIMARY KEY (account_id, doc_id)
) STRICT;
```

Transient sync failures are retried (lease released, worker re-syncs later);
policy failures are terminal for the document's current adoption.

**Future work.** The de-adoption story is weak today: a document that
exceeds a size limit is dropped from the adopted set and can only return via
a re-sponsored membership event. Future work should address de-adoption
holistically — e.g. a re-admission path that keeps the document around but
out of quota, a grace window before eviction, or rejection feedback to the
account — rather than the hard drop specified here. Not blocking v1.

**Future optimization.** Rejecting size-limit violations before writing bytes
to subduction at all (check the delta against the limit before the insert,
not after) — noted here; not required for the first cut.

### 7. Account-level free-tier limits

Configured locally (per-account in structure, one free tier in practice):

```text
max_docs                  (e.g. 5_000)
max_total_bytes           (e.g. 250 MiB)
max_sedimentree_bytes     (40 MiB, per-doc)
max_sedimentree_objects   (4_000 commits, per-doc)
```

Enforced at adoption (§3, count) and at storage time (§6, bytes/objects).

### 8. Rejection taxonomy

`SyncPolicyRejectionKind` gains stable kinds for the admission layer:

```text
QuotaExceeded
DocumentLimitExceeded
RateLimited
```

mapped through `classify_put_rejection` so the sync session reports clean,
per-item policy rejections rather than storage errors aborting the session.
This implies enhancing Subduction to carry the custom kinds through
`SyncPolicyRejectionKind` (additive, and the current `classify_put_rejection`
default maps unknown policy errors to `Other`).

---

## Consequences

### Positive

* **Sponsorship is user-driven and relay-minimal**: no relay-created groups,
  no relay Admin authority, no trust inversion.
* **The 5,001-doc problem is structural**: adoption is a pull-side
  projection; group membership never forces storage.
* **The hot path stays network-free**: admission reads the adopted group +
  in-memory leases; byte accounting is transactional with the writes.
* **Per-sedimentree usage is exact and cheap**: incremental counters in the
  same transaction, no O(n) scans.
* **Cursor acks are generic**: one mechanism serves every backend, no
  relay-specific sync protocol.

### Costs & Trade-offs

* Adoption is eventually consistent; the account observes the adopted set
  directly and cursor acks for relay progress.
* Per-sedimentree size rejection happens after the bytes are written to
  storage (future: check before write).
* The relay refuses keyhive sync with unknown peers; that changes the
  onboarding story for a fresh peer and must be matched with an out-of-band
  contact-card exchange for every account agent.
* De-adoption is irreversible until a document is re-sponsored within budget.

---

## Deferred Decisions

* **Recovery principal** (creation, admin grant, recovery flow, throwaway
  identity) — [ADR 006].
* **Keyhive metadata metering internals** — where the per-peer keyhive event
  cap lives inside subduction_keyhive.
* **Paid tiers / egress charging** — sponsor-pays-by-default, provider pays
  for public docs.
* **Token / vouching / crypto-reputation** — issuer model at
  `authorize_connect`.
* **HTTP publishing** — publication projections and provider-pays egress.
