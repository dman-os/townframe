# big_blobs replacement — scope and plan

Status: Draft — scope/plan only; exploratory implementation is paused pending design review
Owner: operator + big_blobs track
Updated: 2026-10-30
Scope: Replace the blob subsystem in `src/daybook_core/blobs.rs` and its blob-specific index/worker glue with a Cabinet-style `big_blobs` service and workers. This document is the PR plan and design-review record; it is not evidence that the exploratory code in the worktree is complete or accepted.

## 0. Decision requested

Before implementation resumes, agree on the target shape below, especially:

1. Whether `big_blobs` is a Cabinet-style service crate consumed through `big_node`, with legacy Daybook left running until a later cutover, rather than a wholesale Daybook migration in this PR.
2. Whether the portable service surface stays deliberately small (`put` / bounded `read` / `stat`) while large transfers, filesystem references, materialization, and sync remain native/embedder concerns.
3. Whether blob-specific indexing belongs in `big_blobs` as a projection over Cabinet's generic `datum_set`, rather than adding blob semantics back into `big_cabinet`.
4. How representation retention is owned and released across document references, inventory pins, uploads, and remote sync memberships. The current exploratory code has an unresolved shared `held:`-tag ownership problem here.
5. Whether pin-only virtual ciphertext must remain servable after the document/key path that originally registered it becomes unavailable, and if so what durable non-secret record makes that possible.

If any of those answers differ, change this plan first; do not let the exploratory implementation silently decide them.

### 0.1 Drawer-native design

Superseded content was removed from this section. The drawer-native design is maintained in [big-blobs-drawer-design.md](./big-blobs-drawer-design.md) (design log: settled/proposed/open items, corrections, flags), and its settled parts are now ADRs:

- [ADR 023](../adrs/023-cursor-consumption-is-long-poll.md): cursor consumption is long polling.
- [ADR 024](../adrs/024-indexes-are-fed-by-the-change-stream.md): indexes are fed only by the change stream; the drawer projection is frontier-fed; datum_ref edges fold into the datum-set worker.
- [ADR 025](../adrs/025-cabinet-datum-predicates-and-polling.md): the Cabinet datum predicate language and `PollDatums`.
- [ADR 026](../adrs/026-drawer-native-blobs.md): drawer-native blobs — co-located JWK, per-doc slots, inventories as drawer meta docs, self-doc children, per-drawer partitions, relay sponsorship walk.

Where this plan, the log and an ADR disagree, the ADR wins; the log keeps the discussion trail and the items still open. The work items are consolidated in the log's §14.

## 1. Objective

Create a new blob stack alongside the existing Daybook stack:

- `big_blobs_core`: portable datum declarations, blob identity, and the service protocol.
- `big_blobs`: native service implementation, blob store, encryption, workers, projections, retention, and sync backend.
- `big_node`: composition/lifecycle integration, analogous to how Cabinet services are booted and stopped.
- Legacy `daybook_core` remains functional during the transition. This PR should not migrate every drawer, facet, plug, runtime, or blob caller in Daybook.

The intended end state is that `daybook_core/blobs.rs` no longer owns the blob system. The migration path is incremental: first make the new stack correct and independently testable, then move consumers, then delete legacy ownership.

## 2. Sources and invariants

Primary design sources:

- [ADR 001 — Blob inventories](../adrs/001-blob-inventories.md): logical plaintext identity is separate from physical representation identity; relays see representation digests only.
- [ADR 003 — Encrypted blob mirrors and key storage](../adrs/003-cipherblob.md): encryption is a representation concern; RFC 8188 `aes128gcm`; deterministic ciphertext regeneration; virtual ciphertext serving; pins are derived, never independently authored. Its §19 key documents, `keyScope` and `grp:` domains are withdrawn by ADR 026: the JWK is co-located with the cipherBlob in the content doc.
- [ADR 013 — Keyhive access delta stream](../adrs/013-keyhive-access-delta-stream.md) (**Status: Proposed**): access changes are durable wake-ups; consumers seed from live state before opening streams so archive-floor gaps are safe. Its only consumers are the legacy `daybook_core` permission writer and the paused `big_blobs/permissions.rs`; Cabinet-side admission wakes off the authority stream (ADR 015 §3) instead, merged with the datum frontier as in `doc/observe.rs`.
- [ADR 019 — Cabinet drawers](../adrs/019-cabinet-drawers-and-selective-mirroring.md): Cabinet owns generic drawer/document semantics; blob-specific behavior stays out of Cabinet. Amended for meta docs, self-doc children and the publicness contract.
- [ADR 020 — Documents and datum writes](../adrs/020-documents-repo-and-datum-writes.md): consumers must read through the signed Docs gate at pinned heads; pending/rejected/undecodable states are distinct.
- [ADR 021 — Datum schema language](../adrs/021-datum-schema-language-and-automerge-codecs.md): datum types state openness explicitly and preserve unknown members.
- [ADR 022 — Authority, actors, capabilities, and grants](../adrs/022-authority-actors-capabilities-and-grants.md): resource calls carry one scoped domain capability; no identity fallback.
- [Multiprocessing notes](../DEVDOC/log.md): prefer actor/service boundaries that can work in-process today and cross-process later; sync ownership is single-instance; derived-state workers need durable, deduplicated progress.

Non-negotiable invariants:

- Plaintext/content digest and ciphertext/representation digest are different identities and must never be conflated.
- Relay-facing inventories expose representation digest + length only: no plaintext digest, key reference, MIME type, or correlation metadata.
- Ciphertext is reproducible byte-for-byte from plaintext, JWK secret, record size, and padding policy. Preserve the existing RFC 8188 framing and salt derivation exactly unless an ADR changes it.
- Ciphertext may be virtual: plaintext + durable bao outboard + provider registration, without storing ciphertext bytes. Serving must be bounded-memory and range-capable.
- A pair is rooted before anything can point at it; the durable pair-root ledger is written before `ct:`/`pt:` tags and is not cleared until durable retention ownership has replaced it.
- Pending, rejected, undecodable, denied, and unavailable are different states. A transient unreadable document must not shrink retention or delete projection rows.
- Durable effects precede acknowledgements: worker cursors advance only after the derived write they caused is durable.
- No raw Automerge hydration or unsafe inspection may bypass the signed Docs read gate for ordinary blob reconciliation.
- Keys are resolved through accepted `cipherBlob.keyRef` state at pinned heads. Key material is never persisted in the blob store or exposed in debug output.
- Every worker must be restartable, idempotent, and re-armed by a durable source change — not by timers, hot retry loops, or boot-only full scans that can miss later orderings.

## 3. Current state

### 3.1 Legacy system being replaced

`src/daybook_core/blobs.rs` currently owns much more than byte storage:

- `BlobsRepo`: owned-copy/reference file storage, metadata sidecars, iroh-blobs ingestion/export, materialization to staging paths, presence announcements, and sync-backend wiring.
- `encryption_worker`: creates/rotates encrypted representations, resolves keys, installs virtual pairs, and reacts to document/byte/eligibility orderings.
- `pin_worker`: derives inventory pins from Blob/CipherBlob state and releases pair roots.
- `pins_part_worker`: projects pins into Big Sync parts.
- `permission_writer`: mirrors inventory-document access into part membership/access rows.
- `key_source`: resolves JWKs through the legacy drawer.
- `sync`: Big Sync backend that fetches missing representation bytes.
- Moved already in the exploratory worktree: the RFC 8188 codec/store/download/serve modules and `pair_roots.rs` now live under `src/big_blobs/encrypt*` / `src/big_blobs/pair_roots.rs`, with legacy callers updated to import them from there.

Blob-specific legacy index behavior lives in `src/daybook_core/index/facet_set.rs`: in addition to generic facet routing, it extracts the plaintext digest from the **value** of each Blob facet (production Blob facets use the default facet id, so the key does not name the digest) and maintains a digest → documents/branches lookup used to re-arm encryption work when bytes arrive.

### 3.2 Cabinet replacement pieces already available

- `big_cabinet::index::datum_set` is the generic replacement for facet-set routing: it keys routes by `(DocId, DatumKey)`, namespaces by datum tag, and records doc/datum heads without interpreting values.
- `big_cabinet::index::datum_ref` handles declared datum references generically.
- `DocsRepo` provides signed, capability-scoped reads/writes with explicit concurrency modes and command idempotency.
- `DrawerService`/`DrawerAccess` provide local present/originated drawers, roster semantics, and policy-change wake-ups.
- `SchemaRepo` accepts registration groups, so blob datum declarations can be installed by the embedder instead of hard-coded into Cabinet.

Therefore the blob-specific replacement for the old facet-set behavior should be a `big_blobs` projection that consumes `datum_set` routes for Blob datums, reads values through DocsRepo at the reported heads, extracts `Blob.digest`, and maintains a digest → route lookup. Cabinet should not learn what a Blob is.

### 3.3 Exploratory implementation already in the worktree

This work exists but is **not accepted and not fully verified**. Treat it as a prototype to review against this plan:

- `src/big_blobs_core/`
  - `Blob`, `BlobPin`, `CipherBlob`, `Jwk`, `Representation`, and typed `Rfc8188EncodingParameters { record_size, padding, extra }` datum declarations.
  - `BlobId` with bare base58btc and BLAKE3-multihash spellings.
  - Portable unary protocol: `PutBlob`, `ReadBlob`, `StatBlob`, with capability on every request.
  - Cabinet admission fix for generated numeric bounds needed by `Blob.inline`; scoped core/Cabinet tests were reported green by the agent that made that change.
- `src/big_blobs/`
  - `service.rs`: capability-checked put/read/stat over an iroh-blobs store; exclusive filesystem owner lock; durable availability frontier.
  - `documents.rs`: per-document reconciliation of Blob/CipherBlob/JWK datums, deterministic representation creation/reuse, pinned-head key resolution, deferrals instead of silent drops.
  - `projection.rs`: blob-specific digest → route projection over `datum_set`, with durable parking for unavailable reads.
  - `workers.rs`, `work_queue.rs`, `retention.rs`: durable per-document work, retention ownership/effects, inventory part reconciliation.
  - `permissions.rs`: Keyhive access-delta ingress into inventory part membership. Known to be incomplete: it does not yet seed all known parts from live state before opening the stream.
  - `sync.rs`: Big Sync backend for representation downloads and virtual-possession checks. Its tests have not run because compilation is blocked elsewhere.
  - `schema.rs`: blob schema registration group for `SchemaRepo::with_groups`.
- `src/big_node/`
  - Boots Authority, BigRepo, blob service, indexes, DrawerAccess, DocsRepo, DrawerService, and blob workers in reverse-shutdown order.
  - Creates a dedicated single-group inventory drawer/document and a dedicated `big-blobs-v1` part-store scope.
  - Adds node-level tests for capability scoping/revocation and single-owner filesystem reopen.

Known blockers in the prototype:

- Integrated `big_blobs`/`big_node` compilation currently fails (for example, generic type inference in `permissions.rs`); most native blob tests have therefore never executed.
- `held:` tags are shared between service uploads and sync imports. Neither side can safely delete the tag when its own reference disappears without an owner-aware retention ledger, and a check-then-delete races with new references.
- Pin-only virtual representations: keeping `pt:<C>` preserves the plaintext dependency, but after the owning cipher datum/key becomes unavailable there may be no accepted route from which to rebuild the provider's key binding after restart.
- Inventory permission seeding must happen from live Keyhive state for all known inventory parts before the access stream opens; the prototype only reseeds documents as work runs.
- Guardrails/API snapshots, legacy stale exemptions, and the final crate policy files are not finished.

## 4. Target architecture

### 4.1 Crate boundaries

| Crate | Owns | Must not own |
| --- | --- | --- |
| `big_blobs_core` | Datum declarations (`Blob`, `BlobPin`, `CipherBlob`, `Jwk`), `BlobId`, portable protocol DTOs/errors | Native storage, iroh types in public APIs, filesystem paths, key material handling beyond datum shape |
| `big_blobs` | Native blob service, iroh store/provider, encryption codec, document adapter, projection, workers, retention, sync backend | Drawer/roster policy, generic datum indexing, Daybook plug/runtime concerns |
| `big_cabinet` | Generic Docs/Drawer/Authority/schema/index machinery | Blob, CipherBlob, inventory, encryption, or representation semantics |
| `big_node` | Composition: boot order, namespaces/actors, inventory drawer/doc creation, part-store scope, shutdown order | Blob business logic beyond wiring and embedder policy |
| `daybook_core` (during transition) | Legacy consumers until migrated | New blob features; new imports of legacy blob internals |

Dependencies point inward toward the portable core. No compatibility shims: when an interface moves, update callers and delete the old path.

### 4.2 Service surface

Portable protocol (for cross-process/Wasm-shaped consumption):

- `PutBlob { capability, bytes } -> BlobInfo`
- `ReadBlob { capability, blob_id, offset, length } -> BlobRange`
- `StatBlob { capability, blob_id } -> Option<BlobInfo>`

Rules:

- Every request carries exactly one domain capability. The service inspects it and requires: issuer = the blob namespace, `Rights::USE`, and the exact object (`big_blobs/put/v1` for put; `big_blobs/read/v1/<blob_id>` for read/stat).
- Bounds are explicit: portable put and read are capped; large content uses native streaming import, not ever-larger unary payloads.
- Errors distinguish context failure, denial, not-found, invalid input, and storage failure. A revoked or wrong-object capability must fail closed.
- The native service additionally exposes streaming import, local availability (long-polled from a cursor per [ADR 023](../adrs/023-cursor-consumption-is-long-poll.md), not pushed), and store/provider handles to trusted in-process embedders. Those are not portable protocol operations and must not become a remote master-key/path API by accident.

Open design question: does the portable surface need a streaming/chunked transfer operation now, or is unary-small + native-streaming sufficient for this PR? Recommendation: keep unary small in this PR; design chunked transfer separately if a real cross-process large-blob consumer appears.

### 4.3 Datum model

- `Blob { mime, lengthOctets, digest, inline?, urls?, extra }` — logical content identity is `digest` (plaintext).
- `BlobPin { lengthOctets, extra }` — datum key instance is the representation digest; the value carries length only.
- `CipherBlob { representation { digest, lengthOctets }, contentEncoding, keyRef, keyRefHeads, encodingParameters, extra }` — one encrypted representation.
- `Jwk { kty, ...members }` — valid JWK members are carried verbatim; debug output redacts the value.
- `encodingParameters` is typed per encoding. For RFC 8188: `{ recordSize, padding, extra }`. Unknown encoding tokens defer/unsupported before parameter decoding; unknown members inside a known encoding's parameters are preserved, not fed to a strict legacy deserializer that would drop them.

Datum keys, not values, name BlobPins; Blob values, not keys, name plaintext digests. Both directions were bugs in earlier designs and need regression tests.

### 4.4 Workers and data flow

```text
datum_set routes ──► BlobProjection ──► digest → DatumRoute lookup
      │                                        │
      ├──────────────► DocumentBlobs reconcile ┤
      │                  (signed Docs reads)   │
availability ─────────► re-arm parked work     │
drawer policy ────────► re-arm parked work     │
Keyhive access deltas ─► inventory part access │
                                               ▼
                              Retention ledger + effects
                                               │
                        inventory BlobPins ◄───┴───► Big Sync parts
                                               │
                                     BlobSyncBackend (bytes)
```

Worker responsibilities:

1. **Projection**: maintain digest → routes from accepted Blob datums. Unavailable/pending reads park durably and preserve prior rows; absent/rejected removes exactly that route.
2. **Document reconciliation**: for each dirty document, read Blob/CipherBlob/JWK/BlobPin datums through DocsRepo; create or reuse deterministic representations; register virtual pairs; report retained pairs, claims, and deferrals.
3. **Retention**: own the durable (document → representation) ownership ledger and the inventory-effect outbox. Only after ownership commits may inventory pins, part membership, tags, provider registrations, and pair-root ledger rows change.
4. **Permissions**: per-drawer blob parts. Ciphertext part access = Keyhive Read on the drawer's inventory doc (meta-group members, relays included); plaintext part access = Keyhive Read on the docs group. Both are seeded from live Keyhive state before following the ADR 013 access-delta stream ([ADR 026](../adrs/026-drawer-native-blobs.md) §7).
5. **Sync backend**: satisfy Big Sync object tasks by downloading missing representation bytes or proving rooted virtual possession; never export plaintext digests/keys to relays and never serve a representation whose authorization has not been established by the caller/admission layer.

All five share one durable work queue discipline: source revision + document generation are committed atomically, stale completions cannot erase newer work, and parked work is re-armed by the event that could change its outcome (bytes available, key accepted, policy changed, document edited, restart).

**Observation scoping (drawer-native).** See [ADR 025](../adrs/025-cabinet-datum-predicates-and-polling.md) and [ADR 026](../adrs/026-drawer-native-blobs.md) §9: workers long-poll member docs' datums through `PollDatums` with an `InDrawer` clause, from a cursor they persist. Roster changes arrive in the same cursor, because the drawer projection is fed from the datum-set frontier. Group parts and `WorkerGroupScope` are never the gate. The repo-wide `automerge_frontier_group_scope` stays untouched.

### 4.5 Retention and GC ownership

This is the design center of the PR; get it explicitly right.

Reference classes that can keep bytes alive:

- document-owned representations (Blob + CipherBlob pairs reconciled from a document);
- inventory BlobPins (including pins authored by other documents and relay sponsorship);
- in-flight or not-yet-adopted uploads/downloads (`held:` today);
- remote part memberships that caused a local download;
- pair-root ledger rows (pre-claim window only).

Required properties:

- One owner-aware ledger (or equivalent refcount with durable ownership records), not independent tag deletion by each subsystem.
- Adoption/release are idempotent and ordered: durable ownership is recorded before tags are dropped; release removes tags only when no ownership class remains.
- Removing a document claim never releases bytes still pinned, uploaded, or remotely retained.
- Virtual representations need an explicit restart story: either their provider binding can be rebuilt from accepted datums, or they are materialized before the binding source disappears, or the pin is not allowed to keep them alive. Pick one and test it; silently keeping `pt:` is not a complete answer.
- Orphaned uploads need a policy (adopted by a document, explicitly released, or expired by a stated rule). A permanent leak by default is not acceptable, but neither is deleting bytes a concurrent claim is about to adopt.

### 4.6 Node lifecycle

`big_node` boot order (reverse on shutdown):

1. Authority
2. BigRepo
3. Blob service (owns the store owner-lock for its whole lifetime)
4. Generic indexes (`datum_set`, `datum_ref`)
5. DrawerAccess + DocsRepo + DrawerService
6. Blob workers (projection, document/retention loop, permission ingress)
7. Embedder sync/peers

Blob workers stop before Docs/Drawers/BigRepo/store. Filesystem blob storage has exactly one owner process; a second boot must fail with a clear “already has an owner” error and succeed again after clean shutdown.

## 5. Scope

### 5.1 In scope for this PR

- [ ] `big_blobs_core`: datum declarations, `BlobId`, portable protocol, schema-admission compatibility.
- [ ] Move encryption codec, virtual provider, download/store flows, and pair-root ledger from `daybook_core` into `big_blobs` without changing wire format or salt derivation; update legacy callers; delete old paths.
- [ ] Native blob service with capability authorization, bounded portable put/read/stat, streaming import, durable availability, and single-owner filesystem storage.
- [ ] Blob-specific projection over Cabinet `datum_set` replacing the Blob-digest behavior embedded in legacy `facet_set`.
- [ ] Document reconciliation worker (Blob/CipherBlob/JWK/BlobPin), deterministic representation creation/reuse, key resolution at pinned heads, deferral/parking semantics.
- [ ] Retention ledger/effects, inventory maintenance, Big Sync part projection, and inventory part permission mirroring (with live seeding before stream open).
- [ ] Representation sync backend: verified downloads, virtual-possession terminal state, no plaintext/key leakage.
- [ ] `big_node` composition, inventory drawer/document bootstrap, dedicated part-store scope, lifecycle/shutdown ordering.
- [ ] Focused tests per phase (§7), Clippy, formatting, guardrails/API updates, and removal of stale legacy exemptions for moved files.

### 5.2 Out of scope

- Migrating all Daybook drawers/facets/plugs/runtime/UI to Cabinet, or deleting `daybook_core` blob consumers that still legitimately use the moved crypto modules through their new path.
- A full Daybook data migration or on-disk format migration for existing user repos.
- New encryption algorithms, salt-derivation changes, or ciphertext format changes.
- Public relay products: quotas, customer acceptance workflows, billing, retention receipts (ADR 005 territory).
- Cross-document `keyRef` UX, domain-wide key scopes, or key-document UX beyond what reconciliation needs.
- Browser/Wasm blob service hosting, object-store/S3 blob backends, and chunked portable transfer protocol (unless §4.2's open question is answered differently).
- Roster sharding, subset drawers, and public/XRPC indices (already deferred in ADR 019).

### 5.3 Design critiques to consider while implementing

- The legacy system spreads one concern (“keep these bytes”) across `BlobsRepo` metadata sidecars, iroh tags, presence parts, inventory docs, part membership, and worker-local SQL. The new design should have one retention ledger that the others project from.
- Legacy `get_path`-style APIs materialize files implicitly and can trigger downloads on a read path. The new service should make byte acquisition explicit (stat/read/import/sync task), not a side effect of asking where a file would be.
- The old facet-set index mixes generic routing with Blob value semantics. Keep Cabinet generic; blob semantics belong in `big_blobs`.
- Capability objects are exact (`read/v1/<id>`), which is safe but chatty for bulk consumers. If an embedder needs many reads, prefer minting per-object caps in batch at the domain layer over widening the object grammar silently.
- Deterministic encryption intentionally leaks plaintext equality within one key/framing scope. Document/key scope choices (per-document keys by default) bound that leak; do not widen scope for convenience without recording the tradeoff.

## 6. Implementation plan

Each phase lands compiling and tested on its own; do not stack unverified phases.

### Phase 0 — Freeze the contract
- Review and amend this document; resolve §0 questions and §4.5 retention ownership.
- Record any decision that changes ADR 001/003 semantics as an ADR amendment, not a code comment.
- Exit: no open “who owns release?” or “how does a pin-only virtual blob survive restart?” questions.

### Phase 1 — Portable core
- Finalize datum declarations, typed encoding parameters, `BlobId`, protocol DTOs/errors and bounds.
- Keep generated-schema admission compatible (byte bounds) without weakening validation.
- Exit: `big_blobs_core` + affected Cabinet datum tests and Clippy pass.

### Phase 2 — Mechanical crypto move
- Move encryption modules and pair-root ledger unchanged in behavior; update `daybook_core` callers; remove stale guardrail exemptions.
- Exit: moved crypto tests pass; legacy consumers compile; no old import paths remain.

### Phase 3 — Native service
- Store boot (memory + filesystem single-owner), provider registration, capability-checked put/read/stat, streaming import, durable availability frontier.
- Exit: authorization, bounds, revocation, owner-lock/reopen tests pass.

### Phase 4 — Projection and document workers
- Blob digest projection over `datum_set`; durable parking/rearm; signed-read reconciliation; deterministic create/reuse; pinned-head key resolution.
- Exit: ordering tests (doc-before-bytes, bytes-before-doc, key-after-cipher, edit/delete/restart) pass.

### Phase 5 — Retention, inventory, permissions
- Owner-aware retention ledger and effects outbox; inventory pin maintenance; part projection; Keyhive permission seeding + delta ingress.
- Exit: grant/revoke/relay/pin-removal/restart tests pass; no release while any ownership class remains.

### Phase 6 — Sync backend
- Big Sync backend over the retention ledger; verified download completion; virtual possession without export; membership-removal release through the same ledger.
- Exit: two-node download, corruption/retry, virtual no-export, and membership-removal tests pass.

### Phase 7 — Node integration and hardening
- `big_node` wiring, inventory bootstrap, shutdown ordering, guardrails/API snapshots, formatting, documentation links.
- Exit: node end-to-end (put → document claim → inventory pin → second node fetch → revoke → release) passes; focused Clippy/tests green.

## 7. Verification matrix

Use the repo's pinned wrapper (`nix develop .#ci-rust --command ./x/agent-build.ts -- …`), prefer nextest, and keep runs focused per phase — no full-workspace suite as a substitute for targeted proof.

| Area | Required evidence |
| --- | --- |
| Core datums/protocol | Declaration/admission tests; unknown-member preservation; serde spelling tests for both digest forms |
| Crypto move | Existing encryption tests unchanged and passing; salt/framing byte-compatibility vectors |
| Service | Exact-object capability allow/deny; wrong issuer/rights; revocation; put/read bounds; second-owner failure; reopen after shutdown |
| Projection | Digest from Blob value (not key); parked read preserves rows; rearm on availability/policy/restart; removal only on absent/rejected |
| Documents | Create/reuse determinism; key at pinned heads; unknown encoding defers before key read; ledger survives success until retention commit |
| Retention | Multi-owner release ordering; pin-only virtual restart behavior (per §4.5 decision); upload adoption/release; no tag deletion while owned |
| Permissions | Live seed before stream; grant/revoke/group/Relay updates; denied read clears membership; restart recovery |
| Sync | Verified download; corrupted download retries/fails closed; virtual representation completes without byte export; membership removal releases through ledger |
| Node | Boot/shutdown ordering; inventory bootstrap idempotence; filesystem reopen with bytes intact |

## 8. Risks and open questions

1. **Retention ownership (blocking).** Shared `held:` tags cannot be released safely per-subsystem. Needs the §4.5 ledger before sync/service release paths are finalized.
2. **Pin-only virtual restart (blocking).** What rebuilds a provider binding when the cipher/key document path is gone? Options: rebuild from any accepted claim, materialize on last-claim loss, or refuse pins for virtual-only representations.
3. **Permission seeding completeness.** Seeding from DrawerService reach must cover every known inventory part, including documents with no current pins, before the access stream opens; otherwise archive-floor gaps can strand stale access rows.
4. **Availability truthfulness.** A virtual entry whose provider cannot currently serve must not be announced as completely available to sync peers.
5. **Portable streaming.** Unary caps push large transfers to native APIs; confirm no planned cross-process consumer needs large portable transfers in this PR.
6. **Inventory drawer shape.** An inventory is a separate doc owned by the drawer's meta group. The metadata doc carries only a `drawer.metaDoc` reference to it. See [the design log](./big-blobs-drawer-design.md) §4; Cabinet prerequisites are in §12.
7. **Legacy coexistence.** Both stacks may root the same bytes during transition. Confirm GC/tag namespaces cannot delete bytes the other stack still owns (`ct:`/`pt:`/`pin:`/`held:` ownership must be unambiguous).

## 9. Worktree note

The current worktree contains the exploratory implementation described in §3.3 plus this plan. Nothing in §3.3 should be treated as merged, reviewed, or verified beyond the scoped results stated there. Implementation resumes only after §0 is answered; when it does, work proceeds phase-by-phase in §6 and this document gets updated with outcomes and links to the final checks.
