# big_blobs replacement — scope and plan

Status: Active implementation plan; the drawer-native correction phases and their acceptance gates are in big-blobs-drawer-design.md §17
Owner: operator + big_blobs track
Updated: 2026-11-03
Scope: Replace the blob subsystem in `src/daybook_core/blobs.rs` and its blob-specific index/worker glue with a Cabinet-style `big_blobs` service and workers. This document is the PR plan and design-review record; it is not evidence that the exploratory code in the worktree is complete or accepted.

## 0. Decision requested

The drawer-native metadata and mirroring decisions are accepted under ADR 026's
2026-11 amendment. The original service/retention review questions below remain a
record of boundaries to resolve explicitly where still open, not a blanket pause on
Phase A or permission to override the active §17 handoff:

1. Whether `big_blobs` is a Cabinet-style service crate consumed through `big_node`, with legacy Daybook left running until a later cutover, rather than a wholesale Daybook migration in this PR.
2. Whether the portable service surface stays deliberately small (`put` / bounded `read` / `stat`) while large transfers, filesystem references, materialization, and sync remain native/embedder concerns.
3. Whether blob-specific indexing belongs in `big_blobs` as a projection over Cabinet's generic `datum_set`, rather than adding blob semantics back into `big_cabinet`.
4. Representation retention is owned by the serialized durable byte-ownership ledger, with independent document, inventory and native-import claims (§4.5), rather than shared `held:` tags.
5. Pin-only virtual ciphertext remains servable after its source document/key path becomes unavailable: a private local SQL binding retains the accepted key, framing, source and C→P dependency while ownership remains (§4.5). This binding is secret local state, never inventory metadata.

If any of those answers differ, change this plan first; do not let the exploratory implementation silently decide them.

### 0.1 Drawer-native design

Superseded content was removed from this section. The drawer-native design is maintained in [big-blobs-drawer-design.md](./big-blobs-drawer-design.md) (design log: settled/proposed/open items, corrections, flags), and its settled parts are now ADRs:

- [ADR 023](../adrs/023-cursor-consumption-is-long-poll.md): cursor consumption is long polling.
- [ADR 024](../adrs/024-indexes-are-fed-by-the-change-stream.md): indexes are fed only by the change stream; the drawer projection is frontier-fed; datum_ref edges fold into the datum-set worker.
- [ADR 025](../adrs/025-cabinet-datum-predicates-and-polling.md): the Cabinet datum predicate language and `PollDatums`.
- [ADR 026](../adrs/026-drawer-native-blobs.md): drawer-native blobs — co-located JWK, per-doc slots, inventories as drawer meta docs, self-doc children, inventory-bound partitions, relay sponsorship walk.

Where this plan, the log and an ADR disagree, the ADR wins, including ADR 026's
authoritative 2026-11 amendment. The log's §§15–17 record the first-cut critique and
correction; §17 is the active phased handoff, superseding §14's pre-cut backlog. Phase A
is generic Cabinet metadata and successor work, not completion of blob phases B–E;
Phase F requester-aware byte authorization is optional hardening.

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
- [ADR 013 — Keyhive access delta stream](../adrs/013-keyhive-access-delta-stream.md) (**Status: Proposed**): access changes are durable wake-ups; consumers seed inventory parts from live inventory-document authority before opening streams so archive-floor gaps are safe. The legacy `daybook_core` permission writer and `big_blobs/permissions.rs` consume this stream; Cabinet-side admission wakes off the authority stream (ADR 015 §3) instead, merged with the datum frontier as in `doc/observe.rs`.
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

### 3.3 Historical exploratory implementation snapshot

This subsection records the pre-correction prototype review, not current implementation
or validation results. It does not establish completion of any §17 gate:

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

Blockers recorded at that snapshot (not revalidated here):

- Integrated `big_blobs`/`big_node` compilation was reported failing (for example, generic type inference in `permissions.rs`); most native blob tests had therefore never executed.
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
4. **Permissions**: one `inventory_part(inventory_doc)` per inventory, for opaque digest-addressed bytes regardless of representation. Seed each known writer-target or mirrored inventory part from live Keyhive standing on that exact inventory document, including empty inventories, before following ADR 013's access-delta stream. Read+ means Read, Write/Edit, or Admin on that doc. Relay drawer paths obtain Read on public metadata inventories and no sensitive metadata membership; docs-group access never seeds a blob part ([ADR 026](../adrs/026-drawer-native-blobs.md) §7). Removing the drawer-keyed plaintext part is Phase B.
5. **Sync backend**: download missing inventory-named bytes or prove rooted virtual possession only after inventory-specific admission. Transport does not interpret plaintext versus ciphertext. Stock iroh-blobs GETs are hash-only, not requester-aware inventory authorization; the accepted interim plaintext boundary is Keyhive-gated discovery plus strict non-disclosure of sensitive inventory ids, pins, plaintext digests, part membership, and availability. Exact-inventory byte authorization is deferred Phase F hardening, not an implemented second check.

All five share one durable work queue discipline: source revision + document generation are committed atomically, stale completions cannot erase newer work, and parked work is re-armed by the event that could change its outcome (bytes available, key accepted, policy changed, document edited, restart).

**Observation scoping (drawer-native).** See [ADR 025](../adrs/025-cabinet-datum-predicates-and-polling.md) and [ADR 026](../adrs/026-drawer-native-blobs.md) §9: workers long-poll member docs' datums through `PollDatums` with an `InDrawer` clause, from a cursor they persist. Roster changes arrive in the same cursor, because the drawer projection is fed from the datum-set frontier. Group parts and `WorkerGroupScope` are never the gate. The repo-wide `automerge_frontier_group_scope` stays untouched.

### 4.5 Retention and GC ownership

This is the design center of the PR; get it explicitly right.

Reference classes that can keep bytes alive:

- document-owned representations (Blob + CipherBlob pairs reconciled from a document);
- inventory BlobPins (including pins authored by other documents and relay sponsorship);
- not-yet-adopted native imports, each with its own durable staging identity even when the bytes deduplicate;
- remote part memberships that caused a local download;
- pair-root ledger rows (pre-claim window only).

Required properties:

- One owner-aware ledger (or equivalent refcount with durable ownership records), not independent tag deletion by each subsystem.
- Adoption/release are idempotent and ordered: durable ownership is recorded before tags are dropped; release removes tags only when no ownership class remains.
- Removing a document claim never releases bytes still pinned, uploaded, or remotely retained.
- Virtual representations need an explicit restart story: either their provider binding can be rebuilt from accepted datums, or they are materialized before the binding source disappears, or the pin is not allowed to keep them alive. Pick one and test it; silently keeping `pt:` is not a complete answer.
- Native `import_stream` returns `ImportedBlob`. `adopt_import` requires committed document ownership before releasing that import's staging claim; `release_import` abandons only that claim. There is no expiry or Drop-based release. Portable `PutBlob -> BlobInfo` is unchanged and deliberately retains its durable hold: no portable receipt or release protocol is introduced.
- Publication runs inside the byte owner's serialized callback: commit/apply claims before membership and availability, unpublish before final release, and keep physical proof guards alive through external effects. Accepted operations survive caller cancellation; shutdown closes, drains and joins the owner before closing the store.
- Virtual installation prepares ciphertext/outboard once, validates the advertised digest, commits the accepted C/P/key/framing/source binding in private local SQL, then installs the prepared outboard and publishes provider/tags. Boot restores retained providers before ownership recovery. Final release removes provider, tags, dependency and provenance; an outboard without physical bytes or a live provider is not availability.

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
- Follow drawer-design §17 phases B–D: inventory-only parts, durable `(inventory, representation)` state, both arrival orderings, and transitive mirror discovery. Self-doc children use their immediate grouped parents' inventories; standalone companions and self-doc pins are withdrawn.
- Plaintext inventory discovery is Keyhive-gated through the sensitive root. The final hash fetch relies on non-disclosure of plaintext digests under drawer-design §17.3's accepted interim threat model.
- Exit: the §17.5–§17.8 verification gates pass, including the real two-node A→B→C ciphertext mirror test; no release occurs while any byte-owning claim remains.

### Phase 6 — Sync backend
- Big Sync backend over the retention ledger; verified download completion; virtual possession without export; membership-removal release through the same ledger.
- Stock iroh-blobs is storage/transport machinery, not an inventory authorization boundary. The accepted interim boundary is Keyhive-gated discovery plus strict non-disclosure of sensitive inventory ids, pins, and plaintext digests (§17.3).
- Exit: ciphertext and authorized-plaintext mirror download/corruption/retry/virtual tests pass. Exact-inventory requester-aware byte authorization is a future defense-in-depth phase (§17.9), not a current plaintext feature gate.

### Phase 7 — Node integration and hardening
- `big_node` wiring, inventory bootstrap, shutdown ordering, guardrails/API snapshots, formatting, documentation links.
- Exit: node end-to-end (put → document claim → inventory pin → second node fetch → revoke → release) passes; focused Clippy/tests green.

## 7. Verification matrix

Use the repo's default devshell and pinned wrapper (`nix develop --command ./x/agent-build.ts -- …`), prefer nextest, and keep runs focused per phase — no full-workspace suite as a substitute for targeted proof.

| Area | Required evidence |
| --- | --- |
| Core datums/protocol | Declaration/admission tests; unknown-member preservation; serde spelling tests for both digest forms |
| Crypto move | Existing encryption tests unchanged and passing; salt/framing byte-compatibility vectors |
| Service | Exact-object capability allow/deny; wrong issuer/rights; revocation; put/read bounds; second-owner failure; reopen after shutdown |
| Projection | Digest from Blob value (not key); parked read preserves rows; rearm on availability/policy/restart; removal only on absent/rejected |
| Documents | Create/reuse determinism; key at pinned heads; unknown encoding defers before key read; ledger survives success until retention commit |
| Retention | Multi-owner release ordering; pin-only virtual restart behavior (per §4.5 decision); upload adoption/release; no tag deletion while owned |
| Permissions | Inventory-only part seeding; grant/revoke/group/Relay updates; no public route to sensitive inventories; restart recovery; both arrival orderings and no cross-inventory unlocking |
| Sync | Real two-node A→B→C ciphertext coverage; verified download; corruption fails closed; virtual representation completes without export; membership removal releases through ledger; plaintext relies on sensitive-inventory non-disclosure; exact-inventory transfer authorization is deferred hardening |
| Node | Boot/shutdown ordering; inventory bootstrap idempotence; filesystem reopen with bytes intact |

## 8. Risks and open questions

1. **Retention ownership (resolved for the native stack).** The durable byte owner serializes claims, release and publication (§4.5). Legacy Daybook remains a separate stack until its consumers migrate.
2. **Pin-only virtual restart (resolved).** Private local SQL provenance restores byte-identical serving before ownership recovery, and final release removes that provenance (§4.5). Source-document availability is not required to keep an independently owned virtual representation servable.
3. **Permission seeding completeness.** Seed every known writer-target or mirrored inventory part from that inventory document's live Keyhive standing, including inventories with no pins, before opening the access stream. Drawer/docs-group reach is not a substitute; archive-floor gaps must not strand stale access rows.
4. **Availability truthfulness.** A virtual entry whose provider cannot currently serve must not be announced as completely available to sync peers.
5. **Portable streaming.** Unary caps push large transfers to native APIs; confirm no planned cross-process consumer needs large portable transfers in this PR.
6. **Inventory metadata and self-doc phase boundary.** A grouped drawer has three groups and two metadata roots. A separate inventory doc is referenced by `drawer.metaDoc/blobInventory/<name>` on the selected root and backed by that root group's Admin delegation. Cabinet carries root, kind, name, target doc id, and delegation, with no `MetaRepresentation` or blob-specific admission checks. Writer/mirror policy owns representation; optional inventory config is opaque and representation-independent. `CreateMetaDoc` refuses self-doc drawers: they have no metadata documents, companion inventories, or inventory pins. Every immediate containing grouped drawer inventories its self-doc children's blobs. Parentless self-doc inventory mirroring is outside scope. See [the design log](./big-blobs-drawer-design.md) §§17.1–17.4 and §17.8.
7. **Legacy coexistence.** Both stacks may root the same bytes during transition. Confirm GC/tag namespaces cannot delete bytes the other stack still owns (`ct:`/`pt:`/`pin:`/`held:` ownership must be unambiguous).

## 9. Worktree note

§3.3 records the exploratory prototype, not current acceptance evidence or a claim
that its historical blockers still reproduce. Continue under ADR 026's 2026-11 amendment
and the design log's §17 phase order and gates; the older §6 sequence covers the broader
replacement, not an alternative drawer-native contract. Record unresolved §0/§8 product
questions rather than inventing compatibility shims, and update outcomes only with the
exact checks actually run.
