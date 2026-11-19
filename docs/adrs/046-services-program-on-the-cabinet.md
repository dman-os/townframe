# ADR 046: Services program on the cabinet, not on big_repo

**Status:** Proposed (manager, 2026-11-12), from the operator's #84 review (ledger D46). Every claim about existing code cites the survey in D46. Points marked *to verify* are assumptions the implementing lane checks first.

**Related:**
- [ADR 019: cabinet drawers and selective mirroring](019-cabinet-drawers-and-selective-mirroring.md)
- [ADR 023: cursor consumption is long-poll](023-cursor-consumption-is-long-poll.md)
- [ADR 025: cabinet datum predicates and polling](025-cabinet-datum-predicates-and-polling.md)
- [ADR 026: drawer-native blobs](026-drawer-native-blobs.md)
- [ADR 033: what a node is](033-what-a-node-is.md)

## Context

The cabinet (`big_cabinet`) is the layer services are meant to program on: documents, datums, drawers, authority. Several crates outside it reach into `big_repo` directly:
- **big_blobs** reads documents through big_repo's Automerge frontier store (`workers.rs:332,435`). It builds its own keyhive access-delta walker over `repo.database()` (`permissions.rs:64-138`), asks keyhive for access (`admission/project.rs:26`), and holds `DocInterest` leases (`admission/observe.rs:89,108`).
- **big_node's peers** builds the same access-delta walker again (`peers/access.rs:56-87`) and asks keyhive which groups and docs it holds (`peers/policy.rs:122`).
- **day_cli and big_ndk_testing** name `big_repo::Config` only to boot a node.

The operator calls this an anti-pattern (D46). If services can only do their work by going around the cabinet, the cabinet isn't yet enough to program on. It also blocks the multi-process goal: `repo.keyhive()`, `repo.database()` and the frontier store are in-process handles, so none of these services could run in their own address space, such as a wasm guest.

## Decision

### 1. The rule

Only the cabinet and the node's composition root (`big_node`'s boot) depend on `big_repo`. Every other service uses cabinet methods: the docs, drawers and authority services, called through their typed clients. Sync and storage plumbing that sits below the cabinet uses `big_sync` interfaces, not `big_repo` types (§5).

### 2. Document changes: `PollDatums`, not the frontier store

big_blobs watches the frontier store so it can rescan a document's blob references when its content changes. Blob references are `org.example.daybook.blob` datums (ADR 026), so the cabinet already answers that question: `PollDatums { predicate: HasTag { tag: blob }, … }` delivers every document carrying a blob datum, with move-ins and move-outs, behind a durable cursor (ADR 023, ADR 025). big_blobs keeps that cursor, the way any consumer of the change stream does, and drops the frontier store.

*To verify:* that a node-wide `HasTag` poll is cheap enough for the blob worker's volume (index-backed, per ADR 024), and that the rows carry what the worker needs (doc id, the blob datums, their versions). If a field is missing, the poll grows that field; no new method.

**Relays.** A relay holds ciphertext it can't read, and `PollDatums` requires Read access. What a relay retains is decided by sync parts and sponsorship, below the cabinet (§5), not by reading documents. This ADR doesn't change that.

### 3. Document readiness: `observeDocAvailability`, with demand

big_blobs takes a `DocInterest` lease so a document is fetched, then waits for it to become usable. The docs service already streams `observeDocAvailability` (`LocalDocUsability`: not observed, no local content, pending materialization, usable, plus `syncDemand`).

*To verify:* whether observing a document creates sync demand (that's what the lease does). If it doesn't, the request gains `demand: bool`, and an open subscription with demand is the lease: dropping it releases the demand. That's ADR 023's subscription-scoped lifetime, not a new lease object.

### 4. Memberships: a drawers subscription

big_blobs and big_node's peers both need to hear when access changes. Today each builds its own keyhive walker. The drawers service gains:
- `drawers.observeMemberships { capability, after } → stream MembershipChange { drawer, principal, access }`, with a durable cursor. One walker in the cabinet replaces the two copies.
- `drawers.listMemberships { capability, principal } → { drawer: access }`, which answers "what do I hold" (`peers/policy.rs:122`).
- `drawers.getAccess { capability, principal, drawer }`, which answers "can this node read this doc's drawer" (`admission/project.rs:26`).

These speak drawers and principals, not keyhive groups. The one place that genuinely needs keyhive group ids, sync-part computation in big_node's peers (`peers.rs:839`), is plumbing and stays below the cabinet (§5).

*To verify:* that big_blobs' permission check can be phrased per drawer rather than per keyhive document. If it can't, the subscription carries doc-level changes too.

### 5. Below the cabinet: big_sync interfaces

Blob part bookkeeping (`retention.rs`, `admission/runtime.rs`, `workers.rs:134,145`, `blob_boot.rs:65`) and peer sync transport (`peers.rs`) are sync plumbing, and stay outside the cabinet. They depend on a `big_sync` part-store interface rather than `big_repo`'s concrete `SqlitePartStore`, so the blob service links `big_sync_core`, not `big_repo`.

### 6. The node's identity and boot

- The local peer id comes from `getNodeInfo` (provider `builtin:node`), which already reports it.
- Node boot takes a `big_node` storage config, so `day_cli` and `big_ndk_testing` stop naming `big_repo::Config`.

## Consequences

- big_blobs loses its `big_repo` dependency (tests included, once they boot through `big_node`). big_node's peers keeps it only for the sync plumbing in §5, behind the composition root.
- The duplicated access-delta walkers become one cabinet subscription.
- New drawers methods: `observeMemberships`, `listMemberships`, `getAccess`. They're ordinary methods, so a service in a separate address space can call them over XRPC.
- A guardrails rule enforces §1: `big_repo` is an allowed dependency only for `big_cabinet*` and `big_node`.

## Open questions

1. Should `getAccess`/`listMemberships` live on the drawers service or the authority service? The proposal is drawers, because they speak drawers.
2. A relay's retention is decided below the cabinet (§2). Is that acceptable long-term, or should relays get a ciphertext-level cabinet surface of their own?
