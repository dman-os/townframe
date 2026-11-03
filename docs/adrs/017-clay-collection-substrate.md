# ADR 017: Clay collection waist — leased partial replication

**Status:** Proposed. This record withdraws the original `attach` / `demand` /
`release` relay design and replaces the later subscription/hydration draft. The
requirements are decided; the wire protocol remains proposed until the validation
criteria below pass through an actual component boundary.

**Amends:** ADR 014's live-query and optimistic-state requirements and ADR 015's
collection boundary. ADR 023 defines capability references carried by collection
values. ADR 025 defines manifest-declared collections, durable registration scopes, and lazy capability attachments.

## Context

Clay needs one small host–guest data protocol from which unrelated guest libraries
can build rich local collection APIs. A Rust MDK is one consumer, not the definition
of that protocol. Future guests may use TypeScript, React, TanStack DB, signals, or a
library we have not designed.

The two proving workloads are:

1. node-backed projected datum state, where the host acquires selective data from a
   node and maintains shared optimistic predictions; and
2. micro composition, where several micros contribute transient state that is
   combined for consumers and disappears when its owner closes.

The protocol crosses address spaces and therefore cannot assume Rust closures,
borrowed values, a shared heap, or one UI framework. Copying every source row into
every guest is also not acceptable as the permanent model. Each guest instead owns a
partial replica containing only projections it has acquired.

The previous design made every participant a relay with `attach`, opaque demand
messages, eager `Batch` broadcast, coverage lists, and generic whole-row writes. In
the implementation, demand did not narrow delivery and releasing it did not stop the
attached relay. The design also mixed four different concerns:

- replication of authoritative current state;
- local guest query evaluation;
- source-specific mutation semantics; and
- shared optimistic predictions.

This ADR keeps the waist narrow: it replicates selected current state. Collection
features and guest libraries build richer behavior above it.

## Decision

### 1. The waist replicates current-state projections

A collection source exposes keyed projected current state. It is not a history API,
event bus, CRDT change transport, blob transport, or persistence layer.

A manifest-declared collection is a logical host facility independent of the declaring micro's activation. Once its plug definition is admitted, an untouched collection still exists and bootstraps as known-empty. Volatile, durable, and node-backed materializations share this read waist; backing and registration-scope lifetime are specified by ADR 025 rather than exposed through pages.

A consumer acquires an immutable source-defined selector and receives a projection
lease:

```text
reader.acquire(selector, resume?) -> projection-lease

projection-lease.read(committed?, limits, poll-timeout) -> page

drop(projection-lease) -> release host and upstream interest
```

`committed` is absent on the first read and is the `through` cursor of the last page the guest applied thereafter. A resume cursor supplied to `acquire` establishes the lease's starting boundary; the first read still passes no committed cursor. Cursors are scoped to the selector and projection generation and cannot be transferred to another lease except as that selector's best-effort resume token. A lease permits one in-flight read. Retrying from the same committed cursor returns transitions after that cursor, possibly with different page boundaries, and dropping the lease cancels its pending read.

Acquiring a lease is also preloading. The host knows the selection remains mounted
even between reads, so it can retain the corresponding node query, index projection,
or host materialization. Dropping the lease releases that interest. A lease is not a
pushed stream: the guest controls consumption with bounded long polls, and the source
does not queue data pages for an idle consumer.

The selector is opaque to the waist and typed by the collection contract. Examples
include a datum query, a toolbar surface, or the identity selector for a small
collection. Selectors are immutable for a lease; changing one acquires a replacement
lease.

### 2. Source selectors and guest-local queries are different

A **selector** controls partial replication across the host–guest boundary. A
**query** runs over the guest's partial replica.

A collection-specific package defines a typed, data-shaped query IR and two
interpretations:

```text
typed query IR
    ├── encoded selector transported by the waist
    ├── host planner -> node, index, SQL, or materialized-source acquisition
    └── guest evaluator -> local filter, grouping, ordering, and derivation
```

The host and guest implementations obey the same logical contract and share
conformance fixtures. They need not share one language or executable closure. The
waist does not standardize a universal query language.

Selector evaluation is a deterministic, read-only predicate over the selector, key,
and value. In-process source adapters must not re-enter projection operations from
that predicate or acquire locks that can invert projection locking. Upstream interest
acquisition belongs in the separate source acquisition operation, which runs outside
projection locks.

A source may safely overfetch for ordinary filtering, and the guest evaluator may
narrow the result. Authorization never overfetchs: unauthorized keys, values, pending
state, and capability references are filtered before crossing the boundary.

### 3. Pages carry atomic visible-state transitions

A page contains zero or more source transactions:

```text
Page {
    transactions: [
        Transaction {
            changes: [
                Upsert {
                    key,
                    row_version,
                    value,
                    evidence,
                    write_context?,
                }
              | Remove {
                    key,
                    row_version,
                }
            ]
        }
    ],
    through: Cursor,
    bootstrap_complete: bool,
}
```

Values are inline in the first protocol. Listing plus exact-version hydration,
cross-query body caches, and Zero-style client view records are optimizations for a
later protocol revision. The first version does not require historical body retention
or a second hydration round trip.

Rules:

- A transaction is applied atomically to the guest replica and is never split across
  pages. A transaction may exceed a requested soft page-size limit.
- A page may contain several transactions.
- An empty page may advance `through` when source work produced no matching rows.
- A long-poll timeout is a successful empty page, not an error.
- The guest advances its committed cursor only after applying the page.
- Reading has no acknowledgement side effect. Supplying the committed cursor on the
  next read is sufficient pull-driven flow control.

`row_version` identifies one visible version of a key within the projection. The host
projection assigns it; domain ordering such as Automerge heads does not enter this
field. `Cursor` orders projection transactions and is opaque to guests and generic
collection code.

### 4. Bootstrap completion is explicit

Acquiring a lease captures an opening boundary. `bootstrap_complete` means every row
matching the selector at that boundary has been represented through this page. It is
per projection, not a claim that the source, node, or application is globally
synchronized.

```text
opening boundary = 100
page through 60  -> bootstrap_complete = false
page through 100 -> bootstrap_complete = true
page through 101 -> bootstrap_complete = true
```

Later changes do not make acknowledged bootstrap progress incomplete again. A retry
with different limits describes its own returned boundary: an unacknowledged complete
page does not make a shorter retry complete. Merely delivering or cancelling a read
never acknowledges it. The marker distinguishes an empty authoritative result from a
projection whose host-side source is still loading. A guest library may render pages
progressively, retain cached rows while showing loading evidence, or wait for
completion; the protocol does not prescribe UI policy.

Every collection source must be able to bootstrap current state. A live-only event
stream is not a Clay collection.

### 5. Resume is best effort and reset is simple

Guest libraries normally hide cursors. When reacquiring the same selector, a guest
may offer its last committed opaque cursor. The host either resumes or reports
`reset-required` because history was compacted, the host restarted, the projection
implementation changed, or the token is invalid for that selector.

Runtime-neutral in-process projections require a caller-supplied `[u64; 2]` runtime
epoch that is fresh across runtime incarnations between which cursors can travel.
`Projection::new(runtime_epoch)`, `Projection::with_source(source, replay_capacity,
runtime_epoch)`, and `RegistrationCollection::new(runtime_epoch)` require it; none has
a default constructor. Cursors retain both epoch words alongside a checked
process-local source counter. Deterministic platform hash seeds are not restart-safe
source identities. The projection does not acquire entropy or promise durable resume
across restarts.

The first protocol responds to reset by clearing that local projection and
bootstrapping it again. It does not reconcile rows across projection generations.
Reset is expected to be rare and must be safe rather than clever.

Overlapping leases have independent guest-side result sets in the first
implementation. The same logical row may therefore be copied twice. This avoids
incorrect removal when a row leaves one selector but remains in another. A later IVM
or client-view-record implementation may share bodies and membership while
preserving the same selector and lease semantics.

The in-process reference mirror retains current rows, their metadata, and one active
read identity, not historical bodies or tombstones. Each response must be paired with
the local ticket for the read that issued it. Cancellation consumes that identity even
when the committed cursor is unchanged; replacing a lease invalidates all of its old
tickets. This prevents duplicate or late responses from resurrecting removed rows.
A resumed mirror retains its matching logical cursor and rows, but its first wire read
still acknowledges nothing and bootstrap completion is relative to the new lease.

### 6. Access is carried by capabilities, not request fields

The reader capability defines the maximum collection access. A selector only narrows
it. Guests do not send trusted principals, owner identities, or scope IDs.

If a row stops belonging to a projection because it was deleted, no longer matches,
or is no longer authorized, the projection emits `Remove`. The generic guest need
not learn which reason applied. Outstanding reads wake when revocation changes the
projection.

Each collection is an independent consistency domain. A projection transaction is atomic within its collection, and there is no ordering or atomic visibility across collections. When one event, such as an incarnation closing, changes rows in several collections, each collection commits its own transaction. The host defers reader wakes until every affected collection has committed, but a reader that holds leases on two collections may still observe one change before the other. Facts that must become visible together belong in one collection. References between collections travel as capability attachments, which fail closed: closing revokes publications before it removes rows, so a reader that still sees a stale row gets `revoked` when it uses the row's reference, never a dead provider. A cross-collection commit epoch is deferred until a concrete reader needs snapshot reads across collections.

Revocation prevents future host operations and removes data from managed guest
replicas. It cannot revoke knowledge a guest already copied into its own memory, and
the ADR makes no such promise.

### 7. Values are portable data with capability attachments

Collection contracts encode keys, selectors, values, mutation payloads, and opaque
contexts as forward/backward-compatible CBOR shaped by their Lexicons. The transport
carries bytes rather than a recursively allocated WIT value tree.

Some values, such as toolbar entries, refer to live actions or surface mounts. These
are not serialized as trusted IDs. A value contains CBOR attachment slots alongside
recipient-local generic capability references:

```text
EncodedValue {
    body: CBOR,
    capabilities: [capability-ref],
}
```

Copying the CBOR does not copy authority. ADR 023 specifies publication, typed
redemption, proxy ownership, revocation, and replacement of these references.

### 8. Reading and writing are orthogonal capabilities

A read-only guest receives no writer. The read waist does not reverse direction to
become one universal mutation protocol. Sources may expose one or more optional
writer capabilities while retaining the same read projection.

Three writer forms are initially recognized:

1. owner-scoped registration;
2. causal contribution to shared logical rows; and
3. authoritative domain commands.

They share host authorization, resource ownership, and projection delivery, not
mutation semantics.

## Writer layers above the waist

### 9. Owner-scoped registrations

Independently keyed contributions use a host-assigned registration scope. A guest addresses only local keys:

```text
registration-writer.put(local_key, based_on?, value)
registration-writer.remove(local_key, based_on?)
registration-writer.clear()
```

Physical identity is `(registration_scope, local_key)`, so one contributor cannot overwrite or delete another contributor's row. `based_on` is optional compare-and-set protection for the caller's own row. The host serializes accepted mutations and returns acceptance separately from when the resulting projection page reaches the caller.

The registration scope, not a particular writer-table handle, owns row lifetime. Closing a writer releases mutation authority. Closing an incarnation removes all rows in its incarnation scopes atomically; uninstalling a plug removes all rows in its durable installation scopes atomically. Durable rows survive process and guest inactivity. ADR 025 specifies these ownership classes and supersedes the earlier writer-drop rule.

A host materializer combines all inputs into a consumer-facing projection using an explicitly selected policy such as union, grouping, actor priority, host-ordered LWW, rejection of collisions, or provider-defined logic. The framework has no silent default for concurrent writes to the same logical scalar.

### 10. Causal contributions to one logical row

Owner scoping alone is insufficient when contributor A observes combined row `foo`
and asks to change `foo.bar`. The mutation must carry the causal context A actually
observed; host tracking of the last page sent to A would be racy.

```text
causal-writer.mutate(
    key,
    observed_context,
    source_defined_mutation,
)
```

The writer identity is implied by the capability, and the host maintains its actor
sequence. The collection contract interprets the opaque context and mutation. A
reusable implementation may use actor dots plus a version vector:

```text
Dot     = (actor, sequence)
Context = version vector / observed frontier
```

The context distinguishes causally later operations from concurrent ones; it does not
decide their meaning. The collection must name a merge policy such as multi-value,
deterministic host-ordered LWW, actor priority, reject-concurrent, or a domain
reducer.

Contributor state remains in separately retractable actor branches. The materializer
combines active branches into current logical rows. Dropping one writer removes that
branch and recomputes the projection, allowing a previously hidden contribution to
become visible. Operations therefore record contributor intent or field changes, not
a whole combined value that would accidentally take ownership of other actors'
fields.

This machinery is CRDT-like and primarily operation-based, with materialized branch
state for efficient reads. Safe operation compaction is collection-specific. Simple
registration collections pay none of this cost.

### 11. Canonical adoption composes collections

An owner may expose an ephemeral proposal/contribution collection while retaining
exclusive write access to a canonical collection:

```text
contributor input branches
          -> combined proposal projection
          -> owner observes and validates
          -> owner writes canonical collection
```

If a contributor disappears, its proposal disappears. Already adopted canonical
state remains. Adoption is ordinary owner logic, whether the owner is a native or
guest micro; it is not a special replication operation.

### 12. Domain commands and host-owned optimism

Documents and other node-backed domains use explicit command APIs because their
conditions, merge rules, idempotency, and settlement evidence are domain facts.
Document mutations retain Automerge heads as their causal context and return resulting
heads as settlement evidence.

Optimistic state lives entirely on the host:

```text
authoritative projected base
    + host-owned pending mutation intents
    = visible projection replicated to every authorized guest
```

The guest does not implement a shared overlay engine. On submission, the host
installs the prediction and emits an ordinary visible `Upsert` with
`evidence = Pending`. The host submits the domain command, recomputes predictions when
base state changes, and retires the intent only when authoritative observation
contains the source-defined settlement token. Rejection or transport-unknown removes
the intent according to the domain policy.

Pending predictions do not compete with authoritative rows by artificial collection
versions. Collection versions order visible host transitions; datum heads determine
datum causality; command receipts determine settlement. These domains are never
compared.

Multiple document mutations retain their intents and base heads, allowing the host to
recompute their deterministic Automerge result rather than stacking stale whole-row
snapshots. Other guests see the predicted value and generic pending evidence, but not
writer identity, raw commands, or trusted settlement tokens unless separately
granted.

## Initialization and execution

A micro has one asynchronous initialization entry point, analogous to process start:

```text
initialize(arguments and capabilities) -> instance-root
```

Initialization may acquire projection leases, create writer leases, publish
capabilities, and create presentation resources. Returning the root marks successful
initialization; dropping it closes the incarnation and its owned resources. This
replaces framework-specific preload phases: acquiring a lease is preload, and resource
ownership defines cleanup.

The exact WIT may retain a separate `get-args` import when generated bindings or
runtime construction make it more ergonomic. That is an ABI-shape decision, not a
second lifecycle phase: arguments and initial capabilities are acquired once during
initialization.

Presentation exports remain synchronous and may not suspend. Collection acquisition,
long polling, mutations, and command outcomes are asynchronous host capabilities.
Host-owned pending reads must not hold an instance-wide gate that prevents synchronous
presentation from running.

## Protocol evolution

Protocol evolution and collection-schema evolution are separate.

- The WIT surface remains small and explicitly versioned. WIT stability/version annotations record when fields and methods enter the contract, and hosts preserve supported interface versions for existing guests. Breaking transport semantics require a new interface version rather than reinterpretation of an old field.
- Collection selectors, values, contexts, and mutations use Lexicon-compatible CBOR:
  open records/unions, unknown-member preservation, and forward/backward-compatible
  decoding. Ephemeral storage does not remove this requirement because old packaged
  guests run against newer hosts.

## Error model

Expected edge failures are explicit:

- invalid or unauthorized selector at acquisition;
- source unavailable;
- projection closed;
- reset required;
- mutation conflict or rejection;
- transport-unknown domain command outcome; and
- closed or revoked capability reference.

A long-poll timeout is not an error. Malformed host-produced pages, non-monotonic
cursors, wrong capability contract identities, and impossible transaction shapes are
programming/invariant failures and must not be silently recovered.

## Consequences

- The waist is small enough for independent Rust, TypeScript, and future guest
  libraries.
- Active leases give the host explicit interest for preload, upstream acquisition,
  cancellation, and retention without introducing pushed data queues.
- Inline values make the first implementation correct without historical hydration
  retention. Duplicate transfer and query evaluation are accepted initially.
- Host projections isolate guests from node ordering, storage, Automerge, and cache
  mechanics.
- Different write models compose over one read protocol rather than being forced into
  one misleading generic write.
- Causal contribution collections pay for causal branches only when their semantics
  require them.
- Shared optimism is implemented once on the host and appears to every language as
  ordinary projected current state plus pending evidence.
- Capability-bearing values remain serializable data while authority retains RAII
  lifetime and typed redemption.

## Deferred

- Zero-style IVM/client-view records and unioning overlapping selectors;
- cross-projection body sharing and exact-version hydration;
- durable cursor retention across host restarts;
- a generic library of causal materializers beyond the first concrete workload;
- query planning beyond collection-specific typed IRs; and
- backing adapters, durable registration scopes, and manifest admission are specified by ADR 025 and remain implementation work rather than changes to this read waist.

## Validation before acceptance

This ADR becomes Accepted only after the same protocol validates:

1. one ordinary host collection with bootstrap, empty progress, long polling, resume,
   and clear-on-reset;
2. one owner-scoped contribution collection whose rows survive writer-handle closure and disappear atomically when its incarnation scope closes;
3. one causal shared-row contribution with observed context and deterministic
   concurrent resolution;
4. one node-backed datum projection with host-owned optimism and heads-based
   settlement;
5. one capability-bearing toolbar or surface-mount row using ADR 023;
6. one real Wasm component crossing the WIT boundary, not only an in-process Rust
   adapter; and
7. one durable manifest-declared collection that exists while its declaring micro is inactive, reloads after host restart, and removes one installation scope on uninstall.
