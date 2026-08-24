# ADR 002: Automerge Frontier Worker & Materialization Barriers

**Status:** Accepted & Implemented.

## Context

In Daybook's distributed storage and reactive runtime architecture, there is a fundamental distinction between two layers of state:

1. **The Sedimentree Frontier (Transport & Storage Layer):**
   * Tracks raw, cryptographic loose commits and fragments synchronized over the network via Subduction.
   * Commits may be encrypted under Keyhive keys that the local node has not yet received (e.g. `PendingMaterialization`).
   * Includes checkpoint commits that do not directly correspond to Automerge document changes.
   * Heads record durably in BigSync partition logs (`part_store` / `big_sync_members`).

2. **The Automerge Frontier (Application & Indexing Layer):**
   * Decrypted, materialized Automerge `ChangeHash` heads for each document branch.
   * The real frontier concern for app level systems like indexers (`doc_blob_refs`, `doc_facet_sets`, `doc_facet_refs`), background triage workers, and UI listeners via UniFFI (`DrawerRepoFfi`).

---

### The Problem: Control-Plane vs. Data-Plane Races

Previously, downstream consumers like `SwitchWorker` subscribed directly to the **Sedimentree partition log** (`part_store.subscribe_local`). When a peer synchronized changes:

1. **Race Condition on Remote Sync:**
   * BigSync recorded the partition metadata and emitted `SubEvent::Changed` immediately.
   * Meanwhile, BigRepo's transport worker was still decrypting and applying the Automerge commits in `DocWorkerMsg::ApplySyncSession`.
   * `SwitchWorker` read the document handle at the moment the partition event fired, observed stale/pre-sync Automerge heads, concluded nothing changed, advanced the partition cursor, and dropped the update.

2. **The Double-Emission Hazard on Local Writes:**
   * Local writes via `DrawerRepo::update_at_heads` immediately dispatched `DrawerEvent::DocUpdated` on the in-memory registry.
   * A moment later, the local `part_store` emitted a partition log event for that same write.
   * Because the partition consumer did not recognize the write had already been processed, it computed the diff a second time and dispatched duplicate events.

3. **Cold Document Impedance:**
   * `BigRepo` intentionally does not broadcast in-memory change events (`changes.rs`) for cold/unloaded documents that receive commits during background sync.
   * If a consumer relies on ephemeral in-memory change channels, it cannot observe cold updates or survive process restarts.

---

## Decision

We introduce the **Automerge Frontier Worker (`AutomergeFrontierWorker`)** in `big_repo` to decouple Sedimentree transport synchronization from Automerge application consumption, backed by deterministic materialization barriers and source partition filtering.

---

### 1. The Automerge Partition Log & Domain Separation

Instead of downstream indexers and the Drawer switch consuming raw Sedimentree partition logs directly, `big_repo` maintains a dedicated **Automerge Partition Log** (`automerge_docs_part_id()`) inside `part_store`:

```
┌─────────────────────────────────────────────────────────────┐
│ Subduction Network Sync / Local Writes                      │
│ └── Writes loose commits to SQLite (big_repo_subduction_*)  │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Sedimentree Source Partitions (Watched Authority Partitions)│
│ └── Durable monotonic cursor per source partition           │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ AutomergeFrontierWorker (big_repo::runtime2)                │
│ ├── Filters watched source partition scopes (e.g. docs)     │
│ ├── Enforces Materialization Barrier (commit_row_id >= N)   │
│ ├── Enforces Keyhive Barrier (keyhive_seq >= K)             │
│ └── Emits materialized Automerge heads to Automerge Part    │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Automerge Partition Log (automerge_docs_part_id)            │
│ └── Durable monotonic cursor for materialized Automerge state│
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ SwitchWorker & DrawerRepo (Pure Multiplexers)               │
│ └── Diffs dmeta and dispatches single DrawerEvents to Sinks │
└─────────────────────────────────────────────────────────────┘
```

#### Object Domain Separation (`AUTOMERGE_OBJ_MASK`)
To prevent Automerge head payloads from clobbering Sedimentree head payloads in `big_sync_objs(scope_id, obj_id)` or triggering infinite event recursion:
* Automerge object IDs are bijectively mapped from `DocumentId` using XOR masking (`AUTOMERGE_OBJ_MASK = [0x5A; 32]`):
  $$\text{am\_obj\_id} = \text{automerge\_doc\_obj\_id}(\text{doc\_id}) = \text{doc\_id} \oplus \text{0x5A}$$
  $$\text{doc\_id} = \text{automerge\_obj\_to\_doc\_id}(\text{am\_obj\_id}) = \text{am\_obj\_id} \oplus \text{0x5A}$$

---

### 2. Source Partition Filtering & Dynamic Registration

To avoid inspecting every arbitrary document across large repositories:
* `AutomergeFrontierWorker` tails only configured **source partitions** (such as `core_docs_part_id`, `content_docs_part_id`, `default_drawer_part_id`).
* `Config.automerge_source_parts: Option<HashSet<PartId>>`: Specifies initial source partitions at boot.
* `big_repo.watch_automerge_parts(parts)`: Allows dynamic registration of newly created authority group partitions or custom drawer partitions after `BigRepo` initialization.
* When Keyhive CGKA events arrive, documents are only processed if `big_sync_store.obj_parts(doc_id)` intersects the set of currently watched source partitions.

---

### 3. The Storage-Driven Monotonic Watermark Barrier

To eliminate races between Subduction storage writes and `DocWorker` in-memory commit application:

#### A. Watermark Table
We record a watermark mapping each sync transaction to the highest committed row ID:

```sql
CREATE TABLE IF NOT EXISTS big_repo_sync_commits_watermark (
    scope_id INTEGER NOT NULL,
    doc_id BLOB NOT NULL,
    big_sync_txid INTEGER NOT NULL,
    latest_commit_row_id INTEGER NOT NULL,
    PRIMARY KEY(scope_id, doc_id, big_sync_txid)
) STRICT;
```

#### B. Crate-Local Barrier on `LiveDocBundle`
Each live document bundle in `big_repo` maintains:
* `latest_commit_row_id: AtomicI64`
* `barrier_notify: Arc<tokio::sync::Notify>`

Whenever `DocWorker` applies commits (via `ApplySyncSession`, local `commit_delta`, or cold-hydration):
```rust
bundle.latest_commit_row_id.fetch_max(max_applied_row_id, Ordering::Release);
bundle.barrier_notify.notify_waiters();
```

#### C. Monotonic Evaluation (`>=`)
When `AutomergeFrontierWorker` processes an event at `big_sync_txid`:
1. It queries the watermark table for `target_commit_row_id`.
2. It checks:
   $$\text{bundle.latest\_commit\_row\_id.load(Acquire)} \ge \text{target\_commit\_row\_id}$$
3. If `< target_commit_row_id`: it awaits `bundle.barrier_notify.notified()`.
4. Once $\ge \text{target\_commit\_row\_id}$, it inspects `bundle.doc` heads with 100% causal certainty.

---

### 4. Per-Partition and Keyhive Cursors in SQLite

Progress across multiple source partitions and the Keyhive event stream is persisted independently:

```sql
CREATE TABLE IF NOT EXISTS big_repo_automerge_part_cursor (
    scope_id INTEGER NOT NULL,
    source_part_id BLOB NOT NULL,
    cursor INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(scope_id, source_part_id),
    FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
) STRICT;

CREATE TABLE IF NOT EXISTS big_repo_automerge_keyhive_cursor (
    scope_id INTEGER PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
) STRICT;
```

---

### 5. Downstream Simplification (`SwitchWorker` & `DrawerRepo`)

With `AutomergeFrontierWorker` maintaining the Automerge partition log:

1. **`SwitchWorker` as a Pure Multiplexer:**
   * Subscribes to `automerge_docs_part_id()` via standard `part_store.subscribe_local`.
   * Never accesses raw Sedimentree commits, never coordinates with `BigRepo` internals, and never manages decryption timing.
2. **Single-Source Event Emission:**
   * Local writes and remote sync events both resolve through the Automerge partition log.
   * `DrawerEvent::DocUpdated` is emitted exactly once per state change.
   * Eliminates the double-emission hazard permanently.
3. **Guaranteed Materialization:**
   * Every event delivered to indexers (`DocBlobsIndexRepo`, `DocFacetSetIndexRepo`, `DocFacetRefIndexRepo`) is guaranteed to have its Automerge document already decrypted and materialized in memory or on disk.

---

## Consequences

### Positive
* **Deterministic Materialization:** Completely eliminates race conditions where indexers diff Automerge heads before commits have landed in memory.
* **$O(1)$ Atomic Synchronization:** Eliminates expensive DAG walks and commit graph comparisons using monotonic integers (`latest_commit_row_id` and `latest_keyhive_seq`).
* **Source Partition Filtering:** Restricts processing strictly to documents belonging to active authority groups or registered drawers.
* **Clean Abstraction Boundaries:** `BigSync` manages transport partitions; `BigRepo` manages document materialization; `DrawerRepo` / `SwitchWorker` manage application facets.
* **Unified Event Ordering:** Eliminates split-brain local vs. remote event pathways.
