# ADR 051: Guest files: the working mount, local state and on-demand dpath lenses

**Status:** Draft (2026-11-19). Proposed by the nodules lane from the operator's direction of 2026-11-19; nothing here is
built. Decisions marked **To confirm** need the operator.

**Amends:** ADR 047 (its proposed, never accepted sandbox/volume `localState` is replaced by §3), ADR 042 §4 (`importFile`
gains the working mount as its natural source), ADR 049 and ADR 050 (§5 adds visibility to what a cartridge provides).

Related: ADR 026 (drawer-native blobs), ADR 049 (one permission model), ADR 050 (cartridge surfaces), FDR 001 (dpaths
and checkouts, on `feat/dpaths`), `daybook_core::local_state` and `daybook_types::manifest` (today's local state).

## 1. Problem

Guests (nodule routines, micros) need to work with bytes as files: read blobs a grant lets them reach, keep private
state between invocations, and, later, edit documents through a path-shaped view (dpaths). Three facts shape the design:

- **Byte bodies over XRPC are expensive for large content.** A guest reading a body through `into_stream()` pays one
  component-model `stream.read` per octet (wit-bindgen 0.57); intxt measured a 128 KiB body exhausting a call's 50M fuel.
  Chunked reads fix the cost, but every byte still crosses the dispatch layer.
- **There is no virtual `wasi:filesystem` to adopt.** A survey (2026-11-19) found no p3 virtual filesystem in the wild.
  `wasi-virt` bakes a read-only image into a component at build time (P2); `wasi-vfs` is P1; `vfs-host` (monaka) and
  `eryx-vfs` are host-side P2 implementations; Wasmer's `virtual-fs` targets Wasmer only. wasmtime-wasi 48 (our pin) has no
  pluggable backend: its `Descriptor` is a closed `File | Dir` over real OS files, and its bindings map the WIT resource
  to that type. A virtual filesystem means our own `bindgen!` of `wasi:filesystem/types` (about 35 descriptor methods and
  a stream producer for p3's `read-via-stream`), which we would carry against every wasmtime and WASI release.
- **wash-runtime already mounts directories.** Our fork's workloads declare volumes (`HostPath`, `EmptyDir`) handed to
  guests as WASI preopens for p2 and p3 (`engine/volumes.rs`), with the embedder overriding `preopens`.

So the design uses real directories, preopened, with the host deciding what appears in them, and defers a virtual
filesystem until something needs one (§7).

## 2. The working mount, and "make it readable"

Every routine and micro instance gets a **working mount**: one directory per workload, preopened read-write at a fixed
guest path, empty at start, removed when the workload stops (and reaped at node start for leftovers). It is the guest's
scratch space and the place the host puts content the guest asked for.

The host adds one interface, call it `materialize`: given a reference the guest's capability reaches (a blob digest, a
`Blob` datum, later a dpath per §4), it resolves when the content is readable at a path inside the working mount and
returns that path. This is the presigned-URL shape: an authorized request yields a handle, and ordinary file I/O uses it.

- **Authorization** is the existing one: the routine's blobs handle (ADR 049 §6.2, now with drawer reaches) decides what
  it may materialize. The working mount gains nothing the capability does not already allow.
- **Fetching.** If the blob is not local, the host fetches it from peers first; the call resolves only when the bytes are
  complete. Partial blobs never appear.
- **Placement.** Blobs live in iroh-blobs' file store as plaintext `<root>/data/<blake3>.data` files (ciphertext is
  virtual). A complete blob is **hard-linked** into the working mount; blobs of 16 KiB or less live inline in the store's
  database and are **copied** (write a temp name, then rename). If the working mount is on another filesystem, the host
  reflinks or copies. The working mount therefore lives under the blob store's root by default.
- **Immutability.** Hard links share an inode, so the host never opens a store file for writing (complete iroh-blobs
  files are write-once today; this ADR makes it a rule). The guest's own writes go to new names it creates.
- **Live view.** Files can appear (materialized) and disappear (unlinked on revocation) while an instance runs. A guest
  that opened a file keeps reading it; a later open fails. **To confirm:** whether a revocation should only take effect
  between invocations, for deterministic routines.
- **Writing back** already exists: ADR 042's `importFile` takes a descriptor, so a guest writes a file in its working
  mount and imports it as a blob.

The set of preopens is fixed when an instance starts (guests read it once; the C library and Rust std cache it), so the
working mount is created up front and only its contents change.

## 3. Local state

Local state is state a cartridge keeps on this node, never synced: caches, indexes, a SQLite database. Today
`daybook_core::local_state` serves it **host-side**: a plug declares `local_states` of kind `SqliteFile`, the host opens
`<root>/<namespace>/<plug>/<key>.sqlite` through `sqlite_rs` (WAL, `busy_timeout` 5 s), and routines reach it through
`local_state_acl`. Guests never touch the file. Unlike browser OPFS, one cartridge's local state can be depended on by
another (§5).

Lessons taken from OPFS: a private namespace per identity; quotas with a persistent class that the host does not evict;
crash-safe writes by temp-and-rename; single writer; clearing as an explicit act.

Two kinds:

1. **Host-mediated SQLite (today's, the default, and the only kind another cartridge may depend on).** It is correct
   under concurrency because SQLite's own locking runs on the host. The ADR 047 manifest gains it as a cartridge-level
   declaration, keyed by name, with `visibility` (§5).
2. **A raw directory (new, private only).** A persistent per-cartridge directory under the node directory, preopened
   into the cartridge's instances. Because `wasi:filesystem` has no locking API (only `advise`), guests cannot coordinate
   writers, so the host enforces **single writer**: one live instance holds the read-write mount (a lease), any other
   instance of the same cartridge gets it read-only or waits. A read-only mount suits plain files and SQLite opened with
   `immutable=1`; SQLite in WAL mode cannot even read from a read-only mount (readers write the `-shm` file).

No "unsafe" multi-writer mode ships. The upgrade path is a small host lock interface (`locks`: acquire shared or
exclusive on a name, released with its guard, implemented with `flock`), the counterpart of the Web Locks API that
exists because OPFS files have no locks. With it, a raw directory could admit several writers that cooperate.

Quota and lifetime: each local state has a quota and a class (`persistent`, never evicted; `cache`, evictable under
disk pressure). Local state **survives uninstall**, as grants do (D57); clearing it is an explicit call.

wash-runtime does not stop two workloads from mounting the same host directory (it only reserves host paths such as
secrets and the OCI cache). The lease above is enforced by our host, not by wash.

## 4. dpaths: lenses rendered on demand

FDR 001 defines dpaths (path claims as document facets) and checkouts (a selection rendered through lenses into files,
with edits reconciled back against the recorded render). A live dpath filesystem is a poor fit for short-lived routines.
Most of its value is reachable through §2:

1. A guest asks to materialize a dpath, or a selection of dpaths.
2. The host resolves the claims, runs each claim's lens to an entry set, writes the entries into the working mount and
   records the render base.
3. The guest edits the files and calls a commit with the paths it changed; the host runs each lens's import against
   its render base.

This is the FDR's reconciliation without filesystem scanning: the guest says what it changed. The FDR's open questions
(lens selection and fallback, the Body facet, collision naming) are unchanged and block this section. A true checkout
for long-lived agents belongs in their sandbox, which can run a real filesystem.

## 5. Visibility and cross-cartridge dependencies

Nothing a cartridge provides can be marked private today: any cartridge that declares the matching permbind and is
granted it can call a method, and the old daybook manifest lets a routine name another plug's local state directly.

Every provided resource (method, view, collection, local state) gains `visibility`:

- `private` (**default**): only this cartridge's own nodules, routines and micros may use it. Dispatch and the local
  state host refuse other cartridges before consulting any grant.
- `public`: another cartridge may declare a dependency on it, and the person grants it as usual.

On depending on another cartridge's permbinds: **reusing a declaration** (its shape and consent text, like importing a
type) is allowed. **Inheriting another cartridge's grant** is not: that is the confused-deputy pattern, since the person
granted B, not A. The construct for "A uses what B is allowed to do" is B publishing a public method that performs the
work, with A depending on it: B stays the deputy and decides what it exposes. "Once linked it's linked" is what grants
already do: a grant persists, survives uninstall and is not re-asked.

**To confirm:** the default (`private`), and whether micros' collections (ADR 050, written into by other cartridges)
are `public` by construction.

## 6. Out of scope

Writes into another cartridge's local state, sync of local state between sister nodes, and quotas on the working mount
beyond the node's disk.

## 7. Deferred: a virtual `wasi:filesystem`

A host implementation of `wasi:filesystem/types` serving blobs (and later dpath lenses) without on-disk copies is
deferred until a need appears that §2 cannot meet: lazy fetch on first read, per-read revocation, or no plaintext on
disk. Agents, the likeliest first user, may run in a VM sandbox with a real filesystem instead. References when it
comes: eryx-vfs's `VfsStorage` trait, monaka's `vfs-host` design, and wasmtime-wasi's p3 `read-via-stream` producer.

## 8. Order of work

1. Chunked byte-body reads in `big_ndk` (intxt's `forward_body`, PR #213), independent of everything here.
2. The working mount and `materialize` for blobs (§2), with hard-link, copy and cross-device tests.
3. Visibility on provided resources (§5).
4. Host-mediated local state in the cartridge manifest (§3.1).
5. Raw-directory local state with the single-writer lease (§3.2); the `locks` interface only when a cartridge needs it.
6. dpath lenses on demand (§4), after FDR 001's lens questions are settled.
