# ADR 042: Byte bodies and local-only methods

**Status:** Proposed (manager, 2026-11-11), from the operator's review of `org.example.daybook.blobs.*` relayed by intxt. To be reviewed with the operator and intxt.

**Related:**
- [ADR 026: drawer-native blobs](026-drawer-native-blobs.md)
- [ADR 027: cartridges, nodules and operation contracts](027-cartridges-nodules-and-operation-contracts.md)
- [ADR 037: holder-bound capabilities and channel identity](037-holder-bound-capabilities-and-channel-identity.md)
- Ledger D42 (operator identity: the uid only lets a process ask for a grant)

## Context

The blob methods move bytes as base64 inside JSON:
- `blobs.put` takes `PutBlob { capability, bytes: {"$bytes": …} }`, capped at 8 MiB.
- `blobs.read` returns `BlobRange { bytes: {"$bytes": …}, lengthOctets }`, up to 1 MiB per call.

The operator rejects this ("the base64 is honestly very stupid"). Base64 costs a third more on the wire, and a full copy plus an encode and a decode on each side. It also forces the 8 MiB cap because a whole body is one JSON value. intxt's capture runs on the same machine as the node, base64s every file it captures, and skips anything over 8 MiB.

The cause is not in the blob service. Dispatch carries exactly one JSON value each way: `Invoke.input` and `InvokeOutput.output` are `serde_json::Value` (`big_dispatch_core`). The router, the `/xrpc` HTTP surface, the irpc in-process path and the wasm guest provider all inherit that, and the guest additionally caps a whole message at 4 MiB (`MAX_MESSAGE_OCTETS`, `big_nodule_wasm`). Inside the node, `BlobService::import_stream` already ingests a `Stream<Item = io::Result<Bytes>>`, so the ingest path is streaming and only the API isn't.

**Precedent:** atproto's `com.atproto.repo.uploadBlob` declares `input.encoding: "*/*"`; the HTTP body is the raw bytes, `Content-Type` is the mime type, and it returns a blob ref. `com.atproto.sync.getBlob` returns raw bytes. Lexicon already allows any encoding for `input` and `output`, so no multipart and no new schema language is needed. The operator wants us to learn from this, not copy it. Our ids stay BLAKE3, matching the shape of atproto's blob ref, not its sha256 CID.

## Decision

### 1. A method declares its body encodings

A method's Lexicon `input.encoding` and `output.encoding` are either `application/json` (today's only case) or a byte encoding (`*/*` or a specific mime type). This is a property of the method, so it lives in the declaration (`method_core::XrpcMethod`), next to its kind (query, procedure, subscription).

A method with a byte body still has JSON-shaped **parameters**: the Lexicon `parameters` object, carried as the query string on HTTP and as a typed struct in Rust. The capability, the blob id and the range move out of the body into parameters. This is how `uploadBlob` and `getBlob` work.

### 2. Dispatch carries a body that is JSON or a byte stream

`Invoke` and `InvokeOutput` carry a `Body`, with two cases: `Json(Value)`, or `Bytes(stream of Bytes chunks)` with an optional mime type and length. Dispatch checks the body case against the method's declared encoding before calling the provider, the same way it already holds a query to GET.

How each transport carries it:
- **HTTP `/xrpc`:** the raw request or response body, with `Content-Type`. Reads honour `Range` (`206 Partial Content`).
  This includes the node's Unix socket that `day` uses: it serves the same `/xrpc` over HTTP (`service_utils_rs::xrpc::unix`; the client is `day_cli/client.rs`).
- **irpc (in-process clients):** a byte-chunk stream (irpc's client-streaming and server-streaming channels), never a `Vec`. A blob larger than memory never exists as one buffer.
- **Wasm guests:** wasi:http body streams. This is what lifts the 4 MiB message cap for bodies; the cap stays for JSON messages. Until the guest side lands, a nodule reads blob ranges under 4 MiB.

Ranged reads keep iroh-blobs' semantics: any byte range of a local blob, verified against its BLAKE3 tree where the reader asks for verification (remote readers). A local reader asking for plain bytes gets plain bytes.

### 3. Local-only methods

A method's declaration may mark it **local-only**. Dispatch refuses a local-only method unless the call's origin is local.

A local origin is a client on the node's own machine reaching an edge transport directly:
- a peer on one of the node's Unix sockets, whose uid the transport reads with `SO_PEERCRED` (`service_utils_rs::xrpc::unix`);
- a native client embedded in the node's process, such as `day` running the node in-process.

It is **not** a nodule guest, even though guests run in the node's process. Nor is it a call relayed through guest egress, nor one arriving from a remote peer.

Only the edge transport that accepted the call sets the origin in `InvocationContext`; the caller never sets it. A hop never inherits it: a nodule's egress call, made while serving a local caller, has the nodule as its origin, not the local client.

Local-only is an exposure rule, not authority. A local caller still needs a capability for the method, and the uid only lets a process *ask* for a grant (D42); it never confers operator authority.

Precedent: Docker's Unix socket versus TCP split, and D-Bus with polkit checking the peer on the bus.

### 4. Import from a file descriptor, not a path

`blobs.importFile` is local-only and takes file descriptors passed over a Unix socket (`SCM_RIGHTS`), not paths.

A path would make the node open a file as its own uid. That might be a file the caller can't read, which is the classic confused deputy. A passed descriptor proves the caller could open it, which is the xdg-desktop-portal and Wayland precedent.

**The mechanism: a separate descriptor socket with its own small framed protocol.** HTTP can't carry descriptors: hyper reads and writes the stream with plain `read`/`write`, so ancillary data is never sent or is lost. Wayland and D-Bus can pass descriptors because they are message protocols where each message may carry some.

So the node serves a second Unix socket next to the XRPC one (`<node id>.fd.sock`). It is served by whoever holds the node, like the XRPC socket (D42). Each frame there is one call: a length-prefixed header (the method NSID, its parameters including the capability, and the descriptor count), with the descriptors attached to that frame's `sendmsg`. The server turns a frame into an ordinary `Invoke`, whose body is a descriptors case of `Body` that only this transport can construct, with a local origin, and runs it through dispatch. Capability checks and routing stay in one place. The response is a frame of n results, in order.

Rejected:
- Sending descriptors on the XRPC connection right before the HTTP request. That needs a custom accept loop that reads ancillary data before handing the stream to hyper, and it breaks on any request pipelining.
- Exchanging descriptors on a side socket for short-lived tokens that the HTTP call then names. That needs two round trips plus token state and expiry, for what one frame does.

`importFile` always **copies**: the node feeds each descriptor into `import_stream`, and it `fstat`s the descriptor to learn the length up front. Reference in place (iroh-blobs' external import) is left out of v1. A caller that wrongly declares a file immutable would corrupt what the node later serves to remote peers, whose verification then fails. Capture never needs it: worktree files are mutable, and git objects are zlib-compressed. If reference mode comes back, it is operator-only.

### 5. The blob methods after this ADR

| Method | Kind | Parameters | Body |
| --- | --- | --- | --- |
| `blobs.put` | procedure | `capability` | input: bytes (`*/*`), output: `BlobInfo` JSON |
| `blobs.read` | query | `capability`, `blobId` | output: bytes (`Range` for part of it) |
| `blobs.stat` | query | `capability`, `blobId` | JSON, as today |
| `blobs.importFile` | procedure, local-only | `capability` | input: n descriptors (descriptor socket only), output: n `BlobInfo` or refusals, in order |
| `blobs.mintCapability` | procedure | — | JSON, as today |

### 6. Put semantics (capture's requirements, from intxt)

- **The returned id is authoritative.** The node hashes what it reads and returns that `BlobId`. A put never takes an id asserted by the client. A file can change between the client deciding to store it and the node reading the descriptor, so the client records the id the put returns.
- **Idempotent.** Putting bytes the node already holds is a cheap no-op that returns the same `BlobInfo` and adds a new upload hold. A retry needs no `stat` first. Content addressing gives this; the ADR makes it a contract.
- **Batches.** `importFile` takes n descriptors in one call (one `SCM_RIGHTS` message carries many) and returns n results in order. Each result is a `BlobInfo` or a typed per-item refusal, so one bad file doesn't fail the batch.
- **Typed size refusal.** If a size limit remains, going over it is a per-item `TooLarge { limitOctets }`, never a batch failure.
- **The window between put and reference.** A put leaves an upload hold that keeps the bytes. When a document retains the digest, the document claim takes over in the same transaction and every upload hold for that digest is released (`big_blobs::ownership`, which already does this). Missing today: an upload that nothing ever references keeps its hold forever. Proposed: upload holds expire after a grace period (atproto keeps an unreferenced upload for a while, then GCs it), and a client that needs longer re-puts, which is cheap by idempotency. The grace period is an open question.

The `{"$bytes"}` datum form stays for small byte values *inside datums*, where it is a Lexicon type. It goes away only as a transport for blob bodies.

## Consequences

- `big_dispatch_core::Invoke` and `InvokeOutput` break: `input: Value` becomes `body: Body`. Every provider, the router, the HTTP surface and the guest provider change with it. Per AGENTS.md this is a clean interface break, not a shim.
- `method_core` gains body encodings and a local-only flag in `XrpcMethod`; `service_method!` grows a form for byte-bodied methods.
- The XRPC HTTP surface (TCP and the Unix socket) and the irpc in-process path need streaming request and response bodies.
- The node serves a second Unix socket for descriptor calls, with the same lifecycle as the XRPC socket.
- `InvocationContext` gains an origin set only by edge transports.
- intxt's capture stops base64-encoding files and loses its 8 MiB skip.
- The guest's 4 MiB cap remains for JSON; blob bodies in guests wait for wasi:http streams.

## Open questions

Settled in review with intxt (2026-11-11):
- Encodings are one per direction (bytes in, JSON out for `put`, as atproto's `uploadBlob`).
- `importFile` needs no declared length (`fstat`). HTTP `put` has `Content-Length`; a running cap matters only for chunked bodies.
- No path import.

Open for the operator:
1. The upload hold's grace period. Capture is satisfied with hours, because it puts right before it records.
2. Order of work: the dispatch `Body` change first (it has no users besides blobs at first), then the blob methods, then the guest streams.
