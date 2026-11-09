# ADR 033: What a node is, and how its services compose

**Status:** Accepted (operator, 2026-11-04). Drafted from the operator design session of 2026-11-04 (decisions D14–D21 of the
[runtime ledger](../scratch/nodule-runtime-ledger.md)).

**Updated:** 2026-11-06 (see [Revision 2026-11-06](#revision-2026-11-06))

**Related:**
- [ADR 019: cabinet drawers and selective mirroring](019-cabinet-drawers-and-selective-mirroring.md)
- [ADR 022: authority, actors, capabilities and grants](022-authority-actors-capabilities-and-grants.md)
- [ADR 023: cursor consumption is long-poll](023-cursor-consumption-is-long-poll.md)
- [ADR 027: plugs, nodules and operation contracts](027-plugs-nodules-and-operation-contracts.md)
- the 2026-07-04 Multiprocessing entry in the [DEVDOC log](../DEVDOC/log.md)

ADRs 029–031 are reserved for the Clay branch.

## Context

The Multiprocessing entry in the DEVDOC log lists the problems with the old daybook repo:
- It is a web of on-disk formats behind a whole-repo lock file, so a second process (a CLI, another app) cannot use the same data.
- It has no answer for cloud hosting or for running in the browser.

That entry leaned toward actors with a daemon by default. It also kept the git notion of a *checkout*: a directory you can copy and open.

Since then the cabinet, plugs, nodules and dispatch have landed. Every service operation already has an XRPC identity (`service_core_rs::Operation::XRPC`), and every service is reached through `Client<Proto>`. A client is `Direct` (inline), `Irpc` (another task, or another process over IRPC) or `Xrpc` (HTTP).

What is missing is a definition of the node itself:
- what it owns;
- how its services find and call each other;
- how processes outside it reach it;
- what may be assumed about where it runs.

`big_node` today boots everything in one process on one SQLite `Database`, and only nodule operations go through dispatch. Networking exists only in a test helper. And `big_repo`, given `StorageConfig::Disk { path }`, opens its own path.

## Decision

### 1. A node owns its data; there are no checkouts

A node is the owner and controller of a body of data, and the sync identity that data replicates under. Two reasons:
- **It owns the data.** The data is permissioned (ADR 022) and encrypted at rest, so a copyable directory that some other process "opens" is not a meaningful unit.
- **Drawers are the realms.** Separate realms of data inside one node are drawers (ADR 019), not separate checkouts. Moving data out of a node is an explicit export, not a directory copy.

The closest analogy is a **network filesystem**, not an application or a web service. A node is a self-consistent domain for storing, permissioning and syncing data. Many clients use it, and other nodes replicate with it. A FUSE layer over a node is the natural consequence of this shape.

Not part of this decision: files-first APIs that translate files to and from documents. Those are parked elsewhere and build on this.

### 2. The generic node knows nothing about users

`big_node` is the generic node:
- one keyhive identity;
- one authority with an operator;
- the standard services (§6).

It has no notion of users, devices of a user, or the relationships between nodes. Those belong to node *compositions* in their own crates:
- **User nodes** (Dayframe): this device versus another device of the same user, enrolment, and the mother and sister relationships.
- **Relay nodes** (Townframe): sponsorship, retention, admission and quotas (ADR 005).

Each composition boots `big_node` and adds its services and policies. Neither is a variant of the node. Keeping `big_node` free of daybook's user model means another application can build on it unchanged.

### 3. Three layers: services, clients, attachments

1. **Services** implement operations (`Handler<P>` hosted in a `Host`). A service does not know how a request reached it.
2. **Clients** are how anything calls a service: another service, a nodule host, a test, an app. A holder of `Client<Proto>` does not know whether the service is in its task, in another task, in another process or across the network. Node-side and Clay-side code depend only on client handles.
3. **Attachments** are what a particular deployment adds on top: listeners that serve the node's router over a Unix socket (UDS) or TCP, and the peer network endpoint. A node with no attachments is complete. Tests and embedders often run it that way.

Routing a request to a service and accepting connections are separate concerns. Only the attachment knows about UDS, TCP or Iroh.

### 4. One router per node: dispatch moves to the node

Dispatch, meaning the operation routing table, ocap admission and providers, is not nodule-specific, so it leaves `big_nodule`:
- **Its own crate.** Dispatch moves into `big_dispatch` (contract in `big_dispatch_core`), which `big_node` owns.
- **Built-in services become providers.** Docs, drawers, blobs, authority, plugs and nodule management register their operations in it as built-in providers, under the NSIDs they already declare.
- **Nodules register as before.** The nodule service registers its Wasm providers in the same router, as it does today.

After this, every path into the node goes through the same table. A nodule calling `docs.*`, a CLI calling `docs.*` over UDS and a test calling `docs.*` in-process are the same call.

Precedent: one route table in front of many upstreams (Envoy, an HTTP reverse proxy), and dispatch is already shaped that way.

Two parts of the code are not routed:
- **The composition root.** `BigNode::boot` wires services to each other with direct clients. Routing a service to itself through its own router buys nothing.
- **Admission is two-sided.** Built-in services keep checking the capability in the request (`Authorization`), as they do now. Dispatch admission additionally checks before entering a nodule, because guest code cannot be trusted to check for itself.

### 5. Three access paths

| Path | Transport | Who the caller is |
|---|---|---|
| In process | `Direct` / `Irpc` client | Whatever capability the holder was given |
| Unix socket | XRPC over UDS | The kernel's peer credentials. A connection from the node owner's uid acts as the operator; any other uid is refused. A client may present a node-issued token to narrow itself to that token's capability. |
| Network | XRPC over HTTP/WebSocket on Iroh (a TCP listener also exists, see the revision) | A capability presented per call, as dispatch admits today |

The Unix socket follows the standard local-daemon pattern (PostgreSQL `peer` auth, D-Bus `EXTERNAL`, systemd, ssh-agent, Docker):
- **Location.** The socket lives in a directory only the owner can enter (mode `0700`, under `$XDG_RUNTIME_DIR`).
- **Identity.** On accept, the node reads the caller's uid from the kernel: `SO_PEERCRED` on Linux, `getpeereid`/`LOCAL_PEERCRED` on macOS and the BSDs. It admits only the uid that owns the node. The kernel vouches for the uid, so the local user needs no secret.
- **Narrowing.** Tokens are for narrowing, not for admission: a CLI or sandboxed app given a scoped capability presents its token, in the way X11's MIT-MAGIC-COOKIE or D-Bus's cookie mechanism carry an explicit credential.
- **Windows** has `AF_UNIX` but no peer credentials, so there the token is required.

### 6. Networking and peers belong to the node

Networking defines who the node is to others, so `big_node` owns it. It is the successor of the old `IrohSyncRepo` (`daybook_core/sync.rs`).

**One node key.** The node's identity key is both its keyhive identity and its Iroh endpoint key, so "who is this connection" and "what may it have" are answered by the same identity.

**The peers service.** A `big_node` service owns the Iroh endpoint. It:
- accepts connections and identifies the peer;
- registers the peer with the repo, sync and blob RPC services;
- decides which partitions to sync with that peer, and calls `set_peer` on the document and blob sync workers;
- dials, reconnects with backoff, and reports sync progress.

**Policy is a hook the embedder supplies.** `big_node` orchestrates but does not own the policy for who to dial, whether to accept a peer, or which partitions to sync with it. Those decisions differ by node kind, so they come from the composition through a peer policy in `big_node`'s config. A user node dials its known devices and mirrors per its drawer policy. A relay accepts sponsored peers, mirrors by sponsorship and enforces quotas. Changing what kind of node this is requires no change to `big_node` itself.

**The default policy uses ADR 019's split:**
- `big_repo` projects keyhive access into sync partitions: what a peer may have.
- The cabinet answers which drawer partitions are worth syncing with a given peer: this node's mirroring policy intersected with that access.

That per-peer cabinet query is `Drawers::mirrored` (the `mirror` intent of the known-drawers registry); the node intersects it with `big_repo::groups_with_member`. A relay's policy replaces the mirroring half with sponsorship, and the access half still bounds it.

**Configuration.** Node configuration is `big_node`'s:
- the identity;
- the attachments (Unix socket path, TCP address, Iroh settings);
- the resource bindings handed to services;
- the peer policy.

Each service keeps its own configuration section within it.

### 7. The standard service set

Every node runs:
- authority;
- docs and drawers (the cabinet), with indexes;
- blobs;
- sync (keyhive and the BigRepo/BigSync replication);
- plugs (the registry);
- nodules;
- dispatch.

This is the set both relay and user nodes need. "The node works" means this set works across nodes.

### 8. Nothing assumes one node per process, machine, directory or database

We intend to host nodes for people: many tenants in one process, booted and stopped on demand and scaled to zero, as a PDS host serves thousands of accounts. Individual services may also be disaggregated into another address space, such as a Wasm instance or another process. Therefore:
- **A node is a value.** It is booted into `(BigNode, StopToken)` and stopped in reverse construction order. Booting and stopping many nodes in one process, repeatedly, is supported and tested, including that stop releases everything (no leaked databases, files or tasks).
- **No process-global state** belongs to a node. A shared resource pool such as a `SqliteRuntime` may be given to many nodes, but a node's state lives in what it was given.
- **Services are handed their resources; they never open locations.** The composition root gives each service its storage bindings: a SQL database handle with a scope key, a blob store, a secret store. A service that opens a path itself (today `big_repo`'s `StorageConfig::Disk`) moves its open into the composition root.
- **Services address each other only through clients.** Moving a service out of the address space changes the client transport at the composition root and nothing else.
- **Each service acts as its own actor.** Services already hold actors in the node's authority (`documents`, `drawers`, `blobs`, `plugs`, `nodules`, `dispatch`). A disaggregated service presents that actor's capability when it calls the others, like any client.
- **Scaling to zero needs durable truth.** Anything that must survive a stop and reboot is in the node's storage, never only in memory. Rebooting from storage is the normal path, not a recovery path.

### 9. `big_node` is the system test arena

The node's integration tests live in `big_node`'s `e2e` module. They test the standard services the way a high-level user would:
- **Harness:** boot N nodes in one process, link them in a topology, and drive them only through clients.
- **Ladder:** small, ordered scenarios, as in `daybook_core`'s `sync/tests/ladder.rs`:
  - connecting;
  - data created before or while connected;
  - updates in both orders;
  - restarting and reconnecting;
  - offline divergence;
  - bootstrapping a new node.
- **Scale:** for example, 10,000 documents of a realistic shape arriving at one node and replicating.
- **Randomized stress:** seeded, as in `daybook_core`'s `sync/tests/stress.rs` and `big_repo`'s `test2`, with `long_test` names.
- **Nodules across nodes:** a plug published on one node, installed and invoked on another.

The `big_repo` topology and log-nickname helpers are reused rather than rewritten.

## Consequences

- Dispatch's crate boundary changes, and `big_nodule` depends on `big_dispatch` instead of owning it. ADR 027's dispatch sections stay valid, with the owner changed.
- Every built-in operation becomes reachable over the network once a listener is attached, so each one's capability check is now a network-facing security boundary. The existing in-request `Authorization` checks are what make this safe, and the router does not bypass them.
- Node configuration moves from paths to resource bindings, which changes the config of `big_repo` and `big_node`.
- Networking moves from test helpers into `big_node`: the Iroh endpoint attachment and the peers service, with an embedder-supplied peer policy (§6). Today no production code calls `set_peer`; only test helpers do, with empty partition sets.
- The old repo's lock-file model is gone. Other processes reach the node through a client; they don't open its storage.

## Revision 2026-11-06

The sections above are the design. This section records what was built on `feat/big-node` where it differs from or adds to them.

### Credential header

The caller's credential is no longer a body field. It is `Authorization: Bearer <authority>.<signer>`, carried in `Envelope.metadata.authorization` (`Authorization::{header_value, from_header_value, metadata, from_metadata}` in `big_cabinet_core`; `BearerToken` is gone).
- Dispatch admits the caller with that credential and passes the admitted `Caller.authorization` to `ServiceProvider`, which sets it as the service request's metadata. One credential therefore satisfies both dispatch and the service.
- Resource capabilities (`capability: CapId` on doc, drawer and blob data operations) stay in the body, because they designate what is acted on, not who acts.
- A missing or malformed credential is `ContextError::Unauthenticated`, which every service error carries. The XRPC edge maps it to 401.
- A dispatch-scoped handle is not understood by a service domain, so a caller holding only one can reach only operations whose body capability it already holds. See [ADR 022](022-authority-actors-capabilities-and-grants.md).

### Listeners

- **UDS.** The acceptor reports `PeerCredentials` (tokio `peer_cred`). `big_node`'s `OwnerOnly` wrapper refuses any other uid before a request is read, and a token-less request over UDS is the operator. The socket directory is mode `0700`; a `0755` parent is refused, a stale socket is replaced, a live one is refused, and stop removes the socket. Peer credentials are behind `cfg(unix)`.
- **TCP.** The listener exists, and a token-less request over it gets 401. Remote clients do not use it by default: they reach the node with HTTP and WebSocket over Iroh (D31). A future TCP need is narrow and terminates TLS at a reverse proxy, so there is no TLS work on this listener.
- `/xrpc/<nsid>` is a fallback in front of dispatch on the existing XRPC server. Dispatch refusals map to 401, 403, 404 and 503. `XrpcClient::unix` is the client.

### Peers

`big_node::peers` is the peers service of §6. `node_identity_seed` was already the Iroh secret key bytes, so the one-key rule is a derivation (`SecretKey::from_bytes(seed)`) plus an assertion at boot.
- The default policy is `Drawers::mirrored` (mirror intent only) bounded by `big_repo::groups_with_member` (keyhive access).
- Keyhive access drives wakeups: `big_node::peers::access` reads `KeyhiveAccessRevisionStore` through a `ConcurrentDeltaWalker` (walker state `{scope_key}:big-node-peers-v1`). A delta recomputes the parts of the peers it touches. The sweep tick only redials.
- A replay subscription reset re-opens under a fresh subscription id (`big_sync_core`), which fixed a stall where a peer's replay of a document created while it was offline never resumed.

### Node-scoped secrets and the socket

Every keyring secret is named by node id (`wrapping.<node-id>`), and `big_node::delete_secrets` lists every entry a node keeps. The socket is `$XDG_RUNTIME_DIR/day/<node-id>.sock`, falling back to a `0700` `/tmp/day-<euid>` directory (`default_socket_dir` in `day_cli/context.rs`). It is not configurable yet. An old unscoped `wrapping` entry is never read, so a node directory booted before this change refuses to boot instead of re-keying; nothing migrates because no release has shipped.

### Built-in `getNodeInfo`

`org.example.daybook.node.getNodeInfo` (built-in provider `builtin:node`) reports the node id, the node's own endpoint ticket and the connected peers. It sits under the retiring `org.example.daybook` prefix.

### The `day` CLI

`day_cli` is the node's first client. It drives a node in-process or over the daemon's socket, through the same router. `day init` is idempotent: on an existing node it prints `node <id> already initialized` and exits 0, and a `node.json` that is not a valid node is an error. `day status` and `day peers ls` read `getNodeInfo`.

### Bootstrap peers

`node.json` has an optional `peers` section (`bootstrap` endpoint tickets, `loopbackOnly`). When it is present the daemon attaches the peers service and dials the list at start. Which peers connect automatically beyond that is composition policy (§6). `day peers add <ticket>` edits `node.json` and takes effect at the next daemon start.

### Still open

- **Windows.** No local transport or admission rule. Collected in [windows-todo.md](../scratch/windows-todo.md) and to land as one PR on a Windows machine.
- **GET/query operations** through the `/xrpc` fallback are not served yet (for example docs `getDocAvailability`). This is to-do work, not a design question (D31).
- **Dispatch Lexicon validation.** Dispatch does not yet validate operation input and output against the operation's Lexicon, as atproto's XRPC servers do (D31).
- **Narrowing tokens over the wire.** Dispatch `mintCapability` is not served through the router, and client-side token storage is undecided (D22).
- **Node SQL scoping.** The node's own tables (authority, blobs, cabinet, plugs, nodules) are one database per node. Hosting many nodes in one database needs scoping them and namespacing the authority wrapping key.

## Deferred

- **Remote hosting of individual services:** splitting a single node's services across processes or machines. This ADR only keeps the door open (§7).
- **The user node and relay node compositions,** each of which gets its own ADR.
- **Export and import file formats,** and the FUSE layer.
- **Multi-tenant host management:** quotas per tenant, scheduling, and booting nodes on demand from a request.
