# ADR 016: Typed piece interfaces with XRPC network faces

**Status:** Deferred. This draft is not accepted and is not a prerequisite for distributed triage. A separate PR will design XRPC properly; current implementation uses the existing IRPC pattern.

## Context

Daybook is moving toward node compositions of independently addressable pieces. Task-pool coordination and Cabinet both need cross-node APIs. Internal calls must not serialize Rust values to JSON merely to share network schemas. Existing IRPC supports typed in-process calls and postcard-based cross-process/network communication; postcard positional structs do not inherit JSON unknown-field compatibility.

The drawer FDR in the parallel drawers work specifies Lexicons/XRPC as a node API convention without requiring an ATProto PDS or full ATProto identity stack. This ADR records the shared interface direction, not a drawer implementation dependency or an implementation of a new RPC framework.

References: [HTTP API/XRPC](https://atproto.com/specs/xrpc), [Lexicon](https://atproto.com/specs/lexicon), [Event Stream](https://atproto.com/specs/event-stream). XRPC defines HTTP query/procedure conventions; Lexicons also describe subscriptions, whose initial ATProto transport uses WebSocket and binary event-stream framing. A Rust method name plus arbitrary JSON over a QUIC stream is not automatically standard XRPC.

## Decision

### Typed contract, multiple adapters

Use typed request, response, and error values shared by local service handlers and network adapters where their semantics agree. Local IRPC passes owned Rust values without a JSON string or serde_json::Value roundtrip. Existing postcard IPC can remain an internal face with its own compatibility gate. Network XRPC decodes and validates Lexicon input at the edge, invokes the typed handler, and encodes the typed result at the edge.

The logical layout is:

```text
Typed caller -- local IRPC ---- typed piece handler
XRPC caller -- network adapter -- typed piece handler
```

These are adapters over common operations, not XRPC tunneled through IRPC. The local call still supplies authenticated/local caller context; bypassing network serialization must not bypass authorization. Network-only session/framing details need not be forced into every local request type.

Keep one contract owner per piece. Do not introduce a central god-like node API or a second implementation of task semantics. Generation from Lexicons versus handwritten Rust types with conformance fixtures remains a tooling decision; no generator is presumed available.

### Network semantics

Lexicon NSIDs identify operations and schemas. Queries are observational; procedures mutate. Define structured errors, bounds, and optional versus required fields deliberately. Optional additive fields can support evolution only when older readers ignore them and older behavior remains safe. Required placement, authority, idempotency, or execution semantics must never be smuggled into ignored optional hints. Unknown operation names and unsupported required semantics must be reported, not silently treated as success.

Do not require PDS hosting, DIDs, OAuth, or ATProto account authentication merely to adopt the API convention. Daybook must separately specify its authenticated node/principal binding and pool-specific authorization. Transport reachability identity and Daybook node identity may differ.

Task sessions require registration, current-attempt introduction, offers, acceptance/decline, start ordering, cancellation, liveness, and outcome hints. Decide whether these use procedures with session identifiers, subscriptions plus procedures, or another explicitly documented session binding. JSON HTTP request/response alone does not supply bidirectional allocation sessions.

### Compatibility and elections

A transport ALPN identifies a transport protocol, not necessarily every Lexicon operation or application capability. Advertise supported scheduling protocol capabilities in authenticated discovery/session establishment. Persisted signed task/slot schemas are separate from API versions.

Pool routing must consider compatibility before election participation and allocation. Filtering direct peer subscriptions alone is insufficient: an incompatible claim can be retained locally or forwarded by a bridge that speaks multiple versions. Relays may retain encrypted data without implementing scheduling APIs at all.

If incompatible scheduling cohorts elect independently, give each cohort explicit router-control scope/identity (router slot, control part, and heartbeat rendezvous), or specify an equivalently consistent eligibility projection. Shared task tickets and domain settlement may still synchronize where their schemas are understood. Independent cohorts can duplicate attempts, just as partitions can. Do not change task identity merely because the live RPC version changes.

The existing single router slot per pool contract requires amendment if compatibility cohorts are chosen. No such amendment is considered accepted merely by writing this ADR.

## Consequences

- Local typed calls avoid needless JSON allocations and preserve internal IRPC usage.
- Network API evolution is explicit and no longer depends on postcard tuple compatibility.
- Shared schemas do not eliminate authentication, semantic versioning, or session design.
- Generic XRPC infrastructure must serve task coordination and future Cabinet APIs without requiring Cabinet to land first.

## Open decisions before implementation

1. Standard HTTP XRPC over iroh versus a separately specified Lexicon RPC stream binding. Standard XRPC offers direct interoperability; a custom stream binding is not presented as standard HTTP XRPC.
2. Task allocation session transport and reconnection semantics.
3. Daybook authentication/replay protection and mapping of endpoint identity to node/principal authority.
4. Lexicon/Rust contract ownership and generation tooling.
5. Compatibility cohorts versus a shared compatible router policy during rollout; concrete router-control identifiers if cohorts are selected.

## Verification requirements

Exercise the same typed operation through local IRPC and network XRPC with equivalent domain outcomes and authorization. Validate old/new optional-field behavior, structured errors, unsupported required semantics, and session reconnection. Test incompatible router claims arriving indirectly through a compatible bridge, passive relay retention, and shared settlement cancelling duplicate cohort attempts. This document records requirements, not completed verification.
