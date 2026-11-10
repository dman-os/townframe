# ADR 037: Holder-bound capabilities and channel identity

**Status:** Accepted (operator, 2026-11-11). Settled in conversation; the operator approved the design without a line-by-line review, so read §6 for what is still open.

**Related:**
- [ADR 022: authority, actors, capabilities and grants](022-authority-actors-capabilities-and-grants.md) (amended by this ADR)
- [ADR 033: what a node is](033-what-a-node-is.md)
- Ledger D39 (one XRPC header contract) and D41 (method Lexicons), in [the runtime ledger](../scratch/nodule-runtime-ledger.md)

## Context

ADR 022 made a capability an opaque random UUIDv4 handle into the authority's in-memory table: a sparse ("password") capability. Sparse capabilities keep most object-capability properties: no ambient authority, designation is authority (the confused-deputy defence), attenuation, and revocation through derivation dependencies. They lose one: a copied number is a working capability. ADR 022 listed handle theft as undefended.

Two parts of the design made this worse than it needed to be:
- **Resource handles are bearer.** Dispatch refuses a handle presented by anyone but its holder (`big_dispatch/lib.rs`, the bound-signer check), but the docs, drawers and blobs domains record the bound signer without enforcing it. A doc handle leaked into a log works for any actor.
- **Identity travels as a secret.** The caller's credential was `Bearer <authority>.<signer>`, and the signer is itself an actor handle. Whoever holds both ids acts as that actor. Over plain TCP this is an API key.

The property we want is the one a file descriptor has: holding a reference is the security. A module that received a handle can use it, log it, or lose it, and none of that grants anyone else anything. Modules then need no security management of their own.

## Decision

The model is Cap'n Proto RPC's (and OCapN/CapTP's): a capability is an entry in a table that belongs to its holder, meaningless to anyone else, and passing one in a message gives the receiver its own entry.

### 1. Identity comes from the channel, never from the request

The caller's actor is established by the channel the request arrives on. The request carries only the authority it uses: `Authorization: Bearer <handle id>`, on every hop, as D39 already does for nodule guests.

| Channel | Who it proves |
|---|---|
| Unix socket | the operator, by the kernel's peer uid |
| iroh connection | the peer node, by its node key; each authenticated remote node is an actor |
| Nodule workload | the nodule's actor, from the host's workload table |
| In-process built-in service | its own actor |
| Native test harness (`big_ndk_testing`) | the actor bound at `ensure` |
| Plain TCP (HTTP) | nobody: anonymous, and limited to public reads |

Plain HTTP is stateless, so it has no per-connection table to bind to. If TCP ever needs authenticated callers, the precedents are a bootstrap `login` method that returns a scoped handle (Cap'n Proto) or proof-of-possession tokens (DPoP, RFC 9449). Neither is needed for the alpha.

### 2. Every handle is bound to its holder, in every domain

A handle is minted for one actor, and every domain refuses it when presented by anyone else, as dispatch does today. A leaked id is a dead number. The table stays in memory with one live row per handle (no revocation list): retracting removes the row and everything derived from it, and a restart kills every handle, as before.

### 3. Handles in messages are re-minted for the receiver

When a request or response carries a capability, dispatch replaces it with a handle minted for the receiver: no more rights than the sender's, and dependent on the sender's handle, so revoking the sender's handle revokes the receiver's. This is the introduction; it needs no new verb, because it happens on every call. A nodule that receives a caller's drawer handle gets a handle bound to the nodule, so the caller-authority rule (a nodule writes with the handle its caller passed) keeps working, and the nodule cannot pass the raw id anywhere useful.

Dispatch finds capability fields through the method's Lexicon (D41): a capability field is an object `{ "$type": "org.example.daybook.capability#ref", "id": "<handle id>" }`. This is plain atproto Lexicon (an object ref), so our Lexicons stay valid atproto.

### 4. Lifetime follows the minting context

A handle minted for a connection dies when the connection closes. A nodule's handles live as long as its workload. Everything dies at restart.

### 5. Durable authority stays in grants

There are no sturdyrefs (durable bearer references). Every durable case today is a grant re-minted into a live handle when its holder connects or boots: a nodule's standing access, the operator, and installing a cartridge with the caller's authority (D24). The first real sturdyref use case would be a shareable link to non-public content. Public content does not need one, because public reads use keyhive's app-public keys, not handles.

## Consequences

- The credential header changes on the Unix socket and iroh: `Bearer <authority>.<signer>` becomes `Bearer <handle id>`, and `Authorization` loses its `signer` member. This is a breaking change to clients and tests, accepted pre-release.
- Every domain's handle resolution checks the presenter against the holder. Domains that have relied on bearer behaviour (for example a service handing a handle to another actor out of band) must mint for that actor instead.
- Dispatch needs to see capability fields, so it depends on method Lexicons (D41) and the capability-ref object. Until a method's Lexicon marks its capability fields, handles passed through it are not re-minted, and the receiver's use of them is refused. That's correct, not merely safe: the method has to be updated before it can carry handles.
- Micros (Clay) carry capabilities beside values (ADR 029). The capability-ref object is the shared spelling for both.

## Open questions

1. **The actor for a remote node over iroh:** one actor per peer node key, created when the operator first admits the peer, is the proposal. It needs the peers service (ADR 033) to own that mapping.
2. **Acting as a sub-actor on the same channel (proposal).** Harness hooks run the `day` CLI as the operator's uid, so captures by different agents all arrive as the operator, but attribution wants the agent (the datum's signing actor). A per-connection "act as X" handle doesn't fit, because each CLI invocation is a new connection. Proposal: a request may name a *descendant* of the channel's actor to act as (an actor path such as `operator/agents/<session>`), and dispatch attributes the call to that descendant. It can only narrow, because a channel can act only within its own subtree of the actor tree (ADR 022), so it is not identity-as-secret. Precedent: Kubernetes impersonation headers, restricted here to the caller's own descendants. Grants still decide what the descendant can do.
3. **Re-minting cost:** one mint per handle per hop. Measure it once dispatch does it; batch it if it shows.
4. **Order of work:** (a) holder binding in every domain; (b) channel identity and the one-member `Bearer` header; (c) the capability-ref object and re-minting in dispatch, after D41's validator lands.
