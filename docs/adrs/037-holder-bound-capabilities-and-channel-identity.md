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
   - **Decided (operator, 2026-11-10):** a node identity is not an actor identity; they are different levels. The transport authenticates the peer *node* (for iroh, its endpoint key), and the node-to-actor mapping is a separate record that the operator creates when admitting the peer, owned by `big_node::peers` (ADR 033 §6). Nothing else assumes iroh: any transport qualifies once it yields an authenticated peer identity.
2. **Acting as a sub-actor on the same channel (decided, operator 2026-11-10).** Harness hooks run the `day` CLI as the operator's uid, so captures by different agents all arrive as the operator, but attribution wants the agent (the datum's signing actor). A per-connection "act as X" handle doesn't fit, because each CLI invocation is a new connection. Proposal: a request may name a *descendant* of the channel's actor to act as (an actor path such as `operator/agents/<session>`), and dispatch attributes the call to that descendant. It can only narrow, because a channel can act only within its own subtree of the actor tree (ADR 022), so it is not identity-as-secret. Precedent: Kubernetes impersonation headers, restricted here to the caller's own descendants. Grants still decide what the descendant can do. The rule is general, not an operator special case: each channel authenticates exactly one actor, and that channel is trusted for that actor's descendants and nothing else. The operator can act as any of its subsystems because they are its descendants, not because it is the operator.
3. **Re-minting cost:** one mint per handle per hop. Measure it once dispatch does it; batch it if it shows.
4. **How a subject is named. Settled by question 2's answer (manager, 2026-11-10): P2.** Acting as a descendant names that actor by its public path or id, so signing has to work by actor id through the authority, without the domain holding the actor's capability. That is P2; P1 and P3 both leave identity as a capability that domains hold. The original analysis: Implementing (a0) found that today an actor's identity *is* a capability, and the data plane signs with it: a domain handle stores the subject's actor capability (`BoundScope.signer: CapId`), which is a revocation dependency, the input to `ResolveActor`, and what `SignDatumClaim { actor: CapId }` signs with. Naming a subject by public actor id therefore needs an id-to-capability lookup, which is ambiguous (`actor_caps` is keyed by actor, parent and rights, so one actor has several capabilities) and leaks the identity secret to whichever domain asks. Options:
   - **P1:** one canonical capability per actor, plus a gated `ResolveSubject(actor_id)`. This is the smallest change, but the secret still reaches domains.
   - **P2 (recommended; it is this ADR's thesis):** handles store the subject's public actor id; signing goes through the authority by actor id, gated by the domain's capability; revocation becomes an authority-side actor-liveness dependency. No actor secret reaches a domain. This is a larger change to authority `mint`, `sign` and `resolve_actor`.
   - **P3:** an authority `IntroduceActor { holder_handle, subject } -> CapId` returning an opaque acts-as capability stored as `signer`. It is small, but keeps identity-as-capability, the blur this ADR set out to remove.
   Also to decide: `subject: Option<Uuid7>` (where `None` means the presenter) versus required. Until this is decided, (a0), (a1) and (a2) wait; (a1)'s attempted patch is saved in the session scratchpad.
5. **Order of work:** (a0) explicit introduction as a mint primitive: today delegation works by presenting someone else's handle under one's own signer, which rebinds the result (`Rights::DELEGATE`, and the nodule service re-minting its dispatch authority for each nodule), and holder binding forbids exactly that. So first, a mint names its subject: the holder presents its own handle (holder-checked) and names the subject actor by its public actor id; the minted handle is bound to the subject, no wider than the holder's, and depends on it. `DELEGATE` keeps its meaning as the right to introduce. The nodule service holds a dispatch authority bound to its own actor and mints per nodule naming the nodule's actor. This is §3's introduction exposed as an operation; dispatch's implicit re-minting in (c) builds on it. Then (a1) holder binding where the presenter is already known: the shared `DomainCapabilities::source` path (dispatch, nodules, cartridges, and the docs and drawers mint methods), with dispatch's own check folded into it; (b) channel identity and the one-member `Bearer` header, carrying the verified caller in the request context, set only by the channel layer (listener, dispatch, guest host) and never parsed from a client header; (a2) holder binding in the docs and drawers data plane, which today resolves a bare handle id with no presenter (bearer by construction, so it cannot be enforced before (b)), and identities bound to their own actor; (c) the capability-ref object and re-minting in dispatch, after D41's validator lands. Found by the step (a) inventory (2026-11-11): the ~27 docs and drawers request types carry only `capability: CapId`, so the presenter must come from the context, not from new request fields.
