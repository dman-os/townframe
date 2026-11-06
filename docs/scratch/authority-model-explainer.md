# How authority works in the cabinet (explainer and proposed ADR 022 rework)

Status: draft for operator review (2026-11-06). ADR 022 is accurate but written from the mechanism outward. It is also stale: since N2b the caller's credential travels in an `Authorization` header on every request, while the ADR says `Authorization` exists only at mint seams. This note explains the model from the user's side first, maps it to RBAC, states what it defends against, and ends with the proposed changes. Once reviewed, it becomes the ADR 022 rewrite.

## The model in one paragraph

Three things are kept apart:
- An **actor** is *who*: a durable identity with a key (the operator, a service, a nodule).
- A **grant** is *what an actor is entitled to*, durably: "actor A may Write drawer D". It is RBAC's role assignment, stored by the domain that owns the resource (drawers own drawer grants).
- A **capability** is *a live handle you present to act*. It is a random id into the node's in-memory table that names an object and some rights, like a file descriptor. You get one by showing an identity plus an entitlement (a grant, or a capability you already hold) to the domain that owns the object.

At use time the domain checks only the handle, not the actor's grants. Grants are consulted once, when the handle is minted.

## The file-descriptor analogy

| Unix | Cabinet |
| --- | --- |
| user / uid | actor |
| file permissions, group membership | grants (drawer access: Relay < Read < Write < Admin) |
| `open(path)` → fd, permission check happens here | mint → capability, the grant check happens here |
| `read(fd)`, `write(fd)`: no permission re-check | resource operation with the capability: no grant re-check |
| fd rights fixed at open (`O_RDONLY`) | capability rights and scope fixed at mint |
| `openat(dirfd, "x")` | a docs capability scoped to drawer D plus `doc_id` in the request |
| `close(fd)` | retract |
| fds die with the process | handles die with the authority instance (restart = reacquire) |
| passing an fd over a Unix socket | handing a `CapId` to another caller (a bearer handle) |

Like an fd, a capability is both the **reference** (which object) and the **authority** (what you may do to it). That is the "capability is the reference" framing.

## Who presents what, per request

Every request carries two things:
- **`Authorization` header: who is calling.** An actor's identity proof (`Bearer <authority>.<signer>`). Dispatch admits the call on it, and services use it for attribution and mint-time checks.
- **`capability` in the body: what the call acts on.** A docs capability for docs operations, a drawer capability for drawer operations, and so on. The body may name the specific object too (`doc_id`), which must fall inside the capability's scope.

A data operation never accepts an identity in place of a capability ("no identity fallback"). The exception is the node operator, the root of the actor tree. It can mint a capability for anything on its node without a grant row (D24), so it never needs a grant to start with. It still mints and presents capabilities like everyone else.

## Mapping to RBAC

| RBAC | Here | Difference |
| --- | --- | --- |
| role (Reader, Editor, Admin) on a resource | grant: `DrawerAccess` on a drawer | the same idea |
| check on every request: does the user have the role? | check once at mint; requests present the minted handle | revoking the grant must also kill handles minted from it: a revocation **gate** (a dependency) does that |
| service accounts with roles | subsystem and nodule actors; a nodule usually has **no** grants and gets a scoped handle per dispatch | least privilege by default: a plug cannot reach anything its caller did not hand it |
| impersonation / "act on behalf of" | a capability carries a **bound signer** (whose key signs the datum claims) separately from the authority it narrows | a runtime can narrow a user's authority while signing as the plug |
| session tokens | capability handles; they are not signed and only mean something to this node's authority | they cannot be verified offline or on another node |

## What it defends against

- **Confused deputy.** A service acts with the exact handle the caller passed, not with its own broad rights. A nodule asked to "write doc X" can only do it if it was handed a handle covering X.
- **Ambient authority.** Nothing works just because of who you are, except the operator's mint power, and even that goes through minting. A plug actor with no grants holds nothing.
- **Stale authority.** Revoking a grant or retracting a handle kills every handle derived from it, across domains: derivation is a dependency DAG and liveness is checked through it. A restart kills all handles.
- **Escalation through narrowing.** Narrowing can only drop rights or shrink scope, never widen. A bound signer cannot be swapped by attenuation.

What it does not defend against:
- **Handle theft.** A handle is a bearer secret, like a session token. Anyone who learns it can use it until it is retracted. Local transports protect it (UDS 0700, peer credentials); remote clients rely on iroh's encrypted transport.
- **Offline or cross-node verification.** A handle means nothing elsewhere. Peers authorize through keyhive membership, not through these handles.

## What is turned around today

1. **Two domains per drawer.** Acting on a drawer's documents takes a docs capability (scope `drawers: {D: Write}`), while managing the drawer takes a drawer capability. Each is minted separately from the same grant.
2. **Holder-side narrowing is rights-only.** Generic attenuation drops rights but cannot shrink scope. Handing a nodule just one document out of a drawer capability takes a docs-domain mint (`MintDocCapability` with the drawer-scoped capability as its source). This matches Fuchsia, where narrowing a directory to a child is `Directory.Open`, a call to the directory's server. But it reads as "ask permission again" because it is a different operation from attenuation.
3. **Vocabulary.** Namespace capabilities, issuance associations, gates and signer bindings are mechanism names that leak into the mental model.

## Proposed changes

1. **One narrowing operation for holders:** `Narrow { capability, rights?, scope? }`, routed to the capability's owning domain. It accepts any scope the domain can prove is a subset (one doc out of a drawer scope, Read out of Write). It replaces both "generic attenuation" and "domain re-mint from a source" in the public API; the mechanism underneath stays. This is the `zx_handle_duplicate` plus `Directory.Open` pair, collapsed into one verb.
2. **Drawer capabilities imply docs access to the drawer's documents.** The docs service accepts a drawer capability at Read or Write as an `openat`-style reference for any document in that drawer, so one handle covers a drawer and its documents. The alternative is minting both together in one call. Either way, users stop juggling two handles for one drawer.
3. **Rename for readers:** identity proof → *identity*, domain capability → *capability*, standing grant → *grant*, gate → *revocation dependency*. Keep the mechanism terms inside the ADR's implementation section only.
4. **Later, for remote clients:** offline-narrowable tokens (macaroons, biscuit or UCAN) layered on `CapId`, only if iroh clients need to narrow without a round trip.

## Decided

- No expiry on capabilities (operator, 2026-11-06): the ocap system has no clocks. Handles live until retracted or the authority restarts. Dispatch's call deadlines bound a call's duration, not its authority.

## Open questions for the operator

- Change 2: one handle per drawer that the docs service also accepts, or a mint that returns both?
