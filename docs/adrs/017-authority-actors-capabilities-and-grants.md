# ADR 017: Authority actors, capabilities, and grants

**Status:** Proposed

Authority is a **service**, not a repository object. It is declared once as
`AuthorityProtocol` and reached through `service_utils_rs::Client` over direct
calls, IRPC, and XRPC — the same hosted handler on every transport. No consumer
holds the state; a domain service holds a client and a capability.

`AuthorityService` owns four parts that stay independent inside it, so each can
be lifted out later without untangling the others: a capability table, an actor
registry, a grant ledger, and a signer.

## Capabilities

There is one kind of ocap: an opaque random UUIDv4 handle to a table row. It is
like a file descriptor, not a signed claim. The stored row is
`{ id, object, rights, parent, issuer }`; the protocol exposes no `CapabilityKind`
and the generic table has no actor binding. Inspection exposes only the presented
id, object, rights, and issuer. It must not disclose parent handles: those are
bearer authority, not harmless provenance. Issuer UUIDs are non-authorizing
namespace identifiers, not handles.

Rights are `use`, `narrow`, `delegate`, and `mintChild`. Their domain meaning
belongs to the service that issued the ocap. Generic attenuation preserves the
object and issuer and only drops rights, regardless of the object's spelling.
Only the trusted minter seam may name a new object. The seam recognizes the
subsystem using AuthorityActors' exact issuance record, not its object's text or
an ancestor's identity.

Every child has a subset of its parent's rights. A handle is live while it and
all its ancestors are unretracted; itself or an ancestor may retract it. The
operator root cannot be retracted. Issuance refuses a child beyond the supported
chain depth rather than returning an unusable handle.

Containment (`edit doc X` under `drawer D`) remains the minting domain's decision.
An object named `is actor docs` by another subsystem is just that subsystem's
object; names confer no authority in AuthorityActors' namespace.

## Actor registry

**AuthorityActors** is the subsystem that issues `is actor <path>` ocaps. It keeps
an issuance index `(cap_id, actor_uuid)` beside the actors table. The operator's
bootstrap handle is associated there too. Its issuer namespace is persisted at
boot and is distinct from the operator and other actors' namespaces.

Resolving an actor, signing, opening a child, or entering a grant namespace first
checks that the supplied handle is live, then looks up that **exact id** in this
index and checks the required right. Neither matching text, matching issuer
metadata, nor ancestry substitutes for this lookup. Generic attenuation can copy
an `is actor` object's text and issuer, but does not copy its registry association.
Only AuthorityActors issues recognized identity handles. This is subsystem policy,
not a second capability type.

Here, “actor cap” is only shorthand for an ordinary ocap in that issuance index.
AuthorityActors uses `use` for identification and signing, `mintChild` for child
opening, `narrow` for the subsystem's minter seam, and `delegate` for namespace
administration. Those bits on an unrecognized handle cannot invoke these powers.

An actor is a persistent identity: a `Uuid7`, an actor URL
`db+actor://<node>/<path>`, an Ed25519 key, and a node-signed attribution
certificate (ADR 015 §5) binding the three. One key per actor.

Paths are hierarchical and the operator is their root, at the empty path. A
child's path is its parent's path plus exactly one segment, so a path names its
parent and two parents never contend for a child. `openActor` creates the actor
on first use and returns the same identity on every later open. The actor cap it
returns is a handle on that identity: an open with the same parent cap and rights
reuses a live one, otherwise it mints another. A parent rotates a child's cap by
retracting it and opening again; the child's key, URL, and certificate do not
change. A child's actor cap holds a subset of its parent's rights.

Actor and node seeds are sealed with ChaCha20-Poly1305 under one wrapping key in
the OS keyring and stored in the registry's own rows, bound to their row by the
AEAD's associated data. Creating an actor is therefore one SQLite transaction:
no key exists without its registry row and there is no cross-store state to
reconcile. A keyring that no longer holds the wrapping key is a boot failure, not
a reason to mint fresh identities.

## Grant ledger

Caps say what; the ledger says **who**. A grant is a standing entitlement: actor
`subject` may hold `rights` over `object` in the namespace of the actor that named
the object. Rows are `(issuer, subject, object, rights, granted via)`; object
strings carry the domain's verb (`write drawer D`), so the rights are the same
generic vocabulary caps carry and attenuation stays mechanical.

- **Writing.** `grantAccess` needs `delegate` on the presented cap.
  - Through the issuer's own actor cap: any object it may name, with rights
    inside its cap's rights. The grant stands until the issuer revokes it.
  - Through an object cap: exactly that cap's object, with rights inside the
    cap's. The grant records the cap it was written through and stands only
    while that cap is live, so retracting a cap withdraws the grants it made.
    This is how an issuer lets a user share what it gave them without the user
    ever touching the issuer's namespace directly.
  - Granting again through the same cap replaces the rights it recorded.
- **Revoking.** `revokeAccess` needs `delegate`. The issuer's actor cap withdraws
  every grant of the object to the subject; an object cap withdraws the grants
  written through it or through anything derived from it.
- **Reading.** `checkGrant` returns the union of the subject's standing grants
  for the exact object, and only the issuer may ask, with its own actor cap and
  `use`. No object implies another and grants do not pass from a parent actor to
  its children; containment and inheritance are the issuer's to resolve, because
  the issuer is the one that mints from the answer.

Subjects are registered actors. Namespace ownership is resolved from the exact
AuthorityActors issuance association, not from the issuer metadata of an identity
ocap (all identity ocaps share AuthorityActors' issuer). Unrecognized derivatives
of AuthorityActors' ocaps cannot write grants in its namespace. A domain's grant
of an object spelled `is actor X` cannot create an AuthorityActors association.

Issuing or revoking a grant never mutates documents and never invalidates
historical attribution (ADR 015 §3). Withdrawing a standing grant changes subsequent
grant checks; already-issued ocaps remain live until explicitly retracted.
Check-and-mint is the domain service's authorization workflow, not one atomic
authority operation. A domain that needs immediate withdrawal must serialize that
workflow against revocation and retract the ocaps it previously issued.

## Signer

`signFacetClaim` needs an actor cap with `use`, requires the claim's `actorUuid`
to be that actor, opens that actor's sealed key for the one signature, and checks
it against the registered public key. This is the only way an actor signature is
produced.

## Composition

The embedder opens each subsystem — DocumentsRepo, DrawerRepo, the runtime — from
the operator root and hands it its actor cap. A plug is an actor under the
runtime (`rt/plugs/ocr`).

To act on a domain, a caller presents its actor cap to the domain service. The
service resolves it (proving the caller is that actor), asks the ledger what that
actor holds on the object *the service* named, decides containment, and mints an
object cap — `edit doc X` — from **its own** actor cap. Later it checks only that
the cap it is shown is live and was issued by itself, and what rights it carries.
The domain object grammar lives in exactly one place, the service that named it.

A facet write by a plug is signed as the plug. Guests never hold actor caps: the
runtime holds the plug's actor cap and passes it with the dispatch, DocumentsRepo
authorizes the write against the object cap the guest presented, and then signs
with the plug's actor cap. The authority cannot tell DocumentsRepo from any other
holder of an actor cap and does not need to: holding an actor's cap is being that
actor, so actor caps stay inside trusted subsystems. A dispatch is an object cap
derived for that dispatch, not an actor.

## Transport and trust boundary

The ocap format is settled: an opaque UUIDv4 handle. Direct, IRPC, and XRPC carry
the same handle back to the authority that owns its table. It is not a
self-contained credential another authority can verify. No signed-capability
format or CWT/COSE wrapper is required or planned for PR4. Attribution certificates
and signed facet claims are separate cryptographic artifacts.

A handle is bearer authority, so its transport and storage must preserve
confidentiality. Debug output redacts it, and inspection never returns ancestors'
handles. The embedder remains responsible for transport admission and for handing
out only intended handles. A URL, actor UUID, or issuer UUID is descriptive metadata
and cannot substitute for an issued handle.

## What this replaces

The v1 `CapabilityToken` — a node-signed, JSON-serialized bearer token naming
its subject and carrying drawer `Grant`s — is deleted, together with its
identity-based lateral-delegation rule and the imperative
`AuthorityRepo::{delegate, authorize, revoke, sign_actor_claim}` surface.
Keyring-held actor keys and their crash-reconciliation intents are replaced by
sealed keys in the registry. There is no backward compatibility: the stack is
unreleased.

## Boundaries

- Chain bookkeeping (which actor issued an object, which cap a grant came through)
  is the service's; a holder sees only the cap id and what inspection reports.
- BigRepo/Keyhive node authority is checked below Cabinet and is unaffected by
  anything in this ADR.
