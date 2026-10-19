# ADR 017: Authority actors, capabilities, and grants

**Status:** Proposed

`AuthorityRepo` is one service with four parts that stay independent inside it,
so each can be lifted out later without untangling the others. The service owns
all four; none of them knows the vocabulary of the layers above it.

## The four parts

1. **Capability table.** A capability (cap) is a row `{ id, object, rights,
   parent, root_actor? }`. `object` is an opaque string; `rights` is a subset of
   a small generic vocabulary (`Use`, `Narrow`, `MintChild`, `Delegate`).
   `AuthorityRepo` mints a child only when the parent row says the mint is
   allowed and the child's rights are a subset of the parent's. It checks,
   mechanically, that a presented cap is a row it issued and that every step of
   its parent chain narrowed or held. Whether one *object* is narrower than
   another (`doc:X` under `drawer:D`) is the minting domain's decision, made
   before the mint call — cap-to-cap scope attenuation is not something the table
   adjudicates.

2. **Actor registry.** An actor is a persistent subsystem identity: a `Uuid7`, an
   actor URL of the form `db+actor://<node>/<path>`, an Ed25519 public key held
   in `secrets_rs`, and a node-signed attribution certificate (ADR 015 §5)
   binding the three. The same key may hold several UUID bindings, one per URL.
   The returned cap for a subsystem is the "is subsystem `<path>`" cap, rooted at
   that actor. The registry answers exactly two questions about a presented cap:
   did I mint this subsystem cap, and which actor does it root at.

3. **Grant ledger.** Rows of `(subject, object, access)`, all opaque strings.
   The only operation is "does S have access A on O". Matching is exact; any
   containment — a doc inside a drawer, a child drawer inside a parent — is
   resolved by the caller before it asks. Revocation is deleting a row. This is
   the only persistent authorization state; caps derived from a row are not
   ledger state, and the ledger never learns what an object string means.

4. **Signer.** `sign_as(cap, bytes)` walks the cap's chain to its `root_actor`
   and signs with that actor's key. A cap with no root actor cannot sign. This is
   the only way an actor signature is produced.

## Composition

At boot the service returns the root cap: "is operator". A subsystem cap is
minted from it (`MintChild` on the parent). To act, the holder gives its "is
subsystem X" cap to a domain service — DocumentsRepo, DrawerRepo, the runtime.
That service asks the registry whether the cap is a real subsystem cap, reads
the root actor, asks the ledger whether that actor may do the thing on the
object *it* named, and if so mints its own cap — "edit doc X", "branch doc in
drawer D" — parented to **its own** subsystem cap, not to the caller's. The
domain service later only checks that it minted the cap it is shown; the domain
object grammar lives in exactly one place, the service that named it. This is
what keeps drawer and branch rules out of `AuthorityRepo`.

DocumentsRepo is the only caller of `sign_as` for facet claims: it evaluates
the presented cap itself (object, rights, and the drawer membership that
authorizes the claim's document), then signs. Which drawer authorizes which
document is DocumentsRepo's fact, because dmeta and rosters are its domain; the
authority service neither knows nor checks it.

## What this replaces

The v1 `CapabilityToken` — a node-signed, JSON-serialized bearer token naming
its subject and carrying drawer `Grant`s — is deleted, together with the
lateral-delegation rule (a subject holding any live token could not be
delegated to again). That rule was identity-based and is incompatible with an
actor holding many caps; downward-only delegation now follows from subset
checking plus the registry's control of subsystem minting. Local caps are rows
in the table, not signatures: the table is the check, which is why this design
prefers it over portable signed cap tokens. A portable form, if a cap must
cross a node boundary later, is a node-signed CWT/COSE reference to a table row
— the same envelope family as the attribution certificate — not a second token
format, and it is not built here.

There is no backward compatibility: the stack is unreleased, old token and
revocation rows are dropped at boot, and actor keys survive because they live in
`secrets_rs` independently of this crate's tables.

## Boundaries

- Caps say what can be done, not who holds them. Chain bookkeeping (which actor
  a cap roots at, which subsystem minted it) is the service's, invisible to the
  holder beyond the cap id.
- A dispatch is a cap, not an actor. Actors are subsystems; per-dispatch caps
  are derived from the subsystem cap and the dispatch record and are not
  persisted as grant state.
- Issuing or revoking a grant never mutates documents and never invalidates
  historical attribution (ADR 015 §3).
- BigRepo/Keyhive node authority is checked below Cabinet and is unaffected by
  anything in this ADR.
