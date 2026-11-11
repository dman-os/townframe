# ADR 045: Method routes and the dev takeover

**Status:** Accepted (manager, 2026-11-12). It carries out the operator's ledger decision D42 ("Substitution"). The mechanism was settled by precedent, as the ledger delegates dispatch design to the manager. Supersedes ADR 028 §3's "drop-in replacement" substitution.

**Related:**
- [ADR 027: cartridges, nodules and operation contracts](027-cartridges-nodules-and-operation-contracts.md)
- [ADR 028: cartridge identity, naming and dependencies](028-cartridge-identity-naming-and-dependencies.md) §3–§5
- [ADR 044: cartridge configuration documents](044-cartridge-configuration-documents.md) (the per-node config document)

## Context

Today a cartridge can *substitute* for another (`setNoduleSubstitution`, the `nodule.substitution/<cartridgeId>` datum). The substitute fills the original's slot and runs as it, with the same actor, grants, data and dispatch provider id (`big_nodule_core::config` `filler`/`resolve_entry`; `big_nodule::providers`). Dependents' pins stay on the slot, and "the substitution is the consent".

The operator rejected inheriting another cartridge's grants and data as a default (D42), and split the feature in two:
1. **Routing:** "this method is served by that provider", generic over built-ins and nodules.
2. **Takeover:** running as another cartridge, for testing a dev build against real data, and only explicitly.

Dispatch's router maps each NSID to every provider that claims it (`big_dispatch::router`). One claim resolves to that provider; several are `NotReady{Ambiguous}`, never settled by order. Nothing today can choose among claimants.

## Decision

### 1. Routes are a dispatch concept

The router gains a route table: an exact NSID, or an authority prefix (whole segments, as `DispatchScope::authority_owns` matches them), mapped to a provider id. `resolve(nsid)` takes the longest matching route first, the way Envoy's prefix routes work:
- if the routed provider claims the NSID, the call goes to that provider's entry, ready or not ready as its state says;
- if it doesn't, the result is `NotReady{RouteTargetMissing{provider}}`, which is retryable;
- with no matching route, today's logic is unchanged: one claim resolves, several are `Ambiguous`.

There is no fallback to another claimant while the routed one isn't ready: silently falling back would defeat the override. Provider ids stay opaque strings, so built-ins and nodules are equal and the router knows no nodule types. A route doesn't change what a caller's scope permits: scopes are keyed by NSID, not provider.

### 2. Storage and who sets routes

A route is the datum `org.example.daybook.dispatch.route/<nsid-or-prefix>` holding `{ provider }`, in the node config document (ADR 044's per-node document). It's per node and not synced, because which providers exist is per node.

The method `setMethodRoute { pattern, provider: Option }` is privileged (`XrpcMethod::privileged`): a route decides where callers' data goes. Until the node has a durable per-node store that isn't the nodule service's, the nodule service serves this method and persists the datum. It also pushes the route table into the router in the same reconcile pass that registers claims, so routes and claims can't disagree. That is the one place the nodule service stands in for dispatch, and it is named here so it can move when another store exists.

### 3. Setting a route is the operator's consent

Like Cargo's `[patch]` in the root manifest, which redirects every dependent at once, the operator's route applies to dependents without each one confirming. The resolution lock of every request covered by the route moves to the routed provider in the same reconcile. `ProviderIndex::offers` for a routed NSID returns only the routed provider, so a route also settles what would otherwise be `Ambiguous`.

### 4. Forks don't need routes; ADR 028 §5 stays

A cartridge still can't be enabled while it holds an authority prefix another enabled cartridge holds. A fork that replaces an abandoned cartridge ships the same NSIDs; the operator disables or uninstalls the original and enables the fork. It is then the only claimant, and dependents see `ProviderChanged`, which is the existing consent point. Routes are for choosing among providers that may coexist, such as a built-in and a nodule that both serve an NSID. "Only a substitution may hold another cartridge's prefix" goes from §5 with substitution.

### 5. The dev takeover

Taking over a slot stays available, but only as a dev hook:
- The datum is renamed `org.example.daybook.nodule.devTakeover/<cartridgeId>` and the method `setNoduleDevTakeover`. It stays privileged.
- The nodule service refuses the method unless `node.json` sets `dev.allow_slot_takeover: true`. That is read at boot, so neither a nodule nor a runtime call can turn it on.
- `day carts dev-takeover <slot> --by <fork>` sets it, and prints what the fork inherits: the actor, every grant including full drawer access, and the data.
- Full inheritance is the hook's purpose (testing a dev build against real data), so drawer grants are not narrowed. Method grants keep today's narrowing (`substituted_grants`), so a dev build can't silently call methods the original wasn't granted.

The existing machinery (`filler`, `code_source`, `resolve_entry`, `FilledBy`) is reused under the new names. No route, install, upgrade or confirmation ever writes a takeover.

## Consequences

- `NoduleSubstitution`/`setNoduleSubstitution` are renamed, with no aliases (pre-release). `big_nodule_core`'s public API is re-blessed.
- `big_dispatch::router` gains the route table, `set_routes`, and `NotReadyReason::RouteTargetMissing`.
- The tests in `big_nodule/e2e/substitution.rs` become dev-takeover tests, plus one test refusing takeover without the flag. "Dependents pinned to the slot stay satisfied" goes; routing tests replace it: an override settles ambiguity, a target that doesn't claim, a target that isn't ready, a built-in routed over a nodule and the reverse, a prefix route against an exact one, and dependents' locks moving with the route.

## Open questions

None for implementation. When the node gains a store of its own, the route datum and `setMethodRoute` move out of the nodule service (§2).
