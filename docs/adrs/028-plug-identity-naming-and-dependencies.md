# ADR 028: Plug identity, naming, dependencies, and malleability

**Status:** Accepted (operator, 2026-11-04). Drafted from the operator design session of 2026-11-04 (decisions D10–D13
of the [runtime ledger](../scratch/nodule-runtime-ledger.md)), reviewed with the Clay
(`feat/better-collections`) agent. Nothing here is implemented yet; the big_plugs work implements it.

**Updated:** 2026-11-04

**Related:** [ADR 007: plug manifests as drawer docs](007-plug-manifests-as-drawer-docs.md),
[ADR 026: drawer-native blobs](026-drawer-native-blobs.md),
[ADR 027: plugs, nodules, and operation contracts](027-plugs-nodules-and-operation-contracts.md),
[docs wiki: branches](../wiki/docs.md), [drawer wiki](../wiki/drawer.md)

## Context

A plug is the one package format for every kind of plugin: nodules (services), Clay micros,
schemas (Lexicons), and kinds not yet designed. Kinds reference each other (a micro calls a
nodule operation; everything shares schemas), which is why they share one namespace and one
package.

Three goals pull against each other:

1. **Malleability, not an app store.** Anyone holding the source may modify any plug,
   including one they received from someone else, and may publish a new plug they control at
   the drop of a hat.
2. **No masquerading.** A modified plug must never pass as an official upgrade of someone
   else's plug.
3. **Auditable security.** It must be statically answerable whether a plug can ever reach a
   given resource (for example a sensitive drawer), directly or through the services it may
   call. Everything is capability-oriented and schema-bound.

There is no global authority. Each node is its own arbiter of what it accepts.

## Decision

### 1. big_plugs is a registry, and runtimes are its node_modules

big_plugs is the catalog of plugs and nothing more, like npm: it versions what comes in and
keeps published versions immutable. It does no enablement. Each runtime (nodules, Clay)
owns enablement, downgrades, grants, bindings and readiness in its own desired-state datums.
The request, grant and necessity *types* are shared across runtimes; the datums are not.

- **The registry is a drawer.** A plug is a document whose versions are immutable entries,
  each with `Blob` datums for its artifacts (ADR 026). The registry drawer is an ordinary
  drawer and may sync between nodes. Syncing it activates nothing.
- **Retention is by reference.** Each runtime's own drawer names the plug documents and blobs
  it enabled, so removing a plug from the registry drops it from that drawer only. The
  enabled copy stays pinned for as long as a runtime references it. There is no lease system.
- **Runtime configuration is node-local.** It syncs elsewhere for durable backup, but syncing
  it never installs or activates anything on another node. Convergence of several nodes onto
  one configuration is a separate layer on top.

### 2. Identity is separate from names and from verification

Identity, resolution and verification are kept apart. A name may *indicate* that something
was verified; it never *dictates* identity. Moving or redelivering a plug never changes what
it is.

- **A plug id is `<namespace>/<name>`.** Both parts are required, and the name is
  human-readable. The namespace is a **DID**, of any method: `did:key` for a bare public key
  (a keyhive key included), `did:plc` for a rotatable identity that needs no domain, `did:web`
  for those who have one. Nothing here is keyhive-specific.
- **Readable namespaces are aliases, as AT Protocol separates DIDs from handles.** A DNS handle
  is verified both ways (the DID document lists the handle, and DNS lists the DID) and is used
  for display only. Nodes may keep local petnames. Global, secure and human-meaningful names
  cannot all be had at once (Zooko's triangle), so a local nickname fills the gap. An alias
  never appears in durable state; only the DID-based id does. Publishing to friends never
  requires controlling DNS.
- **The publisher is whoever holds the namespace DID's keys,** and signs each version over its
  content. Documents carry the signature through document authorship, and OCI carries it as an
  attached signature. Provenance therefore does not depend on how the plug was delivered.
- **Key rotation depends on the DID method.** `did:key` cannot rotate, so it suits development
  and node-local publishers. Long-lived publishers should use a rotatable method.

### 3. Upgrades, forks and substitution

- **Upgrade rule, as Android and Sandstorm do it.** A node accepts a version as an upgrade only
  within the same plug id (so the same publisher), only with a valid signature, and only in
  increasing version order. A validly signed *older* version is never accepted as an upgrade;
  that would be a downgrade attack. A downgrade is an explicit runtime action on one node.
- **Ids never lie, so every fork is a new id.** A modified plug is published under its own
  namespace (`did:me/photos`) with `basedOn: did:up/photos@<version>` for traceability. A fork
  may be a branch document of the upstream plug document, as [docs branches](../wiki/docs.md)
  define them: a separate document whose branch datums pin the parent and the heads it
  branched from.
- **Drop-in replacement is an explicit node-local substitution,** as Cargo's `[patch]` does it:
  "references to `did:up/photos` are satisfied by `did:me/photos`". The fork's code is
  installed *as* the upstream id. Slot state stays keyed by the upstream id: durable rows,
  endpoints `(micro, publication, contract)`, registration scopes, enablement and bindings. The
  install datum records `filledBy: did:me/photos@<version>`. Rows the fork writes into the slot
  continue upstream's rows: writer identity is (replica, installed-as id), not the code's plug
  id. Removing the substitution is a reinstall of upstream into the same slot. The
  installation-scope rows are rewritten transactionally, endpoints to publications upstream
  lacks report `Unavailable`, and nothing is orphaned.
- **Coexistence costs a rename.** A fork enabled on its own, not substituting, is its own slot
  under its own id. It must rename every NSID it changes, and dependents must be explicitly
  rebound to it.
- **Grants never widen silently.** A substitution or an upgrade is re-checked against the set of
  granted *requests*, not against binding names. Any request not covered by existing grants
  (a new binding, read widened to write, a changed contract) comes up unresolved and needs
  configuration. Enablement is the user's choice and survives both. A widened *required*
  request leaves the plug enabled but not startable until configured (Clay's `needs
  configuration` state). A widened *optional* request stays unbound until granted. Nothing is
  disabled behind the user's back, and nothing gains access without consent.

### 4. Dependencies name what is used, not who provides it

Prior art: OSGi's `Import-Package` over `Require-Bundle`, and Debian's virtual packages
(`Provides:`). Naming the API rather than the provider is what keeps forks drop-in.

- **Manifests request NSIDs.** Operations, datum schemas, collection contracts and Clay
  contracts are named by NSID, in the shared request shape
  `{binding, contract: <nsid>, access, necessity, resolution, minRevision?}`:
  - `binding` is the plug-local name code uses to reach the grant.
  - `access` is an open vocabulary per contract kind (`read`/`write` for data contracts; absent
    for an operation or publication requirement, where the grant is "may call" or "may mount").
  - `necessity` is `required` or `optional`; an unknown member is treated as `required`.
  - `resolution` is `configured` (configuration binds it, with manifest-derived defaults) or
    `at-runtime` (bound later by user consent through a picker, issuing the same handle).

  Whichever enabled provider offers the NSID satisfies the request: upstream, a substituting
  fork, or an independent reimplementation. The concrete provider, collection or document is
  configuration.
- **Compatibility lives in the NSID, following AT Protocol.** A breaking change means a new NSID.
  Within one NSID a schema only grows compatibly. A dependency on a newer feature is
  `{nsid, minRevision}`, using the Lexicon `revision` integer, not a provider's semver.
- **Plugs carry the Lexicons they depend on,** not only their names, as Protobuf consumers carry
  their `.proto` copies. An incompatible view of an NSID is then detected when the plug is
  catalogued, not at runtime. AT Protocol Lexicon resolution becomes the arbiter between copies
  once it is built.
- **Resolution is recorded at enable time,** in the runtime's install datum, like a lockfile:
  each NSID maps to its provider plug id, version and schema revision. A change of provider is a
  change of grant and is re-consented.
- **Strictness is at the NSID level.** Every NSID a plug touches outside its own prefix must
  appear in its manifest requests, or admission refuses. Broad untyped access (any datum in a
  drawer or document) may be added later, but only as an explicit manifest request that an
  audit can see. The default is least access. NSIDs a runtime itself defines (Clay's
  presentation-component contract and shell collections such as `shell.roots`) are requested
  like any other, so the audit sees them, but are satisfied by the runtime rather than by a
  plug.
- **Plug-to-plug dependencies are a delivery hint only** (for example "also fetch
  `did:x/photos ^1.2`"), for a future layer that fetches and resolves plugs. They never decide
  who provides an NSID.
- **A manifest never pins another plug by content digest.** The one plug-level reference allowed
  is byte-level reuse of another plug's artifact (a shared component or asset), named by plug id
  plus artifact key and always resolved through the substitution table. Digests appear only in
  lockfile and install datums.
- **The runtime's host ABI is not a plug dependency.** A WIT world a runtime defines
  (Clay's `townframe:dayframe`) constrains a plug to a runtime version, through the WIT package
  version and the manifest schema field. It never links one plug to another.

### 5. NSIDs and authority

NSIDs stay valid AT Protocol names (reverse DNS), so we can build on AT Protocol Lexicon
resolution. On a node, the "authority" over a prefix is the enabled plug holding it, and only a
substitution may hold another plug's prefix. Two holders without a substitution are refused at
enablement. A fork may implement upstream's NSIDs unchanged, but a changed schema needs a new
NSID under the fork's own prefix: implementing an NSID is not owning it.

An NSID whose domain the author does not control is unverifiable but legal. It works on the
author's node and their friends' nodes, and vendored Lexicons catch conflicts. Publishing a
Lexicon globally is optional. Verified names without one's own domain can come from hosted
per-user namespaces, as Maven Central verifies `io.github.<user>` against a GitHub account.

Verifying a prefix through DNS may later *indicate* legitimacy; it never gates.

## Consequences

- The nodule and Clay manifests lose their own `id` fields (`Nodule.id`,
  `MicroManifest.id` / `package_version`); the envelope's plug id is authoritative. Clay
  contract and schema ids become NSIDs. Nodule `requires` adopts the shared request shape plus
  `minRevision`.
- Clay drops `FacetAccess.owner_plug_id` (the request names the facet's datum NSID, and
  configuration picks the document) and makes contributions target a local binding instead of a
  concrete collection id. Both were plug-level references that broke under substitution. Clay's
  micro id becomes the plug id it is installed *as*, and its causal-map writer identity becomes
  (replica, micro id), so stamps stay totally ordered across nodes and substitutions.
- Install datums for both runtimes record the lineage they run (`filledBy`, version,
  resolution) next to `enabled`, so the effective state is auditable.
- Digests align on BLAKE3, as big_blobs uses.
- A schema registry keyed by NSID, fed by catalogued plugs, sits on top of Cabinet's
  `SchemaRepo`. Cabinet keeps "identifiers and shapes" and never learns who owns a schema.

## Deferred

- Fetching and resolving referenced plugs over the network (the "delivery hint" layer).
- AT Protocol Lexicon resolution, and DNS verification of handles and NSID prefixes.
- Reserving historical NSIDs and other backstops against reusing a retired prefix.
- Multi-node configuration convergence.
- Checking Lexicon compatibility between revisions beyond the vendored-copy comparison.

## Open questions

- Should a plug's micro section ever carry more than one micro? If so, micro ids become
  `<plug id>#<micro>`.
- What concrete signature format should OCI deliveries carry (cosign, Notary)?
