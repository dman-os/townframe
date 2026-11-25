# Cross-cartridge dependencies: a catalogue

Status: research note, nothing decided
Owner: nodules lane
Updated: 2026-11-24
Scope: Catalogue of what one cartridge depends on from another, direct versus NSID-based, with co-provision options, for operator review.

The operator asked for a catalogue of everything one cartridge depends on from another, marking which dependencies
name a specific cartridge (**direct**: a cartridge id, an artifact key, a `<DID>/<name>@<range>#<component>`) and which
name an NSID that any enabled provider can satisfy (**NSID-based**), with the provider chosen by configuration: an
operator route (ADR 045), a grant, or the install's recorded resolution (ADR 028 §4). The suspicion behind it is that
ADR 028 §4 ("manifests request NSIDs; cartridge-to-cartridge `dependencies` are delivery hints only") is not uniformly
true.

**Sources.** ADRs 015, 017, 027, 028, 030, 031, 032, 045, 047, 049, 050, 051 (read in full or by grep for the
cross-cartridge sections); code on this branch (`src/big_cartridges_core`, `src/big_nodule_core`, `src/big_dispatch`);
and for Clay, claytxt's own account (tip `claytxt-68`) with the file:line references it gave. This worktree's
`src/clay_mdk/manifest.rs` is the pre-ADR-050 shape (`publications`, no endpoints); the endpoint vocabulary is on
`origin/feat/daytxt`, and claytxt's line numbers refer to that tree. I did not re-verify claytxt's refs against it
line by line; they are marked "(claytxt)".

## Short answer

ADR 028 §4 holds for the **manifest vocabulary** (permbinds name NSIDs) but not for **what is stored and granted**.
Direct, provider-naming references exist in these places:

1. `Cartridge.dependencies` (cartridge id + semver range): a delivery hint, parsed, only checked for self-reference,
   never read by resolution (`big_cartridges_core/lib.rs:92,164`).
2. `grant/cartridge/<cartridgeId>` (`GrantKey::Cartridge`): a grant of "data of cartridge Y" names the provider by id
   (`big_cartridges_core/config.rs:161,249`). Stored in the cartridge config document, which syncs to sister nodes.
   "Nothing enforces these yet" (`big_nodule_core/config.rs:739`).
3. The lockfile (`resolution_locks`, `LockedProvider.provider`): the provider cartridge id, version and revision per
   method or datum request, node-local, recorded at enable time (`big_nodule_core/providers.rs:34`).
4. The route table (`dispatch.route/<pattern>` to a provider id): the operator's direct naming, by design (ADR 045).
5. The dev takeover (`devTakeover/<cartridgeId>`, `filledBy`): direct, dev-only (ADR 045 §5).
6. `#hosted` component `runtime` (`<DID>/<name>@<range>#<component>`): direct, **proposed and not accepted** (ADR 047
   amendment B). No code.
7. Byte-level artifact reuse "by cartridge id plus artifact key" (ADR 028 §4): direct, stated in an ADR, **no manifest
   field and no code**; the "substitution table" it mentions was removed by ADR 045.
8. Public permbind linking and cross-cartridge local state (ADR 051 §5, §3): "A declares a dependency on `B#permbind`".
   Direct by the ADR's wording, **draft, no code**.
9. Clay callback endpoints and the links stored in contribution rows: an endpoint-ref `(cartridge, micro, endpoint)`
   (ADR 050 §3, claytxt): direct, provider-specific by construction, but it points at the **contributor's own**
   endpoint, not at a dependency the contributor chose.
10. The interim review micro names intxt's drawers directly (claytxt; goes away with ADR 052).

Everything else (methods, datum kinds, vendored Lexicons, collections, presentation components, shell collections)
is NSID-based in the manifest. For datum kinds the NSID is **effectively direct** because a datum NSID has exactly one
possible provider: the enabled cartridge holding its authority prefix (below).

The answer to the operator's presentation-component suspicion is: **no, a micro does not name the providing micro**.
It names a contract NSID (claytxt); the supplier is found by scanning installed micros, and nothing records the choice.

## How to read the tables

- **Names** is how the dependent writes the dependency.
- **Kind** is Direct or NSID.
- **Provider chosen by / recorded** is who picks and where the choice lives.
- **Substitutable** is whether a person can swap the provider without editing the dependent.

## 1. Methods and operations (nodule routine to another nodule's operation; micro method endpoint)

| | |
|---|---|
| Names | A `#method` permbind `{ nsid, necessity, minRevision? }` (`big_cartridges_core/manifest.rs:49`; ADR 047 §5). Clay: a `Request { contract: Nsid }` in `requires` (claytxt, `clay_mdk/manifest.rs:379-411`), becoming a permbind named in the micro's `uses` under ADR 050 §4. |
| Kind | NSID. |
| Chosen by | `ProviderIndex::offers` (`big_nodule_core/providers.rs:195`): enabled nodules that implement the NSID, plus native providers; a route covering the NSID leaves only the routed provider. One offer resolves; none is `NoProvider`; several are `Ambiguous` (never decided by order). Dispatch's router does the same at call time (ADR 045 §1). |
| Recorded | Node-local `resolution_locks`, `{contract, provider, version, lexiconRevision}` (ADR 047 §7). A routed request is **not** pinned: the route is the pin (ADR 045 §6). A different cartridge appearing later is `ProviderChanged`, held until the operator confirms. |
| Grant | `grant/method/<nsid>` or `grant/authority/<prefix>`; applied only as the exact requested methods (`bind_operations`, `lib.rs`). A method grant names an NSID, so it survives a provider swap. |
| Substitutable | Yes: disable the claimant, enable another, or set a route. |
| Micros | Only the Clay-local transport exists: the supplier is a micro answered inside Clay. A nodule supplier for a micro's request is "D57 step 9", not built (ADR 050 §4). |

## 2. Datum kinds and settings (a `#datum` permbind on a kind another cartridge holds)

| | |
|---|---|
| Names | `#datum { nsid, read, write, create, delete }` (`manifest.rs:30`). One provider request per declared access (`provider_requests`, `big_nodule_core/lib.rs`). |
| Kind | NSID in the manifest; **effectively direct**. |
| Chosen by | The provider of a datum NSID is "the enabled cartridge that holds the datum (declares the record under its authority); a copy another cartridge vendors is not an offer" (`providers.rs:11-14`). ADR 028 §5 allows only one enabled holder of an authority prefix. So there is at most one candidate (plus node-native boot schemas). |
| Recorded | Lockfile, as for methods. |
| Grant | Drawers: `grant/drawer/<id>`. "Data of another cartridge": `grant/cartridge/<cartridgeId>` (ADR 049 §4.1), which **names the provider by id**. |
| Substitutable | Only by replacing the holder: disable the original and enable a fork shipping the same NSIDs (ADR 028 §3, ADR 045 §4). ADR 047 §5 says it outright: "The routine depends on its shape, so nothing substitutes it; a grant only gives or withholds the access." Routes do not apply (a route is for operations; `offers` would honour one for a datum NSID, but there is only one claimant to route among). |
| Open | `grant/cartridge/<Y>` is keyed by cartridge id while the permbind it satisfies names an NSID. After a fork replaces Y, the grant to Y does not cover the fork. The code does not enforce `ResolvedGrants.cartridges` yet, so this is untested territory. |

## 3. Lexicons (schemas), including vendored copies

| | |
|---|---|
| Names | The cartridge carries a copy of each foreign Lexicon (`lexicons: NSID -> #datumPin`, ADR 028 §4, ADR 047 §1). A pin is to the cartridge's **own** registry document, never to another cartridge's. |
| Kind | NSID (the NSID is the reference). |
| Admission | A Lexicon file outside the cartridge's authority must be requested (a permbind names it) or an operation it implements; a `ref` into a foreign file likewise (`Admission::lexicon_files`, `resolve_ref`, `big_nodule_core/lib.rs:511,561`). So a type-only reference to another cartridge's schema still needs a permbind. |
| Chosen by | Nobody chooses a provider. The registry compares copies: identical, or both with `revision` and differing revisions (lower is a subset), else conflict; the authority holder's copy is the definition, and a vendored copy that differs from it or has a higher revision is refused; with no holder enabled every copy in a conflicting pair is refused (`lexicon_conflicts`, `big_nodule_core/config.rs:1416`). |
| Recorded | Not recorded; recomputed each reconcile. |
| Substitutable | N/A. `minRevision` selects what is acceptable, not who. |
| Sidecars | Held records only; a vendored copy's sidecar belongs to the holder (`sidecar()`, `lib.rs:547`). |

## 4. Type libraries (`org.example.daybook.defs` and the other node-provided libraries)

| | |
|---|---|
| Names | A `ref` to the library's NSID. |
| Kind | NSID, with the **node** as the only provider. |
| Admission | "A type library the node provides is always resolved, requested or not", and a vendored copy must equal the node's (`provided_library`, `lib.rs:395,516`; `node_type_libraries`, `big_cabinet_core/reference/defs.rs:25`). |
| Chosen by / recorded | Neither; not a cartridge. |
| Substitutable | No. It is the runtime's, like Clay's host ABI (ADR 028 §4: "The runtime's host ABI is not a cartridge dependency"). |

## 5. Collections and collection contracts (Clay)

Claytxt's account (src refs are the daytxt tree):

| | |
|---|---|
| Names | A `collection_requests` entry `Request { contract: Nsid, access }`; under ADR 050 §2 a `nsid#def` of a `token` def in the owner's Lexicon, with the wiring in the owning micro's `collections` map. A consumer binding is a local name; the instance is a host-minted `CollectionId` no manifest names. |
| Kind | NSID. |
| Chosen by | `daytxt_core/bindings.rs:140-180`: each collection request resolves **independently**. The stored `clay/micro-bindings` datum wins per binding; otherwise the default is the sole declared collection implementing the contract (`bindings.rs:123`). |
| Recorded | The `clay/micro-bindings` datum, which records contract and access per binding (`bindings.rs:54-70`). It does not record which cartridge provides the collection beyond the collection it points at. |
| Grant | ADR 049 §3 item 4: a collection another cartridge publishes is reached via a permbind and granted as `grant/cartridge/<Y>`; collections of the same cartridge need no grant. |
| Substitutable | Yes, by editing the binding. Nothing relates two bindings to one provider (see co-provision below). |
| Nodule-side collections | "Collections at cartridge level" are deferred (ADR 050 §9). |

## 6. Presentation components and Clay contracts (micro to micro)

Claytxt's account:

| | |
|---|---|
| Names | `requires: [Request { binding, contract: Nsid, necessity, resolution, minRevision? }]` (`clay_mdk/manifest.rs:379-411`, doc comment "never a concrete supplier"). The supplier declares an endpoint `View { implements: Option<Nsid> }` (`manifest.rs:330-342`; ADR 050 `#viewEndpoint`). The consumer receives an opaque host-issued `ComponentRef` (`clay_host/instance.rs:69`) and mounts it with `Mutation::MountComponent`. Methods work the same way (`Endpoint::Method { nsid }`). |
| Kind | NSID. No micro names another micro. |
| Chosen by | `daytxt_core/requirements.rs:16-80` `resolve`: the supplier is whichever installed micro serves the contract, excluding the consumer. Zero: a required request blocks, an optional one stays unbound. Several: unresolved with an error naming all candidates. |
| Recorded | Not stored; recomputed at every boot. `BindingResolution::AtRuntime` is declared only (`manifest.rs:276`). |
| Grant | ADR 049 §3 item 4 says embedding another cartridge's presentation is a `#method`-shaped permbind accepted at install, and the embedded micro renders with its own handle. |
| Substitutable | Yes in principle (disable one candidate), but there is **no choosing mechanism**: no route applies, because ADR 050 §3 says views resolve "through a Clay-side resolver, never dispatch", though "through the same route table" is also stated. See question Q1. |
| Gap vs ADR | ADR 050 §4 says several providers is an install **error** and "choosing is a route (ADR 045)". The route table is dispatch's; the Clay-local resolver does not consult it (claytxt: "nobody chooses"). |

## 7. Shell and contribution collections (`org.example.clay.shell.roots`, Settings sections, toolbar)

| | |
|---|---|
| Names | A `#contribute`-shaped request on a runtime-defined or other cartridge's collection token; rows via static `contributes` or `contributions` (`manifest.rs:467`, claytxt). |
| Kind | NSID, satisfied by the **runtime** for `org.example.clay.*` (ADR 028 §4: "satisfied by the runtime rather than by a cartridge"). `shell.roots` is a host contract with no Lexicon yet (claytxt). |
| Row targets | A row's `links` hold lazy links to **the contributor's own** endpoints (ADR 050 §3; ADR 031 §7). So the shell reaches the contributor by a stored endpoint-ref `(cartridge, micro, endpoint)`: direct, but chosen by the contributor, and the consumer (shell) depends on no one. |
| Substitutable | The collection owner is the runtime; contributors are open (D58: any micro may write any row). |
| Callback endpoints | `#callbackEndpoint { implements }` are never suppliers and never resolved by NSID; reached only through a link a row carries (`requirements.rs:4`, claytxt). Provider-specific by construction. **Direct.** |

## 8. Shared artifacts (byte-level reuse)

| | |
|---|---|
| Names | ADR 028 §4: "The one cartridge-level reference allowed is byte-level reuse of another cartridge's artifact (a shared component or asset), named by cartridge id plus artifact key and always resolved through the substitution table." |
| Kind | **Direct** (cartridge id + artifact key). |
| State | No manifest field carries it: a component is a key into the cartridge's own `components` map of `#datumPin`s (`Cartridge.components`, `validate` checks `micro.component` is local). "The substitution table" was the pre-ADR-045 `filledBy` mechanism, removed. So this rule has no home in the current manifest. |
| Substitutable | Undefined. |
| Adjacent | `#hosted` (below) is the one proposed reference to another cartridge's component. |

## 9. Local state (ADR 051 §3, §5)

| | |
|---|---|
| Today | `localState` is the cartridge's own, keyed by name, kind `sqlite` only (`manifest.rs:307`). The old daybook manifest let a routine name another plug's local state directly (ADR 051 §5: "Nothing a cartridge provides can be marked private today"). |
| Proposed | Every provided resource (method, view, collection, local state) gains `visibility`, `private` by default. A `public` local state or permbind may be depended on by another cartridge; "Unlike browser OPFS, one cartridge's local state can be depended on by another" (§3). Only host-mediated SQLite is dependable. |
| Names | The ADR does not give the syntax. For permbinds: "A declares a dependency on it (`uses` names `B#permbind`)", which names the providing cartridge **B directly**. |
| Kind | Direct (as worded), draft. |
| Chosen by | The person: "Linking takes one grant, to A", with consent text. The link rides B's grants as they change; revoked at either end; can narrow, never widen; attribution records A-through-B. |
| Recorded | The link grant (cartridge config document); no datum is specified. |
| Substitutable | Not discussed. A link to `B#permbind` is to a specific cartridge's grants, so a fork C of B would need a new link. |
| To confirm (ADR 051) | The `private` default; whether micros' collections are `public` by construction. |
| Out of scope (ADR 051 §6) | Writes into another cartridge's local state. |

## 10. `#hosted` runtimes (ADR 047 amendment B, proposed, not accepted)

| | |
|---|---|
| Names | `…component#hosted { runtime: "<DID>/<name>@<range>#<component>", sources }`: "`runtime` names a component of another cartridge ... (the dependency rules of 028 §4 apply)". |
| Kind | **Direct**: DID, name, semver range and component key. The only manifest field that names another cartridge as a required dependency rather than a hint. |
| Chosen by | The manifest. Open: "whether the runtime range is resolved at install (lockfile, §7) or at publish." |
| Recorded | Open (lockfile or publish-time pin). |
| Substitutable | Not specified. 028 §4 "the dependency rules" are that dependencies are delivery hints and a manifest never pins by digest; a runtime that cannot be substituted contradicts the hint rule, so this needs a ruling. |
| Code | None (`grep hosted` finds only executor hosting). |

## 11. Initializers

| | |
|---|---|
| Rule | An initializer targets an operation the **same** cartridge implements; admission refuses a foreign target (ADR 047 amendment A, decided 2026-11-17; `invocation_target`, `big_nodule_core/lib.rs:367`). |
| Cross-cartridge reach | None through the initializer. A cartridge that must call another during setup does so from its own initializer routine under that routine's `uses` (a method permbind, section 1). Micro initializers call the micro's own `installer` export (ADR 050 §5). |
| Kind | N/A (no dependency). |

## 12. Grants and requests between cartridges

| Reach | Names | Kind | Notes |
|---|---|---|---|
| `grant/method/<nsid>`, `grant/authority/<prefix>` | NSID or prefix | NSID | Applied only to requested methods; a prefix grant never stretches to a dev takeover's additions (ADR 028 §3). |
| `grant/cartridge/<id>` | cartridge id | **Direct** | "may use data from cartridge Y (its collections and datums included)" at `access`. Unenforced. |
| `grant/drawer/<id>` | drawer id | N/A (not a cartridge) | |
| `MissingAuthority { cartridge: Y, request }` (ADR 049 §3) | provider id in an error | Direct, runtime | X's micro calls Y and may offer to grant Y what Y lacks. |
| Confused deputy (ADR 049, 047 §6) | n/a | n/a | A call to another cartridge passes none of the caller's access; the callee acts with its own. |
| Permbind linking (ADR 051 §5) | `B#permbind` | Direct (draft) | See section 9. |

## 13. Other

- **Dev takeover** (ADR 045 §5): `devTakeover/<cartridgeId>` with `by`; direct and dev-only, never a side effect of
  routing or install.
- **Lineage** (ADR 047 amendment C, proposed): a fork records `<id>@<version>` of its base. Provenance, not a dependency.
- **`basedOn`** (ADR 028 §3): same.
- **Interim review micro** (claytxt): holds node capabilities from `INTERIM_GRANTS` (`daytxt_native/interim.rs`) scoped
  to intxt's drawers and calls intxt's nodule methods by NSID. Names intxt's drawers directly; goes away with ADR 052.
- **Kit stylesheet** (claytxt): host-adopted, not a dependency.
- **Host ABI** (`townframe:dayframe` WIT): a runtime version constraint, not a cartridge link (ADR 028 §4).

## Co-provision: can two NSIDs be forced onto the same provider?

**Case.** Cartridge X depends on NSIDs `N1` and `N2` that only make sense when served by the same provider P, for
example two collections whose rows reference each other, a collection and the method that mutates it, a datum kind and
the method that validates it, or a `view` and the collection it renders.

**Today, nothing stops a split, and nothing relates the requests.**

- Nodule side. `provider_requests` yields one request per method permbind and one per declared datum access, and
  `RequestStatus::evaluate` runs on each in isolation against that contract's offers
  (`big_nodule_core/providers.rs:98`, `lib.rs`). Locks are keyed per binding. A route covers one pattern at a time
  (exact NSID or whole-segment prefix, longest match), so an operator can route `N1` to P and `N2` to Q.
  An authority-prefix route moves everything under the prefix together, which co-provisions NSIDs that share an
  authority and nothing else.
- Datum kinds are not splittable by configuration, because one holder per authority prefix means one candidate.
  Co-provision of datum NSIDs under one authority is guaranteed by that rule; of datum NSIDs under two authorities,
  not.
- Clay side (claytxt). Collection requests resolve independently (`bindings.rs:140-180`); the `clay/micro-bindings`
  datum can bind two collections to two different providers. Requirements resolve independently per contract
  (`requirements.rs:16-80`).
- Mixed kinds: a method (dispatch route or lock) and a collection (Clay binding) have no shared mechanism at all.

**Cases where it matters** (to the extent I found them; I did not audit every cartridge in the repo):

1. A micro's collection contract plus the micro's own methods that edit it (ADR 050 §2: the collection is defined by
   the owning micro; the owner's method endpoints mutate it). Same cartridge, so co-provision holds if both are
   claimed by that cartridge, but a route can move the method alone.
2. Two collections defined by one micro and read together (a panel and its detail rows).
3. A datum kind (held by H) plus a method that interprets it (served by S): the two lockfile entries can name two
   cartridges.
4. A view (`implements`) plus the collection or method it displays.
5. The public-permbind link of ADR 051 §5: A inherits B's drawers; it must pair with B's methods.
6. `#hosted`: a runtime and the sources it interprets (already a single direct reference, so co-provision is built
   in).

**Options, without choosing:**

| Option | Mechanism | Costs and open points |
|---|---|---|
| A. A group constraint in the request | Permbinds gain a shared tag (for example `group: "notes"`); resolution must pick one provider that offers every NSID of the group, or none. | New manifest vocabulary; the lock records one provider per group; `Ambiguous` and routes need group semantics (a route for one member breaks the group?). Fits the NSID rule and keeps forks drop-in. |
| B. Depend on a bundle NSID | The provider declares a bundle (an NSID whose Lexicon lists the member NSIDs, or a "profile" that implies them); consumers request the bundle, and one provider offers it whole. | A new Lexicon kind; a bundle is itself a contract, so a fork must implement all or none. Matches OSGi `Import-Package` against "one package, many exports". Needs a rule for revisions of members. |
| C. A direct cartridge dependency | The dependent names provider P (a `dependencies` entry given meaning, or a `#hosted`-style reference). | Reverses ADR 028 §4 for that case; forks stop being drop-in (a fork needs a new id, rename and rebind); simplest to audit. |
| D. Hold co-provision as the operator's responsibility | Document that routes can split; a route at the authority prefix keeps NSIDs together; add a check that warns when a route splits NSIDs one cartridge both requests and the owner provides together. | No manifest change; weakest guarantee. |
| E. Same-authority rule | Require co-provided NSIDs to share one authority prefix, so a prefix route or the one-holder rule moves them together. | Constrains NSID naming; does not cover cross-authority sets or Clay bindings. |
| F. Make a second binding derive from the first | Clay: a request names "the provider of binding B1" instead of a separate selection; one choice, N bindings. | Clay-only; a nodule-side equivalent would be option A. |

Not examined: how each option interacts with the lockfile `ProviderChanged` consent (a group change is one consent or
several), or with at-runtime pickers.

## Questions

**For claytxt**

- Q1. ADR 050 §3 says a view resolves "through the same route table as a method but through a Clay-side resolver,
  never dispatch"; your account of `resolve` says nobody chooses. Does the resolver consult dispatch's route table
  (ADR 045) at all today, or is "several providers" always an error until a route mechanism is wired in?
- Q2. Is `clay/micro-bindings` node-local or does it sync with the cartridge config document? It decides whether a
  collection choice follows the person to a sister node (as grants do) or is per node (as routes and locks are).
- Q3. For the web-components idea (open, not built): the operator floated "give me the element for presentation Y of
  micro X" with host-generated tag names. That names a provider. You recommend requests by contract NSID with the
  host tag generated per binding, not per provider. Listed here as an open design question only.
- Q4. Callback endpoints are direct by construction. When the contributor's cartridge is replaced by a fork, do stored
  rows (lazy links by `(cartridge, micro, endpoint)`) go stale, or are they retracted and rewritten by the fork's own
  install-scope rows?
- Q5. A collection owned by a micro whose consumers (other cartridges) write rows: is the collection's provider
  identity (the owning micro) ever stored beside the consumer's binding, or only the opaque `CollectionId`?

**For the operator (not decisions made here)**

- Q6. `grant/cartridge/<Y>` is the one stored, synced, provider-named grant. After a fork replaces Y, should the
  grant follow the NSIDs it was given for (like method grants), or stay on Y's id?
- Q7. `#hosted` is the first manifest field that names another cartridge as a required dependency. Is that an
  accepted exception to ADR 028 §4, or should the runtime be a requested NSID too?
- Q8. ADR 028 §4's "byte-level reuse of another cartridge's artifact" has no field in the current manifest and its
  "substitution table" is gone. Keep, drop or rewrite the rule?
- Q9. Co-provision: whether any of the options above is wanted, and for which of the cases.
