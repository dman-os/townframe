# Micro manifest vocabulary audit

Status: Reference (an audit of the micro manifest as built on feat/desktop-base)
Owner: nodules lane
Updated: 2026-11-18
Scope: Glossary, schemas, kind-like fields and naming problems of the micro manifest, and the lazy-link chain end to end.

Sources, all read via git refs, nothing built. D = `origin/feat/desktop-base`, S = `stack/node-59-adr-d56`.
- `M:n` = `src/clay_mdk/manifest.rs` on D, line n. `T` = `src/clay_mdk/manifest/tests.rs` on D (`canonical_manifest()` at line 17) and its snapshot `manifest_canonical_wire_shape.snap`.
- ADR30 = `docs/adrs/030-micro-lifecycle-and-manifest-contract.md`, ADR31 = `docs/adrs/031-manifest-declared-collections-and-lazy-capabilities.md` (both on D, status Proposed).
- WIT = `src/clay_guest_session/wit/{capabilities,collections,presentation}.wit` on D.
- CS = `docs/scratch/cartridge-surfaces.md` on S (the current proposal, including the "Manager review" and "Operator review, second pass" sections). LA = `.agents/skills/lexicon-authoring/SKILL.md` on S.

## 0. The model in plain words (no ADR knowledge needed)

A **micro** is a small UI program (a WebAssembly component) that the app runs only when something needs it. Think "a panel", "a toolbar button's behavior", "a settings page". There is at most one running copy of each micro.

Because a micro is usually not running, other parts of the app need to learn what it offers without running it. So a micro ships a **manifest**: a static JSON description of what it could do. Everything in the manifest is a possibility. Nothing in it is live state (ADR30 §11, §15).

Four things a manifest can say:
1. what the micro **offers** when running (publications);
2. what it **needs** from others (requires, configuration requests, collection requests);
3. what **shared tables** (collections) it defines for other micros to fill in (collection contracts);
4. what **rows** it pre-plants in someone else's table so they show up before it ever runs (contributions), with a **lazy link** from the row back to one of its own offerings (attachments).

The toolbar example: the shell defines a table "toolbar items". Weather's manifest plants a row "Weather button" in it. The shell draws the button from the row's static data. Weather is not running. Clicking the button follows the lazy link, which starts Weather, and Weather's offering runs. Section 5 walks that chain.

## 1. Glossary

Key: **M** = manifest field, **R** = runtime concept, **M+R** = both. Examples use Weather (a cartridge with a panel micro) and the shell toolbar.

| Noun | Plain definition | Example | Where | Kind |
|---|---|---|---|---|
| **Micro** | A singleton UI component. Its id names its package, its configuration and its at-most-one live copy. | Weather's panel. | ADR30 §1 | R (id: `MicroId`, M:44) |
| **Micro id** | `<DID>/<plug name>`; the installer supplies the DID. | `did:example:clay-local/dayframe.counter` | M:44, ADR30 §1 | R (manifest holds only the name, in the Sidecar) |
| **Manifest** | Static JSON of what a micro could do; no live state. | `MicroManifest` | M:434, ADR30 §11 | M |
| **Sidecar** | Stand-in on-disk form: manifest members plus `id`, `package_version`, `authority`. Goes away when the cartridge envelope carries identity. | `dayframe.counter.manifest.json` | M:452, CS §6 | M (stand-in) |
| **Authority** | The reverse-DNS NSID prefix a package may declare things under; anything else must be requested. | `org.example.weather` | M:452, ADR30 §11 | M (envelope) |
| **NSID** | A dotted reverse-DNS name (atproto syntax), validated on parse. Today it names contracts, schemas, facet tags. | `org.example.shell.toolbar` | `clay_collection::Nsid`, M:12 | M |
| **Incarnation** | One running copy of a micro, created by one successful `initialize`, discarded on close. | Weather running between a toolbar click and idle shutdown. | ADR30 §1 | R |
| **Entrypoints** | The WIT interfaces the host drives on the component: `initialize`, optional `presentation`, reserved `installer`. | `townframe:dayframe/micro-lifecycle@0.4.0` | M:194 | M |
| **Initialize** | The one start hook. Returns the micro's publications by name. Outward effects stay invisible until it succeeds. | Weather returns `view` and `current`. | ADR30 §5, ADR31 §8 | R |
| **Publication** | A named behavior the micro serves once started, of kind `call` or `presentation`. The only thing a row can point at. | Weather's `view` (presentation), `current` (call). | M:326, ADR31 §5 | M+R |
| **Call** | A request/response operation. Its contract names the request and response schemas. | A toolbar button's action: empty request, empty response. | ADR31 §5, M:326 | M+R |
| **Presentation** | A mountable UI component a consumer embeds inside its own surface. Has no contract NSID; one presentation interface covers all. | Weather's `view` panel. | ADR31 §5, M:326 | M+R |
| **Mount** | The consumer places a presentation inside its UI via `mount-component`. Synchronous, after an asynchronous redeem. | Shell mounts Weather's panel in a slot. | WIT presentation.wit (`mount`, `mount-component`), ADR31 §6 | R |
| **Requirement** (`requires`) | A request for another micro's publication (call or mount). Grants "may call/mount", nothing else. | Weather requires `shelf`. | M:434, ADR30 §12 | M |
| **Request** | The one shared request shape: `{binding, contract, access?, necessity, resolution, minRevision?}`. | see section 2 | M:347, ADR30 §12 | M |
| **Binding** | The micro-local name code uses for a request; configuration decides what it points to. | `"tally-book"` | M:347 | M+R |
| **Contract** | An NSID naming the *shape* of what is wanted (a collection shape, a call's request/response). Never a concrete supplier. | `org.example.test.counter.tallies` | M:347, ADR30 §12 | M |
| **Necessity** | Whether an unresolved binding blocks the micro (`required`, default) or not (`optional`). | | M:262 | M |
| **Resolution** | Who picks the binding: `configured` (default), or `atRuntime` (user, powerbox-style; declared only, no host issues them yet). | | M:280, ADR31 §11 | M |
| **minRevision** | Least Lexicon revision of the contract needed. | `2` | M:347 | M |
| **Configuration request** | Asks for a config document by datum NSID with a per-facet ACL; configuration picks the actual document. | | M:382, ADR30 §13 | M |
| **Facet (access)** | One datum type + optional key + read/write/create/delete rights, as a facet ACL entry. | tag `org.example.test.counter.config`, key `count`, read only | M:369 | M |
| **Collection** | A host-managed keyed table of rows. Read through leased, paged projections; "collection" is also what a micro's code sees through a binding. | The shell's "toolbar items" table. | ADR31 §1-2 | R |
| **Collection contract** | A manifest declaration of a collection's shape: schemas for selector, key, value, optional write-context, and what it offers. | The shell declares the toolbar-items contract. | M:392, ADR30 §13 | M |
| **Selector** | Schema for narrowing which rows a reader acquires; host-boundary scope. | "only slot=toolbar" | M:392, ADR31 §3 | M (schema) + R |
| **Key / Value** | Schemas of a row's key and body (CBOR). | key `counter`; value is the button description. | M:392 | M (schema) + R |
| **Write context** | Optional schema for extra data a write carries. | | M:392, WIT `upsert.write-context` | M (schema) + R |
| **Offered capability** | What a collection contract offers consumers: `read`, `register`, plus `causal-write`, `domain-commands` in code. | Toolbar offers `read`, `register`. | M:311 | M |
| **Collection request** | A request (`access` required) to read or write a collection by contract; configuration binds it to a concrete collection. | Weather asks `write` on toolbar items. | M:434, ADR31 §11 | M |
| **Access** | `read` or `write`; write implies read. | | M:270 | M |
| **Collection instance (id)** | The concrete table a binding resolves to. Today its id equals its contract's id; never named by a manifest. | | ADR31 §11, CS §2 | R |
| **Grant (binding grant)** | Stored configuration `{collection, contract, access}` for a binding. Narrower access still holds; widening request voids it. | | ADR31 §11 | R |
| **Contribution** | A row the package pre-declares into a collection. Written at install, visible while the micro is dormant. | Weather's "Weather button" row. | M:419, ADR30 §14, ADR31 §3,7 | M+R |
| **Registration scope** | The owner of a set of candidate rows. Retraction follows the scope, not the writer handle. | | ADR31 §4 | R |
| **Retention classes** | Scope lifetimes: `IncarnationScope` (retracted on close), `InstallationScope` (survives restarts; retracted on uninstall), `PermanentScope` (needs explicit grant). Code names `Retention::{Installation, Permanent}`; incarnation scopes carry none. | Manifest rows live in an installation scope. | ADR31 §4, `clay_collection/registration.rs:57` | R |
| **Writer / registration-writer** | A revocable handle to put/delete rows in one scope. Dropping it does not choose retention. | | ADR31 §4, WIT collection-registration | R |
| **Attachment** | A slot in a row (by index) holding a link to behavior. In row CBOR it is a tagged index (tag 43000); in manifest JSON the payload writes `{"$attachment": n}`. | Row's slot 0 = "run Weather's `view`". | M:406, `clay_mdk/attachment.rs:21` | M+R |
| **Lazy endpoint** | The stored data form: `(micro id, publication name, contract)`. Names no revision; resolved at activation. | `(Weather, "view", ...)` | ADR31 §7 | R (stored in rows) |
| **Capability-ref** | The live handle a reader gets, minted per recipient from an endpoint. Either *bound* to a live publication or *lazy*. | | ADR31 §6, WIT capability-bank | R |
| **Ref status** | `live, dormant, disabled, faulted, unavailable, revoked`, readable without activating. | Dormant = enabled, not running. | WIT capabilities.wit, ADR31 §6 | R |
| **Redeem** | Turn a lazy ref into a live strong binding, activating the provider if needed. Async. `redeem-presentation`, `call`, `retain-call`. | | ADR31 §6 | R |
| **Eager / root** | A micro started at boot and held live regardless of use (live config flag, not manifest). Shell roots are eager. | The shell micro. | `supervisor.rs:106`, ADR30 §2 | R (CS keeps manifest `activation` as a hint) |
| **Demand** | Resource use that starts or joins activation: explicit root, redeem, invoke, mount, retain. Reading a row never demands. | | ADR30 §4 | R |
| **Strong/weak edges** | Lifecycle classes: Lazy, Weak, Strong, OwnedEffect, NoMicroRetention. Strong holders keep the provider alive. | An in-flight call is strong until it completes. | ADR30 §2, ADR31 §9 | R |
| **Shell roots** | Runtime collection `org.example.clay.shell.roots` where shell micros register a `RootRow{title, content}`; the composition mounts each. | | M:27, `clay_mdk/decl.rs:71` | M (named in a request) + R |
| **Persisted collections** | Collections with *durable* backing (reload rows after restart), vs *volatile* (host memory) or *materialized* (projected from node state). Backing is in ADR31 §2 but absent from the Rust `CollectionContract`. | | ADR31 §2 | R (ADR only; not in struct) |
| **Micro state** | `Disabled, Idle, Blocked, Unconfigured, Initializing, Active, Closing, Faulted`. | | `supervisor.rs:165` | R |
| **Binding datum** | Per-micro config mapping binding to grant or `null`, tag `clay.micro.bindings`. | | ADR31 §11 | R |
| **Contract tags (host tags)** | Non-Lexicon identifiers like `clay.micro.bindings`. | | CS §2 | R |

## 2. Every manifest member: Rust type and JSON shape

All members below are from D (`manifest.rs`), unless noted. Wire is serde JSON; see the snapshot for the exact canonical output.

### 2.1 Top level

```rust
// M:434
pub struct MicroManifest {
    pub entrypoints: Entrypoints,
    pub publications: Vec<PublicationDeclaration>,
    pub requires: Vec<Request>,
    pub configuration_requests: Vec<ConfigurationRequest>,
    pub collection_contracts: Vec<CollectionContract>,
    pub collection_requests: Vec<Request>,
    pub contributions: Vec<ContributionDeclaration>,
    #[serde(flatten)] pub extra: BTreeMap<String, serde_json::Value>,
}
```

Field names are snake_case on the wire, except `Request`, which is camelCase (`minRevision`).
ADR30 §11 still lists `provides` (as `ProvidedContract`), `value`, `endpoints`, `offered_capabilities`, `backing`. The code and snapshot use `publications`, `payload`, `attachments`, `offered` and no `backing`. The ADR text lags the code in that regard.

The manifest has no version and no identity; its format identity is the NSID `org.example.clay.micro.manifest` (M:21).

### 2.2 Entrypoints (M:194)

```rust
pub struct InterfaceRef(String);   // M:179, transparent
pub struct Entrypoints {
    pub initialize: InterfaceRef,
    pub presentation: Option<InterfaceRef>,
    pub installer: Option<InterfaceRef>,      // reserved, never executed (ADR30 §10)
    #[serde(flatten)] pub extra: BTreeMap<String, serde_json::Value>,
}
```
```json
{"initialize":"townframe:dayframe/micro-lifecycle@0.4.0","presentation":"townframe:dayframe/presentation-events@0.4.0","installer":null}
```
Note: `None` serializes as `null` (no `skip_serializing_if`), as the snapshot shows.

### 2.3 PublicationDeclaration (M:326)

```rust
pub struct PublicationDeclaration { pub name: String, pub kind: PublicationKind, pub contract: Option<Nsid> }
```
```json
{"name":"view","kind":"presentation","contract":null}
{"name":"increment","kind":"call","contract":"org.example.test.counter.increment"}
```
`validate` (M:537): names unique; `call` must have a contract; `presentation` must not.

### 2.4 Request, used by `requires` and `collection_requests` (M:347)

```rust
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub binding: String,
    pub contract: Nsid,
    pub access: Option<Access>,            // skipped when None
    pub necessity: Necessity,              // skipped at default `required`
    pub resolution: BindingResolution,     // skipped at default `configured`
    pub min_revision: Option<u32>,         // skipped when None
}
```
```json
// requires
{"binding":"shelf","contract":"org.example.test.shelf","necessity":"optional","minRevision":2}
// collection_requests
{"binding":"tally-book","contract":"org.example.test.counter.tallies","access":"write"}
```
Rule (M:537ff): a requirement has no `access`; a collection request must.

### 2.5 ConfigurationRequest and FacetAccess (M:369, M:382)

```rust
pub struct FacetAccess { pub tag: Nsid, pub key_id: Option<String>, pub read: bool, pub write: bool, pub create: bool, pub delete: bool }
pub struct ConfigurationRequest { pub binding: String, pub facets: Vec<FacetAccess> }
```
```json
{"binding":"counter","facets":[{"tag":"org.example.test.counter.config","key_id":"count","read":true,"write":false,"create":false,"delete":false}]}
```

### 2.6 CollectionContract (M:392)

```rust
pub struct CollectionContract {
    pub id: Nsid, pub selector_schema: Nsid, pub key_schema: Nsid, pub value_schema: Nsid,
    pub write_context_schema: Option<Nsid>, pub offered: Vec<CollectionCapability>,
}
```
```json
{"id":"org.example.test.counter.tallies","selector_schema":"org.example.test.counter.tallySelector","key_schema":"org.example.test.counter.tallyKey","value_schema":"org.example.test.counter.tally","write_context_schema":null,"offered":["read","register"]}
```
Authority rule (Sidecar::validate, M:493): `id` must be under the package authority; the four schemas must be under authority or requested. Contract ids and schemas are therefore ordinary NSIDs that currently are **not** Lexicons (CS §2 audit proposes making them Lexicon defs).

### 2.7 ContributionDeclaration and AttachmentDeclaration (M:406, M:419)

```rust
pub struct AttachmentDeclaration { pub index: u32, pub publication: String }
pub struct ContributionDeclaration {
    pub id: String, pub binding: String, pub key: String,
    pub payload: serde_json::Value, pub attachments: Vec<AttachmentDeclaration>,
}
```
```json
{"id":"toolbar-button","binding":"tally-book","key":"counter",
 "payload":{"badges":[1,null,"beta"],"enabled":true,"label":{"en":"Counter"},"order":2,"slot":"toolbar"},
 "attachments":[{"index":0,"publication":"view"}]}
```
Validation (M:537ff): `binding` must name a collection request with `write` access; every `attachments[].publication` must be one of the manifest's own publications. Observation (unconfirmed whether a gap): the canonical payload contains no `{"$attachment": 0}` marker, so nothing in the shown payload says which payload member is slot 0. The marker convention is in `clay_mdk/attachment.rs:21`.

### 2.8 Sidecar (M:452)

```rust
pub struct Sidecar { pub id: String, pub package_version: u32, pub authority: Option<String>, #[serde(flatten)] pub manifest: MicroManifest }
```
Flat JSON: manifest members plus `id: "dayframe.counter"`, `package_version: 1`, `authority`.

### 2.9 Collection runtime types (not manifest)
`RootRow{title, content: Attachment}` (`clay_mdk/decl.rs:71`) is what shell micros write into `org.example.clay.shell.roots`. The WIT `row-attachment` is `reference(capability-ref) | own-publication(string)` (collections.wit, `collection-registration`).

## 3. Kind-like fields: closed or open, and the proposed Lexicon form

Every vocabulary in the manifest is already open in Rust: `open_vocabulary!` (M:220) produces known variants plus `Other(String)` that round-trips verbatim, serialized `from=String into=String`. So the Rust shape is already the LA "open string vocabulary" (`#[datum(open_string)]` with `Unknown(String)`). What differs is that the manifest is plain serde JSON and not a Cabinet datum, so none of it is published as a Lexicon with `knownValues`. No manifest vocabulary is closed in Rust. The closed enums (`MicroState`, `RefStatus`, the WIT enums) are host-computed runtime states.

LA conventions: open unions are `$type`-discriminated with an `Unknown` carrier; string vocabularies are `type: string` plus `knownValues` and carry unknown values; a member with nothing to say carries `empty: true`.

| Field | Today | Proposal | Form |
|---|---|---|---|
| `PublicationKind` (`call`, `presentation`; M:256) | open string vocabulary | **Open union of objects, `$type`-discriminated.** Members carry different data: `call` carries `implements` (an NSID) and `uses`; `presentation` carries nothing, so it takes `empty: true`. CS §3 already has `CapabilityKind::Call{implements} | Presentation`, which is this union. Keep a closed Rust mirror for operations (LA "service-side closed mirrors"), `known()` returning `None` for carried-but-unrun kinds. | union |
| `Necessity` (`required`,`optional`; M:264) | open string | `type: string`, `knownValues: ["required","optional"]`, default `required`, unknown carried. CS already makes this per-access in permbinds (`"read":"required"`). | string vocab |
| `Access` (`read`,`write`; M:274) | open string | `knownValues: ["read","write"]`, unknown carried. Note: `read` is a subset of `write`; a future member like `append` would be a union only if it carries data. | string vocab |
| `BindingResolution` (`configured`,`atRuntime`; M:282) | open string | **Remove** per CS §6 (runtime grant requests become a host service). If kept: `knownValues: ["configured","atRuntime"]`. | string vocab (or drop) |
| `CollectionCapability` / `offered` (M:311) | open string, 4 members in code, ADR lists 3 | `knownValues: ["read","register","causal-write","domain-commands"]`. Hyphenated spellings are Rust-wire; Lexicon prefers lowerCamel, so decide spelling before publishing. CS keeps `"offered": ["read","register"]`. | string vocab |
| `Entrypoints` | object with `extra` flatten | object, open by default (LA): keep `extra`. Roles (`initialize`, `presentation`, `installer`) are fields, not a vocabulary, so no union. | open object |
| Surface sections (CS second pass) | n/a | `$type`-discriminated list `[{"$type":"…#nodule"},{"$type":"…#micro"}]`, unknown carried unread. | open union |
| `activation` (CS: `Lazy \| Eager`, "open") | not in D | `knownValues: ["lazy","eager"]`. | string vocab |
| `Backing` (Volatile/Durable/Materialized, ADR31 §2) | not in code struct | If it enters the manifest: `knownValues`, since members carry no data. | string vocab |
| `RefStatus`, `MicroState` | closed runtime enums | stay closed; they are host-computed and never read from a package. | n/a |

Two non-kind decisions: object members that name a Lexicon should use `nsid#def` (CS `LexRef`), and a missing `contract` for `presentation` is expressed by `empty: true` on the union member, not by `Option` plus a validate-time rule.

## 4. Naming problems

| Current | Problem | Proposed | Precedent / status |
|---|---|---|---|
| **capability** (`capability-ref`, `published-capability`, `offered`, `CollectionCapability`) | "Capability" already means an authority handle (`CapId`, ADR 037) in the node. A micro's "capabilities" read as its permissions. `capability-ref` is also not always authority (it can be a lazy pointer). | In the manifest use `endpoints` (CS Manager review) or `offers`. Rename `capability-ref` to `endpoint-ref` or `lazy-ref`; rename `CollectionCapability` to `CollectionVerb`/`offered operations`. | Fuchsia `offer`/`expose`; ADR31 §7 "lazy endpoint". CS flipped `capabilities` to `endpoints` after review. Unconfirmed: which the operator chose. |
| **publication** | Sounds like publish/subscribe or an events feed; the kind list is "call, presentation". | `endpoints` (CS recommendation after review). Alternatives: `offers`, `services` (collides with node services). | CS §2 "Noun for publication". Status: proposal, not built. |
| **contract** (collection contract, call contract, requirement contract) | Three different things share the word: a collection's shape bundle, a call's request/response pair, and a "requirement contract". Also confusable with "Lexicon" which also carries "contract" talk. | **Collection**: "collection definition" or "table type", as a `#def` in a Lexicon. **Call**: the method Lexicon (`implements`). Reserve "contract" for none, or only for the umbrella. | CS §2 audit: "NSIDs name Lexicons only"; atproto `ref` `nsid#def`. |
| **surface** | Two meanings: (a) a *cartridge surface* (nodule/micro/tiles section) and (b) the Clay UI `surface` (WIT `presentation.surface` resource, `mount-point`). Reader cannot tell "the micro surface" from "a UI surface". | Cartridge sections: `entry kinds` / `runtimes` ("nodule runtime", "micro runtime") or keep "surface" only for cartridge-level and rename WIT `surface` to `canvas`/`frame`. Precedent: atproto "record/procedure" types; Android "component types". | CS title and WIT `resource surface` (presentation.wit:100). Both live now. |
| **attachment** | A slot index in a row (`Attachment`), a manifest `AttachmentDeclaration`, and WIT `row-attachment`. All three differ in shape. | Keep `attachment` for the in-row slot; call the manifest form `link`/`slot`; or follow CS which keeps `attachments` but references `capability` by name. | CS §3 `Attachment{index,capability}`. |
| **lazy** | Describes a status of a reference, not an object ("lazy endpoint", "lazy component", "lazy action") and ADR31 §5 says "there is no third lazy kind". | Say "unstarted link" in docs; keep `LazyEndpoint` as the stored data form. | ADR31 §5-7. |
| **contribution** | A manifest term ("contributions") and a runtime term, also "contribution" is used for plug contributions (`MicroContribution` in the envelope, ADR 028). Collision with the envelope's meaning. | `rows`/`plants` for manifest-declared rows; keep "contribution" only for the envelope. | CS §3 keeps `contributions`. Unconfirmed: the envelope meaning is from M:12 doc comment and CS §6 (`MicroContribution{schema, description}`). |
| **binding** | Used for request local names, for grants, for collection binding in WIT (`collection-binding{name, reader}`), and for `retain-call` "call-binding". | `alias`/`slot`/`handle` for the local name; keep "binding" for the resolved connection. | none strong; unconfirmed it matters. |
| **registration** (`collection-registration`, `RegistrationScope`, `registration`) | Used for writing rows at runtime *and* for installation-time scopes *and* for `registration collection` (the causal map). | "contributing rows"/"row writer". | ADR31 §4. |
| **requires / necessity / resolution** | `requires` entries need not be required; `necessity` default `required`. Overlaps. | Collapse to per-use necessity as CS permbinds do; drop `resolution`. | CS §2-3, §6. |
| **facet** | Facet ACL means a config datum access entry; "facet" in other parts means something else. | `settings access` / `datum access`. | CS §3 turns into `#datum` permbind. |
| **eager / root** | "Eager" micro and "shell root" (a `RootRow`) are different: one is a start flag, the other a mount target. | Keep eager; rename `SHELL_ROOTS` rows to `shell frames`. | `supervisor.rs:106`, `decl.rs:71`. |

## 5. The lazy-link chain, end to end (the toolbar example)

Setup. The shell cartridge owns a toolbar and declares a collection contract "toolbar items": key = item id, value = `{label, slot, order, icon, action}`, offered = `read`, `register`. Weather's manifest has:

```json
{
  "publications": [
    {"name":"open-weather","kind":"call","contract":"org.example.shell.toolbar.action"},
    {"name":"view","kind":"presentation","contract":null}
  ],
  "collection_requests": [{"binding":"toolbar","contract":"org.example.shell.toolbar","access":"write"}],
  "contributions": [{
    "id":"toolbar-button","binding":"toolbar","key":"weather",
    "payload":{"label":{"en":"Weather"},"slot":"toolbar","order":2,"action":{"$attachment":0}},
    "attachments":[{"index":0,"publication":"open-weather"}]
  }]
}
```
(Fictional Weather values; shapes are from the canonical test. Whether `action` uses the `$attachment` marker in a real toolbar contract is unconfirmed.)

1. **Contribution (manifest).** Weather declares a row under a `write` collection request. Contributions target a *binding*, never a concrete collection (ADR30 §14, M:537ff).
2. **Install: row in the target's collection.** Installation resolves `toolbar` through Clay's binding configuration (ADR31 §11), validates schemas and authority, and writes the row into Weather's **installation scope** of the toolbar-items collection. No Weather code runs. The row stays across restarts and disappears when Weather is uninstalled (ADR31 §4, §7; ADR30 §10). If the binding is unconfigured the row is skipped.
3. **Attachment.** The row carries its slot 0 as a stored **lazy endpoint** `(Weather, "open-weather", toolbar.action contract)` (ADR31 §7). It names neither a package revision nor an incarnation.
4. **Read.** The toolbar reads the collection through a leased projection. For each endpoint the host mints a *recipient-local* lazy `capability-ref` and places it in the row's attachment table (ADR31 §7). The toolbar draws "Weather" from `payload` with Weather still stopped. `status()` reports `dormant` and never activates anything (ADR31 §6).
5. **Redeem (demand).** The user presses the button. The toolbar calls `capability-ref.call` (or for a custom widget, `redeem-presentation`). That is the demand; reading alone never was (ADR30 §4, ADR31 §6).
6. **Incarnation.** The supervisor starts or joins one activation of Weather (concurrent presses join one), runs `initialize`, which returns `open-weather` and `view` by name; the host admits them against the manifest (kind and contract must match) and exposes them atomically. If `open-weather` is not returned, the press fails `unavailable` while the incarnation stays up (ADR31 §8).
7. **Call/mount.** For a call, the request runs as one async `call` export task; the call is a strong edge only until it completes. For a presentation the toolbar gets a `presentation-component` (strong while held), then **mounts** it synchronously via `mount-component` (ADR31 §6, WIT).
8. **Close.** When the last strong edge drops (call completes, component unmounted) and no eager root holds Weather, the supervisor closes it: reject new publication, run `instance-root.close()` once, revoke publications, retract incarnation-scoped rows, drop owned resources (ADR30 §6, ADR31 §9). The installation-scope row survives, the toolbar still shows the button, and the next press starts a fresh incarnation. A mounted presentation reports `revoked` and its slot redeems again.

What is manifest vs runtime: steps 1 and 3's declaration are manifest data; 2, 4-8 are runtime. Dynamic badge state (e.g. unread count) must not live in this static row: ADR31 §13 puts it in a separate volatile collection written by the live micro.

## 6. Open questions flagged

- ADR30 §11/§13 and ADR31 text trails code (`provides`, `value`, `endpoints`, `backing`, `offered_capabilities` vs `publications`, `payload`, `attachments`, `offered`). Reconcile before anyone relies on the ADRs as a schema reference.
- `offered` has 4 members in code (incl. `causal-write`) and 3 in ADR30 §13 (`Read|Register|DomainCommands`).
- `Backing` (volatile/durable/materialized) is described in ADR31 §2 but absent from `CollectionContract`. Where it is declared is unconfirmed.
- CS dropped `collection_contracts` from the manifest in favor of a `token` def plus a `micro.collections` wiring map (Manager review item 1). This changes section 2.6 entirely if adopted; status of adoption unconfirmed.
- `resolution: atRuntime` has no issuer and is slated for removal (CS §6).
- The canonical payload does not show the `$attachment` marker (section 2.7).
- Several micros per cartridge are proposed (CS second pass): the micro id then needs a name segment beyond the cartridge id. Unconfirmed how.
