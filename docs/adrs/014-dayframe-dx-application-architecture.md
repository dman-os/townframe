# ADR 014: dayframe_dx Application Architecture

**Status:** Accepted.
**Supersedes:** ADR 010, `dayframe_dx Architecture — Pieces, Faces, and the Composition Root`.

## Context

`dayframe_dx` is a native-first Dioxus application, not a web request/response application and not a React clone. Dioxus supplies component rendering, signals, tasks, and platform integrations; it does not supply the application architecture that a framework such as Next.js places around React.

The application still needs durable answers for routing, feature composition, data lifecycles, optimistic writes, asynchronous node work, release gating, and platform composition. A local operation may cross an actor, daemon, storage boundary, or sync boundary and therefore must not be assumed to complete by the next render frame.

The multiprocessing analysis in `docs/DEVDOC/log.md` and the previous architecture ADR establish two constraints:

- The application must remain usable whether node services are hosted in-process or behind a daemon.
- GUI-driving events and durable derived-state work have different delivery, replay, and deduplication requirements.

The existing Kotlin application also demonstrates the useful shape of feature view models: a feature owns a coherent set of observable data, commands, and lifecycle work rather than scattering repository calls across individual UI functions. The new application should retain that feature ownership while providing common rules for local data, optimistic state, accepted mutations, and source-owned projection updates.

## Decision

### 1. Use a shared application crate

Add a `dayframe_app` crate under `src/dayframe_dx/app`.

The crate owns the Dayframe application runtime:

- route definitions and route transitions;
- the persistent application shell;
- feature modules;
- the local reactive data runtime;
- feature gates and release-channel evaluation;
- application-level error, task, and lifecycle policy.

This is an application composition crate, not a generic client abstraction and not a replacement for the node service contracts. It exists so desktop, mobile, and web do not independently reimplement the application, routes, features, or data lifecycle.

The existing crates have these responsibilities:

```text
src/dayframe_dx/ui
  Design-system tokens, generic presentational components, and renderer-safe styles.
  MUST NOT depend on Daybook data, application routes, or feature flags.

src/dayframe_dx/app
  Shared Dayframe application runtime, routes, features, data runtime, and app state.

src/dayframe_dx/desktop
src/dayframe_dx/mobile
src/dayframe_dx/web
  Thin platform composition roots. They select platform services, construct the
  application backend, and launch dayframe_app. They do not duplicate features.

src/dayframe_dx/showcase
  Curated executable evidence for the design language. It is not application code.
```

A platform composition root decides whether the application backend uses in-process node services or attaches to a daemon. The rest of `dayframe_app` receives the same typed backend contract in either case.

Node services are the independently addressable capabilities behind that contract: query/read, storage, sync, blobs, progress, configuration, workflow, and future services. This ADR uses “node service” rather than “piece.” The application-facing contract is called the **application backend**, rather than a face.

### 2. Keep presentation and data boundaries explicit

`dayframe_app` has four logical layers:

```text
feature presentation
  -> live queries and feature commands
      -> local data runtime
          -> application backend
              -> in-process or daemon-hosted node services
```

Presentation code:

- renders current state;
- owns ephemeral interaction state such as selected tabs, open panes, and draft focus;
- invokes named feature commands;
- MUST NOT open SQLite, call node services directly, or create ad-hoc shared async caches.

The local data runtime:

- owns normalized collections and derived query results;
- subscribes to backend events;
- starts and deduplicates refreshes and preloads;
- applies optimistic transactions;
- commits accepted optimistic mutations into the local authoritative base;
- exposes reactive snapshots to features;
- is independent of Dioxus and must be testable as ordinary Rust.

A collection source owns the meaning and ordering of its source cursor. The application runtime may
carry an opaque serializable cursor through backend snapshots and events, but MUST NOT compare,
increment, or interpret it. A source MUST provide a gap-free snapshot-to-live handoff, such as a
subscribe-from-cursor operation or an atomic snapshot-plus-subscription operation. This prevents a
refresh from racing the live stream without asking the application runtime to infer ordering from
Automerge heads or another source-specific token. A collection may be a projection over multiple
services or repositories; it is not required to map one-to-one to a repository.

The application backend:

- exposes typed data reads, commands, and GUI-driving events;
- may be backed by in-process services, a daemon, or a mock;
- carries data-only values across process boundaries;
- does not expose renderer handles, Dioxus signals, or UI components.

### 3. Adapt the TanStack DB model, not its implementation

TanStack DB provides the vocabulary and lifecycle model we want:

- normalized **collections**;
- reactive **live queries** over collections;
- query-driven loading and preloading;
- optimistic collection writes;
- named intent-based actions for multi-collection mutations;
- mutation handlers that define persistence and acceptance;
- separate synced state and optimistic overlays;
- explicit transaction lifecycle and rollback.

Dayframe will implement a narrower Rust-native version of these concepts. It will not adopt a generic TypeScript query engine, a PostgreSQL compatibility layer, or a second sync/database model.

A feature may own the query shape and collection projection for its domain. A collection can
combine rows or events from multiple services and repositories when that is the feature projection's
natural boundary. The shared data runtime supplies the common mechanics:

```text
Collection<T>
  normalized local data plus optimistic transactions

LiveQuery<T>
  reactive query over one or more feature collections

Preload<T>
  starts or retains a query subscription before a view needs its first result

Action
  named feature intent, potentially affecting multiple collections

Transaction
  optimistic local change plus backend persistence and acceptance
```

The exact implementation may use feature-specific indexes or acceleration structures. The shared runtime MUST NOT force all features into one universal query language. A feature that needs a sorted index, graph projection, or specialized cache may implement that projection behind the common live-query and transaction contract.

### 4. Make local data immediately readable and refresh in the background

Mounting a live query has two independent effects:

1. Read the current local collection immediately.
2. Ensure a refresh/subscription request is sent to the application backend.

A refresh being pending MUST NOT make an existing local value disappear or force a feature into a generic loading state. A preloaded query should make the first draw useful when the route requires data immediately, but every mounted query still ensures that the latest backend data is requested.

A query result therefore describes the current local view and its evidence, rather than being only `Loading | Ready | Error`:

```text
LiveQuery<T> {
  current: T or an explicitly known empty result,
  availability: unknown | partial | complete,
  freshness: local | refreshing | current | stale,
  pending_transactions: transaction identifiers,
  error: optional refresh or source-update error,
}
```

The application should normally render the best available `current` value. A feature may show a structural first-use placeholder when no useful local value exists, but a network-like spinner MUST NOT be the default consequence of an asynchronous refresh.

Preloads are retained according to a query policy chosen by the feature. Small, stable data such as settings may be eager. Large document or catalog data should be on-demand or progressive. The application should preload the next likely route or pane rather than preload the entire repository.

### 5. Define optimistic mutation semantics explicitly

A feature mutation follows this lifecycle:

```text
intent
  -> optimistic transaction applied to local collections
  -> command sent to application backend with its stable command ID
  -> successful mutation response commits the optimistic changes locally
  -> later source snapshots/events keep the projection current and may correct transformations
     or remote changes
  -> backend rejection or transport failure rolls the optimistic transaction back
```

The runtime MUST distinguish these milestones:

- **locally applied**: the optimistic state is visible in local collections;
- **accepted**: the responsible service returned a successful mutation response, which is the
  durable acceptance boundary for this application transaction;
- **rejected**: the backend explicitly rejected the command and the optimistic state was rolled
  back;
- **rolled back**: the optimistic state was removed because of rejection or a transport/backend
  failure.

There is no separate optimistic-write settlement phase that waits for a later collection event.
A successful command response MUST commit the optimistic changes into the collection's local
base without requiring an event watermark. Later source events and snapshots remain authoritative
for keeping the projection current and may apply server-side transformations or remote changes.
Command IDs are idempotency identities and MUST be preserved across retries. Transport/backend
errors and explicit command rejection SHOULD remain distinguishable to feature error handling.

A collection source owns any Automerge heads, global event cursors, per-item versions,
deduplication, and ordering needed to apply those later source events. The generic collection
runtime does not infer source ordering from these values.

Simple CRUD mutations may use collection-level operations. Multi-collection or intent-heavy operations MUST use named feature actions. Examples include creating a document and its initial facets, changing a selected tab while updating related state, or provisioning ML tooling and observing its progress task.

The runtime should follow TanStack DB's useful defaults:

- optimistic state is layered separately from synchronized state until acceptance;
- successful persistence commits the optimistic changes into synchronized local state;
- failures remove the relevant optimistic transaction;
- multiple edits may be merged when the feature's intent semantics permit it;
- operations that are unsafe to guess optimistically may opt out and wait for authoritative data.

### 6. Separate GUI-driving events from derived-state work

The application backend exposes GUI-driving events for every open application instance. These carry
source data and, when useful for the source adapter, opaque cursors. Source adapters apply their own
ordering and handoff rules before updating local collections and live queries.

Durable derived-state work remains a separate node-service concern. It must be crash-resistant, idempotent, and deduplicated according to the service's coordination rules. UI code must not treat a derived-state task as a synchronous extension of a button click unless the feature explicitly models that task and its progress.

This preserves the event-lane distinction established by `docs/DEVDOC/log.md` while giving the application a clear read model.

### 7. Standardize feature modules without creating micro-frontends

Features are vertical modules inside `dayframe_app`. They are not independently deployed applications and do not receive separate crates merely because they have a directory.

Rust module roots use `feature.rs` plus a sibling directory, never `mod.rs`:

```text
app/src/
  features.rs
  features/
    settings.rs
    settings/
      page.rs
      layout.rs
      components.rs
      components/
        section_list.rs
        detail_pane.rs
      data.rs
      commands.rs
      state.rs
      loading.rs
      error.rs
      tests.rs
```

The feature contract is:

- `feature.rs`: feature boundary, exports, and route registration;
- `page.rs`: route entry and high-level composition;
- `layout.rs`: optional feature layout or list/detail arrangement;
- `components.rs` and `components/`: feature-specific presentation;
- `data.rs`: live-query declarations and feature projections;
- `commands.rs`: named feature actions and mutation intent;
- `state.rs`: ephemeral UI state only;
- `loading.rs`, `error.rs`, and `not_found.rs`: optional feature-specific states;
- `tests.rs`: feature-local tests.

Empty files are not required. A feature uses only the modules it needs.

Generic design-system components remain in `ui`. Application-wide composition components belong in `app/components.rs` or its sibling directory. Feature-specific components stay with the feature. No feature may become a dumping ground for generic UI or direct storage access.

### 8. Make feature flags a first-class application concern

The application is continuously delivered from the main branch and may receive over-the-air updates. Code can therefore be present in a live build without being enabled for every user.

Dayframe uses three distinct mechanisms:

1. **Cargo features** for compile-time platform or dependency capabilities, such as native camera support or an optional heavy subsystem. Cargo features are not the user rollout system.
2. **Runtime feature gates** for whether a shipped feature is available in the current build, release channel, installation, or user cohort.
3. **Experimental attributes** for targeting previews to named channels, cohorts, devices, or developer accounts.

Runtime flags are centrally evaluated from a `FeatureContext` during application bootstrap.
Feature IDs use an extensible namespaced `FeatureId` newtype (for example,
`settings.plugs.import`) and are defined or evaluated through one registry rather than as arbitrary
strings scattered through components. The registry MUST NOT require a central enum variant for every
runtime flag. Gates control, as appropriate:

- route registration;
- navigation visibility;
- query preloading;
- command availability;
- background task startup;
- experimental UI affordances.

A disabled feature must not start its data subscriptions or background work merely because its module is compiled. Feature code should be capable of existing in a shipped build while remaining unreachable through the active route tree.

Feature flags do not replace data compatibility. Persisted data migrations, node-service contracts, and OTA version boundaries must remain safe when an older or differently gated application encounters data written by another version.

### 9. Develop the UI against mock application backends first

The UI and feature contracts are designed before their production node-service implementation. `dayframe_app` therefore provides a mock application backend for development and tests.

The mock backend MUST be able to model more than successful static fixtures:

- immediate local snapshots;
- delayed refreshes;
- event-driven updates;
- optimistic success;
- optimistic rejection and rollback;
- authoritative conflicts or rebases;
- unavailable services;
- progress-producing commands.

This lets the UI act as the product design surface while backend work follows the agreed feature contract. A production backend adapter can then be added without changing feature presentation code.

### 10. Test the application in layers

- Pure data runtime tests cover collection updates, live-query invalidation, preload deduplication,
  optimistic transactions, accepted command responses, rollback/rejection, mutation merging, and
  source-adapter event application.
- Feature tests use the mock application backend and exercise the feature's real query and command paths.
- Shared UI tests cover generic component semantics and design-language states without application data.
- Native tests cover renderer/layout/input behavior that cannot be established by pure data or virtual-DOM tests.
- Platform smoke tests verify only the composition root and platform services.

Race-sensitive completion behavior must have deterministic interleaving tests. Sleeps, retries, and enlarged timeouts are not substitutes for ordering tests.

## Proposed amendment: micro composition requirements

**Status:** Proposed; the accepted decisions above remain the foundation. This amendment records
agreed direction from the micro design discussion, but requires resolution of the questions below
before its composition contract is accepted. It specifies responsibilities, not concrete Rust APIs.
Where this amendment revises shell ownership or feature registration, the revised direction is
identified explicitly; it does not imply that the existing runtime already implements it.

### Purpose and boundaries

Dayframe is a configurable, browser-shaped workbench for consuming Daybook services. It must make
ordinary features inexpensive to develop, independently testable, and replaceable without reworking
unrelated features. End users must be able to change composition beyond CSS. The default chrome is
not a privileged hardcoded arrangement: a composition micro may implement the sidebar/main-content
layout, and a future alternative may arrange content on a canvas.

This revises the feature-first shell direction in sections 1 and 7: feature modules remain useful
code organization, but contribute through explicit contracts rather than assuming a fixed shell.
Feature gates remain availability policy; they are not the composition or extension mechanism.

The ownership boundary is:

- **Nodes and nodules:** durable computation, persistence, synchronization, and long-lived work.
- **Application data runtime:** service-backed reactive collections and optimistic mutations,
  retaining the acceptance and source-adapter semantics established above.
- **Dayframe host:** configuration reconciliation, capability mediation, dependency lifetimes,
  effect ownership, and adapters to rendering/platform facilities.
- **Micros:** transient interactive features and composition, consuming explicitly granted facilities.
- **Portable tiles/views:** the narrower, host-independent presentation surface. A tile MUST NOT
  require knowledge of micros, Dayframe chrome, or a particular workspace arrangement.

A micro may start durable node work and display progress; unloading the micro MUST NOT undo accepted
mutations or implicitly cancel work whose lifetime belongs to the node. Micros are not a second
backend. General process hosting, an ECS scheduler, and a universal micro-to-micro event bus are not
requirements.

### Vocabulary and provision inventory

These names identify roles, not a mandatory public type for every row:

| Name | Responsibility |
| --- | --- |
| Host | Supplies the stable runtime boundary and mediates access and lifecycle. |
| Micro provider | Implementation and declared contracts from which instances can be configured. |
| Micro instance | Configured identity that composes functionality and owns its transient effects. |
| Integration | A composable part of a micro with its own requirements and owned lifetime. |
| Mount | One live presentation, distinct from registering its provider. |
| Capability | Opaque authority token restricting permitted operations; not an interface or an ability. |
| API contract | A known, typed facility against which a micro compiles. |
| Provision | An actually published implementation of an API contract. |
| Contribution | An owned registration into a facility exposed by the host or another micro. |
| Override | An independently owned presentation adjustment, permitted only by an explicit contract. |
| Action | Invocable operation; its implementation is separate from UI discovery and placement. |
| Trigger | Provider-defined occurrence with explicit context, not an ambient broadcast of the world. |
| Observable query | Maintained result over state available through a granted capability. |
| Composition | Arrangement and binding of presentations, driven by configuration/workspace state. |

The initial provision needs are **panel content**, **semantic toolbar/menu entries**, **actions and
input/UI placements**, **scoped interaction context**, and **contribution facilities exposed by
composition owners**. Micros may also supply or host tiles; portable tiles remain independent.
This is not a closed list of micro kinds. A normal chat feature can be one panel-providing micro;
its component tree MUST NOT require micro-specific ceremony throughout its implementation.

Capabilities needed to support these provisions include:

- granted application-data reads and mutation intents through the existing data boundary;
- configuration-facet access using existing manifest facet tags, key IDs, owner selection, and rights;
- owned contribution registration and observation for named collections/extension points;
- explicitly granted action definition/invocation and contextual presentation;
- provider-defined triggers and structured observable queries;
- component-provider registration, mediated mounting adapters, and appropriately granted composition access;
- theme/presentation environment and required platform adapters supplied by the host.

The precise contract granularity, scope resolution, and initial platform inventory remain open.
No micro receives an unrestricted service, facet, action, or collection lookup by default.

### Shared state, observation, and interaction

Composition contribution collections MUST remain separate from service-backed data collections.
Their writes concern registration ownership and disposal, not optimistic persistence or source cursors.
A micro may update or withdraw entries it owns. It MUST NOT mutate another owner's entries by default.
An owner may expose an override facility whose separately owned entries derive effective presentation
without restoring stale snapshots during cleanup. Override precedence and permitted properties
require an explicit provider contract.

Toolbar/menu entries SHOULD describe semantic content and action references rather than arbitrary
DOM fragments. Their owner controls rendering, theming, and layout. Internal DOM structure is not a
stable micro extension interface. Editor/buffer-specific malleability is outside this decision.

The host/provider executes structured queries over granted context or contribution state and tracks
active subscriptions. Micro-owned callbacks consume results; a generic callback does not require a
generic world broadcast. Query inspection must identify its owner and requested contract; retaining
actual result values is a separate tracing/privacy policy. Structured descriptions allow evaluation
sharing or indexing where justified, but do not require a universal query language.

Triggers and maintained query results are distinct. A menu-opening trigger may supply an explicit
resource target and a contribution facility for that occurrence. Normal DOM events inside a native
component use the renderer's normal component APIs. Cross-provider interactions use explicit semantic
contracts rather than making native event objects the portable boundary.

Panels, buttons, and placement roles belong to composition grammar and its providers, not necessarily
host primitives. The host mediates component references and mounting as well as action invocation;
ordinary native components use local adapters. A portable component reference is not a DOM pointer.
Required renderer facilities and ownership/teardown contracts remain to be established. This direction
does not grant unrestricted cross-micro DOM querying or make internal DOM structure a stable API.

### Composition and lifecycle requirements

Configured enablement, dependency availability, implementation activation, and presentation mounting
are different facts. The design MUST permit enabled functionality to remain unloaded/inactive until
needed; exact demand triggers and descriptor requirements remain unresolved.

Boot configuration selects root instances. Downstream creation occurs through an explicitly granted
instantiation capability and MUST name an owning lifetime. Instantiating a child is an owned effect;
removing its owner disposes the child. Multiple windows may instead be mounts within one instance.
Persistence is a separate operation through granted configuration facilities, not an automatic
consequence of instantiation. Persisted entries retain identity independent of placement.

Explicitly granted ambient scopes may own longer-lived transient instances that clean themselves up.
They remain bounded by application shutdown; they are not unowned tasks, implicit persistence, or
substitutes for durable node work. Boot ownership and downstream owned creation settle the lifecycle
structure. Multiple instances are permitted; launching once is not a singleton enforcement policy.

A broad micro MUST be composable from smaller units without requiring one trait implementation per
contribution role. Shared authoring helpers may implement conditions and integrations in micro-owned
code on top of simpler trusted registration/dependency primitives. The host owns enforcement of
ownership and authority, not every authoring policy. Optional integrations must be able to appear and
disappear without restarting unrelated parts of their parent micro.

Every host-mediated transient effect MUST have an attributable owner and a disposal path. Removal
withdraws that owner's effects without erasing unrelated contributions. A contribution created for a
trigger occurrence must be bounded by both the contributing integration and the occurrence lifetime.
Activation/disposal races, partial activation, and provider replacement must not leak registrations.
Dependents must finish required cleanup before their bound provider is destroyed, retaining access to
that dependency for cleanup. Persistent workspace/configuration records are not transient effects.

Stable configuration identities and live registration identities MUST be distinguished so stale
cleanup cannot withdraw a replacement registration. Configuration must retain enough identity to
refer to providers across disablement or unavailability; placeholder/restoration policy is open.

Declarations, desired configuration, and live provisions MUST NOT be conflated. A declaration that a
provider supports a contract does not satisfy a consumer dependency; an actual live provision does.
Absent requirements must be inspectable. Mandatory versus conditional provision declarations and the
response to a missing promised provision remain to be decided.

### Authority, portability, and verification

Dependency requests name contracts rather than importing another micro's implementation. Requests
are not automatic grants. Authority must build on the existing capability-oriented regime, including
facet-tag/key access; read, contribute, arrange, override, and invoke need not be the same permission.
Native micros are trusted linked code: this API discipline is not an OS sandbox.

Contracts MUST preserve a path to WIT/JS and separate-address-space implementations. Data, explicit
handles, subscriptions, invocation, and acknowledged disposal form the portable boundary. Native
callbacks and Dioxus factories may remain convenient local adapters; they are not transport values.
No immediate WASM loader, marketplace, process model, or universal serialized DOM is required.

A deterministic test environment must exercise independent contributors, disable/re-enable, failed
activation, dependency withdrawal, context changes, and cancellation/late results without requiring
full application startup. Inspection must identify active requirements, provisions, queries, owned
registrations, and outstanding lifecycle work. Renderer/input behavior requires separate native
verification; type checking an authoring example is not lifecycle evidence.

### Desktop Settings slice and the document narrow waist

The desktop MUST consume a running Settings micro and a separate configurable contributor before
expanding the framework. Settings defines a typed section API; the host owns collection storage,
read/insert/own-entry authority, component references, root mounts, and recovery. The contributor
receives Settings as an explicit typed dependency, not through generic interface discovery.
Settings MUST NOT receive the contributor's document grant merely because it mounts its content.
Native render factories stay in the renderer adapter; only mediated references cross the host boundary.

Each context receives an authorized sparse document mirror. A shared host service owns targeted
hydration, refresh deduplication, retention/eviction, and the existing optimistic collection runtime.
Watch selections MUST support multiple explicit documents and ordinary facet tags or exact keys;
well-known tags are not a separate watch vocabulary. A document ID obtained through another service
is not authority. The host MUST filter before hydration/delivery, revoke subscriptions, and reject
stale handles. Active watches and pending writes retain their projections. Failed writes roll back;
accepted writes commit without waiting for a later source event. Instance closure does not undo writes.

The future core adapter will consume `index/facet_set.rs` and hydrate values through drawer. It MUST
preserve bounded frontier reads, replay/removal semantics, source-owned cursors, and gap-free handoff.
The current document-selector scan is not an indexed lookup and MUST NOT be described as one.
Listing/discovering documents by facet requires a future, separately granted API. The desktop slice
uses a mock source; it does not introduce a public index/search service or wire to core.

### Initial micro provider seam

The first implementation keeps feature code modular without making the application runtime a hand-written list of concrete micros. `ApplicationRuntime` owns a dynamic provider registry. A provider has a stable `MicroId` and versioned descriptor, declares its surface/command/placement/context contribution contracts, implements the host's `MicroProvider` contract, and is activated through a fresh host-owned instance. Provider registration and configured activation are separate operations, so one implementation can serve multiple instances and an instance can be withdrawn without unregistering its provider.

Native activation adapters construct their micro's typed context from host facilities and explicitly bound requirements. `MicroActivation` is trusted adapter authority, not the feature's capability context. Configuration binds a declared contract to a configured supplier instance. The registry checks that an actual live provision with a compatible version exists before activating the consumer, and records the dependency for host-owned withdrawal. No arbitrary interface enumeration is exposed to feature code.

Publication is staged until activation succeeds and all declared mandatory provisions are fulfilled. Failure closes owned effects without exposing partial outputs. Committed provisions belong to the runtime incarnation; owner closure withdraws them, and stale cleanup cannot remove a replacement. The initial native adapter permits one output per contract per instance; foreign adapters and multiple named outputs remain future extensions, not simulated transport values.

Micros live under `dayframe_app::micros`, using the `settings.rs` plus `settings/` convention. Consumers import public contracts, not provider controllers. Settings and Sample use the same registration and activation boundary. The application runtime contains neither implementation; desktop selects instance bindings and retains mock/demo controls, but does not assemble their feature contexts. Portable tiles remain below this boundary.

Boot reconciliation consumes a projection of configured instances: stable identity, provider identity, enablement, and explicit requirement-to-supplier bindings. It does not introduce another configuration store. Dependency order is derived from declarations and bindings; unchanged live incarnations are retained. Disabled, removed, or reconfigured instances are closed, and host dependency ownership withdraws affected consumers without restarting unrelated instances. Enabled consumers remain blocked when suppliers are unavailable, and a later reconciliation restores fresh incarnations when their requirements become available.

Malformed binding edits and dependency cycles are rejected before changing the live graph. Reconciliation reports disabled, stopped, unavailable-provider, blocked, active, and failed states separately. Activation failures are retained until configuration or dependency incarnations change; unrelated edits do not repeatedly invoke a failing provider. `ApplicationRuntime::configure` applies a desired projection; one application-owned local `observe` future reconciles host closure and provider-registration changes and publishes current reports. Subscription precedes the initial snapshot, and host closure notifications follow the complete dependent recovery. Explicitly closing an incarnation leaves it stopped until its configuration changes, rather than automatically restarting it; dependency withdrawal blocks consumers until the supplier can return. Host shutdown ends observation after cleanup. Newly available incompatible declarations publish a configuration error without disturbing the previous live graph. This remains an event-driven mechanism, not a timer retry loop; the desktop's desired projection is still in-memory, not a durable storage adapter.

### Decisions required before implementing the composition contract

1. **Identity:** support multiple configured instances with stable configured identities, fresh runtime
   incarnations, and independently owned presentation mounts. No provider multiplicity barrier is imposed.
   Keyed creation may reuse a resource experience; it is not a universal singleton rule.
2. **Activation:** use a shared, host-evaluated structured query/condition vocabulary for activation,
   observation, and trigger selection, extensible as real needs arise. Its syntax and treatment of
   occurrences versus maintained state remain open; dependency availability is a separate condition.
3. **Contextual contributions:** support asynchronous activation and contribution directly. Authors may
   implement placeholders/staging through shared primitives; predeclared synchronous stubs are not a
   mandatory contract. Fixed occurrence targets and maintained contextual results remain distinct;
   cancellation, late-result handling, and visible-update policy need specification.
4. **Provision fulfillment:** all mandatory provisions MUST be present before activation is reported
   successful. A missing promised mandatory provision fails activation and requires partial-effect
   cleanup. Conditional provisions must be identified separately. Cordis resolves actual provisions
   but does not itself enforce this stronger fulfillment rule; failure reporting remains to be specified.
5. **Invocation:** host-mediated action handles, not transferred callbacks/pointers. The provider
   executes its action in its own runtime; the invoker need not know the provider's embedding details.
   Use an asynchronous public contract across address spaces, permitting local direct-call optimization
   without changing authority or lifecycle semantics. Results and in-flight cancellation remain open.
6. **Composition persistence:** layout implementations own remembering placement through granted
   persistence/configuration facilities; common implementations/helpers can supply ordinary behavior.
   Panel availability and micro enablement do not imply a particular placement. Develop a shared layout
   grammar for semantic placement intent (for example primary/secondary), interpreted by layout owners
   rather than exposing their physical arrangement. Whether that grammar is provided by a dedicated
   micro or a common contract, provider-switch migration, and unavailable-layout recovery remain open.
7. **Resolution and overrides:** use deterministic tie-breaking rather than activation order for
   equally ranked candidates; exact priority and stable tie-break keys remain to be specified. Provider
   selection, placement/presentation resolution, and invalid-configuration recovery still need contracts.
8. **Capability surface:** which frontend contracts micros may define, optional dependency semantics,
   scope isolation, and compatibility checking against existing manifest/configuration mechanisms.

### Evidence and deliberately limited borrowing

- [Cordis paper](https://arxiv.org/abs/2608.25512), sections 3.4.2 and 5–6: tagged owned registrations,
  dependency-mediated lifetimes, configuration reconciliation, and access mediation. Its implementation
  tracks supplied inverses but does not verify their correctness; declaration-only predictions assume
  fulfillment of declared provisions. Dependency activation is not demand-based module fetching.
- [VS Code commands](https://code.visualstudio.com/api/extension-guides/command) and
  [contributions](https://code.visualstudio.com/api/references/contribution-points): handler registration,
  contextual visibility/enablement, and UI placement are distinct.
- [Emacs named features](https://www.gnu.org/software/emacs/manual/html_node/elisp/Named-Features.html):
  composable libraries and demand loading, not our authority or removal guarantees.
- [lazy.nvim specifications](https://lazy.folke.io/spec): explicit loading triggers; not evidence of
  general reversible runtime unloading.
- [Bevy plugins](https://docs.rs/bevy/latest/bevy/app/trait.Plugin.html): ordinary function-based assembly;
  startup `cleanup` and entity despawn do not constitute general plugin removal.
- Patchwork motivates data-driven, inspectable composition. Detailed correspondence to its current
  source must be verified before adopting implementation-specific claims.

The TypeScript material in `.agents/drafts/dayframe-micros.*` is exploratory evidence only. Subsequent
work should implement bounded real Rust contracts and verify them with ordinary Dioxus integration,
not mechanically translate that sketch or treat its unchecked generics as capability enforcement.

## Consequences

- Desktop, mobile, and web share one application architecture without sharing platform assumptions.
- Features can be designed and tested before their production node-service implementation exists.
- Most views can render local data immediately while refresh and synchronization continue in the background.
- Optimistic behavior has an explicit acceptance-and-rollback protocol instead of ad-hoc per-screen state.
- Feature rollout is independent from whether code has shipped in the main OTA build.
- The application gains a shared data runtime, but it does not prematurely adopt a universal query language or a second database/sync system.
- The `ui` crate remains reusable and renderer-focused instead of becoming a domain application crate.

## Migration

1. Add the `dayframe_app` crate and thin platform launchers.
2. Add the application backend trait and a mock implementation first.
3. Implement the shared collection/live-query/transaction protocol with one feature, preferably Settings.
4. Implement the Settings feature using the standardized module layout and mock backend.
5. Add the feature registry and a disabled experimental route to prove the rollout path.
6. Add the production application-backend adapter after the UI contract is stable.
7. Connect the remaining features incrementally; do not port the Kotlin view models as giant classes.

## Deferred decisions

- The exact node-service inventory and which services remain in `daybook_core` versus new crates.
- The first platform's in-process versus daemon deployment mode.
- The concrete local collection index implementations for each feature.
- Whether a future feature needs a differential-dataflow-style query engine; no such dependency is justified before measurements.
- The persistent storage and restart semantics for pending optimistic transactions.
- The exact remote feature-flag delivery mechanism and OTA manifest format.
- The final IRPC/wRPC mapping for server-hosted services.
