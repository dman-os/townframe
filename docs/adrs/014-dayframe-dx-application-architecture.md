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
