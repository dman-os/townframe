# Task attribution conventions (tracing → OpenTelemetry)

Purpose: any task a process spawns must be attributable from its own log lines back to the
work item that caused it (a hub command, a worker, a peer, a document, an object). The span
tree is the correlation mechanism — we deliberately did **not** add a spawn API, a purpose
enum, or a wrapper. These are the conventions the current pass established; follow them for
new code.

## Rules

1. `#[tracing::instrument(...)]` on functions that **own a loop or spawn tasks**. Never on a
   per-operation helper, and never inside a per-part/per-key loop: span count must be
   `O(tasks)`, not `O(operations)`.
2. Span names are **static and low-cardinality** (the function name, or `name = "..."`).
   Ids go in fields, never in names.
3. Declare ids you do not know yet as `tracing::field::Empty` at span creation, then
   `tracing::Span::current().record("peer_id", tracing::field::display(&peer_id))` once known.
   Fields **cannot** be added after creation — this is the tracing contract.
4. For a future handed to `spawn`, use `.instrument(span.or_current())` (documented as
   cheaper than nesting `instrument(...)` inside `in_current_span()`; `or_current` also keeps
   the caller's span as parent when the new span is disabled). Prefer `tracing::Instrument`
   over `tracing_futures::Instrument`.
5. Never hold `Span::enter()` across an `.await` — it produces incorrect traces. Use
   `.instrument()` for async code and `Span::in_scope(...)` for synchronous sections.
6. When one unit of work spawns several related background tasks, relate them causally with
   `follows_from` (`span.follows_from(&cause)` or `#[instrument(follows_from = causes)]`).
   Do not fake nesting to represent causality.
7. On fallible boundaries prefer `#[instrument(err(Debug))]` / `ret` so failures surface as
   span status rather than only as log lines.
8. On RPC/command surfaces add `otel.kind = "server"` (or `"client"`) as a field.
9. Preserve existing comments and style; keep diffs reviewable; instrumentation must be
   behaviour-neutral.

## Field vocabulary (reuse these exact names)

| field | meaning |
|---|---|
| `worker` | the long-lived worker/machine this task belongs to (kebab-case label) |
| `task_id` | id of the unit of work, unique **within its worker** (hence `worker` is always paired with it) |
| `peer_id` | authenticated remote peer |
| `doc_id` | document being worked on |
| `obj_id` | object/part/blob identity |
| `otel.kind` | `"server"` / `"client"` for OTel span kind (default internal) |

## Level policy

- **INFO** for long-lived worker/loop spans: one per worker, bounded, created at
  construction. This matches the existing convention (`big_repo/changes.rs`,
  `big_repo/ephemeral.rs`, `big_repo/runtime2/hub.rs`, `daybook_core/sync.rs`) and is what an
  OpenTelemetry exporter selects by default — long-lived worker spans are the ones worth
  exporting.
- **debug** for task-scoped spans (e.g. a per-task `run`), so a task's own lines carry its
  identity without flooding INFO.
- A span that was already INFO in the base keeps its level when extended.

## OpenTelemetry readiness

Today the exporter is a configuration change, not a code change, because:

- parent/child structure is honest (causality uses `follows_from`, not fake nesting);
- `otel.kind` marks client/server surfaces; `otel.name` is the sanctioned way to override a
  display name; `otel.status_code`/`otel.status_description` exist if we set status explicitly;
- semantic-convention attribute names can be assigned directly as fields
  (e.g. `"server.port" = 80`).

Special field prefixes (`otel.*`) are reserved by `tracing-opentelemetry` and ignored by
other layers, so they are safe to carry in ordinary builds. For counts, prefer the
OpenTelemetry metrics path over hand-rolled counters.

## Known gaps (decisions still open, do not silently "fix")

- `big_sync_core`'s `ConcurrentDeltaWalker` and `TokioKeyedScheduler` address keys with only
  `Ord + Clone` / `Eq + Hash + Clone` — no `Debug`/`Display` — so those spans are named by
  what is in scope (physical `task_id`, `cursor`, `durable_revision`). Widening the public
  generic bounds would change interfaces consumed by `daybook_core` and
  `big_repo/runtime2`.
- Machine-task per-kind ids (`MachineTaskDeets`) are `pub(crate)` to `big_sync_core`, so the
  outer `machine_task` span carries `task_id` + `worker` and the per-kind `run` spans carry
  `peer_id`/`obj_id` underneath.
- RPC **client** surfaces have no spans; per-request spans there are `O(operations)` and are
  therefore out of scope until we decide a sampling story.

## Verification

Instrumentation is cheap but not free: check changes against the recorded baselines —
isolated tier10 stress ≈ 51 s, `-p big_repo` ≈ 85 s, loaded 4-package run ≈ 234 s.
Instrumentation must stay within noise on all three. Gates: `cargo check --all-targets`,
`cargo clippy -D warnings`, `cargo fmt --check` on the touched crates.

Note on environment: CI sets `UTILS_RS_TIMEOUT_MULTIPLIER=3` (`.github/workflows/checks.yml`),
which scales internal deadlines; local full-parallel stress runs default to 1.
