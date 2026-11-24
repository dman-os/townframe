# Waker-driven render for guest views

Status: proposal for the operator. Nothing here is built.
Owner: claytxt lane.
Updated: 2026-11-25
Scope: how a wasm guest's Dioxus views re-render when async work wakes them, replacing the manual
`notify_views()` that every cross-executor delivery has to remember.

## The problem

A guest has two schedulers:
- **Dioxus.** A view's tasks (`use_resource`, `spawn`) are polled only inside `VirtualDom::render_immediate`. The
  guest calls that from `GuestDom::render` (`src/clay_mdk/guest/dom.rs`), and today `render` runs only from the
  synchronous `mount`, `unmount` and `dispatch` exports and from `update`.
- **The guest executor.** Component-model tasks, where node calls, byte bodies and `Context::sleep` run.

When executor work finishes, it wakes the Dioxus task that awaits it. A Dioxus wake only sends a message on the
VirtualDom's scheduler channel. Nothing reads that channel until the next `render_immediate`, so no render follows.
Every delivery path therefore calls `notify_views()` (`src/clay_mdk/guest.rs`), which bumps each presentation's
tick and renders.

A forgotten call is a silent hang. `call_bytes` hung this way: it notified after the head but not after each chunk.
`Context::sleep` needed the same call, and blobs and `fetch` will need it next.

## Proposal: one render pump per guest

Upstream Dioxus already has what a host runtime uses for this. `VirtualDom::wait_for_work()` (public, upstream,
`packages/core/src/virtual_dom.rs`):
- drains the scheduler channel;
- returns as soon as a scope is dirty or a task was woken;
- otherwise parks on the channel until a waker fires.

Dioxus's own desktop and web runtimes loop on it: wait for work, then render.

The guest does the same, on its own executor:

1. **At the guest's first mount, the MDK spawns one render pump.** This is an incarnation-owned task, the kind
   layer 60 added for `Context::sleep`. It loops:
   - wait for work;
   - `GuestDom::render()`, which renders and pushes each surface's edits through `surface.apply`, as it does now.
2. **The pump borrows the VirtualDom only inside each poll.** It never holds the borrow across an `await`.
   `GuestDom` is shared with the synchronous exports (`dispatch`, `mount`), which must still get
   `&mut VirtualDom` between pump polls.
   - So the pump polls a fresh `wait_for_work()` future under a short borrow each time it is woken, and drops it
     once it returns `Pending`.
   - That is sound because the parked state is the channel receiver's registered waker, which outlives the dropped
     future. A message sent meanwhile stays queued in the channel.
3. **`notify_views()` and the presentation tick for task wakes are deleted.**
   - A woken Dioxus task now wakes the pump, so every delivery path renders by itself, including ones nobody has
     written yet.
   - Mirror changes from the host (`changed`, `lagged`) still enter through `update`, because those are signal
     writes made from outside the runtime, not task wakes.
4. **Dropping the incarnation drops the pump.** Cleanup is RAII, as for `Context::sleep`.

### What changes where

| Where           | Change                                                                                    |
|-----------------|-------------------------------------------------------------------------------------------|
| Dioxus fork     | Nothing. `wait_for_work` is upstream public API.                                          |
| WIT / host      | Nothing. The guest already pushes its own batches through `surface.apply`.                 |
| `clay_mdk`      | The pump; `notify_views` and its callers removed (`context.rs`, `hooks.rs`, `guest.rs`).    |
| Host batching   | Each pump render is one batch, the same as a `notify_views` render today.                 |

### Why it is upstreamable

It uses only public Dioxus API, the same loop Dioxus's own renderers run. Nothing goes into the fork, so nothing has
to be upstreamed.

## Risks and checks

- **Render storms.** A task that wakes itself in a loop now renders in a loop. Dioxus already guards against this:
  its `queue_events` drains with `try_recv` so a self-requeueing task can't deadlock the runtime. A page of N
  deliveries still costs N renders, as it does today. If that shows up in profiles, coalescing (one render per
  executor turn) goes into the pump.
- **Executor support.** The pump needs a guest task that outlives the export that spawned it. `Context::sleep`
  already depends on that, so wasmtime and wcmp support it. The browser (JSPI) lane is untested for per-call
  exports (ledger) and needs the same check.
- **intxt builds on the guest view API.** Removing `notify_views` touches intxt's review micro wherever it calls it.
  This proposal goes to them before the layer lands.

## Tests

- **Delivery with no manual notify.** The `clay_node` e2e `a_view_reads_a_multi_chunk_byte_body_in_order` (layer 43
  fix) passes with the per-chunk `notify_views` calls removed.
- **Sleep wakes a view.** A view awaiting `Context::sleep` re-renders with no notify.
- **Dispatch still works.** An event dispatched while the pump is parked still gets `&mut VirtualDom` and renders,
  so the borrow discipline in step 2 holds.
