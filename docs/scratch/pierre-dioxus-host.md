# Pierre's hosts over standard Dioxus (claytxt lane, draft)

Status: draft, 2026-11-10. Replaces the web-sys `WebHost` in `pierre_diffs_dioxus` (S10). Inputs:
W1/W2 (`docs/scratch/pierre-spikes.md` on `spike/pierre-scroll`), the host traits ported in S10
and S11a, and the operator's rules for the primary (browser/webview) backend: use standard
Dioxus APIs, don't design around Blitz's gaps, never assume JS.

## The problem

The ported classes (`Virtualizer`, `ResizeManager`, `ScrollSyncManager`, the scrollbar gutter
probe, and next `InteractionManager` and `FileDiff`) reach the page through host traits:

| Trait | Asks for |
|---|---|
| `VirtualizerHost` | scroll top and heights of the root and window, element tops and heights, first child, anchor lines by `data-line-index`, scroll writes, resize and intersection observers, scroll and resize listeners |
| `ResizeManagerHost` | children, tag name, attributes, the `[data-line-annotation*=","]` query, bounding height, style property writes, connectedness, a resize observer |
| `ScrollSyncHost` | children, `dataset` membership, scroll listeners per side, `scrollLeft` reads and writes, timers |
| `ScrollbarGutterHost` | connectedness, `offsetHeight - clientHeight` of a probe |

Upstream answers all of these synchronously from a live DOM. Inside a Clay micro there is no
DOM: the guest renders Dioxus elements, and the host owns layout.

## The split: structure from the model, geometry from events

Every request above is one of two kinds.

1. **Structure:** children, tag names, attributes, queries, `dataset`, style writes. The guest
   rendered that tree itself, from hast that the ported renderers produced. So the answer
   already exists in the guest. `Element` becomes a stable key of a rendered hast node, and
   structural reads walk the hast model synchronously, exactly as upstream walks the DOM. Style
   and attribute writes edit the node's `Properties` and mark the component dirty, and Dioxus's
   diff turns that into mutations. Nothing crosses the wire to answer a structural question.
2. **Geometry:** heights, tops, scroll offsets, intersection. Only the host knows these. The
   adapter attaches standard listeners to the nodes the classes observe, and keeps a cache:

| Upstream | Dioxus source |
|---|---|
| `ResizeObserver` entry, `borderBoxSize.blockSize`, `contentBoxSize.inlineSize` | `onresize`: `ResizeData::get_border_box_size().height`, `get_content_box_size().width` (horizontal writing mode) |
| `IntersectionObserver` entry | `onvisible`: `VisibleData::is_intersecting`, `get_bounding_client_rect`, `get_intersection_ratio` |
| scroll listener, `scrollTop`, `scrollLeft`, `scrollHeight`, `clientHeight` | `onscroll`: `ScrollData::scroll_top`, `scroll_left`, `scroll_height`, `client_height` |
| `getBoundingClientRect` on demand | `MountedData::get_client_rect` (async; see below) |
| `scrollTo`, `scrollLeft =` | `MountedData::scroll(coordinates, behavior)` |
| `isConnected` | the node is in the rendered model and its `onmounted` has fired |

`observe(element)` therefore means "render this node with `onresize` (or `onvisible`) attached",
and `unobserve` removes the listener on the next render. A node's `MountedData` is captured in
`onmounted` and dropped when the node leaves the model.

## Synchronous reads against async APIs

The traits are synchronous because upstream's are. The cache answers most reads synchronously.
For the reads that must be fresh when they're used (scroll correction after a height change),
W2 measured the answer: read the offset and then scroll by the delta, as two element commands
in the same task. That left no visible jump; using the last cached scroll top left 50 px. So
the adapter turns upstream's `scrollFix` (read `scrollTop`, then write `scrollTop + delta`)
into one command pair: `get_scroll_offset().await` followed by `scroll(offset + delta)`.
This is the one place the port changes upstream's control flow, and it has to be recorded as
a divergence.

That depends on host contract 1 in the review-loop doc: commands issued from an event or resize
handler run in the same task, before paint. In W1/W2 it held because of the executor, not
because anything guarantees it. Clay has to make it a contract.

## Frames: `requestAnimationFrame`

`UniversalRenderingManager` queues render callbacks for the next animation frame, and the
`Virtualizer` and `ResizeManager` lean on that batching. Dioxus has no animation-frame API,
and JS is ruled out. The options:

- **A. Flush the queue at the end of each dispatch.** Every event that queued a render runs the
  queue before returning. W1 showed that writes made inside a dispatch land before paint. Cost:
  several scroll events in one frame each render, where upstream would batch them into one.
  W1 saw about 50–130 response events per 3 s run with Pierre's window, so it's affordable, but
  it is a semantic change.
- **B. A host frame event.** Clay delivers a frame tick to micros that asked for one (an
  `onframe`-like listener on the surface). This keeps upstream's batching exactly, but it's a
  Clay invention, not a Dioxus API.

Recommendation: start with A, measure with the W1 harness, and only add B if batching shows up
in the numbers. Either way it's a decision for the operator, because B extends the wire.

## Timers

`ScrollSyncManager` uses `setTimeout`. In the guest that becomes a spawned task awaiting a
sleep from the guest's clock (wasi clocks on web are `performance.now()`). Cancelling drops
the task. To verify: the wcmp runtime's `first_poll` rule panics if a synchronous export
suspends, so timers must run as their own tasks and never inside a dispatch.

## Open measurements

- **Scrollbar gutter** (`offsetHeight - clientHeight` of a probe): `onresize` gives the border
  box and content box. Their difference includes padding, so the probe needs zero padding to
  make the subtraction exact. Until that's verified, use `--diffs-scrollbar-gutter-fallback`.
- **Anchor lines:** upstream queries the shadow root for `[data-line][data-line-index]`. The
  guest answers this from its model (which rows it rendered, in order). Their tops come from the
  `onvisible` rects, or from summed `onresize` heights below the file's top.
- **Coalescing:** one `onresize` dispatch per observed row on mount (about 200 rows in Pierre's
  window) costs about 0.2 ms of wire each. A coalesced "sizes changed" surface event is a
  possible later optimisation (review-loop host contract 4).

## Prerequisites in Clay

- Open `data-*` names on the surface (Pierre emits about 60), and scoped stylesheets with
  egress sanitizing. Both are in the review-loop doc's needs table; they need the operator's
  input before design.
- Forward `visible` on the wire (the listener list has `resize` and `scroll`, not `visible`).
- The same-task command contract (above).

## Build order

1. After S11b: build the adapter in `pierre_diffs_dioxus`, replacing `WebHost`, against
   all four traits plus InteractionManager's. Test it in a headless VirtualDom with
   synthesized events, comparing against the S10/S11a oracle traces where the scenarios fit.
2. Smoke it in the browser through `dayframe_web` with a plain (non-micro) Dioxus page first,
   then inside a micro once the surface vocabulary allows Pierre's attributes and stylesheet.
3. Rerun the X5 and W2 harnesses on the result.
