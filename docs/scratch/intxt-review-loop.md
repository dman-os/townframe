# intxt review loop, top down (claytxt lane)

Status: draft, 2026-11-10. Decided by the operator: boundary A, virtualization matching Pierre (X5), C1 parked for design. Owner: the claytxt lane (Clay and the
supporting pieces). The intext lane owns the datums, the intxt micro, MCP and the annotation
anchor model; this doc only states what the UI needs from them.

## Goal

intxt replaces pull requests. The first thing that has to work is the **write loop** on the
phone and the desktop. Agents push changes and annotate them. The operator reads the diff and
the agents' annotations, replies, adds annotations of their own, and resolves threads. Every
device runs its own node, and the loop is live: an annotation made on the desktop appears on
the phone through node sync. The web gets the same composition once the webnodes lane lands.
Pure read-only viewing is a later goal.

Build order is top down: every screen below first runs on mock data in Clay collections, and
each lane swaps a mock for the real piece when it exists.

## Journeys

| # | Journey | What the user does |
|---|---|---|
| J1 | Inbox | Sees the changes waiting for review, newest activity first, with unread annotation counts. |
| J2 | Change overview | Opens a change: its title, the agent's summary, the files touched with +/- counts, and open threads. |
| J3 | File diff | Opens a file: the diff with annotation threads inline at their anchors. Unified on a phone, split when wide. |
| J4 | Thread | Reads a thread, replies, resolves or reopens it. |
| J5 | New annotation | Selects a line or range (tap the gutter; drag or extend on touch) and writes an annotation there. |
| J6 | New revision | The agent pushes again. The diff updates, and threads either re-attach or show as outdated. |
| J7 | Verdict | **Not in the alpha** (operator, intext ledger D18): intxt doesn't manage trunks yet, so approve/request-changes vocabulary waits. A change is done when its threads are resolved. |

## Screens and where they live in the shell

The shell (ADR 032) already does list-and-details, owns navigation, and keeps arrangement. The
intxt micro contributes panels; it doesn't build its own navigation.

| Screen | Shell placement | Compact (phone) | Wide (desktop) |
|---|---|---|---|
| Inbox (J1) | list panel | full screen | sidebar list |
| Change overview (J2) | details of an inbox row; itself a list of files | pushes over the inbox | list column |
| File diff (J3) | details of a file row | pushes; back returns to the files | details column |
| Thread (J4) | inline in the diff; expanded as a sheet | bottom sheet over the diff | inline, expanded in place |
| Composer (J4, J5) | sheet | bottom sheet with the keyboard | inline under the selected range |

Back on the phone walks thread → diff → files → inbox through the shell's navigation stack.
Unified versus split comes from the window width. For the alpha both come through the shell's
existing `HostInputs` context. C1 (host contracts in place of that side channel) is parked:
arrangement state has to become datums under the new design, and some of the concepts it needs
don't exist yet. The operator's requirement is that the app remembers their preferences.

Arrangement and open-context state follow ADR 032 §4 and the `tables.rs` precedent. Section
weights and preferences such as the default diff layout persist and sync. The navigation stack
and open panels are device-local. Host facts such as the window's pixel size are not persisted.

## The boundary between the diff view and intxt

Pierre renders annotations as rows injected between diff lines (`DiffHunksRenderer` hooks,
ported in S6). In intxt, those rows are the threads, and the threads belong to the intxt micro:
its datums, its reply and resolve actions. So the diff view has to host content it doesn't own.
There are two ways to do that:

**A. The diff view is a library the intxt micro compiles in. (Decided for the alpha, 2026-11-10; a custom element can come later for other uses.)**
`pierre_diffs` plus a Dioxus component layer is a plain Rust crate, written without assuming it
runs as a micro, as agreed. The intxt micro renders the whole file screen. It passes the diff and
its annotations to the component and renders thread rows itself through a render callback, the
same way Pierre's own framework adapters work.
- Works with today's embed contract (props only) because nothing crosses a micro boundary.
- Threads are ordinary Dioxus inside the intxt micro, so input, focus and the keyboard behave like
  the rest of the micro.
- Cost: every micro that shows diffs links the diff code (wasm size), and a fix ships when each
  micro rebuilds.

**B. The diff view is its own micro, embedded with slots.**
- One diff micro serves every app, and its updates ship independently.
- Needs slots in the embed contract: the host micro projects content into rows the diff micro
  places. The contract has deferred slots (props-only first), and slots are the expensive part:
  ordering, focus and measurement across two micros' surfaces.

A doesn't block B. The crate in A is exactly what B would wrap later.

**What crosses the boundary (in both cases):**
- In:
  - **Base and head text, the first-class input.** intxt keeps each touched file as an Automerge
    Text doc and pins a revision as a set of heads, so its micro has texts, not patches. The diff
    crate diffs them itself (`parseDiffFromFile` and the `similar`-backed diff module, ported in
    S3). A parsed patch stays a secondary input.
  - Layout (unified or split) and theme.
  - Annotations already resolved to (file, side, line range), each with a stable key and a state
    (current or outdated). A range may span hunks and include unchanged context lines. A
    whole-file annotation has no range and renders as a file-header row.
  - The selected range.
  - **Ranges new since this reader last reviewed**, already computed by the intxt micro, so the
    view can mark them. Each reader has one datum pinning what they last reviewed, and it
    survives rebases (like Jane Street Iron's per-reviewer brain).
- Out: range selected, annotation row toggled, context expanded, which rows are visible. Read
  state built from the visible rows is per reader, one datum slot each and never a shared counter,
  but that's the intxt micro's business; the diff view only reports what's on screen. Those
  reports feed the per-reader "last reviewed" pin.
- Agents read and write an intxt as plain datums, with no feedback API of their own, so an
  agent's annotation reaches the view exactly like a human's.
- The diff view never sees the anchor model. The intxt micro resolves anchors to line ranges
  (re-attaching across revisions, J6) and hands over the result. That keeps the anchor model
  wholly the intext lane's.

## Virtualizer host over standard Dioxus

Operator direction (2026-11-10): the browser and webview backend is the primary deployment. We
don't design around Blitz's gaps for now, and we never assume JS (no eval, no JS interop). The
diff view is a guest library (boundary A), so its virtualizer runs on standard Dioxus events and
`MountedData`.

**What W1/W2 measured (web, `docs/scratch/pierre-spikes.md` "W1/W2 micro scroll latency" on
`spike/pierre-scroll`):**
- The guest runs on the page's main thread (wcmp over the page's `WebAssembly`, no worker), and
  event dispatch into it is synchronous: the host listener calls the guest, which handles,
  renders, flushes and applies before returning. The wire's own overhead is about 0.2 ms per
  event (p95 0.4).
- In all 2000 response events, the guest's DOM writes landed in the same frame as the scroll,
  before layout and paint: 0 frames late. (Main-thread frames; a compositor-driven wheel fling
  was not proven.)
- Standard Dioxus already crosses the wire on web: `onresize` delivers `ResizeData`, and
  `MountedData::get_scroll_offset` and `scroll` work as element commands.
- A thread row growing above the viewport (20/80/240 px) is corrected with **no visible jump**
  when the guest reads the scroll offset and then scrolls by the delta, from the resize event or
  from its own height model. Correcting from the last known scroll top instead leaves 50 px
  (one event's travel at 3000 px/s), worse than nothing for small growth.
- The browser's own scroll anchoring also gives no jump, but only if the full-height spacer is a
  sibling of the rendered window rather than its parent. A height change on an ancestor of the
  anchor node suppresses Chromium's anchoring.
- The real cost is the render, not the wire: a 50-row commit with Pierre's window is about
  4400 mutations and 10.9 ms median (p95 13 to 14) of a 16.7 ms frame.

**Consequences:**
- Pierre's virtualization can run inside a micro on the primary backend with no new wire
  features. The host adapter keeps a layout cache fed by `onscroll`, `onresize` and `onvisible`,
  and does scroll corrections as read-then-scroll command pairs.
- The S10 browser layer (`pierre_diffs_dioxus`, over web-sys) is replaced by that adapter,
  because web-sys works only in a browser page and not in a micro.
- Mutations per row (about 88) are the first performance target.

**Host contracts worth making explicit (Clay, ours):**
1. Element commands issued from an event or resize handler run in the same task, before paint.
   It held in W1/W2 by accident of the executor.
2. A per-row identity attribute: the surface allows only `data-testid`, `data-state` and
   `data-value` today.
3. `overflow-anchor` has to survive closing raw `style` (the no-egress work).
4. Possibly a coalesced "sizes changed" event, since each `resize` listener is one guest
   dispatch per row.

Blitz lacks `onresize` and `onvisible` (dioxus-native panics with `unimplemented!`;
DioxusLabs/blitz#1197). That's for later, with upstream.

## What each journey needs from Clay

| Need | Journeys | Status | Owner |
|---|---|---|---|
| Shell list-and-details, panels contributed by a micro | J1–J3 | built (#80) | claytxt |
| Phone composition | all | built (#81) | claytxt |
| Per-mount instances (two diffs open at once) | J3 | built, not yet a PR (`feat/clay-per-mount`) | claytxt |
| Window width, back | J2–J4 | today's `HostInputs`; C1 parked for design | claytxt |
| Preferences remembered (default diff layout, weights) | all | gap list pending, for the C1 design session | claytxt |
| Diff logic: parse, layout math, hunks, highlight records | J3 | ported (S3–S7) | claytxt |
| Diff components: virtualized file and diff views | J3 | pending (S10–S12); row structure decided; window matches Pierre (about 200 rows, 50 per commit; X5) | claytxt |
| Syntax highlighting in wasm under a fuel budget | J3 | H1, last step open | claytxt |
| Range selection on touch (gutter tap, drag, extend) | J5 | not started; needs Android input checks | claytxt |
| Bottom sheet with the keyboard up | J4, J5 | ADR 032 §6 sheets; keyboard behaviour on Android unverified | claytxt |
| Text input for the composer | J4, J5 | Dioxus input; Android IME unverified | claytxt |
| Open `data-*` attribute names on the surface (Pierre's port emits about 60, e.g. `data-line`, `data-line-index`, `data-line-annotation`; the surface allows 3). Dioxus attribute names are `&'static str`, so arbitrary names need a bounded interner. | J3, J5 | design needed | claytxt |
| Scoped stylesheets for a micro: Pierre's look is a large stylesheet with attribute selectors and CSS variables; the surface has only inline style properties. Must be sanitized for egress (no `url()`, fonts or imports without a capability). | J3 | design needed | claytxt |
| Datums (intext's model, see below) | all | intext lane | intext |
| Anchor model and re-attaching on a new revision | J3, J6 | intext lane | intext |
| Live sync between devices | J6, the live loop | node lane | node |

## How the screens map onto intext's model

This summarises ADR 036 (`docs/adrs/036-intxt-review-loop-datums.md` on `feat/intxt`, p3N-3,
pending operator review), which is the source of truth. References use the `dtxt:` scheme
(ADR 035), which replaces db+datum references.

- **Change** = one intxt, one drawer: a descriptor, a history doc, one file doc per touched
  path, session docs and a notes doc.
- **File** = `file/main`, an Automerge Text; every observed state is appended as line splices.
- **Commit** = `gitCommit/<sha>`, pinning each changed file's base and head states as dtxt URLs
  with heads. That pair is the diff view's base and head text.
- **Revision** = a gitRef position; `refMove/<uuid>` lists the revisions.
- **Annotation** = `note/<uuid>`. Its targets are dtxt refs with a `[cursor-cursor]` fragment
  for a text range, plus a quote as fallback. A **reply** is a note with `inReplyTo`. A
  **thread** is a root note plus its replies; `thread/<root>` holds `{resolved}`. The micro
  resolves cursors to line ranges before the diff view sees them.
- **Seen state** = `seen/<actor>`, per reader: the notes-doc heads plus the ref target they
  last reviewed, for "new since I last looked" across rebases.
- **Structure** = `structure/<uuid>`: tasks, sections and guide order (the change overview's
  ordering, J2).
- **Verdict** (J7): not in the alpha (D18).

## Work order for the claytxt lane (Sonnet lanes, at most two building at once)

1. **Now:** X5 (running). Open the per-mount PR stacked on #80.
2. **Preferences gap list** for the C1 design session (read-only: what the shell persists today
   against ADR 032 §4).
3. **Diff components S10–S12**: virtualized unified and split views, built as a crate
   that doesn't assume micro execution, with a render callback for injected rows (option A).
4. **A mock review micro:** J1–J4 over mock collections, built on the diff crate. It's a stand-in
   the intext lane replaces with the real intxt micro, so it stays deliberately thin.
5. **Touch range selection, the composer sheet, and Android IME checks** (J5).
7. **H1, the last step:** highlighting in the guest under a fuel budget.

## Questions for the operator

1. **Mock review micro (step 4):** should claytxt build it, or should the intext lane start from
   the diff crate directly? (The intext lane prefers a thin mock that they replace.)
