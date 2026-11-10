# intxt review loop, top down (claytxt lane)

Status: draft for the operator's review, 2026-11-10. Owner: the claytxt lane (Clay and the
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
| J7 | Verdict | Approves or requests changes. **Open: is a verdict part of the alpha, or does resolving every thread replace it?** |

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

Back on the phone walks thread → diff → files → inbox through the shell's navigation stack. That
needs C1 step 3 (back as a call to the shell, plus the published "can go back"). Unified versus
split comes from the host's window width (C1 steps 1–2), not from a hidden Dioxus context.

Arrangement and open-context state follow ADR 032 §4 and the `tables.rs` precedent. Section
weights and preferences such as the default diff layout persist and sync. The navigation stack
and open panels are device-local. Host facts such as the window's pixel size are not persisted.

## The boundary between the diff view and intxt

Pierre renders annotations as rows injected between diff lines (`DiffHunksRenderer` hooks,
ported in S6). In intxt, those rows are the threads, and the threads belong to the intxt micro:
its datums, its reply and resolve actions. So the diff view has to host content it doesn't own.
There are two ways to do that:

**A. The diff view is a library the intxt micro compiles in. (Recommended for the alpha.)**
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
- In: the parsed diff, or base and head text; layout (unified or split); theme; annotations
  already resolved to (file, side, line range), each with a stable key and a state (current or
  outdated); the selected range.
- Out: range selected, annotation row toggled, context expanded, which rows are visible (for
  read state).
- The diff view never sees the anchor model. The intxt micro resolves anchors to line ranges
  (re-attaching across revisions, J6) and hands over the result. That keeps the anchor model
  wholly the intext lane's.

## What each journey needs from Clay

| Need | Journeys | Status | Owner |
|---|---|---|---|
| Shell list-and-details, panels contributed by a micro | J1–J3 | built (#80) | claytxt |
| Phone composition | all | built (#81) | claytxt |
| Per-mount instances (two diffs open at once) | J3 | built, not yet a PR (`feat/clay-per-mount`) | claytxt |
| Window width as a host contract | J3 layout | C1 steps 1–2 | claytxt |
| Back as a call, "can go back" | J2–J4 | C1 step 3 | claytxt |
| Package config defaults, one built-ins list | the intxt app itself | C1 steps 4 and 7 | claytxt |
| Diff logic: parse, layout math, hunks, highlight records | J3 | ported (S3–S7) | claytxt |
| Diff components: virtualized file and diff views | J3 | pending (S10–S12); row structure decided, virtualization waits on X5 | claytxt |
| Syntax highlighting in wasm under a fuel budget | J3 | H1, last step open | claytxt |
| Range selection on touch (gutter tap, drag, extend) | J5 | not started; needs Android input checks | claytxt |
| Bottom sheet with the keyboard up | J4, J5 | ADR 032 §6 sheets; keyboard behaviour on Android unverified | claytxt |
| Text input for the composer | J4, J5 | Dioxus input; Android IME unverified | claytxt |
| Datums: change, revision, file, annotation, thread, reply | all | intext lane | intext |
| Anchor model and re-attaching on a new revision | J3, J6 | intext lane | intext |
| Live sync between devices | J6, the live loop | node lane | node |

## Work order for the claytxt lane (Sonnet lanes, at most two building at once)

1. **Now:** X5 (running). Open the per-mount PR stacked on #80.
2. **C1 steps 1–3** once the operator settles where host facts live.
3. **Diff components S10–S12** after X5: virtualized unified and split views, built as a crate
   that doesn't assume micro execution, with a render callback for injected rows (option A).
4. **A mock review micro:** J1–J4 over mock collections, built on the diff crate. It's a stand-in
   the intext lane replaces with the real intxt micro, so it stays deliberately thin.
5. **Touch range selection, the composer sheet, and Android IME checks** (J5).
6. **C1 steps 4 and 7**, before the intxt app composition exists.
7. **H1, the last step:** highlighting in the guest under a fuel budget.

## Questions for the operator

1. **Diff view boundary:** A (a library the intxt micro links) for the alpha, with B later? This
   departs from "heavy components live in micros" for now.
2. **J7:** is a verdict (approve or request changes) part of the alpha?
3. **Mock review micro (step 4):** should claytxt build it, or should the intext lane start from
   the diff crate directly?
