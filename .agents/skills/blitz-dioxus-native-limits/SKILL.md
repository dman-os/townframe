---
name: blitz-dioxus-native-limits
description: Check and work around the upstream limits of Blitz / dioxus-native that constrain what dayframe_dx can build. Use when an app idea depends on pointer capture, DOM measurement, keyboard focus, CSS transforms, custom widgets, scrolling, or accessibility bounds, or when a Blitz/dioxus-native API panics or silently does nothing.
---

# Blitz / dioxus-native limits

`src` runs on a pinned, pre-release Blitz stack. Several ordinary DOM capabilities are
missing, and a few APIs **panic** rather than degrade. Knowing this up front changes design decisions
(a feature can be impossible for reasons unrelated to the app code), so consult this list before
committing to an approach that depends on one of these.

## Verify first — this is a cache, not a spec

Two failure modes have already burned real cycles here, so treat every entry as a *claim to re-check*:

1. **The local checkout is not necessarily the crate being resolved.** A session concluded "Blitz has
   no CSS transforms, hit-testing ignores them" from a stale reading, and then found that the build
   resolved `blitz-dom 0.3.0-beta.2` **from crates.io** while `../blitz` was a different tree. That
   wrong conclusion survived two rounds of design work and changed a recommendation.
2. **Versions move.** `feat/dayframe` patches `dioxus`/`dioxus-native` to local paths but takes
   `blitz-*` from crates.io. A limit recorded here may be fixed, and a workaround may be obsolete.

Before relying on an entry:

```sh
cargo tree -i blitz-dom --workspace        # which blitz version is actually resolved
cargo tree -i dioxus-native --workspace
```

Then read the resolved source, not `../blitz`. When you confirm or refute an entry, **edit this file**.
Record the version you checked, because "verify" without a version is worthless next month.

## The API contract traps

### Measuring the DOM from Dioxus code: don't

`get_client_rect()` internally takes a **mutable** borrow of the document (`doc_mut()` →
`doc.borrow_mut()`), so calling it from `onmounted` or an event handler panics with
`RefCell already borrowed`. The API's own neighbouring comment says to use it from background tasks
("Returns `None` if the document is currently mutably borrowed") but the rect getter does not use
`try_doc()`. A related measurement attempt reported `0x0`, because a node has no resolved layout at
mount.

Consequences and the pattern to use instead:

- **Don't measure.** Express the geometry so it needs no window size. A drag band flush with an edge
  and full height gives distance-from-that-edge and offset-from-that-edge from its own relative
  pointer coordinates — no window dimensions required.
- When a viewport really is needed, **ask the window, not the DOM**: `dioxus_native::use_window()`
  with `surface_size()` / `scale_factor()`, subscribing to `SurfaceResized` and `ScaleFactorChanged`.
  Pass the result **down as a prop**, so shared UI crates stay renderer-agnostic and the native shell
  owns the platform call. (`winit` 0.31 renamed these: `Resized` → `SurfaceResized`,
  `inner_size()` → `surface_size()`.)
- Verified 0.8.0-alpha / `blitz-dom` 0.3.0-beta.2.

### Linux accessibility: enable the backend; semantic actions are not wired; post-mount DOM edits do not update AT-SPI

`blitz-shell 0.3.0-beta.4` disables defaults on `accesskit_xplat`. Accessibility alone does not enable its Unix backend; desktop explicitly enables `accesskit_xplat = "0.2.1"` defaults on Linux. Once enabled, initial AT-SPI registration works (`InitialTreeRequested` builds the first tree), but two upstream defects remain in `blitz-dom 0.3.0-beta.4` / `blitz-shell 0.3.0-beta.4`:

1. `BaseDocument::has_changes(&self)` (`blitz-dom-0.3.0-beta.4/src/document.rs:1028`) returns `self.changed_nodes.is_empty()` (inverted) and `changed_nodes` is never cleared after initial node creation, so `window.poll()` in `blitz-shell-0.3.0-beta.4/src/window.rs:379` (`if inner.has_changes() { self.accessibility.update_tree(&inner); }`) never pushes DOM mutations that land after `InitialTreeRequested` (such as async `use_future` micro activation or state updates) to AT-SPI.
2. `BlitzShellEvent::Accessibility → ActionRequested` (`blitz-shell-0.3.0-beta.4/src/application.rs:87`) is still an empty `// TODO`. An accessibility CLI click can therefore report dispatch success without changing the UI. Do not use that receipt as evidence of an interaction; verify the resulting state.

### `tabindex` panics, so keyboard focus is unavailable

Setting `tabindex: 0` routes Dioxus into `set_focus`, which calls `doc_mut()` and panics the same way
as above. **This is an accessibility regression, not just a limitation**: a widget can carry its
slider/button role and ARIA attributes and still be unreachable by keyboard. If you ship that, flag it
explicitly rather than letting it look intentional. A `<button>` may already be focusable without
`tabindex`, which is worth testing before assuming the whole tree is keyboard-dead.

### No pointer capture — a drag that leaves its box stops receiving events

`handle_pointer_move` hit-tests every move, and only pan/text-select/scrollbar-thumb get special
`DragMode` treatment. So a drag which travels outside the originating node's box silently stops
delivering. This blocks anything drag-like by default.

Workaround, portable to a browser: on `pointerdown`, flip `pointer-events: auto` on a **full-window
overlay** for the duration of the gesture. Because Blitz re-hit-tests each move independently, every
subsequent move lands on the overlay however far the pointer travels. Also add a `pointerleave`-style
terminator so a release outside the app cannot leave the gesture stuck engaged.

### Two coordinate spaces, and the wrong one silently drifts

`element_coordinates()` is relative to the **event target**, and the target can change mid-gesture
(band → overlay) as the pointer crosses an edge. `page_coordinates()` is document-space and
target-independent. Blitz builds both via `to_logical(scale)`, i.e. logical px, matching CSS/viewport
units. Use `page_coordinates()` for anything that spans a gesture.

## Things that are missing vs. things people assume are missing

| Capability | Status |
|---|---|
| `scroll_to` / `scroll` from dioxus-native | **Returns `NotSupported`** in the pinned version, though `blitz-dom` has `Document::scroll_to` / `scroll_node_by` and the scroll offset is already honoured by paint, hit-testing and the wheel path. A ~10-line patch in dioxus-native makes panning O(1) instead of a style-dirtying transform. Check before assuming; if still true, this is the cheap win for any camera/pan work. |
| CSS `transform` / `translate` / `scale` / `rotate` + `transform-origin` | **Supported**, resolved in spec order (`transform-origin × (translate·rotate·scale·transform) × origin⁻¹`), applied as a stack during paint, and hit testing applies the **inverse**, and it creates a stacking context. Layout **size** is unaffected, which is what a camera wants; glyphs are transformed in the scene graph so scaled text stays crisp (but a 15px title at 0.3 zoom is a crisp, illegible hairline — geometry cannot solve "overview"; shed content instead). |
| Cost of a transform-based camera | The transform itself is near-free (~5–8 µs, near-flat over 3.3× tree size). The real cost is **style invalidation**: the camera node is an ancestor of the whole world, so dirtying one inline style restyles everything. Panning by `left`/`top` dirties the same traversal, so the property is irrelevant — only the *frequency* is. An idle camera costs nothing. |
| AccessKit geometry | `blitz-dom` / `blitz-shell` carried **no `Bounds` at all** in the version checked — AccessKit has no geometry, so "a11y bounds are in world coordinates under a transform" is currently a non-issue. This is the kind of entry most likely to invert upstream; re-check it if a11y positioning ever misbehaves. |
| Custom widgets | API exists (`Widget` trait: `connected`/`disconnected`, `attribute_changed`, `can_create_surfaces`, `requires_redraw` for a continuous redraw loop, `handle_event(&UiEvent)`, `intrinsic_sizes`, `paint -> Scene`; `PaintScene::draw_glyphs` can draw text). Two hard constraints: `CustomWidgetAttr` is **write-once** (the boxed widget is `take()`n when the attribute lands, so live state must flow via string attributes or a shared `Rc<RefCell<..>>`), and widgets have **no accessibility tree** (literal TODOs in the trait for `accessibility_tree`). Making the world's content a custom widget therefore throws away the AT-SPI story. Treat it as the escape hatch of last resort, not the default. |

## Design consequences worth remembering

- Prefer **DOM + real elements** (buttons, lists, headings, labels) for anything that should be
  semantically inspectable. The a11y tree *is* the agent interface for this app.
- Prefer **content shedding over geometric scaling** when zooming out: fewer nodes, still-legible
  text, same roles. As a bonus, layout cost falls as you zoom out, which is the opposite of the naive
  argument against a layout-space camera.
- Because there is **no pointer capture and no pointer injection**, anything drag-shaped must be
  verified by the operator. See `.agents/skills/dayframe-native-dev-loop` for the protocol.
- A patch to `blitz`/`dioxus-native` is viable (local `[patch]` precedents exist), but the bar should
  be "this blocks two features", not "this is annoying".

## Related

- `.agents/skills/dayframe-native-dev-loop` — how to build, screenshot and inspect the app.
- `docs/adrs/016-dayframe-dx-architecture.md` — renderer duality (Blitz ambition, wry prod) and the
  rule for when forking upstream is justified.

## Verified against

- `dioxus 0.8.0-alpha.3` (`dioxus-native` local fork `c47a76bd8097`), `blitz-dom 0.3.0-beta.4` and `blitz-shell 0.3.0-beta.4` from crates.io. Recorded 2026-10.
