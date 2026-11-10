# Blitz gap log

Status: living log, started 2026-11-10. Owner: the claytxt lane; every lane appends.

This is the list of Blitz / dioxus-native gaps that have **blocked or bent our work**, kept so we
can burn through them upstream in one focused stretch. Other projects are patching the same gaps
on their own forks (for example the libero-ui wishlist, DioxusLabs/blitz#1197), so a precise list
with evidence is what makes it cheap to coordinate with the maintainers instead of forking.

How to use it:
- Add an entry when a Blitz gap blocks a feature, forces a workaround, or changes a design. Name
  what it cost us, not just what's missing.
- Give evidence: the probe test, the upstream source location with the version you read, or both.
  Version matters: check `cargo tree -i blitz-dom` and read the resolved source, not `../blitz`
  (see the `blitz-dioxus-native-limits` skill for why).
- Link the upstream issue, or write "none found" with the date you searched.
- When a gap closes upstream or in our fork, mark it and keep the entry. The history shows which
  workarounds can go.
- Workaround patterns and API traps live in `.agents/skills/blitz-dioxus-native-limits/SKILL.md`.
  This log tracks what to fix upstream.

The current product direction (operator, 2026-11-10): the browser/webview backend is the primary
deployment, and designs are not constrained by these gaps except "no JS". So an entry here means
"Blitz falls short of the browser", not "we designed around it".

Versions referenced unless stated: `blitz-dom` 0.3.0-beta.4, `dioxus` fork `a3e1e68`
(dioxus-native, dioxus-native-dom).

## Events and observers

| Gap | What it blocked | Evidence | Upstream |
|---|---|---|---|
| `onresize` is unimplemented: dioxus-native `convert_resize_data` is `unimplemented!("... requires support in blitz")`, so using the event panics. | Pierre's virtualizer and `ResizeManager` (row heights, annotation rows) on desktop and phone; any ResizeObserver-style component. | `dioxus-native-dom/src/events.rs:136` | DioxusLabs/blitz#1197 (wishlist, size L) |
| `onvisible` is unimplemented (`convert_visible_data`, same panic). | Pierre's per-file visibility (IntersectionObserver), "what the reader has seen" for intxt's last-reviewed marking. | `dioxus-native-dom/src/events.rs:140` | DioxusLabs/blitz#1197 |
| No scroll anchoring (CSS `overflow-anchor`). | Keeping the reader's place when a thread above the viewport grows. Browsers do it natively; on Blitz the guest must read-then-scroll (W2 in `docs/scratch/pierre-spikes.md`, branch `spike/pierre-scroll`). | Not implemented in blitz-dom; no anchoring code found. | none found (2026-11-10) |
| No pointer capture: each move is re-hit-tested, so a drag that leaves its box stops receiving events. | Drag selection of line ranges (Pierre's InteractionManager), any drag gesture. Workaround: full-window overlay during the gesture. | skill `blitz-dioxus-native-limits`, "No pointer capture" | none found (2026-11-10) |
| `get_client_rect` takes a mutable document borrow, so calling it from `onmounted` or an event handler panics (`RefCell already borrowed`). | Measuring anything at mount or in a handler; forced the width-class-as-prop design (`clay_dioxus_kit/width.rs`). | skill, "Measuring the DOM"; verified 0.8.0-alpha / blitz-dom 0.3.0-beta.2 | none found (2026-11-10) |
| No `transitionend` / `animationend` events. | Sequencing UI after a transition (sheets, panels). | skill, focus section | none found (2026-11-10) |

## CSS layout and rendering

From the Pierre stylesheet probe (`src/dayframe_desktop/e2e/blitz_limits.rs`, branch
`feat/blitz-pierre-css` at c64b8214), which pins each of Pierre's CSS features against Blitz:

| Gap | What it blocked | Evidence | Upstream |
|---|---|---|---|
| `subgrid` is ignored (`stylo_taffy` maps it to "no template"). | Pierre's diff layout, where gutter and content rows share tracks. Drove the "one grid row per line, gutter in its own pane" layout. | `subgrid_rows_and_columns_are_not_shared_with_the_parent_grid`; `stylo_taffy` `wrapper.rs:791`, `convert.rs:811` ("TODO: Implement subgrid and masonry") | none found (2026-11-10) |
| `position: sticky` paints like `static` (taffy leaves sticky offsets to the caller; Blitz applies none). | Pierre's sticky file header and gutter. | `position_sticky_scrolls_away_like_static` | DioxusLabs/blitz#1197 |
| Container queries and `cqi` units are unsupported (`query_container_size` is a stub). | Pierre's container-width rules (skipped whole behind `@supports`). | `container_queries_and_cqi_units_are_unsupported`; `blitz-dom/src/stylo.rs:1214` ("FIXME: Implement container queries") | none found (2026-11-10) |
| `:host` never matches (no shadow DOM; Blitz renders light DOM). | Pierre's custom properties and `color-scheme` live under `:host`; the port has to rescope them. | `host_rules_never_match_in_light_dom` | none found (2026-11-10) |
| `:has()` drops the whole rule. | Any parent-state styling (Pierre uses it). | `has_selector_drops_the_whole_rule` | none found (2026-11-10) |
| `@media (pointer: coarse)` never matches; `pointer` is always `fine`. | Touch-specific styling on the phone. | `media_pointer_is_fine_and_never_coarse` | none found (2026-11-10) |
| `scrollbar-gutter: stable` reserves no space; scrollbars are overlay-only in headless layout. | Pierre's scrollbar gutter measurement; falls back to its fallback variable. | `scrollbar_gutter_stable_reserves_no_space` | none found (2026-11-10) |
| `text-overflow: ellipsis` parses but paints like `clip`. | Truncated file names and labels. | `text_overflow_ellipsis_parses_but_paints_like_clip` | none found (2026-11-10) |
| `word-break: break-word` is ignored (`overflow-wrap` works). | Wrapping long tokens in code. Workaround: `overflow-wrap`. | `white_space_modes_work_and_word_break_break_word_is_ignored` | none found (2026-11-10) |
| `tab-size` is ignored. | Rendering tabs in code at the file's width. | `tab_size_is_ignored` | none found (2026-11-10) |

## Focus and accessibility

| Gap | What it blocked | Evidence | Upstream |
|---|---|---|---|
| `has_changes` is inverted and `changed_nodes` is never cleared, so DOM changes after the first tree never reach AT-SPI. | Inspecting the app through accessibility after anything async (micro activation, state updates); the a11y tree is our agent interface. | `blitz-dom-0.3.0-beta.4/src/document.rs:1028`, `blitz-shell-0.3.0-beta.4/src/window.rs:379` | none found (2026-11-10) |
| Accessibility `ActionRequested` is an empty TODO. | Driving the app through AT-SPI actions; a "click" reports success without effect. | `blitz-shell-0.3.0-beta.4/src/application.rs:87` | none found (2026-11-10) |
| `:focus-visible` and `:focus-within` are hard-coded false; no events for programmatic focus; no `inert`; no positive-`tabindex` order. | Keyboard focus styling and focus containment in sheets and dialogs. | skill, `tabindex` section; `blitz-dom` `stylo.rs:477-478` | none found (2026-11-10) |
| Custom widgets have no accessibility tree. | Using a custom widget for content without losing AT-SPI. | skill, "Custom widgets" | none found (2026-11-10) |

## Closed or patched in our fork

| Gap | Where it was fixed |
|---|---|
| `MountedData::set_focus` panicked when called while the document was borrowed during render. | dioxus fork `a3e1e68` (regression test `task_polled_during_render_can_access_document`). Upstream status unverified. |
| `MountedData::scroll_to` / `scroll` returned `NotSupported`. | Implemented in the pinned dioxus fork `a3e1e68` (`dioxus-native-dom/src/events.rs:240-290`, through `scroll_into_view` and `Document::scroll_to`). Whether that came from upstream or our patch is unverified. The skill's `NotSupported` row is stale. |
