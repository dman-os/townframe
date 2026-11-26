# clay_dioxus_kit

The shared Dioxus component library for Clay compositions (claytxt, the agentic IDE). It links
into native hosts, the browser build and Wasm guest micros alike, so it depends on Dioxus (default
features off) and the design-token vocabulary only: no backend, no renderer, no `clay_*` crate.
Design: [ADR 032 section 7](../../docs/adrs/032-shell-micro-and-component-kit.md); vocabulary:
[FDR 006](../../docs/fdrs/006-shell-grammar.md); styling:
[the styling contract](../../docs/design-language/styling-contract.md). Every later component
copies what is already here, so the rules below are binding; a change to a rule is a change to
this file.

Every component is a headless part and a stylesheet, and follows `KIT-200` to `KIT-206`: the Rust
renders structure, ARIA and behaviour, and `styles/kit/<name>.css` is the whole of its look.

| Component | Headless part | Skin | Specimen |
|---|---|---|---|
| `Button` (variants, sizes, toggle) | `headless::button` | `styles/kit/button.css` | `claytxt_showcase/specimens/button.rs` |
| `Tabs`, `TabPanel` (ARIA tabs, roving focus) | `headless::tabs` | `styles/kit/tabs.css` | `specimens/tabs.rs` |
| `Card`, `CardHeader`, `CardContent`, `CardFooter` | `headless::card` | `styles/kit/card.css` | `specimens/card.rs` |
| `Separator` | `headless::separator` | `styles/kit/separator.css` | `specimens/separator.rs` |
| `Alert` (info, success, warning, danger) | `headless::alert` | `styles/kit/alert.css` | `specimens/alert.rs` |
| `Chip` (static, toggle, removable) | `headless::chip` | `styles/kit/chip.css` | `specimens/chip.rs` |
| `ScrubStrip` (Niagara-style index strip) | `headless::scrub_strip` | `styles/kit/scrub_strip.css` | `specimens/scrub_strip.rs` |
| `Sections` (resizable split) | `headless::sections` | `styles/kit/sections.css` | `specimens/sections.rs` |
| `ListDetails` (list/details scaffold) | `headless::list_details` | `styles/kit/list_details.css` | `specimens/list_details.rs` |
| `Sheet` (bottom/side sheet) | `headless::sheet` | `styles/kit/sheet.css` | `specimens/sheet.rs` |
| `NavButton` (back / open sidebar) | `headless::nav_button` | `styles/kit/nav_button.css` | `specimens/nav_button.rs` |
| `PanelPage` (app bar pinned over a scrolling body) | `headless::panel_page` | `styles/kit/panel_page.css` | `specimens/panel_page.rs` |
| `AppBar` (leading slot, title, trailing actions) | `headless::app_bar` | `styles/kit/app_bar.css` | `specimens/app_bar.rs` |
| `Drawer` (left drawer of a compact host) | `headless::drawer` | `styles/kit/drawer.css` | `specimens/drawer.rs` |
| `ListRow` (openable, highlighted-when-open list row) | `headless::list_row` | `styles/kit/list_row.css` | `specimens/list_row.rs` |

## Conventions

Rules are numbered so a review can cite them. "Checked" says what fails when a rule is broken;
the rest are enforced by review.

### Structure

- **KIT-001** The crate depends on `dioxus` with `default-features = false` and nothing else at
  runtime, and builds for native, `wasm32-unknown-unknown` and `wasm32-wasip3`. The one optional
  addition is the `host` feature (`FoundationStyles`, `ThemeRoot`: the generated stylesheet
  bundle linked as an asset), which turns on `dioxus/asset` and `dioxus/document` for a host that
  has a document and an asset pipeline; a wasm guest never enables it. Checked:
  `guardrails.toml` `[builds.*]`, `cargo x guardrails check-matrix --package clay_dioxus_kit
  --target <t>`.
- **KIT-002** A component is a public module `headless/<name>.rs` (reached as
  `clay_dioxus_kit::headless::<name>`, its component re-exported at the crate root) and a
  stylesheet `styles/kit/<name>.css`. There is no styled Rust part: nothing visual lives in Rust.
  Module item budgets (40) are the pressure against piling components into one file. Checked:
  `cargo x guardrails lint`, `conventions::a_component_is_a_public_headless_module_and_a_skin`.
- **KIT-003** Types a caller needs to drive a component (`Split`, `NavButtonMode`, `WidthClass`,
  ...) are re-exported at the root, so callers never import from `headless` for them.

### Behaviour

- **KIT-010** The headless part owns state, ARIA, events and nothing visual: roles, accessible
  names, `data-state` and the other vocabulary of `KIT-201`, listeners, and the
  controlled/uncontrolled contract.
### The styling contract

The contract itself is `docs/design-language/styling-contract.md`; these are its rules as the kit
enforces them. They apply to every component.

- **KIT-200** A component is its headless part and `styles/kit/<name>.css`, and nothing
  else visual: no `*Skin` struct, no styled Rust part, no class literal but its root. The only
  inline style is a custom property (`style: "--ct-<component>-<what>: {x}px"`) that the
  stylesheet reads; an inline declaration of any other property is refused at run time by the
  test harness (a guest's wire would admit it, so the harness is the check) and by
  `conventions::contract_parts_set_only_custom_properties_inline`.
  Variants are explicit enum props (`NavButtonMode`, `ButtonVariant`, `WidthClass`); a component
  takes no `class` or `style` pass-through (COMPOSE-003). Geometry the host can only classify is
  px (FOUND-008) and reaches CSS through a custom property or a data attribute, never a number in
  Rust that is a layout decision.
- **KIT-201** The vocabulary a stylesheet keys on: a `ct-<name>` class on the root (kebab case),
  `part="<component>-<part>"` on internals (the component is the root class without `ct-`; `part` is what `::part()` selects by and its namespace is flat per shadow root, ADR 059), `data-state` for a part's one enumerated state, a bare
  `data-<name>` for a boolean state (`data-disabled`), `data-orientation`, and the axes the caller
  picks: `data-variant`, `data-size`, `data-status`. Native pseudo-classes (`:hover`,
  `:focus-visible`, `:disabled`) are used where one exists. A module's docs list its parts,
  states and axes, and its tests assert them (`Page::by_class`, `Page::parts`). Adding a name is
  compatible; renaming or removing one is a breaking change to every theme.
- **KIT-202** The skin is `styles/kit/<name>.css`, imported with `layer(kit)` by
  `tailwind.css` and so part of the generated bundle (`assets/tailwind.css`, regenerated with
  `deno run --allow-all x/check-kit-bundle.ts --write`, which also regenerates the token CSS; the
  same script without `--write` is the `kit-bundle` pre-commit hook and fails on any difference).
  Rules are keyed on the root class, `part` and `data-*`, read tokens as `--ct-*`, and use no
  `!important` (layers order the cascade), no `@apply` and no `url()`. Checked:
  `conventions::contract_skins_are_stylesheets_in_the_kit_layer`.
- **KIT-203** Motion and focus are the skin's: a `transition` or `animation` is declared in the
  stylesheet and removed under `prefers-reduced-motion: reduce` (INTERACTION-007); a focusable
  part has a `:focus-visible` outline in the `focus` role (INTERACTION-002); a pressable is 48 px
  on a coarse pointer (`KIT-106`). Rust never runs a timer to animate. Checked for the
  reduced-motion guard; the rest by review and the specimen.
- **KIT-204** A component has a specimen page, `claytxt_showcase/specimens/<name>.rs`, that shows
  every variant and state it has. One that is laid out against its host's width (`AppBar`,
  `Drawer`, `ListDetails`, `Sections`, `Sheet`) shows a compact and an expanded frame: the kit never
  measures, so a frame is the host, and its width is both its CSS width and the `WidthClass` /
  extent it hands the component. The showcase is where a new component is reviewed. Checked:
  `conventions::every_component_has_a_specimen`.
- **KIT-205** Behaviour tests state the vocabulary: root class, parts in order, each axis value
  and state (`data-state`, ARIA), the keyboard map and, where focus moves, that it moved (the
  harness's `mount` backs an element that records `set_focus`). Invariant violations in props
  (`KIT-023`) live in a pure `validate` function so a test can see them panic: Dioxus catches a
  panic inside a component, so a bare `assert!` there is only logged.
- **KIT-206** The kit is structure and a quiet default; the app's look is a sheet (ADR 057). A
  skin carries layout, states, focus and legibility, and reads everything two reasonable themes
  would choose differently (a radius, a border, a shadow, a fill) from a component token,
  `--ct-<component>-radius|border|shadow|fill`, never from a literal. `styles/tokens.css` is the
  token list with its defaults; `styles/identity/daytxt.css`, imported with `layer(identity)`
  between `kit` and `user`, sets them. Checked:
  `conventions::component_tokens_are_listed_and_the_identity_layer_sits_between_kit_and_user`.

### Controlled state and callbacks

- **KIT-020** Anything a parent could want to own is a prop plus a callback: the component never
  keeps it (`Split` in, `on_resize` out). Only transient interaction state is local (a drag in
  flight).
- **KIT-021** Callbacks are `EventHandler<T>` carrying a domain value (new weights, a row key),
  never a raw DOM event, and a gesture reports once, when it completes, not on every move.
  `onclick` on a leaf button (`NavButton`) is the one name taken from the DOM: it is literally a
  press and carries `()`.
- **KIT-022** No component reads global or application state. The one context allowed is an
  action channel a scaffold provides to what it renders (`use_list_details`), so a row deep in a
  list can ask to open itself without prop drilling; it holds callbacks that track the latest
  props (`use_callback`), never values. `ListDetails` also carries which row is open
  (`current`, owned by the caller like everything else) so a `ListRow` can mark itself with
  `aria-current` without the list keeping a second copy of the selection. This context exists
  only inside one Dioxus tree: a list in a Wasm guest cannot use it (see `headless/list_row.rs`).
- **KIT-023** A prop that breaks an invariant (weights and panes of different lengths, a
  non-positive weight or extent, a NaN width) panics. It is a programming error, not an input.

### Geometry

- **KIT-040** Components never measure the DOM. Blitz panics on rect reads inside handlers and a
  guest has no DOM, so the host classifies its width into `WidthClass` (breakpoints live in
  `width.rs` and nowhere else) and passes it down, and passes the px length of a split axis
  (`Split::extent`) when pointer travel must become weights.
- **KIT-041** A drag reads `page_coordinates()` (the event target changes mid-gesture; page space
  does not), assumes no pointer capture (it mounts a window-sized overlay for the gesture's
  duration and ends the gesture when the pointer leaves the window), and, because touch pointers
  are captured to the element that received `pointerdown`, puts the same move/release handlers
  on that element too.
- **KIT-042** Size limits and snap points are logical px props, not tokens: they describe a
  renderer or layout boundary. Rendering them as `rem` waits on FOUND-016 (the native root basis).
  (Surface sizes, by contrast, are spacing steps: `KIT-104`, `KIT-105`.)

### Accessibility

- **KIT-050** Every interactive part has a role and an accessible name. Names that depend on a
  mode come from one function (`NavButtonMode::label`), so every mount reads the same.
- **KIT-051** Decorative graphics are `aria-hidden`. A covered pane (the list under compact
  details) is `aria-hidden` too.
- **KIT-052** Activation uses native `<button>`s. A widget that needs `tabindex` is gated on
  `Renderer::takes_focus` (set through `Split::renderer`); browser engines opt in and the web
  build carries the keyboard tests. Modal panels (`Sheet`, `Drawer`) are the one exception: `tabindex="-1"` and focus moved in on
  mount (`headless::focus_on_mount`, which faults if the renderer cannot focus), so Escape works;
  the focus trap and restore are not built. (The Blitz
  `tabindex` panic that motivated the gate is fixed in the pinned dioxus fork; K7 ledger notes.)
  Focus is neither trapped nor restored anywhere yet; Blitz has `set_focus` and Tab traversal but
  no `inert` and no focus events for programmatic focus (K7 ledger notes); `Sheet` is not
  web-shippable until it is.
- **KIT-053** A headless part uses only the tags, attributes and style properties a Wasm guest's
  tree may name (`clay_surface::render`'s vocabulary). Checked: the test harness asks `clay_surface`'s own admission (a dev-dependency)
  and panics on anything else. Inline `style` strings are passed whole and are not checked
  property by property.

### Adaptive layout, layering and motion

Rules `KIT-100` to `KIT-109` close the design-language gaps found while building the first
components. The values live in the foundation tokens (`semantic.tokens.json`: `layer.*`,
`motion.*`; the colour roles, including the `scrim`, derive from the palette) and the Tailwind spacing scale; the
kit names them and never restates a number. Each rule is checked by a harness test in the module
named after it.

- **KIT-100** Three width classes, by logical px width: `Compact` below 600, `Medium` from 600,
  `Expanded` from 840 (the Material window-size classes). The breakpoints are in `width.rs` and
  nowhere else (`KIT-040`). Reason: one fact, classified once by the host, keeps every component's
  arrangement consistent with every other's; three classes because phones, small tablets and
  desktop windows want different sheet sizes while two panels side by side want only one rule
  (`WidthClass::fits_side_by_side`, true from `Medium`).
- **KIT-101** What changes at each class. Compact is the only class that stacks or overlays;
  Medium and Expanded differ only in how big a surface is. Checked per component.

  | Component | Compact (< 600) | Medium (600 to 839) | Expanded (>= 840) |
  |---|---|---|---|
  | `ListDetails` | one pane; details cover the list | list and details side by side | same |
  | `Drawer` | left drawer over a scrim while `open` | never shown: the sidebar is a panel beside the content | same |
  | `Sheet` | bottom sheet, 5/6 of the height | side sheet, 24rem | side sheet, 32rem |
  | `AppBar` | 56 px bar | same | same |

  The drawer takes the `width` and drops itself on a wider host, rather than leaving every owner
  to repeat "drawer only when compact": a window that grows while the drawer is open then swaps it
  for the side panel without the owner doing anything.
- **KIT-102** One z-order scale, the `layer.*` tokens (`semantic.tokens.json`, emitted as
  `--ct-layer-*`): content, raised, app bar, drawer, sheet, toast, gesture, lowest first. A skin
  names a layer as `z-index: var(--ct-layer-x)` and never a number; a headless part writes no
  `z-index` at all (it writes no layout). A pane is a `content` stacking context so an app bar
  inside it cannot climb over the resize handle (`raised`) overlapping its edge. Reason: the
  previous constants (10, 20, 40, 50) were each local choices, and an app bar needing a layer of
  its own would have collided with them. Checked:
  `conventions::skins_stack_on_the_layer_scale_and_never_name_a_z_index_number`.
- **KIT-103** The scrim. An overlay (`Drawer` while open, every `Sheet`) renders one: a button
  named `Dismiss <name>` (`part="sheet-scrim"` or `part="drawer-scrim"`) filling the overlay, under the panel by tree order
  (no `z-index` of its own). It is always shown, dims with the `scrim` colour role (a fixed 32%
  black, so it darkens a light and a dark theme alike, where `text` at low opacity would lighten a
  dark one), and a press on it calls `on_dismiss`, the same as Escape. Reason: dismissal by tapping
  outside is the one path every renderer delivers (Escape needs a focused element; Blitz can focus
  one via `set_focus`, but nothing focuses a freshly opened sheet yet). The consequence for
  sizing: no panel covers its overlay (`KIT-104`), or the scrim could not be reached. The scrim is
  one rule for both overlays, in `styles/kit/overlay.css`.
- **KIT-104** Sheet size follows the width class (`KIT-101`): a bottom sheet is 5/6 of the host
  height and the full width; a side sheet is 24rem (`Medium`) or 32rem (`Expanded`), at most 5/6
  of the width. Reason: a phone sheet is a task, so it takes most of the screen, but leaves the
  scrim strip to tap; a wide window's side sheet is a complement to the content, so it keeps a
  fixed reading width instead of growing with the window.
- **KIT-105** App bar metrics: 56 px (3.5rem) tall and fixed; a 48 px nav button 8 px from the
  leading edge; the title a single `h2` line (ellipsis when it does not fit, the bar never grows)
  8 px after the leading slot, or, with no leading slot, 16 px from the edge so it lines up with
  page content (`data-state="bare"`); trailing actions are 48 px targets (`KIT-106`) at the end.
  The bar is on `Layer::AppBar`. Reason: a fixed height keeps content from jumping between panels
  and a title that wrapped would push it by an unpredictable amount.
- **KIT-106** Every pressable is at least 48 logical px on both axes (Material's touch minimum):
  `NavButton` and a `ListRow` are `--ct-dimension-target-coarse`, a scrim is the whole overlay. The one
  exception is the `Sections` resize handle, 24 px wide (the WCAG 2.5.8 minimum): it overlaps the
  pane edges on both sides, and 48 px would put a pane's own scrollbar and first controls under
  it. Sizes are spacing steps (`rem`), so they equal 48 px only while the root font size is 16 px
  (FOUND-016 is open for native).
- **KIT-107** The open row of a list is `ListRow`'s `aria-current="true"` (a button cannot be
  `aria-selected`) plus `data-state="current"`, painted with the `selected` / `on-selected`
  colour roles, which alias the accent container today and which a theme may change. Exactly one
  row of a scaffold is current, because the scaffold owner supplies the one key (`KIT-022`). The
  wire vocabulary carries `aria-current` and `data-state`, so a guest's row can say the same.
- **KIT-108** Motion. Overlays enter, they do not exit: the drawer slides in from the leading
  edge, a bottom sheet rises, a side sheet slides in from the trailing edge, and every scrim fades
  in (the `ct-overlay-*` keyframes in `styles/kit/overlay.css` and the `animation` of each panel in
  `drawer.css` and `sheet.css`; durations and easing are the `motion.*` tokens: 250 ms for
  panels, 150 ms for the scrim, one decelerate curve). Every animation is removed under
  `prefers-reduced-motion: reduce` (INTERACTION-007). Reason for no exit: an exit keeps the closed
  surface mounted until its animation ends, which needs `animationend`, and neither Blitz nor the
  guest wire delivers it; a timer standing in for it is forbidden (`KIT-071`). A dismissed surface
  disappears at once. Blitz does not evaluate `prefers-reduced-motion`, so natively the entry
  animation always plays. Checked: `conventions::contract_skins_are_stylesheets_in_the_kit_layer`
  pins the reduced-motion guard; whether Blitz renders the keyframes is a screenshot matter (the
  ledger's K1 notes).
- **KIT-109** Safe areas never reach the kit. A host pads its whole frame by the system insets
  (`daytxt_core::Application` does) and hands the shell the size that remains, so every
  surface, including an overlay and its scrim, lies inside the safe rectangle and the system bars
  show the window behind. Reason: a Wasm guest mounts `AppBar` and `ListRow` in its own tree, with
  no context to read an inset from, so a per-component inset prop would work only for the shell's
  own panels and leave guests under the status bar. The cost: a scrim does not extend under the
  status bar. Letting overlays bleed to the screen edge needs a way to hand insets to guests and is
  not designed yet.

### Tests and specimens

- **KIT-070** Behaviour tests live in the component's module and run through
  `src/testing.rs`: a real `VirtualDom` renders into a small retained DOM that implements
  `WriteMutations`, events dispatch through the runtime, and elements are found by ARIA role and
  accessible name (`Page::by_role`, which skips `aria-hidden` subtrees). Rejected: Dioxus SSR
  (renders markup, cannot dispatch an event, and adds a crate to every target); driving a
  renderer (`dioxus-native-dom` pulls Blitz, which cannot take input in a test, and the kit may
  not depend on a renderer); Playwright for everything (the right place for cross-browser
  keyboard and focus flows, too slow and too coarse for per-component behaviour). Pure logic
  (`Split::dragged`, `WidthClass::from_px`) gets plain unit tests beside it. A test that cannot
  find its target by role and name has found a missing role or name: fix the component.
- **KIT-071** Tests are deterministic: no sleeps, no timeouts, no polling. A gesture is a
  sequence of dispatched events. Tests that need to promote the harness to other crates turn
  `testing` into a `testing` cargo feature then, not earlier. The one cross-crate harness that
  exists is `dioxus_storm` (a dev-dependency, no retained DOM and no wire vocabulary check):
  each headless module ends with a `storm` test that feeds every listener every event kind in
  seeded random order under a real owner, with animation timers on a paused clock, so it is
  still deterministic and sleep-free. A seed that finds a panic stays as a regression case.
- **KIT-072** Every component has a headless test module.

### Renderer limits

- **KIT-090** A limit of Blitz, the browser or the guest wire that shaped a component is written
  in that component's module docs under `# Renderer limits`, with the workaround, so callers
  never rediscover it. See `headless/sections.rs` (no measuring, no pointer capture, no
  `tabindex`, page vs element coordinates) and `headless/sheet.rs` (focus).

## Design-language gaps

- No **directionality**: "leading" is the left edge; there is no right-to-left layout.
- The `motion.*` and `layer.*` tokens are consumed by the kit but `INTERACTION-004` asks for the
  reduced-motion rule first; `INTERACTION-007` is that rule, added with the tokens and awaiting
  the design owner's approval.
- No **icon button**: trailing app-bar actions are the caller's own buttons, held to `KIT-106` by
  review only.

## Open work

- Keyboard and focus tests belong in the browser build (Playwright): arrow-key resizing, Escape,
  and the missing focus trap/restore for `Sheet` (KIT-052).
- Renderer capability facts (can this renderer take `tabindex`?) must come from the host; a guest
  micro cannot know which renderer mounts it. The shell has to decide how `Split::renderer`
  reaches guests.
- The tile list lands with home (ADR 032 section 7).

## Mechanical enforcement

What fails, and where, when a rule is broken. `src/conventions.rs` reads this crate's sources as
text (comments stripped, test modules excluded where the rule is about production code) and runs
under `cargo nextest run -p clay_dioxus_kit`, so it gates CI with the rest of the tests; its
component list is derived from `headless/`, so a new component needs no edit there. Rules not
listed are review-only: KIT-003, 020 to 023, 041, 042, 050, 051 and 205. KIT-053 and
KIT-070 are enforced by the harness itself (`src/testing.rs`).

| Rule | Enforced by |
|---|---|
| KIT-001 | `guardrails.toml` (`allowed_dependencies = []` refuses any workspace crate, so no micro, host or renderer crate; `[builds.*]` compiles all three targets in `cargo x guardrails check-matrix`); `conventions::the_only_runtime_dependency_is_dioxus_without_default_features` refuses any other dependency |
| KIT-002 | `cargo x guardrails lint` (module item budgets); `conventions::a_component_is_a_public_headless_module_and_a_skin` (the headless module is declared and re-exported, its stylesheet exists, no styled Rust part, tests, README table row) |
| KIT-010 | `conventions::headless_parts_hold_no_styling` (no class literal but the root's, no utility strings, no skin in `headless/`) |
| KIT-102 | `conventions::skins_stack_on_the_layer_scale_and_never_name_a_z_index_number` |
| KIT-040 | `conventions::breakpoints_live_in_width_rs_and_nothing_measures_the_dom` (no `600.0`/`840.0` outside `width.rs`; no `get_client_rect`, `onmounted`, `use_window`, `web_sys`, ...) |
| KIT-052 | `conventions::tabindex_is_only_the_renderer_focus_gate` (`tabindex` only in `headless/sections.rs`, gated on `.takes_focus()`, and `-1` on the modal panels; focus moves only in `headless.rs`) |
| KIT-053 | the harness panics on a tag, attribute or style property outside the guest vocabulary |
| KIT-071 | `conventions::tests_neither_sleep_nor_wait` (no sleeps, timers, clocks, threads, async runtimes) |
| KIT-072 | `conventions::a_component_is_a_public_headless_module_and_a_skin` (every `headless/<name>.rs` has a `#[test]`) |
| KIT-200 | `conventions::contract_parts_set_only_custom_properties_inline`; the harness (`src/testing.rs`) panics on any other inline style, per-property or raw; `a_component_is_a_public_headless_module_and_a_skin` (no styled Rust part); `headless_parts_hold_no_styling` (only the root class); `components_take_no_class_or_style_pass_through` |
| KIT-201 | the component's own tests (`Page::by_class`, `Page::parts`); `contract_skins_are_stylesheets_in_the_kit_layer` (the sheet styles its root class) |
| KIT-202, KIT-203 | `conventions::contract_skins_are_stylesheets_in_the_kit_layer` (layer import, bundle in sync, no `!important`/`@apply`/`url(`, reduced-motion guard) |
| KIT-204 | `conventions::every_component_has_a_specimen` |
| KIT-206 | `conventions::component_tokens_are_listed_and_the_identity_layer_sits_between_kit_and_user` |
| KIT-090 | `conventions::a_module_that_mentions_blitz_documents_it_under_renderer_limits` |
| every cited `KIT-nnn` | `conventions::cited_rules_exist_in_the_readme` |

The Blitz limits several rules route around are pinned, as defects, in
`daytxt_desktop/e2e/blitz_limits.rs` (see the dayframe ledger, "K2 notes").
