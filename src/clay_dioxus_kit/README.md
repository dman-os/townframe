# clay_dioxus_kit

The shared Dioxus component library for Clay compositions (claytxt, the agentic IDE). It links
into native hosts, the browser build and Wasm guest micros alike, so it depends on Dioxus (default
features off) and the design-token vocabulary only: no backend, no renderer, no `clay_*` crate.
Design: [ADR 032 section 7](../../docs/adrs/032-shell-micro-and-component-kit.md); vocabulary:
[FDR 006](../../docs/fdrs/006-shell-grammar.md); styling:
[the styling contract](../../docs/design-language/styling-contract.md). Every later component
copies what is already here, so the rules below are binding; a change to a rule is a change to
this file.

Two generations of component live here. The **contract** components (the first seven rows below)
are a headless part and a stylesheet, and follow `KIT-200` to `KIT-205`. The **legacy**
components (the rest) are a headless part and a styled Rust part of Tailwind classes, and follow
`KIT-011` to `KIT-013` until the contract reaches them (it is the next lane's work).

| Component | Headless part | Skin | Specimen |
|---|---|---|---|
| `Button` (variants, sizes, toggle) | `headless::button` | `styles/kit/button.css` | `claytxt_showcase/specimens/button.rs` |
| `Tabs`, `TabPanel` (ARIA tabs, roving focus) | `headless::tabs` | `styles/kit/tabs.css` | `specimens/tabs.rs` |
| `Card`, `CardHeader`, `CardContent`, `CardFooter` | `headless::card` | `styles/kit/card.css` | `specimens/card.rs` |
| `Separator` | `headless::separator` | `styles/kit/separator.css` | `specimens/separator.rs` |
| `Alert` (info, success, warning, danger) | `headless::alert` | `styles/kit/alert.css` | `specimens/alert.rs` |
| `Chip` (static, toggle, removable) | `headless::chip` | `styles/kit/chip.css` | `specimens/chip.rs` |
| `ScrubStrip` (Niagara-style index strip) | `headless::scrub_strip` | `styles/kit/scrub_strip.css` | `specimens/scrub_strip.rs` |
| `Sections` (resizable split) | `headless::sections` | `Sections` (Rust, Tailwind) | `claytxt_showcase/kit.rs` |
| `ListDetails` (list/details scaffold) | `headless::list_details` | `ListDetails` | same |
| `Sheet` (bottom/side sheet) | `headless::sheet` | `Sheet` | same |
| `NavButton` (back / open sidebar) | `headless::nav_button` | `styles/kit/nav_button.css` | `specimens/nav_button.rs` |
| `AppBar` (leading slot, title, trailing actions) | `headless::app_bar` | `AppBar` | same |
| `Drawer` (left drawer of a compact host) | `headless::drawer` | `Drawer` | same |
| `ListRow` (openable, highlighted-when-open list row) | `headless::list_row` | `ListRow` | same |

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
- **KIT-002** A component is two modules: `headless/<name>.rs` (public, reached as
  `clay_dioxus_kit::headless::<name>`) and `<name>.rs` (private; its component is re-exported at
  the crate root). Module item budgets (40) are the pressure against piling components into one
  file. Checked: `cargo x guardrails lint`.
- **KIT-003** The headless and the styled component share the component's name; the skin struct
  is `<Component>Skin`. Types a caller needs to drive the styled component (`Split`,
  `NavButtonMode`, ...) are re-exported at the root so callers never import from both layers.

### Behaviour and skin

- **KIT-010** The headless part owns state, ARIA, events and nothing visual: roles, accessible
  names, `data-state` / `data-value`, listeners, and the controlled/uncontrolled contract.
- **KIT-011** (legacy; contract components follow `KIT-200` to `KIT-203`) The skin is a plain struct of `&'static str` class strings (`SectionsSkin`, ...)
  that the headless part applies to its parts. The styled part is the only code that names
  classes, and it uses only Tailwind utilities that resolve to semantic tokens (`bg-surface`,
  `text-text`, `rounded-xl`, spacing steps); no palette utilities and no arbitrary values
  (COMPOSE-001). The only `[...]` forms allowed are Tailwind's `data-[state=...]` and
  `group-data-[state=...]` variants (the latter styles a part from its group's state, as the
  `Sections` grip does), which are COMPOSE-003's "data attribute" variant mechanism. A second skin is a second set of strings
  over the same headless part.
- **KIT-012** (legacy) Variants are explicit enum props (`NavButtonMode`, `WidthClass`). Styled components
  take no `class` or `style` pass-through (COMPOSE-003).
- **KIT-013** (legacy) Inline `style` in a headless part is limited to what breaks behaviour when it is
  missing: flex weights, an overlay's `position: fixed`, `touch-action`, a handle's hit area.
  Geometry is px (FOUND-008); appearance never is. A `z-index` is never a number here (`KIT-102`).

### The styling contract (contract components)

The contract itself is `docs/design-language/styling-contract.md`; these are its rules as the kit
enforces them. They apply to a component that has a stylesheet in `styles/kit/`.

- **KIT-200** A contract component is its headless part and `styles/kit/<name>.css`, and nothing
  else visual: no `*Skin` struct, no styled Rust part, no class literal but its root. The only
  inline style is a custom property (`style: "--ct-<component>-<what>: {x}px"`) that the
  stylesheet reads; an inline declaration of any other property is refused at run time by the
  test harness (a guest's wire would admit it, so the harness is the check) and by
  `conventions::contract_parts_set_only_custom_properties_inline`.
- **KIT-201** The vocabulary a stylesheet keys on: a `ct-<name>` class on the root (kebab case),
  `data-part="<part>"` on internals, `data-state` for a part's one enumerated state, a bare
  `data-<name>` for a boolean state (`data-disabled`), `data-orientation`, and the axes the caller
  picks: `data-variant`, `data-size`, `data-status`. Native pseudo-classes (`:hover`,
  `:focus-visible`, `:disabled`) are used where one exists. A module's docs list its parts,
  states and axes, and its tests assert them (`Page::by_class`, `Page::parts`). Adding a name is
  compatible; renaming or removing one is a breaking change to every theme.
- **KIT-202** The skin is `styles/kit/<name>.css`, imported with `layer(kit)` by
  `tailwind.css` and so part of the generated bundle (`assets/tailwind.css`, regenerated with
  `deno run --allow-all x/check-kit-bundle.ts --write`, which also regenerates the token CSS; the
  same script without `--write` is the `kit-bundle` pre-commit hook and fails on any difference).
  Rules are keyed on the root class, `data-part` and `data-*`, read tokens as `--ct-*`, and use no
  `!important` (layers order the cascade), no `@apply` and no `url()`. Checked:
  `conventions::contract_skins_are_stylesheets_in_the_kit_layer`.
- **KIT-203** Motion and focus are the skin's: a `transition` or `animation` is declared in the
  stylesheet and removed under `prefers-reduced-motion: reduce` (INTERACTION-007); a focusable
  part has a `:focus-visible` outline in the `focus` role (INTERACTION-002); a pressable is 48 px
  on a coarse pointer (`KIT-106`). Rust never runs a timer to animate. Checked for the
  reduced-motion guard; the rest by review and the specimen.
- **KIT-204** A contract component has a specimen page, `claytxt_showcase/specimens/<name>.rs`,
  that shows every variant and state it has. Checked: `conventions::every_component_has_a_specimen`.
- **KIT-205** Behaviour tests state the vocabulary: root class, parts in order, each axis value
  and state (`data-state`, ARIA), the keyboard map and, where focus moves, that it moved (the
  harness's `mount` backs an element that records `set_focus`). Invariant violations in props
  (`KIT-023`) live in a pure `validate` function so a test can see them panic: Dioxus catches a
  panic inside a component, so a bare `assert!` there is only logged.

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
  tree may name (`clay_surface::render`'s vocabulary). Checked: the test harness mirrors the lists
  and panics on anything else. Inline `style` strings are passed whole and are not checked
  property by property.

### Adaptive layout, layering and motion

Rules `KIT-100` to `KIT-109` close the design-language gaps found while building the first
components. The values live in the foundation tokens (`semantic.tokens.json`: `layer.*`,
`motion.*`, the `scrim`, `selected` and `on-selected` colours) and the Tailwind spacing scale; the
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
- **KIT-102** One z-order scale, `Layer` (`layer.rs`): `Content`, `Raised`, `AppBar`, `Drawer`,
  `Sheet`, `Toast`, `Gesture`, lowest first, mirroring the `layer.*` tokens (a test compares the
  two). A headless part writes `z-index:{Layer::X.css()}`, which is a `var(--ct-layer-x)`; a
  numeric `z-index` in a headless part fails a test. A pane is a `Content` stacking context so an
  app bar inside it cannot climb over the resize handle (`Raised`) overlapping its edge. Reason:
  the previous constants (10, 20, 40, 50) were each local choices, and an app bar needing a layer
  of its own would have collided with them.
- **KIT-103** The scrim. An overlay (`Drawer` while open, every `Sheet`) renders one: a button
  named `Dismiss <name>` filling the overlay, under the panel by tree order (no `z-index` of its
  own). It is always shown, dims with the `scrim` colour role (a fixed 32% black, so it darkens a
  light and a dark theme alike, where `text` at low opacity would lighten a dark one), and a press
  on it calls `on_dismiss`, the same as Escape. Reason: dismissal by tapping outside is the one
  path every renderer delivers (Escape needs a focused element; Blitz can focus one via `set_focus`, but nothing focuses a freshly opened sheet yet). The
  consequence for sizing: no panel covers its overlay (`KIT-104`), or the scrim could not be
  reached. A skin takes the shared `SCRIM` classes and may not restyle them.
- **KIT-104** Sheet size follows the width class (`KIT-101`): a bottom sheet is 5/6 of the host
  height and the full width; a side sheet is 24rem (`Medium`) or 32rem (`Expanded`), at most 5/6
  of the width. Reason: a phone sheet is a task, so it takes most of the screen, but leaves the
  scrim strip to tap; a wide window's side sheet is a complement to the content, so it keeps a
  fixed reading width instead of growing with the window.
- **KIT-105** App bar metrics: 56 px (`h-14`) tall and fixed; a 48 px nav button 8 px from the
  leading edge; the title a single `h2` line (ellipsis when it does not fit, the bar never grows)
  8 px after the leading slot, or, with no leading slot, 16 px from the edge so it lines up with
  page content (`data-state="bare"`); trailing actions are 48 px targets (`KIT-106`) at the end.
  The bar is on `Layer::AppBar`. Reason: a fixed height keeps content from jumping between panels
  and a title that wrapped would push it by an unpredictable amount.
- **KIT-106** Every pressable is at least 48 logical px on both axes (Material's touch minimum):
  `NavButton` is `size-12`, a `ListRow` `min-h-12`, a scrim is the whole overlay. The one
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
  in (`animate-slide-in-*`, `animate-fade-in` in `ui/styles/motion.css`; durations and easing are
  the `motion.*` tokens: 250 ms for panels, 150 ms for the scrim, one decelerate curve). Every
  animation class is paired with `motion-reduce:animate-none` (INTERACTION-007). Reason for no
  exit: an exit keeps the closed surface mounted until its animation ends, which needs
  `animationend`, and neither Blitz nor the guest wire delivers it; a timer standing in for it is
  forbidden (`KIT-071`). A dismissed surface disappears at once. Blitz does not evaluate
  `prefers-reduced-motion`, so natively the entry animation always plays. Checked: skin tests
  pin the classes; whether Blitz renders the keyframes is a screenshot matter (the ledger's K1
  notes).
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
- **KIT-072** Every component has a headless test module; a styled part with logic of its own
  (width to placement) has a skin test.
- **KIT-080** Every component has a specimen in `claytxt_showcase/kit.rs`, at a compact and an
  expanded host width. A specimen is a fixed-size frame standing in for the host: its width is
  both its CSS width and the `WidthClass` / extent it hands the component. The showcase is where
  a new component is reviewed.

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
listed are review-only: KIT-003, 013, 020 to 023, 041, 042, 050, 051 and 205. KIT-053 and
KIT-070 are enforced by the harness itself (`src/testing.rs`).

| Rule | Enforced by |
|---|---|
| KIT-001 | `guardrails.toml` (`allowed_dependencies = []` refuses any workspace crate, so no micro, host or renderer crate; `[builds.*]` compiles all three targets in `cargo x guardrails check-matrix`); `conventions::the_only_runtime_dependency_is_dioxus_without_default_features` refuses any other dependency |
| KIT-002 | `cargo x guardrails lint` (module item budgets); `conventions::a_component_is_a_public_headless_module_and_a_skin` (the headless module is declared; a legacy component's styled module exists, private and re-exported; a contract component is re-exported from `headless`; README table row) |
| KIT-010, KIT-011 | `conventions::headless_parts_hold_no_styling` (no `class: "..."` literal, no utility strings, no import of a styled part in `headless/`) |
| KIT-011 | `conventions::styled_parts_use_semantic_tokens_only` (no palette or `white`/`black` utilities, no `[...]` except the `data-[state=...]` variant and its `group-` form) |
| KIT-012 | `conventions::styled_components_take_no_class_or_style` |
| KIT-040 | `conventions::breakpoints_live_in_width_rs_and_nothing_measures_the_dom` (no `600.0`/`840.0` outside `width.rs`; no `get_client_rect`, `onmounted`, `use_window`, `web_sys`, ...) |
| KIT-052 | `conventions::tabindex_is_only_the_renderer_focus_gate` (`tabindex` only in `headless/sections.rs`, gated on `.takes_focus()`, and `-1` on the modal panels; focus moves only in `headless.rs`) |
| KIT-053 | the harness panics on a tag, attribute or style property outside the guest vocabulary |
| KIT-071 | `conventions::tests_neither_sleep_nor_wait` (no sleeps, timers, clocks, threads, async runtimes) |
| KIT-072 | `conventions::a_component_is_a_public_headless_module_and_a_skin` (every `headless/<name>.rs` has a `#[test]`) |
| KIT-080 | `conventions::every_component_has_a_specimen` (the showcase names the legacy component; a framed specimen has a `COMPACT_PX` and an `EXPANDED_PX` frame; `NavButton` and `ListRow` are the two unframed ones) |
| KIT-200 | `conventions::contract_parts_set_only_custom_properties_inline`; the harness (`src/testing.rs`) panics on any other inline style; `a_component_is_a_public_headless_module_and_a_skin` (no styled Rust part beside a stylesheet); `headless_parts_hold_no_styling` (only the root class) |
| KIT-201 | the component's own tests (`Page::by_class`, `Page::parts`); `contract_skins_are_stylesheets_in_the_kit_layer` (the sheet styles its root class) |
| KIT-202, KIT-203 | `conventions::contract_skins_are_stylesheets_in_the_kit_layer` (layer import, bundle in sync, no `!important`/`@apply`/`url(`, reduced-motion guard) |
| KIT-204 | `conventions::every_component_has_a_specimen` |
| KIT-090 | `conventions::a_module_that_mentions_blitz_documents_it_under_renderer_limits` |
| every cited `KIT-nnn` | `conventions::cited_rules_exist_in_the_readme` |

The Blitz limits several rules route around are pinned, as defects, in
`daytxt_desktop/e2e/blitz_limits.rs` (see the dayframe ledger, "K2 notes").
