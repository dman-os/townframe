# clay_dioxus_kit

The shared Dioxus component library for Clay compositions (Dayframe, the agentic IDE). It links
into native hosts, the browser build and Wasm guest micros alike, so it depends on Dioxus (default
features off) and the design-token vocabulary only: no backend, no renderer, no `clay_*` crate.
Design: [ADR 032 section 7](../../docs/adrs/032-shell-micro-and-component-kit.md); vocabulary:
[FDR 006](../../docs/fdrs/006-shell-grammar.md). Every later component copies what is already
here, so the rules below are binding; a change to a rule is a change to this file.

| Component | Headless part | Styled part | Specimen |
|---|---|---|---|
| `Sections` (resizable split) | `headless::sections` | `Sections` | `dayframe_showcase/kit.rs` |
| `ListDetails` (list/details scaffold) | `headless::list_details` | `ListDetails` | same |
| `Sheet` (bottom/side sheet) | `headless::sheet` | `Sheet` | same |
| `NavButton` (back / open sidebar) | `headless::nav_button` | `NavButton` | same |

## Conventions

Rules are numbered so a review can cite them. "Checked" says what fails when a rule is broken;
the rest are enforced by review.

### Structure

- **KIT-001** The crate depends on `dioxus` with `default-features = false` and nothing else at
  runtime, and builds for native, `wasm32-unknown-unknown` and `wasm32-wasip3`. Checked:
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
- **KIT-011** The skin is a plain struct of `&'static str` class strings (`SectionsSkin`, ...)
  that the headless part applies to its parts. The styled part is the only code that names
  classes, and it uses only Tailwind utilities that resolve to semantic tokens (`bg-surface`,
  `text-text`, `rounded-xl`, spacing steps); no palette utilities and no arbitrary values
  (COMPOSE-001). The one `[...]` form allowed is Tailwind's `data-[state=...]` variant, which is
  COMPOSE-003's "data attribute" variant mechanism. A second skin is a second set of strings
  over the same headless part.
- **KIT-012** Variants are explicit enum props (`NavButtonMode`, `WidthClass`). Styled components
  take no `class` or `style` pass-through (COMPOSE-003).
- **KIT-013** Inline `style` in a headless part is limited to what breaks behaviour when it is
  missing: flex weights, an overlay's `position: fixed`, `touch-action`, a handle's hit area.
  Geometry is px (FOUND-008); appearance never is.

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
  props (`use_callback`), never values.
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

### Accessibility

- **KIT-050** Every interactive part has a role and an accessible name. Names that depend on a
  mode come from one function (`NavButtonMode::label`), so every mount reads the same.
- **KIT-051** Decorative graphics are `aria-hidden`. A covered pane (the list under compact
  details) is `aria-hidden` too.
- **KIT-052** Activation uses native `<button>`s. A widget that needs `tabindex` takes an opt-in
  prop (`Split::keyboard`), because setting `tabindex` panics Blitz; the web build opts in and
  carries the keyboard tests. Focus is neither trapped nor restored anywhere yet (Blitz has no
  focus API to build on); `Sheet` is not web-shippable until it is.
- **KIT-053** A headless part uses only the tags, attributes and style properties a Wasm guest's
  tree may name (`clay_surface::render`'s vocabulary). Checked: the test harness mirrors the lists
  and panics on anything else. Inline `style` strings are passed whole and are not checked
  property by property.

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
  `testing` into a `testing` cargo feature then, not earlier.
- **KIT-072** Every component has a headless test module; a styled part with logic of its own
  (width to placement) has a skin test.
- **KIT-080** Every component has a specimen in `dayframe_showcase/kit.rs`, at a compact and an
  expanded host width. A specimen is a fixed-size frame standing in for the host: its width is
  both its CSS width and the `WidthClass` / extent it hands the component. The showcase is where
  a new component is reviewed.

### Renderer limits

- **KIT-090** A limit of Blitz, the browser or the guest wire that shaped a component is written
  in that component's module docs under `# Renderer limits`, with the workaround, so callers
  never rediscover it. See `headless/sections.rs` (no measuring, no pointer capture, no
  `tabindex`, page vs element coordinates) and `headless/sheet.rs` (focus).

## Design-language gaps

Found while building the first four components. Each used the nearest existing token; none were
invented. They are filed in the format of the `dayframe-design-language` skill in the PR report.

- No **breakpoint** scale: `WidthClass` defines 600 / 840 px (the Material window-size classes)
  in `width.rs`.
- No **scrim / overlay** role: `Sheet` dims with `bg-surface-sunken opacity-80`, the nearest token that darkens in both themes (`text` at low opacity lightens a dark theme).
- No **layering** scale: `z-index` values (10 handle, 20 covering details, 40 sheet, 50 gesture
  overlay) are structural constants in the headless parts.
- No **sheet size** vocabulary: `h-5/6` (compact bottom sheet) and `w-96` (side sheet) are
  Tailwind fraction/spacing utilities.
- No **motion** tokens, so nothing animates (INTERACTION-004).
- No **hit-target size** token: the 16 px handle hit area and the `size-11` (44 px) nav button are
  hard-coded.

## Open work

- Keyboard and focus tests belong in the browser build (Playwright): arrow-key resizing, Escape,
  and the missing focus trap/restore for `Sheet` (KIT-052).
- Renderer capability facts (can this renderer take `tabindex`?) must come from the host; a guest
  micro cannot know which renderer mounts it. The shell has to decide how `Split::keyboard`
  reaches guests.
- The tile list lands with home (ADR 032 section 7).
