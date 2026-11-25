# Styling contract

Status: approved by the operator 2026-11-18; applied by every kit component (`Button`, `Tabs`, `Card`, `Separator`, `Alert`, `Chip`, `ScrubStrip`, `AppBar`, `Drawer`, `ListDetails`, `ListRow`, `NavButton`, `PanelPage`, `Sections`, `Sheet`) and by our micros except `micro_review` (see [What is left](#what-is-left)).

A component's appearance is a stylesheet, and a stylesheet is something an end user can replace. The contract says what a component emits so that a stylesheet, any stylesheet, can restyle it without touching code. It is written for the DOM and CSS, not for Rust: a micro written in Solid or React meets the same contract by emitting the same vocabulary (see [Other frameworks](#other-frameworks)).

The kit's own skin is one such stylesheet. If a visual change needs a Rust edit, the component broke the contract.

## The five rules

1. **Rust owns structure and behaviour; CSS owns every visual.** The headless part renders the DOM, ARIA, focus, keyboard handling and events. It names no colour, size, spacing, radius, font, shadow, motion or layout that is not needed to make the behaviour work.
2. **The public theming surface is three things.** The `--ct-*` tokens, the stable part and state names (below), and CSS layers. Nothing else (a class a Rust file made up, the nesting depth of the DOM, a tag name) is promised to a theme.
3. **Layers order the cascade: reset, tokens, kit, user.** A micro's own unlayered rules sit above all of them. See [Layers](#layers).
4. **No inline styles.** Geometry that is computed at run time (a pointer position, a measured extent) reaches CSS as an inline CSS custom property (for example `style="--ct-strip-push: 12px"`) that the stylesheet reads. An inline declaration of any other property is a bug.
5. **The kit's skin is a stylesheet keyed on part names and states.** It is not Tailwind utility strings in Rust and not a skin struct of class strings.

Consequences a reviewer can check:

- A headless part contains no class literal except its root's `ct-<component>`, and no utility string.
- A skin selector is a root class, a `data-part` and a `data-*` state, never a tag or a position (`div > span:nth-child(2)`).
- A skin never uses `!important`. Layers, not weight, decide who wins, so a user rule needs no weapon to beat the kit.
- Every animation is a CSS `transition` or `animation` that the stylesheet declares and `prefers-reduced-motion` removes. Rust never runs a timer to animate.

## Part naming

One component is one root and some named parts.

| Thing | Spelling | Example |
|---|---|---|
| Root | `class="ct-<component>"`, kebab-case, on the root element only | `ct-button`, `ct-scrub-strip` |
| Internal part | `data-part="<part>"`, kebab-case, on the part's element | `data-part="tab"` |
| The one enumerated state of a part | `data-state="<state>"` | `data-state="active"` |
| Boolean state | a bare data attribute, present or absent | `data-disabled` |
| Orientation | `data-orientation="horizontal" \| "vertical"` | on `ct-tabs`, `ct-separator` |
| A stylistic axis the caller picks | `data-variant`, `data-size` | `data-variant="outline"` |
| A semantic axis the caller picks | `data-status` | `data-status="warning"` |
| A property computed at run time | an inline custom property, `--ct-<component>-<what>` | `--ct-strip-push` |

Rules for the names:

- Part names are the component's nouns, not its looks: `list`, `tab`, `panel`, `label`, `marker`, `bubble`; never `blue-bar` or `left`.
- A part name is unique within its component and is selected as `.ct-<component> [data-part="<part>"]`. Components nest (a `Card` holds a `Button`), so a parent's rule that must not reach a child component's parts uses the child combinator or `:where()`.
- States go on the element the user perceives as changing, and describe the state, not the cause (`active`, not `clicked`). Attributes that carry meaning to assistive technology stay ARIA (`aria-selected`, `aria-pressed`, `aria-current`); `data-state` mirrors them so a stylesheet has one vocabulary.
- A native pseudo-class is used where one exists (`:hover`, `:active`, `:focus-visible`, `:disabled`), because a theme can then be written for a component it has never seen. State that has no pseudo-class gets a `data-*`.
- Adding a part, state or axis value is compatible. Renaming or removing one is a breaking change to every theme and is treated like a public API change.

### Examples

```html
<!-- Button: variants and sizes are axes, the label is a part. -->
<button class="ct-button" type="button" data-variant="outline" data-size="md">
  <span data-part="label">Save</span>
</button>

<!-- Tabs: the state is on the tab and on the panel it controls. -->
<div class="ct-tabs" data-orientation="horizontal">
  <div data-part="list" role="tablist" aria-label="Sections">
    <button data-part="tab" role="tab" data-state="active"   aria-selected="true">Overview</button>
    <button data-part="tab" role="tab" data-state="inactive" aria-selected="false" tabindex="-1">Activity</button>
  </div>
  <div data-part="panel" role="tabpanel" data-state="active">...</div>
</div>

<!-- Alert: the status is a semantic axis. -->
<div class="ct-alert" role="status" data-status="success">
  <span data-part="icon" aria-hidden="true"></span>
  <div data-part="content"><p data-part="title">Saved</p><p data-part="body">...</p></div>
</div>

<!-- ScrubStrip: geometry is computed, so it arrives as custom properties. -->
<div class="ct-scrub-strip" data-state="engaged" style="--ct-strip-thumb: 812px">
  <span data-part="marker" data-state="active" style="--ct-strip-push: 96px; --ct-strip-dy: -34px">K</span>
</div>
```

### What a theme can do

Because every visual is a rule over this vocabulary, and a `user` layer rule beats a `kit` layer rule at any specificity:

```css
@layer user {
  /* Lowercase every button. */
  .ct-button { text-transform: lowercase; }

  /* Replace the outline button's border highlight with an underline. */
  .ct-button[data-variant="outline"]:hover {
    border-color: transparent;
    text-decoration: underline;
    text-underline-offset: 0.3em;
  }

  /* Square tabs with a bottom rule on the active one. */
  .ct-tabs [data-part="tab"] { border-radius: 0; }
  .ct-tabs [data-part="tab"][data-state="active"] { box-shadow: inset 0 -2px 0 var(--ct-color-primary); background: none; }

  /* A different success colour everywhere (alerts, chips, any later component). */
  :root { --ct-color-success: oklch(0.62 0.17 150); }
}
```

## Layers

Order, lowest precedence first:

| # | Layer | Contents | Where it comes from |
|---|---|---|---|
| 1 | Tailwind's own (`theme`, `base`, `components`, `utilities`) | preflight, and the utilities the showcase and the micros not yet converted still use | the Tailwind bundle (`src/clay_dioxus_kit/assets/tailwind.css`) |
| 2 | `reset` | `box-sizing`, margin and padding resets | `styles/reset.css` in the same bundle |
| 3 | `tokens` | every `--ct-*` custom property: the generated foundation, then the Material theme-provider adapter | `styles/tokens.generated.css` (from `src/clay_dioxus_kit/styles/tokens/*.tokens.json` by `x/design-tokens.ts`) and `styles/tokens.css` |
| 4 | `kit` | the contract components' skins, one file per component | `styles/kit/<component>.css` in the same bundle |
| 5 | `user` | the end user's theme | the shell's `ShellTheme` facet, applied by `micro_shell::view::use_theme` |
| 6 | unlayered | a micro's own stylesheet | the micro, sanitized by `clay_surface` |

`@layer reset, tokens, kit, user;` is declared once, after Tailwind's import, so Tailwind's layers sit below ours and `user` still outranks a utility (which is what a theme must do to restyle a component still on utilities).

How each reaches a mount, as built today (`docs/scratch/theme-lab-micro.md` has the full account):

- **Host document (the shell and native, light-DOM micros).** The bundle is linked once by `FoundationStyles` (`document::Link`, an `asset!`), so layers 1 to 4 are ordinary document layers. The theme is a `<style>` of `@layer user { ... }` that `use_theme` renders at the top of the frame. Layer order is the one declared in the bundle.
- **A micro mounted in a shadow root.** The Dioxus fork's interpreter (`packages/interpreter/src/ts/core.ts`, `rebuildHost`) flattens every stylesheet of the host document into one constructable sheet wrapped in `@layer host { ... }`, declared as `@layer host, user;`, and adopts it into every mount's root. Layers 1 to 4 therefore arrive as the sublayers `host.theme ... host.reset`, `host.tokens`, `host.kit` in the same relative order, and the end user's theme in the host document (a `@layer user` block) as `host.user`. The micro's own sanitized sheet (the `data-dioxus-shadow-sheet` attribute that `clay_dioxus::MountedSurface` sets) follows, unlayered, and `MountedSurface` appends the theme to it as a top-level `@layer user { ... }` from the `UserStylesheet` context. A top-level layer declared after `host` outranks everything inside `host`, and unlayered rules outrank both.
- **A renderer without shadow roots (Blitz).** The mount's sheet is a light-DOM `<style>` ahead of its content, in the same order.
- **A micro in another framework.** It cannot bring the kit's stylesheet (a guest's stylesheet is its own, sanitized, unlayered sheet) and does not need to: the host document's sheets are adopted into its mount, so emitting the vocabulary is enough.

The two names do not collide: the layer that holds all host sheets in a shadow root is `host`, so the contract's own layer reads `host.kit` there and `kit` in the host document. (It was once also called `kit`, which read `kit.kit`.) A theme's `@layer user` rule beats a kit rule in both places: in the document `user` is declared after `kit`; in a mount the theme is a top-level `user` layer after `host`.

Where a theme sets tokens: custom properties inherit through shadow roots, so a token set on `:root` in the host document reaches every mount. That includes the colour roles: the foundation derives them from the palette on `:root` in the `tokens` layer (`palette.generated.css`, ADR 055), so a theme's `:root { --ct-color-primary: ... }` in the `user` layer replaces them, and a theme that sets the palette (`--ct-palette-<ramp>-<step>`) restyles every role derived from it. The palette values are `light-dark()`, which a custom property carries unresolved to the element that uses it, so the roles follow that element's `color-scheme`; the root takes its `color-scheme` from the mode class a descendant carries (the `.ct-theme.dark` element of `ThemeRoot`) through `:root:has(.dark)`, so an engine without `:has()` keeps only the light default there.

## Other frameworks

The contract is the attribute vocabulary and the stylesheet, so a Solid or React component is conforming when its DOM matches. Nothing here depends on Dioxus.

```tsx
// Solid
<button class="ct-button" type="button" data-variant={props.variant ?? "primary"}
        data-size={props.size ?? "md"} data-disabled={props.disabled ? "" : undefined}
        disabled={props.disabled}>
  <span data-part="label">{props.children}</span>
</button>

// React
<div className="ct-scrub-strip" data-state={engaged ? "engaged" : "idle"}
     style={{ "--ct-strip-thumb": `${thumbY}px` } as React.CSSProperties}>
```

What a port must do:

- Emit the same root class, `data-part`, `data-state`, `data-*` axes and ARIA as the Rust part. Each headless module's docs (`src/clay_dioxus_kit/headless/<name>.rs`) list the parts, states and axes of its component, and its tests are the executable statement of them.
- Set `data-*` booleans as present-or-absent (`data-disabled=""`), never `="false"`.
- Compute dynamic geometry in JS and write it as an inline custom property of the documented name. The stylesheet decides what to do with it.
- Own behaviour the same way: keyboard map, roving focus, `aria-*`. The kit's tests list the keys.
- Not import or restate any skin. It gets the kit skin and the user theme from the host document.

A guest micro (any language) sends its tree over the wire, whose vocabulary admits open `data-*` names and a raw `style` of custom properties (`clay_surface::render`), so the same markup is legal there.

## Blockers to theming

Things that stop a theme from restyling the app today, seeded 2026-11-18. Counts are approximate (grep over the source at the time; the point is the size of the job).

| Blocker | Size | Status |
|---|---|---|
| The old `ui` crate's inline `style:` strings (tiles, `ArcIndex`, `CanvasProbe`) | 63 | Removed with the crate in the lane that wrote this contract. `ArcIndex` returns as `ScrubStrip` with no inline style. |
| Tailwind utility strings as the skin of the old kit components (`app_bar`, `drawer`, `list_row`, `nav_button`, `panel_page`, `sheet`, `sections`, `list_details`) and their `*Skin` structs | about 30 | Fixed 2026-11-18: each is a headless part and `styles/kit/<name>.css`; `Layer`, `SCRIM` and `styles/motion.css` are gone with them. |
| Tailwind utility strings in `micro_theme_lab`, `micro_shell`, `micro_settings`, `micro_sample`, `micro_counter`, `micro_lab`, `micro_layout` | about 60 | Fixed 2026-11-18: kit components, and a micro stylesheet (`styles.css`, one `ct-<micro>` root and local `data-part`s, layout and token-built looks only) for what the kit has no component for. `micro_toolbar` emits the `ct-button` vocabulary by hand (see below). |
| Tailwind utility strings in `micro_review` (and `review_mock`) | about 37 | Open; the intxt lane converts it from this document. |
| Tailwind utility strings in `daytxt_core/lifecycle.rs` (the Settings lifecycle section), `clay_mdk/tests.rs` (a fixture) and the showcase's layout glue (`flex`, `gap`) | about 15 | Open; not on the lane's list. The showcase's glue is by design (the contract's specimens are cells in a page). |
| A micro stylesheet that names `--ct-color-*` tokens (the `micro_sample` text field, the `micro_lab` experiment rows, the `micro_theme_lab` field and text roles) outranks the `user` layer, so a theme restyles them only through the tokens, not by part. | 3 micros | Open; needs the design-session items below (a text input, a list-row primitive that can stand alone, text roles) so these stop being local looks. |
| `Button` cannot carry `aria-description`: it is not in the guest wire vocabulary (`clay_surface::render`), which a kit component is held to, so `micro_toolbar`'s unavailable-because-X button emits `ct-button`, `data-variant`, `data-part="label"` itself. | 1 micro | Open design question: admit `aria-description` to the wire vocabulary, then give `Button` a `description` prop. |
| The status roles (`info`, `success`, `warning`, `danger`, with `on-` pairs) are provisional (FOUND-012: Material has no equivalent, so `info` follows its secondary colour, `danger` its error, and the other two keep authored values). Checked 2026-11-18 as text on the Material surface: `info` and `danger` pass in every scheme (5.8 to 14.4:1); `success` was 3.05 to 3.19:1 in the three dark schemes and now takes green 400 there (6.3 to 6.6:1, ink on it 6.3:1); `warning` is 8.1 to 8.5:1 on dark and **1.8:1 on the light schemes**, which fails AA as text (it is used as a border and a tint, with `--ct-color-text` for the words, so no kit skin reads it as text today). | 2 token files, `tokens.css` | Open for `warning` as text on light: it needs a text-safe role or a darker light value (a design question). The type scale is three sizes (`--ct-dimension-font-size-{sm,md,lg}`). |
| `url()` is refused in a theme and in a micro's sheet, and `@font-face` and `@import` with it. Custom fonts and images cannot be themed. | n/a | Open. Needs a design: `data:` or `blob:` font and image sources, admitted by `clay_surface::refuse_fetching_stylesheet` (which exists to stop the host fetching on a guest's behalf, so a payload carried inline is a different case). |
| The `reset` layer sits above Tailwind's `utilities` (the contract's layers are declared after Tailwind's so that `user` outranks a utility), so a reset rule beats a utility on the same property. The reset only sets `box-sizing` and zeroes `html`/`body` margin and padding. | 1 file | Open; harmless today, revisit if a utility must override it. |
| Focus pseudo-classes (`:focus-visible`, `:focus-within`) did not show in the headless sway/WebKitGTK check, where the webview never becomes the active page, though the keys reached the focused element. The skins' focus rings and `ScrubStrip`'s keyboard reveal are therefore verified by unit tests only. | n/a | Open, verification gap. |
| A micro's own unlayered rules outrank `user`, so a micro that hard-codes colours in its own sheet is not themeable. | by design | Decision recorded in `docs/scratch/theme-lab-micro.md`; revisit if micros start styling themselves. |
| Blitz has no shadow roots, no `prefers-reduced-motion`, and its CSS transitions and `color-mix()` coverage is unverified. A theme's animation may not play there. | n/a | Open, renderer limit (never shapes the contract). |
| The web shell's CSP (`style-src-elem 'self'`) refuses the host document's inline `<style>`, so the shell's own document is not themed there, only the mounts. | web shell | Open; the web shell is superseded. |
| `x/design-tokens.ts` authors tokens only. A component-level token (`--ct-button-radius`) has no authoring path, so the kit's skins read foundation tokens directly. | n/a | Open; add component tokens only when a reusable primitive needs a stable contract (FOUND-011). |

## What is left

Design-session items (decide the vocabulary first; none is built):

- Text roles and typography beyond three sizes (`--ct-dimension-font-size-{sm,md,lg}` only): headings, body, muted and subtle text, an emphasis weight, a type scale a theme can step. Micros build their text from the colour roles locally today.
- Line clamping (`-webkit-line-clamp` or a `max-lines` axis) for a title or a summary that must stop at N lines.
- A stack or gap layout primitive (a flex column or row with a `--ct-dimension-spacing-*` gap): every micro stylesheet is mostly this today.
- A scroll-container part (an element that scrolls on its own axis, with the scrollbar and the shadow cues a theme can restyle); `PanelPage`'s body is the one instance.
- An inline-code component (monospace run, background, padding) over `--ct-font-family-mono`.
- A text input component (the `micro_sample` field and the `micro_theme_lab` textarea), a standalone list row, and the `warning` text role named above.
- Not designed: `aria-description` in the guest vocabulary; directionality; a way to hand safe-area insets to guests.

Not converted yet: `micro_review` (the intxt lane) and the other sites in the blockers table above. The README's legacy rules (`KIT-011` to `KIT-013`) are retired: every component follows `KIT-200` to `KIT-205`.
