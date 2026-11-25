# ADR 057: Kit baseline and the identity sheet

**Status:** Accepted (2026-11-25, operator, in the theme-lab review). Proposed by the claytxt lane. Nothing here is built.

**Amends:** FOUND-011 (names the component tokens this ADR needs), `docs/design-language/styling-contract.md` (where a
look lives), ADR 055 §5 (the shipped sheet carries more than a palette).

Related: [ADR 055](055-colour-system-palettes-roles-and-schemes.md) (palettes, roles, schemes),
`docs/design-language/styling-contract.md` (layers, parts), `docs/design-language/foundations.md`.

## 1. Context

Reviewing the kit on the theme lab, the operator asked for cards and alerts without borders, better shadows on sheets
and drawers, and a different identity for the app overall, and then asked the question this ADR answers: when a
border goes, does that change a theme sheet or the component's implementation?

Today it changes the implementation. Every visual choice is a rule in the component's skin (`styles/kit/*.css`):
`card.css` draws a hairline border on `outline` cards and a medium shadow on `raised` ones; `alert.css`, a status
`card` and `chip.css` draw a border in the status colour; sheets, drawers and the scrub bubble cast one offset black
shadow. A sheet in the `user` layer can override any of them, but only by restating component selectors, so the
look the app ships is not a sheet at all: it is the kit.

The operator also named the look to avoid: the "hyper slopified" visual language of generated UIs, catalogued as a
parody at slopstrap.mikelacher.com: tone-coloured borders and left stripes, gradients, glow, frosted glass, nested
bordered boxes, pills and badges on everything, blinking dots, emoji as icons, low-contrast grey body text.

## 2. Decision: two layers of look

### 2.1 The kit is structure and a quiet default

The kit's skins carry what every theme needs and no theme should have to restate:
- layout, spacing, sizing and the part vocabulary;
- states: hover, pressed, selected, disabled, open, engaged;
- the focus ring, and legibility (every text pair at AA, ADR 055 §3.3);
- functional motion and functional fades (the scrub strip's scrim, a scroll edge).

Everything two reasonable themes would disagree on is a **component token** with a quiet default, not a rule: a
border, a shadow, a radius, a fill choice. The kit's defaults are deliberately plain: **no borders on containers, no
shadows on anything that sits in the flow, no decorative gradients**. A kit with every identity sheet removed is
usable and unremarkable.

The test for where a choice goes: if every theme needs it (usability, accessibility, the part contract), it is kit.
If two reasonable themes would choose differently, it is a token the identity sets.

### 2.2 The identity is one sheet on top

The app's look is the **identity sheet**: the first sheet in the stylesheet stack (ADR 055 §5), above `kit` and below
the user's own sheets. ADR 055 made it a palette; it now also sets the component tokens (shape, borders, elevation) and,
where a token is not enough, component rules over the part vocabulary. Changing the app's look means editing this
sheet, never Rust and never `styles/kit/*.css`. A user's theme is another sheet after it, with the same powers.

Until the stylesheet stack exists, the identity sheet is a file in the kit bundle (`styles/identity/daytxt.css`) in its
own cascade layer, `identity`, declared between `kit` and `user`:

```css
@layer reset, tokens, kit, identity, user;
```

In a shadow-root mount it arrives inside `host` like the other bundle layers (`host.identity`), so the ordering argument
of the styling contract is unchanged.

### 2.3 Component tokens

FOUND-011 allows a component token only for a stable, themeable contract. These are those contracts, one family per
component, consumed by its skin and set by the identity:

| Token | Components | Kit default |
|---|---|---|
| `--ct-<c>-radius` | card, alert, button, chip, sheet, drawer, list-row, tabs, popover | the current radius scale step |
| `--ct-<c>-border` | card, alert, chip, button (outline variant only), popover | `none` (a `border` shorthand value) |
| `--ct-<c>-shadow` | sheet, drawer, popover, scrub bubble | the elevation step in §4 |
| `--ct-<c>-fill` | card, alert | the variant's surface or container role |

`<c>` is the component's class name without `ct-` (`--ct-card-border`). A theme that wants bordered cards writes
`--ct-card-border: 1px solid var(--ct-color-border)` and nothing else. The list grows only when a component needs it
(FOUND-011 still holds); a token is never added for one screen.

## 3. Containers: fill, not edge

A container separates itself from its background by **fill**, one step on the surface ladder of ADR 055, not by a
line.

- **Card variants** become:
  - `tonal` (the default): `surface-raised` on `surface`, or `surface` on `canvas`; no edge, no shadow;
  - `sunken`: `surface-sunken`, for a well that holds other content (code, previews, inputs grouped together);
    it sits inside a surface (a tonal card, a panel), one step below it, and never directly on the canvas;
  - `plain`: no fill; it only groups and pads (a section of a form, a run of rows);
  - `outlined`: a hairline border, opt-in, for a container on a busy background where fill alone is not enough.

  The `raised` card (a shadow in the flow) is removed: a card does not float, so it casts nothing (§4).
- **Status** (alert, status card, chip): the status's container quartet from ADR 055 (`danger-container`,
  `on-danger-container`, ...), with no border and no stripe. The status icon carries the status for readers who do not
  see the colour; colour is never the only signal.
- **Nesting:** a container inside a container takes the next ladder step or `plain`; it never adds an edge.

The theme lab board shows the four card variants side by side, so the identity's choice is made on the board.

## 4. Elevation: only what floats casts

Shadows mean "this floats above the page and will go away": sheets, drawers, popovers and menus, the scrub bubble.
Nothing in the flow casts a shadow.

The current shadows look cheap for three reasons: one hard layer of pure black, an offset that points down even on a
sheet anchored to the bottom edge (it falls off-screen and leaves only a grey haze along the top), and the same shadow
in dark mode, where it is invisible against dark surfaces. Instead:

- **Layered:** each step is a hairline ring, a tight contact shadow and one or two wide ambient shadows, each at low
  alpha, the way light falls on a real edge.
- **Tinted:** the ink is the neutral ramp's darkest step, not black, so shadows carry the palette's temperature.
- **Edge-anchored surfaces use no offset:** a sheet, a drawer and a full-height panel cast an even, zero-offset ambient
  shadow plus the ring; a direction only makes sense for something that floats free (a popover, the bubble).
- **Dark scheme:** elevation comes from the surface ladder (the floating surface is lighter), with a faint light
  ring on its edge; the shadow stays for separation from the scrim but is not what reads as height.

Two steps, written once with `light-dark()` (ADR 055 §4.2); values are the starting map, tuned on the board:

```css
--ct-shadow-ink: light-dark(var(--ct-palette-neutral-12), #000);
--ct-shadow-ring: light-dark(
  color-mix(in oklab, var(--ct-shadow-ink) 8%, transparent),
  rgb(255 255 255 / 0.07)
);

/* popover, menu, scrub bubble: floats free, small and directional */
--ct-elevation-float:
  0 0 0 1px var(--ct-shadow-ring),
  0 1px 2px color-mix(in oklab, var(--ct-shadow-ink) 6%, transparent),
  0 4px 12px -2px color-mix(in oklab, var(--ct-shadow-ink) 10%, transparent);

/* sheet, drawer: anchored to an edge, so no offset */
--ct-elevation-anchored:
  0 0 0 1px var(--ct-shadow-ring),
  0 0 2px color-mix(in oklab, var(--ct-shadow-ink) 6%, transparent),
  0 0 24px color-mix(in oklab, var(--ct-shadow-ink) 10%, transparent),
  0 0 64px color-mix(in oklab, var(--ct-shadow-ink) 8%, transparent);
```

`--ct-elevation-sm`, `-md` and `-raised` go; the shadow primitives in `primitives.tokens.json` are replaced by these
two semantic steps. The colour check of ADR 055 §3.3 does not apply to shadows; the board is the check.

## 5. What the identity must not do

The kit does not stop a user sheet from doing anything; this list binds the shipped identity:
- no decorative gradients, glow, or frosted/blurred glass;
- no coloured borders or stripes to mark status or emphasis;
- no shadow on anything in the flow (§4);
- no edge on a container nested in a container (§3);
- no pills or badges for information that is not a control or a count;
  pills are for controls and counts only: filter and toggle chips stay pills, while a status chip
  (information) is a small rounded rectangle (`radius-sm`) that keeps its glyph;
- no emoji as icons; no low-contrast body text (ADR 055 AA holds for every text role).

## 6. Consequences

- Positive: the app's look is one file the operator (and later the end user) edits; a restyle never touches a
  component.
- Positive: the default kit is plain enough that a theme adds character rather than removing noise.
- Positive: a theme lab change to a component token restyles every instance, in every micro, live.
- Cost: every skin is reworked once to read its tokens; the `raised` card variant and three elevation tokens are
  removed (unmerged code: no migration).
- Cost: one more cascade layer to keep ordered across the host document and shadow-root mounts.

## 7. Order of work

1. The `identity` layer and `styles/identity/daytxt.css`; the component tokens of §2.3 with kit defaults; skins read
   them. With the identity sheet empty, the board shows the quiet default.
2. Containers (§3): card variants, status as container fills, borders off; the board shows the four card variants.
3. Elevation (§4): the two steps; sheets, drawers, popover and bubble use them; the old steps and primitives go.
4. The shipped identity: today's choices that survive the review re-expressed in `daytxt.css` (radius, any opt-in
   borders), judged on the board in both schemes.
5. The theme lab edits the identity sheet's tokens live, like the palette.
