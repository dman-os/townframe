# ADR 055: Colour system: palettes, roles and schemes

**Status:** Accepted (2026-11-25; the open mappings of §3.2 settled by the operator the same day). Proposed by the claytxt lane in the design pairing and accepted by the operator the
same day. Nothing here is built. The exact step each role takes is tuned in the colour sitting on the theme lab's
specimen board (§4 gives the starting mapping).

**Amends:** FOUND-012 (Material stops being the theme provider; it becomes one way to make a palette, §7) and the
"Semantic roles" section of `docs/design-language/foundations.md`.

Related: `docs/design-language/` (foundations, styling contract), `docs/scratch/theme-lab-micro.md` (the theme lab and how
a sheet is applied).

## 1. Context

Today the colour roles (`--ct-color-*`, 26 of them) are wired straight to Material:
- `tokens.css` sets each role from a Material scheme variable (`--scheme-color-*`), and `material.css` generates those
  from `material-theme.json`;
- the scheme (light, dark and their contrast variants) is a class on `ThemeRoot`, read at the root through `:has`;
- statuses Material has no colour for (success, warning) are patched per scheme by hand.

So the colours and the roles are one layer: a theme author has to set roles, there is no palette to set, and light and
dark are two unrelated sets of role values. The scheme lives on the root, so nothing below it can show the other scheme
(the theme lab's canvas cannot preview dark while the app is light).

## 2. Prior art

- **Base16.** A theme is exactly 16 values: `base00`–`base07`, one neutral ramp from background to foreground, each
  slot with a job (default background, a lighter background, selection, comments, the foregrounds), and
  `base08`–`base0F`, eight accent hues. Applications map their roles onto slots, which is why one theme fits every
  application. The theme supplies values, never roles.
- **Solarized.** Eight neutral tones laid out symmetrically plus eight accents: light and dark are the same palette with
  the neutral ramp mirrored.
- **Gruvbox.** Background and foreground ramps plus seven hues, each in bright, neutral and faded variants that the light
  and dark variants pick between.
- **Catppuccin.** One role vocabulary (`base`, `mantle`, `crust`, `surface0`–`2`, `overlay0`–`2`, `text`, `subtext0`–`1`,
  14 named accents) over four palettes (Latte, Frappé, Macchiato, Mocha), with a style guide that assigns each role a
  job. Dracula and Nord are the same shape with fewer names.
- **Material 3.** Key colours expanded into 0–100 tonal palettes; roles are tone picks (`primary` at tone 40 in light,
  80 in dark; `on-primary` at 100 and 20), so light and dark are two tone mappings of one palette. Each colour role
  is a quartet (X, on-X, X-container, on-X-container) readable by tone distance; contrast levels move the tones
  apart; a surface-container ladder (lowest to highest) replaces elevation tints. Dynamic colour derives the key
  colours from a seed or an image.
- **Radix Colors.** 12-step scales where every step has a defined job: 1–2 backgrounds, 3–5 component backgrounds
  (normal, hover, pressed), 6–8 borders and focus, 9–10 solid fills, 11–12 text.

The common lesson: two layers. A palette (a neutral ramp and a few hues) is what a theme author writes; roles are an
assignment onto it, owned by the design system.

## 3. Decision: two layers

### 3.1 Palette: what a sheet supplies

A palette is a set of ramps of 12 steps, with Radix's jobs:

| Step  | Job                                            |
|-------|------------------------------------------------|
| 1–2   | app and subtle backgrounds                     |
| 3–5   | element backgrounds: normal, hover, pressed     |
| 6–8   | subtle border, border, strong border and focus |
| 9–10  | solid fill, and its hover                      |
| 11–12 | low-contrast and high-contrast text            |

Ramps:
- `neutral`: the greys every surface, text and border comes from;
- `primary` and `accent`: the brand hues;
- `danger`, `success`, `warning`, `info`: the statuses.

Each ramp also names a `contrast` value: the text colour readable on its step 9 (white or near-black, per hue).

Custom properties: `--ct-palette-<ramp>-<step>` and `--ct-palette-<ramp>-contrast`. A sheet that sets the palette has
themed everything; it needs no role. A Dracula or Gruvbox port is a palette.

### 3.2 Roles: what the kit consumes

The roles stay the theme API the kit and micros use (COMPOSE-001); they are now **derived** from the palette by the
foundation, not authored per theme. A sheet may still override a role directly (the user-chrome case), and that
override wins over the derivation.

Role families follow Material's quartet, so every pair is readable by construction:

| Family                     | X                | on-X              | X-container | on-X-container |
|----------------------------|------------------|-------------------|-------------|----------------|
| primary, accent            | step 9           | ramp `contrast`   | step 3      | step 11        |
| danger, success, warning, info | step 9       | ramp `contrast`   | step 3      | step 11        |

`selected`/`on-selected` become accent-container/on-accent-container (one role, not two names for one colour).
`warning` text uses `on-warning-container` (step 11), which is readable on light surfaces, where today's `warning`
fails AA.

Surfaces become a ladder over the neutral ramp: `canvas` (1), `surface` (3), `surface-raised` (4). Text: `text` (12),
`text-muted` (11), `text-subtle` (11, the same step as `text-muted`). Lines: `border` (6), `border-strong` (7), `focus`
(primary 9, the solid fill, so the ring is the brand colour and keeps the contrast of a fill).

- **`surface-sunken` is the one exception to the ladder.** It is a value of its own per scheme (the neutral ramp's
  `sunken`), not a step. A well is darker than the surface it sits in; in the light scheme that puts it below the
  surface and canvas, which a ramp that runs monotonically from light to dark cannot also hold while step 3 is the
  surface. (Dark has no such conflict; its sunken is the canvas colour, as it was under Material.) The check treats it as a surface
  like the others.
- **`scrim` is black at the scrim alpha (32%) in every scheme**, not neutral 12: in a dark scheme step 12 is a near-white
  that would lighten what the scrim covers.
- **State backgrounds** (steps 4-5, hover and pressed) get no role until a component needs one; the steps stay in the
  palette.

### 3.3 Readable pairs are checked

`x/design-tokens.ts` and the theme lab check every quartet and every text role against the surfaces it is meant for,
at WCAG AA (4.5:1 for text, 3:1 for non-text UI), for every scheme and contrast level a sheet supports. The more-contrast
level is held to AA like the standard one, not to a stricter bar: it moves roles to darker or lighter steps, it does not
change the threshold. The lab shows
the ratio on each swatch and flags failures; the token check fails the shipped palette's build.

## 4. Schemes and contrast

### 4.1 The setting

The shell's settings hold two values, chosen by the user:
- **scheme**: `system` (follows `prefers-color-scheme`), `light` or `dark`;
- **contrast**: `system` (follows `prefers-contrast`), `standard` or `more`.

### 4.2 How a sheet answers it

- The host sets `color-scheme: light` or `dark` on the root from the setting.
- Every palette value is written once with CSS `light-dark(<light>, <dark>)`. A custom property holding `light-dark()`
  resolves where it is used, against the `color-scheme` of the element using it. So:
  - a sheet states both schemes in one declaration, never in two selector blocks;
  - any subtree can set its own `color-scheme` and gets the other scheme: the theme lab's canvas toggles light and dark
    for its preview alone.
- Contrast is not a CSS property, so the host sets `data-contrast="more"` on the root when the setting (or
  `prefers-contrast: more` under `system`) asks for it. The foundation's role mapping has a `more` variant (text 12 for
  muted text, `border-strong` for borders, step 10 for X), so a palette gets a high-contrast mode without the sheet
  writing one. The variant is checked at AA like the standard mapping (§3.3), including the plain `border` at 3:1. A sheet may override it under `[data-contrast="more"]`.

### 4.3 Single-scheme sheets

A sheet declares which schemes it supports (its metadata in the stylesheet stack; Dracula is dark-only). When the active
palette supports one scheme, the toggle shows the scheme as fixed by the theme instead of ignoring the user's choice.

### 4.4 Engines

`light-dark()` needs a recent engine: WebKitGTK (the desktop) and Android System WebView must be verified before the
foundation depends on it. If either lacks it, the fallback is the host setting `data-scheme` on the root and sheets
writing per-scheme blocks under it; that keeps one setting and one toggle but loses per-subtree schemes. Blitz never
shapes this decision.

## 5. Where sheets come from

Palettes and role overrides are stylesheets in the stylesheet stack: an ordered list of sheets (shipped, installed,
the user's own), each in its own cascade layer, later winning, every sheet above a micro's own rules. The stack's
own design (storage, ordering, metadata) is a separate decision; this ADR only needs that a palette is one sheet in it
and that a sheet can declare the schemes it supports.

The shipped daytxt look is the first sheet: a palette, nothing more.

## 6. Out of the first cut

- **Palette from an image.** A generator in the manner of Material's dynamic colour: pick key colours from an image or
  a seed, build the 12-step ramps in a perceptual colour space (HCT or OKLCH) with the contrast rules of §3.3, and
  write the result as a palette sheet. Because the output is an ordinary palette, nothing else changes.
- Component-level tokens: still added only when a component needs a themeable contract (FOUND-011).
- More than one accent hue beyond `primary` and `accent` (Catppuccin's 14): wait for a need.

## 7. Consequences

- Positive: a theme is about twenty values; every role, pair and contrast level follows.
- Positive: light, dark and more-contrast come from one palette and one setting; a subtree can show either scheme.
- Positive: readable pairs are checked, not hoped for.
- Cost: `tokens.css`'s Material adapter and `material.css` go; Material becomes one palette generator among others.
- Cost: an engine dependency on `light-dark()` (§4.4), to be verified first.

## 8. Order of work

1. Verify `light-dark()` on WebKitGTK and Android System WebView with a canvas-only toggle in the theme lab.
2. Palette tokens in `x/design-tokens.ts` (12-step ramps, `contrast`), the role derivation, and the contrast check.
3. The shipped palette: today's look re-expressed as a palette, so nothing visibly changes; Material adapter removed.
4. The theme lab: a Palette page (each ramp with its steps' jobs), contrast ratios on swatches, the scheme toggle on the
   canvas.
5. The shell's scheme and contrast setting, applied by the host.
6. The colour sitting: two or three candidate palettes judged on the board.
