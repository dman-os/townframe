# dayframe_dx design system / showcase

Status: Proposal
Owner: operator + dayframe design track
Scope: Shared `ui` design language and the native `showcase` inspection surface.
Updated: 2026-10-22

## Intent

- Shared `ui` owns the visual language.
- `showcase` is a native Dioxus inspection surface, not production UI.
- The showcase has no core, IPC, persistence, network, or platform service dependencies.
- Tile grammar remains a future experiment page; the current tile prototype is not treated as shippable design-system code.

## Renderer constraints

| Constraint | Rule |
| --- | --- |
| Native first | Iterate with the Blitz/native desktop renderer. |
| Android | Use Android for layout, text, focus, and input smoke checks. |
| Webview fallback | Keep shared CSS within the subset honored by Blitz and wry. |
| CSS | Prefer custom properties, basic selectors, flex/grid, and simple color values. |
| Layers | Use Tailwind v4 `@layer base`, `theme`, and `components` for source ordering. |
| Avoid | `@property`, `color-mix()`, modern color functions, DOM/JS assumptions. |

## Material source of truth

| Artifact | Location | Role |
| --- | --- | --- |
| Theme JSON | `src/dayframe_dx/ui/styles/material-theme.json` | Included fixture and generator input. |
| Generator | `src/dayframe_dx/ui/styles/gen-material-css.ts` | Copied from Gursha. |
| Generated CSS | `src/dayframe_dx/ui/styles/material.css` | Material palette and scheme custom properties. |
| Dayframe aliases | `src/dayframe_dx/ui/styles/tokens.css` | `--df-*` names mapped onto Material scheme tokens. |
| Loader | `src/dayframe_dx/ui/src/foundation.rs` | Loads the generated Tailwind output containing Material CSS and Dayframe aliases. |
| Color page | `src/dayframe_dx/showcase/src/main.rs` | Includes JSON and renders every palette/scheme entry. |

### Generator

The copied generator uses Gursha's Node dependencies (`colorjs.io`, `zod`, `tsx`):

```sh
cd ../ecma/gursha
pnpm exec tsx \
  ../townframe/src/dayframe_dx/ui/styles/gen-material-css.ts \
  ../townframe/src/dayframe_dx/ui/styles
```

Generated output:

- `material.css`
- `material-tokens.ts` (generated reference artifact; not consumed by Rust)

The JSON and generated CSS must remain from the same generator run.


## Theme architecture

- Theme family and mode are separate: `material + light`, `material + dark`, or a user family with either mode.
- `ThemeRoot` establishes `df-theme theme-<family> <light|dark>` on an ancestor element.
- Material CSS supplies the baseline scheme variables.
- Dayframe `--df-*` variables are the stable component contract.
- User stylesheets may override only selected variables; missing values inherit from the active baseline.
- Subtrees may override variables by setting them on an ancestor element; descendants inherit the override.
- `ThemeStyles` injects trusted user CSS; theme sheets should only set contract variables.
- Tailwind `@theme inline` aliases compact utility tokens to `--df-*` variables.
- Theme styles must define variables, not structural component selectors.


## Obsidian lessons

Obsidian separates global foundations, semantic/component variables, editor content, plugins, and window chrome. Dayframe should keep the same separation while exposing only stable `--df-*` names to components.

- Prefer semantic roles such as canvas, surface, text, border, accent, and interactive states over raw palette names.
- Keep mode-dependent declarations at the theme boundary and allow sparse overrides to inherit.
- Give state variants explicit names (`hover`, `active`, `focus`, `disabled`, `error`) instead of making components derive them ad hoc.
- Obsidian exposes companion channels for translucent colors; we should add those only when a real renderer need appears rather than proliferating aliases.
- A future Obsidian converter should map semantic variables into `--df-*`, not make Obsidian names the production contract.
- Use classes and `data-*` selectors for finite component variants and interaction states; reserve CSS variables for theme contracts and genuine scales, not every per-instance knob.
## Spacing and geometry

- Primary grid: 4px (`--size-4-*`).
- Fine grid: 2px (`--size-2-*`), used sparingly.
- Radius, border-width, spacing, color, and Tailwind aliases live in `tokens.css`.
- Components should use token variables rather than literal spacing, radius, or border values.

## Design grammar

| Layer | First-pass vocabulary |
| --- | --- |
| Foundations | Material colors, semantic aliases, type, spacing, radius, elevation, motion, iconography |
| Primitives | tile, button, field, menu, badge, dialog, list |
| Tile grammar | ordered list, app/widget/group, nested sublist, column, overlay, stack |
| Patterns | navigation, search, filtering, empty/loading/error, editor, task progress |
| Screens | home, drawer, editor, tables, progress, settings, capture |

## Current foundations slice

- [x] Establish Material JSON as the color input.
- [x] Generate and check in `material.css`.
- [x] Load the generated Tailwind output containing Material CSS and Dayframe aliases.
- [x] Map `--df-color-*` aliases to Material scheme variables.
- [x] Add shared `FoundationStyles` loader.
- [x] Apply aliases to the remaining shared shell styles.
- [x] Add native showcase package.
- [x] Include the JSON in Rust with `include_str!`.
- [x] Render every palette and scheme color with readable foreground text.
- [x] Add the Obsidian-style 2px/4px spacing grid and geometry tokens.
- [x] Define typography roles and text specimens.
- [x] Define surface, divider, focus, and interactive-state primitives.
- [x] Add `ThemeMode` and `ThemeRoot` with light/dark switching in the showcase.
- [x] Add focused/selected/disabled/hover/pressed/error state fixtures.
- [ ] Replace remaining ad-hoc colors in shared UI.

## Showcase page inventory

- [x] Material palettes.
- [x] Material schemes: light, contrast variants, dark, contrast variants.
- [x] Typography specimens.
- [x] Spacing and layout specimens.
- [x] Shape and elevation specimens.
- [x] Interactive state matrix.
- [x] Primitive components (tabs, buttons, cards, separators).
- [ ] Realistic screen compositions from `dayframe-app-model.md`.
- [x] Tile grammar lab with mixed App, Widget, and Group tiles, peer columns, stacks, content variants, and sublist presentation.

## Color-page requirements

- Parse the included JSON into typed `schemes` and `palettes` maps.
- Show every palette name, color name, and source value.
- Show every scheme name, color name, and source value.
- Use deterministic ordering.
- Choose readable foreground text from the source hex value.
- Keep the page independent from generated CSS so the JSON remains inspectable.

## Acceptance

```sh
cargo fmt -p ui -p showcase -- --check
cargo clippy -p ui --all-targets --all-features
cargo clippy -p showcase --all-targets --all-features
```

Manual:

```sh
target/dioxus-cli/bin/dx serve -p showcase --platform desktop --renderer native
```

The first useful review is visual: inspect all palette/scheme swatches, then iterate on token names and hierarchy before adding components.
