# claytxt styles

## Material generation

`material-theme.json` is the optional Material Theme Builder input. The copied Gursha generator can still turn it into a `material.css` (the file is no longer committed or imported):

```sh
cd ../ecma/gursha
pnpm exec tsx \
  ../townframe/src/clay_dioxus_kit/styles/gen-material-css.ts \
  ../townframe/src/clay_dioxus_kit/styles
```

`material.css` is no longer part of the bundle (ADR 055): the shipped palette (`tokens/palette.tokens.json`) took its scheme values, and `material-theme.json` stays as the input a Material palette generator would read.

## claytxt foundation tokens

The authored foundation source is in `tokens/*.tokens.json` and follows the supported DTCG subset. Validate and generate the runtime contract with:

```sh
deno run --allow-read --allow-write x/design-tokens.ts --write
deno run --allow-read --allow-write x/design-tokens.ts --check
deno run --allow-read --allow-write x/design-tokens.ts --self-test
```

### The palette

`tokens/palette.tokens.json` is the authored colour source (ADR 055): twelve-step ramps (`neutral`, `primary`, `accent`, `danger`, `success`, `warning`, `info`), each step and each ramp's `contrast` written once for light and dark. The same generator writes `palette.generated.css`: the ramps as `--ct-palette-<ramp>-<step>` custom properties holding `light-dark(<light>, <dark>)`, the `--ct-color-*` roles derived from them (the table is `roles` in `x/design-tokens.ts`), and a `:root[data-contrast="more"]` variant. Every derived pair (text on surfaces, each X on-X quartet, focus and strong borders as non-text UI) is checked at WCAG AA for each scheme and contrast level on every run, and a failing pair fails the build with the colours and the ratio. `tailwind.css` imports it, and it is the shipped palette: Material's scheme values for the primary, accent (secondary) and danger (error) ramps and the surface ladder, so the look is the one Material gave. There is no Material adapter any more; Material survives only as the palette generator input below.

Foundation units follow `docs/design-language/foundations.md`: rem is used for scalable spacing and layout dimensions, px for borders and explicit boundaries, and em only for documented component-local relationships. The native root rem basis remains an implementation concern.

The generator writes `tokens.generated.css`, which owns the default `--ct-*` values and the Tailwind theme aliases. Do not edit that generated file directly. `tokens.css` holds the root's own rules (the colour-scheme and canvas of the page).

`../tailwind.css` imports the generated claytxt token grammar, the generated palette, the root rules, and feature styles in that order. `assets/tailwind.css` is the generated bundle consumed by the Dioxus foundation loader.
