# Dayframe styles

## Material generation

`material-theme.json` is the optional Material Theme Builder input. `material.css` is generated from it with the copied Gursha generator:

```sh
cd ../ecma/gursha
pnpm exec tsx \
  ../townframe/src/ui/styles/gen-material-css.ts \
  ../townframe/src/ui/styles
```

The Material files provide the external theme-provider values used by `.df-theme`; they are not the authored Dayframe semantic token source.

## Dayframe foundation tokens

The authored foundation source is in `tokens/*.tokens.json` and follows the supported DTCG subset. Validate and generate the runtime contract with:

```sh
deno run --allow-read --allow-write x/design-tokens.ts --write
deno run --allow-read --allow-write x/design-tokens.ts --check
deno run --allow-read --allow-write x/design-tokens.ts --self-test
```

Foundation units follow `docs/design-language/foundations.md`: rem is used for scalable spacing and layout dimensions, px for borders and explicit boundaries, and em only for documented component-local relationships. The native root rem basis remains an implementation concern.

The generator writes `tokens.generated.css`, which owns the default `--df-*` values and the Tailwind theme aliases. Do not edit that generated file directly. `tokens.css` contains the reset and the Material theme-provider adapter for theme-dependent semantic colors.

`../tailwind.css` imports Material CSS, the generated Dayframe token grammar, the runtime adapter, and feature styles in that order. `assets/tailwind.css` is the generated bundle consumed by the Dioxus foundation loader.
