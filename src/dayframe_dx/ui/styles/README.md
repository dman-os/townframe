# Dayframe styles

## Material generation

`material-theme.json` is the input. `material.css` is generated from it with the copied Gursha generator:

```sh
cd ../ecma/gursha
pnpm exec tsx \
  ../townframe/src/dayframe_dx/ui/styles/gen-material-css.ts \
  ../townframe/src/dayframe_dx/ui/styles

The generator writes `material.css` and `material-tokens.ts`. Keep the generated CSS and JSON from the same run.

`tokens.css` defines the renderer-neutral `--df-*` aliases consumed by shared UI. Its color aliases must point at Material scheme variables rather than introduce a second palette.

`../tailwind.css` imports these source styles; run Tailwind from `ui/` to write the only bundled asset, `assets/tailwind.css`.
