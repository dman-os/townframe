# Desktop

Desktop composition root for Dayframe and native renderer experiments. Shared design tokens and components live in `../ui`; the engine-neutral composition runtime lives in `../dayframe_composition`.

```sh
deno run --allow-all x/dayframe.ts desktop   # Wry; --fresh: empty store; --native: Blitz
```