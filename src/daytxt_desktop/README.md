# Desktop

Desktop composition root for daytxt and native renderer experiments. Shared design tokens and components live in `../ui`; the engine-neutral composition runtime lives in `../daytxt_core`.

```sh
deno run --allow-all x/daytxt.ts desktop   # Wry; --fresh: empty store; --native: Blitz
```