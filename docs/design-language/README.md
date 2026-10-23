# Dayframe design language

This directory is the handwritten normative contract for Dayframe's platform-agnostic visual language. It is intentionally small. Token files define structured values; these documents define how values may be used; the showcase provides selected executable examples.

## Reading order

1. [Foundations](foundations.md) — token layers, supported values, naming, units, and generation.
2. [Composition](composition.md) — rules for combining reusable primitives.
3. [Interaction](interaction.md) — state, focus, motion, and accessibility requirements.

Feature grammars, including tiles, columns, widgets, and screens, are not foundation tokens. They belong in their own design documents and may consume this foundation.

## Source of truth

- `src/dayframe_dx/ui/styles/tokens/*.tokens.json` is the authored DTCG-compatible foundation source.
- `x/design-tokens.ts` validates the supported subset, resolves aliases, and generates `src/dayframe_dx/ui/styles/tokens.generated.css`.
- `src/dayframe_dx/ui/styles/tokens.generated.css` is generated; do not edit it directly.
- `src/dayframe_dx/ui/styles/tokens.css` contains the runtime reset and the external Material theme-provider adapter. It may override theme-dependent color roles, but it is not an authoring source for foundation values.
- `src/dayframe_dx/ui/tailwind.css` imports the generated token grammar before feature styles.
- `src/dayframe_dx/ui/styles/material-theme.json` remains an external Material theme input and image-to-theme compatibility fixture. It is not the Dayframe semantic token source.
- `src/dayframe_dx/showcase/` is curated executable evidence, not the normative source and not an exhaustive token catalogue.

Tailwind aliases are derived from semantic token paths. For example, `color.surface-sunken` produces the `--color-surface-sunken` theme alias and the `bg-surface-sunken` utility. Existing aliases such as `bg-background` remain compatibility aliases while components migrate; new shared components use the semantic vocabulary defined by the foundation rules.

Validate and regenerate with:

```sh
deno run --allow-read --allow-write x/design-tokens.ts --write
deno run --allow-read --allow-write x/design-tokens.ts --check
deno run --allow-read --allow-write x/design-tokens.ts --self-test
```

Rules use stable identifiers such as `FOUND-001`. A change that violates a rule must either be rejected or explicitly update the rule and its examples.
