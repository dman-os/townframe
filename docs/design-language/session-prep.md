# Design session prep

An agenda of open design questions for the operator and the claytxt lane, gathered from the docs. Questions only; none is answered here. Sources: `SC` is [the styling contract](styling-contract.md) (its blockers table and "What is left"), `KIT` is [the kit README](../../src/clay_dioxus_kit/README.md), `TL` is [the theme lab note](../scratch/theme-lab-micro.md), `IX` is [interaction](interaction.md), `FD` is [foundations](foundations.md).

## Type and text

1. Text roles and typography beyond three sizes (`--ct-dimension-font-size-{sm,md,lg}`): headings, body, muted and subtle text, an emphasis weight, a type scale a theme can step. (SC, What is left; SC blockers, the 3 micros that build text from colour roles locally)
2. Line clamping (`-webkit-line-clamp` or a `max-lines` axis) for a title or summary that stops at N lines. (SC, What is left)
3. An inline-code component (monospace run, background, padding) over `--ct-font-family-mono`. (SC, What is left)
4. Fonts, given `url()`, `@font-face` and `@import` are refused: admit `data:` or `blob:` font and image sources via `clay_surface::refuse_fetching_stylesheet`? (SC blockers; TL, the fetch rule)

## Layout primitives

5. A stack or gap layout primitive (flex column or row over `--ct-dimension-spacing-*`): every micro stylesheet is mostly this. (SC, What is left)
6. A scroll-container part (own axis, restylable scrollbar and shadow cues); `PanelPage`'s body is the only instance. (SC, What is left)
7. Phone-width layout rules: micros must be responsive, so what does a micro get to know about the width class, and what do the kit components do at compact? (SC, What is left on safe-area insets and `WidthClass`; KIT, KIT-101)
8. A text input component, a standalone list row, and a `warning` text role, so `micro_sample`, `micro_lab` and `micro_theme_lab` stop carrying local looks. (SC blockers, micro stylesheets naming `--ct-color-*`)

## Colour and theme

9. Status colours in dark mode: `success` was retuned to green 400 on dark schemes; is the status set (`info` follows secondary, `danger` follows error) acceptable as provisional? (SC blockers; FD, Semantic roles)
10. `warning` fails AA as text on the light schemes (1.8:1): a text-safe role or a darker light value? (SC blockers)
11. The Material adapter's future: keep it as the theme provider, or replace it with authored light and dark schemes? (FD, FOUND-012; SC, Layers)
12. A micro's own unlayered rules outrank the `user` layer, so a micro that hard-codes colours is not themeable; keep, or switch to "theme beats micro" (one line in `MountedSurface` plus an exemption for the lab)? (TL, Layers decision to confirm; SC blockers)
13. The web shell hardcodes dark mode (`ThemeMode::Dark`); how should light/dark and contrast variants be chosen and stored? (`src/daytxt_web/shell.rs`; FD, Semantic roles)
14. The shell chrome is not themeable on web: the CSP (`style-src-elem 'self'`) refuses the host `<style>`; planned fix is adopted stylesheets, or relax the CSP? (TL, Not done; SC blockers)
15. Component-level tokens (`--ct-button-radius`) have no authoring path in `x/design-tokens.ts`; add them, and when? (SC blockers; FD, FOUND-011)

## Motion and interaction

16. Motion and animation tokens: four durations and two easings exist; reduced-motion is defined as removal. Which named motions (enter, exit, fade) become normative, and exit animations for sheet and drawer (not possible on the wire today)? (IX, INTERACTION-004 and 007; KIT, Sheet and Drawer limits)
17. Focus rings are verified only by unit tests (the headless WebKitGTK check never showed `:focus-visible`); how to close the gap? (SC blockers)
18. Focus is neither trapped nor restored in `Sheet` and `Drawer`. (KIT, sheet and drawer docs)

## Guest wire and other

19. `aria-description` in the guest wire vocabulary, then a `description` prop on `Button`. (SC blockers; SC, What is left)
20. Directionality (right-to-left) and handing safe-area insets to guests: not designed. (SC, What is left; KIT, drawer docs)
21. The `reset` layer sits above Tailwind's `utilities`; revisit if a utility must override it. (SC blockers)
22. intxt (`micro_review`, still on Tailwind utilities): needs a monospace token (see 3) and a row that opens something, i.e. a `ListRow` usable outside a `ListDetails` scaffold (see 8). (SC blockers and What is left)
