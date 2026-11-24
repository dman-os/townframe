# The daytxt identity as a replaceable stylesheet

Status: proposal for the first foundations sitting. Nothing here is built.
Owner: claytxt lane.
Updated: 2026-11-25
Scope: how the app's look separates from the kit, so an identity is one stylesheet that can be swapped whole and a
user theme tweaks whichever identity is active. It is the frame for the foundations work in
`docs/design-language/`, driven through the theme lab's specimen board.

## Today

- `src/clay_dioxus_kit/styles/tokens/*.tokens.json` author the primitives and semantic roles.
  `x/design-tokens.ts` generates `tokens.generated.css` into `@layer tokens`.
- The colour roles are wired to Material. `tokens.css` sets each `--ct-color-*` role from a Material scheme
  variable (`--scheme-color-*`), which `material.css` generates from `material-theme.json`.
  - The scheme classes are `light`, `dark` and their medium- and high-contrast variants, on `ThemeRoot`, read at the
    root through `:has`.
  - Success/warning are patched per scheme by hand.
- The cascade inside a mount is `@layer host` (the kit, tokens included) < the micro's own unlayered sheet <
  `@layer user`... except that unlayered beats every layer, so today a micro's own rules beat the user theme
  (theme-lab note, "Decision to confirm").

So the look is spread over three sources (the token JSON, the Material adapter, per-scheme patches), and no single
file is "the daytxt look".

## Proposal: three layers

1. **Foundation: the kit and the theme API.**
   - The token *names* and what each means: the semantic roles (colour, type, space, shape, elevation, layer,
     motion) plus the primitive scales they may draw from.
   - The kit's component rules, which consume only semantic roles (COMPOSE-001).
   - Neutral fallback values, so a bare kit renders legibly with no identity loaded. Plain greys, one accent.
   - The complete list of roles an identity must assign is the **theme API**, documented in
     `docs/design-language/foundations.md` and checked by `x/design-tokens.ts`.
2. **Identity: the daytxt look, one replaceable sheet.**
   - `daytxt.identity.css`, in its own layer (`@layer identity`). It assigns every role in the theme API for each
     scheme (light, dark, and contrast variants), plus type families and scale, radii, elevation and motion.
   - Material stops being the provider: an identity is authored directly, and a Material-derived identity, if
     wanted, is just another identity sheet generated from `material-theme.json`.
   - Replacing the identity means loading a different sheet. Nothing else changes.
3. **User theme: tweaks on top.** The sheet the theme lab edits and applies, as today, overriding roles of whichever
   identity is active.

## Decisions for the sitting

1. **Is the semantic role list the complete theme API?** Today it is these groups:
   - 26 colour roles: canvas, the surfaces, text, border, focus, primary, accent, selected, scrim, and the status
     colours danger, success, warning and info;
   - spacing, target size, radii, border widths, elevation, layers, and motion durations and easing.

   Typography has only three sizes and no roles yet (session-prep 1). What should we add (text roles, a type scale,
   status roles) and drop?
2. **Layer order inside a mount.** Proposed, lowest first: kit < identity < micro < user.
   - A user's theme always wins.
   - An identity restyles every micro that uses kit roles.
   - A micro may still override the identity for its own parts, but never the user's theme.
   - This means the micro's sheet moves into its own layer (`@layer micro`). Today it is unlayered, and unlayered
     beats every layer. The theme lab's preview then needs its draft to sit above the applied user theme in the
     lab's own mount; one exemption.
3. **Schemes.** Is the scheme set light, dark, and medium- and high-contrast variants of each, as today? How is the
   scheme chosen and stored? (The web shell hardcodes dark today; session-prep 13.)
4. **Where the identity lives.** A file in the kit, loaded by the host before any micro? Or a document, like the
   user theme, so a different identity can be installed without a build?

## Then

Colour first. On the specimen board I bring two or three identity directions as complete identity sheets. Each
assigns the whole colour theme API for light and dark, with contrast checked against WCAG AA for every text role
on its surfaces. You pick or push back. The choice is written as the identity sheet plus `FOUND-xxx` rules, the same
day.
