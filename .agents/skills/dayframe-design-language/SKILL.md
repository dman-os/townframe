---
name: dayframe-design-language
description: Apply and extend the Dayframe design language when implementing or reviewing UI. Use for any dayframe_dx UI work involving tokens, Tailwind classes, components, screens, layouts, states, or bespoke visual requests.
---

# Dayframe design language

Use this skill for UI work in `src/dayframe_dx`. It is the design-system counterpart to `dayframe-native-dev-loop`, which covers running, screenshotting, and semantically inspecting the native app.

The design language is a maintained implementation contract, not background prose. New UI MUST use it, and a request that exceeds its expressivity MUST be made explicit before being silently implemented as an ad-hoc value.

## Read before changing UI

Read the repository `AGENTS.md`, then the relevant design-language documents:

- `docs/design-language/README.md` — entry point and vocabulary.
- `docs/design-language/foundations.md` — normative foundation rules and `FOUND-*` IDs.
- `docs/design-language/composition.md` — layout and composition rules.
- `docs/design-language/interaction.md` — state and interaction rules.
- `src/dayframe_dx/ui/styles/README.md` — stylesheet integration.

For token or Tailwind work, also inspect:

- `src/dayframe_dx/ui/styles/tokens/primitives.tokens.json`
- `src/dayframe_dx/ui/styles/tokens/semantic.tokens.json`
- `x/design-tokens.ts`
- `src/dayframe_dx/ui/tailwind.css`

For native build or visual verification, also read `.agents/skills/dayframe-native-dev-loop/SKILL.md`.

## Source-of-truth rules

- Authored token JSON is canonical. Generated CSS and bundled Tailwind output are derived artifacts and MUST NOT be hand-edited.
- Run `x/design-tokens.ts --check` after token changes. Regenerate with `--write`, then regenerate `src/dayframe_dx/ui/assets/tailwind.css` through the documented Tailwind command.
- Foundation tokens are application-agnostic. They MUST NOT contain tiles, columns, widgets, screens, routes, workflows, or other product-specific grammar.
- Keep the dependency direction `primitive -> semantic -> optional reusable component`. A primitive MUST NOT depend on a semantic or feature token.
- The showcase is curated executable evidence, not the normative source and not an exhaustive token catalog.
- Material theme JSON is an external compatibility/theme-provider boundary, not the canonical Dayframe foundation source.

## Vocabulary first

Before writing CSS or RSX, classify the request:

1. **Existing foundation** — use the existing semantic token and Tailwind vocabulary.
2. **Reusable composition** — implement a shared UI utility/component only when the behavior is demonstrated in more than one real context.
3. **Feature grammar** — keep product-specific structures in the feature's own stylesheet/module and label the boundary clearly.
4. **Missing design-language expressivity** — stop and surface the gap before inventing a silent one-off.

New code SHOULD prefer semantic utilities such as `bg-surface`, `text-foreground`, `border-border`, `rounded-md`, and token-derived spacing over raw values or compatibility variables. Existing compatibility aliases may remain during migration; do not expand a migration into unrelated files.

Do not use arbitrary Tailwind values merely because they are convenient. A raw value is acceptable only when it is already permitted by a normative rule, is intrinsic to a feature grammar, or has been explicitly approved and documented.

## When the language is not expressive enough

If a user asks for a bespoke treatment that the current language cannot express, do not quietly add a magic number, arbitrary Tailwind class, or undocumented component exception.

Make the gap explicit using this structure:

```text
Design-language gap
- Request: <the requested visual/interaction behavior>
- Nearest existing rule/token: <rule ID and token/class>
- Missing degree of freedom: <scale, state, relationship, geometry, or behavior>
- Proposed scope: foundation | reusable component | feature grammar
- Proposed rule/token: <smallest coherent extension>
- Alternatives: <at least one, when meaningful>
- Decision needed: <the concrete question for the user>
```

Then use `notify-desktop` when the decision blocks work, as required by the repository instructions. Do not answer a product/design question on the user's behalf.

Once approved, make the extension durable:

- Add or revise a concise normative rule with a stable ID.
- Add authored tokens only at the correct layer.
- Add generator validation/self-tests when the constraint is machine-checkable.
- Add or update curated showcase evidence when the rule needs visual proof.
- Keep feature-specific exceptions in the feature grammar unless the user explicitly chooses a foundation rule.

Normative documents MUST use precise `MUST`, `MUST NOT`, `SHOULD`, and `MAY` language. Do not turn implementation notes or generated inventories into normative prose.

## Current foundation policies

Treat these as established unless the documents are intentionally changed:

- `rem` is used for typography, scalable spacing, and normal radii/layout dimensions.
- `px` is used for borders, hairlines, renderer/layout boundaries, and shadow geometry.
- `em` is not a foundation scale; it is reserved for documented local proportional relationships.
- `dimension.radius.pill` is exactly `999px`, an intentional maximum-radius sentinel.
- Unsupported or unclassified token units MUST be rejected by the generator.
- Native/web root-font equivalence is not yet guaranteed; do not claim cross-renderer `rem` equivalence until the native root basis is addressed.
- Tiles are a separate feature grammar. Do not move tile concepts into foundation token files.

## Feature implementation loop

For each UI feature:

1. Identify the applicable rule IDs and semantic token paths.
2. Implement the smallest composition using the existing vocabulary.
3. Keep feature grammar separate from foundation grammar.
4. If a requested detail cannot be expressed, stop and raise the design-language gap before coding around it.
5. Add explicit semantic roles, labels, and states; native accessibility is part of the UI contract.
6. Validate the authored/derived pipeline and the narrowest affected Rust target.
7. For native UI, follow the native dev loop for screenshots and semantic inspection; do not infer native behavior from web CSS alone.

The goal is not to prohibit bespoke design. The goal is to ensure that bespoke design is either a deliberate, durable grammar extension or an explicitly bounded feature decision.

## Validation checklist

For token or styling changes, normally run:

```sh
deno check x/design-tokens.ts
deno run --allow-read --allow-write x/design-tokens.ts --self-test
deno run --allow-read --allow-write x/design-tokens.ts --write
deno run --allow-read --allow-write x/design-tokens.ts --check
tailwindcss -i src/dayframe_dx/ui/tailwind.css -o src/dayframe_dx/ui/assets/tailwind.css
cargo fmt --all -- --check
```

Then run the narrowest affected check, preferably `cargo clippy --all-targets --all-features -p <crate>`. Do not run the full workspace suite without a reason.

After edits, verify that:

- Generated files match the authored sources.
- No generated file was hand-edited.
- No arbitrary value bypasses an existing rule.
- New rules have stable IDs and concise normative wording.
- Foundation and feature grammar remain separate.
- Unrelated dirty working-copy changes remain untouched.

## Recently bitten by

- A design document alone is not enforcement; token-path unit rules need generator validation and self-tests.
- A generated custom shadow containing its own color can prevent Tailwind shadow color modifiers from working. Preserve colorable composition explicitly and preserve all intended shadow layers.
- CSS-only native changes may require the native dev-loop rebuild path because styles can be baked into the binary.
- The showcase is evidence, not permission to promote every showcase-specific value into the foundation.
