# Foundations

This document defines the platform-agnostic foundation layer. It does not define product features, screens, or widget grammars.

## Layers

The dependency direction is one-way:

```text
primitive values → semantic roles → reusable component tokens (when needed)
```

**FOUND-011** `primitive` tokens define scales and raw values. `semantic` tokens assign intent to those values. A component token MAY be added only when a reusable primitive needs a stable, themeable contract; a feature token MUST NOT be added here.

### Normative rules

- **FOUND-001** Foundation token files MUST use the DTCG token shape: a token is an object with `$value`; `$type` MUST be explicit or inherited and MUST NOT be guessed from a value's shape.
- **FOUND-002** A semantic token MAY alias a primitive token or another semantic token, but a primitive MUST NOT depend on a semantic or feature token.
- **FOUND-003** Foundation token names MUST describe a value, scale, or semantic role. They MUST NOT name tiles, columns, widgets, screens, routes, or product workflows.
- **FOUND-004** Token descriptions SHOULD explain unusual intent or constraints. Descriptions MUST NOT replace the normative rules in this document, and documentation MUST NOT be generated as one paragraph per token.
- **FOUND-005** Alias references MUST resolve locally and MUST be acyclic. Unsupported or ambiguous values MUST fail validation rather than being guessed.
- **FOUND-006** The supported token type set is deliberately smaller than the complete DTCG type catalogue. A type is added only when the project has a defined runtime mapping for it.

## Units and scales

The unit policy separates scalable design relationships from values that represent a deliberate renderer or geometry boundary. `rem` is relative to the root font size and avoids `em`'s nested compounding. `em` is relative to the current element's font size and is therefore appropriate only for deliberately local component relationships. `px` is a logical CSS pixel and remains appropriate for hairlines and explicit boundaries; it is not a claim about physical display pixels.

- **FOUND-007** Spacing and scalable layout dimensions MUST use explicit DTCG `dimension` values authored in `rem`. The base spacing value is `0.25rem` (4px at a 16px root); fine-grained values MAY use named smaller rem values. Components MUST consume a token or a documented Tailwind mapping rather than introducing an arbitrary spacing value.
- **FOUND-008** Borders, hairlines, and explicit renderer or layout boundaries MUST use `px` unless a documented component contract requires a different unit. Border width MUST NOT be made font-relative merely for consistency with spacing.
- **FOUND-017** The pill radius MUST use the `999px` sentinel. This is a maximum-radius shape sentinel rather than a scalable radius value, so it is the explicit exception to the rem radius policy.
- **FOUND-013** `em` MUST NOT be used as a foundation scale. It MAY be used for a component-local proportional relationship whose intentional dependence on the component's computed font size is documented.
- **FOUND-014** Shadow offsets, blur, and spread SHOULD use `px` initially so elevation geometry remains predictable while the shadow model is being established.
- **FOUND-015** A generator MUST preserve each authored unit unless a documented target adapter performs an explicit conversion. Unit conversion MUST NOT be implicit.
- **FOUND-016** Native and web roots MUST establish an equivalent rem basis before the project claims that rem-based tokens have equivalent cross-platform scaling. Defining that native root behavior is an implementation concern, not a reason to change the foundation token policy.

## Semantic roles

Semantic color roles describe purpose, not palette membership. The initial roles are canvas, surface, text, border, focus, primary, accent, and danger where a mapping is established. Success and warning mappings remain provisional until their visual and accessibility semantics are reviewed.

**FOUND-012** Material palettes and schemes are external theme inputs. Their group names MUST NOT be treated as Dayframe semantic roles by tooling.

## Tailwind output

**FOUND-009** Tailwind remains the utility grammar, but its theme aliases MUST be derived from the same semantic token vocabulary. A generated alias MAY translate a token path to Tailwind's required syntax (for example, a token path may become a kebab-case CSS custom property); the mapping MUST be deterministic and documented. Raw palette utilities are not a substitute for semantic roles in shared components.

## Renderer boundary

**FOUND-010** Blitz, webview, Android, and CSS compatibility limitations are implementation constraints, not foundation semantics. They belong in platform or build documentation and MUST NOT redefine token meaning.
