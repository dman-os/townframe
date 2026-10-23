# Interaction

Interaction rules apply to reusable UI primitives. Product-specific workflows may add behavior without changing the foundation token vocabulary.

### Normative rules

- **INTERACTION-001** Interactive states MUST be distinguishable by more than color alone when color perception is insufficient; focus and selection MUST have an explicit visual treatment.
- **INTERACTION-002** Keyboard focus MUST remain visible and MUST use the focus semantic role or an explicitly documented equivalent.
- **INTERACTION-003** Hover MUST NOT be the only way to discover an action or important state. Touch and coarse-pointer input MUST have an equivalent usable path.
- **INTERACTION-004** Motion MUST use named duration and easing values when motion is part of the foundation contract. Reduced-motion behavior MUST be defined before motion tokens become normative.
- **INTERACTION-005** A component MUST NOT start repeated or unbounded visual work in response to pointer movement. State changes SHOULD be idempotent and bounded.
- **INTERACTION-006** Theme changes MUST preserve semantic roles. A theme MAY override token values, but it MUST NOT change the meaning of a role.

Renderer-specific event and scrolling limitations belong in implementation documentation and are not token semantics.
