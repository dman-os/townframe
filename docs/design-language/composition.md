# Composition

Composition rules describe reusable visual primitives. They do not define application feature grammars.

### Normative rules

- **COMPOSE-001** A component MUST consume semantic foundation roles where a role exists; it MUST NOT reach into a raw palette solely because the raw value is convenient.
- **COMPOSE-002** Components SHOULD remain visually quiet by default. Borders, elevation, and filled surfaces MUST communicate a meaningful boundary or hierarchy.
- **COMPOSE-003** A component variant MUST be represented by an explicit component API, class, or data attribute. A token MUST NOT be created solely to encode a finite variant or interaction state.
- **COMPOSE-004** Components MUST use the shared spacing, shape, typography, and motion scales when those scales cover the need. A new scale value requires a documented reason.
- **COMPOSE-005** Foundation primitives MAY be composed by feature grammars. Feature grammars MUST NOT redefine foundation token meaning or add their vocabulary to foundation token files.
- **COMPOSE-006** The showcase MAY demonstrate a composition, but an example does not authorize a new foundation rule. New rules belong in this document and require a stable identifier.

The tile grammar is a feature grammar. Its `Tile`, `Widget`, `Column`, stack, and overlay concepts are deliberately outside the foundation token taxonomy.
