---
name: lexicon-authoring
description: Author big_cabinet_core datums (Lexicons) that stay forward/backward compatible — open unions and string vocabularies, the empty-member convention, Extra/UnknownMember carrying, member identities, and the declaration→publication→admission→codec test chain. Use when adding or changing a datum type, a union, or an enum in big_cabinet_core, or when a datum fails admission.
---

# Lexicon authoring for Cabinet datums

The rule that decides most of this: **carrying is not operating** (docs/wiki/docs.md
§Evolvability). A node that cannot interpret a value must still read, store, list and
round-trip it untouched. Refusal belongs at the operation that needs the interpretation,
never at decode, storage, or projection.

## Defaults

- **Objects are open.** `#[serde(flatten)] extra: crate::datum::Extra` on every datum and
  member. Never `#[serde(deny_unknown_fields)]` on a type that also flattens `Extra` — the
  macro refuses that combination because it would reject what it claims to preserve.
- **Unions are open.** `#[macros::datum]` (not `closed`) on enums, with exactly one
  `#[datum(other)] Unknown(crate::datum::UnknownMember)` variant. The macro writes serde and
  the schema; do not derive `Serialize`/`Deserialize`/`JsonSchema` on an open union.
  Closed is the exception and needs a stated reason at the site (e.g. a vocabulary that is
  another system's closed enum, mirrored — not extended — here).
- **String vocabularies are open** when a peer may extend them: `#[macros::datum(open_string)]`
  with an `#[datum(other)] Unknown(String)`. A closed vocabulary (plain enum + serde/schemars
  derives, no macro) is for values this node itself grants/interprets; an unknown one is a
  refusal at the point of use, and its admission path needs the `enum` supplement support.
- **A member with nothing to say carries `empty: true`.** A Lexicon object states at least
  one property, so a property-less member cannot publish. Convention: an optional
  `empty: Option<bool>` field, emitted as `Some(true)` by the type's constructors and
  skipped when `None`. Do not invent a one-variant vocabulary or a dummy field to satisfy
  the object rule; future fields land beside `empty` without a new tag.
- **Members are identified by their datum id.** A union member payload declares
  `#[macros::datum(id = "org.example.…")]`; the union references it by that id. Adding a
  member is a new payload type under the same enclosing datum id — never a new datum tag
  for the enclosing datum.
- **Service-side closed mirrors are fine.** Operations need a closed vocabulary to switch
  on (`LayoutKind` beside the open `DrawerLayout`). Serde for the mirror spells the same
  wire form; conversion (`known()`) is the one place foreign values become `None`, and
  callers must handle `None` as "carried, not operated" — not as an error at read time.

## Storage follows the same rule

A projection must not reject what the datum carries. Store open datums verbatim
(serialized) rather than decomposed into columns with CHECK constraints on tags or
vocabularies; interpretation happens at read time. A SQL `CHECK (layout IN (...))` is a
closed union wearing a database costume.

## The test chain (all four, per datum)

1. **Declaration**: `Declaration::of::<T>()` succeeds (alignment gate).
2. **Publication**: the type appears in `Universe::collect([...])` beside the datums that
   reference it; union members are declared too — admission requires their lexicons
   carried.
3. **Admission**: `Bundle::validate_lexicon(id, file, &carried_others)` succeeds for every
   published lexicon, with all others carried (this is what service boot does).
4. **Codec round-trip**: an unknown member / unknown field / unknown vocabulary value
   decodes, re-encodes byte-identically, and the operating seam reports it as unknown.
   Also: the known values still decode to the known variants.

A datum that only passes (1)–(2) is not done: the failures this chain exists to catch
(union supplements, member/hoist disagreement, discriminant-blind authoring) all live in
(3)–(4).

## Known machinery seams (already fixed; do not re-derive)

- Union supplements are admitted in `datum/input.rs`: branches match by `$type` `const`,
  the fallback branch (discriminant `not`) carries nothing, and a string vocabulary's
  `enum`/`const` must restate the primary's exactly.
- A union member's own declaration and the copy hoisted into a referencing datum's `$defs`
  differ by the datum document frame (`$schema`, `title`); `Universe::collect` screens
  hoists against datums by name with the frame stripped.
- `authoring_matches` short-circuits a union branch whose `$type` `const` does not match
  the value's discriminant; without it every member is walked and "another member rejects
  it" reads as ambiguity.
