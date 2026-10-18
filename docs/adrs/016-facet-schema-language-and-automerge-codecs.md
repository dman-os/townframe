# ADR 016: Facet schema language, conventions, and Automerge codecs

**Status:** Accepted

**Related:** ADR 007 (doc and branch identity), ADR 014 (drawers), ADR 015 (document format and facet
writes — §2 codec declarations, §4 claim canonicalization, §6 verification).

## 1. What a facet schema has to do, and the three layers it is made of

A facet has three related representations with different jobs:

- **Automerge storage** preserves collaborative semantics: `Text`, `Counter`, object
  identity, lists, timestamps, bytes, and scalar kinds. It is not constrained to JSON's
  value distinctions.
- **Lexicon describes canonical JSON.** Rust-authored facets emit values compatible with
  the ATProto data model by default. The facet tag identifies the Lexicon definition.
- **Sidecar JSON Schema** supplies the supported supplementary constraints and
  `x-automerge` storage annotations that Lexicon cannot express. It may narrow a Lexicon
  `unknown` map but never admit a value outside the canonical data model.

`schemars` remains the Rust authoring mechanism for the sidecar schema and storage walk.
The declaration macro checks that the Rust wire representation, Lexicon-facing shape, and
sidecar declarations agree or refuses the type. SchemaRepo identifies a facet Lexicon by
its tag and makes its sidecar bundle available to Cabinet; discovery and publication of
those bundles are embedder concerns.

## 2. The use cases we actually have

The facet vocabulary that exists today is small enough to enumerate, and it sets the floor for what
the schema language must express. Daybook-owned facets under `org.example.daybook.*` are dmeta,
refGeneric, labelGeneric, titleGeneric, pathGeneric, pending, body, note, blob, blobPin,
imageMetadata, ocrResult, embedding, plugManifest, plugsConfig, branch and branches. Plug-owned
vocabularies are plabels (pseudo-label, pseudo-label-error, pseudo-label-candidates) and dayledger
(claim, txn, account, meta). A facet whose schema is not installed arrives as `FacetRaw`.

| need | today |
| --- | --- |
| identifiers as strings | `DocId`, `Uuid`, and `FacetKey`, which serializes as one `tag/instance` string |
| hashes | `Multihash` for blob digests; `ChangeHashSet` for Automerge heads |
| urls with a frontier | `db+facet:/<docId>/<tag>/<instance...>` plus a separate heads field |
| timestamps | `Dmeta.createdAt` and `updatedAt: Vec<Timestamp>`, unix seconds, carrying a `FIXME: unix timestamp codec` |
| integers | `Blob.lengthOctets: u64`, `ImageMetadata.widthPx`, `Embedding.dim: u32` |
| bytes | `Blob.inline: Option<Vec<u8>>`; `Embedding.vector: Vec<u8>` with an `EmbeddingDtype` |
| maps keyed by string | `HashMap<String, UserMeta>` and `PlugsConfig.enabled` |
| maps keyed by an identifier | `HashMap<Uuid, FacetKey>` and `HashMap<FacetKey, FacetMeta>` |
| arrays, nested objects | `Body.order`, `updatedAt`, `OcrResult.textRegions`; `FacetMeta`, `UserMeta`, `KnownPlug`, `OcrTextRegion` |
| open strings, closed sets | `MimeType`, `modelTag`, `compression`; `EmbeddingDtype { F32, F16, I8, Binary }` |
| references | `facetRef: Url` together with `refHeads: ChangeHashSet`; weak branch doc references |
| text | `Note.content`, `OcrResult.text`, chat transcripts |
| unions of named types | facet values themselves (`WellKnownFacet` or `FacetRaw`), the profile example's attachment, plabels' three variants |
| non-integer numbers | `OcrTextRegion.confidenceScore: Option<f32>`, OCR bounding boxes, plabels' `topScore: f64` |

Three consequences shape the rest of this document.

**Typed maps are load-bearing.** Dmeta includes `FacetKey -> FacetMeta`. JSON
Schema expresses this directly as `{"type":"object","additionalProperties": <schema>}`, with the
keys necessarily strings, which is also what Automerge's `Map` allows. `FacetKey` and `Uuid` already
produce their string form, so nothing is strained.

**A facet URL names a facet; an optional sibling `refHeads` selects a frontier.** The URL uses `db+facet:/<doc-id>/<tag>/<key-id...>` with no authority component. `self` resolves against the containing doc, and the entire path remainder after the tag is the key ID.

**Non-integer numbers appear in exactly three places**, all of which already have a better
representation, and none of which is a reason to remove `F64` from the storage model.

## 3. Canonical ATProto-compatible JSON and the sidecar

Canonical facet JSON uses the ATProto data model rather than unrestricted JSON Schema.
This gives Rust-authored facets one outgoing spelling that can travel through ordinary
Lexicon-aware systems. JSON Schema remains necessary as a sidecar because storage forms,
typed dynamic-map refinements, and several validation constraints are not Lexicon
semantics.

The canonical rules include:

- signed integers restricted further to JavaScript's exact safe range,
  `-(2^53 - 1)..=(2^53 - 1)`;
- declared floats emitted as decimal strings, though declared float positions may accept
  JSON-number input and normalize it;
- inline bytes emitted only as `{"$bytes":"..."}`;
- protocol-defined `$type`, `$bytes`, and `$link`, while application-defined `$` keys are
  rejected — recursively, at every depth, including inside a position no schema describes, and
  at both the codec's edge and the Lexicon projection;
- `unknown` as the broad object-oriented escape hatch: Lexicon has no map type, so a typed
  dynamic map projects as `unknown` and the sidecar's `additionalProperties` narrows it. The
  narrowing has a floor — the sidecar may say less than Lexicon, never admit a value the
  canonical data model does not have — so the value type is projected for its alignment and
  then dropped;
- an undeclared fractional number is *accepted* at a position no schema describes and stored as
  the `F64` it is. It is not canonical JSON and it is never emitted: a declared float position
  writes the decimal string. Be liberal in what arrives, canonical in what leaves;
- open vocabularies represented by `knownValues` and open unions preserving unknown
  tagged members;
- internal `$type` tagging as the only published union layout. Adjacent and external tagging are
  representable in the sidecar and refused at both the declaration and the publish (§5).

Rust-authored types do not serialize alternative spellings. Foreign base64, key mappings,
renaming, or wrapping belong to an explicitly selected adapter outside ordinary facet
reconciliation. Legacy plain-base64 input is not accepted by the canonical path.

Facet tags are NSID-shaped identifiers, but this decision does not adopt ATProto record
keys, TIDs, PDS repositories, DNS resolution, or XRPC. Those are independent publication
and node-interface designs.

## 3a. Enforcement is rejection, not rewriting

A macro in this design states a fact or refuses a declaration. It never transforms what the author
wrote, because a transformation is a second, invisible spelling of the same thing: the moment a macro
rewrites a name into a value, the name in the source and the value on the wire can drift, and nothing
in the source says which one is real.

So `#[facet(open_string)]` derives a value's spelling from its variant name — `MyValue` is
`"myValue"` — and **refuses** a variant it cannot derive from, rather than mangling it. There is no
case-conversion machinery behind it: `#[serde(rename_all = "camelCase")]` is read because it agrees
with the rule, any other case is refused with a message saying so, and the one override is an explicit
`#[serde(rename = "…")]`, which is a statement rather than a rule. The attributes the macro reads are
consumed, because on a type whose wire form the macro writes nothing registers `serde`, so an
attribute left behind is a compile error rather than a silent no-op.

The same reasoning is why a union member's tag is its id rather than a variant rename (§5), and why
`check_shape` compares a declaration with the schema a *different* mechanism (`schemars`) produced
instead of trusting either one.

## 4. Objects are named, and their openness is stated

**An object is always a named type.** Its schema is a `$defs` entry, so a peer can point at it, a
union member can take its id, and a `$ref` from another schema can reach it. Even an object used at a
single field is named, because the alternative is an inline object whose fields nothing can
reference.

**Openness is stated by the type, not inferred.** An open object has exactly one
`#[serde(flatten)]` overflow field of type `Extra`; its presence is the only thing that makes
`schemars` emit `additionalProperties: true`. A closed object has no such field, carries
`#[serde(deny_unknown_fields)]` — which is what makes the schema emit `false` and what makes the type
reject an unknown member — and says `#[facet(closed)]`. Nothing else is accepted, because the states
in between do not round-trip. Measured:

| declaration | `decode({"a":1,"b":2})` | emitted schema |
| --- | --- | --- |
| plain struct, nothing declared | **accepts, then drops `b`** | no `additionalProperties` at all |
| `#[serde(flatten)] extra: Extra` | accepts, preserves `b` | `"additionalProperties": true` |
| `#[serde(deny_unknown_fields)]` | rejects | `"additionalProperties": false` |
| both | **rejects `b`** | `"additionalProperties": false` |

The first and fourth rows are the failure this ADR exists to prevent, and the first is the important
one: **a missing `additionalProperties` reads as permissive, so the schema claims the object is open
while `serde` silently drops every member the type does not name.** A correctly open type and an
accidentally closed one differ only in a key the second one omits. That is why the publish-time check
requires `additionalProperties` to be *present*, rather than reading its absence as open, and why
the declaration macro refuses a type that states neither.

The fourth row is why `deny_unknown_fields` beside an overflow field is a compile error rather than a
warning: the deny wins on both the wire and in the schema, so the declaration would be asserting an
openness it does not have, and nothing downstream could tell.

`Extra` is an `IndexMap<String, Value>`, not `serde_json::Map`: nothing in the workspace enables
`serde_json`'s `preserve_order`, so that map is a `BTreeMap` and a pass-through rewrite re-sorts a
peer's members. Signed bytes do not depend on this, since JCS and DRISL both canonicalize key order
before hashing; display fidelity does. The union fallback wraps one of these rather than being one
(§5), so that a member the reader does not know is a type that says so instead of a bare map.

## 5. A union is a position, not a name

**A union is never a named schema definition.** It is inlined where it is used, as a `oneOf` whose
members are `$ref`s to the member definitions. Nothing else may reference it, which makes a union of
unions unrepresentable rather than merely forbidden.

**A union member is a named type with its own id.** The tag is the member's id, not the variant's
local name, so the same member carries the same tag at every site and the variant identifier never
reaches the wire. The id is declared, and it is the only thing that makes a type usable as a member.

**Internal tagging is the convention**: `{"$type": "<member id>", ...payload fields}`. Off-the-shelf
lexicon members already carry `$type` on the object, so that is the layout a published declaration
aligns to. The discriminant is always the `$type` const on the inlined branch. There is no `value`
key. Adjacent tagging is the explicit wrapper for a payload that is not an object, pinned as
`{"$type": "<member id>", "value": <payload>}`. External tagging is recorded and flagged, and it is
not lexicon-aligned. A published declaration refuses a union that is not internal.

The earlier case for adjacent tagging — every payload shape, a closed payload left untouched, no
tag-key collision, a flatten into AT Proto, and `$type` outside the payload — is why adjacent stays
representable. It is not the default. Internal deserialize strips `$type` before the payload is
read, so a closed payload's `deny_unknown_fields` still holds and an open payload's `Extra` does
not capture the discriminant.

**An open union has a fallback member; a closed union does not and says so.** The fallback holds the
member object verbatim, so a round trip through a node that does not know the type is lossless, and
a peer that does know it still gets its data. The fallback schema is `$type` as a string whose
`not.enum` excludes the known ids, with no const. Under internal tagging that object is the member,
so the fallback payload is `UnknownMember`, a newtype over the same `IndexMap` under a name that
says what it is and an `id(tag)` that reads the discriminant back. It is not a bare `Extra` because
the *meaning* is the point: an overflow field and a member nobody declares are different things, and
only one of them is an error to find in the wrong place.

Closedness is the default in the ecosystem and has to be opted out of, not into: a `oneOf` has no
branch for an unlisted discriminant, and so does a Rust enum derived with `serde`. That is the
opposite of the object case, where the type is accidentally *open* in the schema and closed in
Rust, and it is why the two need separate flags rather than one rule.

### A vocabulary of strings is the third shape

Objects and unions are both sets of *named types*. A string vocabulary is not: its values are
strings, so there is nothing for a peer to point at and no type behind a value whose name could be
the value. It is declared `#[facet(open_string)]`, with `#[facet(other)]` on the one variant carrying
a `String`, and the macro writes the wire form and the schema the same way it does for an open union —
for the same reason: a fallback that keeps an unrecognized value is not expressible as a serde
attribute.

A known value's spelling is its **variant name in lowerCamel**, derived rather than written, with an
explicit `#[serde(rename = "…")]` as the only override (§3a).

Its schema is `{"type": "string", "knownValues": [ … ]}` rather than an `enum`, which is the point:
`knownValues` lists what a reader knows and constrains nothing else, while `enum` would make every
future value an error. A *closed* vocabulary of strings declares nothing at all — it is a plain
derived enum, and serde and `schemars` cannot disagree about the values of an enum whose variants
both of them read, so there is no shape for the macro to check.

## 6. What serde can and cannot express, measured

For a **closed** union, serde still derives `Serialize` and `Deserialize`. The macro injects
`#[serde(tag = "$type")]` when the author did not, and it emits `JsonSchema` itself: schemars' enum
encoding is not the contract. The emitted `oneOf` inlines each payload's properties beside a `$type`
const. Nothing is a `value` wrapper.

For an **open** union, no serde attribute expresses the fallback, and this was measured rather than
reasoned about:

| declaration | `decode({"$type":"future","value":{"x":1}})` |
| --- | --- |
| adjacent, last variant is a newtype | `Err(unknown variant \`future\`, expected …)` |
| internal, last variant is a newtype | `Err(unknown variant \`future\`, expected …)` |
| `#[serde(other)]` on a unit variant | `Err(invalid type: map, expected unit variant …)` |

A newtype variant last is not an implicit catch-all, whatever the internals read like. `#[serde(other)]`
*is* the catch-all and does match an unknown discriminant, but it must be a unit variant, and then the
adjacent content cannot be deserialized into it — so the discriminant is caught and the value is
unreachable.

So an open union carrier gets three hand-written impls: `Serialize`, `Deserialize`, and `JsonSchema`.
`Serialize` is not optional, because the fallback's discriminant is data and a derive would write the
*fallback variant's own name* instead; `JsonSchema` is not optional, because a derive emits branches
for declared variants only.

**The logic is written once, in `big_cabinet_core::facet`, and the carrier gets three generated shims
that call it.** It belongs in the core because nothing in it knows a daybook type: the shape
vocabulary, the conventions and the codec are about how a declaration is written and stored, and
every daybook type is a facet declared *against* them. So `daybook_types` depends on
`big_cabinet_core`, never the reverse, and the daybook-side names (`FacetTag`, `WellKnownFacetTag`,
`FacetKey`, `FacetManifest`) are the ones that move onto it as they are settled. The shims are emitted in the crate that declares the carrier, and that is forced:
a blanket `impl<C: Carrier> Serialize for C` in our library is an orphan-rule violation, because the
self type is a bare type parameter. `impl Serialize for MyUnion` is legal only where `MyUnion` is
local. Per-carrier shims of three lines each, over library functions, keep the codec written and
tested once; every other type in the system keeps its derives.

This is the same shape the repository already ships: `define_enum_and_tag!` hand-writes
`Serialize`/`Deserialize` for its tag enum today because the wire form it needs is not what a derive
produces.

**The carrier is a named enum, not a library tuple type.** Both were considered. They are identical
on the wire and in the schema — both inline, both dispatch on the member's id, both need the same
library code — so the choice is purely Rust ergonomics. A library `AnyOf<(A, B)>` cannot offer
per-member `From` impls, because two generic impls for one container unify when the two member types
are equal and are therefore rejected by coherence; construction and access degenerate to positions
(`at::<0>`, `get::<0>()`), and variant names disappear from docs, error messages and the manifest's
member list. The cost of keeping names is three generated lines per union.

**Consequence worth having:** once our impls exist for a carrier, `#[serde(…)]` on that type becomes
an unregistered attribute and the compiler refuses it, so `rename_all`, `alias`, `skip`, `other`,
`untagged` and `with` stop being writable there rather than needing to be checked for. For the types
where serde still owns the wire, those attributes remain available and remain meaningful, and the
checks in §10 are what catch the ones that would contradict a convention.

### How a carrier states its members

The id is declared on the **member type**, never on the variant, so the same type carries the same
tag wherever it is used and a variant needs no rename at all:

```rust
#[facet(id_prefix = "org.example.daybook.")]     // + the type name, lowerCamel: `…daybook.note`
#[facet(id = "org.example.daybook.note")]        // exactly, for a key that is not the type name
```

`id_prefix` derives `prefix + lowerCamel(TypeName)`, which is the string `define_enum_and_tag!`
already builds, so the tag a persisted facet key holds does not change when a type moves to this
declaration. `FacetTag::from(id)` resolves that string to the well-known tag it names, so one id
serves as both the `$type` value and the key tag, and neither is written twice.

That splits the two cases cleanly:

- **Closed**: serde owns the wire form, so every variant states its tag with
  `#[serde(rename = "…")]`, and the macro emits `const _: () = assert!(str_eq(rename, <P as
  FacetMember>::ID))` — spanned on the variant, checked by the compiler. A tag and an id cannot
  drift, and a closed union needs no codec.
- **Open**: the macro writes the wire form, so the tag is `P::ID` and there is no literal to drift
  from. The carrier must *not* derive `Serialize`, `Deserialize` or `JsonSchema`, because the macro
  emits them, and every other `#[serde(…)]` key on it is refused with the keys that *are* read.

An open union also cannot be externally tagged: external tagging has no discriminant key for the
codec to dispatch on, so the fallback could not be found.

## 7. Where names are required

> **A name is required when something has to point at the thing from outside the position where it
> is written.**

This one rule derives the whole composition story:

- **Objects are named**, always. A `$defs` entry is how a peer reads a schema, a union member's id
  comes from one, and a `$ref` needs one.
- **Unions are never named.** A position needs no id and must not be pointed at.
- **Everything else is structural**: maps are `additionalProperties`, arrays are `items`, scalars and
  codecs are a keyword at their position, optionals are the absence of `required`.
- **A reference names its target and is itself structural.**

The useful consequence is that **no nested declarations are needed anywhere.** Everything that needs
a name is declared at the top level; everything else is written at its position. The nested payload
bodies in `define_enum_and_tag!` were a consequence of declaring variants' payloads inline; under
this rule they are ordinary top-level types that the union names.

## 8. Numbers

**`F64` stays in the storage model.** Automerge has it, the vocabulary above contains three
non-integer fields, and removing it would be a storage-model change to serve a transfer-format
concern.

The transfer concern is real but narrower than it first appears. Two nodes writing "the same" float
produce different Automerge changes regardless — different actors, different hashes — so
byte-identical values were never a goal, and a claim is signed over the value each writer stored. An
epsilon comparison would therefore be a churn suppressor rather than a correctness guarantee, and it
would make writes lossy in a way that has to be declared per field. It is not adopted: there is no
epsilon mechanism in the core.

What does matter is a proxy that parses and re-serializes JSON in between. A conformant parser
preserves a JSON string byte for byte, whereas a number survives only if the parser holds binary64 or
better and prints the shortest round-trip form — a parser that goes through f32 or a fixed-precision
decimal changes the value, and a reformatter may too. So the rule is:

> **A value that must be exact declares an exact transfer representation** — an integer, a decimal
> string, or bytes — and one that is merely compared approximately may be a `number`.

That is a convention on our vocabulary, not a language restriction, and it is now real: a float
position spells the decimal string when its value's type is `facet::Float32` or `Float64` (§9), and
a bare `f32`/`f64` field is the opt-out — the plain number, stated by the Rust type itself. A plain
number is not canonical JSON, so a facet that carries one is a codec document rather than a
publishable declaration: it assembles for the walk and is refused by the Lexicon gates (§10).

## 9. How a value is stored is the field's type

ADR 015 §2 requires each definition to declare its Automerge representation, and says new definitions
must declare rather than rely on field-name suffixes. The current implementation does the opposite:
`am_utils_rs::codecs::json` chooses the representation from the field name — a name ending in
`Base64` becomes a byte vector, one ending in `_at`, `_ts`, `At` or `Ts` becomes a `Timestamp`,
integers that look like milliseconds are divided by a thousand — and it is 1343 lines including its
tests.

One gap in it is worth naming because it is the strongest argument for declaration: on the read side
it hydrates an Automerge `Text` object to a JSON string, but on the write side a JSON string always
becomes a scalar `Str`, because the only call the writer makes for a string is `reconciler.str`.
**Collaborative text is therefore readable today and not writable**, and no suffix can fix that,
because the choice is not visible in a name.

What replaces it is a bounded vocabulary of storage forms, declared at the position each describes:

| JSON | the document holds | declared by |
| --- | --- | --- |
| `{"$bytes":"..."}` | bytes | the `facet::Bytes` Lexicon/sidecar declaration |
| an RFC 3339 string | a timestamp in milliseconds | `format: "date-time"` |
| a number | a counter | `x-automerge: "counter"` |
| a string | a collaborative text | `x-automerge: "text"` |
| a decimal string | a float | `x-automerge: "float"`, which `facet::Float32`/`Float64` emit |

The timestamp row uses JSON Schema's standard `format: "date-time"` — the one standard keyword the
vocabulary uses. Canonical bytes are the `{"$bytes": "…"}` object and nothing else: `contentEncoding`
is an annotation, and the codec never sniffs a plain base64 string into bytes, because a reader that
trusted the sniff would store a string the writer meant as a blob. `facet::Bytes` supplies the
matching Lexicon, serde, and sidecar storage declaration. Counter and Text merge behavior, and the
float storage convention, use `x-automerge` because neither Lexicon nor ordinary JSON Schema
describes those native semantics. A bare `f64` derives a plain number only through the explicit
authoring opt-out described above.

A type declares the complete representation it owns. No registry guesses from a field name, and
neither `chrono` nor an arbitrary bytes type gains native storage merely because its JSON happens to
look similar. Map and List remain ordinary Automerge objects; unsupported native shapes are not
exposed as authoring shortcuts.

The boundary is three data models, and each one states its own part:

| | stated by |
| --- | --- |
| the Rust type, and its storage form | `autosurgeon`'s `Reconcile`/`Hydrate` impl for that type |
| the JSON form of a value | serde, and `schemars` deriving from it |
| what a peer reads it as | the storage form the schema declares |

That is why the codec says nothing about JSON: serde already says it. `Vec<u8>` is an array of numbers
to serde and bytes to Automerge, and the declaration is the only part serde cannot state.

**A value is checked before anything is written, and the check is the type state.**
`Stored::check(document, value)` walks the value and the schema together and refuses a value the
declarations cannot hold — a malformed `$bytes` object where the schema says bytes, a number where
it says text — naming the position. What comes back is a `Stored`, whose `reconcile` cannot fail on the
value's account: the conversions inside it are `expect`s, because the check is what makes them total.
This is "asserted rather than sniffed" made structural, and it is exactly where the old codec is soft:
`json.rs` logs a warning and stores an undecodable base64 field as a string, so a reader that trusts
the suffix gets a different type than the writer wrote.

The walk follows the value and the schema together: a `$ref` resolves in the document's definitions,
an `anyOf` narrows to the branch the content's kind fits (`Option<T>`), `items` declares a list's
elements, `additionalProperties` declares the members a schema does not name, and everything else is
plain JSON. Two compositions are **refused** rather than walked in part — `allOf` and `prefixItems` —
because a declaration hidden inside one would be stored as a plain value without a word. `schemars`
emits neither for any shape this facility declares.

**`autosurgeon` is the codec registry, and it is our fork.** Its `Reconciler` addresses every
Automerge shape per type — `bytes`, `timestamp`, `str`, `u64`, `i64`, `f64`, `boolean`, `none`, `map`,
`seq`, `text`, `counter` — and the fork already ships `Text`, `Counter`, `ByteVec`, `ByteArray<N>` and
`Uuid` with both impls. The schema declares the *meaning*; the reconciler's method performs the
*conversion*. Two consequences that decided the code's shape:

- **`Vec<u8>` cannot be bytes.** `autosurgeon` reconciles `Vec<T>` as a list, so bytes need a type of
  their own: `facet::Bytes`, which carries the JSON form, the schema and the storage, where the
  alternative is a `serialize_with`, a `deserialize_with` and a `schema_with` on every byte field.
- **A `Text` is not a string assignment.** Reconciling a hydrated `Text` against heads that have moved
  returns `StaleHeads`, because a splice is relative to the text you hydrated. A JSON string is a
  different thing: `TextReconciler::update` "calculate[s] a diff between the current value and the new
  value and then convert[s] that diff into calls to splice", which automerge documents as the best
  available answer when the edits were not captured as they happened — and a JSON document is exactly
  that case. So a string at a text position writes the difference, and an editor's spliced `Text`
  writes the edits themselves, which merge better.

### A number is stored as the schema says, because JSON has one number type

JSON Schema draws the distinction Automerge needs — `integer` and `number` are different types, and
`format` states the width and the sign — and `schemars` emits both for the Rust types that matter:
`f32` is `{"type": "number", "format": "float"}`, `f64` is `"double"`, `i64` is
`{"type": "integer", "format": "int64"}`, `u64` is `"uint64"`
(`schemars/src/json_schema_impls/primitives.rs:63-65`). Automerge's scalars make the same
distinctions: `Int`, `Uint` and `F64`.

What the JSON token says is not enough, and the failure hides in both other directions. Measured: one
JSON `1` sent to three positions declared `int64`, `uint64` and `float` is stored as `Uint(1)` in all
three, because a token that looks like an integer is written as unsigned. The document's scalar type
is then the token's and not the field's — and `autosurgeon` reads an `i64` from `Int` only and a `u64`
from `Uint` only, so hydrating that document **into the Rust type it came from fails**
(`Unexpected(Uint)`) while hydrating it to JSON returns `1` and looks perfectly correct. The same
ordering (`as_u64` first) is in `am_utils_rs::codecs::json`, so an `i64` field written through the
daybook's codec today cannot be read back into an `i64`.

So the position's schema decides: `type: number` is an `F64` whatever the token looks like,
`type: integer` with `format: uint*` is a `Uint`, any other `type: integer` is an `Int` — that type
includes the negatives, and a schema that means unsigned says so — and a position that declares no
type at all keeps the token's kind, because there is nothing else to go on. The type is read whether
it is written as one type or as a list of them, since `Option<i64>` is `["integer", "null"]`.

Two things this deliberately is not. It is not validation: a string at a `type: number` position is
the validator's business, and the assembled document is exactly what a validator consumes — the walk
decides how an admissible value sits in a document, not whether it is admissible. And it is not a
precision decision, because `f32` and `f64` are one `F64` in a document: the JSON number is where the
precision is stated, serde writes a float at the precision of its own type, and both round trip
exactly. `format` states which one the Rust side is.

**A document can hold a number JSON cannot state at all.** `ScalarValue::F64` holds any sixty-four
bits, and `serde_json::Number::from_f64` refuses NaN and the infinities, so a peer can leave a value
in a document that no JSON document can carry. The first version of this code asserted "a document
holds a finite number", which is false, and would have panicked on it; it is now a refusal that names
the value.

### The unit a timestamp is stored in is milliseconds, and the daybook writes seconds

`ScalarValue::Timestamp` is "a 64 bit integer which is milliseconds since the unix epoch"
(`automerge-0.11.0/src/lib.rs:48`), and the format encodes that integer unchanged
(`columnar/encoding/properties.rs:171`). A millisecond is therefore what a document holds, so the
codec writes `instant.as_millisecond()` and reads back with `from_millisecond`, and an instant finer
than a millisecond — every `Timestamp::now()`, and nothing that comes from JavaScript — keeps its
millisecond and loses the rest, because that is the precision the storage has.

`am_utils_rs::codecs::json` does the opposite: it writes `raw / 1000` and reads with
`Timestamp::from_second`, and its own test round trips `Timestamp::from_second(...)`. So the daybook's
documents hold **seconds where the format defines milliseconds**, which a format-conformant reader —
automerge-repo, the JavaScript API's `Date` — reads as a date a thousand times too early. That the two
directions agree with each other is precisely why it has gone unnoticed. §12 carries it as a
migration, since converting existing documents belongs with the daybook's migration and not with this
facility.

### A float is a decimal string, declared by its value's type, and a bare number is the opt-out

A float position that wants the convention carries it the way every other storage form does: **the
declaration is the value's type's**. `facet::Float64` and `facet::Float32` are newtypes in the codec,
in the same shape as `Bytes` — their own `Serialize` and `Deserialize` spell and read the decimal
string, their own `JsonSchema` says `{"type": "string", "x-automerge": "float"}`, inlined with
`format` naming the width for the reader that cares — so a field of that type spells strings in
*every* JSON the type takes part in: the repo's emission, the wire between peers, the plug's
payloads. One spelling, everywhere.

**The spelling is the shortest round-tripping decimal of the value** — `"0.1"`, `"30.0"` with the
point kept, so a reader knows it was never whole — and an `f32`'s is spelled at its own width, so a
value never reads back as the `f64` it widened into on the way through a JSON value. The three
non-finite values — which JSON's number cannot state at all, and which serde writes as `null`,
silently turning `Some(NaN)` in an `Option<f32>` into `None` — travel by their names, `"NaN"`,
`"Infinity"`, `"-Infinity"`, borrowed from protobuf's JSON mapping; JS parses all of them natively,
and `Float64`'s `Deserialize` reads them back, so with a declared type a non-finite value round trips.

Why the convention at all, in the order it weighed:

1. **Lossy JSON clients can turn a read-modify-write into an unintended edit.** DocumentsRepo owns
   Automerge; ordinary plugs, XRPC clients, and future local IPC clients exchange whole facets as
   JSON, not change bytes. A client may read a float through reduced precision and submit it unchanged
   in intent but changed in value while editing another field. Decimal strings survive that path
   without requiring every client to implement Automerge. Ciphertext relays are not the threat.
   RFC 8949's preferred encoding can also erase float-versus-integer spelling, but schema-directed
   storage already handles that distinction; it is not by itself evidence of precision loss.
   Epsilon reconciliation is rejected: it would silently discard intentional small edits and needs
   a field-specific tolerance.
2. **The float stays a footgun on purpose.** A numeric string cannot be accidentally rounded,
   compared as a number, or put into arithmetic without a conversion every language makes explicit.
   Forcing consumers to deserialize is forcing them to decide, which is what JSON's `1.0` → `1` did
   silently.

And why the *layer* the first draft of this added is not there. It converted the convention's JSON
back to plain serde's at every parse, against a schema the consumer would have to carry; that is a
second answer to a question the existing mechanism already answers one way — the byte row above does
its conversion *inside the value's type* (`Vec<u8>` is not bytes in a document; the declared type is)
— and it asked a consumer querying several types to receive a schema each time, which is silly. One
JSON shape, consumed by plain serde against the declared types, is the whole read contract.

**A facet's bare `f64` or `f32` field is refused: the opt-out is marked, never assumed.** The macro
scans a facet's fields, and a bare number field states its spelling or does not compile — use
`facet::Float32`/`Float64`, or mark the field
`#[schemars(extend("x-automerge" = "number"))]`, which is the statement the derived schema carries.
The reason is the same as the closed keyword's: an accidental plain number looks correct in a JSON
document and is wrong in every place the convention is read; the statement is also where a reviewer
sees it. Types that are *not* facets — a third-party type, a plain serde struct nested inside a
facet — are not scanned, so they are plain numbers by construction and nobody is punished for a
crate we do not own.

What it costs, said plainly: a float that must survive a mangling stack has to be *declared*, one
type change per field, exactly where the decision is made; and `minimum`/`maximum` stop applying at a
declared float position, because a stock validator does not range-check a string (the walk still
refuses a value that is not a number at all, and the plug-level checks can carry the rest).

And the boundary is a table, not a rule: an **integer** stays a JSON number only in the
JavaScript-safe range `-(2^53 - 1)..=(2^53 - 1)`; a value outside that range needs a separately
declared string or bytes representation rather than silent rounding. A **vector of floats** is
better carried as bytes, little-endian with
its width stated in the declaration — which is `Embedding.vector`'s practice today — because a
thousand-element float list is a thousand scalars the CRDT addresses one by one. And the same model
catches the byte containers: a facet's bare `Vec<u8>` field is refused, since almost certainly a blob
is what was meant and a CRDT list stores it one op per byte and grows per edit; it compiles as
`facet::Bytes` (canonical `$bytes`), as the field's own `#[serde(with = "…")]`, or as the
deliberate array marker `#[schemars(extend("x-automerge" = "list"))]`. The container drift the scan
does *not* catch is a map of numbers stored inside a facet — what a map holds is its own convention,
`Table` is not exposed, and nobody has asked for one yet.

**The codec belongs to the value's type, not to the field's name.** `facet::Bytes`,
`facet::Counter` and `facet::Text` carry their own `JsonSchema` impls, so `schemars` emits the
declaration at exactly the position it describes — which is what makes `Vec<Bytes>` and
`Option<jiff::Timestamp>` expressible at all, since a name-suffix rule cannot see inside a container.

## 10. Enforcement, in three places

**At compile time**, in the declaration macro, which lives in the `macros` crate because refusing a
declaration means reading the item's attributes — `macro_rules!` cannot ask whether a given `meta` is
present anywhere in an attribute list, and a derive may only add items. Refused: an object that
states neither openness; a closed object without `deny_unknown_fields`; an open object with more than
one overflow field, or with `deny_unknown_fields` as well; a union whose tagging is not stated, or
that is internally tagged because a tag was written with no content key; an open union that names
another tagging, since the codec needs a discriminant key; a union declared open without exactly one
fallback variant, or closed with one; a variant that does not carry exactly one payload type, which
covers `A`, `A()`, `A{}` and `A(Member, Member)` alike; a member payload that does not implement the
member trait, which is also what excludes primitives, maps, `Option` and other carriers; a closed
union whose variant states no tag, or whose stated tag disagrees with the payload's id, the second
being the const assertion the compiler evaluates; a `serde` key on a carrier or variant that the
macro does not read; a bare number field that states no spelling, the marker being the field's
`#[schemars(extend("x-automerge" = "number"))]`, since a plain number that is not deliberate is
invisible in a JSON document (§9); a bare byte-array field that states none, the same way, because a
CRDT list of scalars is one op per byte and the doc grows per edit (§9); `id` and `id_prefix`
together; an `id_prefix` that does not end in the separator; and a generic facet type.

**At the plug level**, where the two artifacts meet. Two checks live here, and they are different
kinds of check:

- `check_shape`, per declaration, run from `Declaration::of::<T>()` where the type and its schema are
  both in hand — it needs the shape the *type* claims, and only the compiler can supply that. A
  disagreement is an error; a flag is a value.
- `Universe::validate`, over the plug's declarations and its dependencies, which catches what no
  single declaration can see: two facets claiming one key tag, two declarations disagreeing about a
  name they both reach, and a reference to a type nothing in the plug or its dependencies declares.

**The Lexicon gates are the third kind: a projection that either produces the published file or
refuses the declaration.** A declaration meant for publishing — `Declaration::of` — is projected at
authoring time, and a union that is not internally tagged is refused there on its declared shape.
A codec document — `Declaration::assemble`, for a type carrying a deliberate plain number — skips
that gate and is refused at the publish instead: `Bundle::lexicons()` projects every facet against
the definitions the whole bundle carries, and refuses a facet that does not project or carries the
`NonInternalTagging` flag. The Lexicon file is not stored on the declaration, because a reference a
facet makes resolves against the plug and its dependencies and not against the declaration alone.
Nothing outside this crate reads the files yet, so the projection guards itself: every file it emits
is checked against the Lexicon grammar — allowed keys per type, local references that resolve, no
`#main`, a closed union with members — before it is returned.

**At the edge, per value**, in `Stored::check`, which is §9's "asserted rather than sniffed" given a
place to happen. A manifest's declarations can be checked without a value, but a storage declaration
is a claim about *data*, and the data is what arrives from outside: a JSON facet from a peer, an
import, a file. That is the boundary, so that is where a value that does not hold its declaration is
refused, with the position named; what travels onward is a `Stored`, and nothing past that point can
fail on the value's account.

**A manifest is more than its facets, and its definitions are hoisted.** A facet type is written at
a position, and every type it names is a separate definition a peer must resolve — but those
definitions are not facets: nothing keys a facet on them and they have no display or reference
behaviour of their own.

So `Declaration::of::<T>()` takes the schema `schemars` derived and **splits it**: the facet's own
schema *is* its definition, and every other definition it reached is hoisted out to travel beside it
in the manifest, addressed by name. A definition two facets both reach is carried once. The facet's
schema then refers to `#/$defs/<name>` like any bundler's would, and a reference is resolved in a
context — §10 says which.

**The definitions are carried, not looked up.** There is no registry: a plug carries the schema of
every type its facets reach, *including* one that belongs to another plug, so a plug can be read on
its own and a definition can vanish from the world without taking its consumers with it. Duplication
is the price of that, and it is worth paying.

This is what makes the reference check load-bearing rather than decorative: a hoisted schema's
references are *not* self-contained by construction, so `Undeclared` has something real to catch.

Two details worth keeping:

- `additionalProperties` cannot see properties contributed by `allOf` or by a `$ref` sibling, so a
  composed object schema is checked with `unevaluatedProperties` instead. Getting that wrong would
  make the check silently pass on exactly the schemas that need it.
- The check runs per declared type, so a foreign type's schema is taken as given. If it is an object
  with no `additionalProperties` we cannot tell whether it drops data; that is its own declaration's
  business, and when it matters the answer is to wrap it in a type of ours that states its openness.

### The context is the bundle

A definition is read in a context, and the context is explicit: **the plug, plus the plugs it
references.** This is the OpenAPI arrangement — a document resolves against its own components and
the documents it includes — narrowed to what we control, and it is deliberately unlike AT Proto,
where the context is every NSID in the world because a name carries its authority.

`Bundle { plug, dependencies }` is that context: `resolve(name)` answers what a name means in it,
`validate()` is the plural check of §10, and `document(facet_name)` assembles the single document a
peer — a stock validator, or the codec's own walk — needs: every definition in scope as `$defs` and a
root that is a `$ref` to the named facet's definition, so the facet is carried once like any other
definition rather than spliced into the root. The manifest carries definitions flat because that is what lets two facets share one; the
document is what keeps that from costing a consumer anything.

## 11. ATProto-compatible by default

Canonical facet JSON is already compatible with the ATProto data model; interoperability is not a
second stringified representation at the boundary. A facet Lexicon supplies the broad representation,
while its sidecar JSON Schema adds storage annotations and supported refinements. `$bytes`, `$type`,
and `$link` retain their protocol meanings. Application-defined `$` keys are refused in the normal
canonical path, and recursively inside content no schema describes — refusing them is what leaves the
spelling free to be relaxed later, and accepting them would spend it.

Imported ATProto object unions and our internal layout are the same shape: the member object carries
`$type`, and the branch inlines that member's properties. The discriminant is the `$type` const.
There is no `value` key on an internal branch. Adjacent tagging (`$type` plus a pinned `value`) is
an explicit wrapper, not the lexicon carrier, and a published declaration does not align it.
`lexicon_projection` and `carrier_parts` must not assume an adjacent `$type`+`value` wrapper; that
assumption in the outlined `schema_compat.rs` reads a key an internal branch does not have. Scalar
or list union payloads need object wrappers when represented in Lexicon. Runtime code follows the
declaration and discriminant rather than guessing a layout. Lexicon references use one optional
`#definition` fragment; `main` is referenced without `#main`.

Rust-authored facets emit only the canonical spelling. A foreign schema that needs renaming,
wrapping, plain base64, or incompatible `$` keys uses an explicit embedder-selected adapter. Ordinary
facet reconciliation neither guesses such mappings nor invokes Panproto. Third-party Rust types whose
wire form is already canonical may still use `schemars`'s `with` or `schema_with`; incompatible types
need a wrapper owned by the authoring crate.

Using Lexicon for facet values does not adopt PDS publication, record keys, repositories, XRPC, or
node discovery. Those remain separate protocols around the same values.

## 11a. Evolution must preserve JSON and native storage

A stable facet tag binds a compatible interpretation, not an arbitrary sequence of schemas.
Same-tag evolution must work in both directions: older and newer readers accept the other's valid
values and preserve their native storage. A storage-breaking change requires a new tag or an explicit
migration protocol; a per-claim schema hash is not adopted as the default solution.

JSON-equivalent does not mean storage-compatible. Str and Text both emit strings, while Int, Uint,
F64, Counter, and Timestamp may look numeric but have different meanings and merge behavior.
Changing units or a codec keyword can break interpretation without changing the JSON type. Adding
an optional Text field is not automatically safe: older software must preserve an existing Text,
but cannot infer its representation when creating it from JSON without the newer declaration.

Changes requiring compatibility scrutiny include opening or closing objects; adding or removing
required fields; narrowing or widening closed enums; changing constraints, array item types, or
additionalProperties; changing integer sign, number kind, format, codec, or units; and adding union
members whose unknown payloads older code cannot preserve natively. A new optional field can also
collide with a previously permitted extension key. A single schema publisher does not prevent data
writers from creating that collision. The core comparator can report JSON, native-storage, and
Lexicon compatibility or “not established”; SchemaRepo v1 only resolves known schemas. Publication
and evolution policy are later repository features.

**Authoring is checked; unknown-schema content is read-only best effort.** A local write to a
facet whose schema is unavailable is refused. Field-name prefix or suffix hints are not part of the
Cabinet codec: they cannot recover a complete schema, float width, declared JSON spelling, or native
merge semantics. Known schemas and explicit adapters are the only authoring routes. Unknown facets
may still be exposed as unresolved data without being silently treated as validated or writable.

Full JSON round-tripping is chosen over hidden-field projection (ADR 015 §2); it is not proof of native preservation. Unchanged
unknown Text, Counter, Bytes, objects, and union payloads must not be rewritten merely because the
local schema does not describe them. Lists and changed union discriminants make correspondence a
real design question. Schema-projected reads are not adopted; the base-relative reconciliation and
copy contracts are to be designed with DocumentsRepo (ADR 015 §2).

A future facet-wide LWW JSON backend is separate work. Sharing a value model does not establish
that a snapshot-only engine can reproduce Automerge's concurrent Counter/List semantics.

## 12. Out of scope, and what remains open

Not adopted here: record keys and TIDs, the ATProto repository/collection model, DNS
resolution, PDS publication, XRPC, CID/link adoption, and a global schema registry. Lexicon describes
the canonical value while the sidecar JSON Schema carries supplementary constraints and storage
annotations. Conflicting definitions for one tag are reported rather than selected by installation
order (ADR 015 §2).

1. Whether `Blob.digest` stays a `Multihash` or migrates to a link, and whether a reference frontier
   becomes its own type or stays a sibling field.
2. ~~The member id, the codec keyword, and the shape of the published artifact.~~ Settled: `id` or
   `id_prefix` on the type, validation at the plug level, definitions hoisted into the manifest flat
   and resolved in a `Bundle`, a stock validator served the assembled document. And the codec is now
   built rather than planned: the vocabulary is one standard keyword (`format`, for the timestamp)
   plus `x-automerge` for the shapes JSON Schema cannot describe, a value is checked into a `Stored`
   before anything is
   written, and a timestamp is stored in the milliseconds the format defines (§9). The float has
   since joined it, declared by its value's types (`facet::Float32`/`Float64`) with a bare `f64` the
   opt-out the Rust type states, and an earlier draft's *re-spelling layer* — a walk that converted
   the convention's JSON back at every parse, against a schema handed over per read — is deliberately
   **not** adopted: one JSON shape and plain serde is the read contract.
3. **The daybook's timestamps are a thousand times too small, and that is a migration.** Its existing
   documents hold seconds in a field the format defines as milliseconds, so reading them with this
   codec shifts every date to 1970. Converting the scalars is a pass over synced documents — every
   node, every replica — and it belongs with the daybook's own migration rather than with the
   facility. Until then, the daybook's timestamps cannot be declared. The float is the opposite
   case and needs no migration: today's documents write plain numbers at float positions, and the
   read accepts both spellings, so the convention lands without touching a document.
4. **The declarations the core mirrors.** `display_config` moves off the facet declaration into
   another field of the manifest, and facet references are adopted as they are; both then have to
   land somewhere when the plug manifest is broken into sections — some of which are daybook-only,
   since the same core has to move data without a daybook node. §10's plug-level codec check —
   refusing a declaration that states a storage form this build does not implement — belongs with
   that work, because today it fires when a *value* arrives rather than when a manifest is validated.
5. The tag-table half of `define_enum_and_tag!`: which part of it survives as the well-known facet
   vocabulary and how those tags are declared once rather than twice.
6. Test vectors for the conformance work. Already in place: a codec round trip in both directions for
   every storage form, the schema-decides-not-the-field-name pair, the accept-and-reject pairs of §4
   and §10, and a schema of ours validated by an independent implementation. The Lexicon projection
   is pinned too: the typed map that projects as `unknown` and whose value type is still checked,
   carried local references and recursive definitions, the union whose member payloads are projected
   even though the union only names them, the grammar check on every emitted file, and both gates —
   a declaration refused at authoring and an assembled codec document refused at the publish. The
   float's are there
   too: the decimal-string projection with the point kept and the width stated, the non-finite
   values by their names, the canonicalizing encoder that destroys the opt-out's numbers and cannot
   touch the convention's strings, and the refusals for a spelling that does not parse. Still
   missing: a `Text` written by one node and read by another, and a
   union round-tripping an unknown member unchanged across a real sync. §9's typed path is pinned by
   a test in one direction only — a whole-facet `Reconcile` *write* is not generated yet, so the
   typed read is demonstrated with the derives against a codec-written document, and the macro's
   part in generating them stays open. The consumer side is pinned the way the repo's read will be:
   the emitted JSON deserialized by plain serde against the declared type, with nothing between.
   `DocumentRepo` reads nothing yet, so the first real call site is its read path — emission, then
   the consumer's own serde — and the daybook's existing hydrate-through-JSON read
   (`am_utils_rs`'s `ThroughJson`) migrates onto the same shape at adoption.


Panproto integration is deferred. The existing proof of concept remains unchanged and is not the
validation, schema-store, or transformation engine for this PR.
