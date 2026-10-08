# ADR 016: Facet schema language, conventions, and Automerge codecs

**Status:** Proposed

**Related:** ADR 007 (plug manifests), ADR 014 (drawers), ADR 015 (document format and facet
writes — §2 codec declarations, §4 claim canonicalization, §6 verification).

## 1. What a facet schema has to do, and the three layers it is made of

A facet value is described twice. The host needs a Rust type it can read, write and hand to
Automerge; a peer needs the schema it validates against. Publishing a facet also has to be able to
refuse — at the moment a developer adds a type, not months later when a round trip silently drops a
peer's field.

Three layers, each with one job:

- **Values are Automerge's.** `ObjType` is `Map`, `List` and `Text` (`Table` is retained for
  backwards compatibility and behaves as a `Map`), over the scalars `Bytes`, `Str`, `Int`, `Uint`,
  `F64`, `Counter`, `Timestamp`, `Boolean`, `Null`, and an `Unknown` scalar for values from a future
  Automerge. This model — not a JSON-shaped one — is what a facet holds, which is why a `Counter`
  accumulates concurrent increments where an `Int` is last-writer-wins, and a `Text` merges character
  edits where a `Str` does not. JSON is the transfer representation, not the storage model.
- **Schemas are JSON Schema 2020-12.** `schemars` derives one from the Rust type. The keywords
  already reach the whole value model: `object` and `array` for `Map` and `List`, `string` for `Str`
  and `Text`, `number` for `F64` and the integer types, plus `required`, `enum`, `const`, `$ref`,
  `$defs`, and `additionalProperties`, which is also how a map of known value type is written.
- **The conventions are ours.** Openness, where a union's discriminant sits, and how a value is
  *stored* are decisions JSON Schema deliberately leaves open. They are not a divergence from the
  specification: an unknown keyword is an annotation by specification — "A JSON Schema MAY contain
  properties which are not schema keywords. Unknown keywords SHOULD be treated as annotations" —
  and a vocabulary "need not be published outside of its scope of use". So a convention is a sidecar
  key that a conforming validator collects and ignores, and our schemas stay machine-readable by
  anyone else's tooling.

## 2. The use cases we actually have

The facet vocabulary that exists today is small enough to enumerate, and it sets the floor for what
the schema language must express. Daybook-owned facets under `org.example.daybook.*` are dmeta,
refGeneric, labelGeneric, titleGeneric, pathGeneric, pending, body, note, blob, blobPin,
imageMetadata, ocrResult, embedding, plugManifest, plugsConfig, branch and branches. Plug-owned
vocabularies are plabels (pseudo-label, pseudo-label-error, pseudo-label-candidates) and dayledger
(claim, txn, account, meta). A facet whose schema is not installed arrives as `FacetRaw`.

| need | today |
| --- | --- |
| identifiers as strings | `DocId`, `Uuid`, `BranchId`, and `FacetKey`, which serializes as one `tag/instance` string |
| hashes | `Multihash` for blob digests; `ChangeHashSet` for Automerge heads |
| urls with a frontier | `db+facet:///<docId>/<tag>/<instance>?branch=…&at=<head>\|<head>` |
| timestamps | `Dmeta.createdAt` and `updatedAt: Vec<Timestamp>`, unix seconds, carrying a `FIXME: unix timestamp codec` |
| integers | `Blob.lengthOctets: u64`, `ImageMetadata.widthPx`, `Embedding.dim: u32` |
| bytes | `Blob.inline: Option<Vec<u8>>`; `Embedding.vector: Vec<u8>` with an `EmbeddingDtype` |
| maps keyed by string | `HashMap<String, UserMeta>`, `PlugsConfig.enabled`, `Branches.byName` |
| maps keyed by an identifier | `HashMap<Uuid, FacetKey>`, `HashMap<FacetKey, FacetMeta>`, `HashMap<BranchId, BranchDeclaration>` |
| arrays, nested objects | `Body.order`, `updatedAt`, `OcrResult.textRegions`; `FacetMeta`, `UserMeta`, `KnownPlug`, `OcrTextRegion` |
| open strings, closed sets | `MimeType`, `modelTag`, `compression`; `EmbeddingDtype { F32, F16, I8, Binary }` |
| references | `facetRef: Url` together with `refHeads: ChangeHashSet`; `Branch.documentId` |
| text | `Note.content`, `OcrResult.text`, chat transcripts |
| unions of named types | facet values themselves (`WellKnownFacet` or `FacetRaw`), the profile example's attachment, plabels' three variants |
| non-integer numbers | `OcrTextRegion.confidenceScore: Option<f32>`, OCR bounding boxes, plabels' `topScore: f64` |

Three consequences shape the rest of this document.

**Typed maps are load-bearing.** dmeta is `FacetKey -> FacetMeta`; `Branches` is two maps. JSON
Schema expresses this directly as `{"type":"object","additionalProperties": <schema>}`, with the
keys necessarily strings, which is also what Automerge's `Map` allows. `FacetKey` and `Uuid` already
produce their string form, so nothing is strained.

**A reference carries a version frontier.** `facetRef` is meaningless without `refHeads`.

**Non-integer numbers appear in exactly three places**, all of which already have a better
representation, and none of which is a reason to remove `F64` from the storage model.

## 3. Why not adopt the AT Proto Lexicon

An earlier draft of this ADR adopted Lexicon as the schema language. Two things ended that.

**Lexicons have no place to put a convention.** The type table is closed, so a new `type` is not a
lexicon. `format` on a string is a closed enum in practice — `LexStringFormat` has eleven variants
and no catch-all — so an unknown format fails to deserialize the whole document rather than
degrading. And a definition has no extension field: `LexString` is exactly `description`, `format`,
`default`, `minLength`, `maxLength`, `minGraphemes`, `maxGraphemes`, `enum`, `const`, `knownValues`.
An extra key is not rejected — a conforming reader uses no `deny_unknown_fields` here — it is
*silently dropped* on any parse and re-emit, which is worse than a rejection for a signed artifact.
The one extension convention in AT Proto is `x-`-prefixed **data field names**, not schema keys.
JSON Schema, by contrast, is built out of annotations.

**Our interoperability requirement is wider than AT Proto.** Any JSON document has to be
representable, because the software we exchange data with is arbitrary, and any JSON object is
representable by the boolean schema `true`. Requiring every facet declaration to be a valid lexicon
would put a foreign type system in front of that requirement.

What we did keep from AT Proto, because the specification reached these conclusions first and
independently of us:

- **Unions are open by default.** The specification says implementations "should be permissive when
  validating, in case they do not have the most recent version", which is our forward-compatibility
  requirement in someone else's words.
- **`knownValues`, not `enum`, for a vocabulary of strings.** The style guide says closed enum sets
  "should almost always be avoided" because they cannot be extended without breaking evolution. That
  matches the vocabulary above, where only `EmbeddingDtype` and the delegation scope kind are
  genuinely closed.
- **A discriminant is the member's own identifier, not a local label.** In a union the tag is the
  member's NSID, which is the same shape as our facet tag: reverse domain notation, already required
  of `FacetKey.tag`.
- **`$type` appears exactly where the type is ambiguous** — at a union member, not on an object whose
  type the declaration already names. This is what lets a facet payload itself be untagged, since
  `FacetKey.tag` names its type from outside the value.
- **The `map` definition type**, which is a proposal by an atproto maintainer rather than adopted
  spec: `{"type":"map","keys":{"type":"string",…},"values":<schema>}`. Its notes say values "could be
  almost any lexicon definition type" and, decisively, that it "would not impact the data model,
  just schema validation". If we ever emit a lexicon for interop, we adopt that shape and are an
  early implementer of it.

What we do not adopt at all: record keys, TIDs, the collection and repository model, the DNS
`_lexicon` resolution chain, any global registry, and the XRPC primary types. Section 11 says how
values still reach a system that expects those.

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

**Adjacent tagging is the convention**: `{"$type": "<member id>", "value": <payload>}`. Chosen over
the alternatives for five reasons, in order of weight:

1. **It is the only shape that works for every payload.** The discriminant is a sibling key, so a
   member's payload may be an object, an array or a scalar. Internal tagging requires the tag to be
   a member of the payload object and therefore requires every payload to be an object; external
   tagging needs no content key but has no fixed discriminant name.
2. **It leaves the payload type untouched, so closed payloads work.** Internal tagging forces every
   payload to be open, since the tag is one of its members.
3. **It has no tag-key collision when a payload is itself a union**, whereas internal tagging does:
   the inner discriminant would compete with the outer one. This is the reason this repository's
   earlier prototype had to strip the tag before dispatching to the payload.
4. **It converts to an internally tagged system by flattening** — `{"$type": t, "value": {…}}` to
   `{"$type": t, …}` — which is what interop with AT Proto requires (§11).
5. **`$type` outside the payload is what makes an open union round-trip**, since the fallback keeps
   the whole member object including its discriminant.

**An open union has a fallback member; a closed union does not and says so.** The fallback holds the
member object verbatim, so a round trip through a node that does not know the type is lossless, and
a peer that does know it still gets its data. Under adjacent tagging the member object is an object,
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

For a **closed** union, `serde`'s and `schemars`' derives do the whole job: `#[serde(tag = "$type",
content = "value")]` with an explicit rename per variant, and the emitted `oneOf` has a `$type`
const in every branch and no catch-all. Nothing is hand-written.

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
a bare `f32`/`f64` field is the opt-out — the plain number, stated by the Rust type itself.

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
| a base64 string | bytes | `contentEncoding: "base64"` |
| an RFC 3339 string | a timestamp in milliseconds | `format: "date-time"` |
| a number | a counter | `x-automerge: "counter"` |
| a string | a collaborative text | `x-automerge: "text"` |
| a decimal string | a float | `x-automerge: "float"`, which `facet::Float32`/`Float64` emit |

The right-hand column is what keeps us from owning a schema language: **two of the five storage forms
are standard JSON Schema keywords, and our schemas already say them without our help.** `jiff`'s
`Timestamp` emits `format: "date-time"` (`schemars/src/json_schema_impls/jiff02.rs:42`), and the
daybook's byte fields already emit `contentEncoding: "base64"` (`daybook_types/doc.rs`'s
`base64_string_json_schema`, on `Embedding.vectorBase64` among others). Three shapes need the
keyword, and there is exactly one: `x-automerge`. Two are the CRDT shapes JSON Schema cannot describe
— a counter, which is a number that merges by addition, and a text, which is a string with an
identity of its own. The third is a float, whose JSON spelling the convention owns (§9 below), and
whose declaration is its value's own type; a bare `f64` derives a plain number, which is the opt-out
stated by the Rust type itself, not by anything of ours.

So a type declares its storage by what its `JsonSchema` says, and nothing opts in or registers:
`jiff::Timestamp` is stored as a timestamp today, and `chrono`'s or `bytes::Bytes` would be the
moment its schema says so. This is also the complete list: `Map` and `List` are already CRDTs and need
nothing declared, `F64`, `Unknown` and `Table` are not exposed, and a transport that ignores the
declaration still yields a valid string or number, so it degrades rather than corrupts.

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
declarations cannot hold — a string that is not base64 where the schema says bytes, a number where it
says text — naming the position: ``at `$.vectorBase64`: this position is stored as bytes, so it is a
base64 string, not a number``. What comes back is a `Stored`, whose `reconcile` cannot fail on the
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

1. **A number is unfaithful even when every stack behaves.** RFC 8949's preferred serialisation
   spells a float that is also a whole number as the integer, because that is shorter and reads back
   the same double — a *conforming* encoder destroys the distinction. MessagePack stacks that pick
   float32 truncate an `f64`'s digits. A carrier of cabinet data that knows nothing — a relay, a
   backup tool, any re-encoder — can mangle every number; it cannot mangle a string.
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

And the boundary is a table, not a rule: an **integer** stays a JSON number, because every stack
carries a small integer faithfully (the JS `2^53` boundary is its known cost, and no facet today
declares a `u64` that big). A **vector of floats** is better carried as bytes, little-endian with
its width stated in the declaration — which is `Embedding.vector`'s practice today — because a
thousand-element float list is a thousand scalars the CRDT addresses one by one. And the same model
catches the byte containers: a facet's bare `Vec<u8>` field is refused, since almost certainly a blob
is what was meant and a CRDT list stores it one op per byte and grows per edit; it compiles as
`facet::Bytes` (`contentEncoding: "base64"`), as the field's own `#[serde(with = "…")]`, or as the
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

- `check_shape`, per declaration, run from `FacetDecl::of::<T>()` where the type and its schema are
  both in hand — it needs the shape the *type* claims, and only the compiler can supply that. A
  disagreement is an error; a flag is a value.
- `Universe::validate`, over the plug's declarations and its dependencies, which catches what no
  single declaration can see: two facets claiming one key tag, two declarations disagreeing about a
  name they both reach, and a reference to a type nothing in the plug or its dependencies declares.

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

## 11. Interoperating with AT Proto

Ingest is mechanical and lossless. A lexicon document translates to JSON Schema one for one —
`object` to `object`, `string` with `knownValues` to `string`, `union` to a `oneOf` of `$type` consts,
`bytes`, `cid-link` and `blob` to our own formats — and AT Proto data is JSON-expressible, so any
AT Proto value validates against a schema of ours. `$link` and `$bytes` are not types to adopt; they
are the JSON projection of a CID and of a byte string, and our links are already strings (a facet
url, or a multibase multihash) for as long as we choose to keep them that way.

Egress is deliberately asymmetric and should not be made to look otherwise: we can read them, they
can carry us. A facet that has to reach a system expecting AT Proto shapes is encoded as a JSON
string or as bytes — one field, opaque to them, lossless to us. For a union specifically, the
conversion is a flatten, `{"$type": t, "value": {…}}` to `{"$type": t, …}`, which works whenever the
member is an object and is not available when it is a scalar, in which case the stringified form is
the only route. That asymmetry follows from adjacent tagging (§5) and is the reason to keep the
stringified escape hatch permanently documented rather than as a temporary measure.

AT Proto compatibility is therefore a property of a conversion at the boundary, not a constraint on
any declaration. When XRPC arrives it should enter as a **second consumer of the same schema
language** — the published wire contract — rather than as a new set of primaries that facet
declarations have to satisfy.

**Third-party types are handled at the use site, using what `schemars` already provides.** For a field
whose wire form is already acceptable, `#[schemars(with = "TheirType")]` describes it using their
schema — schema only, leaving `serde` alone — and `#[schemars(schema_with = "fn")]` does the same when
the type has no `JsonSchema` impl at all. Because that path goes through `subschema_for`, a type that
does not inline itself gets a `$defs` entry named by its own `schema_name` and a `$ref` at the field,
which is how a foreign type ends up named in our schema without us declaring anything;
`#[schemars(inline)]` forces the other way, and `crate = "…"` controls the reference path. `with` and
`schema_with` are mutually exclusive.

Where the wire form needs translating, a wrapper of ours is required, because the codec belongs to
the type (§9). And a foreign type cannot be a **union member**, because a member needs an id and we
can assign ids only to types we declare; it becomes one through a wrapper of ours, or through its own
declaration if it came from a plug that published an id for it. That is the boundary: **field
positions take foreign types, union members take declared ones.**

## 12. Out of scope, and what remains open

Not adopted: Lexicon as the schema language, its type table and primaries, record keys and TIDs, the
repository and collection model, DNS resolution, and any global registry. Registries may distribute
and endorse plugs, but conflicting definitions for one tag must still be reported rather than
resolved by installation order (ADR 015 §2).

1. Whether the manifest's published artifact stays a JSON Schema document or becomes a Lexicon
   document with a derived JSON Schema view. ADR 015 §2 defines a facet definition as describing its
   JSON value, so the second is closer to the specification, and it would change
   `FacetManifest.value_schema`.
2. Whether `Blob.digest` stays a `Multihash` or migrates to a link, and whether a reference frontier
   becomes its own type or stays a sibling field.
3. ~~The member id, the codec keyword, and the shape of the published artifact.~~ Settled: `id` or
   `id_prefix` on the type, validation at the plug level, definitions hoisted into the manifest flat
   and resolved in a `Bundle`, a stock validator served the assembled document. And the codec is now
   built rather than planned: the vocabulary is two standard keywords plus `x-automerge`
   for the shapes JSON Schema cannot describe, a value is checked into a `Stored` before anything is
   written, and a timestamp is stored in the milliseconds the format defines (§9). The float has
   since joined it, declared by its value's types (`facet::Float32`/`Float64`) with a bare `f64` the
   opt-out the Rust type states, and an earlier draft's *re-spelling layer* — a walk that converted
   the convention's JSON back at every parse, against a schema handed over per read — is deliberately
   **not** adopted: one JSON shape and plain serde is the read contract.
4. **The daybook's timestamps are a thousand times too small, and that is a migration.** Its existing
   documents hold seconds in a field the format defines as milliseconds, so reading them with this
   codec shifts every date to 1970. Converting the scalars is a pass over synced documents — every
   node, every replica — and it belongs with the daybook's own migration rather than with the
   facility. Until then, the daybook's timestamps cannot be declared. The float is the opposite
   case and needs no migration: today's documents write plain numbers at float positions, and the
   read accepts both spellings, so the convention lands without touching a document.
5. **The declarations the core mirrors.** `display_config` moves off the facet declaration into
   another field of the manifest, and facet references are adopted as they are; both then have to
   land somewhere when the plug manifest is broken into sections — some of which are daybook-only,
   since the same core has to move data without a daybook node. §10's plug-level codec check —
   refusing a declaration that states a storage form this build does not implement — belongs with
   that work, because today it fires when a *value* arrives rather than when a manifest is validated.
6. The tag-table half of `define_enum_and_tag!`: which part of it survives as the well-known facet
   vocabulary and how those tags are declared once rather than twice.
7. Test vectors for the conformance work. Already in place: a codec round trip in both directions for
   every storage form, the schema-decides-not-the-field-name pair, the accept-and-reject pairs of §4
   and §10, and a schema of ours validated by an independent implementation. The float's are there
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
