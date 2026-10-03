# Lane D design: SELECTIVE dpath support through the manifest + reference-validation engine

Status: PHASE 1 proposal — no code yet. Owner: lane D (dpath support).
Read before writing: docs/fdrs/001-dpaths.md, docs/fdrs/002-vocabulary.md, docs/fdrs/003-vc-primitives.md, docs/fdrs/004-workspace-cli.md, docs/adrs/010-vtree-store.md, docs/adrs/011-checkouts-reconciliation.md, docs/adrs/012-lenses.md (010–012 replaced/propose today), .agents/drafts/002-disposition.md, .agents/drafts/010-012-disposition.md, src/daybook_types/dpath.rs, src/daybook_types/manifest.rs, src/daybook_types/reference.rs, src/daybook_types/url.rs, src/daybook_types/doc.rs (FacetRef/ImageMetadata), src/daybook_core/drawer.rs (validate_facets, validate_facet_reference, system_facet_manifest), src/daybook_core/plugs.rs (system_plugs dpath registration), src/daybook_core/plugs/mutations.rs (ensure_core_plug), src/daybook_core/plugs/validation.rs (validate_facet_reference_manifests, is_schema_compatible), src/daybook_core/index/facet_ref.rs (extract_references), plugs/tests.rs whole-doc dpath test, daybook_cli checkout dpath/old-node tests.

## 0. Ground truth (what the code actually does today)

- `system_plugs()` registers the dpath tag `org.example.daybook.dpath` with schema
  `anyOf [null, {}-exact]` and `references: []`. Comment pins it: selective
  `targets`/`facetRef` values are typed in `daybook_types::dpath` but not
  registered "until the reference-validation engine can represent them". This
  workspace's core already accepts whole-doc claims; fresh nodes get it at
  first boot via `ensure_core_plug` (creates the manifest doc once if core is
  not yet enabled; early-returns otherwise — an existing node never re-seeds).
- `DpathFacet::from_json_value` parses: `null` / `{}` → whole-doc;
  `{"targets": [...]}` → selective (targets has **precedence**: a value with
  both `targets` and `facetRef` silently ignores `facetRef`);
  `{"facetRef", "refHeads"}` → single-target shorthand. Unknown sibling keys
  ignored; `{"targets": []}` reads as whole-doc; neither-key objects and
  non-objects error. `DpathTarget.{facetRef: Url, refHeads: Option<Vec<_>>}`
  — `refHeads` absent ≡ `[]` ≡ dict.md same-transaction.
- `validate_facets` (drawers) = per-facet JSON-schema validation, then
  `validate_facet_reference` for each declared `FacetReferenceManifest`.
  Reference manifests are **tag-level**: every declared manifest is applied to
  every facet value of the tag.
- `validate_facet_reference`:
  - **Errors unconditionally when the selected path is absent**
    ("reference path … is missing") — today's existence/selection gate.
  - Decodes per variant: string or string-array values (`UrlString*`) vs
    `{ref, heads}` objects (`UrlObject*`). The dpath `targets` spelling
    `{facetRef, refHeads}` matches **neither** spelling.
  - Heads rules: with `at_commit_json_path` (UrlStringSplit), the sibling path
    must select **exactly one** array-of-hash-strings value (absent → error;
    empty array ⇒ "self/whole" mode). Without it: empty/absent heads ⇒
    same-transaction ⇒ **self**-references must exist in
    `resulting_facet_keys` (else the heads must come from the URL `at=`
    fragment; non-self empty heads without a fragment are rejected).
- The facet-ref index (`index/facet_ref.rs`) consumes the same manifests per
  tag: empty selection contributes zero edges (no error), wrong shapes error.
  Today a dpath facet has `references: []` ⇒ selective claims produce **no
  indexed edges** — "unindexed refs were explicitly rejected before", so
  selective claims must come with real manifests.
- Authoring-time gate `validate_facet_reference_manifests` checks each
  manifest's json_path against the facet's value schema via
  `schema_node_for_json_path`, which descends into `anyOf`/`oneOf`/`allOf`
  branches and returns the first branch that contains the path.
- Version/compat gates: core bump needs a strictly greater semver than the
  latest seen record; `is_schema_compatible` value-schema widening check is
  bypassed for 0.x **minor** bumps (`is_major`: 0.x minor > baseline minor).

## 1. Decision 1 — extend the GENERIC reference engine; no dpath branches in generic code

**Break the interface cleanly** (project rule: change interfaces, don't shim).
The five-variant enum `FacetReferenceManifest` is replaced by one struct whose
knobs express exactly the missing capabilities: *presence* (optional
references) and *per-value reference kind* (which fields of a selected value
carry the URL and the heads). No `if dpath` anywhere in `validate_facets` /
the engine; the dpath facet is expressed purely as data in `system_plugs()`.

### 1.1 New shape (daybook_types/manifest.rs)

```rust
pub struct FacetReferenceManifest {
    /// JSON pointer (`/a/b`) or root-dot path (`$.a.b`) selecting the
    /// values this manifest reads references from.
    pub json_path: String,
    /// Behavior when the path selects nothing.
    /// false (default): write-gate error — today's behavior for required
    /// references ("reference path … is missing").
    /// true: the facet simply holds no reference of this shape; contributes
    /// nothing (this is what makes whole-doc `null`/`{}` dpath values valid
    /// under registrations that also serve selective values).
    pub optional: bool,
    /// How each selected value decodes into (facet URL, heads pins).
    pub value: FacetReferenceValue,
    /// Optional sibling path (absolute) supplying heads for string-style
    /// values (today's UrlStringSplit). Absent ⇒ heads come from the URL
    /// `?at=` fragment (dict.md) or the same-transaction convention.
    pub at_commit_json_path: Option<String>,
    /// Heads source may be absent: absent ⇒ dict.md same-transaction
    /// (empty-heads) semantics. Also covers absent object head fields.
    /// false (default): absent heads = error.
    pub heads_optional: bool,
}

pub enum FacetReferenceValue {
    /// URL strings.
    UrlString,
    /// Arrays of URL strings.
    UrlStringArray,
    /// Objects whose own fields carry the URL and heads.
    /// `ref_field`/`heads_field` are plain declarations ("the URL lives in
    /// field X, heads in field Y") — generic vocabulary, not dpath names.
    UrlObject { ref_field: String, heads_field: String },
}
```

Migration of every existing registration (the break is completed, no compat
variants kept):

| today | becomes |
| --- | --- |
| `UrlString { json_path }` | `UrlString` (+ `optional: false`, no at_commit) |
| `UrlStringSplit { json_path, at_commit }` (ImageMetadata, Embedding) | `UrlString` + `at_commit_json_path: Some(..)`, `heads_optional: false` |
| `UrlStringMany { json_path }` (Body `/order`) | `UrlStringArray` |
| `UrlObjectMany { json_path }` (dayledger claims `$.srcRefs[*]`) | `UrlObject { ref_field: "ref", heads_field: "heads" }` |
| `UrlObject { json_path }` | same object kind at the non-array path |
| plug_plabels `$.sourceRef` | `UrlString` |

Call sites touched by the break (enumerated, all of them): `drawer.rs`
(`validate_facet_reference`), `index/facet_ref.rs` (`extract_references` +
URL/object appends), `manifest.rs` (`facet_has_reference_to_tag` predicate
helper), `plugs/validation.rs` (`validate_facet_reference_manifests`,
generalized: the url_field node must allow strings, the heads field node must
allow arrays of strings, at_commit node arrays of strings, and an `optional`
manifest must either resolve in some schema branch or be explicitly allowed
where the path provably doesn't exist), `plugs/tests.rs` fixtures,
`plug_plabels`, `plug_dayledger`. `FacetReferenceKind::UrlFacet` (index edge
column) is unchanged — every manifest still yields the same edge kind.

**Rejected alternatives:**
- Keep the enum and add `OptionalUrlStringSplit` etc. — five extra variants
  that all fan out in three match sites; top-level proliferation, and mutually
  exclusive shapes still can't share a tag cleanly.
- Conditional manifests ("if `$.targets` present use manifest set A else B") —
  strictly more machinery; with `optional` manifests + a `oneOf` value schema
  the same exclusivity is enforced by the schema, which also already runs
  before the reference pass in `validate_facets`. No need.
- Dpath-specific branch in `validate_facets` — prohibited by the lane contract
  and by project rules (abstraction boundaries).

### 1.2 Resulting engine semantics (precise)

- Presence: `selected_values.is_empty()` ∧ `optional == false` ⇒ error
  (unchanged); `optional == true` ⇒ return no references (unchanged rest).
- Decoding per `FacetReferenceValue`; a present-but-wrongly-typed selected
  value is still an error (writer bug — we only ever write these shapes).
- Heads: the heads source (object field or at_commit sibling) is read per
  selected value. `heads_optional == true` ∧ source absent ⇒ treat as empty
  heads, i.e. exactly dict.md same-transaction: for a **self** reference the
  target must be in `resulting_facet_keys` (existing check, untouched); for a
  **cross-doc** reference with empty heads and no `at=` fragment the existing
  "must include commit heads in URL fragment" error applies (unchanged).
- **Existence gating (decision 4):** nothing new and nothing removed.
  `validate_facet_reference` never existence-checks cross-doc targets —
  `target-not-found` is a *state* (FDR 001 §6), consistent with
  `DpathFacet::validate_targets` treating unresolvable refs as normal. The
  only existence gate is the existing same-transaction self-reference check
  against `resulting_facet_keys` (dict.md: "changes that violate self
  references will be rejected"). The `selected_values.is_empty()` case is a
  *selection* gate, re-governed by `optional`, not an existence gate.

## 2. Decision 2 — exact dpath registration (selective + shorthand alongside whole-doc)

`system_plugs()` dpath facet becomes (core version **0.0.1 → 0.1.0**, a 0.x
minor bump, which is treated as major by the compat gate — see §3):

```rust
key_tag: DPATH_FACET_TAG,
value_schema: json!({
    "oneOf": [
        { "type": "null" },
        { "type": "object", "additionalProperties": false },                  // {} whole-doc
        { "type": "object", "additionalProperties": false,
          "required": ["targets"],
          "properties": { "targets": { "type": "array", "minItems": 1,
            "items": { "type": "object", "additionalProperties": false,
                       "required": ["facetRef"],
                       "properties": {
                           "facetRef": { "type": "string" },
                           "refHeads": { "type": "array", "items": { "type": "string" } } } } } } },
        { "type": "object", "additionalProperties": false,
          "required": ["facetRef"],
          "properties": { "facetRef": { "type": "string" },
                          "refHeads": { "type": "array", "items": { "type": "string" } } } },
    ]}),
references: vec![
    // Selective claims: each entry is a {facetRef, refHeads} object.
    FacetReferenceManifest {
        json_path: "$.targets[*]".into(), optional: true,
        value: UrlObject { ref_field: "facetRef".into(), heads_field: "refHeads".into() },
        at_commit_json_path: None, heads_optional: true,
    },
    // The single-target shorthand: URL string, heads in the sibling field.
    FacetReferenceManifest {
        json_path: "$.facetRef".into(), optional: true,
        value: UrlString,
        at_commit_json_path: Some("$.refHeads".into()), heads_optional: true,
    },
],
```

Why this shape is right, mechanically:

- Whole-doc `null`/`{}`: both manifests' selections are empty (`$.targets[*]`
  and `$.facetRef` select nothing on `null`/`{}`) → `optional: true` ⇒ skip ⇒
  validated with zero references. This is exactly why the naive
  "add the two manifests to the current registration" attempt fails today
  (the write gate bails on "reference path … is missing") and why `optional`
  belongs to the generic engine, not a dpath special case. Note both manifests
  carry `optional: true` (the shorthand's `at_commit` path is also allowed
  absent via `heads_optional: true`).
- `heads_optional: true` on both: `refHeads` absent ≡ `[]` ≡ same-transaction
  (dict.md; FDR 001 §2 selective example uses `"refHeads": []`), which keeps
  `UrlStringSplit`-style "refHeads must be present" from applying to optional
  dpath heads. A same-transaction self-target must be facet in the same
  validated write (`resulting_facet_keys`), otherwise the empty-heads rule
  demands an `at=` fragment and rejects — the dict.md "empty refs must shift
  to a proper hash on later updates" behavior, preserved untouched.
- `UrlObject { facetRef, refHeads }` is a *generic* "fields carry the
  reference" kind; the dpath spellings never appear in engine code.
- **Old-node behavior, precisely:** old nodes keep the active 0.0.1 core
  manifest (`ensure_core_plug` early-returns once core is enabled; nothing
  re-publishes). Whole-doc `null`/`{}` writes stay accepted (existing
  behavior); selective `targets`/shorthand writes fail **schema validation**
  ("facet … failed schema validation") in the write gate; the facet-ref index
  holds no reference specs for dpath ⇒ no edges. Remote materialization of a
  selective claim on an old node is not a write and is never gated (CRDT
  merges are tolerant, FDR 001 §4) — the claim is held, just unvalidated and
  unindexed for old-node readers. **No backcompat/upgrade machinery this
  phase**: fresh nodes get the 0.1.0 manifest from `system_plugs()` at first
  boot and accept all three shapes immediately.
- Node-age check interplay: `ensure_checkout_support` (daybook_cli/checkout)
  tests registration *presence* by tag lookup, not shape — unchanged.

## 3. Decision 3 — schema correctness (anyOf vs oneOf, precedence, refs vs index)

- **`oneOf`, not `anyOf`.** The four branches (null / `{}`-exact /
  targets-object / shorthand-object) are mutually exclusive shapes, and the
  mixed writer bug `{"targets": [...], "facetRef": ...}` must be rejected: it
  fully matches **zero** `oneOf` branches (branch 2 forbids any property,
  branch 3 forbids `facetRef`, branch 4 forbids `targets`) whereas an `anyOf`
  would accept it (targets branch alone validates it). oneOf encodes the
  exclusivity that the typed reader's `targets`-precedence only papers over.
- **`minItems: 1` on targets.** `{"targets": []}` is *read* as whole-doc by
  `DpathFacet::from_json_value`, but the manifest rejects it: the canonical
  serialization of a whole-doc claim is `{}` (`skip_serializing_if
  Vec::is_empty`), so `{"targets": []}` is never something we write; keeping
  branches exact keeps the "shape exact" property the whole-doc slice locked
  in. (Residual: a CRDT merge could still *produce* `{"targets": []}` or a
  mixed value from concurrent writes — reads stay tolerant, per FDR 001 §4;
  see §7.)
- **`additionalProperties: false` per branch** (and inside target items):
  consistent with the whole-doc slice's shape-exactness; unknown-key
  forward-compat remains a *reader* tolerance, not a *writer* one.
- **refs vs index:** the two manifests are real `FacetReferenceManifest` entries —
  selective/shorthand claims get validated at every write through the normal
  `validate_facets` gate AND feed the `doc-facet-ref-index` machine
  (`reference_specs` per tag comes from the active core manifest; empty
  selection contributes zero edges so whole-doc claims index nothing, exactly
  like today). Consequence: predicates (`HasReferenceToTag` on dpath targets,
  via `facet_has_reference_to_tag`) and any future read-side consumer see
  selective claims as manifest-declared references. Target heads pins are
  consumed by the write-gate heads rules, not stored as index edges (the index
  records origin heads only, unchanged — not lane D scope).

## 4. Decision 4 — validate_targets semantics ⇒ write-gate existence gating

As stated in §1.2: missing/unresolvable *references* are an unresolved state
(`target-not-found`), never a write-gate rejection; the only existence rule is
the same-transaction self-reference check against the validated facet set,
which `refHeads` absent/empty opts into. Concretely for valid selective
writes: self-target with heads pinned → accepted without existence check;
self-target same-transaction with the target facet in the same write →
accepted; same-transaction self-target whose facet is *not* in the write →
rejected (must pin heads); cross-doc target any heads → heads-syntax check
only, never existence. Unresolvable-at-read remains a placeholder state.

## 5. Tests (phase 2)

- `daybook_types/dpath.rs`: keep the parser matrix; add explicit cases for
  the shorthand round-trip and `{"targets": []}`-reads-as-whole-doc note.
- `daybook_core/plugs/tests.rs`: replace
  `whole_document_dpath_facet_schema_is_registered_and_shape_exact` with an
  accept/reject matrix over the `oneOf` schema: accept `null`, `{}`,
  `{targets:[1]}, {facetRef,…}`; reject `{targets:[]}`,
  `{targets:[…], facetRef:…}` (mixed), unknown keys, non-string facetRef,
  `refHeads` non-strings, non-string/non-object values; plus a manifest
  authoring test that `validate_structure` accepts the new registration
  (manifest↔schema path resolution through the oneOf branches).
- `daybook_core/drawer/tests.rs`: write-gate matrix via `validate_facets`
  directly — whole-doc passes with references registered (the regression the
  naive attempt would have caught); selective self same-transaction with
  target in write → OK; without → rejected & pinned-heads OK; cross-doc with
  heads → OK, without heads/fragment → rejected; malformed URL / malformed
  heads → rejected; each manifest's shape errors still error.
- Validated-write e2e through the normal gate (crate-local e2e, per repo test
  rules — no cargo/tests/): in the existing checkout unix test module (where
  the whole-doc e2e lives), add a test that writes a document with a selective
  dpath facet through the ordinary validated path and asserts (a) acceptance
  on a fresh node, (b) facet-ref-index edges materialize for the targets
  (manifest-declared refs), and (c)(if cheap) old-manifest-node rejection via
  the same fixture pattern
  `node_without_dpath_registration_is_refused_as_predating_checkout` uses.

## 6. Decision 6 — lane C interaction

Lane C's pending behavior-widening question (ADR 012 Q5, lenient handling of
hard errors) does not change anything here: the *manifests* remain the
authority for what is a valid dpath claim. This design produces structured
manifest-driven validation outcomes, not bespoke error messages; if lane C
later softens how validation outcomes surface (bounce vs placeholder), the
same manifests drive both. Nothing in this design should encode error-message
shapes.

## 7. Explicit non-goals (this lane, this phase)

- `derive_tree` / Rule 1–3 collision naming, binding stickiness — untouched.
- Checkout projection semantics: `pauperfuse_daybook` producer continues to
  refuse selective claims (`Unsupported("selective dpath")`) — widening that
  consumer is a separate lane.
- URL-scheme rework: `db+facet:///` grammar, `self` doc id, `at=`/fragment
  heads conventions unchanged (the FDR 001 `db+facet://self/…` mismatch stays
  a documented fixture note).
- Backcompat/upgrade machinery for old nodes; GC; blob strategy; lens
  customization facets; intra-doc entry-set collisions (ADR 012 Q4).
- The `drawer.rs` header FIXMEs (reference race question) — noted upstream,
  not addressed here.

## 8. Phased plan

1. **Phase 2a — engine break:** manifest.rs struct + `FacetReferenceValue`;
   migrate drawer gate, facet-ref index, predicate helper, authoring schema
   gate, all registrations/fixtures. Verify: `cargo clippy --all-targets
   --all-features -p daybook_types -p daybook_core` (+ plabels/dayledger
   crates), targeted `daybook_types`/`daybook_core` nextest for the touched
   modules. No dpath behavior change yet (registrations byte-equivalent).
2. **Phase 2b — dpath registration:** new oneOf schema + two optional
   manifests, core version 0.1.0, rewrite the plugs shape test to the matrix;
   drawer write-gate matrix tests.
3. **Phase 2c — e2e:** validated selective write through the normal gate +
   facet-ref index edge assertions + old-node rejection e2e (fixture pattern
   from the checkout slice).
4. **Phase 2d — docs:** update `daybook_types::dpath` module docs (shapes
   unchanged), manifest docs, and note the `{"targets": []}` read-as-whole-doc
   residual.

## 9. Questions that settle during implementation

- `jsonpath_rust` behavior on a `null` root with `$.facetRef` /
  `$.targets[*]`: expected `Ok([])` (lenient engine); verify with the first
  unit test of 2a and harden if it errors (an error would surface even for
  optional manifests — if so, `select_json_path_values` gains a documented
  absent-is-empty guarantee for non-matching object paths; that is a
  reference-module change, not a dpath branch).
- Whether `is_schema_compatible` (non-major) would have rejected the
  anyOf→oneOf widening — moot with the 0.1.0 minor bump, but record the
  comparator's behavior for future schema edits.
- Exact error text of the generalized authoring gate (`heads_optional`
  manifest against a schema where the heads path exists only in one branch).
- Whether the facet-ref index should also index `at=` fragment heads from
  shorthand strings (today fragment heads are validated but not stored as
  edges) — presumed no for this phase; confirm during 2c if the edge schema
  makes it free.

## 10. Residual risks

- CRDT merges of concurrent targets/shorthand writes can produce a mixed
  value; readers take targets-precedence (one opinion silently outranks
  another at *read* time). Write-gate rejects fresh mixed writes; surfacing
  the merged case is FDR 001 §4 conflict-opinion territory — flagged, not
  designed here.
- `{"targets": []}` written by a peer through a *newer* node? — schema
  forbids it anywhere the manifest is active; old nodes just store it; typed
  read treats it as whole-doc. No action.
- plabels/dayledger fixtures must migrate with the engine break; they are
  compiled into the same workspace, so `cargo clippy` catches all sites.