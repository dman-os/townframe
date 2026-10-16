# PR 63 review ledger

Working scratch record for both supplied LLM reviews and subsequent operator decisions.
Tracked home: docs/scratch/pr63-review-ledger.md. Runtime child reports are linked below; their findings remain subject to parent review.
No review claim is considered confirmed merely because an LLM stated it. ADR edits are provisional pending consistency review. No commits; never edit docs/wiki/docs.md.

## Current decisions

- D01: Whole-facet JSON API; ordinary clients need not know Automerge.
- D02: Explicit base heads; AtCurrent explicit unsafe, no implicit retry loop.
- D03: Round-trip JSON chosen. Hidden-schema-field projection rejected: list-element correspondence and required unknown union members make it unsuitable. Earlier ADR text calling round-tripping only a goal needs updating after writer finishes.
- D04: Omission in a complete round-trip put means removal, including unknown properties. Deleting an enclosing object removes its contents. Apply at selected base heads, not unseen current state.
- D05: Unchanged unknown content must retain native storage and object identity. JSON-equivalent is not native-equivalent.
- D06: Pending evidence is not forgery. Ordinary reads accepted only; separately marked optimistic pending reads cannot authorize dependent ordinary writes.
- D07: V1 conservative whole-document closure on proven forgery; no history removal or last-accepted rewind. Recovery remains open, not an approved forge-replace protocol.
- D08: Revocation/historical node authority belong to Keyhive. Actor attribution distinct from authority; original signer distinct from forwarding peer.
- D09: Float strings protect arbitrary JSON client read-modify-write, not encrypted relay transport. No epsilon reconciliation. Bare float opt-out must be stated.
- D10: Same-tag evolution must preserve JSON and native storage interpretation. Mandatory per-claim schema hashes not adopted. Breaking codec change needs new tag or explicit migration.
- D11: Self-drawer retained as cheap case; sharing exposes retained history. Later separation cannot retract disclosure.
- D12: Deterministic routing per layout; split checkpoints use predecessor heads. Universal permanent predecessor byte retention rejected. Causality-aware late migration unresolved.
- D13: Origin-private default; origin-visible and origin-derived policies use delegation, not copied authority. Optimization optional; replay is heads-only, not fetch-all-bytes.
- D14: Relay owns accepted subset; roster communicates acceptance, not fetched/retained proof.
- D15: UUID aliases preserve independent concurrent creation evidence for convergent keys and durable external references.
- D16: Alternative facet-wide LWW JSON backend deferred to its own design.
- D17: Keep panproto POC test-only; do not delete. Core owns schema facility; do not change daybook_types yet.

## Reconciliation / concrete code findings

| ID | Review issue | Disposition / remaining work |
| --- | --- | --- |
| C01 | Open union oneOf fallback also accepts known discriminants | CONFIRMED and fixed: known discriminants excluded from fallback; independent validator regression. Parent reran affected 4 tests successfully and core all-target/all-feature clippy. |
| C02 | Union codec selects first object-shaped branch, ignoring discriminant | CONFIRMED and fixed: discriminant-aware JSON/native dispatch, branch refs, nested Option kind resolution. Parent inspected source diff and reran 4 affected tests plus clippy. List/counter/unknown-field redesign not included. |
| C03 | Maps delete omitted keys against current state, deleting unseen additions | Omission now intentionally removal, but only relative to selected base. Design repo transaction/base contract; concurrency test needed. |
| C04 | Unchanged unknown Text becomes Str through generic JSON write | Accepted requirement D05; oracle evaluating target-aware native fallback and skip-unchanged algorithm. |
| C05 | CounterReconciler::set replaces counter, loses concurrent increments | Verify pinned fork, not review assertion alone. Design base-relative counter delta and overflow/kind cases; concurrency test. |
| C06 | List writes by index rewrite later elements after front insertion | Inspect actual autosurgeon sequence/key algorithm and what walk bypasses. No automatic x-automerge-key adoption. |
| C07 | Lists with duplicates / reordering need element correspondence | Round-tripping removes hidden-property ambiguity but does not prove identity matching. Determine existing-key/LCS semantics and deterministic limits. |
| C08 | Text splices must be relative to base | Inspect historical transaction support and update behavior. Test concurrent edits, not just JSON equality. |
| C09 | Signature list reconciliation loses concurrent entries | Verify dmeta encoding against current-competing-set contract. Signature-ID map proposed, not adopted. |
| C10 | Missing concurrency tests in facet codec e2e | Add narrow reproductions for approved fixes; later map/list/counter/text cases depend on design. |
| C11 | Unknown/new field on absent target cannot infer native kind from JSON | Unknown local creation/copy/paste policy OPEN. Do not silently ban unknown fields or pretend inference is lossless. |
| C12 | Unknown union payload can contain specialized native types | Native preservation must recurse; variant switch may replace payload. Exact rule OPEN. |
| C13 | Existing native kind vs declared schema conflict | Define compatibility/refusal/migration behavior; avoid silently rewriting on schema install. |
| C14 | Integer token-decided storage breaks typed hydration | Previous implementation addresses core signed/unsigned/F64 selection; am_utils_rs still needs adoption review. |
| C15 | Daybook timestamp seconds vs Automerge milliseconds | Migration scheduling OPEN; no transparent guessing or accidental adoption. |
| C16 | Float/byte compile-time statement checker too permissive | Prior assistant said arbitrary schemars with/extend is sufficient; verify exact marker parsing, non-number marker validation, direct u8 accidentally caught, wrapper/alias/maps blind spots. Do not trust earlier final claims. |

## Provenance / trust / signatures

| ID | Review issue | Disposition / remaining work |
| --- | --- | --- |
| P01 | Schema-dependent JSON CID cannot be reconstructed without interpretation | D10 constrains evolution, but schema-less verification remains a real open gap. Native canonical encoding vs carried logical witness vs trusted schema availability NOT settled. Do not claim stable tags alone solve missing schema. |
| P02 | Timestamp units or codec revisions change signed value | D10 and migration documentation; specify immutable meaning and all breaking examples. |
| P03 | Can't physically remove forged AM changes without causal fallout | Physical history retained; conservative closure D07. No arbitrary accepted-state replay engine adopted. |
| P04 | Honest writes can reuse rejected native objects without reading them | Ordinary write barrier plus accepted/base state enforcement required; v1 closure simplifies, implementation tests still required. |
| P05 | Pending evidence visible as accepted allows dependent writes | D06. Scope of pending write barriers and permanently pending liveness/recovery OPEN. |
| P06 | Invalid conflict winner and last-accepted fallback | Last-accepted rewind rejected; v1 closure. Independent pending/concurrent valid facets need precise behavior. |
| P07 | Forge-replace recovery with fresh objects | Candidate only; not a settled mandatory v1 protocol. |
| P08 | Temporary branch for edits based on pending data | Deferred explicit open item; merge only after verification design needed. |
| P09 | Signature, attribution, authorization, acceptance conflated | Preserve distinct verdicts; prove original signer authority, not transport peer. |
| P10 | Historical / concurrent revocation and backdating | Keyhive layer; frontier or epochs proposals unresolved. Pure causal proof cannot order all offline writes. |
| P11 | Actor delegation references exact Keyhive grant | Operator raised possibility; reviewer suggested change frontier instead. Neither adopted. |
| P12 | Schema revision in every claim | Blanket requirement rejected; retained schema-dependent signature concern P01. |
| P13 | Portable witnesses when branching/compacting | Existing ADR covers inherited message/value; generalize evidence preservation without inventing new claim ledger. Verify byte vectors. |
| P14 | Merged value falsely attributed to an individual signature | Existing ADR distinguishes transaction view from merge; preserve in API/materialization tests. |
| P15 | Dmeta's own writes signed? | OPEN: clarify self-reference/circularity and which envelope data is authenticated. |
| P16 | No-op repeated writes create claims because predecessor frontier changed | OPEN: define unchanged write vs deliberate adoption of merged value; don't suppress signatures by JSON equality alone. |
| P17 | UUID alias rationale absent | ADR updated D15; concurrent creation tolerance policy still OPEN, cannot reject invisible remote creator at local commit. |
| P18 | Acceptance globally deterministic vs local trust policy | OPEN; don't conflate optional trusted shortcuts with cryptographically verified status. |
| P19 | Signing/verification conformance vectors | Pending JCS numbers, BLAKE3 CIDs, claim/delegation CBOR, Ed25519, branch evidence, signed change handoff. |
| P20 | Verification resource limits treated as forgery | Must not brick documents on resource refusal; keep pending/unavailable/resource-limited distinct. |

## Drawers / subsets / roster / lifecycle

| ID | Review issue | Disposition / remaining work |
| --- | --- | --- |
| R01 | Optimization group selection disclosure | D13; eligible Keyhive observers only, not global graph. Need exact grants/levels and user-facing leakage description. |
| R02 | Private selective mirror promises no origin disclosure | Can't promise zero interest leakage; object vs origin-head replay cost model. FDR alignment pending. |
| R03 | Origin-private / visible / derived subset policy | ADR added; precise authority levels, editing vs decrypting, and possible extra subset members need tests. |
| R04 | Opt-group membership revocation when subset shrinks | OPEN lifecycle ownership/retraction; do not confuse roster removal with access revocation. |
| R05 | Skip optimization fallback missing | D13 now optional; bounded object or full-origin heads replay; metadata not all document bytes. |
| R06 | Relay accepted set unknown to sponsor | D14; new subset descriptor/roster needs own read path, origin keys only cover original member docs. |
| R07 | Roster one facet/member overhead | Keep provenance rationale; benchmark dmeta bytes and verification, not redesign into giant map solely for overhead. |
| R08 | updatedAt/deletedAt array unbounded append | Review misunderstood updatedAt: clear observed then append, concurrent entries survive. Specify behavior for both fields and test, don't assume deletedAt identical without evidence. |
| R09 | Concurrent add/remove across shards | Automerge-style target; shard translation must preserve logical causality. Exact cross-document merge remains OPEN. |
| R10 | Deterministic routing and targeted lookup | D12; routing hash/ranges/version/physical shard ID choices OPEN. |
| R11 | Split checkpoint net JSON diff loses remove/re-add causality | Affects inherited admission vs independent post-split Set. Need claim lineage or logical replay, not sequential raw-op transplant. |
| R12 | Retain predecessor history forever? | Rejected universally; late writer may supply source history. Keep routing/checkpoints/dedupe safely compactable; chained splits required. |
| R13 | Duplicate concurrent physical child shard creation | OPEN; deterministic routing doesn't determine cryptographic DocumentId by itself. |
| R14 | Self-drawer vs descriptor with multiple members | D11; naming clarified, stable identity/historical disclosure documented. |
| R15 | Layout audience expansion leaks original history | ADR warning added; explicit migration consequences and crash-safe transition needed. |
| R16 | Multi-step roster admission + document grant durable intent | Local temp-add and idempotency approach accepted in principle. Inspect ../townframe-3; pending state persistence/retry/visibility still needs concrete contract; no cross-node retrievable intent requirement adopted. |
| R17 | Catch-up peer eligibility vs actual bytes | Eligibility only candidate. Empty replay cheap; preflight inventory check not required. Provider-agnostic worker, bounded fanout/pending work and batching need design. |
| R18 | Catch-up scheduling after roster admission even advanced cursor | Preserve missing-trigger rationale; heads cursor doesn't certify selected bytes/retention. |
| R19 | 'source online' wording and worker notes contradict provider model | Pending ADR cleanup preserving operator comments; any authorized provider can satisfy request. |
| R20 | Listing/verification at Wikipedia scale | Separate targeted lookup, enumeration, XRPC search/pagination, full roster sync; benchmark before new Merkle catalog. |
| R21 | Overlapping retention reasons and revoked copies | Existing contract; narrow tests and accounting required. |

## Documentation / composition / adoption

| ID | Review issue | Disposition / remaining work |
| --- | --- | --- |
| A01 | FDR mirror definition and Related relationships disagree with 014 | Review source and align settled definitions; do not edit wiki/docs.md. |
| A02 | Wiki public collection Read contradicts 014 | Record and flag; wiki/docs.md is protected from edits. |
| A03 | ADR 007 logical branch identity vs separate document | Inspect and revise relevant ADR only after identifying exact conflicts. |
| A04 | docs.md same logical branch / truncated Dmeta | Protected file: report, do not fix without operator lifting restriction. |
| A05 | Two ADRs numbered 007 | Confirm filenames/links; renumber is separate decision, not automatic churn. |
| A06 | Cargo comment says timestamp seconds | Verify/fix core comment if stale; distinct from data migration. |
| A07 | Cabinet uses daybook.* in agnostic core | Tag ownership/naming decision OPEN; distinguish conceptual docs from runtime code. |
| A08 | Panproto five exact-pinned dev dependencies | Keep POC; measure/build impact and record why test-only, no deletion. |
| A09 | Hoisted unqualified defs collide across plugs | Existing conflict detection not namespace solution. Local bundle/qualified names design OPEN. |
| A10 | Manifest split/display_config/references and closed codec vocabulary | Still pending facility work; check declarations at publication, not only value time. |
| A11 | Compatibility checker in core, port existing PlugsRepo logic | Upcoming authorized direction; inventory actual latest ../townframe-3 logic before implementation. No checker implemented yet. |
| A12 | Incompatible evolution examples | ADR 016 section 11a added: openness both ways, required add/remove, optional removal in closed object, closed enums widen/narrow, array item types, constraints tighten/loosen, typed additionalProperties, integer/number and sign, codecs/formats/units, optional specialized fields, open union member preservation, extension collisions. Confirm completeness and examples. |
| A13 | Hidden-field reads proposed then rejected | Update ADR 015/016 and track rationale D03; raw read/write surface not automatically adopted. |
| A14 | Earlier ADR bare float 'type itself opt-out' remnants | Current compile-time marker requirement contradicts old prose; clean up after writer finishes. |
| A15 | Native target-aware fallback | User proposal under evaluation: schema first, existing native kind second; conflict/error/missing-target rules open. |
| A16 | Field prefix/suffix compatibility hints | Operator approved provisional hints as additional best-effort channel; new authoring must be macro-checked and schema annotations still required. External wire names use annotations. Exact grammar, precedence, failure behavior and container scope OPEN; do not confuse hints with full schema or silently override native/declaration. |
| A17 | Automorph vs autosurgeon | Local ../automorph inspected by oracle: positional Vec/JSON arrays; JSON Str overwrites Text; u64 beyond i64 range becomes F64; nonfinite JSON load substitutes zero; snapshot Text save deletes/reinserts. Do not switch for this seam. Direct ReadDoc+Transactable is useful API precedent. Source evidence in oracle artifact; not executed reproductions. |
| A18 | Custom JSON-only walker vs full typed reconciliation stack | Feasible scope proposal; weigh against serde backbone and maintainability, no new per-type codec layer. |

## Active delegated work

Workflow 03c1c6cc-c0b7-495e-bc37-eeeccf246bce, mission 1bf293ea-7160-49f3-9761-562d3b8f1788.
- codec-unions: only source writer, bounded known-tag fallback and schema branch dispatch, narrow tests and core clippy. No design choices for maps/lists/counters/copy.
- reconciliation-grill: read-only oracle, latest townframe-3 drawer methods and autosurgeon fork, target-aware/base-relative algorithm challenge. Needs focused continuation with latest D03/D04 and A16 question.
- Parent must inspect diff and rerun affected tests/clippy before acceptance. Native async completion; no polling loops.

## Next verification cases

1. Known open-union members each match exactly one schema branch; unknown matches fallback only.
2. Tagged codec dispatch independent of declaration order, with Bytes/Text/Float payload differences.
3. Old schema round-trips newer native Text untouched; known adjacent edit leaves object identity and concurrent splice intact.
4. Omitted property removed at base while unseen remote addition survives concurrent merge.
5. Counter concurrent deltas accumulate; no-op and overflow behavior explicit.
6. List front insert/delete/reorder and duplicate elements preserve expected identity and concurrent edits.
7. Unknown existing native values changed, nullified, kind-switched; absent target copy/paste refuses or uses an explicitly chosen policy.
8. Same content/heads under missing/new schema cannot silently acquire a different signed meaning.
9. Split inherited admission removal vs concurrent successor remove/re-add, with repeated migration and chained splits.
10. Pending vs proven forgery vs resource refusal, write barriers, conservative shutdown and future repair.
