# PF Lane F — Publication / Reconciliation design (Phase 1, read-only, design gate)

**Lane:** F (publication & reconciliation: staged checkout branches → upstream main, per ADR 011 §6/§7), workspace **townframe-pfuse-a** (same workspace as lanes A/C/B artifacts).
**Contract sources:** ADR 011 §4–§10 (accepted), ADR 010 vtree (§5 offers/acknowledgement), ADR 012 §6–§8 (preparation/validation/round-trip), FDRs 001–004, ADR 012-bigsync (transport context).
**Seam inputs:** lane A ingest design (`pf-lane-a-ingest-design.md` — receipts/staging/no-op decisions are the upstream interface this doc builds on) and lane C lens contract (stage traits, `Recipe`, selection is stateless recompute).
**Read-only phase:** this doc proposes; no code, no manifest edits, no `docs/` edits.

---

## 0. What exists today — verified against the code (this is the API contract I design against)

### 0.1 CLI coordinator — `src/daybook_cli/cmds/checkout.rs`
- `CheckoutCommands::{Create, Status}` only. Unix-only `unix` module. No ingest verb yet (lane A adds it); certainly no publish verb.
- Marker `.daybook-checkout`: `Checkout { version, id, node_path, node_key, drawer, projection: Projection, basis (main heads at create), branch = "/tmp/checkout/<id>", branch_id, render_heads, state }`; `State::Pending { failure } | Ready { length, digest, generation }`. `replace_marker` does atomic temp-file rename with identity re-check before and after; `deny_unknown_fields` versioning.
- `project()` shows the acknowledgment pattern lane F reuses for the post-publish projection: `create_checkout_branch` → resolve ref (`BranchKind::Local` asserted) → `get_doc_bundle_at_branch` records `render_heads` → `Daybook` producer `observe()` → `VtreeStore::replace` → `Scan` → `FilePut { expected }` → `TokioFs::prepare`/`apply()`/`cleanup()` → `State::Ready` evidence = observed file evidence (`length, digest`) + vtree generation.
- `status()` is pure observation (evidence compare + untracked walk). Lane A extends it with receipts; lane F extends it with publication state.

### 0.2 Drawer branch APIs — `src/daybook_core/drawer/`
All names below verified at their locations:
- **`update_at_heads(patch, branch, heads: Option<ChangeHashSet>)`** (mutations.rs:428). Important precision for the whole design: `Some(heads)` is the **transaction basis**, not a refusal CAS. It reads existing facet keys *at* `heads`, validates, and `transaction_at`s from those heads — if `heads` are stale-but-present in history, Automerge happily creates a *concurrent* change rather than refusing. Lane A's staging discipline ("Some(live heads)") is sound only because ingest is the single writer; **publication must not lean on it as a CAS.** CAS must be a heads *comparison + refusal*.
- **`create_checkout_branch`** (mutations.rs:617): sole creator of `BranchAuthority::Checkout` / `BranchKind::Local` branches (must be `/tmp/*`; pending-group-only parents make the local user sole authority). Local branches live in drawer-local store, not the replicated entry; no origin delegation. This is why staged work survives origin-authority loss (ADR 011 §7).
- **`merge_from_heads(id, to_branch, from_branch, from_heads, user_path)`** (mutations.rs:991): snapshots the **source** at `from_heads` (`fork_at`; fails if the source is missing those heads), then `merge_and_log_patches` into the **target's live heads**. Gates both branch refs via `branch_doc_reachable`. **There is no target-side expected-heads check.**
- **`merge_from_branch`** (mutations.rs:1400): resolves live from-branch heads, delegates to `merge_from_heads`.
- **`ensure_branch_at_heads_from_branch`** (mutations.rs:974), **`update_batch`** (mutations.rs:1371 — `buffered_unordered`, per-index error map, *not* a transaction): present but not suitable as-is for the publication sequencing below.
- **Read surfaces** (queries.rs): **`get_doc_bundle_at_branch`** (:744) returns live branch heads + facet snapshot + entry in one call — this is the heads read upstream publication races against. `get_with_heads` (:790), `get_doc_with_facets_at_branch_heads` (:719 — exact-heads read lane A uses for render-base facets), `facet_keys_at_branch_heads` (:829). **`get_branch_heads_for_path` is `pub(crate)`** (drawer.rs:553) — a heads-read for callers outside the drawer today has to go through `get_doc_bundle_at_branch`/`get_with_heads`.
- **`validate_facets`** (drawer.rs:776, `pub(crate)`): plug-manifest-driven facet validation used before every transaction. Candidate validation must reuse this same logic (lane A reached the same conclusion for `--allow` imports).
- `Projection::select` (pauperfuse_daybook/lib.rs:40) reads **`main`** via `get_with_heads(&document, BranchPath::new("main"), None)` — the interpretation is hardcoded to `main`; a heads-parameterized variant is needed for candidate validation (§3).

### 0.3 big_repo / big_sync publication surfaces
- **There is no separate "publish" API in big_repo.** Publishing a document change **is** a merge into the destination branch (`main`)'s content doc; `BigDocHandle::with_document` (big_repo/lib.rs:1340–1400) runs all automerge work "under a short sync lock" (`surelock::key::lock_scope`), and remote mutations applied by the runtime2 ingest path are serialized through the same doc state. **Therefore a heads check inside the merge closure is a genuine CAS against concurrent upstream writers** — this is the load-bearing fact for ADR 011 §6 step 7.
- Committed doc changes reach peers through the existing event/part pipeline: drawer `events.rs` consumes `BigRepoChangeNotification`s and the `big_sync` worker (`spawn_big_sync_worker`) exchanges parts. Publication and distribution are already decoupled; lane F owns only the merge, never the transport.
- Authority on `main` is the node's ordinary drawer content-doc access (the checkout branch is the same logical doc's `BranchKind::Local` branch document; `merge_from_heads` between them is the normal ADR-007 branch-merge path). Publish denial behaves like the existing local access failure path (`BranchNotFound`-style gating on unreachable branch docs) plus keyhive write denial — it must be surfaced as *denied*, not misread as a conflict.

### 0.4 pauperfuse
`VtreeStore::{register, lookup, version, replace, scan}` — observed-tree recording; `Scan` streams ordered entries; `TokioFs::{observe, prepare, apply, cleanup}` with `ExpectedFile`/`FileEvidence` — the existing acknowledged-projection machinery the post-publish projection reuses verbatim.

### 0.5 Lane A seam (the interface this design builds on)
Lane A stages file edits onto the checkout branch via `drawer.update_at_heads(patch, checkout_branch, Some(live_heads))` and records **receipts** in the marker: `{ path, file: {length, digest}, branch_heads, sequence }`, plus a per-checkout `blocked { operation, failure }` record on `State::Ready`. Ingest **never touches `main` and never advances render evidence**; it ends with "staged on checkout-local branch; not published upstream" and refuses any publish attempt outright. Publication's job is everything after that point.

---

## 1. The publication pipeline (ADR 011 §6 verbatim, mapped to real calls)

Invocation = the checkout's single writer for its duration (same CLI-serial assumption as lane A; §9 non-goal on locking machinery).

The scope is a **batch of destinations** (documents) that the batch's staged receipts name. In the current slice this is one document, but the structure below is per-destination from day one, because the durable record (§4) and the pre-flight gate (§1.1) demand it.

### 1.1 Step-by-step

For a batch of destination documents `D`, each with an associated set of receipt paths:

1. **Observe (recompute).** Open the marker via `discover` + `read_marker` (integrity-validated), `verify_node`, resolve the branch ref (`get_branch_ref`) and assert `branch_id`. **Loaded:** marker bindings, receipts, prior publication records, render evidence, `basis`. **Recomputed:** everything that is *correctness-relevant*: live receipt-path evidence via `TokioFs::observe`; live checkout-branch heads via `get_doc_bundle_at_branch`; live upstream heads via the same call at `"main"`.
2. **Gate on ingest state.** Every path driving a destination must be in a settled receipt state: `clean` (no-op path) or `ingested` with `receipt.branch_heads == live checkout-branch heads`. **Any `modified`/`missing`/`ingested (unconfirmed)` path refuses the batch** (FDR 004: never silently ingest; ADR 011 §5: refuse affected work rather than ingest implicitly). This is the inverse of lane A's gate: publication requires staging to be *complete*, ingest required it to be *prepared*.
3. **Gate on prior state.** An unexplained `blocked` record refuses with its stored diagnostic. Destinations whose publication record says `published` are skipped (§4 resume rule).
4. **Per destination: read upstream heads `H`.** `get_doc_bundle_at_branch(doc, "main", None)` → `branch_heads == H`; snapshot upstream facets at `H` when needed (`get_doc_with_facets_at_branch_heads`). This read is the *expected-heads record* for this attempt.
5. **Merge upstream into an in-memory Automerge candidate.** Candidate = checkout-branch doc @ live heads `K` merged with upstream @ `H`. **Nothing is persisted yet.** Mechanism: a new `daybook_core` drawer surface that clones/fork_at-s the two branch docs inside existing handles and merges in memory, returning the candidate snapshot without committing (§2.3). This API addition is the one unavoidable `daybook_core` change; `merge_from_heads` cannot express "validate before persist" because it commits into the target as a side effect.
6. **Validate the candidate (§3).** Facet schema (reuse `validate_facets`' manifest logic on the hydrated candidate facet set) **and** the checkout's lens interpretation re-run against the candidate heads. A failed candidate persists nothing and blocks (§3.2).
7. **Persist the valid candidate to the checkout branch.** `merge_from_heads_checked(id, checkout_branch ⇒, from = "main", from_heads = H, expected_to_heads = K)` — the persistent form of the same merge, guarded by the target-side CAS. Result: checkout branch heads `K′ ≥ K` now contain upstream @ `H`. Re-read `K′` from the branch handle for the CAS input of step 8 and the outcome record. This persist is itself safe to repeat: CRDT merge of the same upstream heads is idempotent.
8. **Publish only if upstream is still at `H`.** `merge_from_heads_checked(id, "main" ⇒, from = checkout_branch, from_heads = K′, expected_to_heads = H)`. Because the checkout branch already contains `main`@`H` (step 7), this is exactly the fast-forward of the *already merged and validated* state that §6 step 7 demands — never an unchecked fresh merge. On `HeadConcurrency` refusal (upstream moved), retry step 4.
9. **Record per-destination outcomes** (§4): `{ doc, expected_heads H, published_heads, attempts, outcome }` written via `replace_marker`.
10. **Prepare and apply the resulting filesystem output plan** (§6 step 9). Where the merged candidate re-renders an output whose bytes differ from acknowledged disk evidence (upstream brought changes beyond the staged base), project through the **exact lane-A/create machinery**: `Daybook` producer at the new branch heads → `VtreeStore::replace` → `Scan` → `TokioFs::prepare { expected: ExpectedFile::Present(evidence) }` → `apply()` → acknowledged evidence. Where the merged render equals current disk bytes, the projection is a verified no-op and **nothing advances**. Render bindings advance only for actual acknowledged outcomes.

### 1.2 Recomputed vs loaded

| | Recomputed each invocation (authority) | Loaded (retained input, never authoritative) |
|---|---|---|
| Filesystem state | `TokioFs::observe` evidence of every receipt path | prior `Ready` evidence, receipt `file` evidence |
| Upstream state | live `main` heads via `get_doc_bundle_at_branch("main")` | marker `basis` (create-time heads), prior `expected_heads`/`published_heads` |
| Checkout branch | live heads via `get_doc_bundle_at_branch(branch)`; facet readback at heads | receipt `branch_heads` (matches gate), `render_heads` (project base) |
| Candidate | fresh in-memory merge, fresh validation | prior validation results are never trusted |
| Interpretation | lens/`Projection` re-run at candidate heads | stored `projection` selection (retained identity) |

---

## 2. Race retries and the expected-head CAS discipline against real APIs

### 2.1 The one new primitive: `merge_from_heads_checked`

`merge_from_heads` (mutations.rs:991) commits into the target's live heads unconditionally. Per ADR 011 §6 step 7 the publish must refuse if upstream moved. Because all mutations of a content doc serialize through `BigDocHandle::with_document`'s short sync lock (big_repo/lib.rs:1354), checking target heads and merging in the same closure is atomic against both concurrent local writers and remote applies. Proposed, minimal, breaking-change-with-a-single-concept (AGENTS.md style rule, no `merge_variant*` proliferation):

```rust
pub async fn merge_from_heads(
    &self,
    id: &DocId,
    to_branch: &BranchPath,
    expected_to_heads: Option<&ChangeHashSet>,   // NEW param: refusal CAS on the target
    from_branch: &BranchPath,
    from_heads: &ChangeHashSet,
    user_path: Option<&UserPath>,
) -> Result<(), DrawerError>            // new variant: HeadConcurrency { branch, expected, actual }
```

Inside the existing `handle.with_document(...)` closure, before `merge_and_log_patches`: read `am_doc.get_heads()`; if it differs from `expected_to_heads`, refuse with `HeadConcurrency` — **nothing committed**. `None` keeps current behavior. Existing internal/test call sites update mechanically. `merge_from_branch` gains/forwards the same parameter. This single change is what converts ADR §6 steps 7–8 from prose into mechanics.

### 2.2 The bounded retry loop

```
attempts  = 0
loop:
    H  = bundle_at(doc, "main").branch_heads            // fresh read each attempt
    K  = bundle_at(doc, checkout).branch_heads          // fresh read each attempt
    candidate = in_memory_merge(checkout@K, main@H)
    validate(candidate)? — invalid ⇒ FAIL-INVALID (§3.2), no retry, blocked
    merge_from_heads(doc, checkout, expected=Some(K), "main", H)    // persist candidate
    K' = bundle_at(doc, checkout).branch_heads           // re-read after persist
    merge_from_heads(doc, "main", expected=Some(H), checkout, K′)   // publish, CAS
        Ok            ⇒ outcome = published {_heads: read back from main}; break
        HeadConcurrency ⇒ attempts += 1; if attempts == PUBLISH_MAX_CAS_ATTEMPTS
                            ⇒ FAIL-EXHAUSTED (§6.4); blocked; staged work preserved
                            else retry with fresh H
```

- **Bound:** `PUBLISH_MAX_CAS_ATTEMPTS` (proposed 3; per ADR §6 step 8 "finite bound… implementation setting"). *Not* exponential backoff, *not* unbounded: upstream racing continuously means the environment changed; blocking is the contract.
- **Why no TOCTOU window remains on the CAS:** the expected-heads comparison and the merge run inside one `with_document` lock scope on the target doc; remote application is serialized against the same lock. The window ADR §6 warns about ("a future batch merge API can reduce the race window") concerns the *cross-document* span (§4), not this per-document CAS.
- **Persist-before-publish ordering** means a retry never re-stages: the checkout branch already holds the validated merge; only the upstream CAS is redone. And an exhausted-retry checkout is blocked with `K′` already containing the validated upstream merge — the branch is still ordinary, reusable history.
- **`update_at_heads(Some(...))` is explicitly not part of this discipline** — it is a transaction basis, not a refusal CAS (§0.2). Publication never fabricates CAS semantics from it.

### 2.3 The in-memory candidate surface (daybook_core, minimal)

One new `DrawerRepo` method, e.g. `prepare_merge_candidate(id, to_branch, from_branch, from_heads) -> MergeCandidate`, exposing the internals `merge_from_heads` already performs *before* its commit: read target handle + source fork_at snapshot (with the same missing-heads invariants and reachability gates), run `merge_and_log_patches` in memory, return `{ to_branch_heads, candidate: Automerge snapshot (or a heads + facets snapshot), modified_facet_keys }` without committing. `merge_from_heads_checked` is then its committing twin. Validation consumes the candidate (§3); persist re-executes the merge deterministically (same two doc states ⇒ same merged state; heads differ only by actor/timestamp metadata, which no validation depends on).

This is deliberately **not** a big_repo/big_sync change, no batch merge API (non-goal), and no cross-document transaction (ADR §6: none promised).

---

## 3. Validation of merged candidates; invalid-merge handling

### 3.1 What validation runs

Two layers on the **candidate** only (the branches themselves were valid when staged — ADR §6: individually valid histories can merge invalidly):

1. **Facet schema.** Hydrate the facet set at candidate heads from the candidate snapshot; validate every modified facet key against the plug manifests by reusing `validate_facets`' manifest logic (extracted to accept a hydrated facet set + resulting key set — the same split lane A flagged to avoid duplicating validation for `--allow`). Any rejection is schema-invalid ⇒ invalid candidate.
2. **Lens/interpretation re-run — yes, preparation re-runs; production does not.** The checkout's lens interpretation is re-run against the candidate heads — for this slice, the `Projection` invariants (whole-doc dpath → Body `order[0]` → self, unpinned, unpinned-branch Note `text/plain`; `validate_note`; dpath→`RelPath` still parses and passes `validate_output`). If the merged document breaks the projection contract (e.g. a concurrent upstream change removed the Body or retargeted the dpath), the candidate is invalid — **the disk file is not touched, nothing is published, no bytes are re-rendered.** Re-running `LensProduce`/rendering is explicitly *not* part of validation (ADR 012 §8: production happens at projection time, its own failure taxonomy).

### 3.2 Invalid-merge handling — ordinary branch surfaces only (NO bounce doc)

Per ADR 011 §7, superseding the old `/tmp/conflicts/<facet-id>` scheme:

- Outcome: **FAIL-INVALID.** Nothing published, nothing persisted (the candidate lives only in memory and dies with the invocation), checkout branch and upstream both remain exactly as read (both stored states preserved).
- Durable effect: `blocked` record on the marker with `operation: "publish"` and a diagnostic naming the document, the candidate's failing facet keys/interpretation, and both sets of heads read. Exit non-zero.
- Resolution: ordinary branch/merge surfaces integrated with checkout bindings (lane C's `LensDiff::describe_difference` is the eventual explain-surface): the user may (a) discard or re-stage local work (lane A surfaces), (b) edit upstream through an ordinary `main` write so the next candidate merges validly, or (c) reconcile via explicit branch operations on the logical document. No special pick-only viewer, no conflict markers in files, no bounce branch, no new document identity.
- A failed candidate never invalidates either existing branch; the next publication attempt recomputes from scratch.

---

## 4. Partial multi-document publication: durable record, no duplicate imports, no reapplication

### 4.1 Where the record lives — the checkout marker, extending lane A's shape

**Explicitly NOT the vtree** (ADR 010: reps are observations; correspondence and outcomes are checkout state) and NOT a new SQLite surface in this slice (the marker's atomic-rename replace + `deny_unknown_fields` versioning is the established durability mechanism; the `.dtree`/SQLite migration is a later technical milestone both ADRs leave open).

```jsonc
// marker additions (checkout `version` bump together with lane A's receipts/blocked fields):
"publications": [ {
  "seq": 3,                              // per-checkout monotonic, same last-writer-wins rule as receipts
  "doc": "<doc-id>",                     // destination document (later: per destination incl. cross-doc facet targets)
  "expectedHeads": ["<hash>"],           // H used for this attempt's candidate validation
  "branchHeads": ["<hash>"],             // K′ persisted after step 7
  "publishedHeads": ["<hash>"] | null,   // main heads after the CAS merge (null = not published)
  "outcome": "published" | "refused" | "blocked",
  "failure": "<diagnostic> | null",      // refused/blocked only
  "attempts": 2
} ]
// checkout-level: lane A's "blocked": { "operation": "publish" | "ingest", "failure }
```

Receipts stay **staging-scoped** (path → file evidence ↔ branch heads); publication records are **destination-scoped** (doc → heads/outcome). They do not overwrite or reinterpret each other; `ingested` paths stay `ingested` after publication (still unpublished-render facts), with status deriving the published truth from the publication records (§7).

### 4.2 Crash windows and resume

Enumerated by the write order (persist candidate → CAS publish → record):

| Crash after | Marker state | Recovery behavior |
|---|---|---|
| nothing | receipts only | next invocation recomputes from scratch; nothing staged twice |
| step 7 (branch persist), before publish | no publication record; branch heads moved behind no receipt | harmless: the next attempt reads fresh `K`, recomputes candidate (merge with `main`@`H` again is idempotent); no reapplication of already-accepted ops |
| step 8 CAS-merge committed to `main`, record not yet written (the one unavoidable window) | record missing, `publishedHeads` unknown | **resume rule:** read live `main` heads `H₂`. The prior record (if any) shows the previous expected heads `H`; because the committed publish CAS-merge moved `main`, `H₂ ≠ H`, so the destination reads as in-flight/pending and the pipeline reruns for it: fresh `H₂`, fresh candidate. The CRDT merge of the checkout branch (whose changes are already upstream) produces a no-change merge or a trivial metadata-only delta — **no operations are re-authored**, no duplicate import (imports were settled by receipts at ingest, not by publication). Automerge merge is idempotent; "not reapplying accepted operations" is structural, not bookkeeping. |
| record written | `outcome: published` | skipped by the resume gate (§1.1 step 3); if `main` moved since, that is ordinary upstream divergence for a *future* batch, not a resumable one |

**No-duplicate-import guarantee** rests entirely on lane A: imports create identities at ingest with receipts; publication never imports. **No-reapply guarantee** rests on the CRDT merge being idempotent + the per-destination gate skipping destinations whose record says `published`.

**Cross-document partiality:** destinations succeed/fail independently (ADR §6 "no rollback promised; expected-head publication can partly succeed"). Each writes its own outcome; a `blocked` checkout-level record marks the batch incomplete; automatic activity (a future watch) is blocked; explicit retry resumes only non-`published` destinations. A future big_repo batch merge API may shrink the window but not change this ledger shape.

### 4.3 What is *not* recorded durably
No change-ID/message annotations (FDR 004 §4 leaves `-m` representation open — publish accepts no message in this slice). No upstream acknowledgement-of-delivery (distribution is big_sync's concern; `publishedHeads` is a local merge fact, per ADR 010 §5 "record its actual outcome, not the original proposal").

---

## 5. Publication ↔ ingest receipts ↔ render heads — stated precisely

- **Publishing does not update ingest receipts.** Receipts record "these disk bytes are staged at these branch heads" — publish moves document history, not disk or receipt facts. A published path still reports `ingested … unpublished` until publication records make status derive `published` (§7). Re-ingest after publication remains a no-op (round-trip vs branch heads — lane A §4 decision, unchanged by publish since publish only merges upstream into the branch).
- **Publishing updates `State::Ready` render evidence ONLY via the step-10 projection and only for acknowledged outcomes.** Specifically: if the merged candidate's render equals current disk bytes → projection is a verified no-op; `Ready.{length,digest,generation}`, `render_heads`, and receipts all stay untouched. If upstream brought changes beyond the staged base (render differs from disk) → the plan applies the new bytes through `TokioFs::prepare { expected: Present(last acknowledged evidence) }` → `apply()`; on success `render_heads` advances to the projected branch heads and `Ready` evidence becomes the applied file evidence, exactly the create-path acknowledgment pattern. Dirty-target refusal at apply time (file changed since receipts were captured) blocks like any filesystem plan failure — it never overwrites the newer edit.
- **Publishing never touches `basis`,** which stays the create-time main heads (retained provenance).
- **Ingest after publication:** unaffected; receipts keep working off live branch heads.
- **The dependency direction is one-way:** publish consumes settled receipts; it must never trigger ingest (FDR 004 "a read must not destroy the pending state it reports" generalizes: a publish must not ingest what it refuses to publish).

---

## 6. Failure taxonomy and watch blocking

| # | Class | Concrete causes (this slice) | Immediate behavior | Durable state | Watch effect |
|---|---|---|---|---|---|
| F1 | **Preparation/integrity** | marker invalid/unreachable, node identity changed, branch ref missing or `branch_id` mismatch, blocked record present, receipt state not settled (`modified`/`missing`/`ingested unconfirmed` paths) | refuse whole batch, exit non-zero, diagnostics per path | prior marker (`blocked` unchanged if a F1 cause *is* a blocked record) | blocked (checkout-level `blocked` persists; automatic flows refuse while set) |
| F2 | **Invalid candidate (merge-if-valid)** | schema-invalid facet at candidate heads; lens interpretation fails on merged state | nothing persisted or published, no retry, both stored states preserved, failed candidate reported and discarded | `blocked { operation: "publish" }` + diagnostics naming doc + facets + heads | blocked until resolved via ordinary branch surfaces |
| F3 | **CAS race** | `HeadConcurrency` on persist or publish | retry from fresh heads, `PUBLISH_MAX_CAS_ATTEMPTS` bound | on exhaustion: `blocked { operation: "publish", failure }`; branch keeps validated merge from last attempt | blocked; staged work preserved; explicit retry resumes |
| F4 | **Authority/denied** | keyhive write denial on the destination content doc; unreachable branch docs (`branch_doc_reachable` gates in `merge_from_heads`) | surfaced as denied, never misreported as conflict; never a bounce | `blocked` + per-destination `refused/denied` outcome; local bytes and staged history untouched | blocked; permission evidence refreshed (projected mode bits refresh is lane C/projector work, flagged) |
| F5 | **Repository/runtime** | drawer errors, IO, vtree errors | propagate; best-effort `blocked` record if marker writable; per-destination outcomes for completed destinations kept — no rollback (ADR §6/§10) | partial `publications` + `blocked` | blocked until explicit recovery |
| F6 | **Filesystem application (step 10)** | dirty target vs acknowledged evidence; obstructed path; partial apply | per-file outcomes recorded; published upstream work is *not* undone (no rollback promised) | `Ready`/render evidence advance only for acknowledged files; mismatching files block future projections | blocked projection-side; publication itself complete |

Non-semantic failures (network, missing bytes, codec runtime) never mutate document history and never become F2 (ADR 011 §7 / 012 §6). `status` and configuration remain fully available in every row (ADR 011 §7).

---

## 7. CLI surface (working names; final grammar stays FDR-004-open)

```text
db checkout publish [directory]        # propose as the publication action verb
```

- **`publish`** runs §1 for the whole checkout. Explicit verb; refused/batched work never publishes implicitly; no read path publishes (ADR 011 §5). Exit 0 on success **including the no-op** (`nothing to publish` when all destinations are `published`-settled or the checkout is clean — with an explicit line saying so, mirroring lane A's no-op honesty).
- **Refusals with diagnostics** (non-zero exit): `State::Pending` checkout ("create never completed: <failure>"); un-ingested `modified`/`missing` paths (named per-path, "ingest must complete before publish cannot proceed" — refuses affected work, never silently ingests); `ingested (unconfirmed)` paths (crash window — "receipts behind branch heads"); unexplained `blocked` record (naming `operation` + failure); marker/node/branch identity failures. Each diagnostic names the recovery verb (fix + re-ingest / resolve blocked / retry).
- **`status` extension** (continuing lane A's vocabulary):
  - per-path lines unchanged (`clean / ingested / ingested (unconfirmed) / modified / missing / untracked`), plus a derived `published` marker on `ingested` paths whose destination record is `outcome: published` (status derives this; publication never rewrites receipts).
  - per-destination publication lines: `publish <doc> published <publishedHeads>` / `… pending (expected <H>)` / `… blocked: <failure>` / `… upstream moved since publish` (live heads ≠ last `publishedHeads`, informational divergence).
  - the existing `blocked` surface shows publication blocks identically.
- **No `--publish` on ingest** (lane A already refuses it); no pull verb in this lane (pull reuses §2's candidate/persist mechanics minus the publish step — named as future reuse, not implemented); no `-m`/message surface (FDR 004 question 4 stays open, and a message cannot name "one change across documents" honestly).
- Verb spelling `publish` vs `commit`(retired)/`push`/`sync` remains open per FDR 004; `publish` is proposed because "publication" is ADR 011 §5's own term for this action and `commit`/`sync` mean different things today.

---

## 8. Explicit non-goals

- **No pull**: publication only; pull's candidate-merge validation is future reuse of §2/§3, not shipped here.
- **No one-shot `db import` redesign**; no `adopt`, no `by-ID` checkouts, no detach/discard verbs.
- **No watch implementation, no notifications, no implicit publication from any read path** — `blocked` records are the contract a future watch lane consumes.
- **No WASI codecs / lens host**, no lens-installation machinery beyond what lane C's contract demands for the slice.
- **No cross-document batch-merge API in big_repo/big_sync**, no distribution/transport changes (big_sync untouched), no deletion/trash semantics, no move/rename interpretation, no blob retention (ADR 013), no `.dtree`/SQLite migration of the marker, no locking machinery beyond the process-serial CLI assumption (shared with lane A; revisit with the watch lane).
- **No message/annotation surface** (`-m`), no history-rewrite anything.

---

## 9. Phased plan

1. **F1 (this doc):** design gate. STOP. No code.
2. **F2 (after lane A lands in this workspace):** `daybook_core` drawer — extend `merge_from_heads`/`merge_from_branch` with `expected_to_heads` (§2.1) + `HeadConcurrency` variant; add `prepare_merge_candidate` (§2.3); extract candidate facet validation from `validate_facets`. Targeted drawer tests only.
3. **F3:** `daybook_cli` — `publish` verb implementing §1 (batch gates, retry loop, marker `publications`/`blocked`), §7 status extension. Marker version bump coordinated with lane A's landed receipts schema.
4. **F4:** test proposal sign-off, then targeted suites (`cargo nextest` scoped `-p daybook_core -p daybook_cli`, trycmd grammar file), `clippy --all-targets --all-features` per touched crate; instrument-only commits kept separate.

**Test sketch (F4, trimmed at sign-off):** publish-after-ingest round trip with main-untouched assertion *until* publish; upstream-divergence retry (inject upstream commit between heads read and publish via a test-only hook) → retry succeeds, `attempts` recorded; exhausted-retry → blocked with staged work + preserved branch; invalid candidate (concurrent upstream facet removal) → F2 blocked, both branches verified unchanged on disk; crash-after-publish-merge (truncate `publications`) → resume publishes no duplicate change; partial multi-doc (two destinations with `--allow` docs, inject failure on second) → first remains `published`, resume skips it; step-10 no-op vs re-render cases; trycmd for grammar/refusals/status lines.

---

## 10. Questions that settle during implementation (not in this doc)

- Retry bound value; `HeadConcurrency` error-shape spelling; `prepare_merge_candidate` exact signature/snapshot encoding.
- Marker publication-record field spellings + version-bump mechanics shared with lane A's receipts landing (single coordinated bump or two — decided when the first of the two lands).
- Whether candidate validation needs a `heads`-parameterized `Projection::select` variant exposed from `pauperfuse_daybook` or the interpretation invariants are re-expressed in the CLI lane (lane C contract boundary).
- `--allow`-imported destinations (lane A creates their main docs at ingest): publication treats them as ordinary destinations; whether an import's `main` already carrying the staged content makes its publication a verified no-op is asserted by test, not pre-decided.
- Status line vocabulary exactness; whether `publish` takes `[paths]` narrowing in this slice (proposed: no — whole-checkout like ingest).

**Flagged for the operator now rather than silently decided:**
1. The single `daybook_core` API change (§2.1 param + §2.3 candidate surface) is the implementation's breaking-change footprint — confirm both are in scope for lane F when workspace time is assigned.
2. Step-10 projection-on-publish applies upstream renders to disk when they differ from staged bytes. The alternative (publish leaves disk alone until an explicit re-project/pull) contradicts ADR §6 step 9; confirming here because "publish may rewrite my files" is user-visible.
3. Publication records stay in the marker this slice (bumping shared schema twice with lane A vs once — coordination point).

---

**STOP (phase 1):** design gate only. Read-only phase honored — no code, no manifest edits implemented. Supervisor approval required before any implementation phase (which will be reassigned to a workspace when approved).