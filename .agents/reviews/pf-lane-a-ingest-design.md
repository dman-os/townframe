# Lane A — Checkout INGESTION: full design proposal (phase 1, no code)

**Lane:** A (ingestion: file edits → document operations on the checkout-local branch), workspace `townframe-pfuse-a` only.
**Contract sources:** ADR 010 (bridge/vtree), ADR 011 (checkouts/staging/publication), ADR 012 (lenses), consolidated disposition ledger `.agents/drafts/010-012-disposition.md` (sibling repo `townframe-2`). This doc validates those against the code that exists in *this* workspace and proposes the ingest vertical slice.

---

## 0. What exists today (validated against the code)

- **CLI** (`src/daybook_cli/cmds/checkout.rs`): `CheckoutCommands::{Create, Status}` only. Unix-only `unix` module. Marker file `.daybook-checkout` with `Checkout { version, id, node_path, node_key, drawer, projection, basis (main heads at create), branch = "/tmp/checkout/<id>", branch_id, render_heads, state }`; `State::Pending { failure }` | `Ready { length, digest, generation }` — the Ready evidence is the *acknowledged projection* install evidence (blake3 length+digest of the file at last projection) plus the vtree generation. Vtree sqlite lives under `<node_root>/local_state/checkouts/<id>/vtree.sqlite`.
- **Create path**: `Projection::select` (whole-doc dpath → Body → single `text/plain` Note, ref must be `self`, unpinned, ASCII) → `preflight` (real dirs, free output, marker reservation) → `drawer.create_checkout_branch(doc, "/tmp/checkout/<id>", "main", basis)` → `get_branch_ref` asserts `BranchKind::Local`, records `branch_id` → `get_doc_bundle_at_branch` records `render_heads` → `Daybook` producer `observe()` → vtree `replace` → scan → one `FilePut { expected: Absent }` → `TokioFs::prepare` (stages bytes outside checkout, verifies expected state) → `apply()` (rename + re-verify evidence) → `Ready` marker.
- **Status path**: nearest-marker `discover` from cwd, node/drawer identity re-verification branch ref/branch_id integrity, then per-file evidence comparison: `clean` / `modified` (evidence vs Ready evidence) / `missing`, plus a recursive `untracked` scan of everything else (marker and, in Pending state, the bound path are excluded). No ingestion, no publication — pure observation, per ADR 011 §5.
- **Drawer** (`src/daybook_core/drawer/`): `create_checkout_branch` is the sole creator of `BranchAuthority::Checkout` / `BranchKind::Local` branches (must be `/tmp/*`; no document-authority inheritance; pending-group-only parents make the local user sole authority). `update_at_heads(patch, branch, heads:? Some|None)` is the branch facet-write surface (`DocPatch`), validates facets against plug manifests + reference manifests before any transaction. `get_doc_with_facets_at_branch_heads(doc, branch, heads, facet keys)` reads exact heads. `merge_from_heads/merge_from_branch`, `update_batch`, `batch_add` exist. `Note`/`Body`/dpath are not system-managed, so `FacetWriteScope::User` writes are legal.
- **pauperfuse**: `RelPath` + `TokioFs` (`observe` → `ExpectedFile`, `prepare`/`apply`/`cleanup` with sibling 0700 staging dir, same-dev checks, per-file rename + verify, cancellation is not rollback) and `VtreeStore` (relational tree, `replace`, ordered `scan`, explicit generations). `pauperfuse_daybook::Daybook` is a `Producer` over an independently owned checkout branch; `Source` = `{ backend, output{doc,facet,branch}, version{heads} }`; `open()` reads the facet *exactly at* the selected heads.

This slice is deliberately a **single-document checkout with one whole-doc dpath whose Body references one `text/plain` Note** — i.e., the *one supported representation* is raw UTF-8 text on disk ↔ `Note{mime:"text/plain", content}` on the checkout-local branch. `Projection` is simultaneously the production recipe and the round-trip identity.

There is **no ingest verb anywhere today** — `grep ingest` hits only the status doc-comment and unrelated blob code. Lane A adds it.

---

## 1. Ingest trigger surface

New subcommand on `CheckoutCommands` (spelling per FDR 004 is explicitly unfrozen; this slice picks the working name):

```text
db checkout ingest [directory] [--allow <path> ...]
```

- **Explicit verb; no watch; no publish.** Nothing on any read path (`status`, `ls`, `cat`, …) ingests. ADR 011 §1/§5: correctness never depends on a watcher; this slice ships none.
- [directory] resolves through the existing nearest-marker `discover` (cwd default), then re-verifies node identity (`verify_node`) and marker integrity exactly as `Status` does.
- Exit code: success (including pure no-op) is `0`; refused/blocked batch is non-zero.
- Progress lines on stdout, one per fact: classified evidence (`missing <path>`, `untracked <path>`), what was staged (`ingested <path> -> <branch heads summary>`), created documents (from `--allow`), the no-op line (`clean <path>; nothing to ingest`), and a final explicit statement that nothing was published upstream (see §6).
- A `--publish` flag is **not** offered in this slice; publication refuses with a dedicated diagnostic if ever requested before the publication lane lands (see §6 and §9).
- Invocation is *the* single writer for the checkout for its duration (ADR 011 §9). No new lock machinery is proposed in this slice; the process-serial CLI usage is the current precedent (flagged in §11 / residual risks).

## 2. Whole-checkout diff → one prepared batch

Ingest computes the entire checkout's classification **before** any document operation is staged (ADR 011 §6 steps 1–2; the batch is the explicit operation scope).

### 2.1 Classification of the bound output path

For the single bound path `projection.path` (native `root/<to_native_path(path)>`):

| Case | Criterion | Batch action |
|---|---|---|
| `clean` | current `TokioFs::observe(path)` evidence == acknowledged render evidence (`Ready.{length,digest}`) **and** no pending receipt diverges from it (§5) | no document operation; no receipt movement |
| `staged` | current evidence == receipt's file evidence **and** receipt's `branch_heads` == live checkout-branch heads | no-op (idempotent repeat) |
| `modified` | evidence differs from both render evidence and (any) receipt evidence | **enters the batch** |
| `staged, file since changed further` | receipt exists, evidence != receipt evidence | `modified` → enters batch (stages a further change, or a revert if file moved back) |
| `missing` | `observe` → `Absent` | reported, **excluded** from the batch, no facet removal, bytes/document untouched (deletion is out of scope, §9) |
| `obstructed` | entry exists but is a symlink/directory, or a parent link flipped | preparation failure, blocks the batch |

The `modified`-vs-`staged` boundary needs live drawer readback (`get_branch_heads` for the branch + facet at heads) — status already resolves the branch ref, so this is the same evidence chain, extended.

### 2.2 Untracked classification

- Everything under the checkout root other than: the marker file, the bound path, and checkout-owned bookkeeping (`local_state` is under the node root, not the checkout root — nothing else is reserved in this slice). Empty directories are listed as `untracked <dir>/` but cannot be ingested by the text representation; no ignore file in this slice (ADR 011 §3 ignore-file is separate technical work).
- **Never auto-ingested.** Default ingest leaves them exactly where they are and reports them.
- **Per-path opt-in:** `--allow <path>` (repeatable). Requirements for an allowed path to *enter* the batch as a new-document import:
  1. classified `untracked` (not bound/tracked, not the marker, not a directory);
  2. a regular file whose bytes are valid UTF-8 (the only supported representation) — non-UTF-8 is an explicit preparation failure naming the path, never a silent ignore (ADR 011 §3, ADR 012 §5);
  3. its relative path converts losslessly via `TokioFs::from_native_path` → a legal `RelPath` that passes the same `validate_output` rules (not marker-conflicting, not root).
- **Opt-in import shape** (uses only existing APIs): propose document via the same `Projection` interpretation — `drawer.add(AddDocArgs { branch: "main", facets: {Note{mime:text/plain, content}, Body{order:[self note]}, Dpath('<file path>') → {}}, user_path: None })` (this is exactly the shape the existing e2e `note_document` builds, so the pipeline is proven), then `create_checkout_branch` for the new document, then record the binding + receipt. Blob reuse for identical bytes "may permit blob reuse but never merges document IDs" (ADR 011 §2) — irrelevant here since Note content is an inline text facet in this slice, not a blob.
- **Batch gate (ADR 012 §6):** *all* entries (modified + allowed imports) must prepare successfully before any staging begins. One non-UTF-8 file under `--allow`, one obstructed path, one drawer resolution failure ⇒ the entire invocation stages nothing.

## 3. Reverse receiver: staging FROM checkout files

This is the mirror of `TokioFs::prepare` (which reads bytes from a `Source`, verifies expected state, stages, applies with rename+re-verify). Proposed as a small new piece in `pauperfuse::backends::tokio_fs` (lane A only), shaped on `prepare`:

- **For each take `{ path, expected: ExpectedFile }`** (expected = evidence captured during classification):
  1. `checked_destination`-equivalent recheck: parent chain are real dirs on the checkout filesystem (reuses existing validation);
  2. capture evidence → read bytes, hashing on the fly, so **bytes and digest are the same read** — a mid-read/mid-batch change is caught as `FileError::Changed` at the final recheck;
  3. **final expected-state recheck immediately before the drawer write**: re-`observe` each take; any mismatch aborts the batch before staging (ADR 010 §5 step 2 / "checked again against expected state before replacement"). For the one-facet slice the "staged copy" step of TokioFs prepare is unnecessary — the facet content is the bytes themselves, re-verifiable by re-hash; a byte-staging directory is only needed once lens operations buffer transformed payloads (deliberately not built now, see settled questions).
- **Obstruction/collision handling** is preparation failure (blocking, §7): non-regular files, unreadable files, changed-under-us, non-UTF-8, an allowed path that turned out to be bound/tracked or a directory, byte-key/naming failures. Nothing visible is modified on any of these — ingest never writes to the filesystem.
- **No deletion:** there is no remove path in the reverse receiver in this slice. A missing bound file produces an observation, never `facets_remove`, never a trash doc, never a dpath-facet removal (ADR 011 §8 as amended by the disposition ledger).

## 4. Lens-reverse step: file edit → facet operation (ADR 012 §6 audit)

The ADR-012 ingestion contract requires specific inputs. Precisely what lane A supplies for the one supported representation:

| ADR 012 §6 input | Supplied by |
|---|---|
| changed bytes/path/kind | `FileEvidence {length, digest}` + bytes (single streamed read), `RelPath`, regular-file kind |
| bound output slot | the marker's single binding: `projection.path` ↔ `projection.document` / `projection.facet` (Note) at `branch` |
| **recorded render heads** | marker `render_heads` — the heads at which the last *acknowledged projection* was produced (bundle heads recorded by `create`, updated by future re-projection, never by ingest) |
| **production recipe** | the serialized `Projection { document, facet, path }` in the marker — for this representation the recipe is exactly: "raw UTF-8 bytes of the Note facet referenced by the whole-doc dpath, rendered by the `pauperfuse_daybook::Daybook` producer of this crate version". An explicit recipe/lens-version field on the receipt is proposed but its exact spelling stays an implementation choice (§10) |
| **facet state at those heads** | `drawer.get_doc_with_facets_at_branch_heads(document, branch, render_heads, [projection.facet])` → `validate_note` — the Note as last shown on disk. This is the round-trip base: ingest must not reinterpret old filesystem edits against a state never shown on disk (ADR 011 §4) |
| sibling output observations | single-output slice: none (the untracked scan is reported but is not lens context) |
| declared inputs | none beyond the Note (Body/dpath are identity scaffolding, unchanged by content edits) |

**Inverse operation (raw UTF-8 Note):**

- Decode file bytes as UTF-8; failure = preparation failure (explicit diagnostic; ADR 012 §13 byte-key policy covers names, not content).
- Candidate facet value: `WellKnownFacet::Note(Note { mime: "text/plain", content })`.
- **No-op rule (round-trip stability, ADR 012 §8):** read the Note **at the live checkout-branch heads**; if it equals the candidate, there is no operation and no receipt movement. The branch — base + staged local work — is the staging surface, so this rule both (a) suppresses repeats of an already-staged ingest and (b) *stages a revert* when the file is put back to render-content while the branch still carries older staged bytes. Comparing against render-head state instead would re-stage duplicates; the choice of branch-current state as comparison basis is stated here so reviewers can veto it (§11 Q3).
- Output: an optional `DocPatch { id: projection.document, facets_set: { projection.facet → candidate } }; facets_remove: []` — prepared, never written by this step. Schema conformance of the prepared facet is enforced by the drawer's `validate_facets` inside `update_at_heads`, which runs **before** any transaction — so the all-batch prepare gate is preserved (a schema-invalid prepared facet fails staging with nothing visible changed and nothing partially staged for that doc). For `--allow` imports, `batch_add_inner` performs the equivalent all-then-apply validation.
- `user_path: None` (ingest is not a user-authored drawer edit; provenance provenance/refinement stays open, §10).

**Deliberate scope restriction:** upstream `main` is **not read and not merged** during ingest in this slice. Movement of upstream does not block staging (staging is local-first, ADR 011 §6 steps 4–5); reconciliation with upstream belongs to the merge-if-valid candidate step of the publication/pull lanes. Ingest never publishes.

## 5. Receipts: durable file-evidence ↔ branch-heads mapping

**Where:** the checkout marker (`.daybook-checkout`), extended — *not* the vtree store, *not* an upstream acknowledgement (explicitly scoped as local fact about staging, per the task). The marker's atomic-rename replacement (`replace_marker`) and `deny_unknown_fields` versioning already provide the durability/rollback story within one checkout.

Proposed shape (schema is versioned via the existing `version` bump):

```jsonc
// added to Checkout:
"receipts": [ {
  "path": "notes/hello.md",          // RelPath key (same escaping as projection.path)
  "file": { "length": 0, "digest": "<hex blake3>" },  // evidence of the bytes ingested
  "branch_heads": ["<serialized ChangeHashSet>"],     // heads AFTER staging this receipt
  "sequence": 1                       // per-checkout monotonic; last-writer-wins per path
} ]
// State::Ready gains an optional blocked record so automatic flows can be durably "blocked":
// "blocked": { "operation": "ingest", "failure": "<diagnostic>" } | absent
```

- One receipt **per bound path + per imported path**; re-ingest of the same path replaces that path's receipt (monotonic `sequence` makes stale markers detectable).
- **Crash window:** drawer staged but marker not yet replaced ⇒ receipt absent while branch heads moved. Recovery is *recomputation*, not replay: the next ingest recomputes against live branch state (no-op rule makes it idempotent); `status` reports the divergence as `staged (unconfirmed)` and treats mismatched receipt-branch-heads-vs-live-heads as observation, not corruption. `status` must resolve live branch heads (`get_branch_heads` via the branch ref's `branch_doc_id`) to make this exact — status already resolves the ref; this extends it.
- **How status becomes exact** (replacing the current loose render-only comparison):

  | Status line | Meaning |
  |---|---|
  | `clean <path>` | evidence == acknowledged render evidence and no diverging receipt |
  | `ingested <path>` | evidence == receipt evidence and receipt.branch_heads == live branch heads (staged, **unpublished**) |
  | `ingested (unconfirmed) <path>` | staged on branch but receipt missing/behind (crash window or out-of-band branch movement) |
  | `modified <path>` | evidence differs from receipt and from render |
  | `missing <path>` | bound path absent from disk |
  | `untracked <path>` | everything else under root |

  Note `ingested` outranks `modified` only when both evidence *and* heads match — this is what makes "status is exact" true rather than approximate: a staged-but-not-yet-receipted state can no longer masquerade as modified, and a modified-after-ingest state can no longer masquerade as ingested.
- `State::Ready { length, digest, generation }` keeps meaning *acknowledged render*; ingest never advances it (no projection happens during ingest). `State::Pending { failure }` stays create-phase; ingest blocking records go into `blocked` on `Ready` (and ingest refuses to run at all on a `Pending` checkout).

## 6. Commit to the checkout branch; publication refused

- Staged through existing drawer APIs **at the recorded branch only**:
  - modified bound path → `drawer.update_at_heads(patch, BranchPath::new(&checkout.branch), Some(live_branch_heads))` — the `Some(heads)` compare-and-swap asserts the exact basis we read facets at; combined with the single-writer rule this gives the ADR 011 expected-head discipline at staging granularity. After success, live heads are re-read for the receipt.
  - `--allow` import → `drawer.add(...)` then `drawer.create_checkout_branch(...)` for the new document, then receipt + binding recording. `create_checkout_branch` remains the *sole local authority* creator (`BranchAuthority::Checkout`, `/tmp/*`, local-only) — no delegation is created toward any origin; the checkout-local branch does not appear in the replicated entry (verified today by the `BranchKind::Local` assert in `create`).
- **Never touches origin:** no `merge_from_heads`/`merge_from_branch`, no big_sync publication, no access delegation. Ingest reads the checkout-local branch and the drawer's local branch refs only.
- **Publication refused with explicit diagnostic:** there is no publish verb in this slice. The ingest output ends with an explicit non-published statement ("staged on checkout-local branch; not published upstream"), and `status` reports `ingested … unpublished`. If a publish attempt is somehow made (future verb landing early, or a stray flag), it is refused outright with a diagnostic naming the publication lane as not-landed — the refusal is a first-class, testable behavior, not an accident of a missing flag.

## 7. Failure taxonomy (whole-checkout failure-blocking rule)

Failures are classified at three seams, all of which **block the checkout's automatic flows** per ADR 011 §7 while leaving observation/status/config/inspection/retry fully available:

1. **Preparation failures** (recognition/codec/validation): non-UTF-8 bytes, non-regular entry, unreadable file, obstructed/invalid `--allow` target, missing render-head facet state, prepared-facet schema rejection, marker/drawer integrity violations. ⇒ **nothing staged**, filesystem bytes untouched, `blocked{operation:"ingest", failure}` recorded in the marker, non-zero exit with the per-path diagnostics. Retrying after fixing input is the recovery; `--allow`/exclusion surface is per-path opt-in (exclusion = "don't pass the flag"; no ignore-file yet).
2. **Stale/CAS failures** (expected-head violations): file changed between evidence capture and staging (`Changed` recheck), or live branch heads moved between readback and `update_at_heads`. ⇒ bounded: refresh classification and retry **once** in the same invocation; a second failure records `blocked` and exits non-zero. (Bounded-retry rule mirrors ADR 011 §6 step 8; the number stays an implementation setting, §10.)
3. **Repository/runtime failures** (drawer errors, io): propagate as errors; best-effort `blocked` record if the marker can be written; staging already-committed docs in a multi-doc batch (only possible with `--allow`) keep their per-path receipts — per-document outcomes recorded, remaining work reported, consistent with "no rollback promised; per-document results persisted" (ADR 011 §6/§10).
- **Never**: swallow into a silent skip, fall back to another interpretation, delete bytes, or continue automatic activity while `blocked` is set. Source bytes on disk are always preserved by construction (ingest writes nothing to disk).

## 8. Test matrix

Precedent: real-node in-crate e2e in `cmds/checkout/unix/tests.rs` (big-stack helper, `daybook_core::test_support::test_cx`, real drawer, real tempdir checkout root) and the trycmd blackbox suite in `src/daybook_cli/e2e`. **No fake producers as proof**: unit tests exercise real files/real drawers; the producer adapter is the real `Daybook`. No `skip`/`ignore`, no invented timeouts (nextest owns hard timeouts).

**Unit (in-crate):**

- `pauperfuse_daybook` (or a new lane-A module there): inverse-operation builder — UTF-8 decode success; non-UTF-8 rejection with path context; round-trip no-op against branch-current Note; revert case (file back to render bytes while branch carries staged bytes ⇒ patch present); Note-shape/mime pinned to `text/plain`.
- `pauperfuse/backends/tokio_fs`: reverse-evidence collection — evidence+bytes from one read; mid-batch change detected at final recheck; non-regular entries rejected; no-removal invariant (missing path yields no remove op).
- `daybook_core/drawer` (targeted, not full suite): `update_at_heads` staging at `Some(live heads)` on a `BranchAuthority::Checkout` branch; assert `BranchKind::Local` retained and main untouched.

**Real-node e2e (`cmds/checkout/unix/tests.rs`, plus trycmd for grammar/diagnostics):**

1. create → edit file → `ingest` → status `ingested`; drawer doc on checkout branch has the new Note; **main heads byte-identical to marker `basis`** (never-origin assertion).
2. round-trip: immediately re-`ingest` → no-op; branch heads and receipts unchanged.
3. staged work then revert file to render bytes → `ingest` stages the revert (branch Note returns to render bytes).
4. non-UTF-8 edit → refused; `blocked` recorded; file bytes intact; facet unchanged; subsequent fixed-file re-ingest succeeds and clears `blocked`.
5. delete bound file → `missing` in status; `ingest` reports and excludes; document facet untouched; no receipts churn.
6. untracked default refusal (`untracked` reported, nothing staged); `--allow stray.md` → new document with Note/Body/dpath at the file path, checkout branch created, binding + receipt recorded, status shows the formerly-stray path as ingested/clean thereafter; `--allow` on an already-tracked path refused.
7. batch gate: one valid edit + one non-UTF-8 `--allow` ⇒ **nothing at all** staged, block reported per-path.
8. change during ingest (file rewritten between classification and staging via an injected hook point in the reverse receiver) → refused cleanly, `blocked` recorded, bytes intact.
9. failure blocking + availability: after any blocked ingest, `status` still works and reports exact state; automatic-flow-blocking is asserted by checking `blocked` presence (the only automatic flow in this slice is none — the record is the contract for later lanes).
10. crash-window: stage-without-marker (simulate by truncating/rewinding receipts behind live heads) → `ingested (unconfirmed)` in status; next ingest recovers by recomputation without duplicate staging.
11. trycmd files: CLI grammar, exit codes, diagnostics text, `status` exact lines after each step (a full workflow file mirroring `e2e/edit` precedent).

**Explicitly *not* proof-by:** fake/stub producers, snapshot-only tests without real node state, ignored tests, hand-rolled sleeps.

## 9. Explicit non-goals (this lane)

- No watch daemon/process; no implicit ingestion from any read path; no notification plumbing.
- No publication, no pull, no upstream merge-if-valid candidate validation, no `--publish`, no origin/delegation touching (publication is its own lane; ingest ends at staged receipts).
- No deletion semantics, no trash, no dpath-facet removal, no move/rename interpretation (a moved bound file shows as `missing` + `untracked`).
- No compound/multi-output lenses, selective dpath facets, cross-document Body references, blob/image lenses, WASI codecs, by-ID checkouts, adopt/one-shot import/detach/discard verbs, ignore file, lens upgrade/reselection machinery.
- No re-projection/re-render after ingest; render evidence stays at the last acknowledged projection.
- No locking machinery beyond the process-serial CLI assumption (flagged, §11 Q5).
- Nested/multiple checkouts over one node: only nearest-marker discovery, already existing.

## 10. Proposed phased plan — and questions settled *during* implementation, not in this doc

**Phases (already implied by the task):**

1. (this doc) design gate.
2. Implementation: §1 verb → §2 diff/classification → §3 reverse receiver → §4 inverse ops → §6 staging → §5 receipts/status, with §7 taxonomy threaded through. Each step lands with its targeted unit tests; clippy per touched crate.
3. Test proposal for sign-off: the §8 matrix trimmed/confirmed against the actual touch points, with new trycmd files listed before writing them.
4. Verification/cleanup: full targeted suites (`cargo nextest -p pauperfuse -p pauperfuse_daybook -p daybook_core` scoped, plus `-p daybook_cli` e2e), `CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target` exported, no full-workspace runs; instrument-only commits kept separate per AGENTS.md.

**Settle during implementation (not blocking design):**

- Exact JSON field names/spellings on the marker (`receipts`, `blocked`, `sequence`) and the marker `version` bump mechanics.
- Exact CLI diagnostic wording and status-line vocabulary (`ingested` vs `staged`).
- An upper size bound for the raw-UTF-8-Note representation: oversized non-blob text gets an explicit preparation failure rather than unbounded in-memory content (the value lives inside the facet JSON, so a cap protects the Automerge doc, not just RAM). Default proposed: refuse > a few MiB with a "representation cannot represent this file" diagnostic.
- Single-retry bound for CAS/stale failures and its exact hook point.
- Whether `user_path` should carry an ingest provenance value (e.g. a checkout marker reference) to appear in dmeta.
- Where the pre-staging facet-schema validation for `--allow` imports reuses `batch_add_inner` versus duplicating `validate_facets` (must not duplicate logic; reuse is preferred).

**Flagged here for the operator rather than silently decided (§11 below also lists them):**

1. `missing` bound file: exclude-and-report vs block-the-batch — proposed **exclude-and-report** (deletion semantics live elsewhere; ADR 011 §8 keeps it as observation). Low risk, but it makes `ingest` succeed with pending odd state.
2. `--allow` untracked import ships in this lane alongside modified-file ingestion, or is deferred to a later slice. Proposed: **ships** (it is small given `drawer.add` + `create_checkout_branch` exist and are test-proven), but it is the largest scope element — confirm.
3. Round-trip comparison basis = **live checkout-branch heads** (not render heads). Stated explicitly in §4 because it deviates from a naive reading of "facet state at render heads".
4. Oversize cap value for raw text.

## 11. Open design questions routed to the supervisor/operator

1. **Missing bound file during explicit ingest** — exclude-and-report (proposed) vs preparation failure blocking the batch. Affects whether `ingest` is ever green while the checkout shows `missing`.
2. **`--allow` scope** — new-document import in this slice (proposed) vs modified-files only. If deferred, `--allow` spelling is dropped from phase 2.
3. **Round-trip/no-op comparison basis** — live branch heads (proposed) vs render heads. The former handles reverts and dedupes; the latter is the literal ADR-012 base. I believe branch-current is strictly more correct for a staging surface; veto here before implementation.
4. **Raw-text size cap** — value and diagnostic; alternatively defer the cap entirely for this slice and rely on the facet-JSON failure mode (worse; flagged).
5. **Single-writer enforcement** — accept "process-serial CLI" as the de-facto lock for this slice, or add a checkout-local lock file now (ADR 011 §9 says the mechanism is open implementation work; I propose accepting the CLI-serial assumption and revisiting with the watch lane).

---

**STOP (phase 1):** design gate only. No code implemented. Awaiting supervisor approval before phase 2.