# ADR 011: Daybook checkouts, staging, and publication

- **Status:** Accepted (replaces the superseded revision; see superseded decisions in the drafts disposition ledger).
- **Supersedes:** The original ADR 011's automatic ingestion on read commands, file-deletion-to-trash mapping, separate semantic bounce branches, and unconditional crash-safety claims.
- **Depends on:** ADR 010 (generic bridge/vtree), FDRs 001–004, and ADR 007's distinct document/branch identities.
- **Related:** ADR 012 owns lens recognition, preparation, output plans, and effective authority; ADR 013 owns blob retention and GC.

## 1. Scope and responsibilities

A checkout selects Daybook content, binds projected outputs to filesystem paths, and lets ordinary tools edit those files. Daybook remains the data store. A checkout is not one replicated document, a collection-wide Automerge frontier, or an atomic multi-document commit.

The generic bridge records backend trees, offers changes, and brokers byte access. The Daybook backend interprets edits through lenses, maintains checkout branches, and publishes validated changes. The filesystem backend prepares and applies output plans against expected filesystem state. The checkout coordinates ordering and persists correspondence; neither backend inspects the other's internal state.

Correctness must not depend on a running watcher. Explicit operations discover changes missed while a watcher was absent. A watcher is an explicitly started long-lived process, not an implicit side effect of adoption or a read command. Whether it retains open store handles while idle is an implementation choice, not a correctness rule.

## 2. Checkout classes and lifecycle

### Dpath checkouts

A dpath checkout selects documents and their dpath facets through its configured selection, such as a drawer or path-prefix query. Lens outputs are allocated usable local paths according to FDR 001. A binding is durable local correspondence, not a dpath facet and not a content-hash-to-document identity rule.

A fresh checkout allocates names from the whole currently known claim/output set, independent of scan order. Established bindings remain sticky as new outputs arrive; late claimants cannot evict incumbents. The preferred `.d` spill marker is not a reserved suffix. Displaced outputs do not automatically acquire a clean name when its previous owner disappears. Literal names, platform name equivalence, file/descendant collisions, and lens sidecars must all be considered by the naming algorithm. Its exact suffix scheme remains separate technical work.

### By-ID checkouts

A `/by-id` checkout is a separate checkout class, not an extra namespace mixed into a dpath checkout's directory. Users wanting both create separate, possibly sibling, checkout directories. Its identity-oriented representation is particularly useful for agents and virtual filesystem consumers.

Creation and ingestion semantics for by-ID checkouts are deferred to a separate design. This ADR does not infer document deletion from an agent removing a generated identity path, invent a creation inbox, or claim ordinary dpath-removal rules apply there. The Daybook backend's checkout-class policy determines available operations.

### Create, adopt, import, detach

- **Create:** `db checkout` binds a new empty destination to a selected node/content set, initializes checkout state, establishes checkout branches for documents before their first projection, and materializes through the bridge. A conflicting nonempty directory is not silently taken over.
- **Adopt:** attaches an existing directory without moving files, creating documents, publishing, or starting watch. It records local candidates and proposed import destinations. Until explicit ingestion/import, candidate paths are not dpath facets of nonexistent documents.
- **Ingest new files:** creates document identities through the selected lenses and records durable bindings. Default import granularity is one file per document unless a selected compound interpretation or explicit user choice establishes a multi-file document. Equal bytes can permit blob reuse but do not merge document IDs.
- **One-shot import:** need not create or require a checkout. Repeating a drag-and-drop import may intentionally create a second document. Checkout-tracked import uses its own correspondence and recovery state; one-shot import is not automatically an idempotent ordered checkout walk.
- **Detach:** leaves filesystem files and documents intact. It must inspect pending edits, staging branches, partial publication, and identity records before archiving/removing checkout state. Detailed confirmation/export policy remains a recovery UX decision; detach must not silently destroy the only explanation or copy of pending work.

Many checkouts may select one node/document; each has its own bindings, local branches, operation state, and writer. They converge through the document backend, not by sharing one filesystem merge base.

## 3. Durable state and local controls

`.dtree` or equivalent checkout state records:

- checkout class, node and content selection;
- output identity → allocated real path bindings;
- original selected document/branch and its checkout branch;
- the heads and lens/configuration/input versions that produced each acknowledged output;
- observation evidence and pending filesystem changes;
- prepared batches, per-document staging/publication outcomes, and incomplete filesystem application;
- selected lens versions, projection provenance, and recovery/blocking status.

Schema and exact filenames are implementation choices. A dot-prefixed checkout configuration surface exposes lens preferences/overrides, selection, and import behavior. An ignore file controls eligible filesystem input. Adoption may initialize it from an existing `.gitignore`; ongoing implicit interpretation of multiple ignore configurations is not required. Any importer must preserve supported negation/path semantics or report unsupported rules rather than silently alter their meaning. Checkout/node metadata and nested checkout-owned contents are never recursively imported as ordinary input.

New eligible files become documents on explicit ingestion. A running watch automatically ingests new eligible files by default; adoption alone does not. Configuration and ignores must therefore be visible before enabling watch. Failed recognition or ingestion is not an excuse to silently ignore a file.

Projection provenance can live in SQLite state, not a literal text log. Lens installation does not automatically upgrade existing selected versions. An explicit upgrade/reselection checks for pending edits and prepares reprojection. Recognition remains a function of current inputs and configuration; retained versions and user overrides are inputs to that selection, not hidden heuristics.

## 4. Checkout branches preserve the local base

Before projecting a selected document, create its **checkout branch** from the selected version. Branches are separate documents under ADR 007. Checkout branches are locally controlled, local-only staging documents; they do not receive an origin-to-branch access delegation intended to expose them to the origin's readers. Ownership, local retention, and replication policy must explicitly implement this—not infer locality merely from absence of a delegation.

Eager creation is intentional: lazy creation after an edit could fail if read authority was revoked in the meantime. Keeping the projected base on a local branch permits preserving and interpreting local work independently of later origin access. A plain JSON mirror or shared-history deduplication might optimize storage later; neither replaces this initial preservation contract. Branch creation must capture the required base before initial projection is acknowledged.

A checkout branch does not continuously track upstream. Its history holds the checked-out base plus staged local changes and any validated upstream merges. The filesystem's acknowledged **render heads** are recorded separately: the branch can be ahead of disk after staging, normalization, or interrupted projection. Lens ingestion reads owning facets at those render heads alongside changed bytes, sibling outputs, and declared dependencies. It must not reinterpret old filesystem edits against a newer upstream state that was never shown on disk.

Ordinary edits to an existing bound output retain document identity. Changing a JPEG updates the lens's blob/metadata destinations, not a newly invented photo document; Markdown edits likewise update the existing owning content. Selection changes alone cannot split or combine document histories.

## 5. Separate observation, ingest/publication, and pull

These are semantic actions, not a final Git-like command grammar:

- **Observe/status/diff/log:** refresh observations and show local pending edits, upstream divergence, selected interpretation, missing inputs, and blocked operations. Never ingest or publish implicitly.
- **Ingest/publication action:** recognize and prepare filesystem edits, stage them on checkout branches, validate convergence with upstream, and conditionally publish. This is the intended role formerly called `commit`; final CLI spelling and message grammar belong to FDR 004.
- **Pull:** merge upstream into checkout branches if valid and project locally. It never publishes checkout-local work upstream. Refuse affected pull work if filesystem edits have not been ingested; do not silently ingest them.
- **Discard:** explicitly replace pending bound-file edits with the currently selected Daybook projection, not necessarily the old render. It changes no document history. Unbound files remain; conflicts with them reject the new projection rather than authorize their deletion.
- **Watch:** react to both filesystem changes and upstream document changes, staging local work first and then orchestrating publication/pull/projection as applicable. Read commands are not watcher ticks.

Explicit lens switching/upgrading refuses dirty affected interpretations or entry sets until ingestion or discard. Unrelated clean entry sets need not be blocked by this check. Recognition can choose a new preferred lens from current context, but safe ownership and historical render evidence remain necessary to interpret pending edits.

## 6. Preparation and publication sequence

A batch is an explicit operation scope, not a global document transaction. Its normal ingest path is:

1. Observe filesystem and Daybook state; resolve bindings and eligible new files.
2. Gather recognition evidence, select proposals, and prepare facet/document operations for the entire requested batch. No document edits are published during this preparation.
3. Present selected interpretations and the prepared plan for interactive review where applicable. Recheck input/configuration/version/authority assumptions before application; stale review cannot authorize different inputs.
4. Stage validated edits on local checkout branches, recording progress and preserving all inputs. Failures here can be recovered locally; no upstream publication begins until staging and merge readiness succeed for the batch.
5. For each destination, merge current upstream history into an **in-memory Automerge candidate** with the checkout branch and validate its facets/schema. No Keyhive-visible candidate document ID is needed. Persist only a valid candidate to the checkout branch.
6. Validate every candidate in the batch before publishing any destination. A known invalid candidate prevents publication from starting.
7. Publish each checkout branch to its upstream destination **only if upstream is still at the expected heads** used for validation. The intended publication is a fast-forward of the already merged valid state, not an unchecked fresh merge into main.
8. If heads changed, refresh, merge/validate again, and retry conditionally. Retries have a finite bound; exhaustion blocks the checkout with staged work preserved. The numeric limit is an implementation setting, not an unbounded retry loop.
9. Record successful and pending destinations. Prepare the resulting filesystem output plan and apply it through the receiving backend; advance render bindings only for actual acknowledged outcomes.

Pull uses the candidate merge/validation and projection portions without publication to upstream. A candidate that fails validation does **not** mean either existing branch is invalid: individually valid histories can have an invalid combination. Preserve both stored states and report the failed candidate.

No rollback is promised. Expected-head publication can partly succeed across documents if failures occur after earlier writes. Persist operation identities and per-document results so recovery resumes without duplicating imports or edits. A future batch merge API can reduce the race window; it does not by itself provide cross-document IO atomicity.

## 7. Failure, authority, and resolution

A preparation/recognition/codec failure blocks the checkout's automatic ingestion and projection. Watch cannot silently fall back or continue changing unrelated files behind the user's recovery work. Observation, status, configuration changes, inspection, retry, exclusion, and explicit recovery remain available. A partially published batch similarly blocks automatic activity until recovered/resolved.

Invalid facets produced by a lens are preparation errors. Invalid combinations discovered while merging valid local/upstream history are merge-if-valid failures. Neither needs a new bounce document: checkout branches already preserve local work. Resolution uses ordinary branch/merge surfaces integrated with checkout bindings, not a special pick-only conflict viewer or inline conflict-marker rendering. This supersedes the old `/tmp/conflicts/<facet-id>` branch scheme.

Filesystem bytes remain untouched when ingestion fails. 'Disk always shows the last-good render' is withdrawn: disk may hold the user's un-ingested or staged work. No conflict markers are injected into ordinary files.

Effective write access for a lens spans **all input documents**, including recognition-only context. Projected permissions reflect that weakest access, for example through Unix mode bits. Permission bits are a guard, not the authority boundary: users can chmod or replace files through parent directories. Daybook verifies authority when writing/publishing. Origin authority loss does not erase locally controlled staging work. Read/write denial, unavailable inputs, and codec runtime failures are not semantic merge conflicts.

On authority changes, refresh visible permission evidence. Preserve pending bytes and report denied/unpublished work rather than overwrite, discard, or create a new document. Successful publication and subsequent projection leave the checkout branch reusable; retain old render heads until outputs are actually acknowledged, and never delete the branch merely because one batch succeeded.

## 8. Deletion and moves

Filesystem deletion never automatically deletes its Daybook document or creates `/trash` facets. Explicit document deletion/trash is a separate product operation. Removing a sole ordinary dpath output removes the associated dpath projection/address through Daybook policy; removing a member of a compound output is offered to its lens. If the lens cannot represent that edit, preserve the pending removal and report the problem rather than delete the whole document or silently restore the file.

Removing a projection/dpath facet preserves other addresses and document history. Track-only adopted-file removal updates local observation only; there is no document to modify. By-ID mutation semantics remain outside this ADR.

Remote output removal only removes a bound target if it remains unchanged from acknowledged application. Provenance alone is insufficient. Unbound files and backend bookkeeping are untouched. Apply removals deepest-first and nonrecursively; unexpected children obstruct a removal rather than being recursively erased.

Moves are interpreted using bindings, render bases, and any credible available evidence. Snapshots of identical files can be ambiguous: retain the existing binding at a path, not infer identity from a hash. Better filesystem heuristics may improve recognition without placing Daybook identities in generic inode state. The CLI/GUI must expose consequential ambiguous correspondence and permit explicit correction.

A dpath facet can live in one document while referencing content in another. Moving its address can affect the holder's dpath facet, while content edits affect the referenced owning facets. ADR 012 requires declared write destinations and lens-wide authority; such operations can involve multiple documents and the publication rules above still apply.

## 9. Filesystem plans, bulk work, and concurrency

Prepare a complete output plan and its required bytes outside the visible checkout. Preflight ownership, kinds, collisions, permissions, dirty targets, and unbound obstructions before changing files. Known failure rejects the requested projection batch. Then use backend-supported staging/moves and atomic per-file replacement to minimize the mutation window. This does not promise one atomic filesystem namespace switch for arbitrary directory trees.

Unexpected filesystem failure records partial progress for verification and resume. Files changed by external tools during preparation must be checked again against expected state before replacement. A SQLite transaction can update checkout metadata; it cannot make document writes and filesystem moves one transaction.

Initial materialization, large reports, and transfer plans use canonical ordered walks and durable progress. A cursor includes the observation/plan generation; if the tree changed, restart or reconcile that plan rather than blindly resume at a path. Completed operations must be identifiable/idempotent, but a cursor alone is not recovery. One-shot import may use its own progress records and must not be forced into a fictitious checkout lifecycle.

One writer operates a checkout at a time. A lock must cover the operation's semantic critical section, not only a brief SQLite transaction between external writes. The exact locking mechanism is implementation work; SQLite write locking alone must not be presumed to serialize filesystem work outside its transaction. Watch notifications coalesce into scans; event storms do not cause one full cycle per notification. Scans are authoritative. Upstream notifications or cheap heads polling are triggering mechanisms, not liveness guarantees.

## 10. Retention and recovery

Reps are current row sets, so no DAG GC is needed. Detaching a rep can drop its rows, but bindings, local branches, and incomplete operation records need lifecycle-aware cleanup. Blob GC/backup is ADR 013's concern; staging document history does not guarantee blob bytes survive deletion of their only external copy. Keep production inputs/codecs needed by active references or report unavailability explicitly—never fabricate stub files.

Store corruption is not guaranteed to heal by rehashing: a lost binding can lose identity, and a lost checkout branch can lose local work. Rebuild observations where possible, preserve filesystem bytes, and require conservative recovery for missing correspondence. Ordinary branch resolution and detach/recovery UX must expose retained local work, rather than aggressively pruning 'temporary' branches.

## 11. Remaining technical work

Low-fidelity Rust experiments should establish: multi-document batch preparation/staging, merge-if-valid with expected-head publication, interrupted imports without duplicate identities, transformed lens output, loss of origin access, partial filesystem plan recovery, and one document with multiple output slots. Verify checkout-local branch ownership/retention against the actual authority implementation before claiming it survives revocation.

Exact branch storage, operation IDs, cursor formats, lock mechanism, ignore/config filenames, by-ID write semantics, codec retention, and resolution UI remain separate implementation or follow-up designs. They must respect the boundaries above; they are not permission to restore read-triggered ingestion, trash-on-file-delete, or implicit cross-document rollback.

## Appendix (landed revisions)

Publication records live in the checkout marker (v3): per-destination `Publication { seq, doc, expectedHeads, branchHeads, publishedHeads, outcome (published|refused|blocked), failure, attempts }` with `Blocked { operation: ingest|publish, failure }`; markers are local-only files with `deny_unknown_fields` versioning — an old CLI can never silently ignore publication state. CAS on the upstream side is expressed through `merge_from_heads(expected_to_heads)` with typed `HeadConcurrency` refusal (heads compare + merge happen inside one doc lock); validation-before-persist uses `prepare_merge_candidate`/`validate_merge_candidate`. `update_at_heads(Some(heads))` is a transaction basis, not a refusal CAS — publication never leans on it. Retry bound is 3; exhaustion records a blocked publication and the explicit retry resolves it when upstream stops moving.
