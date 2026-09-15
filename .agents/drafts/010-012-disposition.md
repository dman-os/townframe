# ADRs 010–012: consolidated fidelity and decision ledger

Replacements: `010-vtree-store.md`, `011-checkouts-reconciliation.md`, `012-lenses.md` in this directory. Originals under `docs/adrs/` remain untouched. This ledger supersedes the earlier triage questions; `010-fidelity.md` and `010-unison-audit.md` are historical working notes, not the final decisions.

The drafts are architectural contracts, not assertions that existing Rust code implements them. Their remaining technical choices are explicit. The TypeScript demo is a limited experiment (single-output text fixtures, supplied move evidence, in-memory state), not a completed recovery or selector implementation.

## ADR 010 — source sections accounted for

| Original section/detail | Replacement disposition |
|---|---|
| §1 revision history; rejection of hashed DirNode DAG | Retained in §§1–2: relational path observations avoid rendering to discover change. |
| §1 backend-vs-rep / rep-vs-rep diffs | Retained as ordered observations/comparison, with separate applied correspondence. |
| §1 blob fast path, watcher stat cache | Retained in §§7–8, but unconditional 'never copy/hash a large blob' removed as unsafe/untrue guarantee. |
| §1 availability stubs | Explicitly removed by user decision; lazy producer references are not missing-byte placeholders. |
| §1 local-only tokens/cache | Retained. Rendered cache bytes never become synced blob artifacts; document/blob backends own their real replication. |
| §2.1 bridge-only role; backend-owned tracking; broker usable by WASI | Retained and expanded for on-demand producer/version resolution. |
| §2.2 rep rows, generation, directory rows, no historical versions | Retained, including SQL sketch and O(1) generation indicator. 'Rep is the merge base' superseded by separate acknowledged correspondence. |
| §2.3 origin/content/stat/claim; symlink equality; multihash encoding | Retained; provenance does not authorize dirty-file deletion. Stable output identity separated from production/content identity. |
| §2.3 per-path recipes and drift continuity | Retained as output-specific dependencies, not one-facet-only restriction. Verification/native result prevents false identity continuity. |
| §2.3 'any receiver equality policy is correct' | Withdrawn: unsafe Current can discard work. Receiver must respect correspondence and expected state. |
| §2.4 availability | Replaced with explicit unavailable-input/error versus virtual production-reference semantics. |
| §2.5 ByReference/ProduceAgain/CopyBytes; device-local render | Retained; immutable source alone no longer justifies writable hardlink aliases. Stream rather than mandate whole-file buffer. |
| §3 schema, encoded paths, WITHOUT ROWID, versioned blobs, query files/plan twins, sqlite feature | Retained in §8; avail removed, future ledger/producer-reference encoding deliberately not fabricated. |
| §3 stat threshold, dirty identity, TouchStat, directory stat exclusion | Retained as policy in §§3/8; full-read verification remains possible. |
| §3 cursor ordered scan | Retained with generation validation; cursor alone no longer claims recovery safety. |
| §4 FS/producer report sketches | Replaced by equivalent responsibilities and policy/cost contracts, not preserved as misleading executable algorithm. Producer reports metadata/identity without bytes. |
| §4 N-way merge and rep-before-cycle staging | Replaced by pairwise comparison and policy, with durable checkout correspondence in ADR 011. |
| §4 transfer execution / record actual result / avoid echo / target-only bookkeeping | Retained in §§5/7; no direct rep-to-rep copy treated as application. |
| §4 backend method list | Every responsibility retained in §5; exact method signatures remain advisory. |
| §4 error taxonomy and object-safe boundary | Retained in §9. Failed scans cannot look like deletion. |
| §5 VCS/blob-version/GC/lens exclusions | Preserved through scope/retention boundaries and ADR 011. No workspace-wide frontier or generic history store. |
| §6 cost table and §7 consequences | Retained with corrected verification costs and ordered-diff work. No unproven hash-pruning asymptotics. |
| §8 doc token comparability | Versioned opaque recipes; reproducibility cannot silently use another version. Exact token scheme remains producer work. |
| §8 1M rows / chunk inventories | Retained as measurements and ADR 013 concerns. |
| §8 symlink platform kind / claim-versus-origin / unclaimed deletion | Retained in §§3/7/8. |

## ADR 011 — source sections accounted for

| Original section/detail | Replacement disposition |
|---|---|
| §1 generic/daybook boundary | Retained in §1; no facet/lens/branch knowledge inside bridge. |
| §1 no correctness-dependent daemon / lazy store opens | Correctness independent of watch retained; 'must close/open store per event' demoted to implementation choice. Explicit watch can be long-lived. |
| §1 sqlite lock alone is arbiter | Single writer retained; lock must cover external operations, exact mechanism open. |
| §1 local writes before upstream | Retained with staged branches and render-head base. |
| §2 create/adopt/detach/many checkouts/spec | Retained with corrections: adopt creates candidates, not dpath facets; detach protects durable local work; per-checkout controls remain. |
| §3 commands all reconcile / sync cycle | Superseded by observation versus ingest/publication versus pull. Exact CLI grammar intentionally not frozen. |
| §3 upstream heads polling, watcher optimization | Retained as available trigger, alongside notifications; scanner authority and event coalescing remain. |
| §4 rep removal, claim, deepest-first nonrecursive delete | Retained with unchanged-target proof. |
| §4 file removal becomes trash | Removed. Dpath/output edits do not delete documents; explicit trash is separate. |
| §4 track-only deletion | Retained as local observation only. |
| §5 validation-only conflict / /tmp/conflicts branch | Replaced by preparation errors versus invalid merge candidates, preserved checkout branches, and merge-if-valid. |
| §5 disk always last-good / bounce never blocks / pick-only UI | Superseded: preserve pending bytes; block automatic checkout activity on failures; use ordinary branch/merge resolution. No inline markers retained. |
| §6 ordered bulk walk/cursor/progress/no journal | Walk/progress retained; generation and operation outcomes required, no cursor-only recovery claim. One-shot imports are not forced into checkout state. |
| §7 rep/trash/blob GC | Responsibilities retained; local branches/ledger need lifecycle management; unavailable bytes not stubs. |
| §8 per-file atomic/single writer/corruption | Retained with partial-plan recovery and explicit unreconstructible state; 'worst case only hashing' withdrawn. |
| §9 kind flips/event storms/new path import policy | Preflight kind/ownership, notification coalescing, and explicit eligible new-file/watch policy retained. |
| §9 'Current' for unimported file | Removed: ignoring an input is not acknowledgement that ingestion succeeded. |

Newly settled decisions: separate dpath/by-ID checkout classes; eager independent local checkout branches; all-batch preparation/readiness gate; in-memory candidate merge validation; expected-head fast-forward publication with bounded retries; no multi-document rollback; explicit discard preserving unbound files; projection staging outside checkout; selected lens-version provenance; dirty pull and dirty interpretation-switch refusal.

## ADR 012 — source sections accounted for

| Original section/detail | Replacement disposition |
|---|---|
| §1 per-format, plugs, Daybook-owned codec | Retained in §1 and interface section. |
| §2 round trips/determinism | Retained with formatting fidelity explicitly lens-specific, not universal user-byte preservation. |
| §2 totality or bounce/no partial output | Superseded by complete valid preparation gate and explicit partial filesystem application recovery; render failure does not create branch. |
| §2 LensAnno/version stamping/upgrade | Provenance retained; recipe expanded to all dependencies. Upgrade explicit, not automatic installed-version selection. Exact annotation structs not frozen. |
| §2 one doc N entries/collisions/incrementality | Retained in §§8–9; `.d` allocation checkout-owned; multi-input recipes permitted. |
| §3 classify/ingest/render/diff API | Responsibilities preserved and expanded to interest/proposal/prepare/produce/diff, with render-base context. Old signatures superseded, not hidden as compatibility APIs. |
| §3 Daybook types and opaque bridge identity/claim | Retained, ownership distinct from content/version and all actual write targets declared. |
| §3 digest-based accept/canonicalized output | Retained with actual-result and newer-edit safety, not eager bytes on all reports. |
| §4 ingest/render pseudocode and ordered bulk | Replaced by pipeline and batch contract, coordinated with ADR 011; no per-delta reclassification ignoring its base. |
| §5 Bounce/diff viewer/last-good | Ordinary diff/branch surfaces and no markers retained; separate bounce model superseded. |
| §6 extension/priority/no global policy | Replaced by shared demand-driven signals, proposal hierarchy, stable default and local overrides. Basic text/blob fallback retained without execution-driven retry. |
| §7 WASI lazy mounts/determinism/no ambient authority | Retained and clarified: references, not stubs; execution compatibility provenance; recognition providers separate. |
| §8 zip/.doc, epub, image/sidecars/gallery | All examples retained, with explicit limitations instead of claiming the API already suffices or GC cannot orphan inputs. |
| §9 streaming/error taxonomy/normalization | Retained in interface, failure and production sections. |
| §9 customization facets | Explicit open choice, not silently removed or turned into a mandatory policy layer. |
| §9 internal output collisions and sandbox versioning | Retained as validation requirements and compatibility/retention work. |

Newly settled decisions: Body-driven whole-document selection; recognition signals include optional model classification; complete proposal ownership/dependency/write-target declarations; conservative all-input authority; no automatic execution fallback; watch failures block checkout; explicit lens upgrades; in-memory merge validation on pre-existing local checkout branches.

## Cross-document follow-up, not edits made

Earlier FDR drafts need reconciliation with the final decisions:

- FDR 001's mixed dpath/by-ID presentation is superseded by separate checkout classes.
- FDR 003's separate semantic bounce flow is superseded for checkout edits by eager local branches and merge-if-valid; general branch surfaces still apply.
- FDR 004's open new-file watch default is now eligible automatic import; ingest/publication and pull are distinct actions; ignore/lens controls, discard and blocked recovery need reflecting.
- ADR 007 / document repository design must establish independent local checkout-branch ownership, retention, and conditional validated publication. Absence of origin delegation alone does not prove local-only behavior.
- ADR 013 must account for referenced blob retention and source-version availability; staged document JSON/history does not preserve missing payload bytes.

These are flagged, not silently changed. The older disposition and fidelity notes describe earlier rounds and must not be treated as approval of superseded interfaces.
