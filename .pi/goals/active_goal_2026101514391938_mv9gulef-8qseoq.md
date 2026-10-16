{
  "version": 3,
  "id": "mv9gulef-8qseoq",
  "objective": "=== Goal ===\nObjective: Deliver the full standalone big_cabinet PR established by the October 6–13 design transcript: highly usable, high-quality, comprehensively tested Cabinet components, without supplanting existing Daybook code, using concurrent GPT 5.6 Luna high workers for virtually all implementation and substantial qualification.\n\nRestoration and authoritative sources:\n- Restore the contract of prior goal mv7hha27-mvo9ci, retained at .pi/goals/active_goal_2026101405212542_mv7hha27-mvo9ci.md. This is a continuation, not a restart or permission to discard existing work. Carry forward its completed scope-inventory and delegation-setup evidence and all subsequently qualified components. Do not rerun broad discovery merely because tracking was restored.\n- The original design transcript is primary authority: /home/asdf/.pi/agent/sessions/--home-asdf-repos-rust-townframe-2--/2026-10-06T11-44-56-371Z_01a11107-f42f-7640-a044-473fae20dfdb.jsonl. Consult relevant original turns and compaction digests before answering design questions. Reports and recaps are leads, not substitutes for original evidence.\n- Where the transcript does not answer a question, inspect daybook_core and higher-layer consumer usage to establish actual needs, without preserving old defects or migrating those consumers.\n- Maintain docs/scratch/big-cabinet-delivery.md as the exhaustive source-backed ledger of deliverables, ownership, dependencies, acceptance gates, exact evidence, integration status, unresolved questions and residual risks. Preserve these authorities and decisions in every continuation/compaction handoff.\n\nSuccess criteria:\n- Deliver all mandatory transcript-scoped facet/schema authoring, canonical codecs and reconciliation, compatibility checking and schema registration, cleaned facet indexes, documents and branching, drawers and child drawers, authority/actors/capabilities, shared service infrastructure, typed clients, agreed transport adapters, interpreted projections, mutation receipts, observations, lifecycle and documentation.\n- Reorganize SampleRepo into shared service infrastructure and separate sample-specific modules, preserving agreed semantics and providing a usable tested reference.\n- Demonstrate genuine production Cabinet workflows, starting with authorized document create → facet mutation → read/reopen against real BigRepo persistence and public receipt semantics, then complete drawer admission/access, branching, source-node attribution, projections and public services. Primitives or synthetic tests alone do not satisfy these workflows.\n- Evaluate each component for consumer usability, correctness, performance, transcript alignment and meaningful coverage. Placeholders, shallow tests, unqualified worker claims and unimplemented seams cannot stand in for delivery.\n- Independently accept every mandatory ledger item and complete integrated qualification and documentation. Report exact commands/results and distinguish functioning features from explicitly agreed deferrals. Never claim completion while mandatory items remain blocked.\n\nCurrent baseline to retain and verify at relevant integration barriers:\n- Scope inventory and execution setup were completed under the prior goal; preserve their evidence rather than treating them as new feature work.\n- Root selective Core integration has independent scoped acceptance: 152 tests passed, zero skipped, affected strict clippy and formatting passed. This accepts qualified primitives/integration, not production DocumentsRepo, DrawerRepo, Authority workflows, full facet policy or network delivery.\n- Authority typed facet-claim signing and durability have substantial qualification, but Cabinet lint, real document/drawer target binding and typed node attribution remain mandatory gaps.\n- Ordinary document claim-history implementation exists in its assigned workspace but tests/clippy remain unqualified because the assigned NTFS target reproduced EINVAL after its one authorized quarantine/retry. Do not treat implementation as acceptance.\n- Service cancellation RAII independent acceptance and substantive Iroh repair remain pending. Iroh must compose with embedder routing and reuse node-to-node connections/multiple streams; it is secondary to the local production Cabinet critical path.\n- All three children of workflow 9603c2cf-4d3b-46b3-80c7-54cf0e8bd365 completed. Consume retained substantive reports and inspect actual source before new assignments; no worker from that workflow is still running.\n- Repository repair restored full all() traversal across all 11 worktrees and preserved source files, symlinks and bookmarks; only four automatic Root .pi tracking files changed during verification. Authority's local checkout was reconstructed with byte-identical non-VCS contents and the same quuuszol change identity, now at 198bc39f. Preserve its original directory at /home/asdf/repos/rust/townframe-cabinet-authority.before-jj-repair-ld5bom5l and metadata/evidence at /tmp/nix-shell.elXImm/jj-corruption-repair-ld5bom5l. Historical corrupt objects were not reconstructed; do not mistake restored active traversal for recovery of their original bytes.\n\nBoundaries:\n- Build Cabinet alongside existing code. Do not supplant or migrate daybook_core, daybook_types, runtime, plugs or application production consumers. Exercise representative consumers through Cabinet-local tests/examples instead.\n- Deliver big_cabinet, big_cabinet_core and necessary supporting macros, shared service infrastructure, tests and documentation according to transcript scope. Select API details without reopening settled design, subject to usability and agreed semantics.\n- Preserve the Panproto POC. Panproto integration, upstream/Daybook integration, subsets, self-group drawers, roster sharding, recovery principals and other explicitly deferred work remain out unless original transcript evidence establishes otherwise.\n- ADRs/FDRs, wiki documents, play.stack.md, task documents and review reports support reconstruction; resolve discrepancies against the original transcript rather than silently inventing requirements.\n- HTTP over Iroh is the agreed node-to-node replacement for cross-node IRPC; IRPC is an optimized implementation detail, not canonical. In-process typed calls retain caller-owned execution. IPC envelopes and HTTP adapters share operations. Do not make secondary transport work block a local production Cabinet vertical.\n\nConstraints:\n- Preserve the user's approximately 99.99% worker-execution division. GPT 5.6 Luna high workers perform virtually all implementation, fixes, substantial reviews, test construction/execution and qualification. Parent owns scope, bounded assignments, dependencies, coordination, evidence scrutiny, acceptance and truthful tracking; parent must not become another major implementation worker.\n- Discover executable agents and explicitly select exact openai/gpt-5.6-luna:high. No silent model/provider substitution, external CLI, foreground or protocol fallback. Use eligible retained contexts where appropriate; use fresh-context reviewers for independent acceptance. Follow governed delegation requirements and report infrastructure failures precisely.\n- At most three concurrent workers, one source writer per workspace. Reuse existing workspaces and qualified work rather than recreating everything. Source workers must run no VCS commands or mutate .jj/.git. Any further repository recovery requires a separately bounded preservation-first plan. Never use git commands.\n- Preserve all unpushed work, dirty/untracked worktree files, bookmarks, Root tylozxul, feat/drawers parent rukwmuvx/b23321df184cb9dbe8b543b7c3fa2ac43fb6f9e9, townframe-3 and frozen mrxxkwto/184b81f031cab664ed6078631fc4c0965c016ad5. Do not reset, broadly restore, garbage-collect, remove recovery copies or clean user data as a shortcut.\n- Exactly one Cargo build/discovery/test/clippy process at a time, using flock /tmp/cabinet-cargo-build.lock and CARGO_BUILD_JOBS=2; nextest uses --test-threads 2. Check available memory after acquiring the lease; below 4 GiB available, stop the build lane and report the resource blocker.\n- Before every Cargo command, silently source the workspace's assigned .env and assert its exact assigned CARGO_TARGET_DIR under /run/media/asdf/p3N/tmp. Do not print credentials, change target directories or RUSTFLAGS, or cause rebuild-inducing flag churn.\n- Monitor memory and disk. Existing NTFS EINVAL failures are an unresolved infrastructure blocker, not permission for cleanup. Only an explicitly approved exact-package non-destructive quarantine and one unchanged-target retry are allowed; stop on recurrence. No deletion, whole-target clean, filesystem repair/remount, alternate target or uncoordinated cleanup without approval.\n- Follow AGENTS.md, README.md, CONTRIBUTING.md and applicable skills. No new skips, lint suppression to hide gaps, abstraction-breaking shims or fake-equivalent evidence. Pin or instrument surfaced failures; do not dismiss them as unrelated.\n- Prioritize actual production consumer workflows; avoid repeated inventories and redundant review churn. Bundle coherent fixes and consolidate independent reviews at meaningful integration barriers.\n\nVerification contract:\n- Define tailored gates for each deliverable: original design provenance, practical boundary usability, correctness/performance review and narrow meaningful tests. Use property, snapshot and deterministic simulation tests where they add real confidence, not mechanically.\n- Implementation workers run affected cargo-nextest tests, cargo clippy --all-targets --all-features for affected crates with -D warnings, and formatting. No full-suite runs for bounded assignments. Independent Luna reviewers inspect actual changes and rerun targeted gates; parent scrutinizes reports and source and routes findings through workers.\n- Exercise real identity, storage, authority and authorization boundaries. Preserve exact domain-separated deterministic-CBOR FacetClaim signing, historical verification versus current authorization, the real BigRepo document-ID representation and shared validated facet identities; no fabricated adapters or duplicate ordinary claim objects.\n- Include Cabinet-local cross-component tests for schema/storage semantics, heads-aware edits, branching/drawer admission, source/projection ordering, state-before-event, read-after-event, receipt/completion meaning, transport ambiguity, cancellation, replay/live handoff, bounded admission/backpressure, external changes and supervised shutdown where applicable. Read the message-ordering-audit skill before accepting receipt or completion semantics.\n- Preserve explicit shutdown joins and Drop-as-abort-only semantics. Do not claim projection completion from source-head persistence or transactionality across stores where none exists.\n- Finish with worker-led integrated qualification and fresh independent acceptance against every mandatory ledger item. Historical results establish a baseline, not automatic acceptance of changed production integrations.\n\nIf blocked: Consult original transcript evidence first, then actual consumer usage. Record the exact blocker and evidence; continue genuinely independent unblocked work. Stop the affected lane and ask the user for a genuine unanswered product decision, irreconcilable source contradiction, scope change, model/protocol substitution, resource/target change or further recovery operation. Never invent a decision, retry indefinitely, silently switch infrastructure or claim completion over mandatory gaps.",
  "status": "active",
  "autoContinue": true,
  "usage": {
    "tokensUsed": 5821984,
    "activeSeconds": 50136
  },
  "sisyphus": false,
  "createdAt": "2026-10-15T11:39:19.383Z",
  "updatedAt": "2026-10-16T23:00:46.358Z",
  "activePath": ".pi/goals/active_goal_2026101514391938_mv9gulef-8qseoq.md",
  "revision": 1230,
  "scheduler": {
    "version": 1,
    "owner": "01a1382a-38dd-740e-ad82-f7ed726e882d",
    "generation": "1d5f12c9-9d50-496f-bfc9-cb7f56e02dbb",
    "used": 39,
    "phase": "idle",
    "repairUsed": false
  },
  "taskList": {
    "tasks": [
      {
        "id": "scope-inventory",
        "title": "Carry forward completed transcript-first scope inventory and acceptance ledger",
        "status": "complete",
        "completedAt": "2026-10-15T11:39:26.278Z",
        "evidence": "Carried forward prior goal's completed inventory: original contract/task tree read intact; docs/scratch/big-cabinet-delivery.md remains authoritative ledger. No discovery restart.",
        "verificationContract": "Retain prior completed evidence and authoritative source locations; reconcile only genuine new findings. Do not restart broad discovery."
      },
      {
        "id": "delegation-setup",
        "title": "Carry forward Luna-high execution, independent review and workspace/build ownership",
        "status": "complete",
        "completedAt": "2026-10-15T11:39:26.278Z",
        "evidence": "Prior setup retained: governed Luna-high workflow9603 completed all3 children; existing isolated workspaces/env targets and exclusive Cargo lease preserved. Revalidate runner/model before new launches",
        "verificationContract": "Retain prior setup evidence; verify current executable model availability and retained-worker eligibility before launch, with three-worker cap and exclusive Cargo lease."
      },
      {
        "id": "service-foundation",
        "title": "Complete shared service infrastructure and SampleRepo restructuring",
        "status": "pending",
        "verificationContract": "Independent Luna qualification of typed declarations/clients, dispatch/context/errors, service versus sample module boundaries, concurrency, clocks and cancellation RAII."
      },
      {
        "id": "facet-schema",
        "title": "Complete and qualify facet authoring, codecs, reconciliation and compatibility",
        "status": "pending",
        "verificationContract": "Retain scoped Core acceptance; independently qualify remaining mandatory policy and production integration, including canonical representations, unknown-data preservation, heads/concurrency, atomicity and diagnostics."
      },
      {
        "id": "indexes-schema-service",
        "title": "Complete cleaned indexes and schema registration/service boundaries",
        "status": "pending",
        "verificationContract": "Retain qualified comparator/index/SchemaRepo primitives; qualify real projection producers/queries, admission/evolution and consumer-facing configuration without Daybook migration."
      },
      {
        "id": "authority",
        "title": "Complete actors, capabilities, typed delegation and secrets-backed authority",
        "status": "pending",
        "verificationContract": "Retain qualified durability/signing; resolve remaining production lint and real membership/document-origin-branch binding, typed node attribution, private-key confinement and Keyhive boundary gaps."
      },
      {
        "id": "documents",
        "title": "Deliver real authorized document create/write/read/reopen, history and branching",
        "status": "pending",
        "verificationContract": "Qualify existing history code once its build blocker is resolved; prove the first production vertical against actual BigRepo identity/persistence, then heads-aware writes, history acceptance, branching, schema refusals, receipts and concurrency."
      },
      {
        "id": "drawers",
        "title": "Deliver drawer facets, rosters, child drawers and access propagation",
        "status": "pending",
        "verificationContract": "Independently qualify explicit admission, effective-authority versus roster distinction, new-ID creation, Relay/Read/Write/Admin, cycles/multiple paths, teardown and partial-operation recovery."
      },
      {
        "id": "projection-lifecycle",
        "title": "Integrate projections, observations, receipts, cancellation and lifecycle",
        "status": "pending",
        "verificationContract": "Meaningful interleaving/cross-component tests prove state-before-event, source-versus-projection completion, replay/live handoff, coalescing, bounded admission, ambiguity, external changes and supervised shutdown."
      },
      {
        "id": "public-services",
        "title": "Integrate usable standalone production APIs and agreed transports",
        "status": "pending",
        "verificationContract": "Exercise representative Cabinet-local consumer workflows, typed in-process calls and agreed envelopes/adapters; independently qualify authority/serialization and repair reusable, embedder-composable Iroh behavior without blocking local delivery."
      },
      {
        "id": "integrated-acceptance",
        "title": "Complete integrated qualification, documentation and independent final acceptance",
        "status": "pending",
        "verificationContract": "Fresh Luna audit against the exhaustive mandatory ledger with actual source inspection and affected nextest/strict clippy/format gates; resolve findings, preserve user work and report exact evidence and explicit deferrals."
      }
    ],
    "blockCompletion": true,
    "proposedAt": "2026-10-15T11:38:42.181Z"
  },
  "currentTaskId": "service-foundation"
}

# Goal Prompt

=== Goal ===
Objective: Deliver the full standalone big_cabinet PR established by the October 6–13 design transcript: highly usable, high-quality, comprehensively tested Cabinet components, without supplanting existing Daybook code, using concurrent GPT 5.6 Luna high workers for virtually all implementation and substantial qualification.

Restoration and authoritative sources:
- Restore the contract of prior goal mv7hha27-mvo9ci, retained at .pi/goals/active_goal_2026101405212542_mv7hha27-mvo9ci.md. This is a continuation, not a restart or permission to discard existing work. Carry forward its completed scope-inventory and delegation-setup evidence and all subsequently qualified components. Do not rerun broad discovery merely because tracking was restored.
- The original design transcript is primary authority: /home/asdf/.pi/agent/sessions/--home-asdf-repos-rust-townframe-2--/2026-10-06T11-44-56-371Z_01a11107-f42f-7640-a044-473fae20dfdb.jsonl. Consult relevant original turns and compaction digests before answering design questions. Reports and recaps are leads, not substitutes for original evidence.
- Where the transcript does not answer a question, inspect daybook_core and higher-layer consumer usage to establish actual needs, without preserving old defects or migrating those consumers.
- Maintain docs/scratch/big-cabinet-delivery.md as the exhaustive source-backed ledger of deliverables, ownership, dependencies, acceptance gates, exact evidence, integration status, unresolved questions and residual risks. Preserve these authorities and decisions in every continuation/compaction handoff.

Success criteria:
- Deliver all mandatory transcript-scoped facet/schema authoring, canonical codecs and reconciliation, compatibility checking and schema registration, cleaned facet indexes, documents and branching, drawers and child drawers, authority/actors/capabilities, shared service infrastructure, typed clients, agreed transport adapters, interpreted projections, mutation receipts, observations, lifecycle and documentation.
- Reorganize SampleRepo into shared service infrastructure and separate sample-specific modules, preserving agreed semantics and providing a usable tested reference.
- Demonstrate genuine production Cabinet workflows, starting with authorized document create → facet mutation → read/reopen against real BigRepo persistence and public receipt semantics, then complete drawer admission/access, branching, source-node attribution, projections and public services. Primitives or synthetic tests alone do not satisfy these workflows.
- Evaluate each component for consumer usability, correctness, performance, transcript alignment and meaningful coverage. Placeholders, shallow tests, unqualified worker claims and unimplemented seams cannot stand in for delivery.
- Independently accept every mandatory ledger item and complete integrated qualification and documentation. Report exact commands/results and distinguish functioning features from explicitly agreed deferrals. Never claim completion while mandatory items remain blocked.

Current baseline to retain and verify at relevant integration barriers:
- Scope inventory and execution setup were completed under the prior goal; preserve their evidence rather than treating them as new feature work.
- Root selective Core integration has independent scoped acceptance: 152 tests passed, zero skipped, affected strict clippy and formatting passed. This accepts qualified primitives/integration, not production DocumentsRepo, DrawerRepo, Authority workflows, full facet policy or network delivery.
- Authority typed facet-claim signing and durability have substantial qualification, but Cabinet lint, real document/drawer target binding and typed node attribution remain mandatory gaps.
- Ordinary document claim-history implementation exists in its assigned workspace but tests/clippy remain unqualified because the assigned NTFS target reproduced EINVAL after its one authorized quarantine/retry. Do not treat implementation as acceptance.
- Service cancellation RAII independent acceptance and substantive Iroh repair remain pending. Iroh must compose with embedder routing and reuse node-to-node connections/multiple streams; it is secondary to the local production Cabinet critical path.
- All three children of workflow 9603c2cf-4d3b-46b3-80c7-54cf0e8bd365 completed. Consume retained substantive reports and inspect actual source before new assignments; no worker from that workflow is still running.
- Repository repair restored full all() traversal across all 11 worktrees and preserved source files, symlinks and bookmarks; only four automatic Root .pi tracking files changed during verification. Authority's local checkout was reconstructed with byte-identical non-VCS contents and the same quuuszol change identity, now at 198bc39f. Preserve its original directory at /home/asdf/repos/rust/townframe-cabinet-authority.before-jj-repair-ld5bom5l and metadata/evidence at /tmp/nix-shell.elXImm/jj-corruption-repair-ld5bom5l. Historical corrupt objects were not reconstructed; do not mistake restored active traversal for recovery of their original bytes.

Boundaries:
- Build Cabinet alongside existing code. Do not supplant or migrate daybook_core, daybook_types, runtime, plugs or application production consumers. Exercise representative consumers through Cabinet-local tests/examples instead.
- Deliver big_cabinet, big_cabinet_core and necessary supporting macros, shared service infrastructure, tests and documentation according to transcript scope. Select API details without reopening settled design, subject to usability and agreed semantics.
- Preserve the Panproto POC. Panproto integration, upstream/Daybook integration, subsets, self-group drawers, roster sharding, recovery principals and other explicitly deferred work remain out unless original transcript evidence establishes otherwise.
- ADRs/FDRs, wiki documents, play.stack.md, task documents and review reports support reconstruction; resolve discrepancies against the original transcript rather than silently inventing requirements.
- HTTP over Iroh is the agreed node-to-node replacement for cross-node IRPC; IRPC is an optimized implementation detail, not canonical. In-process typed calls retain caller-owned execution. IPC envelopes and HTTP adapters share operations. Do not make secondary transport work block a local production Cabinet vertical.

Constraints:
- Preserve the user's approximately 99.99% worker-execution division. GPT 5.6 Luna high workers perform virtually all implementation, fixes, substantial reviews, test construction/execution and qualification. Parent owns scope, bounded assignments, dependencies, coordination, evidence scrutiny, acceptance and truthful tracking; parent must not become another major implementation worker.
- Discover executable agents and explicitly select exact openai/gpt-5.6-luna:high. No silent model/provider substitution, external CLI, foreground or protocol fallback. Use eligible retained contexts where appropriate; use fresh-context reviewers for independent acceptance. Follow governed delegation requirements and report infrastructure failures precisely.
- At most three concurrent workers, one source writer per workspace. Reuse existing workspaces and qualified work rather than recreating everything. Source workers must run no VCS commands or mutate .jj/.git. Any further repository recovery requires a separately bounded preservation-first plan. Never use git commands.
- Preserve all unpushed work, dirty/untracked worktree files, bookmarks, Root tylozxul, feat/drawers parent rukwmuvx/b23321df184cb9dbe8b543b7c3fa2ac43fb6f9e9, townframe-3 and frozen mrxxkwto/184b81f031cab664ed6078631fc4c0965c016ad5. Do not reset, broadly restore, garbage-collect, remove recovery copies or clean user data as a shortcut.
- Exactly one Cargo build/discovery/test/clippy process at a time, using flock /tmp/cabinet-cargo-build.lock and CARGO_BUILD_JOBS=2; nextest uses --test-threads 2. Check available memory after acquiring the lease; below 4 GiB available, stop the build lane and report the resource blocker.
- Before every Cargo command, silently source the workspace's assigned .env and assert its exact assigned CARGO_TARGET_DIR under /run/media/asdf/p3N/tmp. Do not print credentials, change target directories or RUSTFLAGS, or cause rebuild-inducing flag churn.
- Monitor memory and disk. Existing NTFS EINVAL failures are an unresolved infrastructure blocker, not permission for cleanup. Only an explicitly approved exact-package non-destructive quarantine and one unchanged-target retry are allowed; stop on recurrence. No deletion, whole-target clean, filesystem repair/remount, alternate target or uncoordinated cleanup without approval.
- Follow AGENTS.md, README.md, CONTRIBUTING.md and applicable skills. No new skips, lint suppression to hide gaps, abstraction-breaking shims or fake-equivalent evidence. Pin or instrument surfaced failures; do not dismiss them as unrelated.
- Prioritize actual production consumer workflows; avoid repeated inventories and redundant review churn. Bundle coherent fixes and consolidate independent reviews at meaningful integration barriers.

Verification contract:
- Define tailored gates for each deliverable: original design provenance, practical boundary usability, correctness/performance review and narrow meaningful tests. Use property, snapshot and deterministic simulation tests where they add real confidence, not mechanically.
- Implementation workers run affected cargo-nextest tests, cargo clippy --all-targets --all-features for affected crates with -D warnings, and formatting. No full-suite runs for bounded assignments. Independent Luna reviewers inspect actual changes and rerun targeted gates; parent scrutinizes reports and source and routes findings through workers.
- Exercise real identity, storage, authority and authorization boundaries. Preserve exact domain-separated deterministic-CBOR FacetClaim signing, historical verification versus current authorization, the real BigRepo document-ID representation and shared validated facet identities; no fabricated adapters or duplicate ordinary claim objects.
- Include Cabinet-local cross-component tests for schema/storage semantics, heads-aware edits, branching/drawer admission, source/projection ordering, state-before-event, read-after-event, receipt/completion meaning, transport ambiguity, cancellation, replay/live handoff, bounded admission/backpressure, external changes and supervised shutdown where applicable. Read the message-ordering-audit skill before accepting receipt or completion semantics.
- Preserve explicit shutdown joins and Drop-as-abort-only semantics. Do not claim projection completion from source-head persistence or transactionality across stores where none exists.
- Finish with worker-led integrated qualification and fresh independent acceptance against every mandatory ledger item. Historical results establish a baseline, not automatic acceptance of changed production integrations.

If blocked: Consult original transcript evidence first, then actual consumer usage. Record the exact blocker and evidence; continue genuinely independent unblocked work. Stop the affected lane and ask the user for a genuine unanswered product decision, irreconcilable source contradiction, scope change, model/protocol substitution, resource/target change or further recovery operation. Never invent a decision, retry indefinitely, silently switch infrastructure or claim completion over mandatory gaps.

## Progress

- Status: running
- Auto-continue: on
- Sisyphus mode: no
- Time spent: 13h55m36s
- Tokens used: 5.8M (5,821,984) tokens
## Tasks

<!-- blockCompletion: true -->
- [x] scope-inventory: Carry forward completed transcript-first scope inventory and acceptance ledger — evidence: Carried forward prior goal's completed inventory: original contract/task tree read intact; docs/scratch/big-cabinet-delivery.md remains authoritative ledger. No discovery restart.
- [x] delegation-setup: Carry forward Luna-high execution, independent review and workspace/build ownership — evidence: Prior setup retained: governed Luna-high workflow9603 completed all3 children; existing isolated workspaces/env targets and exclusive Cargo lease preserved. Revalidate runner/model before new launches
- [ ] service-foundation: Complete shared service infrastructure and SampleRepo restructuring — contract: Independent Luna qualification of typed declarations/clients, dispatch/context/errors, service versus sample module boundaries, concurrency, clocks and cancellation RAII.
- [ ] facet-schema: Complete and qualify facet authoring, codecs, reconciliation and compatibility — contract: Retain scoped Core acceptance; independently qualify remaining mandatory policy and production integration, including canonical representations, unknown-data preservation, heads/concurrency, atomicity and diagnostics.
- [ ] indexes-schema-service: Complete cleaned indexes and schema registration/service boundaries — contract: Retain qualified comparator/index/SchemaRepo primitives; qualify real projection producers/queries, admission/evolution and consumer-facing configuration without Daybook migration.
- [ ] authority: Complete actors, capabilities, typed delegation and secrets-backed authority — contract: Retain qualified durability/signing; resolve remaining production lint and real membership/document-origin-branch binding, typed node attribution, private-key confinement and Keyhive boundary gaps.
- [ ] documents: Deliver real authorized document create/write/read/reopen, history and branching — contract: Qualify existing history code once its build blocker is resolved; prove the first production vertical against actual BigRepo identity/persistence, then heads-aware writes, history acceptance, branching, schema refusals, receipts and concurrency.
- [ ] drawers: Deliver drawer facets, rosters, child drawers and access propagation — contract: Independently qualify explicit admission, effective-authority versus roster distinction, new-ID creation, Relay/Read/Write/Admin, cycles/multiple paths, teardown and partial-operation recovery.
- [ ] projection-lifecycle: Integrate projections, observations, receipts, cancellation and lifecycle — contract: Meaningful interleaving/cross-component tests prove state-before-event, source-versus-projection completion, replay/live handoff, coalescing, bounded admission, ambiguity, external changes and supervised shutdown.
- [ ] public-services: Integrate usable standalone production APIs and agreed transports — contract: Exercise representative Cabinet-local consumer workflows, typed in-process calls and agreed envelopes/adapters; independently qualify authority/serialization and repair reusable, embedder-composable Iroh behavior without blocking local delivery.
- [ ] integrated-acceptance: Complete integrated qualification, documentation and independent final acceptance — contract: Fresh Luna audit against the exhaustive mandatory ledger with actual source inspection and affected nextest/strict clippy/format gates; resolve findings, preserve user work and report exact evidence and explicit deferrals.

