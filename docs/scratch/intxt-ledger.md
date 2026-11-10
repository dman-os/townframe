# intxt ledger

Status: Active
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: Decisions, open questions and work in flight for the intxt lane (datums, reference model, intxt micro, MCP). Design: `intxt-design.md`. Node-side counterpart: `docs/scratch/intxt-node-requirements.md` and `big-ndk-intxt-sketch.md` on `feat/big-node` (townframe-p3N-2).

## Lanes

- intxt (this lane, p3N-3): datums, reference/anchor model, intxt micro, MCP surface.
- Node (p3N-2): `big_ndk`, intxt nodule kit surface, day node, relay, `day`.
- claytxt (p3N-4): Clay, the diff view (Pierre port) the intxt micro embeds, shell, Android. Reviews anchors from the diff side.
- Webnodes (separate lane): node in the browser; web runs the same composition on a throwaway webnode.

## Decisions

- **D1 (2026-11-10, operator)** One intxt is one drawer.
- **D2 (2026-11-10, operator)** Clients write intxt datums directly through the generic datum writes; no API over the datums where avoidable. The nodule only does what clients can't (design §5).
- **D3 (2026-11-10, operator, recorded by the node lane)** Nodule methods act with the capability the caller presents, never the nodule's own grants.
- **D4 (2026-11-10, operator)** GitHub sync is on the roadmap, probably not this week.
- **D5 (design discussion)** A future VCS is separate from the intxt: an intxt composes artifacts and doesn't own their histories.
- **D6 (2026-11-10, operator)** Focus is the write loop ("replacing PRs"); pure viewing is later.
- **D7 (2026-11-10, operator)** Sessions: our own normalized datums with ACP-compatible naming (O1); one doc per session, subagents as linked sessions (O2). Later option: variant sessions as Automerge forks of their parent session doc.
- **D8 (2026-11-10, operator)** Design philosophy: datums are containers/records of history. Multiplayer editing may use other datums over the same use case, but not by default.
- **D9 (2026-11-10, operator)** Work structure (tasks, decisions) is its own datums; adapters can map harness task systems (todo lists, plans) into them.
- **D10 (2026-11-10, operator)** Agent interface: MCP, CLI and hooks all eventually; the first shipped use case sets the order.
- **D11 (2026-11-10, operator)** Evidence coverage across stacks (U10) is deferred past this week; keep it enumerated.
- **D12 (2026-11-10, operator)** FDRs: one for the general system, one per shipped feature, starting with the first. FDRs describe what users do, and users include harness adapters, GitHub, agents, humans and supporting software. No reference-model internals in FDRs.
- **D13 (2026-11-10, operator)** Multiplayer is a requirement for every datum: concurrent writers must not clobber each other.
- **D14 (2026-11-10, operator)** First feature: the agent review loop (capture → phone review → agent picks up feedback → next revision), aimed at needs today's tooling serves poorly (e.g. Claude remote control shows no files or diffs).
- **D15 (2026-11-10, operator)** The intxt graph is over arbitrary datums, not filesystem-first. A reference is a datum ref pinned at heads plus a selector into that datum; nothing more is needed in the core. Code (file docs), git/jj objects, sessions, guides and CI results are all just datums. Supersedes the multi-kind `Target` union in `intxt-design.md` §3.
- **D16 (2026-11-10, operator)** No VCS units in the intxt. intxt and a future VCS may have separate change units. "Logical change" (O5) is not a v0 concept; grouping is a structure datum with several references.
- **D17 (2026-11-10, operator)** Git/jj objects are imported as datums (commits; jj op log objects if needed). All observed commits are kept; a separate ref/bookmark datum points at a commit, and walking parents from it visits only reachable commits. No predecessor links: the ref datum's own Automerge history records what it pointed at before (a reflog for free), which gives revision-to-revision pairing.
- **D18 (2026-11-10, operator)** No verdicts in the alpha: intxt isn't managing trunks, so approve/request-changes vocabulary waits.
- **D19 (2026-11-10, operator)** Agent interface order: harness hooks + CLI first, MCP after.
- **N1** dpaths ADR 010 explicitly excludes git integration and keeps reps as latest state, not history, so it offers no git-like history model to reuse.

## Open

- **L1** Operator review of `intxt-design.md` (layers, refs, datums). Then send shapes to the node lane and the anchor model to claytxt.
- **L2** gitoxide (or another git) building for wasm32-wasip3 and diffing a checkout: decides whether the nodule builds revisions from a granted directory (node lane asked this lane). Needs a build; mbx is ready (switch this checkout first).
- **L3** Private session layers: child drawer of the intxt (proposal) vs. a separate referenced drawer.
- **L4** Can a `PollDatums` predicate filter by signing actor? If yes, `awaitResponse` is a plain poll; if no, a thin nodule method.
- **L5** The read API must expose a datum version's signing actor, so the UI can show authorship without an unsigned `author` field. Check what `ReadDatums` returns today; ask the node lane if missing.
- **L6** Claytxt requirements on anchors (2026-11-10): content to re-attach against (`quote`), ranges spanning hunks with context lines, file-level anchors. Addressed in design §3; needs their review.

## Findings

- **F1 (2026-11-10) Automerge cost of carrying source as Text** (automerge 0.11, real PRs #76 7.4 MB / #79 5.4 MB base+head of touched files; scratchpad experiment `am-exp`, single runs on a loaded machine, ±2x):
  - On-disk `save()` is 0.16–0.24× input for every variant (#76: one doc 1.2 MB, per-file docs 1.4 MB total, patch-as-Text 0.8 MB, plain Str 1.6 MB). Size is not the problem.
  - Memory and load are: one doc holding all files as Text peaks at 840 MB building and 514 MB / 7 s loading. One doc per file peaks at 160 MB building, 75 MB with all 404 loaded, 6.3 s to load all (a viewer loads only the files it shows). Plain Str loads in 0.07 s.
  - Build edits with line-diff splices; automerge's `update_text` is character-level and 25× slower (447 s vs 17 s) with 7× the patches.
  - An incremental revision (81 files edited to #77) is 345 KB; a 1% edit in 20 files is 16 KB.
  - Cursors made at rev1 resolve correctly at rev2 in 52/53 (#76) and 53/60 (#79) files; all misses are moved blocks, which a line diff expresses as delete+insert. Moves still need quote matching or a move-aware diff.
- **F2 (2026-11-10) Harness transcripts** (Claude Code, OpenCode, Pi on this machine; Codex/omp have no turn data left): conversation is ~6% of a Claude Code log; every harness with data yields per-edit unified patches; none records the commit per turn. Delta ingests transcripts as ACP session updates and files via a checkout mount; its agent does no extra annotation work.
- **F3 (2026-11-10)** `feat/dpaths` (FDR 001–003, ADR 010–012) designs checkout ingest of filesystem edits into documents (watch mode, no staging) and leaves multi-doc ingest messages to a future design: the natural hook for edit→turn provenance in the VCS stage.

## In flight

- Nothing building. `feat/intxt` has `chore/mbx` merged (2d0f3ff1); p3N-4 since rebased `chore/mbx` as `adfd2778` with the same fixes. Build with `env -u CARGO_TARGET_DIR nix develop -c …`.
