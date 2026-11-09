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

## Open

- **L1** Operator review of `intxt-design.md` (layers, refs, datums). Then send shapes to the node lane and the anchor model to claytxt.
- **L2** gitoxide (or another git) building for wasm32-wasip3 and diffing a checkout: decides whether the nodule builds revisions from a granted directory (node lane asked this lane). Needs a build; mbx is ready (switch this checkout first).
- **L3** Private session layers: child drawer of the intxt (proposal) vs. a separate referenced drawer.
- **L4** Can a `PollDatums` predicate filter by signing actor? If yes, `awaitResponse` is a plain poll; if no, a thin nodule method.
- **L5** The read API must expose a datum version's signing actor, so the UI can show authorship without an unsigned `author` field. Check what `ReadDatums` returns today; ask the node lane if missing.
- **L6** Claytxt requirements on anchors (2026-11-10): content to re-attach against (`quote`), ranges spanning hunks with context lines, file-level anchors. Addressed in design §3; needs their review.

## In flight

- Nothing building. This checkout is on `feat/intxt` off `main`; `chore/mbx` not merged yet.
