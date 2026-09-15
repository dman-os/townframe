# FDR 003: VC primitives — findings before rewrite

Source: `docs/fdrs/003-vc-primitives.md`. Compared with FDR 001/002 drafts, `docs/fdrs/004-workspace-cli.md`, `docs/adrs/007-doc-branch-identity.md`, and `feat/drawers` FDR 001 / ADRs 014–015. No files in `docs/` changed.

| Section | Disposition | Issue |
| --- | --- | --- |
| Introduction/context | Rewrite plainly | “CRDT merging always succeeds so conflict never blocks work” conflates physical convergence with accepted/renderable semantics and rejected/pending remote changes in drawers ADR 015. Remove review archaeology and stale ADR references. |
| §1 main by default | Preserve product intent; decide observation | Ordinary writes target the document of record, not a Git-style staged snapshot. Clarify local ingest vs publication and what a failed semantic write preserves. Recommend that observational `status`/`diff`/`log` not publish; user decision pending. |
| §2 CLI verbs | Retain scope, not every spelling as locked | `commit` applies local edits; `-m` spanning multiple documents cannot be one Automerge change. Historical `checkout --at <version>` lacks one collection-wide version identifier. `db status` contradicts FDR 004's every-`.dtree`-command auto-commit. |
| §3 branches | Rewrite for drawers branch identity | New design: branch is an independent document with its own DocumentId, not a BranchId under a logical main DocumentId automatically following its drawers. Discovery, names, sharing, and merging need a product contract. `/tmp` branch names cannot serve as globally unique branch IDs. |
| §4 immutable history | Retain | Successor document is **supersession**, not redaction/erasure from replicas. Historical reads are per-document heads. Forking from history creates a distinct document, not undoing main. |
| §5 multi-doc history | Retain rejection of a required collection frontier | Local grouped timelines can be derived; timing/author heuristics are not proof of one atomic action. An op-log may aid local UX but cannot confer replicated atomicity. Do not make a shared op ID or global frontier prerequisite. |
| §6 branch-on-conflict | Split local vs remote | Local edit from last-rendered heads may bounce to separately identified branch on semantic validation failure; failures of lens runtime/bytes are not bounces. Already-received invalid history stays in its document. ADR 015 distinguishes accepted/pending/rejected. Conflict branches may carry the only copy of user edits; blanket pruning/TTL unsafe. |
| §7 Diff | Preserve semantic requirement, defer exact struct | Diffs compare real tree, rendered base, and per-document heads; a moved path can be ambiguous. Blob diff by ref and unavailable content matter. A single typed diff structure across renderers is ADR/API work, not a normative FDR record layout. |
| §8 deletion/trash/GC | Major revision | Checkout-file removal cannot delete a document (FDR 001 draft). `/trash` is a normal path filtered by default, not reserved. Drawer roster membership is independent: a trashed doc does NOT automatically remove itself from drawers (drawers ADR 014). Multiple dpaths and restore need policy. GC cannot equate unlinking with guaranteed remote erasure. |
| Appendix Patchwork details | Move to design notes | Useful evidence but not normative product behavior; outdated branch analogy. |

Questions that block a faithful rewrite:
Decisions and remaining questions:
1. **Decided:** a branch is a separately identified document; sibling/source relationships are available to the checkout and CLI for navigation, but branch documents are not automatically added to the source's drawers. Path arguments may resolve to the owning document; stable document IDs remain accepted. Exact branch-creation scope is a CLI question.
2. **Decided:** removing a checkout file does not create a `/trash/...` claim. Trash is a separate product/document deletion flow. Other dpaths and drawer membership remain independent.
3. **Decided (as understood):** `db status`, `diff`, and `log` may refresh observations but do not ingest/publish; watch and explicit mutation may write to the document of record. Confirm if “non-read commands not committing” meant otherwise.
4. What does `db branch <name>` mean in a multi-document checkout now that branches are independent documents? Per selected document, batch fork of selected documents, or postpone that command until a worked CLI design exists?
5. Should `db checkout --at` accept only an explicit per-document version/selection for now, rather than claiming a single checkout-wide historical version? No collection-wide frontier is desired.
