# intxt: the review micro (D62: a GitHub PR on the phone, and its intxts)

Status: Proposal, for the operator's review (revision 2: operator's answers of 2026-11-17 folded in)
Owner: intxt lane (the micro is intxt's end to end; claytxt provides CodeView, the micro runtime and the phone shell, nodules the node)
Updated: 2026-11-17
Scope: What the review micro shows, what it calls, and which pieces of infrastructure it waits on. Builds on `intxt-commit-membership.md` (the PR names its intxts) and `intxt-review-surface.md` (R5, GitHub). Written in Rust.

## The flow

1. Open a PR in the review micro (from the PRs the node holds, or by URL).
2. The micro reads the PR's description and finds the intxts it links to: any ADR 035 URL to an intxt's marker, `dtxt:datum:<drawer>/org.example.intxt/main`. The drawer id is the intxt's descriptor document id, global, so the URL is all a PR needs. There is no line grammar to learn: an `intxt:` prefix is decoration, and `Intxt::named_in` finds the URLs anywhere in the text (golden vectors in #146).
3. The micro shows a row per linked intxt (title, description, link) and opens one on tap: its guide, its threads, and the diff of each entry, rendered by CodeView with intxt's threads in its annotation slots.

The other direction is a datum: `day x intxt github pull` writes a `githubPullEntry/<owner>/<repo>/<n>` into every intxt the PR links to (#150), so an intxt lists its PRs.

PRs that link to no intxt show none. Finding intxts by commit is deferred to a cartridge index built by processors (ledger, 2026-11-17).

## What exists and what doesn't (2026-11-17 survey of the stack)

| Need | State | Owner |
|---|---|---|
| Author a UI micro | Rust + `clay_mdk` (`micro_counter` pattern: code crate + manifest crate, `export_micro!`, wasm32-wasip3). | claytxt (runtime), intxt (the micro) |
| Micro → cartridge method | **Missing.** A micro reaches only Clay collections, config, and other micros' publications (`clay_mdk::Context`). | claytxt (the Clay host import) with nodules (routing, the micro's capability), alongside claytxt running on the node this week (operator) |
| The PR on the node | Built: `day x intxt github pull` (#150), option A below. | intxt |
| Diff view | CodeView port through C5 (#145); C6, the Dioxus host, exposes the needs below. It may land as a library the micro links (boundary A, 2026-11-10) or as a micro of its own (operator, 2026-11-17); the needs are the same either way, only the embedding differs. | claytxt |
| Phone shell that loads micros | **Missing.** `dayframe_mobile` renders a heading only. | claytxt (operator) |

`summarizeIntxts` (#146) is a batch read of each linked intxt's descriptor, for a micro that can't read datums itself. If micros get direct datum reads with the micro → node calls, it goes.

## Getting the PR onto the node

**A. The CLI imports it (built, #150).** `day x intxt github pull <owner>/<repo>#<n>` (or the PR URL) fetches the PR with the user's `gh` login and writes `githubPull/<owner>/<repo>/<n>` `{ title, body, state, head, base, headSha, baseSha, updatedAt, url }` into the PR's own document, in a node-level GitHub drawer. One document per PR, created idempotently, so an intxt that links to the PR shares just that PR. No node capability, no token on the node. Cost: only imported PRs are visible, as fresh as the last pull.

**B. A node routine fetches it.** Needs a GitHub egress permbind and secret storage in the node (nodules). Any PR is readable from the phone. The datum is the same, so the micro doesn't change.

## What I need from CodeView (sent to claytxt for C6)

- Input: a unified patch for one file, or old and new text plus the path (`parse_patch_files`, `parse_diff_from_file`).
- Per-line annotation slots at (side, line) for intxt's threads (`lineAnnotations` + `renderAnnotation`).
- Reveal a (path, side, line) anchor (`CodeView::scroll_to`, items keyed by path).
- View-only first; line selection → "comment here" next (ported in `InteractionManager`).

## Scope for this week

1. Done: `github pull`, `githubPull`, `githubPullEntry` (#150).
2. The review micro in Rust on the desktop shell: PR list → linked intxts → intxt view with CodeView, on fixtures until micro → node calls exist.
3. The phone when claytxt's shell loads micros.

## Open (for the operator)

1. **Where the intxt can be found: a `via=` query key (ADR 035 extension).** A reader of the PR needs to know which nodes hold the intxt, and ADR 035 deliberately puts no locator in identity (§2.2: any node holding a document can serve it). Its settled points (2026-11-10) say "locators are query keys if ever needed", and the old `db+blob` URL had `?via=`. Proposed: `via=<node key>|<node key>` (the multibase node keys of `dtxt:actor`, sorted, `|`-joined like `heads=`), on any `dtxt:` kind:

   ```
   dtxt:datum:z5Kd…/org.example.intxt/main?via=zNodeA…|zRelay…
   ```

   It is a hint, never authority: it grants nothing, a reader may try other nodes, and the reference's identity (and equality) ignores it. `Intxt::reference` takes the nodes to name (this node, plus the relays it syncs with); `named_in` keeps the hint for the caller. Open: whether a hint names a node or a relay ticket, and whether stored references (datums) may carry it or only shared text.

Not open any more: a GitHub-clickable form (operator, 2026-11-17: not needed today). `summarizeIntxts` is expected to go once micros get datum and cabinet services from the Clay docs crate (operator, 2026-11-17).
