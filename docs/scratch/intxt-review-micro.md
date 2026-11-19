# intxt: the review micro (D62: a GitHub PR on the phone, and its intxts)

Status: Proposal, for the operator's review (revision 2: operator's answers of 2026-11-17 folded in)
Owner: intxt lane (the micro is intxt's end to end; claytxt provides CodeView, the micro runtime and the phone shell, nodules the node)
Updated: 2026-11-17
Scope: What the review micro shows, what it calls, and which pieces of infrastructure it waits on. Builds on `intxt-commit-membership.md` (the PR names its intxts) and `intxt-review-surface.md` (R5, GitHub). Written in Rust.

## The flow

1. Open a PR in the review micro (from the PRs the node holds, or by URL).
2. The micro reads the PR's description block (below) and gets the intxts it names: `dtxt:doc:<drawer>` URLs, the drawer id being the intxt's descriptor document id (global), with a `via=` hint of where to find it.
3. The micro shows a row per named intxt (title, description, link) and opens one on tap: its guide, its threads, and the diff of each entry, rendered by CodeView with intxt's threads in its annotation slots.

This is for reading **other people's** intxts. The user's own intxts come from the intxt list, which is where intxts are created; an intxt names its refs, commits and PRs (`githubPullEntry/<owner>/<repo>/<n>`, written by `day x intxt github pull`, #150). There is no "create an intxt for this PR?" flow (operator, 2026-11-17).

PRs without a block show no intxts. Finding intxts by commit is deferred to a cartridge index built by processors over local state, the baseline for cartridge-specific lookups (ledger, 2026-11-17).

## The PR description block (operator, 2026-11-17)

No custom grammar: a collapsed `<details>` holding a JSON code block. `$type` names the shape, so it can grow (more fields, tickets) without a parser change, and unknown fields are carried, as Lexicon open types are. What the author's tooling writes at the end of the description:

````markdown
<details>
<summary>intxt</summary>

```json
{
  "$type": "org.example.intxt.prLinks",
  "intxts": [
    {
      "ref": "dtxt:doc:z5KdV9aHq3vXr2M8sT1bNcE7wYpJfLgU4oQiZkDxR6m?via=zNodeA…|zRelay…",
      "title": "intxt: commits live with the intxt",
      "refs": ["refs/heads/stack/intxt-16-capture-records-commits"]
    },
    {
      "ref": "dtxt:doc:z9QaW2bTn5yLk8P3vR7cHdF1xMsJgEoU6iZpKfDqA4t?via=zNodeA…",
      "title": "review micro: the PR on the phone"
    }
  ]
}
```

</details>
````

On GitHub it renders as one collapsed "intxt" line. The reader finds the first ```` ```json ```` block inside a `<details>` whose `$type` is `org.example.intxt.prLinks`, and reads `intxts[].ref` (required; parsed as an ADR 035 `doc` URL) and the optional `title` and `refs` (a hint for readers without access; the intxt itself is the truth). A malformed block names nothing and is reported, not guessed at. The writer (`day x intxt github push`, or a printed block to paste) replaces its own block on each write and leaves the rest of the description alone.

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

1. **Accepted 2026-11-17 (operator): as sketched, a general-purpose requirement for other systems too; tickets may be added later.** Where the intxt can be found: a `via=` query key (ADR 035 extension). A reader of the PR needs to know which nodes hold the intxt, and ADR 035 deliberately puts no locator in identity (§2.2: any node holding a document can serve it). Its settled points (2026-11-10) say "locators are query keys if ever needed", and the old `db+blob` URL had `?via=`. Proposed: `via=<node key>|<node key>` (the multibase node keys of `dtxt:actor`, sorted, `|`-joined like `heads=`), on any `dtxt:` kind:

   ```
   dtxt:doc:z5Kd…?via=zNodeA…|zRelay…
   ```

   It is a hint, never authority: it grants nothing, a reader may try other nodes, and the reference's identity (and equality) ignores it. `Intxt::reference` takes the nodes to name (this node, plus the relays it syncs with); `named_in` keeps the hint for the caller. Open: whether a hint names a node or a relay ticket, and whether stored references (datums) may carry it or only shared text.

Not open any more: a GitHub-clickable form (operator, 2026-11-17: not needed today). `summarizeIntxts` is expected to go once micros get datum and cabinet services from the Clay docs crate (operator, 2026-11-17).
