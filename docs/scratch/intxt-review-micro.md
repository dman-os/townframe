# intxt: the review micro (D62: a GitHub PR on the phone, and its intxts)

Status: Proposal, for the operator's review
Owner: intxt lane (the micro is intxt's end to end; claytxt provides CodeView and the micro runtime, nodules the node)
Updated: 2026-11-17
Scope: What the review micro shows, what it calls, and which pieces of infrastructure it waits on. Builds on `intxt-commit-membership.md` (the PR names its intxts) and `intxt-review-surface.md` (R5, GitHub).

## The flow

1. Open a PR in the review micro (from a list of PRs the node knows, or by URL).
2. The micro reads the PR's description and finds its `intxt:` lines (`Intxt::named_in`; the grammar and golden vectors are in `intxt-commit-membership.md`).
3. One `summarizeIntxts { drawers }` call returns a row per intxt: found (title, description, link, entries), not found, or unreadable (#146).
4. Tapping a row opens the intxt: its guide, threads and the diff of each entry, the diff rendered by CodeView with intxt's threads in its annotation slots.

PRs without `intxt:` lines show none. Finding intxts by commit is deferred to a cartridge index built by processors (ledger, 2026-11-17).

## What exists and what doesn't (2026-11-17 survey of the stack)

| Need | State | Owner |
|---|---|---|
| Author a UI micro | Rust + `clay_mdk` (`micro_counter` pattern: code crate + manifest crate, `export_micro!`, wasm32-wasip3). The TS/QuickJS runtime (`micro-runtime.md`, #129) is a draft. | claytxt |
| Micro → cartridge method (`summarizeIntxts`) | **Missing.** A micro reaches only Clay collections, config, and other micros' publications (`clay_mdk::Context`). A Clay host import carrying the micro's capability is a proposal (`cartridge-authoring.md`, principle 3, lane T1). | open: nodules or claytxt |
| The PR on the node | **Missing.** No egress: micros have none, nodules reach only `dispatch.local`. GitHub is only reachable from the `day` CLI. | intxt (see below) |
| Diff view | CodeView port through C5 (#145). C6, the Dioxus host, exposes the props. Embedded content is the embedder's Dioxus `Element` at a slot marker (`pierre-dioxus-host.md`), so CodeView is a library inside the review micro, not a cross-micro component. | claytxt |
| Phone shell that loads micros | **Missing.** `dayframe_mobile` renders a heading only; no micro loading, no deep links or share intents. | open: claytxt (Android, #81)? |

## Getting the PR onto the node (intxt's)

**A. The CLI imports it (proposed for this week).** `day x intxt github pull <owner>/<repo>#<n>` (R5 of `intxt-review-surface.md`) fetches the PR with the user's `gh` credentials and writes a `githubPull/<owner>/<repo>/<n>` datum `{ title, body, state, head, base, headSha, updatedAt, url }` into a per-repository GitHub document. The micro lists and reads those datums like any other. No new node capability, and no token on the node. Cost: only PRs someone imported are visible, and they're as fresh as the last pull (a `--watch` or a hook can refresh).

**B. A node routine fetches it.** The intxt cartridge gets a GitHub egress capability and a stored token, and fetches on demand. Any PR is readable from the phone. Cost: a new egress permbind and secret storage in the node (nodules' work), and a decision about where the token lives.

A now, B when the node grows egress. The datum shape is the same in both, so the micro doesn't change.

## What I need from CodeView (for claytxt, before C6)

- Input: a unified patch for one file, or old and new blob text plus the path; the micro has both from the entry's pack.
- Per-line annotation slots: the micro renders a thread marker or an expanded thread at (side, line).
- Scroll or reveal a (path, side, line) anchor, for opening a thread from the list.
- View-only first. Line selection → "comment here" is the first event wanted, after props (operator: props first, events later).

## Proposed scope for this week

1. `github pull` (A) and the `githubPull` datum, on my stack.
2. The review micro in Rust on the desktop shell: PR list → named intxts → intxt view with CodeView, against a fixture until micro → cartridge calls exist.
3. The phone once the mobile shell loads micros, and `summarizeIntxts` once a micro can call a cartridge method.

## Open (for the operator)

1. Who builds micro → cartridge calls (the Clay host import carrying the micro's capability)?
2. Who builds micro loading in the mobile shell, and is it this week?
3. A (CLI import) for this week, B later?
