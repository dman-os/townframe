# intxt: the app's structure, one view tree for the viewer and the review micro

Owner: intxt lane; operator decides; claytxt reviews (placement, components).
Updated: 2026-11-25
Scope: Which views intxt has, how a reader moves between them, what is read-only on the web and editable in the app, and which code the two hosts share. Visuals and theming are claytxt's (D88) and out of scope.

Status: Proposal, for the operator's review. Intxt `z` (the first intxt opened under the operator's 2026-11-25 rule) records the work on it.

## Why now

Two hosts render intxts today and they have already drifted:

- **The web viewer** (#310, hosted on mothership-alpha) renders one page per intxt: title, description, every entry with its commits, the range patch, session summaries, and comment threads, all stacked.
- **The review micro** (desktop, D88) starts from a GitHub PR: Layout → Review → the PR → the intxts its block names → "Show changes", which renders the patch in CodeView.

D95 says they share their code, with only state sourcing differing. D86 says mutations happen only in the app. claytxt's journeys J1–J7 (`intxt-review-loop.md`, 2026-11-10) are the right skeleton, but they predate D70 (an intxt is an index of entries: commit ranges, refs, jj changes, sessions, PRs), the read-only viewer, and recorded sessions, and they model a "change" as a single diff. This proposal refreshes them against the model as built, so both hosts grow along one tree instead of two.

## Principles

1. **One view tree, two hosts.** Every view is `view(state)`: a pure model built from datums (`IntxtView`, `SessionSummary`, …) and Dioxus components that render it. A host supplies how state is read (the viewer from its relay node; the micro from the node it runs on) and how the reader moves (URLs on the web; the shell's navigation stack in the app). Nothing else differs.
2. **Read on the web, write in the app (D86).** Every view exists in both hosts. The app adds actions to views (compose, reply, resolve, edit entries); the web shows the same view without them. No view is web-only or app-only, except the composer, which is an action.
3. **Every view renders its gaps (D95, FDR 006 "Readers expect gaps").** A datum that can't be read is marked where it would have been; the view still renders.
4. **An intxt is live.** It grows while the work goes on (product-layer note 2). The app updates in place as documents sync; the web shows the state at load (live web waits for webnodes).
5. **People's nouns on the surface, the data layer's underneath.** The surface says intxt, change, session, discussion, file; it does not say drawer, document, datum, worktree, harness.

## The views

Each view has an address (below) that both hosts understand.

| # | View | What it shows | Replaces |
|---|---|---|---|
| V1 | **Home** | Intxts with recent activity, newest first; per intxt its title, what changed since the reader last looked, and unseen comments. | J1 inbox |
| V2 | **Intxt** | Title and description (Markdown). Then its entries grouped by kind: **changes** (commit ranges, refs, jj changes, each with files touched and +/- counts), **sessions** (one summary row each), **PRs** (links out), and **open discussions** with counts. When the author left a guide or structure (ADR 041 `structure`), its order wins over grouping by kind. | J2 overview; the viewer's single page |
| V3 | **Change** | One change entry: the files it touches, then a file's diff with discussions inline at their anchors (J3). Later: per commit, and comparing revisions (J6). | J3; the micro's "Show changes" |
| V4 | **Session** | The summary on top (what the viewer shows today), then the session turn by turn: the operator's prompts, the agent's messages, tool calls, and the file changes each call made, linked to V3. Long tool output folded. | new |
| V5 | **Discussion** | A thread: the root comment, replies, what it points at (a line range, a file, a commit, a session message), and whether it follows the current revision or refers to an earlier one (FDR 006 §"Talking about anything"). | J4 |
| V6 | **PR** | A GitHub PR and the intxts its block names, each opening V2. The app's entry point from forges (the micro's flow today); on the web the PR block links straight to V2, so V6 isn't needed there. | the micro's first screens |
| — | **Composer** (app only) | Write a comment on whatever is selected: lines in V3, a message in V4, another comment in V5. | J5 |

V3 and V4 are where a reader spends time. V2 is how they get there; V1 and V6 are how they arrive.

## Moving between views

**Addresses.** One route type, shared by both hosts:

```
Home
Intxt   { drawer }
Change  { drawer, entry, file: Option<path> }
Session { drawer, session, turn: Option<turn id> }
Discussion { drawer, thread }
Pr      { owner, repo, number }        (app only)
```

- **Web:** `/`, `/i/<drawer>`, `/i/<drawer>/c/<entry>[/<path>]`, `/i/<drawer>/s/<session>[#<turn>]`, `/i/<drawer>/d/<thread>`, each with the existing `?via=` hint. Every view is a link someone can paste.
- **App:** the shell's list/details (ADR 032, claytxt's placement table): V1 is the list panel; V2 its details, itself a list; V3, V4 and V5 the details of a V2 row. On a phone each pushes over the last and back walks the stack; on a desktop V2 stays as the list column. The composer is a bottom sheet on a phone and inline on a desktop.
- **Links inside the content** all resolve to routes: a comment's target opens V3 or V4 at the anchor; a tool call's file change opens V3 at that file; a session row in V2 opens V4; `intxt show` and the PR block print web routes.

## What is shared in code

| Piece | Shared | Per host |
|---|---|---|
| View models (`IntxtView`, `SessionSummary`, change and discussion models) | yes, in `intxt_web`'s `view.rs` today; moves to a crate the micro can depend on when the micro adopts it | — |
| Route type and its parsing | yes | the web maps it to paths; the app to shell navigation |
| Components (Dioxus) | yes, including claytxt's diff view, which is plain Dioxus (claytxt decision 3, 2026-11-11) | the web renders them with SSR; the micro inside Clay |
| Reading state | the read plan (which datums a view needs, in batches) | the transport: TCP XRPC to the relay vs the micro's node calls |
| Actions | — | app only |
| Look | claytxt's tokens (D88) | — |

The diff view on the web needs declarative shadow DOM for SSR (C12); until then V3 on the web renders the plain escaped patch it renders today.

## Slices, in order

1. **Split the viewer into routes.** V2 as the intxt page with grouped entries; V3 over the range patch; V4 from the session summary plus the turns, read in batches; V5. Phone-first. This is the read path of every view, exercised against real intxts on the relay as soon as #381 lands, and on the fixture until then.
2. **The micro adopts the views.** It renders V2–V5 from the shared code over its node, keeping V6 as its entry point; "Show changes" becomes V3. Done when the desktop and the phone browser show the same intxt the same way, apart from actions.
3. **V1 in both hosts.** "What changed since I last looked" needs ADR 041's `seen/<actor>` read; the CLI's `inbox` already finds waiting threads.
4. **Actions in the app:** the composer, reply, resolve and reopen, over the CLI's existing review verbs.
5. **Later:** per-commit diffs and revision compare (wait on reading git packs in wasm, L2), live updates on the web (webnodes), structure and guides authored by agents (FDR 010, deferred by D78).

Each slice is a layer in this lane's stack with its own intxt (operator, 2026-11-25).

## Open, for the operator

1. **"Thread" is taken twice.** The product-layer note uses Delta's sense (a strand of work by an agent or group); the review code uses it for a comment and its replies. Proposed: **discussion** for comment threads on the surface, keeping "thread" free for the product layer's sense. The datums keep their names.
2. **What the web's home lists.** The relay holds every intxt shared with it. Should `/` list them all, or should the web have no home and only open intxts by link (an unlisted model, until "public" exists)? Proposed: no listing on the web for now; V1 is an app view, and the web's `/` explains how to open one.
3. **How deep V4 goes.** Prompts and file changes are clearly in. Proposed: the agent's messages shown, tool calls collapsed to one line each with their output behind a tap, thinking never (it isn't recorded, FDR 007). Subagent sessions open as their own V4, linked from the call that spawned them.
4. **V2's grouping.** Entries by kind (changes, sessions, PRs) until there's a guide, or a single chronological list of everything that happened? Proposed: by kind, because a reviewer wants the changes first; chronology belongs to V4.
