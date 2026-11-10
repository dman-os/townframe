# intxt: prior art on code review

Status: Reference
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: What critiques of the GitHub PR model and its alternatives say, and what intxt takes from them. Gathered by a research agent on 2026-11-10; sources marked "summary only" were not read in full. Delta (delta.dev) and harness transcript formats are in `intxt-ledger.md` F2.

## Sources

| Source | Read | Key points |
|---|---|---|
| Gregory Szorc, [Problems with Pull Requests and How to Fix Them](https://gregoryszorc.com/blog/2020/01/07/problems-with-pull-requests-and-how-to-fix-them) (2020) | full | Review unit too large; feedback not bucketed per commit; force-push orphans comments; commit identity guessed from position; all-or-nothing integration; branch-per-PR makes stacks painful; review metadata in git refs doesn't scale. Fixes: commit-level units with stable ids across rewrites, explicit dependencies, review sign-off separate from landing, metadata in a database |
| Tyler Cipriani, [The Pull Request](https://tylercipriani.com/blog/2024/08/31/the-pull-request/) (2024) | full | The PR was never designed. Review the commit message; attention sets ("whose turn"); user-defined labels; hidden bot comments; push-to-ref creates the review |
| Tyler Cipriani, [stacked patches](https://tylercipriani.com/blog/2023/09/24/stacked-patches) (2023) | full | Feature branches delay integration; big PRs cause review fatigue |
| Julien Danjou, [Twenty years of stacking commits](https://julien.danjou.info/blog/twenty-years-of-stacking-commits/) (2026, vendor) | full | 1,500-line PRs get skimmed; Gerrit lost on UX and friction, not semantics; stacks fit exploratory or coupled work poorly |
| Douglas Creager, [Commits and code review](https://dcreager.net/git/commits-and-code-review/) | full | Commit vs PR as the unit; reviewing a stack that lands as separate commits is unsolved |
| Jane Street, [Iron](https://www.janestreet.com/tech-talks/janestreet-code-review/) | talk | Per-reviewer "brain": the diff each reviewer has accepted, diffed against the current state after any rebase, so they see only what's new. CR/XCR comments reassign whose turn it is. Release blocked while CRs open. Unit is the whole feature |
| Gerrit, [user intro](https://gerrit-review.googlesource.com/Documentation/intro-user.html) | full | Change-Id survives amend and rebase; every patch set retained; label votes and submit rules |
| jj, [FAQ](https://github.com/martinvonz/jj/blob/main/docs/FAQ.md) | full | Change id stable, commit id changes; `jj evolog` keeps prior versions; divergent changes |
| Sapling [ISL](https://sapling-scm.com/docs/addons/isl/) | full | Stacks with drag-and-drop rebase; CI and comment badges per commit |
| Pragmatic Engineer, [Stacked diffs](https://newsletter.pragmaticengineer.com/p/stacked-diffs) | full | Meta's diff culture; reworking an early diff forces rebasing children |
| Graphite ([blog](https://graphite.com/blog/stacked-prs)) | summary only | Only 24% of 1000+ line PRs get any comment (vendor claim) |
| HN, [Reinventing the pull request](https://hn.jakechampion.name/item/47540441) | mirror | Questions "vanish" when code changes, so people add commits instead of amending and then squash |
| Salesforce, [Scaling code reviews for AI-generated code](https://engineering.salesforce.com/scaling-code-reviews-adapting-to-a-surge-in-ai-generated-code/) | full | AI PRs are larger; review time on the largest fell (shallow approvals); flat diffs hide intent. They regroup changes by concept and dependency and attach context and reasoning traces |
| CodeRabbit, [intent](https://coderabbit.ai/blog/bottleneck-in-code-review-is-understanding-intent) | summary only | The bottleneck is understanding intent |
| Pierre, [On rendering diffs](https://pierre.computer/writing/on-rendering-diffs) | full | Large-diff UIs fail on DOM size, memory, scroll blanking; virtualization, estimated heights, deferred highlighting |
| Mitchell Hashimoto, Vouch | secondary | Explicit trust states for who may submit |
| Google, [Modern Code Review](https://research.google/pubs/modern-code-review-a-case-study-at-google/) | secondary | Median change ~24 lines; one reviewer in 75% of reviews |

## What intxt takes from this

| Pain point | Who names it | intxt's answer | Status |
|---|---|---|---|
| Comments orphaned by force-push / rebase | Szorc, HN | Every observed commit kept as a datum; the ref datum's history pairs revisions; comments pin exact versions and follow via cursors | D17, S7 |
| No usable "what changed since I looked" | Gerrit, Iron | **Iron's brain, per reader:** a per-reader datum pinning the versions that reader last reviewed, so "new since you looked" works across rebases. It is also the multiplayer-safe read state | New; adopt in A2 |
| Intent and context lost; AI PRs rubber-stamped | Salesforce, CodeRabbit, Cipriani | Sessions and structure are members of the package; guided, regrouped review over the same data | Core thesis |
| Whose turn is it | Cipriani (attention sets), Iron (CR/XCR) | Per-thread turn derived from who wrote last, or an explicit field; what the agent treats as "waiting on me" | New; decide in A2 |
| Commit message not reviewable | Cipriani | Commits are datums, so they can be annotated like anything else | Free with D15/D17 |
| Review metadata in git refs doesn't scale | Szorc | Review state lives in documents, never in git | By construction |
| Tooling friction kills good models | Danjou | Capture is passive; making an intxt is one command | FDR 007 |
| Branch-per-PR, stacking pain | Szorc, Danjou | intxt isn't tied to a branch; intxts can relate to one another. Stacks are later | Partly deferred |
| CI gates only the final state; intermediate states unverified | Szorc, Cipriani (gap: no source addresses it) | Evidence about exact versions; coverage rules later | Deferred (D11), a differentiator |
| Mobile and remote review | nobody (gap) | The review loop's main target | FDR 007, a differentiator |
| Large-diff UI performance | Pierre | Per-file docs loaded lazily (F1); claytxt's Pierre port | In progress |
| Thin approval vocabulary | Cipriani, Iron, Gerrit | Open vocabularies later; no verdicts in the alpha | D18 |
| Trust at intake | Hashimoto | Out of scope for one operator; agent identities are signed actors | Later |
