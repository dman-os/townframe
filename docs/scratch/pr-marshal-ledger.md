# PR marshal ledger

Status: active
Owner: nodules lane (PR marshal for every lane)
Updated: 2026-11-25
Scope: what lands on main, in what order, and what the operator reviews next, across all lanes' stacks

## Role (operator, 2026-11-25)

- The nodules lane is the PR marshal. It lands reviewed PRs, keeps every lane's stacks in shape, and tells the operator what is next up for review across all stacks, not only its own.
- Stack discipline it enforces on every lane:
  - each lane's work is a native GitHub stack (`gh stack`), not just PRs chained by base branch;
  - PRs are drafts by default (drafts skip CI since #105);
  - only cut points, a PR meant to merge together with everything below it, are un-drafted and greened.
- The operator reviews; the marshal merges. The operator's review mark is a PR number: everything at or below it in a stack is reviewed.

## Decisions

- **M1 (2026-11-25):** the operator reviewed the node stack through #199 (node-55). Land #105–#199 on main. Then land claytxt's three CI cuts, #384 → #385 → #386 (facet port, legacy crates out of the workspace, mltools out).

- **M2 (2026-11-25):** ignore PRs from lanes that aren't active: #47, #50, #52, #54–#62, #85, #117, #216. Leave them as they are; don't close them, chase them or queue them for review.
- **M3 (2026-11-25):** next review is node #233–#252 (node-56…76). #233 directly follows #199; the PR numbers in between belong to other lanes.

- **M4 (2026-11-25):** nothing reaches main before the operator reviews it. Lanes don't rebase or green ahead of review; drafts may sit for weeks. Flow: the operator names a section → the owning lane greens that section's tip → the marshal merges. This supersedes the earlier advice to intxt to rebase after #252 lands: it rebases only once its section is named.

- **M5 (2026-11-25):** ADR 056 (#395): mother and twin between nodes; member and anchor per drawer (an `anchors` datum, ADR 019's DrawerHome renamed DrawerAnchor); pin dropped. A grant does not change mirroring policy: ADR 052 §2 step 4 is dropped (relayed by claytxt), so drawer mirroring is not built in the micros nodule.

- **M6 (2026-11-25):** building ADR 056 (topology, anchors, peer policy) and the grant/invite system (invite tickets, `day peers add-card`) waits until the automata lane delivers keyline, i.e. the upstream keyhive/keyline move. The nodules lane builds it afterwards, along with the topology experiments harness.

## How to land a stack (learned 2026-11-25)

- A PR that belongs to a native GitHub stack can't be merged per PR: `PUT /pulls/{n}/merge` answers 403, "use the asynchronous merge endpoint".
- Use `PUT /repos/dman-os/townframe/pulls/{n}/merge-async` with `merge_method=squash` (and `sha=<head>`), then poll `GET .../merge-async/{uuid}`.
  - Merging a mid-stack PR merges every open PR below it, one squash commit per PR.
  - It refuses if any of those PRs is a draft, so un-draft the range first (`gh pr ready`). Un-drafting starts CI on each one.
- After a merge, GitHub rebases the PRs above it onto the new main and retargets the next one to main. The PR records can lag their branch refs for minutes, and a merge sent while a PR record still shows its pre-rebase head fails with "merge conflict". Wait until every PR's `head.sha` equals its branch ref, then merge.
- `gh stack merge <pr>` failed twice with "This merge could not be completed", yet one of those attempts did land #105 (af0f9a42); the async merge that followed added an empty duplicate commit (b36cfc3d, same tree). Use the async endpoint alone.
- The laptop's network to GitHub is unreliable: run `gh` on mothership with `GH_TOKEN=$(gh auth token -u dman-os)`. The active account there is Yohe-Am; don't switch it.
- **Never move a stacked PR's branch to commit that has nothing over its base.** GitHub then closes the PR and refuses to reopen it ("no new commits"), and a closed PR can't be removed from its native stack. That wedged stack #101 on 2026-11-25 (#112, replaced by #399).
- **A PR record stuck on an old head** (pushes dropped during a GitHub incident): push the branch back to the head GitHub has on record, then forward to the real tip. Each push is then processed. Moving the branch back one commit (`tip^`) is the dangerous variant above.
- **Closing and reopening a PR to resync it** fails once the branch has moved ("force-pushed or recreated"). Reopening needs the branch at the PR's recorded head.
- **When a native stack is wedged:** `gh stack unstack <n>` (merged PRs stay stacked), land one PR at a time with the normal merge endpoint (scratch `land2.sh`: rebase onto the new main, tree check before and after), then `gh stack link` the rest as a fresh stack.
- Verify every landing by tree: each squash commit's tree must equal its layer's reviewed branch tree.

## Landing log

| When (UTC) | What | Main after | Notes |
|---|---|---|---|
| 2026-11-25 ~16:00 | #105 node-09 ci-drafts | af0f9a42 (+ empty b36cfc3d) | tree == reviewed tree |
| 2026-11-25 ~19:00 | #106–#199 (node-10…55), one squash per PR | ee20c626 | every step tree-checked; main tree a36abd5e == reviewed #199. #112 was closed by GitHub (see pitfalls); its change landed inside #113's squash. #399 was a replacement that GitHub marked "merged" into node-15 (a no-op). Stack #101 unstacked; the rest re-linked as **stack #408** (#233 → #352, 38 PRs). |

## Queue

1. #106–#199 (node-10 … node-55): un-drafted; waiting for GitHub's PR heads to catch up with the rebased branches, then async merge #199.
2. #384 → #385 → #386: rebase onto the new main. #384 fails clippy at `src/clay_rt_wcmp_wasmi/tests.rs:345` (`useless_conversion`: drop `.into()`), and the same failure carries up to #385 and #386.
2a. On the next restack, take intxt's 8f3464bd (on #310): `[builds.wasm32-wasip3]` with `default_features = false` in `src/big_blobs_core/guardrails.toml` (check-matrix on alpha passes; `check-matrix` takes `--package` and `--target`, not `--manifest-path`).
2b. Review follow-ups from #105–#199 (operator's comments; checked against node-102 on 2026-11-25), a follow-up layer on the node stack:
   - done on the stack: the local_state rename and dropping Kv (#112), the inline SQL out of the datum_ref tests (#106), the listener doc comment (#113), the headers catalog (#114);
   - to do: move the `x-nodule-*` headers out of `service_core_rs` (#114); rewrite the "person's nouns" doc at `big_cartridges_core/protocol/installs.rs:57` (#182, "unreadable slop"); a `node.json` setting for the blobs ownership window (#107); sweep "operation" → "method" in the grant docs (#108); eradicate non-domain NSIDs, node half (#108); a guardrail against inline SQL in Rust strings (#106); manifests for every cartridge in the repo (#170);
   - design questions back to the operator: #198 (`#net` covers only ipnet; permission iroh per p2p app?), #106 (steelman content-hashed datums), #107 (local file URLs / presigned reads for blobs; ADR 051 materialize).
3. Audit of every lane's stacks (native stack membership, drafts, red cut points): in progress.
4. Tell the operator what is next for review across all stacks.

## Lane audit (2026-11-25)

| Lane | Open | Native stack | Shape | Ready PRs |
|---|---|---|---|---|
| node | 84 | 3439256 (#101) | linear to node-94; then fans out (node-95..102 hang off node-94) | #199 (landing), #337 (CI re-running after rebase) |
| intxt | 53 + #310 | 3796684, all 53 | linear #123 → #388; #310 forks off intxt-34 (#290), outside the stack | none |
| claytxt | 82 | **none** | one chain #254 → #393, rooted on **intxt-37 (#321)**, not main | #344 (green) |
| automata | 19 | 3692939 has only #164–#188 (6) | #200–#291 base-chained on #188; 5 children fan out from #291 (#328–#331, #341 → #359) | none |
| old distributed-tasks | 8 | 1812399 | #54 → #62, 26 behind main | #54–#62 ready, no CI on their heads |
| singletons | #47 #50 #52 #85 #117 #173 #216 | — | on main | #47 #52 #85 (red E2E) #117 (4 red) #173 #216 ("CI only, do not merge") |

- claytxt (2026-11-25): it handed #384–#386 to the marshal. It will wait for intxt-01…37 to land instead of cutting onto main (its root depends on intxt's node-76 fixups), then `gh stack link 254 … tip` in one pass. #344 stays its cut point.

- **Critical path (verified 2026-11-25 with `git cherry`):** aa0fcdcc..intxt-37 holds 238 commits that are node-09…76 at their pre-rebase SHAs, plus 158 of intxt's own. Order: node #233–#252 (node-56…76) lands → intxt rebases intxt-01…37 onto main and greens #321 (its cut point) → #321 lands → claytxt links its stack and lands #344. intxt hosts its web viewer on the fixture until #381 (node-97) is on main.

- automata (2026-11-25): one linear native stack, #166 (164…205, 215, 291, 331, 328, 330, 329, 341, 359); the former siblings are chained by merge commits. Its cut point will be posted once #341 is green. Coming: an operator-approved service rework (drop Transport::Irpc; `protocol!` macro; wire renames blobs.put/read/stat → putBlob/readBlob/statBlob, dispatch.mintCapability → mintDispatchCapability; long_poll; iroh admission fix). Marshal's advice: land it after the critical path; the irpc removal and the iroh fix may be cut ahead on their own.
- automata (update, same day): no early irpc cut. Removing irpc gives 18 conflicts against node-102 (service_utils_rs/host.rs, service_client_native_rs/transport.rs), so both its cut points, A (irpc removal and the iroh admission fix) and B (macro and renames), wait for the marshal's ping after the critical path. The renames also include `blobs.mintCapability` → `mintBlobsCapability`. Cut A also fixes `service_client_core_rs/xrpc.rs` `decode_response`, which parses every response to a `Value` before decoding it (on main since #84).
- **Own violation:** the node stack fans out at node-94 (#352): node-95/102, node-96/97, node-98, node-99, node-100 and node-101 are siblings. Linearize it when restacking after #199.

Violations to chase:
- claytxt: no native stack, and its root sits on intxt's branch, so claytxt's cut point #344 can't land before intxt-01…37 do.
- automata: 13 PRs outside its native stack; five siblings fan out from #291, which a stack can't hold.
- Ready but not a green cut point: #47, #52, #54–#62 (no CI recorded), #85 and #117 (red), #216 (CI-only tip; should be a draft).
- Candidates to close (ask the operator): #50 (sketch/willow, 2026-09), #52 (likely superseded by #55–#62), #47 (dpaths, 2026-10), #85 (premise "land ahead of #84" is gone). #173 is subsumed by #151 (node-32 carries the same test fix): close it once #151 lands.

## Open questions for the operator

- The empty duplicate commit b36cfc3d on main: leave it (harmless) or rewrite main?
