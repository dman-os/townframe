# claytxt lane ledger

Scope: The claytxt lane's stack, decisions and the request accounting.

Status: living ledger of the claytxt lane (Clay, the kit, micros, daytxt apps, Pierre/CodeView, builds/mbx).
Owner: Claude (claytxt manager); operator reviews and merges.
Updated: 2026-11-24 (rebased onto node-76; layers 56-63: CI fixed and scoped, NSID clay half, the micro clock, blob reach)

Keep this head current. Update it in the same layer as the work it records. The dated sections below
"History" are the original #79 design-settling ledger, kept for its decisions.

## Where things are (2026-11-25)

- **Tip:** layer 79 (#380). #344 (layer 69) was green on CI; layers 70-79 are drafts. Three cuts to main are in
  flight from a lane: the facet-grant port (`clay_docs::FacetAccess`), then the legacy workspace exclusion, then
  mltools out, so every stack's CI stops building the legacy crates.

- **On main from this lane:** #79 (Clay collections/lifecycle/manifests), #80 (shell, kit, Settings), #81 (Android),
  #82 (datum_core), #83 (mbx), #120. Nothing since 2026-11-17.
- **Everything else is the claytxt stack** below. `feat/daytxt` (#216) stays a CI-only integration branch, never a
  PR base; the stack's tip reproduces its tree. #212, #231 and #232 were merged into `feat/daytxt`, not main.
- **Base:** the stack sits on the intxt stack tip (`stack/intxt-37-node76-fixups`, intxt #221, tip #321), which sits
  on nodules' node stack (#105..#199, #233..#252, node-76). Our hosts install intxt and need the node APIs, so the
  stack cannot land before those two. Rebased from node-55 on 2026-11-24 night: upstream's node-76 changes to the
  node directory code (keyring-held seed, `node.json` v2, "server" wording, `--bg`) were ported into `day_node`,
  where layer 29 moved that code.
- **Superseded PRs** (their content is in the stack; close once the stack is reviewed): #86, #88, #89, #90, #91,
  #121, #132, #145, #153, #162, #169, #171, #174, #176, #206, #207, #208, #209, #210, #213, #214, #217, #218,
  #219, #220, #222, #223, #224, #87, #129.
- **Not ours / left alone:** #85 (big_repo/cabinet perf from the node branch), #173 (daybook_core test).

## The stack (`stack/claytxt-NN-<slug>`, each PR based on the one below)

Layers 01-36 are cut mechanically by path from `feat/daytxt` (ecb56b9c, CI-green), so each carries the final state
of its paths and lists the original commits in its message. They are topical and need not build alone. Layers
37+ replay branches that never reached `feat/daytxt`, one squashed commit each. Only the tip is greened.

| Layer | PR | What |
|---|---|---|
| 01-build-manifests | #254 | build: workspace members, lockfiles and tool config for the claytxt crates |
| 02-node-crate-fixes | #255 | fix: build-contract, guardrail and egress fixes in node-side crates that claytxt hosts need |
| 03-clay-surface | #256 | feat(clay_surface): element commands, open data-* and the surface tree outline |
| 04-clay-mdk | #257 | feat(clay_mdk): D57 endpoints, guest engine fixes and the micro stylesheet hook |
| 05-clay-host | #258 | feat(clay_host,clay_guest_session): one requirement resolver and per-mount presentations |
| 06-clay-runtimes | #259 | feat(clay_rt_*): wasi:http aliases, wasi:random and runtime fixes |
| 07-clay-dioxus | #260 | feat(clay_dioxus): shadow-root mounts with adopted sheets for guest and built-in micros |
| 08-clay-node | #261 | feat(clay_node): generic micro hosting on the node, with egress to dispatch.local |
| 09-kit-styles | #262 | feat(clay_dioxus_kit): the styling contract's layers, ct tokens and the Tailwind bundle |
| 10-kit-headless | #263 | feat(clay_dioxus_kit): headless primitives |
| 11-kit-components | #264 | feat(clay_dioxus_kit): contract components (Button, Tabs, Card, PanelPage, Sheet, Drawer, ...) |
| 12-drop-ui | #265 | refactor: delete the ui crate (its tokens and ScrubStrip moved into the kit) |
| 13-pierre-core | #266 | feat(pierre_diffs): crate skeleton, types, diff model and JS-compat helpers |
| 14-pierre-utils | #267 | feat(pierre_diffs): port of @pierre/diffs utils |
| 15-pierre-highlights | #268 | feat(pierre_diffs): highlights and scanners |
| 16-pierre-renderers | #269 | feat(pierre_diffs): renderers |
| 17-pierre-managers | #270 | feat(pierre_diffs): managers |
| 18-pierre-components | #271 | feat(pierre_diffs): components (FileDiff, virtualized hosts) and their upstream tests |
| 19-pierre-oracle | #272 | test(pierre_diffs): differential oracle against upstream @pierre/diffs |
| 20-pierre-parity | #273 | test(pierre_diffs): parity matrix, fake DOM hosts and test utilities |
| 21-pierre-dioxus | #274 | feat(pierre_diffs_dioxus): the Pierre diff view as plain Dioxus in a shadow root |
| 22-micro-shell | #275 | feat(micro_shell): the shell on kit components and a micro stylesheet |
| 23-micro-settings | #276 | feat(micro_settings): settings on kit components |
| 24-micros-small | #277 | refactor(micro_counter,sample,toolbar,layout): follow the styling contract |
| 25-micro-lab | #278 | feat(micro_lab): the lab micro for experiments |
| 26-micro-theme-lab | #279 | feat(micro_theme_lab): the theme lab as an end-user micro with live host apply |
| 27-micro-review | #280 | feat(micro_review): the intxt review micro (intxt lane's feat/intxt-review-daytxt) and review_mock |
| 28-daytxt-core | #281 | refactor: dayframe_composition becomes daytxt_core |
| 29-daytxt-native | #282 | feat(daytxt_native,day_node): one host boot; the GUI host holds a node |
| 30-daytxt-desktop | #283 | refactor: dayframe_desktop becomes daytxt_desktop; the package directory installs every micro |
| 31-daytxt-mobile | #284 | refactor: dayframe_mobile becomes daytxt_mobile (WebView default, own node) |
| 32-daytxt-web | #285 | refactor: dayframe_web becomes daytxt_web (strict CSP web shell) |
| 33-showcase | #286 | feat(claytxt_showcase): page-per-component kit specimens (replaces dayframe_showcase) |
| 34-day-cli | #287 | fix(day_cli): claytxt-side day_cli changes |
| 35-docs-tooling | #288 | docs: styling contract, design language, skills, x scripts and README updates |
| 36-lockfiles | #289 | chore: re-resolve the root and guest lockfiles on the intxt stack tip |
| 38-codeview-c1-c3 | #292 | feat(pierre_diffs): port CodeView (C1–C3) |
| 39-codeview-c4 | #293 | feat(pierre_diffs): CodeView C4, the DOM shell and sticky slab |
| 40-codeview-c5 | #294 | feat(pierre_diffs): CodeView C5, the render pass |
| 41-codeview-c6 | #295 | feat(pierre_diffs_dioxus): CodeView C6, the Dioxus host (CodeViewView) |
| 42-codeview-page-scroll | #296 | feat(pierre_diffs_dioxus): CodeView in the page's scroller, top-file output, ArcIndex file nav |
| 43-call-bytes | #297 | feat(clay_mdk): INTERIM Context::call_bytes for byte-answering node methods |
| 44-nix-daytxt | #298 | build(nix): daytxt packages, guest vendor set and static web site as flake outputs |
| 45-legacy-out-of-workspace | #299 | chore: take the legacy daybook/plug crates out of the Cargo workspaces and drop uniffi |
| 46-link-perf | #300 | build: mold on aarch64 and no dependency debug info in dev |
| 47-cranelift-cache-key | #301 | chore(deps): bump wasmtime and wcmp forks so cranelift caches across worktrees |
| 48-agent-gui-loop | #302 | feat(daytxt_web): agent GUI loop, headless Playwright over the daytxt web shell |
| 49-design-session-prep | #303 | docs: design session prep (open questions) |
| 50-mbx-hit-rate | #304 | docs(scratch): why the mbx hit rate is low, and a build-timings logger |
| 51-kotlin-catalogue | #305 | docs(scratch): catalogue of the Kotlin app (daybook_compose) and what replaces it |
| 52-blitz-gap-log | #306 | docs(wiki): Blitz gap log |
| 53-claytxt-design-docs | #307 | docs(scratch): claytxt lane design docs and ledger |
| 54-tip-fixes | #308 | fix: stack tip fixes (kit bundle regenerated, day_cli on day_node's NodeDir) |
| 55-ledger | #309 | docs(scratch): the claytxt ledger rewritten for the stack and the request accounting |
| 56-review-blobs | #313 | fix(daytxt_native,clay_node,micro_review): the review micro decodes its config and reads patches through call_bytes |
| 57-ci-fixes | #314 | fix: CI on the stack tip (doc metadata, guardrails, pierre budgets, the wit-deps hash) |
| 58-ci-scope | #315 | ci: scope PR checks to what changed |
| 59-nsid-clay | #316 | refactor(clay): owned identifiers stop posing as NSIDs (ADR 050 §2) |
| 60-cx-clock | #317 | feat(clay_mdk): Context::sleep and Context::now for micros |
| 61-wit-deps-stale-hash | #318 | build(nix): the wit-deps FOD name carries its inputs' digest, so a stale hash fails at once |
| 62-ledger-docs | #319 | docs(scratch): ledger update and the web components step 2 note |
| 63-review-blob-reach | #323 | feat(clay_node,daytxt_native): the review micro's blobCap reaches its intxt drawers |
| 64-review-codeview | #324 | feat(micro_review): Show changes renders the patch in CodeView (intxt's commit, carried) |
| 65-ci-regen | #332 | chore: regenerate derived files the base left stale (sql gen, wire sidecars, lexicon snaps, CLI docs) |
| 66-style-contain | #338 | fix(clay_surface): admit contain and isolation, and name a refused style property |
| 67-log-faults | #339 | fix(clay_host): log a micro fault where the supervisor records it |
| 68-mltools-out | #340 | chore: take mltools out of the workspace with the legacy daybook crates |
| 69-codeview-admitted | #344 | test(pierre_diffs_dioxus): every CodeView batch admitted by a Clay surface; batch limits sized for it |
| 70-clay-docs-note | #346 | docs(adr): ADR 054, Clay docs on the collection waist |
| 71-docs-source | #351 | feat(clay_cabinet): docs.datums feeds per authority over a fake cabinet |
| 72-predicate-evaluator | #353 | feat(big_cabinet_core): in-memory predicate evaluator and conformance table (ADR 054 §7) |
| 73-waker-proposal | #354 | docs(scratch): waker-driven render proposal |
| 74-specimens-crate | #355 | refactor: specimen and foundation pages move to clay_dioxus_specimens |
| 75-outline-admitted | #356 | fix(clay_surface): admit outlines; every specimen page admitted by a Clay surface |
| 76-specimen-board | #357 | feat(micro_theme_lab): the specimen board |
| 77-clay-nsids | #363 | feat(clay): the Clay half of ADR 050 §2 NSIDs (LexRef, Clay Lexicons, Contract enum) |
| 78-lab-panes | #358 | feat(micro_theme_lab): navigator, canvas and inspector |
| 79-colour-adr | #380 | docs(adr): ADR 055, the colour system |

## Accounting of operator requests (2026-11-03..24)

Built from the raw chat on 2026-11-24 (83 requests: 23 on main, 23 done but unmerged, 19 partial, 11 not done,
1 superseded, 6 unverified). The unmerged ones are all in the stack above. Open, most recent and strongest first:

1. **Stack and ledger hygiene** (asked 11-18 and 11-23): done by this rewrite; keep layers small from here on.
2. **CI improvements** (approved 11-19): path-scoped jobs over `origin/main...HEAD` and nextest final failure output
   are in (layer 58); the tip was greened (layer 57). Open: releases only on main, and which bucket backs the cache
   (the operator's call); Magic Nix Cache throttling (HTTP 418) still fails runs at random and masked a stale
   wit-deps hash for days.
3. **Land the stack** once intxt #221 and the node stack land.
4. **Web components step 2:** mounts as `<ct-…>` custom elements, real `part` attributes, slots (design session).
5. **CodeView parity:** bottom-lines blanking and white flashes on Wry, no syntax highlighting (S9 adapter not
   wired), anti-blanking for the non-CodeView view.
6. **Shell/lab defects:** drag released outside the window keeps dragging; unthemed "hide lab panel" list; lab
   overflow does not scroll; lab micro faults on drag; "+" hover flicker; ScrubStrip activating on scrollbar release.
7. **Kotlin app:** catalogue exists (layer 51); port-or-drop decisions are pending.
8. **Preferences as datums (C1):** only a spike (`wip/claytxt/spike/c1-guest-shell`, `wip/claytxt/dirty/c1-spike`).
9. **Phone viewer:** the panel does not scroll; slow "Loading" screen; release wasm builds on the phone.
10. **Clay on the node:** design accepted 11-17; all code is in the stack, none on main.
11. **TS micro runtime:** deferred to this week, after the node API; whether React/Solid is fast enough in
    QuickJS-in-wasm is unmeasured; cartridge authoring has docs only.
12. Smaller: BoltFFI spike (queued, never started); shell still depends on `micro_*` crates (built-ins should be
    runtime-equivalent); typed props on publish; lazy-activation policy (deferred by the operator); a second
    framework hosting a micro; WIT 0.5.0 bump and the claytxt wire-id rename.
13. **NSIDs name Lexicons** (ADR 050 §2, #235; operator re-raised on #108): the clay half of
    docs/scratch/nsid-audit.md items 1-5, i.e. `org.example.clay.*` collection contracts such as
    `org.example.clay.shell.roots` and the dotted `clay.micro.install` / `clay.micro.bindings` tags, become
    manifest-local keys or host-formed ids. Nodules does the node half above #252.
    Layer 59 did items 4 and 5: collection instance ids are a host-minted `CollectionId` (same text, so stored
    datums read), and the tags became `clay/micro-install` and `clay/micro-bindings`. The rename means install and
    binding facets written under the old tags are no longer read (a desktop falls back to its defaults once).
    Layer 77 did items 1-3: `datum_core::LexRef` (`nsid#def`, bare = `main`) types collection contracts, schemas and
    requests; `org.example.clay.shell#roots` is a token in the `org.example.clay.shell` Lexicon (with `rootKey` and
    `root` siblings), `org.example.clay.micro.manifest` has its Lexicon document, both under `src/clay_mdk/lexicons/`
    and admitted by the datum compiler in `clay_mdk`'s tests; `presentationComponent` is `Contract::Presentation`,
    and a stored endpoint writes it as CBOR null where a call writes its NSID text. Stored presentation endpoints
    under the old text, and collection ids minted from `org.example.clay.shell.roots`, do not carry over.
14. **intxt citizenship (D76), the general mechanisms behind intxt's interim pieces:**
    - `Context::call_bytes` (layer 43) goes away once a method can declare a `method_core::ByteBody` output and
      `Context::call` streams it. Nodules owns the wire; the Clay half is ours, together with the waker-driven
      render below.
    - `ReviewConfig` (minted doc_cap/blob_cap) goes away once micros reach their granted `#drawers` through the
      ADR 049 §6.2 domain handle on Cx.
    - Patch blobs: layer 63 mints the review micro's `blobCap` as `BlobsScope { drawers: {intxtDrawers: Read} }`
      (nodules #244, checked at read time against the drawer's blob inventory) and grants `blobs.read`. intxt
      wires `read_blob` to it. The interim install table names which config members are blob drawers.
    - A clock for micros (asked by intxt for CodeView's pacing): layer 60 adds `Context::sleep` and `Context::now`.
      In a guest the wait runs on an incarnation-owned task that calls `notify_views()` when it ends, the same
      manual wake `call_bytes` needs (design debt below). intxt's CodeView switch goes on top of layer 60.
15. **Declarative shadow DOM for SSR:** intxt's web app (D79) renders the kit and Pierre under dioxus-ssr. The kit is
    plain Dioxus already; the shadow mount needs a `<template shadowrootmode>` form. This is part of the web-components step.
    The design note for the pairing is docs/scratch/web-components-step2.md.
16. **Micro installs move onto the node** (agreed with nodules on 11-24; their ADR 049 §6.1/050 amendment awaits
    operator sign-off). A node `micros` service, isomorphic with nodules, does the actor creation, mints and
    config. The GUI attaches as a client (`micros.attach { cartridgeId }`, `micros.awaitHandles`), in-process or
    over the socket, so the desktop runs while `day` holds the node. A daytxt cartridge declares every micro
    the app ships, and INTERIM_GRANTS goes. review.json disappears once the §6.2 handle and `{drawers}` blobs are
    under us. Mounts never block on the poll. Our part: move clay_node's install side out after sign-off.
    Update (11-24 night): the `micros` service is a nodule in a core cartridge (ADR 053) that opens micro actors
    under its own cartridge actor (node-78); the GUI contract is unchanged. The node SDK builds on our `day_node`
    crate. The node-76 restack conflicts in `day_node` with nodules #239 and #250 (`day_cli/context.rs`).

17. **CodeView for micros (asked by intxt, 11-24 night):** intxt's review micro renders "Show changes" with
    CodeView (their commit, carried above our tip). Our part, in this order: (1) rows on a first render or under
    SSR before any measurement pass, so headless tests and a stopped-clock fixture see file rows; (2) a ScrubStrip
    input with owned labels (the showcase leaks `&'static str` file ids) and an optional or measured viewport;
    (3) `CodeViewFile::from_patch` over bytes that reports non-UTF-8 itself.

Questions still open for the operator:
- None from before 11-25: all seven were answered on 11-24 (see standing decisions). New ones live in ADR 054/055
  and docs/scratch/waker-driven-render.md.

## Standing decisions (summary; dates are when the operator ruled)

- Clay: collections are plain maps, blind writes win, tombstones replicate as rows (11-03); no prefix grants,
  write authority becomes a query later (11-04); Context shutdown is RAII (11-04); no dynamic dispatch unless
  needed (11-06).
- Micros: most are wasm, built-ins only for native resources (11-04, 11-06); web components are the presentation
  protocol, and we embrace them fully (11-06, 11-10, 11-19); open `data-*` (11-10); micros reach the node over
  WASI HTTP and say "datum" (11-12); TypeScript default with Rust first class, JS in wasm, no V8 spike (11-17);
  partial replication stays (11-18); `claytxt` is the WIT namespace (11-19).
- Styling: `--ct` tokens, stable part names, layers (host kit, micro, user), no inline styles, cross-framework
  (11-17, 11-18); CSS prefix `ct` (11-17).
- Names: the app is daytxt, the environment claytxt, the node "day node"; Dayframe is free (11-11, 11-18).
  ArcIndex became ScrubStrip; D57 endpoints are method, callback and view (11-18).
- Apps: desktop, Android and web (localhost node); desktop first and works-on-desktop means works-on-mobile
  (11-18); Android renders in the system WebView (11-18); the desktop holds a node shared with the CLI (11-18).
- Pierre: honest line-by-line port with a differential oracle (11-05); CodeView is the default PR view, in the
  page's scroller (11-17); a generic virtualizer waits for a need (11-12).
- Process: one linear stack of small draft PRs, mechanical splits fine, only the tip green, the operator merges
  with their own tooling (11-17, 11-18, 11-23); push everything to origin, WIP included (11-23); never block on
  Blitz (11-10, 11-17); never file on third-party repos (11-19); clippy only right before pushing, full suites on
  CI (11-19); mbx keeps 10 GiB free and uses the rest (11-18).

- 11-24/25 (operator): pierre budget exemptions OK; close superseded PRs we own; clay_mdk may take any
  dev-dependency; sysadmin e2e is not this lane's; CI cache bucket deferred; formatting is not a concern; mltools out.
  ADR 052/053 signed: micro wasm runs in the host, never on the node; the micros nodule manages installs, grants and
  minting; actors are `micros/<cartridge>/<micro>`; the micros component is embedded so the app starts offline.
  Cartridge micros: replacement by name is the cross-cartridge question; settings are exposed (permissions are
  identical across a cartridge's nodules and micros); a micro failing to load never breaks boot.
- Web components (11-24): the sandbox is wasm plus the host-checked wire, not shadow DOM; scoped registries;
  host-generated tag names per binding, requested by manifest-local binding name (no NSID); a sanitized
  `setHTML`-like op (11-25); built-in micros isomorphic with wasm ones.
- NSIDs (ADR 050 §2, nodules' record): an NSID names a Lexicon, only for what another cartridge could provide; no
  custom primary types; presentations named by WIT interface. Clay half done in layer 77.
- Clay docs (ADR 054, 11-24/25): micros mount queries over node docs as collections; feeds per authority, the node
  is the only filter, no host grants; values cached by version; hydration is the budget; membership pages plus a
  per-micro value stream; one predicate evaluator with a conformance table; no drawer interest, known drawers
  stream their meta group (nodules node-95).
- Styling (11-25): N stylesheets in an ordered stack (shipped, installed, the user's own), each its own layer, all
  above a micro's own sheet; no "identity" layer. Colour system (ADR 055): palette ramps, roles derived, scheme and
  contrast settings via `color-scheme`/`light-dark()`; implementation later.
- Process (11-25): no back-compat, shims or migrations for anything not on main; all non-GUI work (builds, clippy,
  nextest, xtask) on the motherships (mothership, mothership-alpha), the laptop for GUI only.

## Design debt on record

- **Guest view wakes are opt-in** (11-19): a cross-executor delivery needs a manual `notify_views()`; the
  direction is a render request in the waker. Details below.
- **Web components: embrace fully, soon** (11-19). Details below.
- **A view's stylesheet signal is written during render** (`use_mount_stylesheet`, layer 07): revisit with the
  custom-element mounts.

# History

## The #79 design-settling ledger (2026-11-03..06)

Status: Living ledger. Decisions are marked as decided; the in/out list is a proposal awaiting the operator.
Owner: Claude (manager) with Sonnet subagents; operator reviews.
Scope: Branch `feat/better-collections`, the `clay_*` / `micro_*` crates and ADRs 017/029/030/031.
Updated: 2026-11-03 (C0 found incomplete; C1 extended to finish the scope migration)

Normative design: ADRs 017, 029, 030, 031. `clay-architecture-critique.md` is history and the
findings index (F*, Q*, W*). This file is the live state of the PR: what is in scope, what is
decided, what is running, what is open. Update it whenever a checkpoint lands or a decision is made.

## Goal and boundaries

- Settle every open Clay design problem so the next PR can build real micros.
- Clay runs independently on mock data. **No node / big_sync / big_cabinet integration in this PR.**
- `../townframe-p3N-2` (`feat/nodules`) holds `big_cartridges_core` and `big_nodule`. Read them only to
  keep the enablement/config *shape* aligned. Nothing here depends on them landing.
- Existing clay code is disposable. Rewrite rather than shim (AGENTS.md "Breaking changes").

## Decisions taken in this PR (beyond the ADR text)

- 2026-11-03: Publication kinds are Call and Presentation only. Refs are bound or lazy; every use
  is async; mount is sync (ADR 031 §§5–8). Calls are served by one exported
  `call(publication, request)` dispatched by name, not by per-publication exported resources.
- 2026-11-03: Micros are singletons; the micro id is the only identity (ADR 030 §1).
- 2026-11-03: Durable backing is an ordered KV store (prefix scan plus atomic batch): SQLite native,
  IndexedDB browser. `wflow_core::KvStore` is unsuitable (it has no scan or batch).
- 2026-11-03: Collections are independent consistency domains. Per-collection atomic commit, reader
  wakes deferred until every affected collection commits, no cross-collection epoch (ADR 017 §6).

## Checkpoints

| # | Item | State |
|---|---|---|
| C0 | Recover gen-34 B (`9f19a2fb`, scope-owned registration rows) | was broken; completed by C1 |
| C1 | Collapse `_deferred` (dispatch carries output `T`); migrate `clay_guest_session` + engines onto scopes | committed `e22f80b0`; 191/191 (clay_collection, clay_guest_session, clay_rt_wasmtime, clay_rt_wcmp_wasmi), clippy clean. Scopes close at teardown **after** `instance-root.close()` (ADR 030 §6), not at Closing |
| C2 | Lifecycle core: evolve `clay_host::Supervisor` (see "Two controllers" below) | committed `b2f380df` (134/134, guardrails clean) |
| C2b | Move MDK + shells (`dayframe_composition`, desktop, web) onto the Supervisor; delete `clay_micro_core` | committed (see "C2b done"); web Playwright spec updated, not yet run |
| C3a | Host side, no WIT: `Send` activation bridge in `clay_host`; bank refs Bound/Lazy; Call + Presentation publication kinds; in-process tests | committed `a7df27a1` (92/92 host side; 7 engine tests stubbed until C3b/C3c) |
| C3b | WIT reshape + Wasmtime adapter + lifecycle fixture (call + presentation publications, lazy consumer); owned `ContractIdentity` + per-call contract; drain queued activations when `drive` drops | committed `27a07a83` (123/123; 6 wcmp_wasmi tests stubbed until C3c) |
| C3d | Calls as store tasks (lock per burst); serialized per incarnation; waits-for graph refuses cycles (`call-cycle`); forced close mid-call skips guest close | committed `9e87066b` (129/129, 20x loop clean). Fallback baseline; superseded by C3e if the stream spike passes |
| C3e-spike | Prove `serve(inbox: stream<request{publication,payload,responder}>)` on Wasmtime + WCMP: concurrency, sync entry, nested await, drain-on-close | committed `44bbacd2`: all 5 points pass on Wasmtime + WCMP(wasmi), no fork changes |
| C3e | Replace per-call `call` export + `instance-root.close()` with the serve inbox; MDK owns the loop; drop serialization (keep waits-for graph only if still needed) | committed `e1044a06` (243/243 host+session+wasmtime+collection; call/close conformance 20x clean) |
| C3f | WCMP fork: store-task `spawn` (Wasmtime-like JoinHandle), branch `store-spawn` on dman-os | pushed `dman-os/store-spawn` @ `2a3de028` (7 new fork tests; fork Nix lanes broken by pre-existing wasm-bindgen 0.2.127 hash gap) |
| C3c | Repin WCMP to `store-spawn` + port onto the C3e model; check WCMP host-task starvation under guest `thread.yield` | agent running (fresh) |
| C4a | `clay_store`: `OrderedStore` trait (get / scan_prefix / atomic `apply(Vec<BatchOp>)`), `MemoryStore`, `SqliteStore` (sqlite_rs, one `clay_store_entry` table), `IndexedDbStore` (compile-only) | committed `bc52dd23` (17/17, clippy native+wasm clean) |
| C4b | Durable backing in `clay_collection`: scope id = Incarnation(counter) or Installation(durable key); async durable constructor loads before existing; privileged whole-scope `install`/`uninstall` (persist first, then project); incarnation scopes never persisted | committed `d67ff053` (128/128 clay_collection+clay_store; verified by me, agent report lost) |
| C4c | IndexedDB exercised in a browser (dayframe_web e2e: install, reload, rows present) | after C2b (needs a building web shell) |

**App crates are red** since `3e55cbee` ("narrow collections waist", before this session): it
deleted `clay_host::{HostedCollection, CollectionReader}`, which `clay_micro_core`, native `clay_mdk`,
`micro_settings*` and the `dayframe_*` shells still use. Decision: don't patch, because C2b deletes the
old controller and C6 rewrites the MDK collection API. Verify sets exclude those crates until then.
Wasm fixtures: `deno run --allow-all x/check-dayframe.ts --prepare-only`, then export the printed
`DAYFRAME_*` vars. `wit/deps` in a fresh worktree come from `deno run --allow-all x/wit-fetch.ts`.
| C5 | Manifest/catalog admission + Clay enablement/config service on mock storage, shaped like big_nodule | after C4 |
| C6 | Authoring foundation: one semantic `Context` (native + guest), OCI-owned manifest, drop `build.rs`/custom section, multi-presentation | after C3 |
| C7 | Validation micros: toolbar → completion → layout, exercising ADR 031 validation list | last |

## Two controllers (found 2026-11-03)

- `clay_micro_core::micros::{composition, CompositionBroker, MicroActivation}` is the **old**
  controller and the one the shipped app uses: `clay_mdk::{native,wasm,wcmp}`,
  `dayframe_composition`, `dayframe_desktop`, `dayframe_web/shell.rs`. Its model is pre-ADR-031:
  declarations plus `accept`/`accept_all`, and typed Rust `Provision` (`Any`) publications.
- `clay_host::Supervisor` (`src/clay_host/supervisor.rs`, gen-33 A) is the **ADR 030** controller.
  It is engine-neutral, built on `Factory`/`Root`, has strong requirement `Binding`s, staged init,
  and ordered close. Only `clay_guest_session/lifecycle_conformance.rs` uses it.
- Decision: evolve the Supervisor (C2) and delete the old controller (C2b). C2 deltas:
  - `Root::provision(contract)` becomes named publications returned at init (ADR 031 §8).
  - Remove `ActivationRegistration`/`RegistrationTemplate`/`install`/`uninstall`. Durable rows
    replace them (C4/C5).
  - Demand API: start-or-join one pending activation per micro, returning a strong lease. Dropping
    the last external lease triggers idle close; an in-flight call holds a temporary lease.
  - `status()` without activation; demand during `Closing` waits for the next incarnation.
- C2 brief (exact deltas, from reading `supervisor.rs`):
  1. Delete `accepts`, `registration_grants`, `routes`, `RegistrationTemplate`, `ActivationRegistration`,
     `RegistrationError`, `install`/`uninstall`, and the `Requirement.provider: None` path.
     A requirement becomes `{ provider: MicroId, publication: String }`, resolved by configuration.
  2. `Factory::initialize` returns `Initialized<P> { root: Box<dyn Root>, publications: BTreeMap<String, P> }`.
     `Root` keeps only `close`. `provision(identity, contract)` becomes `publication(micro, name)`.
     A requirement whose publication is missing → `Unavailable`; the provider stays up.
  3. `MicroId` newtype moves into `clay_host` (the old one dies with `clay_micro_core::micros` in C2b).
  4. New external strong edge: `acquire(micro, publication) -> Result<Lease<P>, StartError>`
     starts or joins the activation (existing `Starting`/`Initializing` + `Notify` join). `Lease` derefs to `P`.
     Retention count = consumer requirement bindings + live leases. At 0 and not eager → idle close.
     `Lease::drop` is sync, so it enqueues; a `Supervisor::drive()` future (spawned local by the host,
     `.unwrap()` at the top) runs idle closes. No timers yet (idle grace is deferred).
  5. Close order: mark `Closing` (new acquires wait via the existing `AwaitClosed`) → `root.close()` with
     dependencies held → drop the incarnation's publications (revokes them) → instance teardown
     (removes incarnation registration scopes) → release providers. Publications are dropped **before**
     scope removal (ADR 017 §6 fail-closed argument).
  6. Replace the `#[allow]` at `supervisor.rs:510`.
  7. Tests (`supervisor/tests.rs`): N concurrent acquires → one `initialize`; last lease drop → `close`
     exactly once; eager root survives lease churn; acquire during `Closing` → fresh incarnation;
     faulted not retried by demand, only by `retry`; unknown publication → `Unavailable`, incarnation
     stays; acquire resolves only after `initialize` returns (staged visibility); cycle → `Cycle`.
  8. Port `clay_guest_session/lifecycle_conformance.rs` and both engines' lifecycle tests off
     registrations: the "contributor" scenario becomes a requirement plus lease scenario. Lazy rows
     come in C3/C4.
- Implication for F6: typed native `Provision`s go away. Micro-to-micro behavior is only Call or
  Presentation over CBOR Lexicons, so Settings becomes portable by construction. Native-only
  platform micros keep an escape hatch for platform crates, not for micro-to-micro types.

## Open findings from the critique: proposed disposition (operator to confirm)

In scope (design-settling):
- F5/W3/Q5: configuration and boot composition model. The composition root names no micro; drop
  demo controls. Covered by C5.
- F6/W5/Q9: portable Settings and the isomorphic MDK. Covered by C6.
- F7/F10/W6/Q8: package identity by content digest; dedupe `clay_mdk::{wasm,wcmp}` and the
  `clay_rt_*` adapters. Identity is C5 (catalog); dedupe falls out of C3/C6.
- ADR 017 validation #3: one causal shared-row contribution collection. Design is in ADR 017 §10, no
  code. Needs an in-scope decision.

Proposed out of scope:
- ADR 017 validation #4: node-backed projection with host-owned optimism. Excluded (no node);
  ADR 017 stays Proposed on that item alone.
- W7 `dayframe_app` / mobile shell; W8 browser isolation; W9 hop-cost measurement.
- F8 (cross-hop ids): superseded by the ADR 017 rewrite. Close it.

## Open questions for the operator

- Is the causal writer (ADR 017 §10) part of settling the design here, or deferred with the node work?
- Confirm the in/out list above.

## Guardrails state

Pre-existing failures from `85835823 wip`: fixed `clay_guest_session` `test-utils` (native build
contract) and the `clay_mdk/manifest.rs` inline test size (moved to `manifest_tests.rs`). Still open:
`#[allow]` at `lifecycle_conformance.rs:92` (assigned to the C1 agent) and `clay_host/supervisor.rs:510`
(C2 rewrites it). `clay_store` items belong to the C4a agent. Note that `wit/deps` dirs are
git-ignored; C1 copied them from `../townframe-p3N-3` to build `wasm_utils_rs`. How to generate them
in a fresh checkout: TBD.

## Agent log

- C1: Sonnet/medium, collapsing `_deferred` methods in `clay_collection::registration` and updating
  callers. Verify: clippy over clay crates, nextest `clay_collection` + `clay_guest_session`.
- C4a: Sonnet/medium, new `src/clay_store` crate: `OrderedKv` trait (get / scan_prefix / atomic
  batch with DeletePrefix), in-memory + `sqlite_rs` + IndexedDB (compile-only), shared contract
  tests. Browser test of IndexedDB is deferred to a dayframe_web e2e in C4b.

## C3 design (2026-11-03)

Constraint found: engine store state must be `Send`. `WasiView: Send`, and `WasmStoreState` holds the
`CollectionSession`/`CapabilitySession`, which are why the bank is `Arc`/`Mutex`. The Supervisor is
`Rc`/`RefCell` on the local executor. So a guest host import that redeems a lazy ref cannot hold
the Supervisor directly.

- **Activation bridge (clay_host).** `Supervisor::activator() -> Activator<P>`: a `Clone + Send + Sync`
  handle holding an mpsc sender. `Activator::acquire(micro, publication).await` sends a request with
  a oneshot reply. `drive()` serves those requests concurrently (a `FuturesUnordered` inside the loop,
  so one slow activation doesn't block idle closes) alongside the idle queue. `Lease<P>` becomes `Send`:
  `Arc<AtomicUsize>` retention and an `Arc` idle queue with a tokio `Notify`. `Activator::status(micro)`
  reads an `Arc<Mutex<BTreeMap<MicroId, MicroState>>>` mirror that `Supervisor::set` updates, so
  status never activates and never round-trips. Channel lifetime: the supervisor outlives every
  incarnation (shutdown closes them first), so a failed send is an invariant break and panics.
- **Bank refs (clay_guest_session::capabilities).** `CapabilityRef` targets `Bound(publication)` or
  `Lazy { micro, publication, contract, activator }`. Methods: `status()`, `call(request).await`,
  `retain_call().await -> CallBinding`, `redeem_presentation().await -> PresentationComponent`.
  Lazy use = `activator.acquire` → `Lease<CapabilityPublication>` → a proxy that owns the lease
  (strong). `call` holds the lease only for the call. Bound use never activates; it is `revoked`
  after its incarnation closes. Contract checks happen at redemption, as today.
- **Publication kinds.** `PublicationInner` carries `kind: Call | Presentation` and its name.
  Presentation keeps today's `ComponentRef` path. Call needs a cross-incarnation call path: the
  engine registers a `CallTarget` (a `Send` handle into the provider incarnation's driver, which runs
  the guest's exported `call(publication, request)`) when it publishes. In C3a, test with an
  in-process `CallTarget`.
- **WIT (C3b).** `capabilities.wit` per ADR 030 §9; `micro-lifecycle.initialized { root,
  publications: list<named-publication> }` replaces `provisions`; export
  `call(publication, request)`; `presentation-events.mount` gains the publication name.
  `publish-presentation-component()` becomes `publish-presentation(name)` plus `publish-call(name)`.
- **Lazy minting from rows.** `CapabilitySession::mint_lazy(endpoint)` exists in C3a. Projection
  minting for durable rows is wired in C5 together with manifest admission.

## C3a notes carried forward
- Lazy `RefStatus` mapping: Active→Live; Idle, Initializing, Closing→Dormant; Blocked or unknown→Unavailable;
  Disabled→Disabled; Faulted→Faulted. A bound ref is Revoked once its publication or recipient closes.
- A cancelled requester (reply send fails) drops the lease, which queues an idle check, rather than
  panicking. That is accepted.
- `Activator::status` mirror is updated only in `Supervisor::set`, so it may show Active for an
  incarnation that ended outside supervision until the supervisor next observes it. Accepted
  (status is a snapshot).

## C3b decisions (2026-11-03)
- WIT package is 0.4.0 (breaking: sync `as-presentation-component` removed).
- Declared publications reach the engine via `InitialContext.publications` from the caller. C5 derives
  them from the manifest.
- Call errors: `capability(capability-error)` for bank failures and `provider(string)` for handler
  errors. A guest trap during a call is `provider(..)` and closes the incarnation. A provider gone
  mid-call is `Revoked`, since only a forced disable can cause it.
- MDK micros publish nothing by name until C6. That is intended.

## Guest concurrency finding (2026-11-04)
- On `wasm32-wasip3`, wit-bindgen (0.62, unchanged in 0.63) stores the export-task pointer in a libc
  TLS (`std`) or the `wasip3_context` hook. Our wasi-libc v34 does not swap it per CM task, so a
  second async export task (`call`, `initialize`, `instance-root.close`) entering while one is
  suspended asserts in `start_task`. Wasmtime itself allows the re-entry. Upstream LLVM PR #175800
  moves wasip3 SP/TLS into CM `context.get/set`; whether it is merged, and which rustc/wasi-sdk ship it,
  is unverified.
- Baseline committed (C3d): serialize, plus a waits-for graph.
- Proposed to the operator: an **inbox model**. One long-lived async `serve(inbox: stream<call-request>)`
  export started after `initialize`; requests are spawned inside that single task; closing the inbox =
  close (it replaces `instance-root.close()`). This gives concurrent calls and A→B→A today, with no toolchain
  bump. Requires proving `stream<record>` on our wit-bindgen/Wasmtime pins.

## Writes / materializer (2026-11-04)
- Registration scopes are the RAII contributor sets: incarnation-scope rows end with the incarnation;
  installation-scope rows end with uninstall.
- Missing: ADR 017 §9 host **materializer** with an explicit merge policy (union / grouping / priority /
  host-ordered LWW / reject collisions). Readers get the raw per-scope union today. Proposed for this PR.
- ADR 017 §10 field-level causal writer: proposed deferred until a workload needs it (operator to confirm).
- Out of scope (operator agreed 2026-11-04): node-backed optimism, `dayframe_app`/mobile, browser
  isolation, browser hop-cost measurement.

## Decision 2026-11-04: guest inbox model (operator approved)
The guest is an actor with a long-lived main loop: `serve(inbox)` runs for the incarnation, handlers run
concurrently inside it, and closing the inbox is close. This is the design, not only a workaround:
wit-bindgen-spawned work is scoped to its export task, so GUI and background work needs a long-lived
task anyway. Per-call exports may be added back once the wasip3 TLS gap is fixed upstream. The MDK
hides the loop (authors write `async fn` handlers). Costs we own: per-request cancellation signal,
backpressure, and per-call attribution. Risk to retire first: `stream<T>` support on the WCMP polyfill.

## 2026-11-04: materializer reverted; collection model under discussion
- C8 (merge policies inside `RegistrationCollection`) was reverted in `e9817a3b`. Collections are plain maps;
  union/priority/group are N collections or reader-side queries. See memory `collections-are-plain-maps`.
- Proposed model (operator discussing): a retractable LWW map. Each live scope holds its latest write per
  key; readers see the highest host-sequence write among live scopes; scope close reveals the next. The host's
  global order supplies the causal order (observed context = one sequence number). Values are written whole,
  and structure goes in keys.
- Operator: no per-key "withdraw contribution" for guests yet, so `remove` asserts absence.
- Open: two retention classes ("survives the writer" and "goes with the writer"), orthogonal to durability.
  Does the collection owner or the consumer decide?

## C3e provisional decisions (operator has not ruled)
In-flight limit 256 per incarnation (overridable via `InitialContext`); a cancelled handler's future is dropped
(no cancel token); orderly close waits for handlers without bound; a forced close with requests in flight
abandons `serve`; a guest trap during `serve` means callers see `Revoked`.

## TLS hook finding (2026-11-04, under investigation)
`clay_mdk/lib.rs` overrides `__wasilibc_task_hook` so every CM task shares one TLS block. This may be the
real cause of the "one suspended async export" abort that ADR 030 §7a attributes to wasi-libc. An agent is
verifying with a hook-free guest. Do not rely on the §7a TLS rationale until it reports.

## TLS hook finding: resolved (2026-11-04)
- Root cause of the "one suspended async export" abort is **our** MDK `__wasilibc_task_hook` (shared TLS,
  needed so Dioxus/MDK thread-locals survive from `activate` into `mount`) combined with wit-bindgen 0.62
  p3+`std` keeping task state in a `thread_local!`. Stock wasi-libc v34 is per-task TLS and works.
  ADR 030 §7a's toolchain rationale is **wrong**.
- Verified fix on Wasmtime 04bb391, in a scratch crate:
  - wit-bindgen with `default-features = false`. `std` is on today through the workspace dep's default
    features, reached via clay_mdk, micro_collection_probe and wasm_utils_rs.
  - The MDK defines `wit_bindgen_0_62_0_{get,set}_task_context` (a `CURRENT` pointer).
  - The hook parks the task state in CM context slot 0 on AsyncBlock and restores it on AsyncResume,
    exactly as stock v34 parks the TLS base.
  - Result: overlapping async exports complete out of order, and sync exports still see shared TLS.
- Consequence: calls can be ordinary reentrant async exports, one CM task per call. The inbox loop
  (`e1044a06`) and the C3d serialization (`9e87066b`) are unjustified and should be reverted pending
  operator go-ahead.
- Untested: WCMP / browser JSPI keeping slot 0 per task; cancelled parked tasks leaking TaskState.
  The symbol names embed the wit-bindgen version, so add a guardrail that `std` stays off on guest targets.
- C3c partial work (repin + inbox port) is uncommitted in the tree; the inbox part is now moot.

## Collection write semantics: operator correction (2026-11-04)
- Not CAS. Every write carries what it was based on (its Lamport timestamp) and is **always accepted**.
  Per key, the winning write has the highest Lamport timestamp: a write that observed another wins over it
  regardless of arrival order. Equal timestamps (concurrent writes) use a deterministic tie-break. CAS may be
  offered as an opt-in on top.
- Retraction: a dead scope's writes leave the candidate set, and the winner is recomputed by (timestamp,
  tie-break) over the rest.
- "Plain map" was an over-simplification: a collection has one fixed causal merge semantics, with no
  pluggable policies.
- Open: what timestamp a write with no `based_on` gets. Proposed: the MDK fills it from the writer's
  replica of that key.

## Blocker hunt (2026-11-04, running)
Two isolated investigations, both in scratch crates outside the repo:
1. Wasmtime: real Dioxus under overlapping async exports, cancellation, background work after return and
   cooperative threads, reentrancy, close with calls in flight, backpressure.
2. WCMP wasmi + browser JSPI: context slot 0 per task, overlap, starvation, cancellation.
Reverts of `e1044a06`/`9e87066b` wait for these results and operator go-ahead.
- Decided 2026-11-04: at the waist, a write with no `based_on` is a **concurrent creation**. The MDK
  defaults `based_on` to the writer's replica of that key, so an ordinary write supersedes everything
  its writer has observed. That matches the intuition from non-causal LWW structures, while writes the
  writer never saw are still resolved by (Lamport timestamp, tie-break).
- WCMP/browser result: per-call callback-ABI async exports work on WCMP wasmi and headless Chromium 156
  (JSPI engine). Slot 0 is per task (`Thread.context` in `concurrency/thread.rs:41`), overlap of 2 and 50
  calls works, and sync entry is fine. No blocker found for per-call exports.
  - Hazards (medium): a continuously self-yielding guest starves host tasks (`run_turn`,
    `store_context.rs:1097-1205`); wit-bindgen 0.62 yields only via `yield_async` or self-waking futures.
  - No host-side cancel of an export call: dropping the call future cancels nothing, and `JoinHandle::abort`
    only drops the host future, so the guest task runs to completion. Same as Wasmtime at that version, per
    PDD023.
  - Unproven: a real wit-bindgen + MDK-hook guest (the test was hand-written WAT emulating the protocol), the
    guest-side TASK_CANCELLED path, stackful/JSPI-blocking guests, non-Chromium browsers.
- Wasmtime result: no blocker forces a custom loop. Per-call async exports work with real Dioxus (out-of-order
  overlap, sync dispatch interleaved), background work after return (`spawn_local`), reentrancy, 2000
  overlapping calls, and concurrent close.
  1. (high, must configure) Guest wit-bindgen `default-features=false`, features `async, macros, realloc,
     async-spawn, inter-task-wakeup`. Without `inter-task-wakeup`, a cross-task wake panics the instance. Needs
     a guardrail that `std` stays off and that the versioned task-context symbols match.
  2. (medium) No host-initiated cancel of a call in this Wasmtime pin (#11833 open) or in WCMP. Guest-to-guest
     `subtask.cancel` works and does not leak (5001 cycles). Host-side cancel needs a guest-visible token.
  3. (medium) A guest trap poisons the instance; suspended calls never resolve. The host must fail
     outstanding calls itself.
  4. (low-med) Dropping a queued (unstarted) call future traps the store, and backpressure also blocks sync
     exports. Admission control belongs in the host.
  5. (low) A host import must not re-enter the same instance on its first poll; yield or spawn first.
  - Author rules: no RefCell/signal/Dioxus guards across `.await`; Dioxus global APIs after resume need
    `dom.in_runtime`; panics kill the instance.
  - Cooperative threads exist in Wasmtime behind a Config flag, but wit-bindgen 0.62 doesn't wrap
    thread.new/switch.

## Collection model decided (2026-11-04, operator approved)

- One causal map per collection. Per logical key, each live writer scope holds at most one candidate
  (a value or a tombstone) stamped `(lamport, writer id)`. Readers see the winner: the highest stamp.
- Stamp: `lamport = based_on.lamport + 1`, or `1` with no `based_on` (concurrent creation). Writer id is
  a stable, ordered identity, so the tie-break survives restarts. The MDK defaults `based_on` to the
  writer's replica of the key.
- CAS is opt-in: the write fails unless the current winner is exactly the expected stamp (or absent).
- Retention is the writer's scope, not the collection's or the consumer's: incarnation (dies on close),
  installation (dies on uninstall), permanent (never retracted, needs a manifest grant). Scope death
  retracts its candidates, tombstones included, and recomputes only the touched keys.
- A deleting micro that goes away un-deletes: accepted consequence of scope-owned tombstones.
- No `clear` and no per-key withdraw for guests. Write authority is a key-prefix grant; it replaces
  ADR 030 §13 `writer_policy`. Grants live in the host layer, not in `clay_collection`.
- Plan: C9a core rework in `clay_collection` (worktree agent), C9b consumers + WIT + grants + MDK
  `based_on` default after the per-call exports agent lands, C9c ADR 017 §9–11 / 030 §13 / 031 §4.

## C9a status (2026-11-04)

- Done on worktree branch `c9a-causal-map` (c9271e59, a86a32ae; 123/123, clippy clean, verified by
  manager). Not merged: it breaks consumers; merge after the WCMP port lands, then C9b.
- Installed scopes have an async `InstalledWriter`; durable writes serialize per key across the store
  await, sync incarnation writes that slip in make a durable CAS fail and the store row is restored.
- `install` replaces a scope wholesale. Supervisor rule for C9b/C5: manifest-declared rows and runtime
  writes use separate installation scope keys per micro, so an upgrade's reinstall never drops runtime
  rows (settings); uninstall retracts both.
- Open with the operator: how a writer names an observed tombstone. Readers never see tombstone stamps,
  so the MDK `based_on` default treats a re-creation after a seen delete as concurrent and it loses.
  Proposed (b): writes carry the writer's feed position; the host derives the observed lamport.

## Per-call exports landed (2026-11-04)

- Wasmtime 404cdd21, ADR 030 §7a 13b89a49, WCMP repin 1ebb05a7 (fork 7604dc8: `run_until_stalled`
  reports `Stalled::{Idle, Yielded}`; a yielded task starved later tasks), WCMP port a34f643e. Both
  engines run the same 15 conformance scenarios; 294/294 across the clay crates.
- Open: browser (JSPI) lane untested for per-call exports, and `Resuming` still maps to `Idle`; the
  fork's own nix lint/test lanes not yet run on 7604dc8; the guest cancel path is unexercised by host
  tests (no engine can cancel a guest task from the host).
- C9b (consumer migration, WIT stamps/basis, prefix grants, MDK default) running on `c9a-causal-map`.

## Tombstones replicate as rows (decided 2026-11-04)

- Operator: tombstones carry their stamp like any row. The registration projection value is
  `Stamped<Option<V>>`; a winning tombstone publishes an Upsert of `None`, so snapshots and pages carry
  it. `Remove` only when a key has no candidates left. The MDK replica keeps tombstone stamps (hidden
  from app reads) and defaults `based_on` to them, so re-creating after an observed delete wins.
- Kept per-key Lamport stamps. The operator notes a per-collection counter or the feed-position basis
  (option b) resembles big_sync core's keyed frontiers; expect these abstractions to collapse into one
  implementation later. Tombstone GC deferred (scope death retracts; permanent tombstones accumulate).

## C9 done (2026-11-04)

- C9a/C9b merged (e8ded252..10cae0f3), C9c ADRs b7ed9112. 309/309 across the clay crates, clippy,
  guardrails lint/api clean. Supersedes the earlier ledger notes on the ADR 017 §9 materializer and
  the §10 causal writer: both are gone; §10 is now "field-level merges are layers above the map".
- Grants: `RegistrationGrant { micro, prefixes }` enforced in `clay_guest_session` (`forbidden`); test
  mint sites pass a full grant until C5 manifest admission narrows it. Nothing mints installation
  scopes yet, so the manifest/runtime two-scope rule lands with C5.
- Tombstones count toward `rows_per_writer` (they are scope state). A micro's bindings share one
  WriterId; equal stamps between them break by scope id.
- Not compiled: native `clay_mdk` tests (known red from the `HostedCollection` removal, fixed in C2b).

## WCMP fork nix lanes (2026-11-04)

- The fork's devshell is broken since 687e486 (wasm-bindgen pinned to 0.2.127 with no
  `wasmBindgenHashes."0.2.127"` entry in flake.nix). Bootstrapping the hashes builds wasm-bindgen-cli
  through Nix and filled `/` (/tmp) on this machine. Deferred; 7604dc8 verified with plain cargo test only.

## C2b done (2026-11-04)

- Row attachments are bound publications or endpoints `(micro, publication, contract)`; durable rows
  store endpoints only; readers mint lazy refs through the session's `Activator`, which
  `Factory::initialize` now receives. Manifest contracts are `ContractIdentity` (namespace, name,
  version); `ContractRef` is gone.
- Manifest rows become installable via `native::declared_rows` (JSON payload, `{"$attachment": n}`
  stands for the attachment tag; attachments become endpoints to the package's own publications).
  The shell installs them into `<micro>/manifest`; `<micro>/runtime` is reserved for runtime writes.
- Shell: `dayframe_composition::CompositionRuntime` (catalog = manifest, eager, locked, requirements,
  factory), `shell.roots` collection, `serve()` runs `drive()` + eager start + ordered shutdown inside
  a `LocalSet`. Lifecycle panel keeps enable/disable/retry. Dropped: eager/enable toggles,
  preference-driven startup, accept/reject controls, the Wasm fuel-trap e2e flow.
- `clay_micro_core` deleted. `dayframe_web` still needs `--config resolver.feature-unification="selected"`
  for the wasm32 lane: `utils_rs` dev-deps pull tokio `full`/`net` into the unified graph.

## C2b reviewed (2026-11-04)

- Verified myself: `x/check-dayframe.ts` 454/454, guardrails lint/api clean.
- b4ad0d34: the manifest owns `ContractIdentity` again (same fields, no dotted strings); `native`
  converts at admission (`admitted_publication`). `clay_guest_session` is back behind `in_process`;
  the wasip3 guest graph no longer contains it. `PresentationSlot` is reactive over its reference.
- Agent questions settled: eager-start failure stays `Faulted` in the panel (not a crash).
  Open for C5: `RootRow`/`SHELL_ROOTS` live in `clay_mdk::native` but are shell vocabulary; move to
  a shell contract crate when C5 shapes enablement. Two `MicroId`s (clay_host, clay_mdk manifest)
  also remain by design (wire vocabulary vs host type).
- Unrun: Playwright `src/dayframe_web/e2e/counter.spec.ts` (updated, never executed) — C4c.
- Nit seen: `SettingsMicro::edit` maps "preferences still loading" to `AccessError::Denied`.

## C4c part 1 (2026-11-04)

- 527a27fe: `CompositionRuntime::new(store, installed) -> Result<_, StoreError>`; desktop opens
  SQLite at `DAYFRAME_STORE_PATH` or `$XDG_DATA_HOME/dayframe/store.sqlite`, web opens IndexedDB
  `"dayframe"`; boot failures render an alert. Test: three boots over one SQLite file prove load +
  no-op reinstall.
- `IndexedDbStore` is `unsafe impl Send + Sync` under `cfg(not(target_feature = "atomics"))`
  (no threads on that target). Alternative was relaxing `clay_host` hook bounds (`Cleanup`,
  `OutwardHook` are `Send`); not taken.
- Verified: check-dayframe 455/455; dayframe_web Playwright passed on b4ad0d34 (before 527a27fe).
- Part 2 (reload e2e in Playwright) moves to C5: needs persisted enablement to have a visible
  durable fact.

## C5 decisions (2026-11-04, user)

- No key-level write authority until collections have a query language (JSON-style selection and
  filtering, one engine in guest and host); the grant will be the query. Prefix grants are removed
  (agent running), not narrowed.
- Admitted packages start disabled; nothing auto-enables an incoming package. Locked shell micros
  are part of the shell and always run.
- Desired state (install, enabled) is a config datum in a document, shaped like big_nodule's
  `nodule.install/<plug>`. Until the node is wired, the mock `DocumentService` source is backed by
  `clay_store`, so the datum survives a reload and the Playwright reload check (C4c part 2) can
  observe it.
- Plan: C5a prefix removal → C5b durable mock document source → C5c install datum + reconcile into
  the supervisor, lifecycle panel writes the datum → C5d reload e2e.

## C5c: persisted enablement (2026-11-04)

- Desired state is a document: `dayframe.installs` (branch `main`), tag `clay/micro-install`,
  key_id = micro id, value `{ "enabled": bool, "package_version": u32 }`. Shape follows
  big_nodule's `NoduleInstall`; `package_version` is recorded, nothing gates on it yet.
- `InstalledMicro.enabled_on_admission` (false in `new`): launchers set it true for the native
  built-ins (Sample on desktop and web); discovered packages (desktop component files, web
  package index) are admitted disabled. Admission runs in `CompositionRuntime::new`: an existing
  datum wins (only `package_version` is refreshed), locked micros get no datum and are always
  enabled. It writes through `MockDocuments::external_change` (host-side, while the node is mocked).
- `clay_host::ConfiguredMicro.enabled`: a disabled micro starts in `Disabled`, and
  `Supervisor::reconcile` skips disabled roots, so eager + disabled is never started.
- `reconcile_installs` (spawned in `serve()`'s LocalSet, aborted on shutdown before the supervisor
  closes): watches the document through the shell instance and applies `enable` / `disable`;
  close errors go to `CompositionRuntime::lifecycle_failures()` for the lifecycle panel.
- The panel's Enable/Disable calls `CompositionRuntime::set_enabled` (writes the datum); Retry
  stays a direct supervisor call. `MockDocuments::get` is the new host-side source read.
- Tests: composition admission / datum-drives-supervisor / reboot-stays-disabled, clay_host
  eager-disabled, desktop tests enable the counter via the datum, web `counter.spec.ts` enables
  first and adds the reload check (not run here).

## C5 done (2026-11-04)

- Persisted enablement is complete: Playwright passes 2/2, including the reload check (a disabled
  counter is still disabled after `page.reload()`).
- `CompositionRuntime::set_enabled` writes the datum at call time and returns the `persisted()`
  acknowledgement future; pre-serve callers drop it.
- Open issue: a `PresentationSlot` that redeemed a disabled micro stays "unavailable" after the
  micro is enabled; nothing re-redeems on availability changes.

## C6 decisions (2026-11-04, user)

- OCI is not Clay's job: big_cartridges delivers and manages manifests. Clay owns `MicroManifest` types
  only. Until big_cartridges lands, a package is `<name>.component.wasm` + `<name>.manifest.json` on disk
  (stand-in loader); the `dayframe.manifest` custom section and `micro_counter/build.rs` go.
- `activate` is async on both native and guest.
- Collection access: declared → configured → (later) granted at runtime.
  - Manifest declares collection requests per local binding: `{binding, contract, access:
    read|write, necessity: required|optional}` (contract, not a concrete collection).
  - Configuration resolves each binding to a concrete collection or leaves it unbound; manifest
    supplies defaults (a contract with exactly one declared collection binds automatically).
    Configuration is the final word, persisted like the C5 install datum.
  - Lifecycle (OSGi DS-style): unresolved required binding → micro not startable, shown as
    "needs configuration"; unresolved optional → starts, `collection(binding)` is `None`; a binding
    change restarts the micro (no hot rebinding yet).
  - Generic micros: a binding may be "chosen at runtime" (Powerbox / Android SAF-style); the grant
    lands in configuration and arrives as the same handle. Declared only; the picker is ADR 031 §11's
    deferred dynamic consent.
- Steps: C6-pkg sidecar manifest (running) → C6a collection requests + resolution + lifecycle +
  ADR 031 §11 → C6b unified async `Context` (native; binding-based collection/registration,
  multi-presentation, publish_call, require) → C6c guest parity (counter: second presentation + a
  call; a guest registration write) → C7.

- C6 done (2026-11-04): C6a/C6b/C6c landed. One `clay_mdk::Context` over in-process and guest backends,
  `export_micro!(activate, manifest)` dual entry, counter with a second presentation and a `count` call,
  guest collection read + registration write proven on Wasmtime and wcmp/Wasmi. Notes: docs/scratch/clay-c6-context.md
  ("C6c notes").

## big_cartridges coordination (2026-11-04, peer session on ../townframe-p3N-2)

- Operator direction relayed by the peer: a plug is the one package format for every kind
  (nodules, micros, Lexicon schemas); plug-level dependencies; one auditable request → grant →
  minted-capability story across kinds, statically answerable incl. transitively through services;
  node configuration is node-local.
- Answered: Clay needs list/get(section + component bytes by digest)/watch from the plug store;
  enablement, bindings and grants stay in Clay's desired state (ADR 031 §12) — share request/grant
  *types* across kinds, not datums. `MicroManifest` becomes the plug's `micro` section verbatim
  (sidecar JSON is its standalone form); willing to move contract/schema ids to NSIDs.
- Open for the operator: micros requiring nodule operations by NSID (grant minted like a nodule's,
  exposed as the same call capability); whether the envelope's id/version replace the manifest's
  `id`/`package_version`. Schema gaps for later: lookup by schema id (not datum tag) and a repo
  that grows on plug import.

## big_cartridges rulings relayed by the peer (2026-11-04; operator rulings via ../townframe-p3N-2 session)

- big_cartridges = registry/catalog only (npm-like: versioned imports, semver, no enablement). Clay and
  nodules each own enablement, bindings, grants, downgrades, readiness in their own datums.
- The registry is a drawer; a plug is a document with Blob datums. Retention by reference: Clay's
  desired-state drawer will name the plug doc and blobs it enabled (once on the real node).
- NSIDs everywhere (AT Protocol): schema id is the NSID; breaking change = new NSID, no version
  inside. Plan: `ContractIdentity {namespace, name, version}` and bare schema strings → NSID.
- Envelope is authoritative for id, version, artifacts: drop `MicroManifest.id`/`package_version`
  when aligning; digests are BLAKE3 (big_blobs), not SHA-256.
- Strict dependencies: referencing another plug's NSID prefix requires declaring that plug as a
  dependency; access requests name the datum NSIDs they touch.
- Schema registry keyed by NSID on top of Cabinet's SchemaRepo, fed by plugs, grows at runtime.
- Pending: micros requiring nodule operations by NSID.
- Clay plan: alignment step after the envelope/section type names arrive (NSID rename, drop id/
  package_version, BLAKE3); not started.
- Lineage/fork discussion (peer draft: publisher-key lineages, node-local substitution, signed
  Automerge branches). Clay follow-ups raised, not started:
  - Grants re-consent: a binding whose request widens (read→write, contract change) under the same
    binding name must become unresolved, not inherit; compare the granted request set.
  - `WriterId` = micro id breaks the total tie-break once collections sync across nodes (equal
    `(lamport, writer)` with different values); should become (replica identity, micro id).
  - Install datum should record lineage `{publisher, plug doc ref, version}` beside `enabled`.
- Identity approved by the operator (via peer): plug id = DID/name; every fork is a new id;
  node-local substitution. Clay rule proposed: micro id = the plug id it is installed *as* (slot);
  the install datum records `filled_by`. Dependencies are by NSID (OSGi Import-Package style,
  resolved at enable time like a lockfile).
- Alignment-step additions found by the dependency inventory:
  - `ContributionDeclaration.collection` is a concrete collection id; it must name a binding in
    `collection_requests` (ADR 030 §14 says contributions target a local binding).
  - Drop `FacetAccess.owner_plug_id` (plug-id reference that breaks under substitution); the
    request names the datum NSID and configuration picks the document.
- ADR numbering: origin/main has 023–026; the peer branch takes 027 (plugs/nodules) and 028 (plug
  identity, naming, dependencies, malleability). Ours renumber after C6c, before rebasing on main:
  023 → 029 (cross-guest capability bank), 024 → 030 (micro lifecycle/manifest), 025 → 031
  (manifest-declared collections); update every cross-reference in docs and source comments.
  The nodule manifest adopts our request shape `{binding, contract, access, necessity}` +
  `minRevision`.
- ADR 028 accepted by the operator (feat/nodules `bea32514`). Clay alignment step "C6d" after C6c
  and the ADR renumbering: NSID rename (contracts, schemas), drop `MicroManifest.id`/
  `package_version` (envelope authoritative; until the envelope exists the sidecar carries them
  outside the manifest), request shape `{binding, contract, access, necessity, resolution,
  minRevision?}` for requirements and collection requests, drop `FacetAccess.owner_plug_id`,
  contributions target a binding, micro id = installed-as plug id, writer identity = (replica,
  micro id), BLAKE3 digests, grants re-checked against the granted request set. Envelope Rust
  type names arrive from the peer before C7.

## Lazy activation responsiveness (2026-11-04, user; deferred)

- Approved now: slots retry when their micro becomes available (or restarts) instead of staying
  "unavailable". Lands after C6c.
- Deferred by user decision ("cut with what we have"): a responsiveness/utilization policy for lazy
  activation. Candidates recorded for later:
  - Intent prewarm: hover/focus/visibility of a lazy endpoint takes a short warm lease so the
    micro is initialized before the click (prefetch-on-hover, like link prefetching).
  - Keep-warm budget: idle close is delayed by a TTL and bounded by an LRU/memory budget across
    incarnations, rather than closing as soon as the last lease drops.
  - Immediate feedback: a lazy action renders a pending state at once and queues the call until
    the incarnation is ready, so nothing feels dead while initializing.
  - Measure first: activation latency per engine (Wasmtime, browser) before choosing budgets.
- big_cartridges registry landed on feat/nodules (peer, 2026-11-04): `big_cartridges_core::CartridgeId
  {namespace, name}` (text `<did>/<name>`, d12adcd2); registry datums `org.example.daybook.plug.
  identity/main`, `org.example.daybook.plug/<semver>` (envelope, immutable per label),
  `org.example.daybook.blob/plug.artifact/<label>/<artifactKey>` (BLAKE3 Blob) (e1bd3853);
  `PlugsProtocol` (`org.example.daybook.plugs.*`): listPlugs, getPlug, getPlugVersion,
  watchPlugs (long-poll cursor), publish/remove, mintPlugsCapability. Pending there: typed
  `micro` section key, publisher signatures (P3a), importers incl. our sidecar pair (P3b).
  C6d uses `CartridgeId` for micro ids once the micro section key is confirmed.
- Micro section confirmed: envelope contribution `org.example.daybook.plug.micro`
  (`big_cartridges_core::MicroContribution {schema, description}`); `schema` = our manifest format id,
  `description` = `MicroManifest` JSON minus `id`/`package_version`. One micro per plug; micro id =
  installed-as `CartridgeId` (lands with P3b, peer will ping).
- P3b landed on feat/nodules: `big_cartridges::import::micro_pair(component_bytes, manifest_json)`
  imports our sidecar pair (id → `<node did>/<id>`, package_version n → `n.0.0`, component →
  artifact `component` in big_blobs, node-signed). Plug names may contain dots. Micro
  contribution schema NSID = `org.example.clay.micro.manifest`; C6d drops `MicroManifest.schema`
  (the NSID is the format identity; breaking change = new NSID).

## C7b review (2026-11-04)

- Accepted: `RootRow.pinned` keeps a root (the toolbar) mounted when Settings is closed. Generic shell flag, no toolbar vocabulary in the shell.
- Fixed in C7b-2: call contracts may be implemented outside the package's authority (ADR 028 §5); the toolbar owns `org.example.dayframe.toolbar.action`. Durable mock operations settle on local persistence. `Capability::retain_call` in the MDK; the completion test proves one incarnation across retained calls. The pushed `components` list on `presentation.mount` and `ComponentSlot` are removed (micros only pull presentations through capabilities).
- Deferred: volatile per-incarnation button state (badge/enabled) needs manifest-declared volatile collections and a scope-owned writer path; icon vocabulary (string carried as `data-icon`, no glyphs); the press path does not consult `status()`, so a disabled provider's button reports its failure on press.
- big_cartridges P7 (feat/nodules `d5b24d44`): schema registry by NSID. The authority holder's datum Lexicons register live in the cabinet SchemaRepo. Vendored copies are compared per NSID: identical copies, or copies differing only in declared `revision`, agree; the authority holder's copy wins; any other mismatch is refused. Clay follow-up when it moves onto PlugsProtocol: micros vendor the Lexicons they use, with a `revision` when they evolve one.

## C7c review (2026-11-04)

- Accepted: the Wasm layout micro (`micro_layout`, `micro_layout_manifest`) is a pinned root, admitted disabled like any discovered package (no auto-enablement of incoming packages). A missing arrangement shows no panels: showing all by default would start every contributor at boot.
- Accepted: the guest executor doorbell (`a49ae1c1`). `wit_bindgen::spawn_local` from a synchronous entry only queues the future; a parked task in `initialize`'s scope is woken by each spawn so the queue is adopted at once. Revisit if wit-bindgen's executor polls the spawn queue itself.
- Deferred: removing configured ids nothing registers; reordering; the slot's "Section unavailable" wording inside a panel.

## Validation matrix (C7d, 2026-11-04)

Evidence is read from test bodies, not names. Backends: **N** native (in-process micro or fake factory), **WT** Wasmtime
guest, **WI** wcmp over Wasmi (the native build of the wcmp core), **BW** browser wcmp (Playwright against the real
`dayframe_web` build). Conformance scenarios live in `src/clay_guest_session/lifecycle_conformance*.rs` and are run by the
same test list in `clay_rt_wasmtime/lifecycle_tests.rs` and `clay_rt_wcmp_wasmi/lifecycle_tests.rs` (21 scenarios: 18 over the MDK `context`
micro, 3 engine-only over the raw-WIT `lifecycle` fixture), so WT and WI always agree. Since C7g every scenario over the `context` micro
also runs natively: `clay_mdk::native::conformance_tests` runs the same micro source (`clay_mdk/context_micro.rs`) in-process against
the same scenarios. The 3 engine-only ones (`guest_trap_*`, `sync_exports_*`, `dropped_call_*`) are WT and WI only. "conf." below
means that list.

### ADR 030 (10 items)

| # | Item | Tests | Backends | Verdict |
|---|------|-------|----------|---------|
| 1 | manifest and collection definitions inspected without instantiating the guest | `dayframe_composition::tests::manifest_rows_are_installed_before_anything_runs` (every micro `Idle`, row with endpoint attachment read, factory never called); `dayframe_desktop::composition::tests::desktop_runtime_installs_the_counter_section_from_its_package_manifest` (real package sidecar, row read, counter `Disabled`); Playwright `counter.spec` ("Open Counter (Wasm) settings" shows before the micro is enabled) | N, WT (package only, no guest created), BW | covered |
| 2 | durable contribution visible while its provider is inactive | `the_toolbar_renders_the_counters_button_while_it_is_stopped_and_a_press_starts_it_once` (button row read, `status()` `Dormant`, 0 activations); desktop `e2e::the_toolbar_button_increments_a_stopped_wasm_counter`; Playwright `the toolbar button increments a stopped wcmp counter` | N, WT, BW | covered |
| 3 | reading a lazy endpoint does not activate; redemption starts or joins exactly one staged activation | `clay_guest_session::capabilities::lazy_tests::reading_a_lazy_status_never_starts_the_provider`, `concurrent_lazy_calls_and_redemptions_from_three_recipients_initialize_once` (added in C7d: two calls plus a presentation redemption while initialization is parked, one init); `clay_host::supervisor::tests::concurrent_acquires_share_one_initialization`, `an_activator_is_served_by_the_driver_and_status_never_activates`; conf. `lazy_references_activate_retain_and_release` (log empty after `status()`, a call activates) | N (real supervisor), WT, WI | covered |
| 4 | a demanded named publication is returned without polling a secondary collection | `supervisor::tests::an_acquire_resolves_only_after_initialization_returns` (lease only after `initialize` returned, staged invisible before), `acquiring_an_unpublished_name_fails_and_leaves_the_incarnation_up`; conf. `leased_provider_closes_when_idle`/`lazy_references_...` (the call is answered by the publication `initialize` returned); the toolbar press needs only the row's endpoint | N, WT, WI | covered |
| 5 | retaining the redeemed resource keeps the provider active; dropping the final strong edge starts orderly shutdown | `lazy_tests::retained_proxies_keep_the_provider_live_until_dropped`, `a_lazy_call_holds_the_provider_only_for_the_call`; conf. `lazy_references_activate_retain_and_release` (retain-call, then a mounted redeemed presentation, each `Active` until dropped, then `Idle` with one close), `context_retains_a_call`; `dayframe_composition::tests::the_editor_calls_only_the_providers_whose_trigger_matches` (focus/blur) | N, WT, WI | covered |
| 6 | `instance-root.close()` runs once with dependencies retained, then scopes and publications are revoked | `supervisor::tests::concurrent_closes_drive_one_root_close`, `shutdown_closes_dependents_while_their_providers_are_live`, `publications_are_dropped_before_the_incarnation_is_torn_down`, `a_closing_micro_is_recorded_before_its_guest_close_returns` (new, with the C7d fix); `clay_guest_session::collections::staging_tests::closing_keeps_rows_through_cleanup_and_teardown_withdraws_them`; conf. `provider_consumer_graph` (the consumer micro keeps its redeemed proxy in a value its incarnation owns, whose `Drop` reads the proxy and crashes the close unless the provider is still live; no close hook; log shows one close each, lease revoked after; verified by flipping the assertion, which fails on native and on Wasmtime), `leased_provider_closes_when_idle` (guest residue `NONE` after close) | N, WT, WI | covered |
| 7 | failed or cancelled initialization exposes no staged publication | `clay_host::tests::failed_initialization_never_reports_active_and_unwinds_owned_effects`, `cancelling_async_activation_withdraws_owned_effects`; `supervisor::tests::disabling_a_pending_initialization_cancels_it_without_exposure`, `a_failed_provider_blocks_its_consumer_and_leaves_nothing_live`; `staging_tests::failed_initialization_exposes_no_rows_or_capabilities`; conf. `failing_provider_publishes_nothing`, `failed_registration_initialization_publishes_nothing`, `undeclared_publications_fail_initialization` | N, WT, WI | covered |
| 8 | disconnected cycles do not remain live; activation cycles fail precisely | Static: `supervisor::tests::requirement_cycles_are_rejected_before_anything_initializes` (`StartError::Cycle`, nothing initialized, quiescent), `clay_host::tests::closing_a_binding_cycle_is_an_invariant_failure`, model test `lifecycle_cascades_match_the_dependency_model`. Dynamic: `supervisor::tests::incarnations_retaining_each_other_close_together_once_no_root_reaches_them` (live while an external lease reaches either, one close each after), `a_three_member_cycle_closes_as_a_group_and_a_member_may_find_its_peer_revoked` (a member's close sees `b` Closed and does not fault), `an_activation_cycle_fails_the_closing_demand_and_leaves_nothing_pending` (A to B to A, nothing stuck, later demand of B works), `demanding_itself_while_initializing_fails_with_a_cycle`, `a_provider_demanding_its_starting_consumer_fails_the_demand` (configured provider demands its starting consumer); capability layer `lazy_tests::incarnations_retaining_each_others_calls_close_once_no_outside_lease_reaches_them` (real sessions, `retain_call`); guest level (C7f, two supervised `context` micros, each incarnation's own activator): conf. `context_peers_retaining_each_other_close_together` (an outside lease on either keeps both up, `a` and `b` each initialize once and close once after the last lease drops) and `context_peers_demanding_each_other_fail_as_unavailable` (each `activate` calls the other's `echo`; both end `Faulted` "unavailable", nothing live or pending) | N (supervisor, capability layer, and the two scenarios on the in-process engine), WT, WI | covered |
| 9 | faulted micros do not retry from demand churn | `supervisor::tests::demand_does_not_retry_a_faulted_micro`, `a_failed_demand_that_started_the_micro_lets_it_idle_close`; `dayframe_composition::tests::an_unfetchable_guest_faults_alone_until_retried`, `a_slot_does_not_redeem_a_faulted_micro_again_until_it_is_retried` (slot churn leaves the attempt count at 1 until `retry`); `clay_mdk::wasm::tests::guest_activation_failure_faults_the_micro_and_publishes_nothing` | N for the retry policy (engine-independent supervisor code); WT for the guest fault | covered |
| 10 | native, Wasmtime and WCMP micros exercise the same lifecycle contract | The 21 conformance scenarios: 18 over the `context` micro (graph with close order, lazy refs, failing/undeclared publication, registration scope ownership and failed registration initialization, `leased_provider_closes_when_idle`, 5 call-concurrency ones: overlapping, many, re-entry, disable and shutdown with calls in flight; and the 6 `context_*`: collections and calls, follows a provider, retains a call, sees a closed provider revoked, two retaining peers, two demanding peers) on N, WT and WI from one micro source; 3 engine-only over the raw-WIT fixture (trap, sync exports, dropped call) on WT and WI; BW by `counter.spec` (activate, mount, disable, remount, reload) and `layout.spec` | N, WT, WI: the 18 `context` scenarios; WT, WI: the 3 engine-only; BW subset | **covered**, with the adaptations in the #10 gap detail (C7g) |

Gap detail.

- **#8 dynamic half (built, C7e; guest level C7f).** A `Lease` now records its holder: an external root, or the incarnation whose
  factory was handed the activator it was acquired through (`Supervisor::activator()` is the external handle; the factory's
  is minted per incarnation, keyed by scope id). Idle close is reachability from roots (eager micros, external leases,
  incarnations that are initializing or closing), searched backwards from the provider whose edge dropped; an
  unreachable strongly connected group closes together. An activation demand from an initializing incarnation records a
  wait edge, and a demand that would wait on its own staged outcome fails with `StartError::Cycle` (seen by the guest as
  `Unavailable`). `Host::bind`'s strong-edge panic is unreachable from guest behaviour: only the supervisor binds, over
  configured requirements, and leases are supervisor edges, not host bindings. The guest-level scenarios (C7f) run two
  supervised `context` micros (`PeerRig` in `lifecycle_conformance/context.rs`) on native, Wasmtime and Wasmi.
- **#10 native (C7f, closed in C7g).** The `context` micro (`clay_mdk/context_micro.rs`) is written against `clay_mdk::Context` and is one
  source file: the `context` example builds it as a guest component, and `clay_mdk`'s tests link it in-process. `Incarnations` is the
  engine-neutral harness (initialize from an `InitialContext`, publications, inspect, `calls_in_flight`, close); `GuestIncarnations` adds
  `mount`, which only the raw-WIT `lifecycle` fixture's `sync_exports_*` needs. The in-process engine (`InProcess` in
  `clay_mdk/native/conformance_tests.rs`) builds the micro's `Granted` from the scenario's grants and runs `Granted::activate`, the
  function the supervisor's factory also calls, admitting what the micro staged against the scenario's declarations (a guest engine admits
  against the package's sidecar the same way). C7g ported the other 12 portable scenarios with **no close hook** (operator ruling,
  2026-11-04): roles come from what the scenario grants (requirements `presentation`/`service`/`peer`, `feed` rows `reject` and `name`),
  raw-byte calls became typed CBOR-text calls (the same wire, so the raw fixture kept it), and "a consumer whose close redeems its proxy"
  is a value the incarnation owns whose `Drop` reads the proxy it holds. Adaptations, all deliberate: (1) `registration_scope_ownership`
  keeps staging, one-transaction publication, per-scope retraction and survivors, but not the explicit `WriteBasis` sequence
  (compare-and-set, observed supersession, tombstone): the authoring `Registration` derives every basis from its mirror, and that
  sequence is the registration writer's own, covered by `collection_probe_registration_writers_*` (WT, WI) and `clay_collection`'s
  semantics tests; (2) `lazy_references_*` no longer mounts the redeemed presentation (a renderer's job, covered by `sync_exports_*` on
  the guest engines and by `clay_mdk`'s wasm slot tests); (3) the old "the consumer publishes nothing" assertion is gone, since the
  consumer is the same micro and publishes everything; (4) `failing_provider_*` asserts the micro's reason, not an engine's exact wording.
  BW runs only the UI-level flows above (the shared wcmp core is what WI runs).

### ADR 031 (11 items)

| # | Item | Tests | Backends | Verdict |
|---|------|-------|----------|---------|
| 1 | installing a manifest makes an untouched collection bootstrap known-empty without starting its micro | `clay_collection::registration_tests_durable::untouched_durable_collection_bootstraps_known_empty` (primitive); `dayframe_composition::tests::an_untouched_declared_collection_reads_known_empty_without_starting_its_micro` (added in C7d: mirror bootstrapped, 0 rows, editor `Idle`, no incarnation) | N | covered (the composition runs the same collection code for guests) |
| 2 | volatile and durable collections cross the same projection WIT | conf. `context_collections_and_calls` now runs over a volatile `bytes_registrations` and a store-backed `durable_bytes_registrations` (C7d); desktop `e2e::*` and Playwright read the durable `settings.sections` through guests; `clay_rt_*::collection_probe_*` read volatile ones | WT, WI (both kinds); BW (durable) | covered |
| 3 | writer-handle drop preserves rows; incarnation closure removes its scope atomically | `clay_collection::registration_tests_semantics::writer_drop_preserves_candidates_and_owner_close_invalidates_every_writer`, `owner_drop_closes_scope_even_while_writer_survives`; `clay_guest_session::collections::registration_tests::instance_close_withdraws_its_writers_rows_in_one_transaction_and_wakes_readers`; `clay_rt_wasmtime::tests::collection_probe_registration_writers_are_owner_scoped_compare_and_set_and_withdrawn` (+ WI twin); conf. `registration_scope_ownership`, `context_collections_and_calls` (only the seed outlives the guest) | N, WT, WI | covered |
| 4 | durable rows survive restart and inactive providers; uninstall removes only the owning scope | `registration_tests_durable::sqlite_store_reload_reproduces_winners`, `uninstall_removes_only_its_scope_and_unknown_scopes_are_a_no_op`, `permanent_scopes_survive_everything_that_retracts_others`; `dayframe_composition::tests::a_reboot_over_the_same_sqlite_file_keeps_manifest_row_stamps`, `a_disabled_micro_stays_disabled_across_a_reboot`; Playwright `a disabled counter stays disabled across a reload` | N (primitive for uninstall), BW (IndexedDB reload) | covered at the primitive; the composition has no uninstall flow (ADR 031 defers uninstall policy) |
| 5 | schema-invalid selectors, keys, values and attachments are rejected before state changes | Enforced and tested: key/value/row limits (`registration_tests::key_value_and_row_limits_refuse_before_the_collection_changes`), selector/cursor ceilings (`collections::tests::acquire_enforces_selector_and_resume_cursor_ceilings`), fabricated/invalid attachment indices (`capability_tests::fabricated_indices_and_copied_cbor_convey_no_authority`, `clay_rt_*::registration_writer_surfaces_invalid_attachment_for_fabricated_capability_index`), manifest/authority validation (`manifest_tests::validate_rejects_incoherent_manifests`, `a_package_declares_only_nsids_under_its_authority`, composition `a_declaration_outside_the_packages_authority_is_an_installation_error`) | N, WT, WI | **partial; schema validation deferred** |
| 6 | reading a lazy reference, including `status()`, does not activate | `lazy_tests::reading_a_lazy_status_never_starts_the_provider`; conf. `lazy_references_activate_retain_and_release` (`status()` `Dormant` on a reference to the guest provider, log empty after eight yields); toolbar test (`Dormant`, 0 activations) | N, WT, WI | covered |
| 7 | concurrent calls and redemptions perform one staged activation and receive the named publication | `lazy_tests::concurrent_lazy_calls_and_redemptions_from_three_recipients_initialize_once` (C7d, extends the two-call test with a redemption); `supervisor::tests::concurrent_acquires_share_one_initialization` | N (the logic is the engine-independent `Activator`) | covered |
| 8 | retained presentation/call binding keeps the provider live; in-flight call keeps it live to completion; dropping the final binding shuts it down | `lazy_tests::retained_proxies_keep_the_provider_live_until_dropped`, `a_lazy_call_holds_the_provider_only_for_the_call` (stays `Active` mid-call, `Idle` after); conf. `lazy_references_activate_retain_and_release`, `context_retains_a_call`, `calls_reenter_their_provider` | N, WT, WI | covered |
| 9 | a bound reference whose incarnation closed reports `revoked` and never activates a new incarnation | `lazy_tests::a_bound_ref_is_revoked_after_its_incarnation_closes_and_never_reactivates` (status `Revoked`, call and `retain_call` fail, 1 init); conf. `provider_consumer_graph`, `leased_provider_closes_when_idle` (bound reference redeems `Revoked` after close); guest side (C7f): conf. `context_sees_a_closed_provider_revoked` (the guest's `probe` call reads a row's bound attachment: `Live` and a working call and `retain_call` while the provider runs, then after the provider idle-closed `status()` is `Revoked` through the WIT and both calls fail `Revoked`; the provider stays idle, one activation) | N (host-side reference and the in-process guest), WT, WI | covered |
| 10 | toolbar contribution renders from durable metadata, activates on press or mount, no secondary coordination collection | `the_toolbar_renders_the_counters_button_while_it_is_stopped_and_a_press_starts_it_once`; desktop `e2e::the_toolbar_button_increments_a_stopped_wasm_counter`; Playwright toolbar test; mount: desktop `e2e::layout::a_hidden_panel_never_activates_its_provider_until_it_is_shown`, Playwright `layout.spec` | N, WT, BW | covered |
| 11 | native, Wasmtime and WCMP obey the same ownership and lifecycle contract | as ADR 030 #10 | N, WT, WI: the 18 `context` scenarios; WT, WI: the 3 engine-only; BW subset | **covered**: as ADR 030 #10 |

### Found while writing the matrix

- **Fixed (`761cb9ca`).** `Supervisor::close` set `Closing` without refreshing the mirror or ticking `changes`, so
  `Activator::status` stayed `Active`/live while a guest `close` ran and `changes` waiters could not observe a close begin.
  Found because replacing a spin with a `changes` wait hung on exactly that state; pinned by
  `a_closing_micro_is_recorded_before_its_guest_close_returns` (hangs without the fix).
- **Test hygiene (`bf3e034d`).** Polling waits replaced by event waits (see "C7d notes" in `clay-c6-context.md` for the
  list of loops deliberately kept).

## PR #79 CI state (2026-11-04, before a harness restart)

- PR: https://github.com/dman-os/townframe/pull/79 (opened as the `dman-os` gh account: `GH_TOKEN=$(gh auth token --user dman-os)`; push with `GIT_ASKPASS=/home/asdf/dman-creds`).
- First CI run failures and fixes (committed, NOT yet pushed or verified locally):
  - check-matrix: `clay_mdk` guest-only build, irrefutable `let MountInner::Guest` in `hooks.rs` → `7f472994`.
  - counter guest linked `in_process` via `micro_{settings,toolbar}_manifest` → in_process moved to their dev-deps, `293069ba`.
  - `x/check-dayframe.ts` ran `cargo tree --offline` before fetching the new wcmp rev / jiff-tzdb → `cargo fetch` first, `153402ee`.
  - Nix CLI package: crane vendoring of the wasmtime fork failed on `crates/c-api/artifact` missing README → fork commit `783ad10` on `dman-os/wasmtime` branch `drive-until-stalled-48` (pushed), rev bumped in `f3756c5f`.
  - Lint job died without logs (cache throttling); rerun.
- To do after restart: run `cargo x guardrails check-matrix --package <p> --target <t>` for clay_mdk, micro_* manifests, micro_counter, micro_layout on native / wasm32-unknown-unknown / wasm32-wasip3; then the full check; push; watch CI.
- Dev-script agent (D1, `x/dayframe.ts`) may still have uncommitted work; check `git status`.

## Guest view wakes are opt-in: design debt (2026-11-19, operator: "put it in the ledger")

- **Symptom.** A view reading a `call_bytes` body never completed, at any size (intxt #213, pin
  `clay_node` `e2e::egress::a_view_reads_a_multi_chunk_byte_body_in_order`). Fixed on `fix/view-call-bytes-hang`
  (`34ada7f0`, clay_mdk/context.rs; `85427e66` is droppable INSTR).
- **Mechanism.** A guest has two schedulers. A view's Dioxus `use_resource` task is polled only inside
  `render_immediate`, which the host drives through the sync exports. Network work runs on the guest executor
  (component-model tasks). A delivery from the executor wakes the Dioxus task, but a Dioxus wake schedules nothing:
  no render follows unless the delivering code calls `views.notify_views()` itself. Unary `call` does that once at the
  end, which suffices. `call_bytes` copied it, notifying once after the head, so every later chunk and the
  end-of-stream woke a task nobody polled. The fix notifies after the head, after every chunk and at end-of-stream.
- **Why it's design debt.** Liveness depends on every cross-executor delivery path remembering a manual
  `notify_views()` per delivery; a forgotten one is a silent hang. `Cx::blobs()` and `fetch` will hit it next.
- **Direction (open).** Put the render request in the waker: a wake of a view task asks the host for a render pass,
  as Dioxus's own runtime does via `wait_for_work`, and `notify_views()` goes away. Open: whether the host↔guest
  wire can carry a "render requested" signal today (the guest is driven by sync exports). Coordinate with intxt,
  who builds on the guest view API. Candidate for the design session or a small draft PR.

## Web components: embrace fully, soon (2026-11-19, operator)

- **Decision.** We end up fully on web components (custom elements, shadow DOM, `part`/`::part`, slots), with our own
  enhancements on top, "sooner than later". The micro embed contract stops being merely web-component-*isomorphic*.
- **Today.** Wasm guest mounts are `div[data-dioxus-shadow]`: the Dioxus fork attaches a shadow root and adopts the
  layered sheet (`@layer host` kit → micro sheet → `@layer user` theme). Built-in (natively linked) micros render into
  the host document with `document::Style` (CSP failure in the web e2e; no isolation). No custom elements anywhere;
  kit components are light DOM inside a root.
- **Step 1 (in flight, lane S, branch `fix/builtin-micros-shadow-mount` → feat/daytxt):** built-ins through the same
  shadow mount; the wrapper's inline `display: contents` becomes a host-layer rule.
- **Next (design session):** mounts as registered `<ct-…>` custom elements (attributes/properties as the props
  contract, lifecycle, slots for nested surfaces) so Solid/React/plain HTML can host micros; `data-part` → real `part`
  attributes, and how `::part` + custom properties relate to our injected `@layer user` theming (keep it as the
  enhancement, or narrow it); kit components as web components or light DOM inside roots. Blitz has neither shadow
  roots nor custom elements: it stays a fallback and never shapes the contract.
