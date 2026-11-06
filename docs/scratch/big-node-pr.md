# feat/big-node: the node PR description

Status: Draft
Owner: Townframe maintainers
Updated: 2026-11-06
Scope: Draft description for the `feat/big-node` -> `main` PR. Numbers come from `docs/scratch/nodule-runtime-ledger.md`; `TODO(verify)` marks anything the ledger or log does not settle.

## Summary

A node is now a value you boot, not a test helper. `big_node` boots the standard services against storage it is handed, serves one router, and optionally attaches listeners and a peers endpoint. Design: [ADR 032](../adrs/032-what-a-node-is.md), with its 2026-11-06 revision.

- **`big_dispatch` and `big_dispatch_core`** own the router, admission and providers. Authority, docs, drawers, blobs, plugs and nodules register as `builtin:<service>` providers under their existing NSIDs; nodules register as before. `big_nodule` depends on `big_dispatch`.
- **Credential header.** The caller's credential is `Authorization: Bearer <authority>.<signer>` in envelope metadata. Resource capabilities stay in the body (N2b).
- **Listeners.** UDS (operator admitted by peer credentials, `0700` socket directory) and TCP, both serving the router, plus a `/xrpc/<nsid>` fallback in front of dispatch. Remote clients use HTTP/WS over iroh by default, so there is no TLS work on TCP (D31).
- **Storage bindings.** Services are handed opened storage and never open paths: `big_repo::Storage`, `big_blobs::Storage`, an `Arc<SecretStore>` per node. A test boots 20 nodes in one process, stops them, reboots the durable ones and asserts nothing leaks (`e2e::many_nodes`).
- **Peers.** `big_node::peers` owns the Iroh endpoint (one node key for keyhive and Iroh), registers peers, sets partitions, dials with backoff and takes an embedder-supplied peer policy. The default policy is `Drawers::mirrored` bounded by `big_repo::groups_with_member`. Keyhive access changes wake it directly.
- **Test arena.** An N-node harness (`e2e/harness.rs` `Cluster`), a ladder suite, a seeded randomized stress test, a cross-node nodule test, and a costs harness with a counting allocator and a note-cost growth pin.
- **`day` CLI.** The node's first client, in-process or over the daemon socket. Node-scoped keyring secrets and socket (`$XDG_RUNTIME_DIR/day/<node-id>.sock`), idempotent `day init`, bootstrap peers in `node.json`, and the built-in `getNodeInfo` query.
- **Datum cleanup (D32).** Datums drop Rust number widths and shapes that Lexicon and Automerge lack.
- **Docs.** ADR 032 revised; ADR 022 explained from the caller's side; ADR 025 section 7 and ADR 020 amended; scratch docs for cartridge naming (D34) and Windows.

## Major fixes

### Performance

- **Admission wakeups** (`d4d33097`): keyhive admission-log readers wake on append instead of polling every 250 ms.
- **Change bytes, not snapshots** (`820aa83b`): a commit stores its own change bytes instead of a full `doc.save()`.
- **Descendants-first cold walk** (`3f0476ec`): a cold snapshot load walks descendants first, so one causal walk covers a linear history.
- **No envelope re-parse** (`148c3e96`): causal decrypt takes keyhive's already-unwrapped ancestor plaintexts as they are.
- **Single-wait placement** (`c90a2cdf`, `3571b59c`): placing a document issues all three drawers' grants, waits for keyhive once, then publishes the roster entries concurrently.
- **`PollDatums` row stream** (`6d7c60f6`, ADR 025 section 7): rows, move-ins and move-outs replace `enter` and the per-request depth map.
- **Partial dmeta reads** (`87c25501`, `19c1dd22`): a write validates and reconciles only the dmeta parts it names (`Dmeta::write_atomic`, `Dmeta::hydrate_for`), and the gate drops its full-history walk (ADR 020 amended).
- **Blob roster writes** (`6bd49930`): a roster write re-reconciles only the member it names.
- **Reader reconciliation via the membership index** (`81066896`): reader reconcile walks a worker-owned reverse-membership index up from the admitted delegation's subject instead of every doc in the hive.
- **Duplicate keyhive ingest** (subduction `8dc7da49`, pinned in `dbc84be2`): `ingest_events` drops events keyhive already incorporated.
- **Causal checkpoint resolver election** (`0c831218`): when the latest epoch holds updates from two or more issuers, only the lowest MemberId mints a resolving PCS update. Before, every node minted its own and re-conflicted every tree.
- **Refresh loop removed** (`84c60284`): the 2 s keyhive cache refresh loop is gone.

### Correctness

- **Blob member wake** (`92e946ab`): a member doc named by the roster before its content synced in was parked and never read again. A node-wide Automerge-frontier reader now re-dirties parked rows.
- **Prekey index** (keyhive `4661560f`): `pseudorandom_in_range` could pick an index equal to the prekey count, so a non-empty set reported `NoPublishedPrekey`. It now samples strictly below `max`.
- **Tier3 converge helper** (`c9c595b9`): the four tier3 topology tests sync every link until a full round changes nothing before asserting, replacing a harness race.
- **Sync replay reset** (`7e2c2963`): a reset re-opens under a fresh subscription id, which fixed a stall where a document created while a peer was offline was never pulled.
- **Other:** a document's holder seats readers admitted through groups (`926f6d50`); child delegations are read at the member's direct access (`d1ad6603`); blob retention pins only into inventories the node can write (`95990b6b`); the blobs permission cursor is scoped by repo (`3f2fc461`).

## Measured before/after

All figures are from the ledger. Conditions are stated per line; they are not comparable across lines.

| Change | Condition | Before | After |
|---|---|---|---|
| Admission wakeups | note create p50 | 767 ms | 276 ms |
| Admission wakeups | keyhive reconcile wait | 224 ms | 38 ms |
| Blob roster rearm | allocations per note, 30 notes | 460k -> 1M | 100k -> 125k |
| Placement and poll fixes | aarch64 release, 200 notes, note create p50 | 226 ms | 113 ms |
| Placement and poll fixes | first tenth -> last tenth of create | 131 -> 316 ms | 100 -> 122 ms |
| Single-wait placement | release, 50 notes, quiet box, create p50 | 149 ms | 120 ms |
| Single-wait placement | allocations per note | 135,648 | 120,295 |
| Partial dmeta reads | allocations at drawer size 10 vs 30 (debug, pinned ratio) | 234,389 vs 463,475 | 183,219 vs 203,380 (1.11x) |
| Change bytes | drawer stored:saved | 205:1 | 8.4:1 |
| Change bytes | node dir per note | 147 KB | 33 KB |
| Descendants-first walk | reboot at 100 notes | 4.12 s | 1.86 s |
| Descendants-first walk | drawer snapshot decrypts | 5,564 | 107 |
| No envelope re-parse and walk (cost table at `6ddfdef`, aarch64 release, 200 notes) | reboot | 7.0 s / ~22 GiB | 1.58 s / 473 MiB |
| Same table | connect + replicate | 21.2 s / ~51 GiB | 22.9 s / ~10 GiB (wall unchanged) |
| Reader reconcile index | debug, 50 notes, summed lookup | 38 s | 0.5 s |
| Duplicate keyhive ingest | debug, 50 notes, connect | 7.35 s | 5.66 s |
| Causal checkpoint election | persists per seed 2 run | 783-1383 | 40-71 |
| Whole branch (cost table at `94fbb0ac`, aarch64 release, 200 notes) | create note p50 first / last tenth | 131 / 316 ms | 105 / 115 ms |
| Same table | reboot | 7.0 s / ~22 GiB | 1.61 s / 495 MiB allocated |
| Same table | connect two nodes, then replicate 200 notes | 21.2 s | 16.2 s, then 0.65 s |

Notes on what is not fixed:
- Connect is still the outlier (16.2 s at 200 notes, aarch64 release, at `94fbb0ac`). It is mostly keyhive membership ingest, not attributed further.
- Create note first -> last tenth was 142 -> 312 ms in the `6ddfdef` table, and 100 -> 122 ms in the later `3d04999` table (after the dmeta fix). At `94fbb0ac`: 105 -> 115 ms.
- Debug-build numbers (x86 laptop, 10 notes, about 1.4 s per create at the first measurement) are not release numbers.

## Fork pins

From `Cargo.toml` at the branch head:
- keyhive (`keyhive_crypto`, `keyhive_core`, `beekem`): `dman-os/keyhive` rev `4661560f517525a0b03577259941c5b6824c25a8` (`townframe-changes`).
- subduction (`subduction_*`, `sedimentree_core`): `dman-os/subduction` rev `a9c8697cd5407549e6b08be0b5d6e469faf2b063` (`townframe-changes`).
- `wash-runtime`: `dman-os/wasmCloud` rev `e7648c6e939c50ba1be8d9c138ef6da3305a672e`.

No `file://` patch remains. Both revs are the tips of the `townframe-changes` bookmarks on the forks' origins.

## Breaking interface changes

- **Dispatch moves** from `big_nodule` to `big_dispatch` / `big_dispatch_core`; `big_nodule` depends on it.
- **Credential:** `BearerToken` is removed. `Authorization` travels in `Envelope.metadata.authorization`; every service error carries `ContextError::Unauthenticated`. Body `capability: CapId` stays for resources.
- **Storage:** `big_repo::Config.storage` is `big_repo::Storage { sql, keyhive }`; `BigRepo::boot` no longer takes a `SqliteRuntime`. `big_blobs::Storage` is an opened store. `big_node::Config.secrets` is an `Arc<SecretStore>`.
- **Keyring:** secrets are named by node id (`wrapping.<node-id>`). A node directory created before this branch refuses to boot (`StateCorrupt`); nothing migrates (no release shipped).
- **Walker namespaces:** the blobs permissions cursor is `{scope_key}:big-blobs-permissions-v1`, and the keyhive admission consumers' state is scoped by repo. An existing node resumes the blobs cursor from the log's archived floor.
- **`PollDatums`** delivers rows, move-ins and move-outs; `enter` and the per-request depth map are gone (ADR 025 section 7).
- **Datums:** integer widths, tuples, `char`, 128-bit, pointer-sized and `()` leaves are gone; `Float64` is `F64`; a signed Rust field bounded to `minimum >= 0` is refused at declaration.
- **Gate:** `Gate::footprints` is removed and ADR 020 condition 2 is dropped from the default path (ADR 020 amended). Accepted cost: an unsigned erasure of a value together with all its signatures reads as Absent.
- **Naming:** cabinet `LocalDrawers.registry_id` is `known_drawers_id`.
- **Commits** store change bytes, not snapshots. Existing stores keep old snapshot blobs until a fragment covers them; nothing repacks.
- **Sync:** a replay subscription reset uses a fresh subscription id.

## Known open items and follow-ups

- Windows transport, admission and builds: `docs/scratch/windows-todo.md`.
- GET/query operations through the `/xrpc` fallback; dispatch Lexicon validation of input and output (D31).
- Narrowing tokens over the wire (`mintCapability` is not routed) and client-side token storage (D22).
- Orphan dispatch authority after a crash is never retracted (slice 2c follow-up).
- Causal checkpoint resolver liveness: if the lowest issuer is offline, lacks write access or lacks the doc, the others defer until it returns (a stall, not a loop).
- big_repo wrong turns flagged but not fixed (D30; big_repo is a rewrite candidate): one SQLite transaction per change in `persist_local_commits`; `load_doc_snapshot` rebuilds the whole doc on each call; `register_bundle_lease` swallows a send error; the frontier worker's keyed `replace` cancels in-flight publishes; `seen_head_hashes` grows without bound.
- big_cabinet: ~340 `resolveActor` and ~40 capability mint/retract per note, ~800 write and ~2,600 read SQLite leases per note (as measured before the later fixes; TODO(verify) current counts); `PollDatums` pages re-deliver the polled doc, not a delta.
- A member admitted by a peer's drawer cannot `ImportPlugDocument` from it (the registry actor needs a drawer grant, and `GrantDrawer` takes Admin). D24 plans install with the caller's capability.
- Flakes pinned, not fixed: `big_blobs sync::transfer_tests::completion_releases_capacity_for_the_ninth_eligible_transfer` under `-j 6` load; `big_repo test2::stress::tests::long_test_big_repo_tier10_stress_*` hit the 240 s timeout on a loaded box; the `big_node` `blob_discovery` e2e tests take 95-105 s alone and exceed the default nextest kill in parallel.
- Four heavy `big_cabinet` drawer e2e tests hit the 120 s nextest kill under load; compare timings against an origin/main baseline (our branch adds a fourth boot drawer).
- `listeners`, `nodule_wasm` and `dispatch` tests need `test_nodule_guest.wasm` built.
- main-side: `src/big_node/e2e/blob_discovery/mod.rs` is not rustfmt-clean on main; two ADRs are numbered 007; unused workspace dependencies flagged by cargo.
- Not part of this PR: the "plug" -> "cartridge" rename (D34) and the `Operation` -> `Method` crate (D35) wait for the datum_core PR.

## How to review

Commit groups, in order (`git log --oneline main..feat/big-node`, about 117 commits):
1. **Node design and dispatch:** `85b3f287`, `5e34392c`, `66321166` (ADR 032), `38ca61dd` (big_dispatch), `679af51b` (listeners), `c70ac20f` (credential header).
2. **Bindings and peers:** `8ef60a2b` (storage bindings), `b85e19df` (peers), `7e2c2963` (follow keyhive access, replay reset fix).
3. **Test arena:** `b0f58bec` (harness and ladder), `3f2fc461` (stress, scale, cross-node plug), `f5971214` (cost table).
4. **Performance and correctness fixes:** the `perf(...)`, `fix(...)` and `refactor(...)` commits listed under Major fixes, plus `f34241c9` (operator superuser, plug install on the caller's handles).
5. **`day` CLI and datums:** `39888307`, `eb217b24`, `f7f1f89c`, `9e6ec666`.
6. **Dependency pins:** `dbc84be2`, `c087fd50`.
7. **Docs:** `docs(...)` commits and scratch docs; the ledger is a working log, not a normative record.

`instr(...)` commits are instrumentation (traces, samplers, counters). They are kept in their own commits so they can be dropped independently of the fixes and pin bumps.

TODO(verify): whether the `docs(agents)` commits (`cd0166e0`, `16f920e5`, `e240cba7`, `1abb2f04`) belong in this PR.

## Verification

From the ledger; no run was repeated while drafting this description.
- Pinned tests that fail without their fix: `a_note_costs_no_more_to_create_in_a_fuller_drawer`, `cold_load_of_a_linear_history_takes_one_causal_walk`, `a_member_parked_before_its_content_arrives_is_read_again_when_it_materializes`, `tier6_concurrent_pcs_updates_resolve_without_a_mint_loop` (5/5 failures without the election), `concurrent_connect_never_ingests_an_event_twice`, `a_grant_after_connecting_replicates_without_waiting_for_a_sweep`, `pseudorandom_in_range_is_strictly_below_max_for_every_length` (keyhive).
- Tier3 topology stress: 150/150 at `-j 16` after the converge helper.
- Stress (`e2e::stress::long_test_seeded_partitions_restarts_and_grants_converge`, 4 nodes, 40 steps):
  - Seeds 1, 2, 3, 4, 5 and 7 converged in 78-108 s at N6.
  - Seed 6 failed on the prekey bug and passed 3/3 after the keyhive fix.
  - Seeds 2, 3, 4, 5 and 7 passed at `12c8127` on the aarch64 box; seed 1 timed out there, before the checkpoint fix.
  - Seed 2 timed out in 1 of 3 runs under load at `c087fd50`; the election (`0c831218`) cut persists to 40-71 per run, and a run still takes about 190 s of the 240 s budget under 3x load.
  - At `0c831218` on an unloaded box: seed 2 passed 2/2 (68 s, 65 s), seed 1 passed (51 s), big_repo 442/442.
  - At `94fbb0ac` on the aarch64 box, unloaded: the default seed and seeds 1-7 all passed, 45-63 s each.
  - Under ~15-18 load on the x86 laptop, seed 2 passed 2/2 at 123 s and 128 s after the root-issuer election (`a4aec85e`).
- Ladder suite: 14/14 at `2d79a768` (11 of 13 at N5; both stalls fixed since).
- At `2d79a768`: `cargo clippy --workspace --all-targets --all-features -- --deny warnings` clean; `cargo x guardrails lint` 0 diagnostics; `guardrails api --all --check` and `doctor` pass; big_repo 446/446.
- Load-only: four big_node blob_discovery tests can hit nextest's 120 s timeout at `--test-threads 12` under ~13 load. Traced as CPU starvation (same work, 4-10x slower, no wait over 1 s), not a stall.
- TODO(verify): `nix flake check` and `./x/check-dayb.ts` (operator).
