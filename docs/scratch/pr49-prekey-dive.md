# Prekey-dive: `individual … has published no prekey to select from` at the reserved-doc site

Diagnosis only. No fix landed; instrumentation is marked and droppable.

## Verdict

**Class (a): the same class the pin already fixed, reached through a case the fix misses.** With a
named alternative that the evidence cannot yet exclude: **(c) selection reads a copy of the
individual that later ops never update** (the hive registry's copy is not the copy
`pick_prekey` is handed). (b) — "fixed by a newer upstream PR, needs a pin bump" — is possible only
after a pin-lifecycle evaluation of `jtfm/events-for-agents`; nothing in the pin is upstream.

## What is proven

1. `pick_prekey` fails **iff the individual's live prekey set is empty**
   (`keyhive_core/src/principal/individual.rs:119-136`, pinned rev `1063446`, identical in
   `../keyhive`):

   ```rust
   let prekeys_len = self.prekeys.len();
   if prekeys_len == 0 { return Err(MissingPrekeys::NoPublishedPrekey(Box::new(self.id))); }
   ```

   `prekeys` is a **materialized field** (`Individual { id, prekeys: HashSet<ShareKey>, prekey_state }`),
   rebuilt by `receive_prekey_op` and by `Merge`, and **serialized** in `Archive.individuals`
   (`archive.rs:24`) and in every delegation payload that names the individual as its `delegate`
   (`Agent::Individual(IndividualId, Arc<Mutex<Individual>>)`, `agent.rs:40-50`).

2. `PrekeyState::build()` **provably cannot empty a non-empty op log**
   (`individual/state.rs:121-150`): it inserts every `Add`'s key and every `Rotate`'s `new`, then
   applies tombstones with an explicit guard ("the published set must never become empty"). With
   ≥1 op the result is non-empty. `CaMap` is a plain `HashMap` (`util/content_addressed_map.rs:14`),
   so `prekey_ops().len()` and the map `build()` iterates are the same set — there is no
   "parked vs applied" split that could explain the two numbers apart.

   **Therefore the instance that failed held an empty op log** (or a stale `prekeys` field), and the
   diagnostic's `held prekey ops=22` is a **different instance of the same individual**. The
   divergence is the finding: the hive registry's copy (22 ops, live ≥ 1) is not the copy the
   selection used.

3. **Which copy the selection uses**: `Peer::Individual(_, i) => i.lock().await.pick_prekey(doc_id)?`
   (`principal/peer.rs:69`). A group or document peer recurses into its members
   (`group.rs:285-307`), and group members are **the `Agent` embedded in the delegation op** that
   added them — `dlg.payload.delegate.dupe()` (`group.rs:322-334`), not a fresh registry lookup.
   Our `finalize_reserved_doc` builds each coparent as
   `get_agent(identifier)` → `BigKeyhiveAuthority::Agent(agent).into_peer()` (`keyhive.rs:919-931`),
   and `get_agent` returns the registry Arc for individuals but the **group/document** itself for
   group/document ids (`keyhive.rs:get_agent`), whose walk then uses the delegation-embedded copies.
   `explain_missing_prekeys` only ever inspects the registry copy
   (`keyhive.rs:798-804`) — which is why the log can say 22 while the failure is about an empty one.

4. **The class**: prekey ops are advertised to peers after `KeyOp::topsort`
   (`reachable_prekey_ops_for_all_agents`, `keyhive.rs:1746`). Our pin's own comments name the
   consequence: a state that topsorts to nothing means "no peer can ever obtain this individual's
   prekeys, and the next grant naming that individual would fail prekey selection"
   (`individual/op.rs:48-62`, `individual.rs:415`) — exactly our symptom. The pin repairs two
   shapes: rotation-only states are seeded from their unconsumed rotations (`op.rs:48-70`) and
   cyclic rotation-only states are seeded digest-ordered, with an `emitted` dedup that **drops the
   rotation closing a cycle** (`op.rs:72-95`, test comment in `individual.rs:448-470`).

5. **Pin vs upstream.** The pin `1063446711dc` is our own fork commit on `townframe-changes`
   ("fix: prekey issues", 673 insertions across `keyhive.rs` +516, `individual.rs` +188,
   `individual/op.rs` +51, `document.rs`, `beekem/cgka.rs`). The sort the operator remembers
   (`op.rs:94`) is **the pin's** cycle-seed digest ordering, not the upstream "remove the sorting"
   change. Upstream PR branches present locally (none is a descendant of our pin; each carries the
   pre-pin `topsort` signature `HashSet<KeyOp> -> Vec<KeyOp>`):
   - `jtfm/events-for-agents@upstream` `a691f347` — *"Send every prekey op the agent needs without
     topsorting them"*: the general fix; it would sidestep the whole class rather than repair shapes.
   - `jtfm/batch-receive-prekey-ops@upstream` `52592e58`, `jtfm/replace-cgka-topsort@upstream` — older
     API, not on top of the pin.
   The two local commits above the pin (`74d0e924bcf9`, `3993e22b0b99`) touch only
   `beekem/README.md`, `beekem/src/cgka.rs` (+9), `keyhive_core/src/cgka.rs` (+7) — **nothing here**.

6. **`coparent_count=4`**: it is `reservation.parents.len()` (`keyhive.rs:919`) — the authority ids
   our allocation path passed to `reserve_doc_id` (called from `runtime2/native.rs:1231`), resolved
   back to agents at finalize time. So the four are the doc's parent authorities (the repo/app/doc
   principals of the allocation), not four members of a fresh personal group; at least one of them
   is a group or document, which is how the walk reaches delegation-embedded individuals.

## Repro status (honest)

The failure is load-triggered and **did not fire in 8 consecutive runs** of the single test under
CPU contention (10 burners, load avg 11): 8/8 passed, ~51 s each, zero probe hits and zero
diagnostics. The three sightings were all inside full `-E 'test(blobs)'` runs (load 12–22).
Reproducing it needs the full blob set's contention, or a deterministic fixture of the state below.

## Instrumentation landed (droppable)

Both hunks are marked `TEMP-INSTRUMENTATION(prekey-dive)` in `src/big_repo/keyhive.rs`:

1. `explain_missing_prekeys` now also logs the **live** prekey count of the registry copy, so the
   next real failure immediately shows whether that copy is itself inconsistent
   (`ops=22, live=0`) or consistent (`ops=22, live=22`, i.e. the selection used another copy).
2. `probe_coparent_prekeys`, called from `finalize_reserved_doc`'s error branch over a clone of the
   coparent peers, logs per coparent: for `Peer::Individual`, the peer copy's `ops`/`live`, the
   registry copy's `ops`/`live`, and `same_arc = Arc::ptr_eq(registry, peer)`, plus whether
   `pick_individual_prekeys` succeeds and with which error; for `Peer::Group`, each member's
   embedded individual's `ops`/`live`; for `Peer::Document`, the id.

Recommended next probe if they want it without a repro: a **boot census** over every individual
(`ops == 0` or `ops > 0 && live == 0` → WARN with the id), run right after
`restore_from_storage_archive`/`import_prekey_state` in `big_repo/lib.rs:420-432`. That is
deterministic — it needs no flake — and it would show whether the failing state's zero-op copy is
already present at boot. It needs a way to enumerate individuals (no public iterator today; either
a small accessor in the fork, or census the members reachable from our own groups/docs).

Patching `../keyhive` to print the failing instance's `ops`/`live` would settle it in one shot, but
the `[patch]` block is commented out and uncommenting it is a pin-lifecycle change: **not done
here**, flagged as the decision it is.

## Commands run

- `grep`/`sed` over the pin `~/.cargo/git/checkouts/keyhive-a06ceed0eb8b6077/1063446/...` and over
  `../keyhive` (`jj log`, `jj diff`, `jj file show`, `jj bookmark list --all-remotes`; read-only).
- `python3` base58btc decode to map `z4ws5a5G…`=`3aa10c7a…` (the failing creator) and
  `z9F9gGaf…`=`7a7d701a…` (the individual with no selectable prekey).
- `cargo clippy -p big_repo --all-targets --all-features` → clean.
- 8 × `cargo nextest run -p daybook_core -E 'test(iroh_blob_pin_sync_replicates_and_fetches_blobs)'`
  under 10 CPU burners → 8/8 passed, no repro.

## Addendum 2026-10-06 — boot census + deterministic repro attempts

Executed per the operator's prekey-divergence brief. No townframe code changed
except this doc. All keyhive work is confined to the authorized detached
workspace `keyhive-2cycle` (`ykpowzns`, now commit `d3028441`, parent
`wzxlqwrx` = pin `1063446711dc`). No bookmarks moved, nothing pushed, no
townframe jj mutation.

### Boot census — NOT landed in townframe; accessor landed in the fork

The census could not land in townframe because **no keyhive-core accessor to
enumerate registered individuals exists at the pin**, verified by reading the
pinned source:

- `Keyhive.individuals` is a private field (`keyhive_core/src/keyhive.rs:119`,
  `individuals: Arc<Mutex<HashMap<IndividualId, Arc<Mutex<Individual>>>>>`);
  the only public lookups are `get_individual(id)` / `has_individual(id)` —
  id-keyed, no iterator.
- The restored archive's map `Archive.individuals` exists
  (`keyhive_core/src/archive.rs:24`) but is `pub(crate)`; `Archive` exposes
  only `id()`, so townframe cannot read it off the archive either — and
  `restore_from_storage_archive` drops the archive right after
  `try_from_archive` (`big_repo/keyhive.rs:211-248`).
- The public `reachable_prekey_ops_for_all_agents` was considered and rejected
  as the census route: it walks `transitive_members` of **every group and every
  document** (keyhive.rs:1774-1812) — per-doc scans, forbidden by the brief —
  and yields topsorted op lists, not the materialized `prekeys()` count the
  second census class needs.

Per the brief's stop rule, the townframe wiring is withheld. The exact missing
accessor, landed in the fork workspace instead (authorized as a census
accessor commit):

```rust
// keyhive_core/src/keyhive.rs, next to get_individual:
pub async fn registered_individuals(&self) -> Vec<(IndividualId, Arc<Mutex<Individual>>)>
```

One lock-guarded read of the private registry map, no group/doc walks. With a
bump to `d3028441` the townframe census is a small loop after
`import_prekey_state` in `big_repo/lib.rs:420-432`: for each snapshot entry,
`WARN` when `prekey_ops().len() == 0` or (`ops > 0 && prekeys().len() == 0`),
with the `IndividualId` and both counts (both readable through the individual's
existing public getters). **Pin-lifecycle cost of the townframe side: a rev
bump in `Cargo.toml` to a descendant of `1063446711dc` (e.g. `d3028441`) plus a
push of the fork commit — an operator decision, not done here.** The accessor
is exercised by `registered_individuals_enumerates_every_registered_individual`
(keyhive.rs tests, PASS).

### Deterministic repro attempts — all constructible, one RED

All landed in `keyhive-2cycle` `d3028441`; run with
`cargo nextest run -p keyhive_core -E 'test(<name>)'` inside that workspace.

1. Candidate (a), template shape —
   `principal::individual::tests::topsort_published_set_survives_a_cycle_closing_on_an_already_emitted_key`
   (extends the template at `individual.rs:520` with the 2-cycle closing on
   `k1`, already emitted by the `k0 → k1` head, `k0` add pruned):
   **PASS/GREEN — no repro.** The state holds 3 ops; topsort publishes exactly
   `[rot(k0→k1), rot(k1→k2)]` — the closing rotation is dropped by the emitted
   dedup because its produced key `k1` is already advertised. The published key
   set `{k1, k2}` covers the locally materialized set and does not empty it
   (build()'s tombstones retire the cycled keys; the non-empty guard keeps one).
   So the emitted dedup only ever drops a redundant op, never a key.
2. Candidate (a), the pin-gap shape discovered while constructing (1) —
   `principal::individual::tests::topsort_advertises_a_rotation_cycle_next_to_a_disconnected_head`:
   **RED — a reproduction.** With a seeded head (`k0 → k1`, add pruned) standing
   next to a rotation-only cycle (`k2 ⤾ k3`), `KeyOp::topsort` seeds the cycle
   only `if heads.is_empty()` (`keyhive_core/src/principal/individual/op.rs:93`),
   so the gate sees a non-empty head list, seeds nothing from the cycle, and the
   walk (emitting only `k1`) never reaches it. Deterministic: 1 of 3 ops is
   advertised — the cycle's rotations disappear from what peers can obtain.
   This violates the invariant the pin's own
   `rotation_only_cycle_topsorts_to_every_rotation` establishes for the
   standalone cycle, i.e. it is literally "the same class the pin already
   fixed, reached through a case the fix misses". Honest scoping: the
   advertised set stays non-empty (a seeded/Add head always emits ≥ 1 op), so
   the incident's literal "serializes to nothing / nothing selectable" state is
   NOT reproduced — this repro states advertisement losing prekey ops
   outright. The workspace suite is intentionally red on this test until the
   `heads.is_empty()` gate is fixed (e.g. also seeding digest-ordered from
   cycles whose consumed keys are produced only within the cycle).
3. Candidate (c) —
   `keyhive::tests::group_walk_dereferences_the_registered_individuals_live_copy`:
   **PASS/GREEN — the stale-embedded-copy state is not constructible through
   hive flows at the pin.** The delegation's delegate materializes from the
   registry (`add_member` → `agent_by_id`; archive restore re-resolves every
   delegate from the same restored registry map, `keyhive.rs:3040-3061`), and
   `receive_prekey_op` mutates the registered `Arc` in place, so the group walk
   (`dlg.payload.delegate.dupe()`) dereferences the registry's live object:
   `Arc::ptr_eq` TRUE, an op arriving after the delegation raises the embedded
   copy's counts too, and `pick_individual_prekeys` succeeds. The dive's
   class-(c) premise (walk handed a stale empty copy while the registry holds
   22 ops) is therefore not reachable via `add_member`, archive restore, or
   static-event materialization in this code as written; if the incident was
   class (c), something outside these sites (a direct registry replacement or a
   divergent deserialization) would have to be involved — no such site was
   found.

Also worth recording: for a non-empty op map, `topsort` cannot return empty at
all — every head, `Add` or seed, emits at least one op. So a **non-empty**
individual state cannot "serialize empty" in any shape; an empty advertisement
requires an empty op map (an empty op log copy), which is consistent with the
original verdict's "the failing copy held an empty op log".

### Commands run (this addendum)

- `jj workspace list -R ../keyhive` / `jj log -R ../keyhive --limit 8` —
  verified `keyhive-2cycle` (`ykpowzns`/`ea6c2e10`, empty, atop
  `wzxlqwrx`=pin) still exists; read-only.
- Read-only source inspection over the pinned checkout
  `~/.cargo/git/checkouts/keyhive-a06ceed0eb8b6077/1063446/…` (keyhive.rs,
  archive.rs, group.rs, membered.rs, agent.rs, individual.rs, state.rs,
  individual/op.rs).
- `flock /tmp/townframe-cargo-validation.lock cargo nextest run -p keyhive_core
  -E 'test(<4 new test names>)'` in `/run/media/asdf/p3N/tmp/keyhive-2cycle` →
  2 RED (then 1 fixed to GREEN), final: 3 passed / 1 failed, the RED being (2)
  above deliberately.
- `flock /tmp/townframe-cargo-validation.lock cargo clippy -p keyhive_core
  --all-targets` in that workspace → 0 warnings, 0 errors.
- `jj describe -m …` in the keyhive-2cycle workspace only (change `ykpowzns` →
  `d3028441`); `jj st -R <workspace>` clean of unintended files.
- `cargo clippy -p big_repo` NOT run: no townframe code changed by this
  addendum (the census is withheld pending the pin-bump decision).

## Addendum 2026-10-25 — cycle-seeding gate fixed; the RED repro is GREEN

Fix landed as a new commit atop the pin in `keyhive-2cycle`
(`wmwskyrz` atop `ykpowzns` pin content; `d3028441`'s tree untouched): the
`heads.is_empty() && !rotate_key_ops.is_empty()` gate at
`keyhive_core/src/principal/individual/op.rs` (was `:88`/`:93`) is gone.
Cycle-seeding is now by construction, not state shape: the walk below emits a
rotation exactly when the key it consumed was emitted, so its emitted-key set
is precisely the closure of the head keys over "key emitted → keys of the ops
consuming it"; every rotation outside that closure — exactly the cycle ops no
head can reach — is seeded, digest-ordered, regardless of head count, while
rotations the walk reaches are excluded (so nothing the walk emits is seeded
again) and the `emitted` dedup still terminates cycles and emits each op once.

Outcome: the RED
`topsort_advertises_a_rotation_cycle_next_to_a_disconnected_head` is GREEN —
the advertisement is 3 ops (`{k1, k2, k3}`, none twice), i.e. every rotation
of the disjoint cycle is advertised alongside the head. The pin's other two
behaviors are preserved exactly: `topsort_does_not_seed_a_rotation_whose_
consumed_key_is_produced` (`individual.rs:520`) and
`topsort_published_set_survives_a_cycle_closing_on_an_already_emitted_key`
pass with byte-identical assertions (the reachable pre-walk never seeds an op
the walk will emit, so their order and published sets are unchanged), as do
`rotation_only_cycle_topsorts_to_every_rotation` and the `keyhive::tests`
census-accessor + group-walk tests. No widening: before/after pass-line diffs
of `-E 'test(topsort)'` (43 tests) and `-E 'test(prekey)'` (22 tests) show the
only status change is the RED test itself flipping to pass; both sets are
otherwise identical. `cargo clippy -p keyhive_core --all-targets --all-features`
→ 0 warnings, 0 errors. Commands and pass-line captures: baseline
`/tmp/baseline-{topsort,prekey}.txt`, after `/tmp/after-{topsort,prekey}.txt`,
final tree `/tmp/final-{topsort,prekey}.txt` (workspace-side, not archived).
