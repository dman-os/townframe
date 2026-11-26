# Upstream study: keyhive integration, checkpoints, author filtering, automerge 0.12

Status: Findings (facts marked [F], inferences [I], unverified [?]); no decisions.
Owner: the automata lane (prod-readiness).
Scope: how upstream (keyhive, subduction `keyline-take-2`, automerge-repo-keyhive and its
successor, automerge) handles what `big_repo::runtime` must decide. Read-only study.
Updated: 2026-11-26.

Clones studied (in the lane's scratchpad):
- `automerge-repo-keyhive` main `0f5ae05`;
- keyhive main `fef75a5` (`keyhive_core` 0.12.3);
- subduction `keyline-take-2` `a08f3bd`;
- automerge main `eb05b4dd2`, plus PR #1742 as a branch;
- automerge-repo `jtfm/add-keyhive-backend` `34bd67e3`.

Marks: **[F]** a fact read in code or a PR, **[I]** an inference, **[?]** unverified.

## 1. Where the keyhive integration lives now

- **[F] automerge-repo-keyhive (ARK)** `0.8.0-alpha.1` is on Keyline. It pins `@keyhive/keyhive`
  0.6.0 = `keyhive_core` 0.10.0, which includes Keyline (#288), invitation-wrapping Add (#262),
  predecessor chains (#264) and causal encryption (#317).
- **[F] Its successor is `automerge/automerge-repo` PR #789**, "Add Keyhive access control and
  E2EE": open, by John Mumm, branch `jtfm/add-keyhive-backend`, on `v3/experimental`.
  - It adds the package `packages/automerge-repo-subduction-keyhive`: `KeyhiveBackend.ts`,
    `LocalKeyhive.ts`, `KeyhiveSession.ts`, `DocumentRefs.ts`.
  - It pins `@automerge/subduction` 0.27.0 and `@keyhive/keyhive` 0.8.3 (= keyhive main, core
    0.12.3).
  - `KeyhiveBackend` wraps `SubductionBackend` and encrypts **sedimentree records**, not Automerge
    documents.
- **[?] Not inspected:** `keyhive-react`, `keyhive-todo-app-demo`, and the open keyhive PRs #210
  (our "townframe changes") and #205.

## 2. Checkpoints ("nudge edits"): upstream removed them

- **[F]** ARK `390a148` "Tear out the nudge edit machinery!" deleted the nudge field, membership
  nudges, `forcePcsUpdate` plus `saveLeafSecret` plus empty edit, and the leaf-secrets store. A
  new member reads the whole history once synced (`test/new-member-access.test.ts`). Adding a
  member writes nothing to the document. #789 has no checkpoint logic either.
- **[F] What replaced them**, all in keyhive:
  - **#262 invitations:** a CGKA Add wraps ancestor root secrets for the invitee
    (`beekem/src/cgka.rs` ~440-470). Best-effort: if no invitation can be derived, the code logs
    that the invitee can't read pre-add content until a later update wraps those secrets.
  - **#264 predecessor chain:** each Update carries `predecessor_secrets` (nearest update
    ancestors plus `unchained_updates`) sealed under the new PCS key (`cgka.rs:1079-1100`,
    `:107`).
  - **Decrypt:** the ciphertext header holds `content_ref ‖ pred_refs ‖ group_secret_hash ‖
    group_secret_origin_hash` (`keyhive_content/src/encrypted.rs:10-11`). Then
    `find_or_derive_root_secret` tries the recorded secret, the tree root, the update path, the
    predecessor chain, then a rebuild (`cgka.rs:1018-1053`).
  - **Write:** `group_secret_for_write` auto-rotates when the writer can't derive the single
    current root (including concurrent CGKA heads), and that rotation chains the predecessors
    (`cgka.rs:328-365`). **An epoch merge is a side effect of the next ordinary write.**
- **[I] Remaining gap:** concurrent epochs with no later writer. No upstream repo-level test
  covers it; `test/sync.test.ts:326` covers concurrent writes followed by a merge write.
- **[I] Detecting a fork without decrypting:**
  - each frontier block's plaintext `group_secret_origin_hash` can be compared with the public
    CGKA op graph's heads;
  - "covered" is a CGKA-graph property, and the commit DAG only says which blocks are concurrent;
  - **[?]** `unchained_updates` is `pub(crate)`, with no public or wasm API.

## 3. Read-time authorization

- **[F]** Subduction `policy.rs:19-35`: puts check the requestor (at least Relay). Whether an
  author's write counts "is the materializer's decision, made at read time".
- **[F]** Keyline revocations carry a `retain` watermark (`ContentHeads`) that evaluation never
  reads. Whiteout is an open question upstream (`design/keyline/README.md:258,582-585`).
- **[F]** #789 filters nothing by author. Hiding a revoked author is cryptographic: `revokeMember`
  first records the content heads, and the revoked reader can't decrypt later records
  (`LocalKeyhive.ts:138-160`, `membership.test.ts:270`).
- **[F] Automerge's write frontier is the author-filtered materialization** (#1581, merged
  2026-10-09):
  - `Automerge::mask_author(author, heads, &mut PatchLog)` / `reveal_author` (`automerge.rs:565,
    582`);
  - `LoadOptions::write_frontier(HashMap<Author, Vec<ChangeHash>>)` (`:202`);
  - AutoCommit and JS bindings;
  - semantics: per-author inclusive heads; heads not yet present hide the author until they are
    witnessed; partial frontiers are allowed.
- **[F]** Masks are keyed by **Author**: free-form bytes in an actor's first change, which are
  **not authenticated**.
- **[F] automerge PR #1742 "Automerge Attestation"** (open, FintanH, +23k/-1.3k):
  - adds Ed25519 change signing and verification, signature frontiers on fragments, admission
    selection, and `LoadOptions::verification_mode`;
  - **removes `set_author`/`with_author`** in favour of `Config` / `from_config` /
    `fork_with_author`;
  - `Automerge` no longer implements `SyncDoc`; `set_actor` is deprecated and fallible;
  - attested and unattested docs can't merge.
- **[I]** #1742 binds an Author to a key, which a trustworthy mask needs. big_repo would own the
  mapping from a Keyline `retain` watermark to a `write_frontier`, and the binding from Author to
  signer. No upstream doc links `mask_author` to `retain` yet.

## 4. automerge 0.12 and main

- **`dman-os/townframe#54`** (automerge 0.11 → 0.12, automerge-test 0.12, autosurgeon fork on
  upstream 0.14):
  - adopts `set_author`, `Change::author`, `get_author_for_actor`, `BigRepo::local_author()`
    (= peer key bytes), and `reconcile_prop_with_author`;
  - about 10 `set_author` sites in `drawer/mutations.rs`;
  - provenance read back through `change.author`;
  - **[F]** `set_author` mints a random session ActorId per in-memory doc instance, so a doc's
    actor table grows by one per write session.
  - **[I]** #1742 removes `set_author`, so #54's call sites break when it lands. Prefer the
    `Config`/`fork_with_author` shape at the `local_author()` boundary.
- **0.12.0 (2026-09-16):**
  - Author APIs;
  - `Value::as_*`;
  - a large-transaction performance fix;
  - **`1ae4d18df`: fragment checkpoints exclude the fragment head** (it "led to failures to
    converge when syncing over subduction");
  - `ChangeMetadata` and `ExpandedChange` gain `author`.
- **Main, unreleased:**
  - the write-frontier series, with masked reads on indexes and the reindex skipped when nothing
    is hidden (`b1b101953`, +12-32% on load benches);
  - actor chain: new changes with seq > 1 depend directly on the actor's previous change, and
    empty changes advance seq (`f329a292e`);
  - deterministic fragment boundaries (`092532c52`), head-filtered fragment export (`9115c9cab`),
    staging (`2eb5bcf07`, `d09a2ffaf`);
  - `get_marks` 9 ms → 1.6 µs on 100k chars;
  - about 50 hexane hardening PRs; #1743 is a large refactor.
  - Track #1742, #1603, #1728 and #1730.
- **[F] There is no document-less change builder.** `Change::from_bytes` only parses;
  `empty_change` and `isolate` need a doc; staging still needs a doc to apply.
  - The big_repo port ledger §6.3 found `From<ExpandedChange> for Change` usable for
    hydration-free map puts.
  - **[?]** Reconcile that with this finding if the datum face is ever revisited. It's deferred.

## 5. Persist before apply (keyhive and CGKA)

- **[F] Nothing upstream persists a CGKA op or secret before applying it,** and there's no
  prepare-without-mutate API.
  - `try_encrypt_content_keyed` (`keyhive_core/src/keyhive.rs:1799-1832`) mutates
    `known_decryption_keys` and the CGKA graph, then `record_rotation` fires `on_cgka_op`.
  - `pcs_update` and `force_pcs_update` mutate, then return the op.
  - The only transactional tool is `ForkAsync`/`MergeAsync` (`keyhive.rs:4020-4100`). It shares
    the Keyline store; CGKA changes are dropped on failure and certificates kept.
- **[F] ARK:** encrypts in memory; events are saved later in a debounced queue; prekey secrets
  are re-saved only on prekey events, so the rotated leaf secret is saved late. It catches and
  logs.
- **[F] #789 is the first to order persist-before-publish.** `#encrypt` (memory), then
  `LocalKeyhive.persist()`, then `inner.store(ciphertext)` (`KeyhiveBackend.ts:~534-555`).
  "Key material reaches storage before the ciphertext that depends on it."
  - `CGKA_OPERATION` now flags the prekey secrets as changed.
  - On failure it re-queues the events and rejects the waiting stores.
  - Tests: `encryption.test.ts:148,165,46`.
  - **[I]** The guarantee is key material durable before ciphertext, not persist-before-apply.
- **[F] Subduction `keyline-take-2` ingest is apply-then-persist and tolerant.** `199505e` falls
  back to per-event saves when the archive save fails. `save_archive` warns and returns `false`
  (`protocol.rs:1030-1160`). That is catch-and-log; **don't copy it.**

## 6. Checkpoint parents

- **[F]** Upstream has no checkpoint commit. Encryption is sedimentree-level: `#encrypt` takes a
  sedimentree record, builds the content ref from its head, and maps predecessors to fragments or
  to themselves. No Automerge document is loaded. Revocation reads sedimentree heads
  (`subduction.getHeads`).
- **[F] Townframe's checkpoint is already sedimentree-level.** `persist_causal_checkpoint`
  (`native.rs:538-670`) encrypts a `CausalCheckpoint` and calls `store_commit(sed_id, head,
  covered_frontier, blob)`. **Only the frontier choice hydrates:** `reconcile_causal_coverage`
  takes `covered_frontier` from `doc.get_heads()` (`doc_worker.rs:1523-1563`).
- **[I] It can be produced without hydration** if `covered_frontier` is the sedimentree head
  set. Caveats:
  - sedimentree heads include other partitions' checkpoints;
  - the automerge-to-sedimentree head mapping is lossy under fragments;
  - **[?]** covering heads the writer hasn't decrypted is a design question.

## Implications for big_repo::runtime (the study's list)

1. Checkpoints are not mandatory. Re-test on keyhive main: a new member, and concurrent epochs
   with no later writer. If invitations and chains suffice, `reconcile_causal_coverage` shrinks
   to "rotate on write when there is no single derivable root".
2. If a covering step stays, make it a **CGKA update** (`force_pcs_update` on a replica that can
   derive both epochs), not a content commit. Ask keyhive for a public read of
   `unchained_updates` and the CGKA heads as the fork detector.
3. Detect forks from the plaintext header plus the CGKA graph, not by decrypting.
4. If checkpoints stay, build them from `subduction.getHeads` and drop the Automerge reload.
5. Adopt #789's `persist()`-before-`store()` shape. True persist-before-apply needs a keyhive
   change or `ForkAsync`/`MergeAsync` around a CGKA-only encrypt. Raise it with keyhive.
6. Don't copy subduction's ingest recovery (catch-and-log).
7. Build read-time authorization on `mask_author`/`write_frontier`, keyed by Author bytes. big_repo
   maps `retain` watermarks to frontiers and binds Author to signer, after #1742.
8. PR #54 collides with #1742's removal of `set_author`. Stay on 0.12.0, and shape
   `local_author()` toward `Config`/`fork_with_author`. Note the per-session actor growth.
9. There is no document-less change builder; keep checkpoints sedimentree-level.
10. Take 0.12.0's `1ae4d18df`; track staging and deterministic fragments.
