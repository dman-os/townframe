# `daybook_core::sync` audit gaps

This file tracks the parts of the sync rewrite that still need to be brought back to parity with the main branch or tightened up with regression tests.

## Regression coverage that was lost or weakened

- `src/daybook_core/sync/tests/ladder.rs:test_base` is only a smoke test today. It boots two repos, connects them, and then exits without asserting any behavior. That is not regression coverage and it can hide breakage while making the suite look green. It should be replaced with a concrete lifecycle assertion, such as reconnecting after one peer restarts while a live connection was active.

- The removed tests around incremental Automerge sync protocol behavior should come back somewhere in the tree. They proved that sync exchanges incremental changes rather than full history, which matters for large docs and for keeping fragment pressure under control. Those checks now live nowhere obvious in `core::sync`, so the rewrite lost an important safety net.

## Implementation gaps

- `src/daybook_core/sync/bootstrap.rs` now does a direct `RepoSyncRpc -> GetDocsFull -> import_doc` pass while clone bootstrapping. That duplicates the same full-doc fetch/import flow already present in `src/daybook_core/sync/full/import_worker.rs`. The duplication makes the bootstrap path wider than it needs to be and creates two places to fix if the import shape changes. The intended design should be a single reusable import path, with bootstrap only orchestrating the initial pull.

- `src/daybook_core/sync.rs` currently synthesizes peer keys from endpoint IDs in the connection/bootstrap path. That may be equivalent in the current iroh identity model, but the code is no longer demonstrating that assumption. The old code read the peer key from the live connection state. The current version should either derive the same canonical peer identity from the authenticated connection or prove, with tests, that the endpoint-id-derived form is truly equivalent for authorization and session bookkeeping.

## What this should become

- The tests in `core::sync` should prove restart/reconnect behavior, not just reach it.
- The bootstrap path should reuse the same full-doc import machinery as the normal sync worker path.
- The sync worker tests should prove the changed behavior is intentional, not accidental.
- The lower-level sync protocol regressions that used to live in the old samod-era tests should be restored at the new abstraction boundary so future refactors cannot quietly drop them again.
