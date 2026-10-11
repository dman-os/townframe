# PR 47 follow-ups

- [x] Use base58 instead of hex for dpath collision identity names.
- [x] Restore `src/btress_sysadmin` to workspace members; commit the working-copy change.
- [x] Finish the behavior-neutral `checkout.rs` split; focused validation passes.
- [x] Move the filesystem checkout surface under `db dtree`:
  - [x] `db dtree create|ingest|status|publish` for this PR.
  - [ ] Keep future `adopt|detach|watch|diff|log` dtree-scoped.
  - [x] Keep node/document-wide commands top-level; do not retain `db checkout` as an alias.
- [x] Remove pauperfuse migration/backcompat hooks; no legacy compatibility for this PR-introduced surface.
- [x] Align by-ID implementation with the accepted separate-checkout-class decision:
  - [x] Remove mixed `/by-id` handling from dpath derivation and its stale tests.
  - [x] Keep by-ID creation, ingestion, and write semantics deferred until separately designed.
  - [x] Reconcile FDR 001's mixed-surface examples with ADR 011/012.
  - [x] Improve by-ID documentation and record the remaining scope decisions.
  - [x] Add focused tests for the class boundary.
- [ ] Recheck the next PR CI run; investigate blob sync only if it reproduces.
- [ ] Fix the missing `nextest-run.zip` artifact path in CI.
- [ ] Discuss the proposed `pauperfuse_core` abstraction.
- [ ] Resolve remaining review questions: merge coverage, `fork_at` panic handling, and dpath specialization.
