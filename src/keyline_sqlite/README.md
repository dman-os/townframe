# keyline_sqlite

A SQLite backend for [`keyline`](https://crates.io/crates/keyline)'s `Keyline` trait, the
authority graph of keyhive: signed delegations and revocations, evaluated by stratified Datalog.

- The certificate set and its version live in two tables, in a database that can be shared with
  other stores. `SqliteKeyline::insert_in` adds a batch inside a transaction the caller owns, so
  a grant commits or rolls back with the rows written beside it.
- The positive reach (stratum 1) is a table extended at insert, in the caller's transaction, by
  semi-naive SQL rounds; the live set (stratum 2) is computed per subject only where a revocation
  disputes it. Point queries are indexed reads. See the crate docs and
  `docs/scratch/hive-store-design.md`.
- Verified by the upstream conformance suite, a differential proptest against `MemoryKeyline`,
  transaction tests, query-plan tests, statement and row counts for point queries, inserts and gift certificates (with planted bugs the
  checks must catch) and allocation scaling checks. `benches/instr.rs` holds
  the instruction-count benches.

## Platforms

Native and `wasm32-wasip3` (a component guest over `townframe:sqlite`). **Browser support waits on
a `sqlite_rs` browser backend**; the crate has no `wasm32-unknown-unknown` build until then.
