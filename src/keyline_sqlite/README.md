# keyline_sqlite

A SQLite backend for [`keyline`](https://crates.io/crates/keyline)'s `Keyline` trait, the
authority graph of keyhive: signed delegations and revocations, evaluated by stratified Datalog.

- The certificate set and its version live in two tables, in a database that can be shared with
  other stores. `SqliteKeyline::insert_in` adds a batch inside a transaction the caller owns, so
  a grant commits or rolls back with the rows written beside it.
- Set-shaped queries are SQL over indexes; the evaluation is `keyline_memory`'s, cached in memory
  per version and rebuilt (with signatures verified again) from the rows when the cache is stale.
  See the crate docs for why it is not recursive CTEs.
- Verified by the upstream conformance suite, a differential proptest against `MemoryKeyline`,
  transaction tests, query-plan tests and allocation scaling checks. `benches/instr.rs` holds
  the instruction-count benches.

## Platforms

Native and `wasm32-wasip3` (a component guest over `townframe:sqlite`). **Browser support waits on
a `sqlite_rs` browser backend**; the crate has no `wasm32-unknown-unknown` build until then.
