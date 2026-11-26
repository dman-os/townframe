# keyline_sqlite

A SQLite backend for [`keyline`](https://crates.io/crates/keyline)'s `Keyline` trait, the
authority graph of keyhive: signed delegations and revocations, evaluated by stratified Datalog.

- The certificate set and its version live in two tables, in a database that can be shared with
  other stores. `SqliteKeyline::insert_in` adds a batch inside a transaction the caller owns, so
  a grant commits or rolls back with the rows written beside it.
- Set-shaped queries are SQL over indexes. An evaluated query loads the closure of its subject
  from the tables, a round of two indexed statements per level of derivation depth, and runs
  `keyline_memory`'s reference evaluator over exactly those rows: nothing is held between queries
  but a bounded memo of verified rows, so a point query costs its closure and not the set, and the
  answers agree with the reference by construction. See the crate docs and
  `docs/scratch/hive-store-design.md`.
- Verified by the upstream conformance suite, a differential proptest against `MemoryKeyline`,
  transaction tests, query-plan tests, row-count checks that a query reads only its closure (with
  planted bugs the checks must catch) and allocation scaling checks. `benches/instr.rs` holds
  the instruction-count benches.

## Platforms

Native and `wasm32-wasip3` (a component guest over `townframe:sqlite`). **Browser support waits on
a `sqlite_rs` browser backend**; the crate has no `wasm32-unknown-unknown` build until then.
