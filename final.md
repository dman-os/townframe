# Pair-roots / ownership SQL slice

Implemented and committed as `78f04b42` (`Make SQL unit dependencies explicit`).

- Added `sqlite/dependencies.txt` for ownership -> pair_roots dependency.
- Added dependency-first SQL-unit resolution with canonical `src` containment, missing-unit validation, and cycle detection tests.
- Moved pair-root cleanup triggers into ownership migration `003_pair_root_triggers.sql`; regenerated ownership SQL.
- Removed runtime `PairRoots::bind_ownership`, table probing, dynamic trigger creation, and the service call site.
- Boot now applies pair-roots migrations before ownership migrations so trigger creation follows the declared dependency.

Validation:

- `cargo nextest run -p big_blobs --lib --no-fail-fast`: 182 passed.
- `cargo nextest run -p xtask --no-fail-fast`: 22 passed.
- `cargo run -p xtask -- sql gen --check src/big_blobs/pair_roots src/big_blobs/ownership`: passed.
- `cargo run -p xtask -- sql check src/big_blobs/pair_roots src/big_blobs/ownership`: passed.
- `cargo clippy -p big_blobs --lib --all-features -- -D warnings`: passed.
- `cargo clippy -p xtask --all-targets --all-features -- -D warnings`: passed.
- `git diff --check`: passed.

The broader crate-wide inline SQL conversion remains outside this bounded slice.
