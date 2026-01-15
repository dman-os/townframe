# CONTRIBUTING

## Style guide

- Avoid crates with a `src/` directory. 
  - The source files and `Cargo.toml` should reside in the same root.
- Avoid deeply nested directory hierarchies.
- Are you importing that type in too many files? Consider putting it in the `interlude` module of the crate.
