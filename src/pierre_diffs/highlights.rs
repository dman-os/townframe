// Slice S7: the `highlights` highlighter backend. `@pierre/highlights` 0.4.0 is
// a WebAssembly lexer plus JavaScript glue; the wasm is unmodified and lives
// behind `highlighter::HighlightRecords`, the glue (themes, tokens) and the
// diffs adapter (`backend`, `themes`) are ported here.

pub mod backend;
pub mod data;
pub mod highlighter;
pub mod theme;
pub mod themes;
pub mod theming;
pub mod tokens;

#[cfg(test)]
mod oracle_tests;
