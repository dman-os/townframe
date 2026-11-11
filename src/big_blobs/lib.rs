// The `Send` auto-trait proof for the blob projection task's future
// (workers.rs spawning BlobProjection::run) unfolds the projection's nested
// `tokio::select!` loops over cabinet frontier readers, where every opaque
// `impl Future` adds several trait-selection frames. That overflows rustc's
// default 128-step recursion budget, which nightly reports as the crate-wide
// `recursion_depth_exceeding_limit` lint (rust-lang/rust#159228); raising the
// limit lets the proof complete.
#![recursion_limit = "256"]

pub(crate) use sqlite_rs as sqlite_rs_alias;
mod admission;
pub mod documents;
pub mod encrypt;
mod interlude;
mod ownership;
pub mod pair_roots;
mod permissions;
pub mod projection;
mod retention;
pub mod schema;
pub mod service;
pub mod sync;
mod work_queue;
pub mod workers;

pub use admission::Coverage;
pub use admission::CoverageGap;
pub use admission::InventoryMirrors;
pub use admission::InventorySelection;
pub use service::BlobService;
pub use service::Blobs;
pub use service::Config;
pub use service::Storage;
pub use service::boot;
