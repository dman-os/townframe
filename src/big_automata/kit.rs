//! Building blocks the machines are made of and driven by: the shared driver skeleton
//! ([`calls`]), task scheduling and watermarks. Nothing here knows about parts, peers or replay.

pub mod calls;

// std's `Instant::now` panics on wasm32-unknown-unknown, which has no clock syscall. web-time's
// `Instant` is a re-export of std's on every other target and reads `performance.now()` in the
// browser, so every machine's caller-supplied `now` can come from the same type everywhere.
pub type Instant = web_time::Instant;

/// The driver's injected clock; the machine never reads time itself. Its instant is the
/// web-safe [`Instant`].
pub type Clock = std::sync::Arc<dyn Fn() -> Instant + Send + Sync>;

/// Keyed task scheduling with backoff and parking. Public for the machines of other crates that
/// keep one task per key (the Hub's catch-up of documents).
pub mod scheduler;
/// A stream cursor that only advances past settled jobs.
pub mod watermark;
