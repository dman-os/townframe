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

// The durable outbox queues, replaces, cancels and reaps tasks through the keyed scheduler, but
// nothing yet retries a failed task, parks a key or ticks delayed tasks through it: its own
// backoff lives in the outbox machine. That half of the scheduler (the backoff, delayed queue,
// `fail`, `park`/`wake`) is driven by its tests alone, so it is dead in a lib build. The
// expectation turns into a warning the moment a consumer uses all of it.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "retry, backoff and parking have no consumer outside the scheduler's tests yet"
    )
)]
pub(crate) mod scheduler;
pub(crate) mod watermark;
