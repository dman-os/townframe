//! Building blocks the machines are made of and driven by: the shared driver skeleton
//! ([`calls`]), task scheduling and watermarks. Nothing here knows about parts, peers or replay.

pub mod calls;

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
