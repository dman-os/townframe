//! Building blocks the machines are made of: task scheduling, watermarks, the correlated
//! outbox and channel helpers. Nothing here knows about parts, peers or replay.

pub(crate) mod mpsc;
pub(crate) mod outbox;
pub(crate) mod scheduler;
pub(crate) mod watermark;
