//! The reference service. Copy this layout for a new service:
//!
//! * [`protocol`]: the request, output, event, and error types; one
//!   [`Operation`](crate::Operation) impl per request; the irpc protocol enum;
//!   and the [`Route`](crate::Route) impl that binds the enum to a host. This
//!   is the whole public surface a remote consumer needs.
//! * `service`: the implementation. One [`Handler<P>`](crate::Handler) impl
//!   per operation, plus whatever background work the service owns, spawned
//!   on the [`Spawner`](crate::lifecycle::Spawner) of its own
//!   [`StopToken`](crate::StopToken).
//! * [`boot`]: wires the service into a hosted, layered handler and returns
//!   one stop token that shuts down the host first and the service second.
//!
//! The sample itself models a source-and-projector service: writers change a
//! source revision, an owned projector task interprets the source into
//! readable state, and subscribers observe interpreted changes. A write
//! completes only once the projector has caught up with it.

pub mod protocol;
mod service;

#[cfg(test)]
mod tests;

pub use protocol::*;
pub use service::{CallLog, Log, Logged, Sample, SampleHandler, SampleService, boot};
