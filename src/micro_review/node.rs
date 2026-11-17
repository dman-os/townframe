//! The node seam: what the review micro asks of the node. A micro reaches only Clay collections,
//! config and other micros' publications today, so the node's methods (`summarizeIntxts`,
//! `readDatums`) are reached through this trait, which the micro owns and fakes ([`crate::fixture`]).
//!
//! WHERE THE REAL IMPLEMENTATION PLUGS IN: when claytxt's micro -> node transport lands, a
//! wrapper around `clay_mdk::Context` implements [`NodeCalls`]: `call` is `Context::call::<M>(input)`
//! (async only: it is spawned from a resource or an event handler, never awaited in mount or
//! dispatch), `read_capability` is the document grant (or a `mintDocCapability` call) and
//! `held_pulls` is read from the micro's configuration. `activate` in `lib.rs` is the one place
//! that chooses the implementation: it builds `Fixture::sample()` today.
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use method_core::Unary;
use service_core_rs::ClientError;
use std::future::Future;

/// Why a call did not return its answer: both are the edge's failures to report, not invariants.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CallError {
    /// The node answered with the method's error (or refused the call).
    #[error("the node refused the call: {0}")]
    Refused(String),
    /// The call never reached the node.
    #[error("the call did not reach the node: {0}")]
    Transport(String),
}

impl<E: std::fmt::Display> From<ClientError<E>> for CallError {
    fn from(error: ClientError<E>) -> Self {
        match error {
            ClientError::Method(refusal) => Self::Refused(refusal.to_string()),
            ClientError::Transport { message, .. } => Self::Transport(message),
        }
    }
}

/// The micro's view of the node. `Clone` because each task the micro spawns holds its own handle.
pub trait NodeCalls: Clone + 'static {
    /// One call of the node method `M`.
    /// The signature is `clay_mdk::Context::call` of #169 (`feat/micro-dispatch-transport`),
    /// `pub async fn call<M: Unary>(&self, input: M) -> Result<M::Output, ClientError<M::Error>>`,
    /// so its impl is `self.context.call(input)`. `ClientError` is `service_core_rs`'s until
    /// that branch re-exports it from `clay_mdk`.
    fn call<M: Unary>(
        &self,
        input: M,
    ) -> impl Future<Output = Result<M::Output, ClientError<M::Error>>>;

    /// The capability that reads `doc` (the `capability` a `readDatums` carries).
    fn read_capability(&self, doc: &DocId) -> impl Future<Output = Result<CapId, CallError>>;

    /// The documents of the pull requests the node holds, one `githubPull` datum each. No method
    /// enumerates them, so the answer is the micro's configuration, not a node call.
    fn held_pulls(&self) -> impl Future<Output = Result<Vec<DocId>, CallError>>;
}
