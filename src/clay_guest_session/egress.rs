//! The one outgoing channel a guest micro has: its `wasi:http` requests to `dispatch.local`
//! (Clay on the node §3), handed to the host composition as plain `http` types.
//!
//! An engine hosts `wasi:http/client` for its guests and admits only the `dispatch.local`
//! authority; every other origin is refused before this seam is reached, so a composition never
//! sees traffic for another host. What the engine hands over is the guest's request exactly as
//! the nodule transport sends it (`GET`/`POST /xrpc/<nsid>`, trace and budget headers) with any
//! `authorization` header removed: a guest never holds capability bytes. The composition answers
//! it, as the micro's own actor and with the micro's own capability, which it attaches itself.
//! The engine neither knows nor chooses either: the actor comes from the composition's own
//! incarnation table, never from anything the guest sends.

use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::combinators::UnsyncBoxBody;

/// The authority a guest addresses the node at. It resolves nowhere: the engine routes it to
/// [`DispatchEgress`] and refuses every other authority.
pub const DISPATCH_AUTHORITY: &str = "dispatch.local";

/// A request or answer body as the seam carries it: a JSON body whole, any other as a stream.
pub type EgressBody = UnsyncBoxBody<Bytes, std::io::Error>;

/// Answers the `dispatch.local` requests of one micro's incarnation. The composition supplies one
/// per micro (see [`crate::InitialContext::egress`]); a micro without one has no egress at all.
pub trait DispatchEgress: Send + Sync + 'static {
    /// Serves one request. A call that fails is an HTTP answer (an XRPC error body), never a
    /// transport failure: the node is in-process or one hop away, and a refusal is information
    /// the guest's typed client decodes.
    fn send(
        &self,
        request: http::Request<EgressBody>,
    ) -> BoxFuture<'static, http::Response<EgressBody>>;
}
