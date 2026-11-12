//! A micro's only way out: `http://dispatch.local`.
//!
//! Every Clay runtime hosts `wasi:http/client` for its guests, and every runtime's egress is
//! [`MicroEgress`]: it admits `GET` and `POST /xrpc/<nsid>` on the `dispatch.local` authority
//! and nothing else, then hands the request to the host's [`MicroDispatch`]. This is the same
//! contract a nodule's egress has (`big_nodule/egress.rs`), so a micro and a nodule call node
//! methods the same way.
//!
//! The micro the call is made as is fixed when the incarnation's egress is built, from the
//! host's own incarnation table: [`MicroEgress`] holds the [`MicroId`] and passes it to
//! [`MicroDispatch::call`] itself. Nothing in the request (headers, body, path) reaches that
//! argument, so a guest cannot call as another micro. Minting the micro's capability and signing
//! as its actor is the [`MicroDispatch`] implementation's business (the node's dispatch); this
//! crate only keeps the seam honest.

use crate::supervisor::MicroId;
use futures::future::BoxFuture;
use std::sync::Arc;

/// The authority a guest addresses dispatch at. It resolves nowhere: [`MicroEgress`] answers it.
pub const DISPATCH_AUTHORITY: &str = "dispatch.local";

/// The route prefix of every admitted request.
const XRPC_PREFIX: &str = "/xrpc/";

/// A request body or response body larger than this is refused. Dispatch's inputs and outputs
/// are JSON values held in memory, so a bound here keeps a guest from making the host buffer
/// without limit.
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// The verbs `dispatch.local` serves: a query is a `GET`, a procedure a `POST`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchMethod {
    Get,
    Post,
}

/// An admitted call, as dispatch receives it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchCall {
    pub method: DispatchMethod,
    /// The method's NSID, the `<nsid>` of `/xrpc/<nsid>`.
    pub nsid: String,
    /// The raw, still percent-encoded query string of a `GET` (empty without one); dispatch
    /// decodes it by the method's parameters, as it does for the node's own XRPC listener.
    pub query: String,
    /// Exactly what the guest set, lowercase names. Never identity: see the module docs.
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// What dispatch (or this egress, for a request it refuses to forward) answers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchAnswer {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// The host's side of `dispatch.local`: routes one call into the node's dispatch as `micro`.
///
/// `micro` is the incarnation's own identity from the host's table. An implementation maps it
/// to the micro's actor and presents only that actor's holder-bound capability; it never learns
/// who is calling from the call itself.
pub trait MicroDispatch: Send + Sync {
    fn call(&self, micro: &MicroId, call: DispatchCall) -> BoxFuture<'static, DispatchAnswer>;
}

/// A guest request as the runtime's `wasi:http` implementation hands it over, before any policy.
#[derive(Clone, Debug)]
pub struct Outgoing {
    pub method: String,
    pub scheme: Option<String>,
    pub authority: Option<String>,
    pub path_with_query: Option<String>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// The request never reached dispatch because it is not addressed to it, or because the
/// incarnation has no dispatch at all (its egress is `None`: engine tests, no node). Runtimes map this to
/// `wasi:http`'s `HTTP-request-denied`: it is a refused connection, not an HTTP answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("only http://{DISPATCH_AUTHORITY} is reachable")]
pub struct Denied;

/// One incarnation's egress: its identity plus the host's dispatch.
#[derive(Clone)]
pub struct MicroEgress {
    micro: MicroId,
    dispatch: Arc<dyn MicroDispatch>,
}

impl MicroEgress {
    pub fn new(micro: MicroId, dispatch: Arc<dyn MicroDispatch>) -> Self {
        Self { micro, dispatch }
    }

    pub fn micro(&self) -> &MicroId {
        &self.micro
    }

    /// Runs a guest request. A wrong origin is [`Denied`]; a right origin with a wrong path,
    /// verb or body is answered the way an XRPC server answers a malformed request, so the
    /// guest's typed client sees an error body it already parses.
    pub async fn send(&self, outgoing: Outgoing) -> Result<DispatchAnswer, Denied> {
        let call = match admit(outgoing)? {
            Ok(call) => call,
            Err(answer) => return Ok(answer),
        };
        Ok(self.dispatch.call(&self.micro, call).await)
    }
}

/// The policy: the outer result is the origin check, the inner one the request's shape.
fn admit(outgoing: Outgoing) -> Result<Result<DispatchCall, DispatchAnswer>, Denied> {
    if outgoing.scheme.as_deref() != Some("http")
        || outgoing.authority.as_deref() != Some(DISPATCH_AUTHORITY)
    {
        return Err(Denied);
    }
    let bad = |status: u16, error: &str, message: &str| {
        Ok(Err(DispatchAnswer {
            status,
            headers: vec![("content-type".to_owned(), b"application/json".to_vec())],
            body: error_body(error, message),
        }))
    };
    let method = match outgoing.method.as_str() {
        "GET" => DispatchMethod::Get,
        "POST" => DispatchMethod::Post,
        _ => {
            return bad(
                405,
                "InvalidRequest",
                "dispatch.local serves GET and POST /xrpc/<nsid> only",
            );
        }
    };
    let path = outgoing.path_with_query.as_deref().unwrap_or_default();
    let Some(target) = path.strip_prefix(XRPC_PREFIX) else {
        return bad(
            404,
            "NotFound",
            "dispatch.local serves GET and POST /xrpc/<nsid> only",
        );
    };
    let (nsid, query) = target.split_once('?').unwrap_or((target, ""));
    if !is_nsid(nsid) {
        return bad(400, "InvalidRequest", "the method NSID is malformed");
    }
    if method == DispatchMethod::Get && !outgoing.body.is_empty() {
        return bad(
            400,
            "InvalidRequest",
            "query operations must not carry a request body",
        );
    }
    if method == DispatchMethod::Post && !query.is_empty() {
        return bad(
            400,
            "InvalidRequest",
            "procedures take their input as the request body, not a query string",
        );
    }
    Ok(Ok(DispatchCall {
        method,
        nsid: nsid.to_owned(),
        query: query.to_owned(),
        headers: outgoing.headers,
        body: outgoing.body,
    }))
}

/// An NSID is dot-separated ASCII segments; this checks the characters (so a path segment can
/// never smuggle a `/` or an escape), not the Lexicon grammar, which dispatch owns.
fn is_nsid(nsid: &str) -> bool {
    !nsid.is_empty()
        && nsid
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
}

/// The XRPC error envelope `{ "error": .., "message": .. }`, escaped by hand: both inputs here
/// are this module's own constant strings.
fn error_body(error: &str, message: &str) -> Vec<u8> {
    format!(r#"{{"error":"{error}","message":"{message}"}}"#).into_bytes()
}

#[cfg(test)]
mod tests;
