//! `wasi:http/client` for guests: the only network a micro has, and it reaches no network at all.
//!
//! A guest's request is admitted only when it is addressed to [`DISPATCH_AUTHORITY`]. It is then
//! handed to the composition's [`DispatchEgress`] for this incarnation, which answers it as the
//! micro's actor (Clay on the node §3); nothing is ever connected. Every other authority is
//! refused as a denied request, and so is every request of a micro the composition gave no
//! egress. The WASI no-network pin of the context therefore reads "no network except dispatch".
//!
//! The guest never holds capability bytes: its `authorization` header is dropped here, before the
//! seam, and the composition attaches the micro's own credential.

use clay_guest_session::egress::DISPATCH_AUTHORITY;
use clay_guest_session::egress::DispatchEgress;
use clay_guest_session::egress::EgressBody;
use http_body_util::BodyExt;
use std::future::Future;
use std::sync::Arc;
use wasmtime_wasi_http::Error;
use wasmtime_wasi_http::RequestOptions;
use wasmtime_wasi_http::WasiBody;
use wasmtime_wasi_http::WasiHttpHooks;

type Sent = Box<
    dyn Future<
            Output = Result<
                (
                    http::Response<WasiBody>,
                    Box<dyn Future<Output = Result<(), Error>> + Send>,
                ),
                Error,
            >,
        > + Send,
>;

/// One incarnation's outgoing-request policy.
#[derive(Default)]
pub(crate) struct Hooks {
    pub(crate) egress: Option<Arc<dyn DispatchEgress>>,
}

impl WasiHttpHooks for Hooks {
    fn send_request(
        &mut self,
        request: http::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _fut: Box<dyn Future<Output = Result<(), Error>> + Send>,
    ) -> Sent {
        let admitted = request
            .uri()
            .authority()
            .is_some_and(|authority| authority.as_str() == DISPATCH_AUTHORITY);
        let Some(egress) = self.egress.clone().filter(|_| admitted) else {
            return Box::new(async { Err(Error::HttpRequestDenied) });
        };
        let (mut parts, body) = request.into_parts();
        parts.headers.remove(http::header::AUTHORIZATION);
        let body: EgressBody = body
            .map_err(|error| std::io::Error::other(error.to_string()))
            .boxed_unsync();
        let request = http::Request::from_parts(parts, body);
        Box::new(async move {
            let (parts, body) = egress.send(request).await.into_parts();
            let body: WasiBody = body.map_err(Error::Connect).boxed_unsync();
            // Nothing is uploaded out of band: the seam reads the request body itself.
            let io: Box<dyn Future<Output = Result<(), Error>> + Send> = Box::new(async { Ok(()) });
            Ok((http::Response::from_parts(parts, body), io))
        })
    }
}
