//! The guest's `wasi:http/client`: the only network a micro has, `http://dispatch.local`.
//!
//! The linker offers the WASI p3 sockets and filesystem too, but the store's `WasiCtx` grants
//! nothing, so those fail. `wasi:http` is different: it has to answer something, so the store's
//! hooks override `send_request` (the crate's own sender, which opens connections, is never
//! reachable) and hand every guest request to the incarnation's [`MicroEgress`]. That applies
//! the origin and route policy and calls the host's
//! [`MicroDispatch`](clay_host::MicroDispatch) as the micro whose store this is.

use bytes::Bytes;
use clay_host::DispatchAnswer;
use clay_host::MAX_BODY_BYTES;
use clay_host::MicroEgress;
use clay_host::Outgoing;
use http_body_util::BodyExt as _;
use http_body_util::Full;
use http_body_util::LengthLimitError;
use http_body_util::Limited;
use wasmtime_wasi_http::Error;
use wasmtime_wasi_http::RequestOptions;
use wasmtime_wasi_http::WasiBody;
use wasmtime_wasi_http::WasiHttpHooks;

/// One incarnation's `wasi:http` hooks. `None` is an incarnation with no dispatch wired: every
/// request is denied, exactly the old "no network" rule.
#[derive(Default)]
pub(crate) struct EgressHooks {
    egress: Option<MicroEgress>,
}

impl EgressHooks {
    /// Wire the egress of the incarnation, from its initial context.
    pub(crate) fn install(&mut self, egress: Option<MicroEgress>) {
        self.egress = egress;
    }
}

type Answer = (
    http::Response<WasiBody>,
    Box<dyn Future<Output = Result<(), Error>> + Send>,
);

impl WasiHttpHooks for EgressHooks {
    /// A guest's headers reach dispatch as the guest wrote them: no `host` is added on its
    /// behalf, and none of them is identity.
    fn set_host_header(&mut self) -> bool {
        false
    }

    /// The origin check needs the scheme the guest named, so a request without one is a
    /// protocol error instead of defaulting to a scheme this egress would then accept.
    fn default_scheme(&mut self) -> Option<http::uri::Scheme> {
        None
    }

    fn send_request(
        &mut self,
        request: http::Request<WasiBody>,
        options: Option<RequestOptions>,
        fut: Box<dyn Future<Output = Result<(), Error>> + Send>,
    ) -> Box<dyn Future<Output = Result<Answer, Error>> + Send> {
        // The deadline is dispatch's: it parses the request's own `x-timeout-ms` budget, so the
        // transport options carry nothing this egress would act on. `fut` reports response
        // processing errors, which an in-memory answer cannot have.
        drop((options, fut));
        let egress = self.egress.clone();
        Box::new(async move {
            let egress = egress.ok_or(Error::HttpRequestDenied)?;
            let (parts, body) = request.into_parts();
            let body = Limited::new(body, MAX_BODY_BYTES)
                .collect()
                .await
                .map_err(|error| match error.downcast::<Error>() {
                    Ok(error) => *error,
                    Err(error) if error.is::<LengthLimitError>() => {
                        Error::HttpRequestBodySize(None)
                    }
                    Err(error) => Error::InternalError(Some(error.to_string())),
                })?
                .to_bytes();
            let outgoing = Outgoing {
                method: parts.method.as_str().to_owned(),
                scheme: parts.uri.scheme_str().map(str::to_owned),
                authority: parts
                    .uri
                    .authority()
                    .map(|authority| authority.as_str().to_owned()),
                path_with_query: parts
                    .uri
                    .path_and_query()
                    .map(|path| path.as_str().to_owned()),
                headers: parts
                    .headers
                    .iter()
                    .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
                    .collect(),
                body: body.to_vec(),
            };
            let answer = egress
                .send(outgoing)
                .await
                .map_err(|_denied| Error::HttpRequestDenied)?;
            Ok((response(answer)?, Box::new(async { Ok(()) }) as _))
        })
    }
}

/// The `http` response of a dispatch answer. A status or header dispatch could not have put on
/// the wire is a bug in the `MicroDispatch` implementation, which crashes here.
fn response(answer: DispatchAnswer) -> Result<http::Response<WasiBody>, Error> {
    if answer.body.len() > MAX_BODY_BYTES {
        return Err(Error::HttpResponseBodySize(Some(answer.body.len() as u64)));
    }
    let mut response = http::Response::new(
        Full::new(Bytes::from(answer.body))
            .map_err(Error::from)
            .boxed_unsync(),
    );
    *response.status_mut() =
        http::StatusCode::from_u16(answer.status).expect("dispatch answers a valid HTTP status");
    for (name, value) in answer.headers {
        response.headers_mut().append(
            http::HeaderName::from_bytes(name.as_bytes())
                .expect("dispatch answers valid header names"),
            http::HeaderValue::from_bytes(&value).expect("dispatch answers valid header values"),
        );
    }
    Ok(response)
}
