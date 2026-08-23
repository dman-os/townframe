// FIXME: this loads the body always upfront,
// migrate wstd to wasi p3 and expose it to work

use crate::interlude::*;
use http::{Uri, uri::Parts};
use wasip2::{
    http::types::{
        Headers, IncomingBody, IncomingRequest, Method, OutgoingBody, OutgoingResponse,
        ResponseOutparam, Scheme,
    },
    io::streams::StreamError,
};

/// When working with streams, this crate will try to chunk bytes with
/// this size.
pub const CHUNK_BYTE_SIZE: usize = 64 * 1024;

pub fn try_from_incoming(
    req: IncomingRequest,
) -> Result<http::Request<axum::body::Body>, RequestError> {
    let mut builder = http::Request::builder();
    let req_method = method_wasi_to_http(req.method())?;
    let headers = req.headers();

    for (header_name, header_value) in headers.entries() {
        builder = builder.header(header_name, header_value);
    }

    drop(headers);

    let mut body_bytes = Vec::<u8>::with_capacity(CHUNK_BYTE_SIZE);

    {
        // NB(raskyld): consume could fail if, for some reason the caller
        // manage to recreate an IncomingRequest backed by the same underlying
        // resource handle (need to dig more to see if that's possible)
        let incoming_body = req.consume().expect("could not consume body");

        let body_stream = incoming_body
            .stream()
            .expect("could not create a stream from body");

        loop {
            match body_stream.read(CHUNK_BYTE_SIZE as u64) {
                Err(StreamError::Closed) => break,
                Err(StreamError::LastOperationFailed(err)) => {
                    return Err(StreamError::LastOperationFailed(err).into());
                }
                Ok(data) => {
                    body_bytes.extend(data);
                }
            }
        }
        drop(body_stream);
        IncomingBody::finish(incoming_body);
    }

    let mut uri_parts = Parts::default();
    uri_parts.scheme = req.scheme().map(scheme_wasi_to_http).transpose()?;
    uri_parts.authority = req
        .authority()
        .map(|aut| http::uri::Authority::from_maybe_shared(aut.into_bytes()))
        .transpose()
        .map_err(http::Error::from)?;
    uri_parts.path_and_query = req
        .path_with_query()
        .map(|paq| http::uri::PathAndQuery::from_maybe_shared(paq.into_bytes()))
        .transpose()
        .map_err(http::Error::from)?;

    builder
        .method(req_method)
        .uri(Uri::from_parts(uri_parts).map_err(http::Error::from)?)
        .body(axum::body::Body::from(axum::body::Bytes::from(body_bytes)))
        .map_err(RequestError::from)
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RequestError {
    #[error("failed to convert wasi bindings to http types")]
    Http(#[from] http::Error),

    #[error("error while processing wasi:http body stream")]
    WasiIo(#[from] StreamError),
}

pub fn method_wasi_to_http(value: Method) -> Result<http::Method, http::Error> {
    match value {
        Method::Connect => Ok(http::Method::CONNECT),
        Method::Delete => Ok(http::Method::DELETE),
        Method::Get => Ok(http::Method::GET),
        Method::Head => Ok(http::Method::HEAD),
        Method::Options => Ok(http::Method::OPTIONS),
        Method::Patch => Ok(http::Method::PATCH),
        Method::Post => Ok(http::Method::POST),
        Method::Put => Ok(http::Method::PUT),
        Method::Trace => Ok(http::Method::TRACE),
        Method::Other(mtd) => http::Method::from_bytes(mtd.as_bytes()).map_err(http::Error::from),
    }
}

pub fn scheme_wasi_to_http(value: Scheme) -> Result<http::uri::Scheme, http::Error> {
    match value {
        Scheme::Http => Ok(http::uri::Scheme::HTTP),
        Scheme::Https => Ok(http::uri::Scheme::HTTPS),
        Scheme::Other(oth) => {
            http::uri::Scheme::try_from(oth.as_bytes()).map_err(http::Error::from)
        }
    }
}

pub async fn try_into_outgoing(
    axum_res: axum::response::Response,
    out_param: ResponseOutparam,
) -> Res<()> {
    let wasi_res = OutgoingResponse::new({
        let headers = Headers::new();
        for (name, value) in axum_res.headers() {
            headers.append(name.as_str(), value.as_bytes())?;
        }
        headers
    });
    let wasi_body = wasi_res
        .body()
        .map_err(|()| ferr!("unable to take response body"))?;
    wasi_res
        .set_status_code(axum_res.status().as_u16())
        .map_err(|()| ferr!("invalid http status code was returned"))?;

    ResponseOutparam::set(out_param, Ok(wasi_res));

    {
        let output_stream = wasi_body
            .write()
            .map_err(|()| ferr!("unable to open writable stream on body"))?;

        let axum_body = axum_res.into_body();
        let mut body_stream = axum_body.into_data_stream();
        use futures::StreamExt;
        while let Some(buf) = body_stream.next().await {
            let buf = buf.wrap_err("error reading axum response stream")?;
            let chunks = buf.chunks(CHUNK_BYTE_SIZE);
            for chunk in chunks {
                output_stream
                    .blocking_write_and_flush(chunk)
                    .wrap_err("error writing wasi response stream")?;
            }
        }
    }
    OutgoingBody::finish(wasi_body, None).wrap_err("error flushing body")?;
    Ok(())
}
