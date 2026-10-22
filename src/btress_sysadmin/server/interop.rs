use crate::interlude::*;
use crate::wit::wasi::http::types::Fields;
use crate::wit::wasi::http::types::Method;
use crate::wit::wasi::http::types::Request;
use crate::wit::wasi::http::types::Response;
use crate::wit::wasi::http::types::Scheme;
use http::Uri;
use http::uri::Parts;

pub async fn try_from_incoming(
    req: Request,
) -> Result<http::Request<axum::body::Body>, RequestError> {
    use futures_util::StreamExt;

    let mut builder = http::Request::builder();
    let req_method = method_wasi_to_http(req.get_method())?;
    let headers = req.get_headers();

    for (header_name, header_value) in headers.copy_all() {
        builder = builder.header(header_name, header_value);
    }

    drop(headers);

    let mut uri_parts = Parts::default();
    uri_parts.scheme = req.get_scheme().map(scheme_wasi_to_http).transpose()?;
    uri_parts.authority = req
        .get_authority()
        .map(|authority| http::uri::Authority::from_maybe_shared(authority.into_bytes()))
        .transpose()
        .map_err(http::Error::from)?;
    uri_parts.path_and_query = req
        .get_path_with_query()
        .map(|path_and_query| {
            http::uri::PathAndQuery::from_maybe_shared(path_and_query.into_bytes())
        })
        .transpose()
        .map_err(http::Error::from)?;
    let (body_result_tx, body_result_rx) = crate::wit::wit_future::new(|| Ok(()));
    let (body, _trailers) = Request::consume_body(req, body_result_rx);
    let mut body_stream = body.into_stream();
    let mut body_bytes = Vec::new();
    while let Some(byte) = body_stream.next().await {
        body_bytes.push(byte);
    }
    body_result_tx
        .write(Ok(()))
        .await
        .expect("request body receiver disappeared");

    let request = builder
        .method(req_method)
        .uri(Uri::from_parts(uri_parts).map_err(http::Error::from)?)
        .body(axum::body::Body::from(axum::body::Bytes::from(body_bytes)))
        .map_err(RequestError::from)?;
    Ok(request)
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RequestError {
    #[error("failed to convert wasi bindings to http types")]
    Http(#[from] http::Error),
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
        Method::Other(method) => Ok(http::Method::from_bytes(method.as_bytes())?),
    }
}

pub fn scheme_wasi_to_http(value: Scheme) -> Result<http::uri::Scheme, http::Error> {
    match value {
        Scheme::Http => Ok(http::uri::Scheme::HTTP),
        Scheme::Https => Ok(http::uri::Scheme::HTTPS),
        Scheme::Other(other) => Ok(http::uri::Scheme::try_from(other.as_bytes())?),
    }
}

pub async fn try_into_outgoing(axum_res: axum::response::Response) -> Res<Response> {
    use futures_util::StreamExt;

    let headers = Fields::new();
    for (name, value) in axum_res.headers() {
        headers
            .append(name.as_str(), value.as_bytes())
            .map_err(|_| ferr!("invalid response header"))?;
    }

    let (mut body_tx, body_rx) = crate::wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = crate::wit::wit_future::new(|| Ok(None));
    let (wasi_res, _response_result) = Response::new(headers, Some(body_rx), trailers_rx);
    wasi_res
        .set_status_code(axum_res.status().as_u16())
        .map_err(|_| ferr!("invalid http status code was returned"))?;

    let mut body_stream = axum_res.into_body().into_data_stream();
    wit_bindgen::spawn_local(async move {
        let result: Result<(), String> = async {
            while let Some(buf) = body_stream.next().await {
                let buf = buf.map_err(|error| error.to_string())?;
                let remaining = body_tx.write_all(buf.to_vec()).await;
                if !remaining.is_empty() {
                    return Err("response body stream was closed".to_string());
                }
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => trailers_tx
                .write(Ok(None))
                .await
                .expect("response trailers receiver disappeared"),
            Err(error) => trailers_tx
                .write(Err(
                    crate::wit::wasi::http::types::ErrorCode::InternalError(Some(
                        error.to_string(),
                    )),
                ))
                .await
                .expect("response trailers receiver disappeared"),
        }
    });

    Ok(wasi_res)
}
