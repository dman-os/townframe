mod interlude {
    pub use api_utils_rs::{api, prelude::*};

    pub(crate) use crate::method_router;

    pub use axum::extract::Json;
    pub use axum::response::IntoResponse;
    pub use axum::response::Response;
    pub use http::Method;
    pub use http::StatusCode;
}

mod wit {
    // pub mod serde {
    //     wit_bindgen::generate!({
    //         world: "imports",
    //         generate_all,
    //         additional_derives: [serde::Serialize, serde::Deserialize],
    //         // async: true,
    //     });
    // }
    wit_bindgen::generate!({
        world: "server",
        // generate_all,
        // async: true,
        with: {
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,

            "wasi:io/poll@0.2.6": generate,
            "wasi:io/error@0.2.6": generate,
            "wasi:io/streams@0.2.6": generate,
            "wasi:http/types@0.2.6": generate,

            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:daybook-api/ctx": generate,
            "townframe:daybook-api/doc": crate::gen::doc,
            "townframe:daybook-api/doc/doc": crate::gen::doc::Doc,
            "townframe:daybook-api/doc/doc-kind": generate,
            "townframe:daybook-api/doc/doc-tag": crate::gen::doc::DocTag,
            "townframe:daybook-api/doc/doc-blob": crate::gen::doc::DocBlob,
            "townframe:daybook-api/doc/doc-content": crate::gen::doc::DocContent,
            // "townframe:daybook-api/doc/doc-kind": crate::gen::doc::DocKind,
            "townframe:daybook-api/doc/doc-image": crate::gen::doc::DocImage,

            "townframe:daybook-api/doc-create/input": crate::gen::doc::doc_create::Input,
            "townframe:daybook-api/doc-create/error-id-occupied": crate::gen::doc::doc_create::ErrorIdOccupied,
            // "townframe:daybook-api/doc/doc-tag-kind": crate::gen::doc::DocTagKind,
            "townframe:daybook-api/doc/doc-tag-kind": generate,
            "townframe:daybook-api/doc-create": generate,
            "townframe:daybook-api/doc-create/error": crate::gen::doc::doc_create::Error,
        }
    });
}

mod doc;
mod gen;
mod request;

use crate::interlude::*;

/// When working with streams, this crate will try to chunk bytes with
/// this size.
const CHUNK_BYTE_SIZE: usize = 64;

wit::export!(Component with_types_in wit);

struct Component;

// use wasmcloud_component::http;
// http::export!(Component);
use wit::exports::wasi::http::incoming_handler::{
    Guest as HttpIncomingGuest, IncomingRequest, ResponseOutparam,
};
use wit::wasi::http::types::{Headers, OutgoingBody, OutgoingResponse};

impl HttpIncomingGuest for Component {
    #[allow(async_fn_in_trait)]
    fn handle(wasi_req: IncomingRequest, res_handle: ResponseOutparam) -> () {
        utils_rs::setup_tracing().expect("error setting up tracing");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect_or_log(ERROR_TOKIO);
        rt.block_on(app_main(wasi_req, res_handle))
            .expect_or_log("error handling request");
    }
}

async fn app_main(wasi_req: IncomingRequest, out_param: ResponseOutparam) -> Res<()> {
    wit::townframe::daybook_api::ctx::init()
        .map_err(|err| ferr!("error on daybook_api init: {err}"))?;

    let router = axum::Router::new()
        .merge(doc::router())
        .route("/", axum::routing::get(|| async { "hello" }));

    let axum_req: http::Request<axum::body::Body> = crate::request::Request(wasi_req)
        .try_into()
        .wrap_err("error converting to axum req")?;

    use tower::ServiceExt;
    let axum_res: axum::response::Response = router.oneshot(axum_req).await.expect("infallible");

    let wasi_res = OutgoingResponse::new({
        let headers = Headers::new();
        for (name, value) in axum_res.headers() {
            headers.append(&name.to_string(), &Vec::from(value.as_bytes()))?;
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

    eyre::Ok(())
}

fn method_router<H, T, S>(
    method: Method,
    handler: H,
) -> axum::routing::MethodRouter<S, std::convert::Infallible>
where
    H: axum::handler::Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    (match method {
        Method::HEAD => axum::routing::head,
        Method::OPTIONS => axum::routing::options,
        Method::TRACE => axum::routing::trace,

        Method::GET => axum::routing::get,
        Method::CONNECT => axum::routing::connect,
        Method::POST => axum::routing::post,
        Method::PUT => axum::routing::put,
        Method::PATCH => axum::routing::patch,
        Method::DELETE => axum::routing::delete,
        method => panic!("unknown http method {method}"),
    })(handler)
}
