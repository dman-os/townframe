mod interlude {
    pub use utils_rs::prelude::*;
}

mod wit {
    wit_bindgen::generate!({
        world: "server",
        // generate_all,
        // async: true,
        // additional_derives: [serde::Serialize, serde::Deserialize],
        with: {
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:io/error@0.2.6": generate,
            "wasi:io/streams@0.2.6": generate,
            "wasi:http/types@0.2.6": generate,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:btress-api/ctx": generate,
            "townframe:btress-api/user": generate,
            "townframe:btress-api/user-create": generate,
        }
    });
}

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
    wit::townframe::btress_api::ctx::init()
        .map_err(|err| ferr!("error on btress_api init: {err}"))?;

    let router = axum::Router::new().route("/", axum::routing::get(|| async { "hello" }));

    let axum_req = crate::request::Request(wasi_req)
        .try_into()
        .wrap_err("error converting to axum req")?;

    use tower::ServiceExt;
    let axum_res = router.oneshot(axum_req).await.expect("infallible");

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
