mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use axum::extract::Json;
    pub use axum::response::IntoResponse;
    pub use axum::response::Response;
    pub use http::StatusCode;
}

mod wit {
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
            "townframe:wflow/types": generate,
            "townframe:wflow/metastore": generate,
            "townframe:wflow/partition-host": generate,
        }
    });
}
mod request;

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::ingress;
use crate::wit::townframe::wflow::types::JobId;
use crate::wit::townframe::wflow::{metastore, partition_host};

/// When working with streams, this crate will try to chunk bytes with
/// this size.
const CHUNK_BYTE_SIZE: usize = 64;

wit::export!(Component with_types_in wit);

struct Component;

use axum::extract;
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
            .expect(ERROR_TOKIO);
        rt.block_on(handle_http(wasi_req, res_handle))
            .expect("error handling request");
    }
}

async fn handle_http(wasi_req: IncomingRequest, out_param: ResponseOutparam) -> Res<()> {
    let router = main_router();

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

fn main_router() -> axum::Router {
    axum::Router::new()
        .route("/invoke/{key}", axum::routing::post(invoke_route))
        .route("/", axum::routing::get(|| async { "hello" }))
}

async fn invoke_route(
    extract::Path(wflow_key): extract::Path<String>,
    headers: http::HeaderMap,
    extract::Json(args): Json<serde_json::Value>,
) -> Response {
    let idem_key = match headers.get("idempotency-key") {
        Some(val) => match val.to_str() {
            Ok(val) => Some(val.to_owned()),
            Err(_) => return (StatusCode::BAD_REQUEST, "non utf-8 idempotenc-key").into_response(),
        },
        None => None,
    };
    match <Component as ingress::Guest>::invoke(ingress::InvokeArgs {
        wflow_key,
        args: serde_json::to_string(&args).expect(ERROR_JSON),
        idem_key,
    }) {
        Ok(job_id) => (
            StatusCode::CREATED,
            Json(json!({
                "jobId": job_id
            })),
        )
            .into_response(),
        Err(ingress::InvokeError::WflowNotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "wflow_not_found",
            })),
        )
            .into_response(),
    }
}

impl ingress::Guest for Component {
    #[allow(async_fn_in_trait)]
    fn invoke(args: ingress::InvokeArgs) -> Result<JobId, ingress::InvokeError> {
        let meta = match metastore::get_wflow(&args.wflow_key) {
            None => Err(ingress::InvokeError::WflowNotFound),
            Some(meta) => Ok(meta),
        }?;

        // FIXME: use v7 uuid
        let job_id = Uuid::new_v4();
        let job_id = job_id.to_string();

        let _parts_meta = metastore::get_partitions();
        partition_host::add_job(
            // TODO: request hash based assignation of job to partition
            0,
            &partition_host::AddJobArgs {
                id: job_id.clone(),
                wflow: meta,
            },
        );
        Ok(job_id)
    }
}
