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

wit::export!(Component with_types_in wit);

struct Component;

// use wasmcloud_component::http;
// http::export!(Component);
use wit::exports::wasi::http::incoming_handler::{
    Guest as HttpIncomingGuest, IncomingRequest, ResponseOutparam,
};

impl HttpIncomingGuest for Component {
    #[allow(async_fn_in_trait)]
    fn handle(request: IncomingRequest, res_handle: ResponseOutparam) -> () {
        use wit::wasi::http::types::{Headers, OutgoingResponse};
        app_main(request);
        let headers = Headers::new();
        let res = OutgoingResponse::new(headers);
        ResponseOutparam::set(res_handle, Ok(res))
    }
}

fn app_main(request: IncomingRequest) {
    wit::townframe::btress_api::ctx::init().expect("error on init")
}
