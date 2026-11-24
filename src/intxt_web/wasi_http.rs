//! The `wasi:http/handler@0.3.0` export: GET only, stateless, the request body is never read.

use crate::route;
use crate::source::FixtureSource;

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        generate_all,
        async: ["export:wasi:http/handler@0.3.0#handle"],
    });
}

use wit::wasi::http::types::ErrorCode;
use wit::wasi::http::types::Fields;
use wit::wasi::http::types::Method;
use wit::wasi::http::types::Request;
use wit::wasi::http::types::Response;

struct Component;
wit::export!(Component with_types_in wit);

impl wit::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        let page = match request.get_method() {
            Method::Get => route::serve(&FixtureSource, &path).await,
            _ => route::method_not_allowed(),
        };
        Ok(respond(page))
    }
}

fn respond(page: route::Page) -> Response {
    let headers = Fields::new();
    headers
        .append("content-type", page.content_type.as_bytes())
        .expect("static header");
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response.set_status_code(page.status).expect("valid status");
    wit_bindgen::spawn_local(async move {
        let remaining = body_tx.write_all(page.body.into_bytes()).await;
        assert!(remaining.is_empty(), "response body receiver went away");
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("response trailers receiver disappeared");
    });
    response
}
