mod app;

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        generate_all,
        async: ["export:wasi:http/handler@0.3.0#handle"],
    });
}

use wit::wasi::http::types::{ErrorCode, Fields, Method, Request, Response};

struct Component;
wit::export!(Component with_types_in wit);

impl wit::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let method = request.get_method();
        let path = request.get_path_with_query().unwrap_or_default();
        // Stateless and GET-only: the request body is never read.
        let (status, body) = match (method, app::route(&path)) {
            (Method::Get, Some(html)) => (200, html),
            (Method::Get, None) => (404, app::not_found(&path)),
            _ => (405, app::not_found(&path)),
        };
        Ok(respond(status, body))
    }
}

fn respond(status: u16, body: String) -> Response {
    let headers = Fields::new();
    headers
        .append("content-type", b"text/html; charset=utf-8")
        .expect("static header");
    let (mut body_tx, body_rx) = wit::wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit::wit_future::new(|| Ok(None));
    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response.set_status_code(status).expect("valid status");
    wit_bindgen::spawn_local(async move {
        let remaining = body_tx.write_all(body.into_bytes()).await;
        assert!(remaining.is_empty(), "response body receiver went away");
        drop(body_tx);
        trailers_tx
            .write(Ok(None))
            .await
            .expect("response trailers receiver disappeared");
    });
    response
}
