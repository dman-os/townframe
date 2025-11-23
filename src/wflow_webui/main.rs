mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

#[cfg(feature = "ssr")]
#[tokio::main]
async fn main() -> Res<()> {
    use axum::http;
    use axum::Router;
    use leptos::prelude::*;
    use leptos_axum::{generate_route_list, LeptosRoutes};
    use tower_http::ServiceBuilderExt;
    use wflow_webui::app::*;
    use wflow_webui::server;

    utils_rs::setup_tracing().expect("tracing setup error");

    let conf = get_configuration(None)?;
    let addr = conf.leptos_options.site_addr;
    let leptos_options = conf.leptos_options;
    // Generate the list of routes in your Leptos App
    let routes = generate_route_list(App);

    let server_conf = server::ServerConfig {
        // FIXME: perist key
        cookie_sign_key: axum_extra::extract::cookie::Key::generate(),
        kanidm_url: "https://localhost:8443".into(),
        kanidm_client_id: "isis_web".into(),
        self_base_url: "http://localhost:3000".into(),
    };
    let cx = server::SharedServerCtx::new(server_conf);

    let app = Router::new()
        .leptos_routes_with_context(
            &leptos_options,
            routes,
            {
                let cx = cx.clone();
                move || {
                    provide_context(cx.clone());
                }
            },
            {
                let leptos_options = leptos_options.clone();
                move || shell(leptos_options.clone())
            },
        )
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options)
        .with_state(cx.clone())
        .layer(axum::middleware::from_fn_with_state(
            cx.clone(),
            server::session::middleware,
        ))
        .layer(
            tower::ServiceBuilder::new()
                .sensitive_headers(vec![http::header::AUTHORIZATION, http::header::COOKIE])
                .layer(
                    tower_http::trace::TraceLayer::new_for_http()
                        .on_response(
                            tower_http::trace::DefaultOnResponse::new()
                                .level(tracing::Level::INFO)
                                .latency_unit(tower_http::LatencyUnit::Micros),
                        )
                        .on_failure(
                            tower_http::trace::DefaultOnFailure::new()
                                .level(tracing::Level::ERROR)
                                .latency_unit(tower_http::LatencyUnit::Micros),
                        )
                        .make_span_with(
                            tower_http::trace::DefaultMakeSpan::new().include_headers(true),
                        ),
                ),
        );

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    tracing::info!(%addr, "server going online");
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

#[cfg(not(feature = "ssr"))]
pub fn main() {
    // no client-side main function
    // unless we want this to work with e.g., Trunk for pure client-side testing
    // see lib.rs for hydration function instead
}
