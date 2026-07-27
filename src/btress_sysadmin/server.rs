use crate::interlude::*;

use axum_extra::extract::cookie::Key;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

mod interop;
pub mod session;

pub async fn server_main(wasi_req: IncomingRequest, out_param: ResponseOutparam) -> Res<()> {
    use crate::app::*;
    use axum::http;
    use axum::Router;
    use leptos::prelude::*;
    use leptos_axum::{generate_route_list, LeptosRoutes};
    use tower_http::ServiceBuilderExt;

    utils_rs::setup_tracing().expect("tracing setup error");

    // leptos_wasi::handler::Handler::build(wasi_req, response_out)
    //     .unwrap()
    //     // .with_server_fn::<GetCount>()
    //     .generate_routes(App)
    //     .handle_with_context(move || shell(leptos_options.clone()), || {})
    //     .await
    //     .unwrap();
    //
    let conf = leptos::config::get_config_from_env()?;
    let leptos_options = conf.leptos_options;
    info!("generating route list");
    // Generate the list of routes in your Leptos App
    let routes = generate_route_list(App);

    let server_conf = ServerConfig {
        // FIXME: perist key
        cookie_sign_key: load_cookie_sign_key(),
        kratos_public_url: std::env::var("KRATOS_PUBLIC_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:4433".into())
            .trim_matches('"')
            .to_string(),
        self_base_url: std::env::var("SELF_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3000".into())
            .trim_matches('"')
            .to_string(),
    };
    let cx = SharedServerCtx::new(server_conf);

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
        // .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options)
        .with_state(cx.clone())
        .layer(axum::middleware::from_fn_with_state(
            cx.clone(),
            session::middleware,
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

    info!("converting request");
    let axum_req = interop::try_from_incoming(wasi_req).wrap_err("error converting to axum req")?;

    info!("processing request");
    use tower::ServiceExt;
    let axum_res: axum::response::Response = app.oneshot(axum_req).await.expect("infallible");

    interop::try_into_outgoing(axum_res, out_param)
        .await
        .wrap_err("error writing response")?;

    Ok(())
}

fn load_cookie_sign_key() -> axum_extra::extract::cookie::Key {
    if let Ok(raw) = std::env::var("ISIS_COOKIE_SIGN_KEY") {
        let bytes = raw.as_bytes();
        if bytes.len() < 64 {
            panic!("ISIS_COOKIE_SIGN_KEY must be at least 64 bytes");
        }
        return axum_extra::extract::cookie::Key::from(bytes);
    }

    // Stable dev fallback so auth sessions survive hot-reload/server restarts.
    axum_extra::extract::cookie::Key::from(
        b"isis-dev-cookie-sign-key-2026-keep-this-64-bytes-minimum-entropy!!!!!",
    )
}

#[derive(Debug)]
pub struct ServerConfig {
    pub cookie_sign_key: Key,
    pub kratos_public_url: String,
    pub self_base_url: String,
}

pub struct ServerCtx {
    pub config: ServerConfig,
    session_store: Arc<session::Store>,
}

#[derive(Clone)]
pub struct SharedServerCtx(Arc<ServerCtx>);

impl std::ops::Deref for SharedServerCtx {
    type Target = ServerCtx;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl axum::extract::FromRef<SharedServerCtx> for Key {
    fn from_ref(input: &SharedServerCtx) -> Self {
        input.config.cookie_sign_key.clone()
    }
}

impl SharedServerCtx {
    pub fn new(config: ServerConfig) -> SharedServerCtx {
        SharedServerCtx(Arc::new(ServerCtx {
            config,
            session_store: Arc::new(session::Store { kv: default() }),
        }))
    }

    pub async fn session(&self) -> session::Session {
        let session = leptos_axum::extract::<axum::extract::Extension<session::Session>>()
            .await
            .expect_or_log("session not in extension");
        session.0
    }

    pub async fn cookie_jar(&self) -> Arc<session::CookieJar> {
        let cookie_jar =
            leptos_axum::extract::<axum::extract::Extension<std::sync::Weak<session::CookieJar>>>()
                .await
                .expect_or_log("cookie jar not in extension");
        cookie_jar.0.upgrade().expect_or_log("cookie jar is gone")
    }
}
