use crate::interlude::*;

pub mod session;

use axum_extra::extract::cookie::Key;

#[derive(Debug)]
pub struct ServerConfig {
    pub cookie_sign_key: Key,
    pub kanidm_url: String,
    pub kanidm_client_id: String,
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
        let cookie_jar = leptos_axum::extract::<axum::extract::Extension<std::sync::Weak<session::CookieJar>>>()
            .await
            .expect_or_log("cookie jar not in extension");
        cookie_jar.0.upgrade().expect_or_log("cookie jar is gone")
    }
}
