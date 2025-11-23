use crate::interlude::*;

pub use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::SignedCookieJar;

use tokio::sync::RwLock;

pub struct CookieJar {
    old_jar: SignedCookieJar,
    new_jar: RwLock<std::collections::HashMap<String, Cookie<'static>>>,
}

impl CookieJar {
    pub fn load(jar: SignedCookieJar) -> Arc<Self> {
        let new_jar = jar
            .iter()
            .map(|cookie| (cookie.name().to_string(), cookie))
            .collect();
        let new_jar = RwLock::new(new_jar);
        Arc::new(Self {
            new_jar,
            old_jar: jar,
        })
    }
    pub async fn add(&self, cookie: Cookie<'static>) {
        self.new_jar
            .write()
            .await
            .insert(cookie.name().to_string(), cookie);
    }
    pub async fn get(&self, name: &str) -> Option<Cookie<'static>> {
        if let Some(cookie) = self.old_jar.get(name) {
            return Some(cookie.clone());
        }
        self.new_jar.read().await.get(name).map(|c| c.clone())
    }

    async fn signed(self: Arc<Self>) -> SignedCookieJar {
        match Arc::try_unwrap(self) {
            Ok(self2) => {
                let mut jar = self2.old_jar;
                for (_, cookie) in self2.new_jar.write().await.drain() {
                    jar = jar.add(cookie);
                }
                jar
            }
            Err(self2) => {
                warn!("someone is still holding on to the cookie jar, leak!");
                let mut jar = SignedCookieJar::clone(&self2.old_jar);
                for (_, cookie) in self2.new_jar.write().await.drain() {
                    jar = jar.add(cookie);
                }
                jar
            }
        }
    }
}

pub struct Store {
    pub kv: DHashMap<String, String>,
}

struct State {
    needs_update: bool,
    data: CookieData,
}

#[derive(Serialize, Deserialize)]
struct CookieData {
    sid: Uuid,
    user_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct Session {
    id: Uuid,
    cx: super::SharedServerCtx,
    state: Arc<RwLock<State>>,
}

// the kv methods
impl Session {
    pub async fn kv_set(&self, key: &str, value: String) -> Res<()> {
        let id = self.id;
        let key = format!("isis:skv:{id}:{key}");
        self.cx.session_store.kv.insert(key, value);
        Ok(())
    }
    pub async fn kv_get(&self, key: &str) -> Res<Option<String>> {
        let id = self.id;
        let key = format!("isis:skv:{id}:{key}");
        let val = self.cx.session_store.kv.get(&key).map(|val| val.clone());
        Ok(val)
    }
}

/// the private methods
impl Session {
    const SESSION_COOKIE: &str = "ISIS_SESSION";

    async fn sign_in(&self, user_id: Uuid) {
        let mut state = self.state.write().await;
        state.needs_update = true;
        state.data.user_id = Some(user_id);
    }

    async fn init(cx: super::SharedServerCtx, jar: &CookieJar) -> Self {
        let state = if let Some(cookie) = jar.get(Self::SESSION_COOKIE).await {
            let val = cookie.value();
            let data = serde_json::from_str::<CookieData>(val);
            match data {
                Ok(data) => State {
                    needs_update: false,
                    data,
                },
                Err(err) => {
                    warn!("error parsing cookie data: {err} `{val}`");
                    State {
                        needs_update: true,
                        data: CookieData {
                            sid: Uuid::new_v4(),
                            user_id: None,
                        },
                    }
                }
            }
        } else {
            State {
                needs_update: true,
                data: CookieData {
                    sid: Uuid::new_v4(),
                    user_id: None,
                },
            }
        };
        Self {
            id: state.data.sid,
            cx,
            state: Arc::new(state.into()),
        }
    }

    async fn update_cookies(&self, jar: &CookieJar) {
        let state = self.state.read().await;
        if state.needs_update {
            jar.add(
                Cookie::build((
                    Self::SESSION_COOKIE,
                    serde_json::to_string(&state.data).expect_or_log("json error"),
                ))
                .same_site(axum_extra::extract::cookie::SameSite::Lax)
                .http_only(true)
                // TODO: session expiry
                .secure(!cfg!(debug_assertions))
                .build(),
            )
            .await;
        }
    }
}

pub async fn middleware(
    axum::extract::State(cx): axum::extract::State<SharedServerCtx>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let jar = SignedCookieJar::from_headers(req.headers(), cx.config.cookie_sign_key.clone());
    let jar = CookieJar::load(jar);
    let session = Session::init(cx, &jar).await;
    {
        let extensions = req.extensions_mut();
        extensions.insert(session.clone());
        extensions.insert(Arc::downgrade(&jar));
    }
    let res = next.run(req).await;
    session.update_cookies(&jar).await;
    let jar = jar.signed().await;
    (jar, res).into_response()
}
