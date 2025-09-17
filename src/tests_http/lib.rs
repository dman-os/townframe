mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

mod macros;
mod pg;
mod wasmcloud;

#[cfg(test)]
mod sanity_http;

// Common helpers for tests

use std::{collections::HashMap, time::Duration};

pub async fn wait_http_ready(url: &str, timeout: Duration) -> bool {
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        match client.get(url).send().await {
            Ok(resp) => {
                if resp.status().is_success() || resp.status().is_redirection() {
                    return true;
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

pub struct TestContext {
    pub test_name: String,
    pub pg_pools: HashMap<String, pg::TestPg>,
    pub redis_pools: HashMap<String, ()>,
    pub wadm_apps: HashMap<String, wasmcloud::TestApp>,
}

impl TestContext {
    pub fn new(
        test_name: String,
        pools: impl Into<HashMap<String, pg::TestPg>>,
        redis_pools: impl Into<HashMap<String, ()>>,
        wadm_apps: impl Into<HashMap<String, wasmcloud::TestApp>>,
        // redis_pools: impl Into<HashMap<String, TestRedis>>,
    ) -> Self {
        Self {
            test_name,
            pg_pools: pools.into(),
            redis_pools: redis_pools.into(),
            wadm_apps: wadm_apps.into(),
        }
    }

    /// Call this after all holders of the [`SharedContext`] have been dropped.
    pub async fn close(mut self) {
        for (_, _db) in self.pg_pools.drain() {
            // db.close().await;
        }
        for (_, app) in self.wadm_apps.drain() {
            if let Err(err) = app.close().await {
                tracing::error!("error closing app: {err:?}");
            }
            // db.close().await;
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        for db_name in self.pg_pools.keys() {
            tracing::warn!("test context dropped without cleaning up for db: {db_name}",)
        }
        for app_name in self.wadm_apps.keys() {
            tracing::warn!("test context dropped without cleaning up for app: {app_name}",)
        }
    }
}

#[allow(unused)]
async fn test_cx(test_name: &'static str) -> Res<TestContext> {
    utils_rs::testing::load_envs_once();
    let btress_db = pg::TestPg::new(
        test_name,
        std::path::Path::new(&utils_rs::get_env_var("BTRESS_API_ROOT_PATH").unwrap()),
    )
    .await?;
    let btress_http = wasmcloud::TestApp::new(test_name).await?;
    let testing = TestContext::new(
        test_name.into(),
        [("btress".to_string(), btress_db)],
        [],
        [("btress".to_string(), btress_http)],
    );
    Ok(testing)
}

/* pub struct TestRedis {
    pub pool: RedisPool,
}

impl TestRedis {
    pub async fn new() -> Self {
        Self {
            pool: RedisPool(
                bb8_redis::bb8::Pool::builder()
                    .build(
                        bb8_redis::RedisConnectionManager::new(
                            crate::utils::get_env_var("TEST_REDIS_URL")
                                .unwrap_or_log()
                                .as_str(),
                        )
                        .unwrap_or_log(),
                    )
                    .await
                    .unwrap_or_log(),
            ),
        }
    }
}
 */

// pub struct ExtraAssertionAgs<'a> {
//     pub test_cx: &'a mut TestContext,
//     pub auth_token: Option<String>,
//     pub response_head: http::response::Parts,
//     pub response_json: Option<serde_json::Value>,
// }

// pub type EAArgs<'a> = ExtraAssertionAgs<'a>;

// /// BoxFuture type that's not send
// pub type LocalBoxFuture<'a, T> = std::pin::Pin<Box<dyn futures::Future<Output = T> + 'a>>;

// pub type ExtraAssertions<'c, 'f> = dyn Fn(ExtraAssertionAgs<'c>) -> LocalBoxFuture<'f, ()>;
