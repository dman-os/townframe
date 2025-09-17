
use crate::interlude::*;

use eyre::ensure;

#[tokio::test]
async fn sanity_get_root_returns_hello() -> Res<()> {
    let test_name = utils_rs::function_full!();
    let test_cx = crate::test_cx(test_name).await?;
    {
        // Wait for HTTP ready
        // let ok = wait_http_ready(&test_cx.wadm_apps["btress"].app_url, std::time::Duration::from_secs(20)).await;
        // eyre::ensure!(ok, "http server not ready at {url}");

        // Call endpoint
        let http_client = reqwest::Client::new();
        let resp = http_client
            .get(test_cx.wadm_apps["btress"].app_url.clone())
            .send()
            .await.wrap_err("error sending http request")?;
        ensure!(resp.status() == 200, "status was {}", resp.status());
        let body = resp.text().await.wrap_err("error reading http body")?;
        ensure!(body == "hello", "unexpected body: {body:?}");
    }
    test_cx.close().await;

    Ok(())
}
