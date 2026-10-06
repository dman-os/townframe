use utils_rs::prelude::*;

#[tokio::main]
async fn main() -> Res<()> {
    utils_rs::setup_tracing()?;
    let result = tokio::spawn(daybook_core::test_support::distributed_smoke::run())
        .await
        .unwrap();
    if let Err(error) = &result {
        eprintln!("distributed runtime smoke failed: {error:?}");
    }
    result
}
