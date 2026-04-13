use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_base() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    let bootstrap_ba = node_b.sync_repo.connect_url(&ticket_a).await?;

    Ok(())
}
