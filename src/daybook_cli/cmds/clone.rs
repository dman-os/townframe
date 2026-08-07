use crate::interlude::*;

pub async fn run(source_url: String, destination: String) -> Res<ExitCode> {
    let destination = std::path::PathBuf::from(destination);
    let res = daybook_core::sync::clone_repo_init_from_url(
        &source_url,
        &destination,
        daybook_core::sync::CloneRepoInitOptions {
            timeout: std::time::Duration::from_secs(30),
            repo_options: daybook_core::repo::RepoOpenOptions::default(),
        },
    )
    .await?;
    println!(
        "clone initialization completed at {}",
        res.repo_path.display()
    );
    println!(
        "required clone partitions synced (repo_id={}, repo_name={})",
        res.bootstrap.repo_id, res.bootstrap.repo_name
    );
    println!("full sync can continue in future sync sessions (run: daybook sync <ticket>)");
    Ok(ExitCode::SUCCESS)
}
