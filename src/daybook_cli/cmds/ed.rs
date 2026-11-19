use crate::interlude::*;
pub async fn run(id: String, branch: Option<String>) -> Res<ExitCode> {
    let drawer_repo = lazy::drawer_repo().await?;
    let Ok(Some(branches)) = drawer_repo.get_doc_branches(&id).await else {
        error!("document not found: {id}");
        return Ok(ExitCode::FAILURE);
    };
    let branch_path = match &branch {
        Some(val) => {
            if branches.branches.contains_key(val) {
                error!("branch not found for doc: {id} - {val}");
                return Ok(ExitCode::FAILURE);
            }
            daybook_types::doc::BranchPathBuf::from(val.as_str())
        }
        None => {
            let Some(branch) = branches.main_branch_path() else {
                error!(doc_id = ?branches.doc_id,"no branches found on doc");
                return Ok(ExitCode::FAILURE);
            };
            branch
        }
    };
    let Some((doc, heads)) = drawer_repo.get_with_heads(&id, &branch_path, None).await? else {
        eyre::bail!("Document not found: {id}");
    };

    let content = serde_json::to_string_pretty(&*doc)?;

    // Create temporary file
    // TODO: replace with tempfile crate usage
    let tmp_dir = std::env::temp_dir();
    let tmp_path = tmp_dir.join(format!("daybook-edit-{}.json", id));
    tokio::fs::write(&tmp_path, &content).await?;

    // Open editor
    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
    let status = std::process::Command::new(editor).arg(&tmp_path).status()?;

    if !status.success() {
        eyre::bail!("Editor exited with failure");
    }

    // Read back and compare
    let new_content = tokio::fs::read_to_string(&tmp_path).await?;
    let new_doc: daybook_types::doc::Doc =
        serde_json::from_str(&new_content).wrap_err("Failed to parse modified document as JSON")?;

    let mut patch = daybook_types::doc::Doc::diff(&doc, &new_doc);
    if patch.is_empty() {
        println!("No changes detected.");
    } else {
        patch.id = id.clone();
        drawer_repo
            .update_at_heads(patch, "main".into(), Some(heads))
            .await?;
        println!("Updated document: {id}");
    }

    // Cleanup
    tokio::fs::remove_file(&tmp_path).await?;
    Ok(ExitCode::SUCCESS)
}
