use crate::interlude::*;

pub async fn run(id: String, branch: Option<String>) -> Res<ExitCode> {
    let drawer_repo = lazy::drawer_repo().await?;
    let Ok(Some(branches)) = drawer_repo.get_doc_branches(&id).await else {
        error!("document not found: {id}");
        return Ok(ExitCode::FAILURE);
    };
    let branch_path = match &branch {
        Some(val) => {
            if !branches.branches.contains_key(val) {
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
    let doc = drawer_repo
        .get_doc_with_facets_at_branch(&id, &branch_path, None)
        .await?
        .expect("document from entry missing");
    use std::io::{ErrorKind, Write};
    let mut output = std::io::stdout().lock();
    // A reader such as grep -q may finish before the document ends. Pipe closure
    // is normal consumer completion, not a panic that bypasses repository shutdown.
    match serde_json::to_writer_pretty(&mut output, &*doc) {
        Ok(()) => {}
        Err(error) if error.io_error_kind() == Some(ErrorKind::BrokenPipe) => {
            return Ok(ExitCode::SUCCESS);
        }
        Err(error) => return Err(error.into()),
    }
    if let Err(error) = writeln!(output)
        && error.kind() != ErrorKind::BrokenPipe
    {
        return Err(error.into());
    }
    Ok(ExitCode::SUCCESS)
}
