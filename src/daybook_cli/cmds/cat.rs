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
    println!("{:#?}", &doc);
    println!("{}", serde_json::to_string_pretty(&*doc)?);
    Ok(ExitCode::SUCCESS)
}
