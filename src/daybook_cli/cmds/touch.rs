use crate::interlude::*;
pub async fn run() -> Res<ExitCode> {
    let cx = lazy::repo_ctx().await?;
    let drawer_repo = lazy::drawer_repo().await?;
    let doc = daybook_types::doc::AddDocArgs {
        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
        facets: [
            //
            (
                daybook_types::doc::WellKnownFacetTag::TitleGeneric.into(),
                daybook_types::doc::WellKnownFacet::TitleGeneric("Untitled".into()).into(),
            ),
        ]
        .into(),
        user_path: Some(daybook_types::doc::UserPathBuf::from(
            cx.local_user_path.clone(),
        )),
    };
    let id = drawer_repo.add(doc).await?;
    info!(id, "created document");
    println!("{id}");
    Ok(ExitCode::SUCCESS)
}
