use crate::interlude::*;

pub async fn run() -> Res<ExitCode> {
    let drawer_repo = lazy::drawer_repo().await?;
    let doc_entries = drawer_repo.list().await?;
    let mut docs = Vec::new();
    for entry in &doc_entries {
        let Some(main_branch) = entry.main_branch_path() else {
            warn!(doc_id = ?entry.doc_id,"no branches found on doc");
            continue;
        };
        if let Some(doc) = drawer_repo
            .get_doc_with_facets_at_branch(&entry.doc_id, &main_branch, None)
            .await?
        {
            docs.push((entry.clone(), doc));
        }
    }

    use comfy_table::presets::NOTHING;
    use comfy_table::Table;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["ID", "Title", "Branches"]);

    for (entry, doc) in docs {
        let title = doc
            .facets
            .get(&WellKnownFacetTag::TitleGeneric.into())
            .map(|val| {
                match WellKnownFacet::from_json(val.clone(), WellKnownFacetTag::TitleGeneric) {
                    Ok(WellKnownFacet::TitleGeneric(str)) => str.clone(),
                    _ => panic!("tag - facet mismatch"),
                }
            })
            .unwrap_or_else(|| "<no title>".to_string());
        table.add_row(vec![
            entry.doc_id,
            title,
            entry
                .branches
                .keys()
                .map(|key| key.as_str())
                .collect::<Vec<_>>()
                .join(","),
        ]);
    }
    println!("{table}");
    Ok(ExitCode::SUCCESS)
}
