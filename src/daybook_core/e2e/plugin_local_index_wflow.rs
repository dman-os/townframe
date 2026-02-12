use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, FacetKey, WellKnownFacet, WellKnownFacetTag};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqlitePool};
use std::str::FromStr;

#[tokio::test(flavor = "multi_thread")]
async fn test_embedding_processor_indexes_into_plugin_local_sqlite_state() -> Res<()> {
    let test_context = crate::e2e::test_cx(utils_rs::function_full!()).await?;
    let note_facet_key = FacetKey::from(WellKnownFacetTag::Note);
    let note_facet_ref = daybook_types::url::build_facet_ref(
        daybook_types::url::FACET_SELF_DOC_ID,
        &note_facet_key,
    )?;
    let zero_vector = vec![0u8; 768 * std::mem::size_of::<f32>()];
    let embedding_facet: daybook_types::doc::FacetRaw =
        WellKnownFacet::Embedding(daybook_types::doc::Embedding {
            facet_ref: note_facet_ref,
            ref_heads: daybook_types::doc::ChangeHashSet(Vec::new().into()),
            model_tag: "nomic-ai/nomic-embed-text-v1.5".to_string(),
            vector: zero_vector,
            dim: 768,
            dtype: daybook_types::doc::EmbeddingDtype::F32,
            compression: None,
        })
        .into();

    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        facets: [
            (
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note(daybook_types::doc::Note {
                    mime: "text/plain".to_string(),
                    content: "plugin local index vector smoke test".to_string(),
                })
                .into(),
            ),
            (
                FacetKey::from(WellKnownFacetTag::Embedding),
                embedding_facet,
            ),
        ]
        .into(),
        user_path: None,
    };

    let doc_id = test_context.drawer_repo.add(new_doc).await?;
    let (_doc, heads) = test_context
        .drawer_repo
        .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("doc not found after add")?;

    let index_dispatch_id = test_context
        .rt
        .dispatch(
            "@daybook/wip",
            "index-embedding",
            crate::rt::DispatchArgs::DocFacet {
                doc_id: doc_id.clone(),
                branch_path: daybook_types::doc::BranchPath::from("main"),
                heads,
                facet_key: None,
            },
        )
        .await?;
    test_context
        .rt
        .wait_for_dispatch_end(&index_dispatch_id, std::time::Duration::from_secs(120))
        .await?;

    let sqlite_file_path = test_context
        .rt
        .sqlite_local_state_repo
        .get_sqlite_file_path("@daybook/wip/doc-embedding-index")
        .await?;
    let db_url = format!("sqlite:{}?mode=rwc", sqlite_file_path.display());
    let connect_options = SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true)
        .disable_statement_logging();
    let db_pool = SqlitePool::connect_with(connect_options).await?;

    let mut found_index_record = false;
    for _ in 0..120 {
        let row_count_result = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM doc_embedding_meta WHERE doc_id = ?1 AND facet_key = ?2",
        )
        .bind(&doc_id)
        .bind(FacetKey::from(WellKnownFacetTag::Embedding).to_string())
        .fetch_one(&db_pool)
        .await;

        match row_count_result {
            Ok(row_count) if row_count > 0 => {
                found_index_record = true;
                break;
            }
            Ok(_) => {}
            Err(sql_error) => {
                let message = sql_error.to_string();
                if !message.contains("no such table") {
                    return Err(sql_error.into());
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    assert!(
        found_index_record,
        "expected plugin local sqlite index record to be written for embedded doc"
    );

    test_context.stop().await?;
    Ok(())
}
