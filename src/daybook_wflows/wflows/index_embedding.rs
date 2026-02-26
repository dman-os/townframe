use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::sql::types::SqlValue;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();
    let embedding_facet_token = args
        .ro_facet_tokens
        .iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "embedding facet key '{}' not found in ro_facet_tokens",
                args.facet_key
            ))
        })?;
    let sqlite_connection = args
        .sqlite_connections
        .iter()
        .find(|(key, _)| key == "@daybook/wip/doc-embedding-index")
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "sqlite connection '@daybook/wip/doc-embedding-index' not found"
            ))
        })?;

    let embedding_raw = embedding_facet_token.get();
    let embedding_json: daybook_types::doc::FacetRaw = serde_json::from_str(&embedding_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing embedding facet json: {err}")))?;
    let embedding = match WellKnownFacet::from_json(embedding_json, WellKnownFacetTag::Embedding)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not embedding")))?
    {
        WellKnownFacet::Embedding(value) => value,
        _ => unreachable!("embedding tag must parse as embedding facet"),
    };

    if embedding.dtype != daybook_types::doc::EmbeddingDtype::F32 || embedding.compression.is_some()
    {
        return Ok(());
    }
    if embedding.dim != 768 {
        return Err(JobErrorX::Terminal(ferr!(
            "expected embedding dimension 768, got {}",
            embedding.dim
        )));
    }
    let vector_json =
        daybook_types::doc::embedding_f32_bytes_to_json(&embedding.vector, embedding.dim)
            .map_err(JobErrorX::Terminal)?;
    let serialized_heads = serde_json::to_string(&args.heads).expect(ERROR_JSON);

    cx.effect(|| {
        sqlite_connection
            .query_batch(
                r#"
                CREATE VIRTUAL TABLE IF NOT EXISTS doc_embedding_vec
                USING vec0(embedding float[768]);

                CREATE TABLE IF NOT EXISTS doc_embedding_meta (
                    rowid INTEGER PRIMARY KEY,
                    doc_id TEXT NOT NULL,
                    facet_key TEXT NOT NULL,
                    origin_heads TEXT NOT NULL,
                    UNIQUE(doc_id, facet_key)
                );
                "#,
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error initializing vector index: {err:?}")))?;

        let existing_rows = sqlite_connection
            .query(
                "SELECT rowid FROM doc_embedding_meta WHERE doc_id = ?1 AND facet_key = ?2",
                &[
                    SqlValue::Text(args.doc_id.clone()),
                    SqlValue::Text(args.facet_key.clone()),
                ],
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error selecting vector row: {err:?}")))?;

        let existing_rowid = existing_rows.first().and_then(|row| {
            row.iter().find_map(|entry| match &entry.value {
                SqlValue::Integer(value) if entry.column_name == "rowid" => Some(*value),
                _ => None,
            })
        });

        if let Some(rowid) = existing_rowid {
            sqlite_connection
                .query(
                    "UPDATE doc_embedding_vec SET embedding = ?1 WHERE rowid = ?2",
                    &[SqlValue::Text(vector_json), SqlValue::Integer(rowid)],
                )
                .map_err(|err| JobErrorX::Terminal(ferr!("error updating vec row: {err:?}")))?;
            sqlite_connection
                .query(
                    "UPDATE doc_embedding_meta SET origin_heads = ?1 WHERE rowid = ?2",
                    &[SqlValue::Text(serialized_heads), SqlValue::Integer(rowid)],
                )
                .map_err(|err| JobErrorX::Terminal(ferr!("error updating meta row: {err:?}")))?;
        } else {
            sqlite_connection
                .query(
                    "INSERT INTO doc_embedding_vec (embedding) VALUES (?1)",
                    &[SqlValue::Text(vector_json)],
                )
                .map_err(|err| JobErrorX::Terminal(ferr!("error inserting vec row: {err:?}")))?;
            let inserted_rowid_rows = sqlite_connection
                .query("SELECT last_insert_rowid() AS rowid", &[])
                .map_err(|err| {
                    JobErrorX::Terminal(ferr!("error getting inserted rowid: {err:?}"))
                })?;
            let inserted_rowid = inserted_rowid_rows
                .first()
                .and_then(|row| {
                    row.iter().find_map(|entry| match &entry.value {
                        SqlValue::Integer(value) if entry.column_name == "rowid" => Some(*value),
                        _ => None,
                    })
                })
                .ok_or_else(|| JobErrorX::Terminal(ferr!("missing inserted rowid")))?;
            sqlite_connection
                .query(
                    "INSERT INTO doc_embedding_meta (rowid, doc_id, facet_key, origin_heads) VALUES (?1, ?2, ?3, ?4)",
                    &[
                        SqlValue::Integer(inserted_rowid),
                        SqlValue::Text(args.doc_id.clone()),
                        SqlValue::Text(args.facet_key.clone()),
                        SqlValue::Text(serialized_heads),
                    ],
                )
                .map_err(|err| JobErrorX::Terminal(ferr!("error inserting meta row: {err:?}")))?;
        }

        Ok(Json(()))
    })?;
    Ok(())
}
