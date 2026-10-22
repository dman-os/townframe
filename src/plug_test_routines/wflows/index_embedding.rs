use crate::interlude::*;
use wflow_sdk::JobErrorX;
use wflow_sdk::Json;
use wflow_sdk::WflowCtx;

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::sqlite::types::SqlValue;
    use daybook_types::doc::WellKnownFacet;
    use daybook_types::doc::WellKnownFacetTag;

    let args = facet_routine::get_args();
    let embedding_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Embedding).to_string();
    let embedding_facet_token = args
        .primary_doc
        .facets
        .iter()
        .find(|token| {
            token.key() == embedding_facet_key && token.rights().contains(FacetRights::READ)
        })
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!("embedding facet token with read rights not found"))
        })?;
    let sqlite_connection = args
        .sqlite_connections
        .iter()
        .find(|(key, _)| key == "@daybook/test/doc-embedding-index")
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "sqlite connection '@daybook/test/doc-embedding-index' not found"
            ))
        })?;

    let embedding_raw = embedding_facet_token.get().map_err(|err| {
        JobErrorX::Terminal(ferr!("access error reading embedding facet: {err:?}"))
    })?;
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
            .execute_batch(
                r#"
                CREATE VIRTUAL TABLE IF NOT EXISTS doc_embedding_vec
                USING vec0(embedding float[768]);

                CREATE TABLE IF NOT EXISTS doc_embedding_meta (
                    rowid INTEGER PRIMARY KEY,
                    doc_id TEXT NOT NULL,
                    facet_key TEXT NOT NULL,
                    origin_heads TEXT NOT NULL,
                    UNIQUE(doc_id, facet_key)
                ) STRICT;
                "#,
            )
            .map_err(|err| {
                JobErrorX::Terminal(ferr!("error initializing vector index: {err:?}"))
            })?;

        let tx = sqlite_connection.begin_transaction().map_err(|err| {
            JobErrorX::Terminal(ferr!("error beginning sqlite transaction: {err:?}"))
        })?;

        let tx_result: Result<(), JobErrorX> = (|| {
            let existing_rows = tx
                .query(
                    "SELECT rowid FROM doc_embedding_meta WHERE doc_id = ?1 AND facet_key = ?2",
                    &[
                        SqlValue::Text(args.doc_id.clone()),
                        SqlValue::Text(embedding_facet_key.clone()),
                    ],
                )
                .map_err(|err| {
                    JobErrorX::Terminal(ferr!("error selecting vector row: {err:?}"))
                })?;

            let existing_rowid = existing_rows.rows.first().and_then(|row| {
                let idx = existing_rows.columns.iter().position(|column| column == "rowid")?;
                match row.get(idx) {
                    Some(SqlValue::Integer(value)) => Some(*value),
                    _ => None,
                }
            });

            if let Some(rowid) = existing_rowid {
                tx.execute(
                    "UPDATE doc_embedding_vec SET embedding = ?1 WHERE rowid = ?2",
                    &[SqlValue::Text(vector_json.clone()), SqlValue::Integer(rowid)],
                )
                .map_err(|err| {
                    JobErrorX::Terminal(ferr!("error updating vec row: {err:?}"))
                })?;
                tx.execute(
                    "UPDATE doc_embedding_meta SET origin_heads = ?1 WHERE rowid = ?2",
                    &[
                        SqlValue::Text(serialized_heads.clone()),
                        SqlValue::Integer(rowid),
                    ],
                )
                .map_err(|err| {
                    JobErrorX::Terminal(ferr!("error updating meta row: {err:?}"))
                })?;
            } else {
                let insert = tx
                    .execute(
                        "INSERT INTO doc_embedding_vec (embedding) VALUES (?1)",
                        &[SqlValue::Text(vector_json.clone())],
                    )
                    .map_err(|err| {
                        JobErrorX::Terminal(ferr!("error inserting vec row: {err:?}"))
                    })?;
                let inserted_rowid = insert.last_insert_rowid;
                tx.execute(
                    "INSERT INTO doc_embedding_meta (rowid, doc_id, facet_key, origin_heads) VALUES (?1, ?2, ?3, ?4)",
                    &[
                        SqlValue::Integer(inserted_rowid),
                        SqlValue::Text(args.doc_id.clone()),
                        SqlValue::Text(embedding_facet_key.clone()),
                        SqlValue::Text(serialized_heads.clone()),
                    ],
                )
                .map_err(|err| {
                    JobErrorX::Terminal(ferr!("error inserting meta row: {err:?}"))
                })?;
            }

            Ok(())
        })();

        if let Err(err) = tx_result {
            tx.rollback().map_err(|rollback_err| {
                JobErrorX::Terminal(ferr!("error rolling back sqlite transaction after {err:?}: {rollback_err:?}"))
            })?;
            return Err(err);
        }

        tx.commit().map_err(|err| {
            JobErrorX::Terminal(ferr!("error committing sqlite transaction: {err:?}"))
        })?;

        Ok(Json(()))
    })?;
    Ok(())
}
