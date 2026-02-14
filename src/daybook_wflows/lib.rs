#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}

mod wit {
    wit_bindgen::generate!({
        path: "wit",
        world: "bundle",

        // generate_all,
        // async: true,
        with: {
            "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,
            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,

            "townframe:mltools/ocr": generate,
            "townframe:mltools/embed": generate,
            "townframe:sql/types": generate,

            "townframe:daybook-types/doc": generate,

            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/facet-routine": generate,
            "townframe:daybook/sqlite-connection": generate,
            "townframe:daybook/mltools-ocr": generate,
            "townframe:daybook/mltools-embed": generate,
            "townframe:daybook/mltools-llm-chat": generate,
        }
    });
}

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::bundle::JobResult;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::wflow::bundle::Guest for Component {
    fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
        wflow_sdk::route_wflows!(args, {
            "pseudo-label" => |cx, _args: serde_json::Value| pseudo_labeler(cx),
            "test-label" => |cx, _args: serde_json::Value| test_labeler(cx),
            "ocr-image" => |cx, _args: serde_json::Value| ocr_image(cx),
            "embed-text" => |cx, _args: serde_json::Value| embed_text(cx),
            "index-embedding" => |cx, _args: serde_json::Value| index_embedding(cx),
        })
    }
}

fn embed_text(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let working_facet_token = args
        .rw_facet_tokens
        .iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_facet_token = args
        .ro_facet_tokens
        .iter()
        .find(|(key, _)| key == &note_facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "note facet key '{}' not found in ro_facet_tokens",
                note_facet_key
            ))
        })?;

    let current_facet_raw = note_facet_token.get();

    let current_facet_json: daybook_types::doc::FacetRaw = serde_json::from_str(&current_facet_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing working facet json: {err}")))?;

    let current_note = WellKnownFacet::from_json(current_facet_json, WellKnownFacetTag::Note)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not a note facet")))?;
    let WellKnownFacet::Note(note) = current_note else {
        return Err(JobErrorX::Terminal(ferr!("input facet is not note")));
    };

    // FIXME: put this in an effect
    let embed_result = mltools_embed::embed_text(&note.content)
        .map_err(|err| JobErrorX::Terminal(ferr!("error running embed-text: {err}")))?;
    let heads = utils_rs::am::parse_commit_heads(&args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("invalid heads from facet-routine: {err}")))?;
    let facet_key = daybook_types::doc::FacetKey::from(note_facet_key.as_str());
    let facet_ref =
        daybook_types::url::build_facet_ref(daybook_types::url::FACET_SELF_DOC_ID, &facet_key)
            .map_err(|err| {
                JobErrorX::Terminal(err.wrap_err("error creating embedding facet_ref"))
            })?;
    let vector_bytes = embed_result
        .vector
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<u8>>();

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::Embedding(daybook_types::doc::Embedding {
                facet_ref: facet_ref.clone(),
                ref_heads: daybook_types::doc::ChangeHashSet(Arc::clone(&heads)),
                model_tag: embed_result.model_id.clone(),
                vector: vector_bytes.clone(),
                dim: embed_result.dimensions,
                dtype: daybook_types::doc::EmbeddingDtype::F32,
                compression: None,
            })
            .into();

        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating embedding facet")
            .map_err(JobErrorX::Terminal)?;

        Ok(Json(()))
    })?;

    Ok(())
}

fn index_embedding(cx: WflowCtx) -> Result<(), JobErrorX> {
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
        .or_else(|| args.sqlite_connections.first().map(|(_, token)| token))
        .ok_or_else(|| JobErrorX::Terminal(ferr!("no sqlite connection available")))?;

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

fn ocr_image(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_ocr;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let working_facet_token = args
        .rw_facet_tokens
        .into_iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    let blob_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Blob).to_string();
    let blob_facet_token = args
        .ro_facet_tokens
        .into_iter()
        .find(|(key, _)| key == &blob_facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "blob facet key '{}' not found in ro_facet_tokens",
                blob_facet_key
            ))
        })?;

    let ocr_result = mltools_ocr::ocr_image(blob_facet_token)
        .map_err(|err| JobErrorX::Terminal(ferr!("error running OCR: {err}")))?;

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::Note(daybook_types::doc::Note {
                mime: "text/plain".to_string(),
                content: ocr_result.text.clone(),
            })
            .into();

        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating note with OCR result")
            .map_err(JobErrorX::Terminal)?;

        Ok(Json(()))
    })?;

    Ok(())
}

fn test_labeler(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    let args = facet_routine::get_args();

    // Find the working facet token (the one with write access matching facet_key)
    let working_facet_token = args
        .rw_facet_tokens
        .iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    // Extract text content for LLM
    // Use root types since Doc uses root types (not WIT types)
    use daybook_types::doc::WellKnownFacet;

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::LabelGeneric("test_label".into()).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}

fn pseudo_labeler(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::drawer;
    use crate::wit::townframe::daybook::facet_routine;

    let args = facet_routine::get_args();

    // Find the working facet token (the one with write access matching facet_key)
    let working_facet_token = args
        .rw_facet_tokens
        .iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    // Get doc using drawer interface
    let doc = drawer::get_doc_at_heads(&args.doc_id, &args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("error getting doc: {err:?}")))?;

    // Extract text content for LLM
    // Use root types since Doc uses root types (not WIT types)
    use daybook_types::doc::{Note, WellKnownFacet, WellKnownFacetTag};
    let content_text = match doc
        .facets
        .iter()
        .find(|(facet_key, _)| {
            let facet_key = daybook_types::doc::FacetKey::from(facet_key.as_str());
            facet_key.tag == daybook_types::doc::FacetTag::WellKnown(WellKnownFacetTag::Note)
        })
        .map(|(_, val)| {
            WellKnownFacet::from_json(serde_json::from_str(val).unwrap(), WellKnownFacetTag::Note)
        }) {
        Some(Ok(WellKnownFacet::Note(Note { content, .. }))) => content,
        Some(Ok(_)) => unreachable!(),
        Some(Err(err)) => {
            return Err(JobErrorX::Terminal(
                err.wrap_err("unable to parse facet found on doc"),
            ))
        }
        None => {
            return Err(JobErrorX::Terminal(ferr!(
                "no {tag} found on doc",
                tag = WellKnownFacetTag::Note.as_str()
            )))
        }
    };

    // Call the LLM to generate a label
    let llm_response: String = cx.effect(|| {
        use crate::wit::townframe::daybook::mltools_llm_chat;

        let message_text = format!(
            "Based on the following document content, provide a single short label or category (1-3 words). \
            Just return the label, nothing else.\n\nDocument content:\n{}",
            content_text
        );
        let result = mltools_llm_chat::llm_chat(&message_text);

        match result {
            Ok(response_text) => {
                // Clean up the response - remove quotes, trim whitespace
                let label = response_text
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim()
                    .to_string();
                Ok(Json(label))
            }
            Err(err) => Err(JobErrorX::Terminal(ferr!("error calling LLM: {err}"))),
        }
    })?;

    let new_labels = vec![llm_response.clone()];

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::PseudoLabel(new_labels).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}
