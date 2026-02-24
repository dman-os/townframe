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
            "embed-image" => |cx, _args: serde_json::Value| embed_image(cx),
            "embed-text" => |cx, _args: serde_json::Value| embed_text(cx),
            "index-embedding" => |cx, _args: serde_json::Value| index_embedding(cx),
            "classify-image-label" => |cx, _args: serde_json::Value| classify_image_label(cx),
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

const NOMIC_VISION_MODEL_ID: &str = "nomic-ai/nomic-embed-vision-v1.5";
const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const IMAGE_LABEL_CANONICAL: &str = "receipt-image";
const IMAGE_LABEL_LOCAL_STATE_KEY: &str = "@daybook/wip/image-label-classifier";
const PROMPT_MAX_MIN_SCORE: f64 = 0.045;
const PROMPT_ENSEMBLE_MIN_SCORE: f64 = 0.040;
const PROMPT_ENSEMBLE_MIN_HITS: usize = 1;
const NULL_MARGIN_MIN: f64 = 0.000;
const CENTROID_MIN_SCORE: f64 = 0.040;

fn embed_image(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let mut args = facet_routine::get_args();

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

    let blob_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Blob).to_string();
    let mut ro_facet_tokens = std::mem::take(&mut args.ro_facet_tokens);
    let blob_facet_ix = ro_facet_tokens
        .iter()
        .position(|(key, _)| key == &blob_facet_key)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "blob facet key '{}' not found in ro_facet_tokens",
                blob_facet_key
            ))
        })?;
    let (_, blob_facet_token) = ro_facet_tokens.swap_remove(blob_facet_ix);

    let blob_raw = blob_facet_token.get();
    let blob_json: daybook_types::doc::FacetRaw = serde_json::from_str(&blob_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing blob facet json: {err}")))?;
    let blob = match WellKnownFacet::from_json(blob_json, WellKnownFacetTag::Blob)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not blob")))?
    {
        WellKnownFacet::Blob(blob) => blob,
        _ => unreachable!("blob tag must parse as blob facet"),
    };

    if !blob.mime.starts_with("image/") {
        return Ok(());
    }

    let embed_result = mltools_embed::embed_image(blob_facet_token)
        .map_err(|err| JobErrorX::Terminal(ferr!("error running embed-image: {err}")))?;

    let heads = utils_rs::am::parse_commit_heads(&args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("invalid heads from facet-routine: {err}")))?;
    let facet_key = daybook_types::doc::FacetKey::from(blob_facet_key.as_str());
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
            .wrap_err("error updating image embedding facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}

#[derive(Debug, Clone)]
struct SeedRow {
    row_kind: &'static str,
    label: Option<&'static str>,
    description: &'static str,
}

fn classify_image_label(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use crate::wit::townframe::sql::types::SqlValue;
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

    let embedding_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Embedding).to_string();
    let embedding_facet_token = args
        .ro_facet_tokens
        .iter()
        .find(|(key, _)| key == &embedding_facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "embedding facet key '{}' not found in ro_facet_tokens",
                embedding_facet_key
            ))
        })?;

    let blob_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Blob).to_string();
    let _blob_facet_token_present = args
        .ro_facet_tokens
        .iter()
        .any(|(key, _)| key == &blob_facet_key);

    let sqlite_connection = args
        .sqlite_connections
        .iter()
        .find(|(key, _)| key == IMAGE_LABEL_LOCAL_STATE_KEY)
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
        return Ok(());
    }
    if !embedding
        .model_tag
        .eq_ignore_ascii_case(NOMIC_VISION_MODEL_ID)
    {
        return Ok(());
    }
    let parsed_ref = match daybook_types::url::parse_facet_ref(&embedding.facet_ref) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    if parsed_ref.facet_key.tag != daybook_types::doc::FacetTag::WellKnown(WellKnownFacetTag::Blob)
    {
        return Ok(());
    }

    let image_vec = embedding_bytes_to_f32(&embedding.vector)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("invalid embedding bytes")))?;
    let image_vec_json = daybook_types::doc::embedding_f32_slice_to_le_bytes(&image_vec);
    let image_vec_json = daybook_types::doc::embedding_f32_bytes_to_json(&image_vec_json, 768)
        .map_err(JobErrorX::Terminal)?;

    #[derive(Clone)]
    struct PreparedSeedRow {
        row_kind: String,
        label: Option<String>,
        description: String,
        query_text: String,
        embedding_json: String,
        model_id: String,
    }

    let mut prepared_seed_rows = Vec::new();
    for seed in image_label_seed_rows() {
        let query_text = format!("search_query: {}", seed.description);
        let embed_result = mltools_embed::embed_text(&query_text)
            .map_err(|err| JobErrorX::Terminal(ferr!("error embedding seed text: {err}")))?;
        if !embed_result
            .model_id
            .eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID)
            || embed_result.dimensions != 768
        {
            return Err(JobErrorX::Terminal(ferr!(
                "unexpected seed embed model '{}'/dim {}",
                embed_result.model_id,
                embed_result.dimensions
            )));
        }
        let embedding_json = embedding_vec_to_json(&embed_result.vector)
            .map_err(|err| JobErrorX::Terminal(err.wrap_err("error serializing seed embedding")))?;
        prepared_seed_rows.push(PreparedSeedRow {
            row_kind: seed.row_kind.to_string(),
            label: seed.label.map(str::to_string),
            description: seed.description.to_string(),
            query_text,
            embedding_json,
            model_id: embed_result.model_id,
        });
    }

    cx.effect(|| {
        sqlite_connection
            .query_batch(
                r#"
                CREATE TABLE IF NOT EXISTS image_label_examples (
                    row_id INTEGER PRIMARY KEY,
                    row_kind TEXT NOT NULL,
                    label TEXT,
                    description TEXT NOT NULL,
                    query_text TEXT NOT NULL,
                    embedding_json TEXT NOT NULL,
                    embedding_dim INTEGER NOT NULL,
                    model_tag TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_image_label_examples_unique
                ON image_label_examples(row_kind, COALESCE(label, ''), query_text);
                "#,
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error initializing image label db: {err:?}")))?;

        for seed in &prepared_seed_rows {
            sqlite_connection
                .query(
                    "INSERT OR IGNORE INTO image_label_examples (row_kind, label, description, query_text, embedding_json, embedding_dim, model_tag, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
                    &[
                        SqlValue::Text(seed.row_kind.clone()),
                        match &seed.label {
                            Some(label_value) => SqlValue::Text(label_value.clone()),
                            None => SqlValue::Null,
                        },
                        SqlValue::Text(seed.description.clone()),
                        SqlValue::Text(seed.query_text.clone()),
                        SqlValue::Text(seed.embedding_json.clone()),
                        SqlValue::Integer(768),
                        SqlValue::Text(seed.model_id.clone()),
                    ],
                )
                .map_err(|err| JobErrorX::Terminal(ferr!("error seeding image labels: {err:?}")))?;
        }

        let rows = sqlite_connection
            .query(
                "SELECT row_kind, label, description, embedding_json FROM image_label_examples WHERE active = 1 AND embedding_dim = 768 AND model_tag = ?1 ORDER BY row_id",
                &[SqlValue::Text(NOMIC_TEXT_MODEL_ID.to_string())],
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error loading image label seeds: {err:?}")))?;

        #[derive(Clone)]
        struct CandidateRow {
            row_kind: String,
            label: Option<String>,
            description: String,
            embedding_json: String,
            embedding_vec: Vec<f32>,
        }

        let mut candidate_rows = Vec::new();
        for row in rows {
            let row_kind = row_text(&row, "row_kind")
                .ok_or_else(|| JobErrorX::Terminal(ferr!("seed row missing row_kind")))?;
            let label = row_opt_text(&row, "label");
            let description = row_text(&row, "description")
                .ok_or_else(|| JobErrorX::Terminal(ferr!("seed row missing description")))?;
            let embedding_json = row_text(&row, "embedding_json")
                .ok_or_else(|| JobErrorX::Terminal(ferr!("seed row missing embedding_json")))?;
            let embedding_vec: Vec<f32> = serde_json::from_str(&embedding_json)
                .map_err(|err| JobErrorX::Terminal(ferr!("invalid seed embedding_json: {err}")))?;
            candidate_rows.push(CandidateRow {
                row_kind,
                label,
                description,
                embedding_json,
                embedding_vec,
            });
        }

        if candidate_rows.is_empty() {
            return Ok(Json(()));
        }

        #[derive(Default)]
        struct LabelAgg {
            prompt_max: f64,
            prompt_hits: usize,
            prompt_vectors: Vec<Vec<f32>>,
        }

        let mut by_label: std::collections::HashMap<String, LabelAgg> = std::collections::HashMap::new();
        let mut null_max = f64::NEG_INFINITY;

        for row in &candidate_rows {
            let score = sqlite_vec_cosine_similarity(sqlite_connection, &image_vec_json, &row.embedding_json)?;
            if row.row_kind == "null_anchor" {
                if score > null_max {
                    null_max = score;
                }
                continue;
            }

            let Some(label) = row.label.as_ref() else {
                continue;
            };
            let agg = by_label.entry(label.clone()).or_default();
            if score > agg.prompt_max {
                agg.prompt_max = score;
            }
            if score >= PROMPT_ENSEMBLE_MIN_SCORE {
                agg.prompt_hits += 1;
            }
            agg.prompt_vectors.push(row.embedding_vec.clone());
            let _ = &row.description;
        }

        if by_label.is_empty() {
            return Ok(Json(()));
        }

        let mut best_label: Option<String> = None;
        let mut best_prompt_max = f64::NEG_INFINITY;
        for (label, agg) in &by_label {
            if agg.prompt_max > best_prompt_max {
                best_prompt_max = agg.prompt_max;
                best_label = Some(label.clone());
            }
        }
        let Some(best_label) = best_label else {
            return Ok(Json(()));
        };
        let best_agg = by_label
            .get(&best_label)
            .ok_or_else(|| JobErrorX::Terminal(ferr!("missing best label aggregate")))?;

        let mut centroid_scores: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for (label, agg) in &by_label {
            let centroid = mean_normalized(&agg.prompt_vectors)
                .ok_or_else(|| JobErrorX::Terminal(ferr!("cannot compute centroid for label '{}'", label)))?;
            let score = cosine_similarity_f32(&image_vec, &centroid)
                .ok_or_else(|| JobErrorX::Terminal(ferr!("invalid centroid dims for '{}'", label)))?;
            centroid_scores.insert(label.clone(), score);
        }
        let (centroid_best_label, centroid_best_score) = centroid_scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(label, score)| (label.clone(), *score))
            .ok_or_else(|| JobErrorX::Terminal(ferr!("missing centroid scores")))?;

        let prompt_max_pass = best_agg.prompt_max >= PROMPT_MAX_MIN_SCORE;
        let prompt_ensemble_pass = best_agg.prompt_hits >= PROMPT_ENSEMBLE_MIN_HITS;
        let _null_anchor_pass = best_agg.prompt_max.is_finite()
            && null_max.is_finite()
            && (best_agg.prompt_max - null_max) >= NULL_MARGIN_MIN;
        let _centroid_agreement_pass =
            centroid_best_label == best_label && centroid_best_score >= CENTROID_MIN_SCORE;
        // v1 fallback: prefer recall over strict precision, server-side classification remains primary.
        let is_match = prompt_max_pass && prompt_ensemble_pass;
        if !is_match || best_label != IMAGE_LABEL_CANONICAL {
            return Ok(Json(()));
        }

        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::LabelGeneric(best_label.clone()).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating image label facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}

fn image_label_seed_rows() -> Vec<SeedRow> {
    vec![
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_CANONICAL),
            description: "a photo of a long printed grocery store receipt",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_CANONICAL),
            description: "a photo of a paper receipt with itemized prices",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_CANONICAL),
            description: "a close-up photo of a shopping receipt",
        },
        SeedRow {
            row_kind: "null_anchor",
            label: None,
            description: "a photo of a red sports car on a racetrack",
        },
    ]
}

fn sqlite_vec_cosine_similarity(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
    left_json: &str,
    right_json: &str,
) -> Result<f64, JobErrorX> {
    use crate::wit::townframe::sql::types::SqlValue;
    let rows = sqlite_connection
        .query(
            "SELECT (1.0 - vec_distance_cosine(vec_f32(?1), vec_f32(?2))) AS score",
            &[
                SqlValue::Text(left_json.to_string()),
                SqlValue::Text(right_json.to_string()),
            ],
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error computing sqlite-vec cosine: {err:?}")))?;
    let Some(row) = rows.first() else {
        return Err(JobErrorX::Terminal(ferr!("missing sqlite-vec score row")));
    };
    row_real(row, "score")
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing sqlite-vec score value")))
}

fn row_text(row: &crate::wit::townframe::sql::types::ResultRow, name: &str) -> Option<String> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Text(value) if entry.column_name == name => {
            Some(value.clone())
        }
        _ => None,
    })
}

fn row_opt_text(row: &crate::wit::townframe::sql::types::ResultRow, name: &str) -> Option<String> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Text(value) if entry.column_name == name => {
            Some(value.clone())
        }
        crate::wit::townframe::sql::types::SqlValue::Null if entry.column_name == name => None,
        _ => None,
    })
}

fn row_real(row: &crate::wit::townframe::sql::types::ResultRow, name: &str) -> Option<f64> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Real(value) if entry.column_name == name => {
            Some(*value)
        }
        crate::wit::townframe::sql::types::SqlValue::Integer(value)
            if entry.column_name == name =>
        {
            Some(*value as f64)
        }
        _ => None,
    })
}

fn embedding_vec_to_json(values: &[f32]) -> Res<String> {
    let bytes = daybook_types::doc::embedding_f32_slice_to_le_bytes(values);
    daybook_types::doc::embedding_f32_bytes_to_json(&bytes, values.len() as u32)
}

fn embedding_bytes_to_f32(bytes: &[u8]) -> Res<Vec<f32>> {
    if bytes.len() % 4 != 0 {
        eyre::bail!(
            "embedding bytes length {} is not divisible by 4",
            bytes.len()
        );
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

fn mean_normalized(vectors: &[Vec<f32>]) -> Option<Vec<f32>> {
    let first = vectors.first()?;
    let dim = first.len();
    if dim == 0 || vectors.iter().any(|v| v.len() != dim) {
        return None;
    }
    let mut centroid = vec![0.0_f32; dim];
    for v in vectors {
        for (dst, src) in centroid.iter_mut().zip(v) {
            *dst += *src;
        }
    }
    let count = vectors.len() as f32;
    for value in &mut centroid {
        *value /= count;
    }
    let norm = centroid
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 {
        return None;
    }
    for value in &mut centroid {
        *value = (f64::from(*value) / norm) as f32;
    }
    Some(centroid)
}

fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let mut dot = 0.0_f64;
    let mut an = 0.0_f64;
    let mut bn = 0.0_f64;
    for (x, y) in a.iter().zip(b) {
        let x = f64::from(*x);
        let y = f64::from(*y);
        dot += x * y;
        an += x * x;
        bn += y * y;
    }
    if an == 0.0 || bn == 0.0 {
        return None;
    }
    Some(dot / (an.sqrt() * bn.sqrt()))
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
