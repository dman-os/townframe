use super::super::*;
use crate::interlude::*;
use crate::wflows::label_engine::{self, LabelRequest};
use wflow_sdk::WflowCtx;

const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const NOTE_LABEL_ALGORITHM_TAG: &str = "label-note/embed-gauntlet-nomic-v1";
const LOCAL_STATE_KEY: &str = "@daybook/plabels/label-classifier";
const CANDIDATE_SET_ID: &str = "label-candidates";

pub fn run(cx: &mut WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();
    let working_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &args.facet_key).ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found",
                args.facet_key
            ))
        })?;

    let embedding_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Embedding).to_string();
    let embedding_facet_token = tuple_list_get(&args.ro_facet_tokens, &embedding_facet_key)
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "embedding facet key '{}' not found",
                embedding_facet_key
            ))
        })?;
    if !embedding_facet_token.exists() {
        return Ok(());
    }

    let sqlite_connection =
        tuple_list_get(&args.sqlite_connections, LOCAL_STATE_KEY).ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("missing sqlite connection '{LOCAL_STATE_KEY}'"))
        })?;

    let config_facet_key = crate::types::pseudo_label_candidates_key(CANDIDATE_SET_ID).to_string();
    let rw_config_token = tuple_list_get(&args.rw_config_facet_tokens, &config_facet_key);
    let ro_config_token = tuple_list_get(&args.ro_config_facet_tokens, &config_facet_key);
    let error_facet_key = crate::types::pseudo_label_error_key().to_string();
    let error_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &error_facet_key).ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error facet key '{}' not found", error_facet_key))
        })?;

    let embedding_raw = embedding_facet_token.get();
    let embedding_json: daybook_types::doc::FacetRaw = serde_json::from_str(&embedding_raw)
        .map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error parsing embedding facet json: {err}"))
        })?;
    let embedding = match WellKnownFacet::from_json(embedding_json, WellKnownFacetTag::Embedding)
        .map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(err.wrap_err("input facet is not embedding"))
        })? {
        WellKnownFacet::Embedding(value) => value,
        _ => unreachable!(),
    };
    if embedding.dtype != daybook_types::doc::EmbeddingDtype::F32 || embedding.compression.is_some()
    {
        return Ok(());
    }
    if embedding.dim != 768
        || !embedding
            .model_tag
            .eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID)
    {
        return Ok(());
    }
    let parsed_ref = match daybook_types::url::parse_facet_ref(&embedding.facet_ref) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    if parsed_ref.facet_key.tag != daybook_types::doc::FacetTag::WellKnown(WellKnownFacetTag::Note)
    {
        return Ok(());
    }
    let vector_json = daybook_types::doc::embedding_f32_bytes_to_json(&embedding.vector, 768)
        .map_err(wflow_sdk::JobErrorX::Terminal)?;

    cx.effect(|| {
        label_engine::apply_labeling(LabelRequest {
            sqlite_connection,
            rw_config_token,
            ro_config_token,
            working_facet_token,
            error_facet_token,
            input_vector_json: &vector_json,
            source_ref: &embedding.facet_ref,
            source_ref_heads: Some(am_utils_rs::serialize_commit_heads(&embedding.ref_heads.0)),
            algorithm_tag: NOTE_LABEL_ALGORITHM_TAG,
            candidate_set_id: CANDIDATE_SET_ID,
        })
        .map(wflow_sdk::Json)
    })?;

    Ok(())
}
