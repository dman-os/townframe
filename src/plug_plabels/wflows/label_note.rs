use super::super::*;
use crate::interlude::*;
use crate::wflows::label_engine::{self, LabelRequest};
use wflow_sdk::WflowCtx;

const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const NOTE_LABEL_ALGORITHM_TAG: &str = "label-note/embed-gauntlet-nomic-v1";
const LOCAL_STATE_KEY: &str = "@daybook/plabels/label-classifier";
const CANDIDATE_SET_ID: &str = "label-candidates";

pub fn run(cx: WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();
    let working_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &args.facet_key).ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found",
                args.facet_key
            ))
        })?;

    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_facet_token =
        tuple_list_get(&args.ro_facet_tokens, &note_facet_key).ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("note facet key '{}' not found", note_facet_key))
        })?;

    let sqlite_connection = tuple_list_get(&args.sqlite_connections, LOCAL_STATE_KEY)
        .or_else(|| args.sqlite_connections.first().map(|(_, token)| token))
        .ok_or_else(|| wflow_sdk::JobErrorX::Terminal(ferr!("no sqlite connection available")))?;

    let config_facet_key = crate::types::pseudo_label_candidates_key(CANDIDATE_SET_ID).to_string();
    let rw_config_token = tuple_list_get(&args.rw_config_facet_tokens, &config_facet_key);
    let ro_config_token = tuple_list_get(&args.ro_config_facet_tokens, &config_facet_key);

    let note_raw = note_facet_token.get();
    let note_json: daybook_types::doc::FacetRaw =
        serde_json::from_str(&note_raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!("error parsing note facet json: {err}"))
        })?;
    let note = match WellKnownFacet::from_json(note_json, WellKnownFacetTag::Note)
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(err.wrap_err("input facet is not note")))?
    {
        WellKnownFacet::Note(value) => value,
        _ => unreachable!(),
    };
    if note.content.trim().is_empty() {
        return Ok(());
    }

    let embed_result = mltools_embed::embed_text(&format!("search_query: {}", note.content))
        .map_err(|err| wflow_sdk::JobErrorX::Terminal(ferr!("error embedding note text: {err}")))?;
    if !embed_result
        .model_id
        .eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID)
        || embed_result.dimensions != 768
    {
        return Err(wflow_sdk::JobErrorX::Terminal(ferr!(
            "unexpected note embed model '{}'/dim {}",
            embed_result.model_id,
            embed_result.dimensions
        )));
    }
    let vector_json = daybook_types::doc::embedding_f32_slice_to_le_bytes(&embed_result.vector);
    let vector_json =
        daybook_types::doc::embedding_f32_bytes_to_json(&vector_json, embed_result.dimensions)
            .map_err(wflow_sdk::JobErrorX::Terminal)?;

    let source_ref = daybook_types::url::build_facet_ref(
        daybook_types::url::FACET_SELF_DOC_ID,
        &daybook_types::doc::FacetKey::from(note_facet_key.as_str()),
    )
    .map_err(wflow_sdk::JobErrorX::Terminal)?
    .to_string();

    cx.effect(|| {
        label_engine::apply_labeling(LabelRequest {
            sqlite_connection,
            rw_config_token,
            ro_config_token,
            working_facet_token,
            input_vector_json: &vector_json,
            source_ref: &source_ref,
            algorithm_tag: NOTE_LABEL_ALGORITHM_TAG,
            candidate_set_id: CANDIDATE_SET_ID,
        })
        .map(wflow_sdk::Json)
    })?;

    Ok(())
}
