use super::super::*;
use crate::interlude::*;
use crate::types::{pseudo_label_candidates_key, PseudoLabelCandidatesFacet};
use crate::wflows::learn_algo::{
    mean_normalized, merge_label_proposal_with_dedupe, parse_llm_answer,
    validate_and_normalize_proposal,
};
use crate::{embedding_bytes_to_f32, row_blob, row_i64, row_text};
use wflow_sdk::{JobErrorX, Json, WflowCtx};

const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const CANDIDATE_SET_CONFIG_FACET_ID: &str = "label-candidates";
const LOCAL_STATE_KEY: &str = "@daybook/plabels/label-candidates-learner";
const MAX_NOTE_PROMPT_LEN: usize = 4_000;

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let mut args = facet_routine::get_args();
    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();

    let mut ro_facet_tokens = std::mem::take(&mut args.ro_facet_tokens);
    let note_facet_token =
        tuple_list_take(&mut ro_facet_tokens, &note_facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "note facet key '{}' not found in ro_facet_tokens",
                note_facet_key
            ))
        })?;

    let sqlite_connection =
        tuple_list_get(&args.sqlite_connections, LOCAL_STATE_KEY).ok_or_else(|| {
            JobErrorX::Terminal(ferr!("missing sqlite connection '{LOCAL_STATE_KEY}'"))
        })?;

    let config_facet_key = pseudo_label_candidates_key(CANDIDATE_SET_CONFIG_FACET_ID).to_string();
    let rw_config_token = tuple_list_get(&args.rw_config_facet_tokens, &config_facet_key);
    let ro_config_token = tuple_list_get(&args.ro_config_facet_tokens, &config_facet_key);
    if rw_config_token.is_none() && ro_config_token.is_none() {
        return Ok(());
    }

    let note_raw = note_facet_token.get();
    let note_json: daybook_types::doc::FacetRaw = serde_json::from_str(&note_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing note facet json: {err}")))?;
    let note = match WellKnownFacet::from_json(note_json, WellKnownFacetTag::Note)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not note")))?
    {
        WellKnownFacet::Note(value) => value,
        _ => unreachable!(),
    };
    if note.content.trim().is_empty() {
        return Ok(());
    }

    cx.effect(|| {
        use crate::wit::townframe::daybook::mltools_llm_chat;

        let mut proposal_set = load_or_init_proposal_set(rw_config_token, ro_config_token)?;
        ensure_embedding_cache_schema(sqlite_connection)?;

        let note_snippet = note_prompt_snippet(&note.content);
        let llm_text = mltools_llm_chat::llm_chat(&build_note_prompt(&note_snippet))
            .map_err(|err| JobErrorX::Terminal(ferr!("error calling note llm: {err}")))?;

        let Some(parsed_proposal) = parse_llm_answer(&llm_text) else {
            return Ok(Json(()));
        };
        let Some(new_label) = validate_and_normalize_proposal(parsed_proposal) else {
            return Ok(Json(()));
        };

        let merged = merge_label_proposal_with_dedupe(&proposal_set, new_label, |prompts| {
            proposal_centroid(sqlite_connection, prompts).map_err(|err| eyre::eyre!("{err}"))
        })
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("error merging learned label proposal")))?;

        if merged != proposal_set {
            proposal_set = merged;
            if let Some(token) = rw_config_token {
                let facet_raw: daybook_types::doc::FacetRaw = serde_json::to_value(proposal_set)
                    .map_err(|err| {
                        JobErrorX::Terminal(ferr!(
                            "error serializing learned proposal set facet: {err}"
                        ))
                    })?;
                let facet_raw = serde_json::to_string(&facet_raw).expect(ERROR_JSON);
                token
                    .update(&facet_raw)
                    .wrap_err("error updating learned proposal set")
                    .map_err(JobErrorX::Terminal)?;
            }
        }

        Ok(Json(()))
    })?;

    Ok(())
}

fn note_prompt_snippet(note_content: &str) -> String {
    note_content.chars().take(MAX_NOTE_PROMPT_LEN).collect()
}

fn build_note_prompt(note_content: &str) -> String {
    format!(
        r#"You are labeling a note into a reusable concept.

Return ONLY XML-like tags in this exact shape:
<answer>
  <label>snake_case_label</label>
  <positive_prompts>
    <prompt>...</prompt>
    <prompt>...</prompt>
  </positive_prompts>
  <negative_prompts>
    <prompt>...</prompt>
    <prompt>...</prompt>
  </negative_prompts>
</answer>

Rules:
- label must be short, lowercase, snake_case
- positive prompts describe the generic semantic concept
- negative prompts are hard negatives (conceptually similar but different)
- do not describe one-off specifics unique to this exact note

Note content:
{}
"#,
        note_content
    )
}

fn load_or_init_proposal_set(
    rw_config_token: Option<&crate::wit::townframe::daybook::capabilities::FacetTokenRw>,
    ro_config_token: Option<&crate::wit::townframe::daybook::capabilities::FacetTokenRo>,
) -> Result<PseudoLabelCandidatesFacet, JobErrorX> {
    if let Some(token) = rw_config_token {
        if token.exists() {
            let raw = token.get();
            let facet_raw: daybook_types::doc::FacetRaw =
                serde_json::from_str(&raw).map_err(|err| {
                    JobErrorX::Terminal(ferr!(
                        "error parsing config proposal set facet json: {err}"
                    ))
                })?;
            return serde_json::from_value::<PseudoLabelCandidatesFacet>(facet_raw).map_err(
                |err| {
                    JobErrorX::Terminal(ferr!("config facet is not pseudo label candidates: {err}"))
                },
            );
        }

        let value = PseudoLabelCandidatesFacet { labels: vec![] };
        let facet_raw: daybook_types::doc::FacetRaw =
            serde_json::to_value(value.clone()).map_err(|err| {
                JobErrorX::Terminal(ferr!("error serializing default proposal set: {err}"))
            })?;
        let facet_raw = serde_json::to_string(&facet_raw).expect(ERROR_JSON);
        token
            .update(&facet_raw)
            .wrap_err("error writing default learned proposal set")
            .map_err(JobErrorX::Terminal)?;
        return Ok(value);
    }

    if let Some(token) = ro_config_token {
        if !token.exists() {
            return Ok(PseudoLabelCandidatesFacet { labels: vec![] });
        }
        let raw = token.get();
        let facet_raw: daybook_types::doc::FacetRaw =
            serde_json::from_str(&raw).map_err(|err| {
                JobErrorX::Terminal(ferr!(
                    "error parsing ro config proposal set facet json: {err}"
                ))
            })?;
        return serde_json::from_value::<PseudoLabelCandidatesFacet>(facet_raw).map_err(|err| {
            JobErrorX::Terminal(ferr!(
                "ro config facet is not pseudo label candidates: {err}"
            ))
        });
    }

    Ok(PseudoLabelCandidatesFacet { labels: vec![] })
}

fn ensure_embedding_cache_schema(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
) -> Result<(), JobErrorX> {
    sqlite_connection
        .query_batch(
            r#"
            CREATE TABLE IF NOT EXISTS learned_label_text_embedding_cache (
                query_text TEXT PRIMARY KEY,
                model_tag TEXT NOT NULL,
                dim INTEGER NOT NULL,
                vector BLOB NOT NULL
            );
            "#,
        )
        .map_err(|err| {
            JobErrorX::Terminal(ferr!("error initializing learned label cache db: {err:?}"))
        })?;
    Ok(())
}

fn proposal_centroid(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
    prompts: &[String],
) -> Result<Vec<f32>, JobErrorX> {
    let mut vectors = Vec::with_capacity(prompts.len());
    for prompt in prompts {
        vectors.push(get_or_compute_text_embedding(sqlite_connection, prompt)?);
    }
    mean_normalized(&vectors)
        .ok_or_else(|| JobErrorX::Terminal(ferr!("unable to compute proposal centroid")))
}

fn get_or_compute_text_embedding(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
    query_text: &str,
) -> Result<Vec<f32>, JobErrorX> {
    use crate::wit::townframe::daybook::mltools_embed;
    use crate::wit::townframe::sql::types::SqlValue;

    let cache_rows = sqlite_connection
        .query(
            "SELECT model_tag, dim, vector FROM learned_label_text_embedding_cache WHERE query_text = ?1",
            &[SqlValue::Text(query_text.to_string())],
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error querying learned label embedding cache: {err:?}")))?;
    if let Some(row) = cache_rows.first() {
        let model_tag = row_text(row, "model_tag").ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "malformed learned label embedding cache row: missing/invalid field 'model_tag'"
            ))
        })?;
        let dim = row_i64(row, "dim").ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "malformed learned label embedding cache row: missing/invalid field 'dim'"
            ))
        })?;
        let vector_bytes = row_blob(row, "vector").ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "malformed learned label embedding cache row: missing/invalid field 'vector'"
            ))
        })?;
        let vector = embedding_bytes_to_f32(&vector_bytes)
            .map_err(|err| JobErrorX::Terminal(err.wrap_err("invalid cached embedding bytes")))?;
        if dim != vector.len() as i64 {
            return Err(JobErrorX::Terminal(ferr!(
                "malformed learned label embedding cache row: dim {} does not match vector len {}",
                dim,
                vector.len()
            )));
        }
        if model_tag.eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID) {
            return Ok(vector);
        }
    }

    let embed_result = mltools_embed::embed_text(query_text)
        .map_err(|err| JobErrorX::Terminal(ferr!("error embedding prompt text: {err}")))?;
    if !embed_result
        .model_id
        .eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID)
    {
        return Err(JobErrorX::Terminal(ferr!(
            "unexpected text embedding model '{}', expected '{}'",
            embed_result.model_id,
            NOMIC_TEXT_MODEL_ID
        )));
    }
    let vector_bytes = daybook_types::doc::embedding_f32_slice_to_le_bytes(&embed_result.vector);
    sqlite_connection
        .query(
            "INSERT INTO learned_label_text_embedding_cache (query_text, model_tag, dim, vector) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(query_text) DO UPDATE SET model_tag = excluded.model_tag, dim = excluded.dim, vector = excluded.vector",
            &[
                SqlValue::Text(query_text.to_string()),
                SqlValue::Text(embed_result.model_id.clone()),
                SqlValue::Integer(embed_result.dimensions as i64),
                SqlValue::Blob(vector_bytes),
            ],
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error upserting learned label embedding cache: {err:?}")))?;
    Ok(embed_result.vector)
}
