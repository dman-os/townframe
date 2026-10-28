use crate::interlude::*;
use crate::wasm_runtime::row_i64;
use crate::wasm_runtime::row_text;
use plug_plabels_manifest::label_algo::CacheEmbeddingRow;
use plug_plabels_manifest::label_algo::CacheSeedRow;
use plug_plabels_manifest::label_algo::LABEL_SET_CACHE_SCHEMA_VERSION;
use plug_plabels_manifest::label_algo::LABEL_SYNONYM_CENTROID_SIM_MAX;
use plug_plabels_manifest::label_algo::LabelCandidateMetrics;
use plug_plabels_manifest::label_algo::MAX_LABEL_CANDIDATES_TO_GAUNTLET;
use plug_plabels_manifest::label_algo::MAX_MULTI_LABEL_OUTPUTS;
use plug_plabels_manifest::label_algo::NOMIC_TEXT_MODEL_ID;
use plug_plabels_manifest::label_algo::PROMPT_HIT_MIN_SCORE;
use plug_plabels_manifest::label_algo::default_label_set;
use plug_plabels_manifest::label_algo::embedding_vec_to_json;
use plug_plabels_manifest::label_algo::mean_normalized;
use plug_plabels_manifest::label_algo::null_anchor_prompts;
use plug_plabels_manifest::label_algo::run_label_gauntlet;
use plug_plabels_manifest::label_algo::thresholds_for_label;
use plug_plabels_manifest::label_algo::top_k_mean;
use plug_plabels_manifest::label_algo::with_heads_fragment;
use plug_plabels_manifest::types::PseudoLabel;
use plug_plabels_manifest::types::PseudoLabelCandidatesFacet;
use plug_plabels_manifest::types::PseudoLabelEntry;
use plug_plabels_manifest::types::PseudoLabelError;
use plug_plabels_manifest::types::pseudo_label_candidates_key;
use wflow_sdk::JobErrorX;

pub struct LabelRequest<'a> {
    pub sqlite_connection: &'a crate::wit::townframe::sqlite::sqlite_connection::Connection,
    pub rw_config_token: Option<&'a crate::wit::townframe::daybook::capabilities::FacetToken>,
    pub ro_config_token: Option<&'a crate::wit::townframe::daybook::capabilities::FacetToken>,
    pub working_facet_target: super::FacetWriteTarget<'a>,
    pub error_facet_target: super::FacetWriteTarget<'a>,
    pub input_vector_json: &'a str,
    pub source_ref: &'a Url,
    pub source_ref_heads: Option<Vec<String>>,
    pub algorithm_tag: &'a str,
    pub candidate_set_id: &'a str,
}

pub async fn apply_labeling(req: LabelRequest<'_>) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::mltools_embed;
    use sqlite_rs::BackendRef;
    use sqlite_rs::SqlValue;

    use plug_plabels_manifest::label_algo::LabelAgg;

    let config_facet_key = pseudo_label_candidates_key(req.candidate_set_id).to_string();
    let (label_set, config_heads_json, config_heads) = load_label_set(&req).await?;
    let sqlite_connection = BackendRef::new(req.sqlite_connection);

    sqlite_connection
        .execute_batch(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS image_label_prompt_vec
            USING vec0(embedding float[768]);
            CREATE TABLE IF NOT EXISTS image_label_prompt_meta (
                rowid INTEGER PRIMARY KEY,
                label_set_version_id INTEGER NOT NULL,
                row_kind TEXT NOT NULL,
                label TEXT,
                description TEXT NOT NULL,
                query_text TEXT NOT NULL,
                model_tag TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            ) STRICT;
            CREATE TABLE IF NOT EXISTS image_label_label_set_versions (
                version_id INTEGER PRIMARY KEY,
                facet_key TEXT NOT NULL,
                facet_heads_json TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                model_tag TEXT NOT NULL,
                embedding_dim INTEGER NOT NULL,
                is_current INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            CREATE INDEX IF NOT EXISTS idx_image_label_prompt_meta_version
            ON image_label_prompt_meta(label_set_version_id, row_kind, label);
            "#
            .to_string(),
        )
        .await
        .map_err(|err| JobErrorX::Terminal(ferr!("error initializing label db: {err:?}")))?;

    let current_version_rows = sqlite_connection
        .query(
            "SELECT version_id, facet_heads_json, schema_version, model_tag, embedding_dim \
             FROM image_label_label_set_versions WHERE facet_key = ?1 AND is_current = 1 ORDER BY version_id DESC LIMIT 1".to_string(),
            vec![SqlValue::Text(config_facet_key.clone())],
        ).await
        .map_err(|err| JobErrorX::Terminal(ferr!("error loading label set cache version: {err:?}")))?;
    let current_version_row = current_version_rows.rows.first();
    let current_version_id =
        current_version_row.and_then(|row| row_i64(&current_version_rows, row, "version_id"));
    let current_heads_json = current_version_row
        .and_then(|row| row_text(&current_version_rows, row, "facet_heads_json"));
    let current_schema_version =
        current_version_row.and_then(|row| row_i64(&current_version_rows, row, "schema_version"));
    let current_model_tag =
        current_version_row.and_then(|row| row_text(&current_version_rows, row, "model_tag"));
    let current_embedding_dim =
        current_version_row.and_then(|row| row_i64(&current_version_rows, row, "embedding_dim"));
    let config_schema_version = LABEL_SET_CACHE_SCHEMA_VERSION;
    let config_model_tag = NOMIC_TEXT_MODEL_ID.to_string();
    let config_embedding_dim = 768_i64;
    let needs_rebuild = current_version_id.is_none()
        || current_heads_json.as_deref() != Some(&config_heads_json)
        || current_schema_version != Some(config_schema_version)
        || current_model_tag.as_deref() != Some(config_model_tag.as_str())
        || current_embedding_dim != Some(config_embedding_dim);

    let active_version_id = if needs_rebuild {
        let insert = sqlite_connection
            .execute(
                "INSERT INTO image_label_label_set_versions (facet_key, facet_heads_json, schema_version, model_tag, embedding_dim, is_current) VALUES (?1, ?2, ?3, ?4, ?5, 0)".to_string(),
                vec![
                    SqlValue::Text(config_facet_key.clone()),
                    SqlValue::Text(config_heads_json.to_string()),
                    SqlValue::Integer(config_schema_version),
                    SqlValue::Text(config_model_tag.clone()),
                    SqlValue::Integer(config_embedding_dim),
                ],
            ).await
            .map_err(|err| JobErrorX::Terminal(ferr!("error inserting label set version row: {err:?}")))?;
        let version_id = insert.last_insert_rowid;

        let mut cache_rows = Vec::new();
        for label in &label_set.labels {
            for prompt in &label.prompts {
                cache_rows.push(CacheSeedRow {
                    row_kind: "label_prompt".into(),
                    label: Some(label.label.clone()),
                    description: prompt.clone(),
                });
            }
            for prompt in &label.negative_prompts {
                cache_rows.push(CacheSeedRow {
                    row_kind: "negative_prompt".into(),
                    label: Some(label.label.clone()),
                    description: prompt.clone(),
                });
            }
        }
        for prompt in null_anchor_prompts() {
            cache_rows.push(CacheSeedRow {
                row_kind: "null_anchor".into(),
                label: None,
                description: (*prompt).into(),
            });
        }

        let mut prompt_vectors_by_label: std::collections::HashMap<String, Vec<Vec<f32>>> =
            std::collections::HashMap::new();
        for cache_row in cache_rows {
            let query_text = format!("search_query: {}", cache_row.description);
            let embed_result = mltools_embed::embed_text(query_text.clone())
                .await
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
            if cache_row.row_kind == "label_prompt"
                && let Some(label) = &cache_row.label
            {
                prompt_vectors_by_label
                    .entry(label.clone())
                    .or_default()
                    .push(embed_result.vector.clone());
            }
            insert_cache_embedding_row(
                req.sqlite_connection,
                CacheEmbeddingRow {
                    label_set_version_id: version_id,
                    row_kind: &cache_row.row_kind,
                    label: cache_row.label.as_deref(),
                    description: &cache_row.description,
                    query_text: &query_text,
                    model_tag: NOMIC_TEXT_MODEL_ID,
                    vector: &embed_result.vector,
                },
            )
            .await?;
        }

        for label in &label_set.labels {
            let Some(prompt_vectors) = prompt_vectors_by_label.get(&label.label) else {
                continue;
            };
            let centroid = mean_normalized(prompt_vectors).ok_or_else(|| {
                JobErrorX::Terminal(ferr!("cannot compute centroid for label '{}'", label.label))
            })?;
            let centroid_description = format!("centroid for {}", label.label);
            let centroid_query = format!("search_query: centroid {}", label.label);
            insert_cache_embedding_row(
                req.sqlite_connection,
                CacheEmbeddingRow {
                    label_set_version_id: version_id,
                    row_kind: "label_centroid",
                    label: Some(&label.label),
                    description: &centroid_description,
                    query_text: &centroid_query,
                    model_tag: NOMIC_TEXT_MODEL_ID,
                    vector: &centroid,
                },
            )
            .await?;
        }
        sqlite_connection
            .execute(
                "UPDATE image_label_label_set_versions SET is_current = 0 WHERE facet_key = ?1"
                    .to_string(),
                vec![SqlValue::Text(config_facet_key.clone())],
            )
            .await
            .map_err(|err| {
                JobErrorX::Terminal(ferr!(
                    "error clearing current label set version during promotion: {err:?}"
                ))
            })?;
        sqlite_connection
            .execute(
                "UPDATE image_label_label_set_versions SET is_current = 1 WHERE version_id = ?1"
                    .to_string(),
                vec![SqlValue::Integer(version_id)],
            )
            .await
            .map_err(|err| {
                JobErrorX::Terminal(ferr!(
                    "error promoting rebuilt label set version {}: {err:?}",
                    version_id
                ))
            })?;
        version_id
    } else {
        current_version_id.expect("version should exist when cache is current")
    };

    let scored_rows = sqlite_connection
        .query(
            "SELECT m.rowid AS rowid, m.row_kind, m.label, m.description, (1.0 - vec_distance_cosine(v.embedding, vec_f32(?1))) AS score \
             FROM image_label_prompt_meta m JOIN image_label_prompt_vec v ON v.rowid = m.rowid \
             WHERE m.active = 1 AND m.label_set_version_id = ?2 AND m.model_tag = ?3 ORDER BY m.rowid".to_string(),
            vec![
                SqlValue::Text(req.input_vector_json.to_string()),
                SqlValue::Integer(active_version_id),
                SqlValue::Text(NOMIC_TEXT_MODEL_ID.to_string()),
            ],
        ).await
        .map_err(|err| JobErrorX::Terminal(ferr!("error loading scored label rows: {err:?}")))?;
    if scored_rows.rows.is_empty() {
        write_no_hit_error(&req, "no-scored-rows", None, config_heads.as_deref()).await?;
        return Ok(());
    }

    let mut by_label: std::collections::HashMap<String, LabelAgg> =
        std::collections::HashMap::new();
    let mut null_max = f64::NEG_INFINITY;
    for row in &scored_rows.rows {
        let row_kind = row_text(&scored_rows, row, "row_kind")
            .ok_or_else(|| JobErrorX::Terminal(ferr!("scored row missing row_kind")))?;
        let score = row_real(&scored_rows, row, "score")
            .ok_or_else(|| JobErrorX::Terminal(ferr!("scored row missing score")))?;
        if row_kind == "null_anchor" {
            if score > null_max {
                null_max = score;
            }
            continue;
        }
        let label = row_opt_text(&scored_rows, row, "label")
            .ok_or_else(|| JobErrorX::Terminal(ferr!("scored row missing label column")))?;
        let Some(label) = label else {
            continue;
        };
        let agg = by_label.entry(label).or_insert_with(|| LabelAgg {
            negative_prompt_max: f64::NEG_INFINITY,
            ..Default::default()
        });
        if row_kind == "negative_prompt" {
            if score > agg.negative_prompt_max {
                agg.negative_prompt_max = score;
            }
            continue;
        }
        if row_kind == "label_centroid" {
            agg.centroid_score = Some(score);
            agg.centroid_rowid = row_i64(&scored_rows, row, "rowid");
            continue;
        }
        if row_kind == "label_prompt" {
            agg.prompt_scores.push(score);
        }
    }

    if by_label.is_empty() {
        write_no_hit_error(&req, "no-label-candidates", None, config_heads.as_deref()).await?;
        return Ok(());
    }

    let mut candidates = Vec::with_capacity(by_label.len());
    for (label, agg) in &by_label {
        if let Some(candidate) = label_candidate_metrics(label, agg, null_max)? {
            candidates.push(candidate);
        }
    }

    candidates.sort_by(|left_candidate, right_candidate| {
        right_candidate
            .composite_score
            .partial_cmp(&left_candidate.composite_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut shortlisted: Vec<LabelCandidateMetrics> = Vec::new();
    let top_candidate = candidates
        .first()
        .map(|candidate| (candidate.label.clone(), candidate.composite_score));
    for candidate in candidates
        .into_iter()
        .take(MAX_LABEL_CANDIDATES_TO_GAUNTLET)
    {
        let thresholds = thresholds_for_label(&candidate.label);
        let gauntlet_outcome = run_label_gauntlet(&candidate, &thresholds);
        if !gauntlet_outcome.passed {
            continue;
        }

        let mut suppressed_as_synonym = false;
        for kept in &shortlisted {
            let centroid_similarity = sqlite_vec_rowid_cosine_similarity(
                req.sqlite_connection,
                candidate.centroid_rowid,
                kept.centroid_rowid,
            )
            .await?;
            if centroid_similarity >= LABEL_SYNONYM_CENTROID_SIM_MAX {
                suppressed_as_synonym = true;
                break;
            }
        }
        if suppressed_as_synonym {
            continue;
        }
        shortlisted.push(candidate);
    }

    let Some(best_score) = shortlisted
        .first()
        .map(|candidate| candidate.composite_score)
    else {
        write_no_hit_error(
            &req,
            "gauntlet-rejected",
            top_candidate,
            config_heads.as_deref(),
        )
        .await?;
        return Ok(());
    };

    let mut output_labels = shortlisted
        .iter()
        .take(MAX_MULTI_LABEL_OUTPUTS)
        .map(|candidate| PseudoLabelEntry {
            label: candidate.label.clone(),
            score: candidate.composite_score,
        })
        .collect::<Vec<_>>();
    let candidate_set_ref = daybook_types::url::build_facet_ref(
        daybook_types::url::FACET_SELF_DOC_ID,
        &pseudo_label_candidates_key(req.candidate_set_id),
    )
    .map_err(JobErrorX::Terminal)?;
    let candidate_set_ref = with_heads_fragment(candidate_set_ref, config_heads.as_deref());
    let source_ref = with_heads_fragment(req.source_ref.clone(), req.source_ref_heads.as_deref());
    let new_facet = PseudoLabel {
        algorithm_tag: req.algorithm_tag.into(),
        top_score: best_score,
        labels: std::mem::take(&mut output_labels),
        source_ref,
        candidate_set_ref,
    };
    let new_facet: daybook_types::doc::FacetRaw = serde_json::to_value(new_facet)
        .map_err(|err| JobErrorX::Terminal(ferr!("error serializing pseudo labels: {err}")))?;
    let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
    req.working_facet_target
        .write(
            &new_facet,
            "error updating label facet",
            "error creating label facet",
        )
        .await?;
    Ok(())
}

/// Score one label from its aggregated prompt, centroid, and negative-prompt similarities
/// against the input. `null_max` is the best null-anchor score, which every label must beat.
/// A label with no positive prompt rows scored is not a candidate (`None`).
fn label_candidate_metrics(
    label: &str,
    agg: &plug_plabels_manifest::label_algo::LabelAgg,
    null_max: f64,
) -> Result<Option<LabelCandidateMetrics>, JobErrorX> {
    let prompt_count = agg.prompt_scores.len();
    if prompt_count == 0 {
        return Ok(None);
    }
    let centroid_score = agg
        .centroid_score
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing centroid score for '{label}'")))?;
    let centroid_rowid = agg
        .centroid_rowid
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing centroid rowid for '{label}'")))?;
    let prompt_max = agg
        .prompt_scores
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let prompt_top2_mean = top_k_mean(&agg.prompt_scores, 2)
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing prompt scores for '{label}'")))?;
    let prompt_hits = agg
        .prompt_scores
        .iter()
        .filter(|score| **score >= PROMPT_HIT_MIN_SCORE)
        .count();
    let prompt_hit_ratio = (prompt_hits as f64) / (prompt_count as f64);
    let label_negative_max = if agg.negative_prompt_max.is_finite() {
        agg.negative_prompt_max
    } else {
        f64::NEG_INFINITY
    };
    let null_margin = if prompt_max.is_finite() && null_max.is_finite() {
        prompt_max - null_max
    } else {
        f64::NEG_INFINITY
    };
    let negative_prompt_margin = if prompt_max.is_finite() && label_negative_max.is_finite() {
        prompt_max - label_negative_max
    } else {
        f64::INFINITY
    };
    let composite_score = (0.35 * prompt_top2_mean)
        + (0.20 * prompt_max)
        + (0.20 * centroid_score)
        + (0.15 * prompt_hit_ratio)
        + (0.05 * negative_prompt_margin.min(1.0))
        + (0.05 * null_margin.min(1.0));
    Ok(Some(LabelCandidateMetrics {
        label: label.to_owned(),
        centroid_rowid,
        prompt_count,
        prompt_max,
        prompt_top2_mean,
        prompt_hit_ratio,
        centroid_score,
        null_margin,
        negative_prompt_margin,
        composite_score,
    }))
}

async fn load_label_set(
    req: &LabelRequest<'_>,
) -> Result<(PseudoLabelCandidatesFacet, String, Option<Vec<String>>), JobErrorX> {
    if let Some(token) = req.ro_config_token {
        let heads = token.heads().map_err(|err| {
            JobErrorX::Terminal(ferr!("error reading ro config facet heads: {err:?}"))
        })?;
        let heads_json = serde_json::to_string(&heads).expect(ERROR_JSON);
        let raw = token.get().await.map_err(|err| {
            JobErrorX::Terminal(ferr!("error reading ro config label set: {err:?}"))
        })?;
        let facet_raw: daybook_types::doc::FacetRaw =
            serde_json::from_str(&raw).map_err(|err| {
                JobErrorX::Terminal(ferr!("error parsing ro config label set facet json: {err}"))
            })?;
        let label_set =
            serde_json::from_value::<PseudoLabelCandidatesFacet>(facet_raw).map_err(|err| {
                JobErrorX::Terminal(ferr!(
                    "ro config facet is not plug_plabels pseudo label candidates: {err}"
                ))
            })?;
        return Ok((label_set, heads_json, Some(heads)));
    }

    if let Some(token) = req.rw_config_token {
        let heads = token.heads().map_err(|err| {
            JobErrorX::Terminal(ferr!("error reading config facet heads: {err:?}"))
        })?;
        let heads_json = serde_json::to_string(&heads).expect(ERROR_JSON);
        let raw = token
            .get()
            .await
            .map_err(|err| JobErrorX::Terminal(ferr!("error reading config label set: {err:?}")))?;
        let facet_raw: daybook_types::doc::FacetRaw =
            serde_json::from_str(&raw).map_err(|err| {
                JobErrorX::Terminal(ferr!("error parsing config label set facet json: {err}"))
            })?;
        let label_set =
            serde_json::from_value::<PseudoLabelCandidatesFacet>(facet_raw).map_err(|err| {
                JobErrorX::Terminal(ferr!(
                    "config facet is not plug_plabels pseudo label candidates: {err}"
                ))
            })?;
        return Ok((label_set, heads_json, Some(heads)));
    }

    Ok((default_label_set(), "[]".to_string(), None))
}

async fn write_no_hit_error(
    req: &LabelRequest<'_>,
    reason: &str,
    top_candidate: Option<(String, f64)>,
    candidate_set_heads: Option<&[String]>,
) -> Result<(), JobErrorX> {
    let candidate_set_ref = daybook_types::url::build_facet_ref(
        daybook_types::url::FACET_SELF_DOC_ID,
        &pseudo_label_candidates_key(req.candidate_set_id),
    )
    .map_err(JobErrorX::Terminal)?;
    let candidate_set_ref = with_heads_fragment(candidate_set_ref, candidate_set_heads);
    let source_ref = with_heads_fragment(req.source_ref.clone(), req.source_ref_heads.as_deref());
    let (top_candidate_label, top_candidate_score) = match top_candidate {
        Some((label, score)) => (Some(label), Some(score)),
        None => (None, None),
    };
    let facet = PseudoLabelError::NoHit {
        reason: reason.to_string(),
        algorithm_tag: req.algorithm_tag.to_string(),
        source_ref,
        candidate_set_ref,
        top_candidate_label,
        top_candidate_score,
    };
    let facet_raw: daybook_types::doc::FacetRaw = serde_json::to_value(facet)
        .map_err(|err| JobErrorX::Terminal(ferr!("error serializing pseudo label error: {err}")))?;
    let facet_raw = serde_json::to_string(&facet_raw).expect(ERROR_JSON);
    req.error_facet_target
        .write(
            &facet_raw,
            "error updating label error facet",
            "error creating label error facet",
        )
        .await?;
    Ok(())
}

async fn insert_cache_embedding_row(
    sqlite_connection: &crate::wit::townframe::sqlite::sqlite_connection::Connection,
    row: CacheEmbeddingRow<'_>,
) -> Result<i64, JobErrorX> {
    use sqlite_rs::BackendRef;
    use sqlite_rs::SqlValue;
    let embedding_json = embedding_vec_to_json(row.vector)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("error serializing cached embedding")))?;

    let transaction = sqlite_connection.begin_transaction().await.map_err(|err| {
        JobErrorX::Terminal(ferr!(
            "error beginning sqlite transaction for cache: {err:?}"
        ))
    })?;
    let tx = BackendRef::new(&transaction);

    let tx_result: Result<i64, JobErrorX> = (async || {
        let insert = tx
            .execute(
                "INSERT INTO image_label_prompt_vec (embedding) VALUES (?1)".to_string(),
                vec![SqlValue::Text(embedding_json)],
            ).await
            .map_err(|err| JobErrorX::Terminal(ferr!("error inserting vec cache row: {err:?}")))?;
        let rowid = insert.last_insert_rowid;

        tx.execute(
            "INSERT INTO image_label_prompt_meta (rowid, label_set_version_id, row_kind, label, description, query_text, model_tag, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)".to_string(),
            vec![
                SqlValue::Integer(rowid),
                SqlValue::Integer(row.label_set_version_id),
                SqlValue::Text(row.row_kind.to_string()),
                row.label
                    .map_or(SqlValue::Null, |value| SqlValue::Text(value.to_string())),
                SqlValue::Text(row.description.to_string()),
                SqlValue::Text(row.query_text.to_string()),
                SqlValue::Text(row.model_tag.to_string()),
            ],
        ).await
        .map_err(|err| JobErrorX::Terminal(ferr!("error inserting cache meta row: {err:?}")))?;

        Ok(rowid)
    })().await;

    match tx_result {
        Ok(rowid) => {
            transaction.commit().await.map_err(|err| {
                JobErrorX::Terminal(ferr!("error committing cache transaction: {err:?}"))
            })?;
            Ok(rowid)
        }
        Err(err) => {
            transaction
                .rollback()
                .await
                .inspect_err(|err| error!("error on rollback: {err}"))
                .ok();
            Err(err)
        }
    }
}

async fn sqlite_vec_rowid_cosine_similarity(
    sqlite_connection: &crate::wit::townframe::sqlite::sqlite_connection::Connection,
    left_rowid: i64,
    right_rowid: i64,
) -> Result<f64, JobErrorX> {
    use sqlite_rs::BackendRef;
    use sqlite_rs::SqlValue;
    let sqlite = BackendRef::new(sqlite_connection);
    let rows = sqlite
        .query(
            "SELECT (1.0 - vec_distance_cosine(v1.embedding, v2.embedding)) AS score \
             FROM image_label_prompt_vec v1, image_label_prompt_vec v2 \
             WHERE v1.rowid = ?1 AND v2.rowid = ?2"
                .to_string(),
            vec![
                SqlValue::Integer(left_rowid),
                SqlValue::Integer(right_rowid),
            ],
        )
        .await
        .map_err(|err| {
            JobErrorX::Terminal(ferr!(
                "error computing sqlite-vec rowid cosine similarity: {err:?}"
            ))
        })?;
    let Some(row) = rows.rows.first() else {
        return Err(JobErrorX::Terminal(ferr!(
            "missing sqlite-vec rowid score row"
        )));
    };
    row_real(&rows, row, "score")
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing sqlite-vec rowid score value")))
}

fn row_opt_text(
    set: &sqlite_rs::ResultSet,
    row: &sqlite_rs::Row,
    name: &str,
) -> Option<Option<String>> {
    match crate::wasm_runtime::row_value(set, row, name)? {
        sqlite_rs::SqlValue::Text(value) => Some(Some(value.clone())),
        sqlite_rs::SqlValue::Null => Some(None),
        _ => None,
    }
}

fn row_real(set: &sqlite_rs::ResultSet, row: &sqlite_rs::Row, name: &str) -> Option<f64> {
    match crate::wasm_runtime::row_value(set, row, name)? {
        sqlite_rs::SqlValue::Real(value) => Some(*value),
        sqlite_rs::SqlValue::Integer(value) => Some(*value as f64),
        _ => None,
    }
}
