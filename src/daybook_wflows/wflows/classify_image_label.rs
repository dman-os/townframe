use super::super::*;
use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

const NOMIC_VISION_MODEL_ID: &str = "nomic-ai/nomic-embed-vision-v1.5";
const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const IMAGE_LABEL_RECEIPT: &str = "receipt-image";
const IMAGE_LABEL_TWITTER_SCREENSHOT: &str = "twitter-screenshot";
const IMAGE_LABEL_MINECRAFT: &str = "minecraft";
const IMAGE_LABEL_LOCAL_STATE_KEY: &str = "@daybook/wip/image-label-classifier";
const IMAGE_LABEL_SET_CONFIG_FACET_ID: &str = "daybook-wip-image-label-set";
const LABEL_SET_CACHE_SCHEMA_VERSION: i64 = 1;
const PROMPT_HIT_MIN_SCORE: f64 = 0.040;
const PROMPT_MAX_MIN_SCORE: f64 = 0.045;
const PROMPT_TOP2_MEAN_MIN_SCORE: f64 = 0.040;
const PROMPT_HIT_RATIO_MIN: f64 = 0.34;
const NULL_MARGIN_MIN: f64 = 0.000;
const NEGATIVE_PROMPT_MARGIN_MIN: f64 = 0.000;
const CENTROID_MIN_SCORE: f64 = 0.040;
const COMPOSITE_MIN_SCORE: f64 = 0.040;
const MAX_LABEL_CANDIDATES_TO_GAUNTLET: usize = 8;
const LABEL_SYNONYM_CENTROID_SIM_MAX: f64 = 0.97;
const MAX_MULTI_LABEL_OUTPUTS: usize = 4;

#[derive(Debug, Clone)]
struct LabelGauntletThresholds {
    prompt_max_min_score: f64,
    prompt_top2_mean_min_score: f64,
    prompt_hit_ratio_min: f64,
    centroid_min_score: f64,
    null_margin_min: f64,
    negative_prompt_margin_min: f64,
    composite_min_score: f64,
}

#[derive(Debug, Clone)]
struct LabelCandidateMetrics {
    label: String,
    centroid_rowid: i64,
    prompt_count: usize,
    prompt_max: f64,
    prompt_top2_mean: f64,
    prompt_hit_ratio: f64,
    centroid_score: f64,
    null_margin: f64,
    negative_prompt_margin: f64,
    composite_score: f64,
}

#[derive(Debug, Clone)]
struct LabelGauntletOutcome {
    passed: bool,
}

impl LabelGauntletThresholds {
    fn defaults() -> Self {
        Self {
            prompt_max_min_score: PROMPT_MAX_MIN_SCORE,
            prompt_top2_mean_min_score: PROMPT_TOP2_MEAN_MIN_SCORE,
            prompt_hit_ratio_min: PROMPT_HIT_RATIO_MIN,
            centroid_min_score: CENTROID_MIN_SCORE,
            null_margin_min: NULL_MARGIN_MIN,
            negative_prompt_margin_min: NEGATIVE_PROMPT_MARGIN_MIN,
            composite_min_score: COMPOSITE_MIN_SCORE,
        }
    }
}

fn thresholds_for_label(_label: &str) -> LabelGauntletThresholds {
    LabelGauntletThresholds::defaults()
}

fn run_label_gauntlet(
    metrics: &LabelCandidateMetrics,
    thresholds: &LabelGauntletThresholds,
) -> LabelGauntletOutcome {
    let passed = metrics.prompt_count > 0
        && metrics.prompt_max >= thresholds.prompt_max_min_score
        && metrics.prompt_top2_mean >= thresholds.prompt_top2_mean_min_score
        && metrics.prompt_hit_ratio >= thresholds.prompt_hit_ratio_min
        && metrics.centroid_score >= thresholds.centroid_min_score
        && metrics.null_margin >= thresholds.null_margin_min
        && metrics.negative_prompt_margin >= thresholds.negative_prompt_margin_min
        && metrics.composite_score >= thresholds.composite_min_score;
    LabelGauntletOutcome { passed }
}

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use crate::wit::townframe::sql::types::SqlValue;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let working_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &args.facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    let embedding_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Embedding).to_string();
    let embedding_facet_token = tuple_list_get(&args.ro_facet_tokens, &embedding_facet_key)
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

    let sqlite_connection = tuple_list_get(&args.sqlite_connections, IMAGE_LABEL_LOCAL_STATE_KEY)
        .or_else(|| args.sqlite_connections.first().map(|(_, token)| token))
        .ok_or_else(|| JobErrorX::Terminal(ferr!("no sqlite connection available")))?;
    let config_label_set_facet_key = daybook_types::doc::FacetKey {
        tag: daybook_types::doc::FacetTag::WellKnown(WellKnownFacetTag::PseudoLabelSet),
        id: IMAGE_LABEL_SET_CONFIG_FACET_ID.into(),
    }
    .to_string();
    let rw_config_label_set_token =
        tuple_list_get(&args.rw_config_facet_tokens, &config_label_set_facet_key);
    let ro_config_label_set_token =
        tuple_list_get(&args.ro_config_facet_tokens, &config_label_set_facet_key);
    if rw_config_label_set_token.is_none() && ro_config_label_set_token.is_none() {
        return Ok(());
    }

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

    cx.effect(|| {
        #[derive(Default)]
        struct LabelAgg {
            prompt_scores: Vec<f64>,
            negative_prompt_max: f64,
            centroid_score: Option<f64>,
            centroid_rowid: Option<i64>,
        }

        let (label_set, config_heads_json) = if let Some(token) = rw_config_label_set_token {
            let heads_json = serde_json::to_string(&token.heads()).expect(ERROR_JSON);
            let label_set = if token.exists() {
                let raw = token.get();
                let facet_raw: daybook_types::doc::FacetRaw = serde_json::from_str(&raw).map_err(
                    |err| JobErrorX::Terminal(ferr!("error parsing config label set facet json: {err}")),
                )?;
                match WellKnownFacet::from_json(facet_raw, WellKnownFacetTag::PseudoLabelSet)
                    .map_err(|err| JobErrorX::Terminal(err.wrap_err("config facet is not PseudoLabelSet")))?
                {
                    WellKnownFacet::PseudoLabelSet(value) => value,
                    _ => unreachable!(),
                }
            } else {
                let value = default_image_pseudo_label_set();
                let facet_raw: daybook_types::doc::FacetRaw =
                    WellKnownFacet::PseudoLabelSet(value.clone()).into();
                let facet_raw = serde_json::to_string(&facet_raw).expect(ERROR_JSON);
                token.update(&facet_raw)
                    .wrap_err("error writing default PseudoLabelSet config facet")
                    .map_err(JobErrorX::Terminal)?;
                value
            };
            (label_set, heads_json)
        } else if let Some(token) = ro_config_label_set_token {
            if !token.exists() {
                return Ok(Json(()));
            }
            let heads_json = serde_json::to_string(&token.heads()).expect(ERROR_JSON);
            let raw = token.get();
            let facet_raw: daybook_types::doc::FacetRaw = serde_json::from_str(&raw)
                .map_err(|err| JobErrorX::Terminal(ferr!("error parsing config label set facet json: {err}")))?;
            let value = match WellKnownFacet::from_json(facet_raw, WellKnownFacetTag::PseudoLabelSet)
                .map_err(|err| JobErrorX::Terminal(err.wrap_err("config facet is not PseudoLabelSet")))?
            {
                WellKnownFacet::PseudoLabelSet(value) => value,
                _ => unreachable!(),
            };
            (value, heads_json)
        } else {
            return Ok(Json(()));
        };

        sqlite_connection
            .query_batch(
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
                );
                CREATE TABLE IF NOT EXISTS image_label_label_set_versions (
                    version_id INTEGER PRIMARY KEY,
                    facet_key TEXT NOT NULL,
                    facet_heads_json TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    model_tag TEXT NOT NULL,
                    embedding_dim INTEGER NOT NULL,
                    is_current INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_image_label_prompt_meta_version
                ON image_label_prompt_meta(label_set_version_id, row_kind, label);
                "#,
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error initializing image label db: {err:?}")))?;

        let current_version_rows = sqlite_connection
            .query(
                "SELECT version_id, facet_heads_json FROM image_label_label_set_versions WHERE is_current = 1 ORDER BY version_id DESC LIMIT 1",
                &[],
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error loading label set cache version: {err:?}")))?;
        let current_version_row = current_version_rows.first();
        let current_version_id = current_version_row.and_then(|row| row_i64(row, "version_id"));
        let current_heads_json = current_version_row.and_then(|row| row_text(row, "facet_heads_json"));
        let needs_rebuild = current_version_id.is_none()
            || current_heads_json.as_deref() != Some(&config_heads_json);

        let active_version_id = if needs_rebuild {
            sqlite_connection
                .query("UPDATE image_label_label_set_versions SET is_current = 0 WHERE is_current = 1", &[])
                .map_err(|err| JobErrorX::Terminal(ferr!("error clearing current label set version: {err:?}")))?;
            sqlite_connection
                .query(
                    "INSERT INTO image_label_label_set_versions (facet_key, facet_heads_json, schema_version, model_tag, embedding_dim, is_current) VALUES (?1, ?2, ?3, ?4, ?5, 1)",
                    &[
                        SqlValue::Text(config_label_set_facet_key.clone()),
                        SqlValue::Text(config_heads_json.clone()),
                        SqlValue::Integer(LABEL_SET_CACHE_SCHEMA_VERSION),
                        SqlValue::Text(NOMIC_TEXT_MODEL_ID.to_string()),
                        SqlValue::Integer(768),
                    ],
                )
                .map_err(|err| JobErrorX::Terminal(ferr!("error inserting label set version row: {err:?}")))?;
            let version_rows = sqlite_connection
                .query("SELECT last_insert_rowid() AS rowid", &[])
                .map_err(|err| JobErrorX::Terminal(ferr!("error loading label set version rowid: {err:?}")))?;
            let version_id = version_rows
                .first()
                .and_then(|row| row_i64(row, "rowid"))
                .ok_or_else(|| JobErrorX::Terminal(ferr!("missing label set version rowid")))?;

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
            for prompt in image_label_null_anchor_prompts() {
                cache_rows.push(CacheSeedRow {
                    row_kind: "null_anchor".into(),
                    label: None,
                    description: (*prompt).into(),
                });
            }

            let mut prompt_vectors_by_label: std::collections::HashMap<String, Vec<Vec<f32>>> =
                std::collections::HashMap::new();
            let cache_row_count = cache_rows.len();
            for cache_row in cache_rows {
                let query_text = format!("search_query: {}", cache_row.description);
                let embed_result = mltools_embed::embed_text(&query_text)
                    .map_err(|err| JobErrorX::Terminal(ferr!("error embedding seed text: {err}")))?;
                if !embed_result.model_id.eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID)
                    || embed_result.dimensions != 768
                {
                    return Err(JobErrorX::Terminal(ferr!(
                        "unexpected seed embed model '{}'/dim {}",
                        embed_result.model_id,
                        embed_result.dimensions
                    )));
                }
                if cache_row.row_kind == "label_prompt" {
                    if let Some(label) = &cache_row.label {
                        prompt_vectors_by_label
                            .entry(label.clone())
                            .or_default()
                            .push(embed_result.vector.clone());
                    }
                }
                insert_cache_embedding_row(
                    sqlite_connection,
                    CacheEmbeddingRow {
                        label_set_version_id: version_id,
                        row_kind: &cache_row.row_kind,
                        label: cache_row.label.as_deref(),
                        description: &cache_row.description,
                        query_text: &query_text,
                        model_tag: &embed_result.model_id,
                        vector: &embed_result.vector,
                    },
                )?;
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
                    sqlite_connection,
                    CacheEmbeddingRow {
                        label_set_version_id: version_id,
                        row_kind: "label_centroid",
                        label: Some(&label.label),
                        description: &centroid_description,
                        query_text: &centroid_query,
                        model_tag: NOMIC_TEXT_MODEL_ID,
                        vector: &centroid,
                    },
                )?;
            }
            let _ = cache_row_count;
            version_id
        } else {
            current_version_id.expect("version should exist when cache is current")
        };

        let scored_rows = sqlite_connection
            .query(
                "SELECT m.rowid AS rowid, m.row_kind, m.label, m.description, (1.0 - vec_distance_cosine(v.embedding, vec_f32(?1))) AS score \
                 FROM image_label_prompt_meta m JOIN image_label_prompt_vec v ON v.rowid = m.rowid \
                 WHERE m.active = 1 AND m.label_set_version_id = ?2 AND m.model_tag = ?3 ORDER BY m.rowid",
                &[
                    SqlValue::Text(image_vec_json.clone()),
                    SqlValue::Integer(active_version_id),
                    SqlValue::Text(NOMIC_TEXT_MODEL_ID.to_string()),
                ],
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error loading scored image label rows: {err:?}")))?;
        if scored_rows.is_empty() {
            return Ok(Json(()));
        }

        let mut by_label: std::collections::HashMap<String, LabelAgg> = std::collections::HashMap::new();
        let mut null_max = f64::NEG_INFINITY;
        for row in &scored_rows {
            let row_kind = row_text(row, "row_kind")
                .ok_or_else(|| JobErrorX::Terminal(ferr!("scored row missing row_kind")))?;
            let score = row_real(row, "score")
                .ok_or_else(|| JobErrorX::Terminal(ferr!("scored row missing score")))?;
            if row_kind == "null_anchor" {
                if score > null_max {
                    null_max = score;
                }
                continue;
            }
            let label = row_opt_text(row, "label");
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
                agg.centroid_rowid = row_i64(row, "rowid");
                continue;
            }
            if row_kind == "label_prompt" {
                agg.prompt_scores.push(score);
            }
        }

        if by_label.is_empty() {
            return Ok(Json(()));
        }

        let mut candidates = Vec::new();
        for (label, agg) in &by_label {
            let prompt_count = agg.prompt_scores.len();
            if prompt_count == 0 {
                continue;
            }
            let centroid_score = agg.centroid_score.ok_or_else(|| {
                JobErrorX::Terminal(ferr!("missing centroid score for '{}'", label))
            })?;
            let centroid_rowid = agg.centroid_rowid.ok_or_else(|| {
                JobErrorX::Terminal(ferr!("missing centroid rowid for '{}'", label))
            })?;
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
            candidates.push(LabelCandidateMetrics {
                label: label.clone(),
                centroid_rowid,
                prompt_count,
                prompt_max,
                prompt_top2_mean,
                prompt_hit_ratio,
                centroid_score,
                null_margin,
                negative_prompt_margin,
                composite_score,
            });
        }

        candidates.sort_by(|left_candidate, right_candidate| {
            right_candidate
                .composite_score
                .partial_cmp(&left_candidate.composite_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if candidates.is_empty() {
            return Ok(Json(()));
        }

        let mut shortlisted: Vec<LabelCandidateMetrics> = Vec::new();
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
                    sqlite_connection,
                    candidate.centroid_rowid,
                    kept.centroid_rowid,
                )?;
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

        let Some(best_label) = shortlisted.first().map(|candidate| candidate.label.clone()) else {
            return Ok(Json(()));
        };

        let mut output_labels = shortlisted
            .iter()
            .take(MAX_MULTI_LABEL_OUTPUTS)
            .map(|candidate| candidate.label.clone())
            .collect::<Vec<_>>();
        if output_labels.is_empty() {
            output_labels.push(best_label);
        }

        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::PseudoLabel(output_labels).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating image label facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}

#[derive(Debug, Clone)]
struct CacheSeedRow {
    row_kind: String,
    label: Option<String>,
    description: String,
}

struct CacheEmbeddingRow<'a> {
    label_set_version_id: i64,
    row_kind: &'a str,
    label: Option<&'a str>,
    description: &'a str,
    query_text: &'a str,
    model_tag: &'a str,
    vector: &'a [f32],
}

fn default_image_pseudo_label_set() -> daybook_types::doc::PseudoLabelSetFacet {
    use daybook_types::doc::{PseudoLabelSetFacet, PseudoLabelSetLabel};
    PseudoLabelSetFacet {
        labels: vec![
            PseudoLabelSetLabel {
                label: IMAGE_LABEL_RECEIPT.to_string(),
                prompts: vec![
                    "a photo of a long printed grocery store receipt".into(),
                    "a photo of a paper receipt with itemized prices".into(),
                    "a close-up photo of a shopping receipt".into(),
                ],
                negative_prompts: vec![
                    "a shopping app screenshot showing items in a cart".into(),
                    "a printed invoice document with line items".into(),
                    "a restaurant menu with printed prices".into(),
                ],
            },
            PseudoLabelSetLabel {
                label: IMAGE_LABEL_TWITTER_SCREENSHOT.to_string(),
                prompts: vec![
                    "a screenshot of a tweet in the twitter app interface".into(),
                    "a social media post screenshot with twitter reply and like counts".into(),
                    "a screenshot of the x twitter timeline showing a tweet".into(),
                ],
                negative_prompts: vec![
                    "a messaging app chat screenshot conversation".into(),
                    "an email inbox screenshot with messages list".into(),
                    "a spreadsheet screenshot with rows and columns".into(),
                ],
            },
            PseudoLabelSetLabel {
                label: IMAGE_LABEL_MINECRAFT.to_string(),
                prompts: vec![
                    "a minecraft gameplay screenshot with blocky pixelated terrain".into(),
                    "a screenshot from the video game minecraft showing cubic blocks".into(),
                    "minecraft game scene with pixelated block world".into(),
                ],
                negative_prompts: vec![
                    "a realistic first person shooter game screenshot".into(),
                    "a cartoon mobile game screenshot with flat ui icons".into(),
                    "a desktop application window screenshot with menus".into(),
                ],
            },
        ],
    }
}

fn image_label_null_anchor_prompts() -> &'static [&'static str] {
    &[
        "a generic photo of an object",
        "a hard to describe and barren room",
        "a photo of a random assortment of things",
        "a vague picture with no discernable features",
        "a photo of an inscrutable poster",
    ]
}

fn insert_cache_embedding_row(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
    row: CacheEmbeddingRow<'_>,
) -> Result<i64, JobErrorX> {
    use crate::wit::townframe::sql::types::SqlValue;
    let embedding_json = embedding_vec_to_json(row.vector)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("error serializing cached embedding")))?;
    sqlite_connection
        .query(
            "INSERT INTO image_label_prompt_vec (embedding) VALUES (?1)",
            &[SqlValue::Text(embedding_json)],
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error inserting vec cache row: {err:?}")))?;
    let rowid_rows = sqlite_connection
        .query("SELECT last_insert_rowid() AS rowid", &[])
        .map_err(|err| JobErrorX::Terminal(ferr!("error reading cache vec rowid: {err:?}")))?;
    let rowid = rowid_rows
        .first()
        .and_then(|row| row_i64(row, "rowid"))
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing cache vec rowid")))?;
    sqlite_connection
        .query(
            "INSERT INTO image_label_prompt_meta (rowid, label_set_version_id, row_kind, label, description, query_text, model_tag, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
            &[
                SqlValue::Integer(rowid),
                SqlValue::Integer(row.label_set_version_id),
                SqlValue::Text(row.row_kind.to_string()),
                row.label
                    .map_or(SqlValue::Null, |value| SqlValue::Text(value.to_string())),
                SqlValue::Text(row.description.to_string()),
                SqlValue::Text(row.query_text.to_string()),
                SqlValue::Text(row.model_tag.to_string()),
            ],
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error inserting cache meta row: {err:?}")))?;
    Ok(rowid)
}

fn sqlite_vec_rowid_cosine_similarity(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
    left_rowid: i64,
    right_rowid: i64,
) -> Result<f64, JobErrorX> {
    use crate::wit::townframe::sql::types::SqlValue;
    let rows = sqlite_connection
        .query(
            "SELECT (1.0 - vec_distance_cosine(v1.embedding, v2.embedding)) AS score \
             FROM image_label_prompt_vec v1 JOIN image_label_prompt_vec v2 \
             ON v1.rowid = ?1 AND v2.rowid = ?2",
            &[
                SqlValue::Integer(left_rowid),
                SqlValue::Integer(right_rowid),
            ],
        )
        .map_err(|err| {
            JobErrorX::Terminal(ferr!(
                "error computing sqlite-vec rowid cosine similarity: {err:?}"
            ))
        })?;
    let Some(row) = rows.first() else {
        return Err(JobErrorX::Terminal(ferr!(
            "missing sqlite-vec rowid score row"
        )));
    };
    row_real(row, "score")
        .ok_or_else(|| JobErrorX::Terminal(ferr!("missing sqlite-vec rowid score value")))
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

fn row_i64(row: &crate::wit::townframe::sql::types::ResultRow, name: &str) -> Option<i64> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Integer(value)
            if entry.column_name == name =>
        {
            Some(*value)
        }
        _ => None,
    })
}

fn embedding_vec_to_json(values: &[f32]) -> Res<String> {
    let bytes = daybook_types::doc::embedding_f32_slice_to_le_bytes(values);
    daybook_types::doc::embedding_f32_bytes_to_json(&bytes, values.len() as u32)
}

fn embedding_bytes_to_f32(bytes: &[u8]) -> Res<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
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
    let first_vector = vectors.first()?;
    let dim = first_vector.len();
    if dim == 0 || vectors.iter().any(|vector| vector.len() != dim) {
        return None;
    }
    let mut centroid = vec![0.0_f32; dim];
    for vector in vectors {
        for (dst, src) in centroid.iter_mut().zip(vector) {
            *dst += *src;
        }
    }
    let count = vectors.len() as f32;
    for value in &mut centroid {
        *value /= count;
    }
    let norm = centroid
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
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

fn top_k_mean(scores: &[f64], top_count: usize) -> Option<f64> {
    if scores.is_empty() || top_count == 0 {
        return None;
    }
    let mut sorted_scores = scores.to_vec();
    sorted_scores.sort_by(|left_score, right_score| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let take_count = sorted_scores.len().min(top_count);
    let total = sorted_scores.iter().take(take_count).sum::<f64>();
    Some(total / (take_count as f64))
}
