use super::super::*;
use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

const NOMIC_VISION_MODEL_ID: &str = "nomic-ai/nomic-embed-vision-v1.5";
const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const IMAGE_LABEL_RECEIPT: &str = "receipt-image";
const IMAGE_LABEL_TWITTER_SCREENSHOT: &str = "twitter-screenshot";
const IMAGE_LABEL_MINECRAFT: &str = "minecraft";
const IMAGE_LABEL_LOCAL_STATE_KEY: &str = "@daybook/wip/image-label-classifier";
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
struct SeedRow {
    row_kind: &'static str,
    label: Option<&'static str>,
    description: &'static str,
}

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
            prompt_scores: Vec<f64>,
            negative_prompt_max: f64,
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
            let agg = by_label.entry(label.clone()).or_insert_with(|| LabelAgg {
                negative_prompt_max: f64::NEG_INFINITY,
                ..Default::default()
            });
            if row.row_kind == "negative_prompt" {
                if score > agg.negative_prompt_max {
                    agg.negative_prompt_max = score;
                }
                continue;
            }
            if row.row_kind != "label_prompt" {
                continue;
            }
            agg.prompt_scores.push(score);
            agg.prompt_vectors.push(row.embedding_vec.clone());
            let _ = &row.description;
        }

        if by_label.is_empty() {
            return Ok(Json(()));
        }

        let mut centroid_scores: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        let mut centroids_by_label: std::collections::HashMap<String, Vec<f32>> =
            std::collections::HashMap::new();
        for (label, agg) in &by_label {
            let centroid = mean_normalized(&agg.prompt_vectors)
                .ok_or_else(|| JobErrorX::Terminal(ferr!("cannot compute centroid for label '{}'", label)))?;
            let score = cosine_similarity_f32(&image_vec, &centroid)
                .ok_or_else(|| JobErrorX::Terminal(ferr!("invalid centroid dims for '{}'", label)))?;
            centroids_by_label.insert(label.clone(), centroid);
            centroid_scores.insert(label.clone(), score);
        }

        let mut candidates = Vec::new();
        for (label, agg) in &by_label {
            let prompt_count = agg.prompt_scores.len();
            if prompt_count == 0 {
                continue;
            }
            let centroid_score = *centroid_scores
                .get(label)
                .ok_or_else(|| JobErrorX::Terminal(ferr!("missing centroid score for '{}'", label)))?;
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

            let candidate_centroid = centroids_by_label.get(&candidate.label).ok_or_else(|| {
                JobErrorX::Terminal(ferr!("missing centroid vector for '{}'", candidate.label))
            })?;

            let mut suppressed_as_synonym = false;
            for kept in &shortlisted {
                let kept_centroid = centroids_by_label.get(&kept.label).ok_or_else(|| {
                    JobErrorX::Terminal(ferr!("missing centroid vector for '{}'", kept.label))
                })?;
                let centroid_similarity = cosine_similarity_f32(candidate_centroid, kept_centroid)
                    .ok_or_else(|| {
                        JobErrorX::Terminal(ferr!(
                            "invalid centroid dims comparing '{}' and '{}'",
                            candidate.label,
                            kept.label
                        ))
                    })?;
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

        let new_facet: daybook_types::doc::FacetRaw = WellKnownFacet::PseudoLabel(output_labels).into();
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
            label: Some(IMAGE_LABEL_RECEIPT),
            description: "a photo of a long printed grocery store receipt",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_RECEIPT),
            description: "a photo of a paper receipt with itemized prices",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_RECEIPT),
            description: "a close-up photo of a shopping receipt",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_RECEIPT),
            description: "a shopping app screenshot showing items in a cart",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_RECEIPT),
            description: "a printed invoice document with line items",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_RECEIPT),
            description: "a restaurant menu with printed prices",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_TWITTER_SCREENSHOT),
            description: "a screenshot of a tweet in the twitter app interface",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_TWITTER_SCREENSHOT),
            description: "a social media post screenshot with twitter reply and like counts",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_TWITTER_SCREENSHOT),
            description: "a screenshot of the x twitter timeline showing a tweet",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_TWITTER_SCREENSHOT),
            description: "a messaging app chat screenshot conversation",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_TWITTER_SCREENSHOT),
            description: "an email inbox screenshot with messages list",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_TWITTER_SCREENSHOT),
            description: "a spreadsheet screenshot with rows and columns",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_MINECRAFT),
            description: "a minecraft gameplay screenshot with blocky pixelated terrain",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_MINECRAFT),
            description: "a screenshot from the video game minecraft showing cubic blocks",
        },
        SeedRow {
            row_kind: "label_prompt",
            label: Some(IMAGE_LABEL_MINECRAFT),
            description: "minecraft game scene with pixelated block world",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_MINECRAFT),
            description: "a realistic first person shooter game screenshot",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_MINECRAFT),
            description: "a cartoon mobile game screenshot with flat ui icons",
        },
        SeedRow {
            row_kind: "negative_prompt",
            label: Some(IMAGE_LABEL_MINECRAFT),
            description: "a desktop application window screenshot with menus",
        },
        // FIXME: these null anchors suck, we'll
        SeedRow {
            row_kind: "null_anchor",
            label: None,
            description: "a generic photo of an object",
        },
        SeedRow {
            row_kind: "null_anchor",
            label: None,
            description: "a hard to describe and barren room",
        },
        SeedRow {
            row_kind: "null_anchor",
            label: None,
            description: "a photo of a random assortment of things",
        },
        SeedRow {
            row_kind: "null_anchor",
            label: None,
            description: "a vague picture with no discernable features",
        },
        SeedRow {
            row_kind: "null_anchor",
            label: None,
            description: "a photo of an inscrutable poster",
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

fn cosine_similarity_f32(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (left_value, right_value) in left.iter().zip(right) {
        let left_value = f64::from(*left_value);
        let right_value = f64::from(*right_value);
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some(dot / (left_norm.sqrt() * right_norm.sqrt()))
}
