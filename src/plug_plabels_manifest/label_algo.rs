use crate::interlude::*;
use crate::types::PseudoLabelCandidate;
use crate::types::PseudoLabelCandidatesFacet;

pub const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
pub const LABEL_SET_CACHE_SCHEMA_VERSION: i64 = 1;
pub const PROMPT_HIT_MIN_SCORE: f64 = 0.040;
const PROMPT_MAX_MIN_SCORE: f64 = 0.045;
const PROMPT_TOP2_MEAN_MIN_SCORE: f64 = 0.040;
const PROMPT_HIT_RATIO_MIN: f64 = 0.34;
const NULL_MARGIN_MIN: f64 = 0.000;
const NEGATIVE_PROMPT_MARGIN_MIN: f64 = 0.000;
const CENTROID_MIN_SCORE: f64 = 0.040;
const COMPOSITE_MIN_SCORE: f64 = 0.040;
pub const MAX_LABEL_CANDIDATES_TO_GAUNTLET: usize = 8;
pub const LABEL_SYNONYM_CENTROID_SIM_MAX: f64 = 0.97;
pub const MAX_MULTI_LABEL_OUTPUTS: usize = 4;

#[derive(Default)]
pub struct LabelAgg {
    pub prompt_scores: Vec<f64>,
    pub negative_prompt_max: f64,
    pub centroid_score: Option<f64>,
    pub centroid_rowid: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct LabelGauntletThresholds {
    pub prompt_max_min_score: f64,
    pub prompt_top2_mean_min_score: f64,
    pub prompt_hit_ratio_min: f64,
    pub centroid_min_score: f64,
    pub null_margin_min: f64,
    pub negative_prompt_margin_min: f64,
    pub composite_min_score: f64,
}

#[derive(Debug, Clone)]
pub struct LabelCandidateMetrics {
    pub label: String,
    pub centroid_rowid: i64,
    pub prompt_count: usize,
    pub prompt_max: f64,
    pub prompt_top2_mean: f64,
    pub prompt_hit_ratio: f64,
    pub centroid_score: f64,
    pub null_margin: f64,
    pub negative_prompt_margin: f64,
    pub composite_score: f64,
}

#[derive(Debug, Clone)]
pub struct LabelGauntletOutcome {
    pub passed: bool,
}

#[derive(Debug, Clone)]
pub struct CacheSeedRow {
    pub row_kind: String,
    pub label: Option<String>,
    pub description: String,
}

pub struct CacheEmbeddingRow<'a> {
    pub label_set_version_id: i64,
    pub row_kind: &'a str,
    pub label: Option<&'a str>,
    pub description: &'a str,
    pub query_text: &'a str,
    pub model_tag: &'a str,
    pub vector: &'a [f32],
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

pub fn thresholds_for_label(_label: &str) -> LabelGauntletThresholds {
    LabelGauntletThresholds::defaults()
}

pub fn run_label_gauntlet(
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

pub fn default_label_set() -> PseudoLabelCandidatesFacet {
    PseudoLabelCandidatesFacet {
        labels: vec![
            PseudoLabelCandidate {
                label: "receipt-image".to_string(),
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
            PseudoLabelCandidate {
                label: "twitter-screenshot".to_string(),
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
            PseudoLabelCandidate {
                label: "minecraft".to_string(),
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

pub fn with_heads_fragment(mut url: Url, heads: Option<&[String]>) -> Url {
    if let Some(heads) = heads
        && !heads.is_empty()
    {
        url.set_fragment(Some(&heads.join("|")));
    }
    url
}

pub fn null_anchor_prompts() -> &'static [&'static str] {
    &[
        "a generic photo of an object",
        "a hard to describe and barren room",
        "a photo of a random assortment of things",
        "a vague picture with no discernable features",
        "a photo of an inscrutable poster",
    ]
}

pub fn embedding_vec_to_json(values: &[f32]) -> Res<String> {
    let bytes = daybook_types::doc::embedding_f32_slice_to_le_bytes(values);
    daybook_types::doc::embedding_f32_bytes_to_json(&bytes, values.len() as u32)
}

pub fn mean_normalized(vectors: &[Vec<f32>]) -> Option<Vec<f32>> {
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

pub fn top_k_mean(scores: &[f64], top_count: usize) -> Option<f64> {
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
