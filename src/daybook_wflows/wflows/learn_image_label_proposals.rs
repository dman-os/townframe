use super::super::*;
use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

const NOMIC_VISION_MODEL_ID: &str = "nomic-ai/nomic-embed-vision-v1.5";
const NOMIC_TEXT_MODEL_ID: &str = "nomic-ai/nomic-embed-text-v1.5";
const PROPOSAL_SET_CONFIG_FACET_ID: &str = "daybook_wip_learned_image_label_proposals";
const LOCAL_STATE_KEY: &str = "@daybook/wip/learned-image-label-proposals";
const DOWNSIZE_MAX_SIDE: u32 = 896;
const DOWNSIZE_JPEG_QUALITY: u8 = 80;
const DEDUPE_CENTROID_SIM_MIN: f64 = 0.92;
const PROMPTS_MIN_COUNT: usize = 2;
const PROMPTS_MAX_COUNT_PER_SIDE: usize = 6;
const PROMPTS_MAX_COUNT_PER_LABEL: usize = 12;

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let mut args = facet_routine::get_args();
    let _working_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &args.facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    let embedding_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Embedding).to_string();
    let blob_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Blob).to_string();

    let mut ro_facet_tokens = std::mem::take(&mut args.ro_facet_tokens);
    let embedding_facet_token = tuple_list_take(&mut ro_facet_tokens, &embedding_facet_key)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "embedding facet key '{}' not found in ro_facet_tokens",
                embedding_facet_key
            ))
        })?;
    let blob_facet_token =
        tuple_list_take(&mut ro_facet_tokens, &blob_facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "blob facet key '{}' not found in ro_facet_tokens",
                blob_facet_key
            ))
        })?;

    let sqlite_connection = tuple_list_get(&args.sqlite_connections, LOCAL_STATE_KEY)
        .or_else(|| args.sqlite_connections.first().map(|(_, token)| token))
        .ok_or_else(|| JobErrorX::Terminal(ferr!("no sqlite connection available")))?;

    let config_facet_key = daybook_types::doc::FacetKey {
        tag: daybook_types::doc::FacetTag::WellKnown(WellKnownFacetTag::PseudoLabelCandidates),
        id: PROPOSAL_SET_CONFIG_FACET_ID.into(),
    }
    .to_string();
    let rw_config_token = tuple_list_get(&args.rw_config_facet_tokens, &config_facet_key);
    let ro_config_token = tuple_list_get(&args.ro_config_facet_tokens, &config_facet_key);
    if rw_config_token.is_none() && ro_config_token.is_none() {
        return Ok(());
    }

    let embedding_raw = embedding_facet_token.get();
    let embedding_json: daybook_types::doc::FacetRaw = serde_json::from_str(&embedding_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing embedding facet json: {err}")))?;
    let embedding = match WellKnownFacet::from_json(embedding_json, WellKnownFacetTag::Embedding)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not embedding")))?
    {
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

    let blob_raw = blob_facet_token.get();
    let blob_json: daybook_types::doc::FacetRaw = serde_json::from_str(&blob_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing blob facet json: {err}")))?;
    let blob = match WellKnownFacet::from_json(blob_json, WellKnownFacetTag::Blob)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not blob")))?
    {
        WellKnownFacet::Blob(value) => value,
        _ => unreachable!(),
    };
    if !blob.mime.starts_with("image/") {
        return Ok(());
    }

    cx.effect(|| {
        use crate::wit::townframe::daybook::{mltools_image_tools, mltools_llm_chat};

        let mut proposal_set = load_or_init_proposal_set(rw_config_token, ro_config_token)?;
        ensure_embedding_cache_schema(sqlite_connection)?;

        let downsized = mltools_image_tools::downsize_image_from_blob(
            blob_facet_token,
            DOWNSIZE_MAX_SIDE,
            DOWNSIZE_JPEG_QUALITY,
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error downsizing image: {err}")))?;

        let prompt = build_multimodal_prompt();
        let llm_text =
            match mltools_llm_chat::llm_chat_multimodal(&prompt, &downsized.bytes, &downsized.mime)
            {
                Ok(value) => value,
                Err(err) => {
                    return Err(JobErrorX::Terminal(ferr!(
                        "error calling multimodal llm: {err}"
                    )))
                }
            };

        let Some(parsed_proposal) = parse_llm_answer(&llm_text) else {
            return Ok(Json(()));
        };
        let Some(new_label) = validate_and_normalize_proposal(parsed_proposal) else {
            return Ok(Json(()));
        };

        let merged = merge_label_proposal_with_dedupe(sqlite_connection, &proposal_set, new_label)?;
        if merged != proposal_set {
            proposal_set = merged;
            if let Some(token) = rw_config_token {
                let facet_raw: daybook_types::doc::FacetRaw =
                    daybook_types::doc::WellKnownFacet::PseudoLabelCandidates(proposal_set).into();
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

fn build_multimodal_prompt() -> String {
    r#"You are labeling an image into a reusable concept.

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
- positive prompts describe the generic visual concept
- negative prompts are hard negatives (visually similar but different concept)
- do not describe one-off specifics unique to this exact image
- prefer concrete visual categories (receipt_image, twitter_screenshot, minecraft)

Example:
<answer>
  <label>receipt_image</label>
  <positive_prompts>
    <prompt>a photo of a printed receipt with itemized prices</prompt>
    <prompt>a paper receipt photographed on a table</prompt>
    <prompt>a shopping receipt image with line items and totals</prompt>
  </positive_prompts>
  <negative_prompts>
    <prompt>a shopping app cart screenshot with product thumbnails</prompt>
    <prompt>an invoice document page with business header and tables</prompt>
    <prompt>a restaurant menu with printed food items and prices</prompt>
  </negative_prompts>
</answer>
"#
    .to_string()
}

fn load_or_init_proposal_set(
    rw_config_token: Option<&crate::wit::townframe::daybook::capabilities::FacetTokenRw>,
    ro_config_token: Option<&crate::wit::townframe::daybook::capabilities::FacetTokenRo>,
) -> Result<daybook_types::doc::PseudoLabelCandidatesFacet, JobErrorX> {
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    if let Some(token) = rw_config_token {
        if token.exists() {
            let raw = token.get();
            let facet_raw: daybook_types::doc::FacetRaw =
                serde_json::from_str(&raw).map_err(|err| {
                    JobErrorX::Terminal(ferr!(
                        "error parsing config proposal set facet json: {err}"
                    ))
                })?;
            return match WellKnownFacet::from_json(facet_raw, WellKnownFacetTag::PseudoLabelCandidates)
                .map_err(|err| {
                    JobErrorX::Terminal(err.wrap_err("config facet is not PseudoLabelCandidates"))
                })? {
                WellKnownFacet::PseudoLabelCandidates(value) => Ok(value),
                _ => unreachable!(),
            };
        }

        let value = daybook_types::doc::PseudoLabelCandidatesFacet { labels: vec![] };
        let facet_raw: daybook_types::doc::FacetRaw =
            WellKnownFacet::PseudoLabelCandidates(value.clone()).into();
        let facet_raw = serde_json::to_string(&facet_raw).expect(ERROR_JSON);
        token
            .update(&facet_raw)
            .wrap_err("error writing default learned proposal set")
            .map_err(JobErrorX::Terminal)?;
        return Ok(value);
    }

    if let Some(token) = ro_config_token {
        if !token.exists() {
            return Ok(daybook_types::doc::PseudoLabelCandidatesFacet { labels: vec![] });
        }
        let raw = token.get();
        let facet_raw: daybook_types::doc::FacetRaw =
            serde_json::from_str(&raw).map_err(|err| {
                JobErrorX::Terminal(ferr!(
                    "error parsing ro config proposal set facet json: {err}"
                ))
            })?;
        return match WellKnownFacet::from_json(facet_raw, WellKnownFacetTag::PseudoLabelCandidates)
            .map_err(|err| {
                JobErrorX::Terminal(err.wrap_err("ro config facet is not PseudoLabelCandidates"))
            })? {
            WellKnownFacet::PseudoLabelCandidates(value) => Ok(value),
            _ => unreachable!(),
        };
    }

    Ok(daybook_types::doc::PseudoLabelCandidatesFacet { labels: vec![] })
}

fn ensure_embedding_cache_schema(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
) -> Result<(), JobErrorX> {
    sqlite_connection
        .query_batch(
            r#"
            CREATE TABLE IF NOT EXISTS learned_image_label_text_embedding_cache (
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

#[derive(Debug, Clone)]
struct RawProposal {
    label: String,
    positive_prompts: Vec<String>,
    negative_prompts: Vec<String>,
}

#[derive(Debug, Clone)]
struct NormalizedProposal {
    label: String,
    prompts: Vec<String>,
    negative_prompts: Vec<String>,
}

fn parse_llm_answer(text: &str) -> Option<RawProposal> {
    let answer = extract_tag_block(text, "answer")?;
    let label = extract_tag_block(answer, "label")?.trim().to_string();
    let positive_block = extract_tag_block(answer, "positive_prompts")?;
    let negative_block = extract_tag_block(answer, "negative_prompts")?;
    let positive_prompts = extract_repeated_tag_blocks(positive_block, "prompt");
    let negative_prompts = extract_repeated_tag_blocks(negative_block, "prompt");
    Some(RawProposal {
        label,
        positive_prompts,
        negative_prompts,
    })
}

fn extract_tag_block<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open)? + open.len();
    let rest = &text[start..];
    let end = rest.find(&close)?;
    Some(&rest[..end])
}

fn extract_repeated_tag_blocks(text: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut cursor = text;
    while let Some(start_ix) = cursor.find(&open) {
        let start = start_ix + open.len();
        let after_open = &cursor[start..];
        let Some(end_ix) = after_open.find(&close) else {
            break;
        };
        out.push(after_open[..end_ix].trim().to_string());
        cursor = &after_open[end_ix + close.len()..];
    }
    out
}

fn validate_and_normalize_proposal(raw: RawProposal) -> Option<NormalizedProposal> {
    let label = normalize_label(&raw.label);
    if label.is_empty() {
        return None;
    }
    let prompts = normalize_prompt_list(raw.positive_prompts, PROMPTS_MAX_COUNT_PER_SIDE);
    let negative_prompts = normalize_prompt_list(raw.negative_prompts, PROMPTS_MAX_COUNT_PER_SIDE);
    if prompts.len() < PROMPTS_MIN_COUNT || negative_prompts.len() < PROMPTS_MIN_COUNT {
        return None;
    }
    Some(NormalizedProposal {
        label,
        prompts,
        negative_prompts,
    })
}

fn normalize_label(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    let mut last_was_underscore = false;
    for ch in label.trim().chars() {
        let ch = ch.to_ascii_lowercase();
        let mapped = if ch.is_ascii_alphanumeric() {
            Some(ch)
        } else if ch == '_' || ch == '-' || ch.is_ascii_whitespace() {
            Some('_')
        } else {
            None
        };
        let Some(mapped) = mapped else { continue };
        if mapped == '_' {
            if last_was_underscore || out.is_empty() {
                continue;
            }
            last_was_underscore = true;
            out.push('_');
        } else {
            last_was_underscore = false;
            out.push(mapped);
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

fn normalize_prompt_list(prompts: Vec<String>, cap: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for prompt in prompts {
        let prompt = collapse_whitespace(prompt.trim());
        if prompt.is_empty() {
            continue;
        }
        let dedupe_key = prompt.to_ascii_lowercase();
        if !seen.insert(dedupe_key) {
            continue;
        }
        out.push(prompt);
        if out.len() >= cap {
            break;
        }
    }
    out
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[derive(Debug, Clone)]
struct ProposalNode {
    label: daybook_types::doc::PseudoLabelCandidate,
    centroid: Vec<f32>,
    is_new: bool,
}

fn merge_label_proposal_with_dedupe(
    sqlite_connection: &crate::wit::townframe::daybook::sqlite_connection::Connection,
    existing: &daybook_types::doc::PseudoLabelCandidatesFacet,
    new_label: NormalizedProposal,
) -> Result<daybook_types::doc::PseudoLabelCandidatesFacet, JobErrorX> {
    let mut nodes = Vec::with_capacity(existing.labels.len() + 1);
    for label in &existing.labels {
        let centroid = proposal_centroid(sqlite_connection, &label.prompts)?;
        nodes.push(ProposalNode {
            label: label.clone(),
            centroid,
            is_new: false,
        });
    }
    let new_node = daybook_types::doc::PseudoLabelCandidate {
        label: new_label.label,
        prompts: new_label.prompts,
        negative_prompts: new_label.negative_prompts,
    };
    let centroid = proposal_centroid(sqlite_connection, &new_node.prompts)?;
    nodes.push(ProposalNode {
        label: new_node,
        centroid,
        is_new: true,
    });

    let mut dsu = Dsu::new(nodes.len());
    for left_ix in 0..nodes.len() {
        for right_ix in (left_ix + 1)..nodes.len() {
            let same_label = normalize_label(&nodes[left_ix].label.label)
                == normalize_label(&nodes[right_ix].label.label);
            let sim = cosine_similarity(&nodes[left_ix].centroid, &nodes[right_ix].centroid);
            if same_label || sim >= DEDUPE_CENTROID_SIM_MIN {
                dsu.union(left_ix, right_ix);
            }
        }
    }

    let mut clusters: Vec<(usize, Vec<usize>)> = Vec::new();
    for node_ix in 0..nodes.len() {
        let root = dsu.find(node_ix);
        if let Some((_, members)) = clusters
            .iter_mut()
            .find(|(cluster_root, _)| *cluster_root == root)
        {
            members.push(node_ix);
        } else {
            clusters.push((root, vec![node_ix]));
        }
    }

    let mut merged_labels = Vec::with_capacity(clusters.len());
    for (_, members) in clusters {
        merged_labels.push(merge_cluster_labels(&nodes, &members));
    }

    // Preserve existing-first ordering across clusters, then any brand-new cluster at the end.
    merged_labels.sort_by_key(|label| {
        existing
            .labels
            .iter()
            .position(|entry| normalize_label(&entry.label) == normalize_label(&label.label))
            .unwrap_or(usize::MAX)
    });

    Ok(daybook_types::doc::PseudoLabelCandidatesFacet {
        labels: merged_labels,
    })
}

fn merge_cluster_labels(
    nodes: &[ProposalNode],
    members: &[usize],
) -> daybook_types::doc::PseudoLabelCandidate {
    let mut canonical_label = None::<String>;
    let mut canonical_is_new = true;
    for &member_ix in members {
        let candidate = normalize_label(&nodes[member_ix].label.label);
        if candidate.is_empty() {
            continue;
        }
        let candidate_is_new = nodes[member_ix].is_new;
        let prefer = match &canonical_label {
            None => true,
            Some(current) => {
                (!candidate_is_new && canonical_is_new)
                    || (candidate_is_new == canonical_is_new && candidate.len() < current.len())
            }
        };
        if prefer {
            canonical_label = Some(candidate);
            canonical_is_new = candidate_is_new;
        }
    }

    let mut prompts = Vec::new();
    let mut negative_prompts = Vec::new();
    append_dedup_prompts(
        &mut prompts,
        members
            .iter()
            .flat_map(|&ix| nodes[ix].label.prompts.iter()),
        PROMPTS_MAX_COUNT_PER_LABEL,
    );
    append_dedup_prompts(
        &mut negative_prompts,
        members
            .iter()
            .flat_map(|&ix| nodes[ix].label.negative_prompts.iter()),
        PROMPTS_MAX_COUNT_PER_LABEL,
    );

    daybook_types::doc::PseudoLabelCandidate {
        label: canonical_label.unwrap_or_else(|| "image".to_string()),
        prompts,
        negative_prompts,
    }
}

fn append_dedup_prompts<'a>(
    out: &mut Vec<String>,
    inputs: impl Iterator<Item = &'a String>,
    max_count: usize,
) {
    let mut seen = std::collections::HashSet::new();
    for value in out.iter() {
        seen.insert(value.to_ascii_lowercase());
    }
    for input in inputs {
        let value = collapse_whitespace(input.trim());
        if value.is_empty() {
            continue;
        }
        if !seen.insert(value.to_ascii_lowercase()) {
            continue;
        }
        out.push(value);
        if out.len() >= max_count {
            break;
        }
    }
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
            "SELECT model_tag, dim, vector FROM learned_image_label_text_embedding_cache WHERE query_text = ?1",
            &[SqlValue::Text(query_text.to_string())],
        )
        .map_err(|err| JobErrorX::Terminal(ferr!("error querying learned label embedding cache: {err:?}")))?;
    if let Some(row) = cache_rows.first() {
        let model_tag = row_text(row, "model_tag").unwrap_or_default();
        let dim = row_i64(row, "dim").unwrap_or_default();
        let vector_bytes = row_blob(row, "vector").unwrap_or_default();
        let vector = embedding_bytes_to_f32(&vector_bytes).map_err(|err| {
            JobErrorX::Terminal(err.wrap_err("invalid cached learned label embedding bytes"))
        })?;
        if model_tag.eq_ignore_ascii_case(NOMIC_TEXT_MODEL_ID) && dim == (vector.len() as i64) {
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
            "INSERT INTO learned_image_label_text_embedding_cache (query_text, model_tag, dim, vector) \
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

fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return -1.0;
    }
    left.iter()
        .zip(right)
        .map(|(left_value, right_value)| f64::from(*left_value) * f64::from(*right_value))
        .sum()
}

fn mean_normalized(vectors: &[Vec<f32>]) -> Option<Vec<f32>> {
    let first = vectors.first()?;
    if first.is_empty() {
        return None;
    }
    let dim = first.len();
    if vectors.iter().any(|vector| vector.len() != dim) {
        return None;
    }
    let mut centroid = vec![0.0_f32; dim];
    for vector in vectors {
        for (dst, src) in centroid.iter_mut().zip(vector) {
            *dst += *src;
        }
    }
    let inv_count = 1.0_f32 / (vectors.len() as f32);
    for value in &mut centroid {
        *value *= inv_count;
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

#[derive(Debug, Clone)]
struct Dsu {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl Dsu {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
            rank: vec![0; len],
        }
    }

    fn find(&mut self, ix: usize) -> usize {
        if self.parent[ix] != ix {
            let root = self.find(self.parent[ix]);
            self.parent[ix] = root;
        }
        self.parent[ix]
    }

    fn union(&mut self, left: usize, right: usize) {
        let mut left_root = self.find(left);
        let mut right_root = self.find(right);
        if left_root == right_root {
            return;
        }
        if self.rank[left_root] < self.rank[right_root] {
            std::mem::swap(&mut left_root, &mut right_root);
        }
        self.parent[right_root] = left_root;
        if self.rank[left_root] == self.rank[right_root] {
            self.rank[left_root] = self.rank[left_root].saturating_add(1);
        }
    }
}

fn row_text(row: &crate::wit::townframe::sql::types::ResultRow, name: &str) -> Option<String> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Text(value) if entry.column_name == name => {
            Some(value.clone())
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

fn row_blob(row: &crate::wit::townframe::sql::types::ResultRow, name: &str) -> Option<Vec<u8>> {
    row.iter().find_map(|entry| match &entry.value {
        crate::wit::townframe::sql::types::SqlValue::Blob(value) if entry.column_name == name => {
            Some(value.clone())
        }
        _ => None,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_label_snake_case() {
        assert_eq!(normalize_label("Twitter Screenshot"), "twitter_screenshot");
        assert_eq!(normalize_label("foo---bar"), "foo_bar");
        assert_eq!(normalize_label("  bad*&^label  "), "badlabel");
    }

    #[test]
    fn parse_valid_answer() {
        let parsed = parse_llm_answer(
            r#"<answer><label>twitter_screenshot</label><positive_prompts><prompt>a screenshot of a tweet</prompt><prompt>twitter post ui screenshot</prompt></positive_prompts><negative_prompts><prompt>email inbox screenshot</prompt><prompt>spreadsheet screenshot</prompt></negative_prompts></answer>"#,
        )
        .expect("parsed");
        assert_eq!(parsed.label, "twitter_screenshot");
        assert_eq!(parsed.positive_prompts.len(), 2);
        assert_eq!(parsed.negative_prompts.len(), 2);
    }

    #[test]
    fn parse_missing_tag_fails() {
        assert!(parse_llm_answer("<answer><label>x</label></answer>").is_none());
    }

    #[test]
    fn prompt_dedupe_and_cap() {
        let prompts = normalize_prompt_list(
            vec![
                " Hello   world ".to_string(),
                "hello world".to_string(),
                "Two".to_string(),
                "Three".to_string(),
            ],
            2,
        );
        assert_eq!(prompts, vec!["Hello world".to_string(), "Two".to_string()]);
    }

    #[test]
    fn dsu_unions_clusters() {
        let mut dsu = Dsu::new(4);
        dsu.union(0, 1);
        dsu.union(2, 3);
        dsu.union(1, 2);
        let root = dsu.find(0);
        assert_eq!(root, dsu.find(3));
    }

    #[test]
    fn cosine_similarity_basic() {
        let sim = cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]);
        assert!((sim - 1.0).abs() < 1e-6);
    }
}
