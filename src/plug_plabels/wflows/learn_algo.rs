use crate::interlude::*;
use crate::types::{PseudoLabelCandidate, PseudoLabelCandidatesFacet};

const DEDUPE_CENTROID_SIM_MIN: f64 = 0.92;
const PROMPTS_MIN_COUNT: usize = 2;
const PROMPTS_MAX_COUNT_PER_SIDE: usize = 6;
const PROMPTS_MAX_COUNT_PER_LABEL: usize = 12;

#[derive(Debug, Clone)]
pub struct RawProposal {
    pub label: String,
    pub positive_prompts: Vec<String>,
    pub negative_prompts: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NormalizedProposal {
    pub label: String,
    pub prompts: Vec<String>,
    pub negative_prompts: Vec<String>,
}

pub fn parse_llm_answer(text: &str) -> Option<RawProposal> {
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

pub fn validate_and_normalize_proposal(raw: RawProposal) -> Option<NormalizedProposal> {
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

pub fn merge_label_proposal_with_dedupe(
    existing: &PseudoLabelCandidatesFacet,
    new_label: NormalizedProposal,
    mut centroid_for_prompts: impl FnMut(&[String]) -> Res<Vec<f32>>,
) -> Res<PseudoLabelCandidatesFacet> {
    let mut nodes = Vec::with_capacity(existing.labels.len() + 1);
    for label in &existing.labels {
        let centroid = centroid_for_prompts(&label.prompts)?;
        nodes.push(ProposalNode {
            label: label.clone(),
            centroid,
            is_new: false,
        });
    }
    let new_node = PseudoLabelCandidate {
        label: new_label.label,
        prompts: new_label.prompts,
        negative_prompts: new_label.negative_prompts,
    };
    let centroid = centroid_for_prompts(&new_node.prompts)?;
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

    merged_labels.sort_by_key(|label| {
        existing
            .labels
            .iter()
            .position(|entry| normalize_label(&entry.label) == normalize_label(&label.label))
            .unwrap_or(usize::MAX)
    });

    Ok(PseudoLabelCandidatesFacet {
        labels: merged_labels,
    })
}

pub fn mean_normalized(vectors: &[Vec<f32>]) -> Option<Vec<f32>> {
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

pub fn normalize_label(label: &str) -> String {
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

fn merge_cluster_labels(nodes: &[ProposalNode], members: &[usize]) -> PseudoLabelCandidate {
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

    PseudoLabelCandidate {
        label: canonical_label.unwrap_or_else(|| "label".to_string()),
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

fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return -1.0;
    }
    let dot = left
        .iter()
        .zip(right)
        .map(|(left_value, right_value)| f64::from(*left_value) * f64::from(*right_value))
        .sum::<f64>();
    let left_norm = left
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    let right_norm = right
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        return -1.0;
    }
    (dot / (left_norm * right_norm)).clamp(-1.0, 1.0)
}

#[derive(Debug, Clone)]
struct ProposalNode {
    label: PseudoLabelCandidate,
    centroid: Vec<f32>,
    is_new: bool,
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
    fn merge_proposal_preserves_existing_first() {
        let existing = PseudoLabelCandidatesFacet {
            labels: vec![PseudoLabelCandidate {
                label: "receipt_image".into(),
                prompts: vec!["receipt prompt".into(), "receipt prompt 2".into()],
                negative_prompts: vec!["invoice".into(), "menu".into()],
            }],
        };
        let new_label = NormalizedProposal {
            label: "receipt_image".into(),
            prompts: vec!["receipt prompt 3".into(), "receipt prompt 4".into()],
            negative_prompts: vec!["shopping app".into(), "spreadsheet".into()],
        };

        let merged = merge_label_proposal_with_dedupe(&existing, new_label, |prompts| {
            let vector = vec![prompts.len() as f32, 1.0];
            Ok(vector)
        })
        .unwrap();

        assert_eq!(merged.labels.len(), 1);
        assert_eq!(merged.labels[0].label, "receipt_image");
    }
}
