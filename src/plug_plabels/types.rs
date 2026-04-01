#![allow(clippy::enum_variant_names)]

use crate::interlude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PseudoLabelCandidate {
    pub label: String,
    pub prompts: Vec<String>,
    pub negative_prompts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PseudoLabelEntry {
    pub label: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PseudoLabelError {
    NoHit {
        reason: String,
        algorithm_tag: String,
        source_ref: Url,
        candidate_set_ref: Url,
        top_candidate_label: Option<String>,
        top_candidate_score: Option<f64>,
    },
}

daybook_types::define_enum_and_tag!(
    "org.example.daybook.plabel.",
    #[allow(clippy::enum_variant_names)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, schemars::JsonSchema)]
    PlabelFacetTag,
    #[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
    #[serde(rename_all = "camelCase", untagged)]
    PlabelFacet {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        "pseudo-label" PseudoLabel struct {
            pub algorithm_tag: String,
            pub top_score: f64,
            pub labels: Vec<PseudoLabelEntry>,
            pub source_ref: Url,
            pub candidate_set_ref: Url,
        },
        "pseudo-label-error" PseudoLabelErrorFacet type (PseudoLabelError),
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        "pseudo-label-candidates" PseudoLabelCandidatesFacet struct {
            pub labels: Vec<PseudoLabelCandidate>,
        }
    }
);

#[allow(dead_code)]
pub fn pseudo_label_key() -> daybook_types::doc::FacetKey {
    daybook_types::doc::FacetKey::from(daybook_types::doc::FacetTag::Any(
        PlabelFacetTag::PseudoLabel.as_str().into(),
    ))
}

#[allow(dead_code)]
pub fn pseudo_label_candidates_key(id: &str) -> daybook_types::doc::FacetKey {
    daybook_types::doc::FacetKey {
        tag: daybook_types::doc::FacetTag::Any(
            PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
        ),
        id: id.into(),
    }
}

#[allow(dead_code)]
pub fn pseudo_label_error_key() -> daybook_types::doc::FacetKey {
    daybook_types::doc::FacetKey::from(daybook_types::doc::FacetTag::Any(
        PlabelFacetTag::PseudoLabelErrorFacet.as_str().into(),
    ))
}
