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

daybook_types::define_enum_and_tag!(
    "org.example.daybook.plabel.",
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
            pub source_ref: String,
            pub candidate_set_ref: String,
        },
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
