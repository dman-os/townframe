use crate::interlude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PseudoLabelCandidate {
    pub label: String,
    pub prompts: Vec<String>,
    pub negative_prompts: Vec<String>,
}

daybook_types::define_enum_and_tag!(
    "org.example.daybook.plabel.",
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, schemars::JsonSchema)]
    PlabelFacetTag,
    #[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
    #[serde(rename_all = "camelCase", untagged)]
    PlabelFacet {
        "pseudo-label" PseudoLabel type (Vec<String>),
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        "pseudo-label-candidates" PseudoLabelCandidatesFacet struct {
            pub labels: Vec<PseudoLabelCandidate>,
        }
    }
);

pub fn pseudo_label_key() -> daybook_types::doc::FacetKey {
    daybook_types::doc::FacetKey::from(daybook_types::doc::FacetTag::Any(
        PlabelFacetTag::PseudoLabel.as_str().into(),
    ))
}

pub fn pseudo_label_candidates_key(id: &str) -> daybook_types::doc::FacetKey {
    daybook_types::doc::FacetKey {
        tag: daybook_types::doc::FacetTag::Any(
            PlabelFacetTag::PseudoLabelCandidatesFacet.as_str().into(),
        ),
        id: id.into(),
    }
}
