//! The dpath facet value: what a claim declares about its scope (FDR 001 §2).

use crate::interlude::*;

use serde::de::Error as DeError;

use crate::url::parse_facet_ref;

/// The value of a dpath facet (FDR 001 §2).
///
/// An empty target list is the **default whole-document scope**: the lens set
/// materializes every user-visible facet of the doc at that path. A non-empty
/// list is a **selective claim** naming the facets that materialize.
///
/// The facet stays a pure address: no lens hints, no mime expectations, no lens
/// parameters (those live in separate facets resolved by the lens layer).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DpathFacet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<DpathTarget>,
}

/// One entry of a selective dpath claim (FDR 001 §2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DpathTarget {
    /// A `db+facet` URL (`db+facet:///<doc-id>/<tag>/<id>?branch=…&at=…`).
    pub facet_ref: Url,
    /// Head pins; absent and empty both mean "same transaction" (dict.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_heads: Option<Vec<String>>,
}

impl DpathFacet {
    /// A whole-document claim (default scope).
    pub const fn whole_document() -> Self {
        Self {
            targets: Vec::new(),
        }
    }

    /// Whether the claim covers the whole document (default scope).
    pub fn is_whole_document(&self) -> bool {
        self.targets.is_empty()
    }

    /// Check that every target reference is a parseable `db+facet` URL.
    ///
    /// A target that does not resolve is a *state* (`target-not-found`, FDR 001
    /// §6), not a rejected claim; this only reports the ones that are not even
    /// well-formed references.
    pub fn validate_targets(&self) -> Vec<DpathTargetIssue> {
        self.targets
            .iter()
            .enumerate()
            .filter_map(|(index, target)| {
                parse_facet_ref(&target.facet_ref)
                    .err()
                    .map(|error| DpathTargetIssue {
                        index,
                        facet_ref: target.facet_ref.to_string(),
                        reason: format!("{error:#}"),
                    })
            })
            .collect()
    }

    /// Parse the value shapes documented in FDR 001 §2:
    ///
    /// - `{}` or `null` — whole-document claim,
    /// - `{"targets": [ … ]}` — selective claim,
    /// - `{"facetRef": …, "refHeads": …}` — the single-target shorthand used by
    ///   the cross-doc example.
    ///
    /// Unknown keys beside a known one are ignored (forward compatibility: the
    /// facet value may gain keys later, and lens customization deliberately lives
    /// elsewhere). An object with neither `targets` nor `facetRef` is an error:
    /// the shape is one we write ourselves, so a wrong shape is a writer bug
    /// rather than an unvalidated external input (FDR 001 §4 keeps *merges*
    /// tolerant; this is the typed read).
    pub fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        match value {
            serde_json::Value::Null => Ok(Self::whole_document()),
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    return Ok(Self::whole_document());
                }
                if let Some(targets) = map.get("targets") {
                    let targets = serde_json::from_value::<Vec<DpathTarget>>(targets.clone())
                        .map_err(|error| format!("invalid dpath facet targets: {error}"))?;
                    return Ok(Self { targets });
                }
                if map.contains_key("facetRef") {
                    let target = serde_json::from_value::<DpathTarget>(value.clone())
                        .map_err(|error| format!("invalid dpath facet target: {error}"))?;
                    return Ok(Self {
                        targets: vec![target],
                    });
                }
                Err(format!(
                    "dpath facet value has neither 'targets' nor 'facetRef': {value}"
                ))
            }
            other => Err(format!("dpath facet value must be an object, got {other}")),
        }
    }
}

impl<'de> Deserialize<'de> for DpathFacet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(DeError::custom)
    }
}

/// A target reference that is not a well-formed facet reference (FDR 001 §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpathTargetIssue {
    pub index: usize,
    pub facet_ref: String,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dpath_facet_deserializes_documented_forms() {
        let whole: DpathFacet = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(whole.is_whole_document());

        let null: DpathFacet = serde_json::from_value(serde_json::json!(null)).unwrap();
        assert!(null.is_whole_document());

        let selective: DpathFacet = serde_json::from_value(serde_json::json!({
            "targets": [
                { "facetRef": "db+facet:///self/org.example.daybook.blob/main" },
                {
                    "facetRef": "db+facet:///self/org.example.daybook.imagemetadata/main",
                    "refHeads": []
                }
            ]
        }))
        .unwrap();
        assert_eq!(selective.targets.len(), 2);
        assert_eq!(selective.targets[1].ref_heads, Some(Vec::new()));
        assert!(selective.validate_targets().is_empty());

        // The single-target shorthand from the cross-doc example in FDR 001 §2.
        let shorthand: DpathFacet = serde_json::from_value(serde_json::json!({
            "facetRef": "db+facet:///other-doc/org.example.daybook.blob/main",
            "refHeads": ["h1"]
        }))
        .unwrap();
        assert_eq!(shorthand.targets.len(), 1);
        assert_eq!(shorthand.targets[0].ref_heads, Some(vec!["h1".to_string()]));

        // Unknown keys beside a known one are ignored (forward compatibility).
        let extended: DpathFacet =
            serde_json::from_value(serde_json::json!({ "targets": [], "future": 1 })).unwrap();
        assert!(extended.is_whole_document());

        // An object with neither `targets` nor `facetRef` is not a dpath facet
        // value we ever write, so it is an error rather than a silent widening
        // to whole-document scope.
        assert!(serde_json::from_value::<DpathFacet>(serde_json::json!({ "future": 1 })).is_err());
        assert!(serde_json::from_value::<DpathFacet>(serde_json::json!(42)).is_err());
    }

    #[test]
    fn dpath_facet_serializes_whole_document_as_empty_object() {
        let value = serde_json::to_value(DpathFacet::whole_document()).unwrap();
        assert_eq!(value, serde_json::json!({}));

        let selective = DpathFacet {
            targets: vec![DpathTarget {
                facet_ref: "db+facet:///self/org.example.daybook.note/main"
                    .parse()
                    .unwrap(),
                ref_heads: None,
            }],
        };
        let value = serde_json::to_value(&selective).unwrap();
        assert_eq!(
            value,
            serde_json::json!({ "targets": [
                { "facetRef": "db+facet:///self/org.example.daybook.note/main" }
            ]})
        );
        let round_tripped: DpathFacet = serde_json::from_value(value).unwrap();
        assert_eq!(round_tripped, selective);
    }

    #[test]
    fn dpath_target_validation_flags_non_facet_refs() {
        let facet = DpathFacet {
            targets: vec![
                DpathTarget {
                    facet_ref: "https://example.com/not-a-facet".parse().unwrap(),
                    ref_heads: None,
                },
                DpathTarget {
                    facet_ref: "db+facet:///self/org.example.daybook.note/main"
                        .parse()
                        .unwrap(),
                    ref_heads: None,
                },
                // FDR 001 §2 spells its examples `db+facet://self/…`, but the
                // shipped codec requires an empty authority (`db+facet:///self/…`).
                DpathTarget {
                    facet_ref: "db+facet://self/org.example.daybook.blob/main"
                        .parse()
                        .unwrap(),
                    ref_heads: None,
                },
            ],
        };
        let issues = facet.validate_targets();
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[0].index, 0);
        assert_eq!(issues[1].index, 2);
    }
}
