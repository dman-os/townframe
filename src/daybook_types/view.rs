use crate::interlude::*;

/// A plugin-emitted Daybook view description.
///
/// This is the stable boundary between plugin view providers and Daybook hosts. It is intentionally
/// smaller than the host renderer's internal model and must not contain authoring conveniences such
/// as bindings, templates, or layout primitives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "schemaVersion", content = "spec", rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ViewSpec {
    V1(ViewSpecV1),
}

impl ViewSpec {
    pub const CURRENT_SCHEMA_VERSION: &'static str = "v1";
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]]
    #[structstruck::each[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]]
    #[serde(rename_all = "camelCase")]
    pub struct ViewSpecV1 {
        pub root: ViewNodeV1,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]]
    #[structstruck::each[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]]
    #[serde(rename_all = "camelCase")]
    pub struct ViewNodeV1 {
        pub id: ViewNodeId,
        pub kind: pub enum ViewNodeKindV1 {
            Card(pub struct CardNodeV1 {
                #[serde(default)]
                pub title: Option<String>,
                #[serde(default)]
                pub children: Vec<ViewNodeV1>,
            }),
            Section(pub struct SectionNodeV1 {
                #[serde(default)]
                pub title: Option<String>,
                #[serde(default)]
                pub children: Vec<ViewNodeV1>,
            }),
            Text(pub struct TextNodeV1 {
                pub text: String,
            }),
            Markdown(pub struct MarkdownNodeV1 {
                pub markdown: String,
            }),
            Badge(pub struct BadgeNodeV1 {
                pub label: String,
                #[serde(default)]
                pub tone: BadgeToneV1,
            }),
            Amount(pub struct AmountNodeV1 {
                pub decimal: String,
                pub commodity: String,
            }),
            List(pub struct ListNodeV1 {
                #[serde(default)]
                pub items: Vec<ViewNodeV1>,
            }),
            Button(pub struct ButtonNodeV1 {
                pub label: String,
            }),
            ActionGroup(pub struct ActionGroupNodeV1 {
                #[serde(default)]
                pub actions: Vec<ViewNodeV1>,
            }),
        },
        #[serde(default)]
        pub events: Vec<ViewEventBindingV1>,
    }
}

/// Stable node identity within a plugin-emitted view.
///
/// Node IDs are used for event routing and diagnostics. Producers should keep them stable across
/// equivalent renders when practical, but the host must not treat them as persisted storage IDs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ViewNodeId(pub String);

impl<T> From<T> for ViewNodeId
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum BadgeToneV1 {
    #[default]
    Neutral,
    Info,
    Success,
    Warning,
    Danger,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]]
    #[structstruck::each[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]]
    #[serde(rename_all = "camelCase")]
    pub struct ViewEventBindingV1 {
        pub event: ViewEventKindV1,
        pub action: pub enum ViewActionV1 {
            Emit(pub struct EmitViewActionV1 {
                pub name: String,
                #[serde(default)]
                pub payload: serde_json::Value,
            }),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ViewEventKindV1 {
    Click,
    Submit,
    Change,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_spec_v1_serializes_with_explicit_schema_version() {
        let spec = ViewSpec::V1(ViewSpecV1 {
            root: ViewNodeV1 {
                id: ViewNodeId::from("root"),
                kind: ViewNodeKindV1::Card(CardNodeV1 {
                    title: Some("Claim".into()),
                    children: vec![ViewNodeV1 {
                        id: ViewNodeId::from("summary"),
                        kind: ViewNodeKindV1::Markdown(MarkdownNodeV1 {
                            markdown: "**Date:** 2024-01-02".into(),
                        }),
                        events: vec![],
                    }],
                }),
                events: vec![],
            },
        });

        assert_eq!(
            serde_json::to_value(spec).unwrap(),
            serde_json::json!({
                "schemaVersion": "v1",
                "spec": {
                    "root": {
                        "id": "root",
                        "kind": {
                            "card": {
                                "title": "Claim",
                                "children": [{
                                    "id": "summary",
                                    "kind": {
                                        "markdown": {
                                            "markdown": "**Date:** 2024-01-02"
                                        }
                                    },
                                    "events": []
                                }]
                            }
                        },
                        "events": []
                    }
                }
            }),
        );
    }
}
