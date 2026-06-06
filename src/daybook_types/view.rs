use crate::interlude::*;
use std::collections::HashSet;

/// Maximum permitted nesting depth for a validated view tree.
pub const MAX_VIEW_DEPTH: usize = 64;

/// Maximum permitted number of nodes in a validated view tree.
pub const MAX_VIEW_NODE_COUNT: usize = 1024;

/// Maximum permitted length for a validated event name.
pub const MAX_VIEW_EVENT_NAME_LEN: usize = 128;

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

    /// Validate the host-facing view shape before it leaves the runtime boundary.
    pub fn validate(&self) -> Res<()> {
        match self {
            Self::V1(spec) => validate_view_spec_v1(spec),
        }
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]]
    #[structstruck::each[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]]
    #[serde(rename_all = "camelCase")]
    pub struct ViewSpecV1 {
        pub root: ViewNodeV1,
    }
}

fn validate_view_spec_v1(spec: &ViewSpecV1) -> Res<()> {
    let mut state = ViewValidationState::default();
    validate_node(&spec.root, 1, &mut state)
}

#[derive(Default)]
struct ViewValidationState {
    seen_ids: HashSet<String>,
    node_count: usize,
}

fn validate_node(node: &ViewNodeV1, depth: usize, state: &mut ViewValidationState) -> Res<()> {
    if depth > MAX_VIEW_DEPTH {
        eyre::bail!(
            "view tree exceeds maximum depth of {} at node '{}'",
            MAX_VIEW_DEPTH,
            node.id.0
        );
    }

    state.node_count += 1;
    if state.node_count > MAX_VIEW_NODE_COUNT {
        eyre::bail!(
            "view tree exceeds maximum node count of {} at node '{}'",
            MAX_VIEW_NODE_COUNT,
            node.id.0
        );
    }

    if !state.seen_ids.insert(node.id.0.clone()) {
        eyre::bail!("duplicate view node id '{}' in view tree", node.id.0);
    }

    validate_node_events(node.id.0.as_str(), &node.events)?;

    match &node.kind {
        ViewNodeKindV1::Card(card) => {
            for child in &card.children {
                validate_node(child, depth + 1, state)?;
            }
        }
        ViewNodeKindV1::Section(section) => {
            for child in &section.children {
                validate_node(child, depth + 1, state)?;
            }
        }
        ViewNodeKindV1::Text(_) => {}
        ViewNodeKindV1::Markdown(_) => {}
        ViewNodeKindV1::Badge(_) => {}
        ViewNodeKindV1::Amount(_) => {}
        ViewNodeKindV1::List(list) => {
            for child in &list.items {
                validate_node(child, depth + 1, state)?;
            }
        }
        ViewNodeKindV1::Button(_) => {
            // The current IR has no inline layout or flow-positioning constraints, so buttons
            // are validated structurally only.
        }
        ViewNodeKindV1::ActionGroup(group) => {
            for action in &group.actions {
                validate_node(action, depth + 1, state)?;
            }
        }
    }

    Ok(())
}

fn validate_node_events(node_id: &str, events: &[ViewEventBindingV1]) -> Res<()> {
    for event in events {
        match &event.action {
            ViewActionV1::Emit(action) => validate_event_name(node_id, &action.name)?,
        }
    }
    Ok(())
}

fn validate_event_name(node_id: &str, name: &str) -> Res<()> {
    if name.is_empty() {
        eyre::bail!("view node '{}' has an empty event name", node_id);
    }
    if name.len() > MAX_VIEW_EVENT_NAME_LEN {
        eyre::bail!(
            "view node '{}' has event name '{}' exceeding maximum length of {}",
            node_id,
            name,
            MAX_VIEW_EVENT_NAME_LEN
        );
    }

    for segment in name.split('.') {
        if segment.is_empty() {
            eyre::bail!(
                "view node '{}' has event name '{}' containing an empty segment",
                node_id,
                name
            );
        }

        let mut chars = segment.chars();
        let Some(first) = chars.next() else {
            eyre::bail!(
                "view node '{}' has event name '{}' containing an empty segment",
                node_id,
                name
            );
        };
        let Some(last) = segment.chars().last() else {
            eyre::bail!(
                "view node '{}' has event name '{}' containing an empty segment",
                node_id,
                name
            );
        };
        if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
            eyre::bail!(
                "view node '{}' has event name '{}' that must start and end with an ASCII alphanumeric character",
                node_id,
                name
            );
        }
        if !segment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            eyre::bail!(
                "view node '{}' has event name '{}' containing invalid characters; use dot-separated ASCII segments",
                node_id,
                name
            );
        }
    }

    Ok(())
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]]
    #[structstruck::each[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]]
    #[serde(rename_all = "camelCase")]
    pub struct ViewNodeV1 {
        pub id: ViewNodeId,
        pub kind: pub enum ViewNodeKindV1 {
            #[serde(rename = "card")]
            Card(pub struct CardNodeV1 {
                #[serde(default)]
                pub title: Option<String>,
                #[serde(default)]
                pub children: Vec<ViewNodeV1>,
            }),
            #[serde(rename = "section")]
            Section(pub struct SectionNodeV1 {
                #[serde(default)]
                pub title: Option<String>,
                #[serde(default)]
                pub children: Vec<ViewNodeV1>,
            }),
            #[serde(rename = "text")]
            Text(pub struct TextNodeV1 {
                pub text: String,
            }),
            #[serde(rename = "markdown")]
            Markdown(pub struct MarkdownNodeV1 {
                pub markdown: String,
            }),
            #[serde(rename = "badge")]
            Badge(pub struct BadgeNodeV1 {
                pub label: String,
                #[serde(default)]
                pub tone: BadgeToneV1,
            }),
            #[serde(rename = "amount")]
            Amount(pub struct AmountNodeV1 {
                pub decimal: String,
                pub commodity: String,
            }),
            #[serde(rename = "list")]
            List(pub struct ListNodeV1 {
                #[serde(default)]
                pub items: Vec<ViewNodeV1>,
            }),
            #[serde(rename = "button")]
            Button(pub struct ButtonNodeV1 {
                pub label: String,
            }),
            #[serde(rename = "actionGroup")]
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
            #[serde(rename = "emit")]
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

    #[test]
    fn view_spec_validation_rejects_duplicate_node_ids() {
        let spec = ViewSpec::V1(ViewSpecV1 {
            root: ViewNodeV1 {
                id: ViewNodeId::from("root"),
                kind: ViewNodeKindV1::Card(CardNodeV1 {
                    title: None,
                    children: vec![ViewNodeV1 {
                        id: ViewNodeId::from("root"),
                        kind: ViewNodeKindV1::Text(TextNodeV1 {
                            text: "duplicate".into(),
                        }),
                        events: vec![],
                    }],
                }),
                events: vec![],
            },
        });

        let err = spec.validate().unwrap_err().to_string();
        assert!(err.contains("duplicate view node id"));
    }

    #[test]
    fn view_spec_validation_rejects_too_deep_tree() {
        fn chain(depth: usize) -> ViewNodeV1 {
            let id = ViewNodeId::from(format!("node-{depth}"));
            if depth == 0 {
                ViewNodeV1 {
                    id,
                    kind: ViewNodeKindV1::Text(TextNodeV1 {
                        text: "leaf".into(),
                    }),
                    events: vec![],
                }
            } else {
                ViewNodeV1 {
                    id,
                    kind: ViewNodeKindV1::Card(CardNodeV1 {
                        title: None,
                        children: vec![chain(depth - 1)],
                    }),
                    events: vec![],
                }
            }
        }

        let spec = ViewSpec::V1(ViewSpecV1 {
            root: chain(MAX_VIEW_DEPTH),
        });

        let err = spec.validate().unwrap_err().to_string();
        assert!(err.contains("exceeds maximum depth"));
    }

    #[test]
    fn view_spec_validation_rejects_too_many_nodes() {
        let children = (0..MAX_VIEW_NODE_COUNT)
            .map(|idx| ViewNodeV1 {
                id: ViewNodeId::from(format!("child-{idx}")),
                kind: ViewNodeKindV1::Text(TextNodeV1 {
                    text: format!("child {idx}"),
                }),
                events: vec![],
            })
            .collect();

        let spec = ViewSpec::V1(ViewSpecV1 {
            root: ViewNodeV1 {
                id: ViewNodeId::from("root"),
                kind: ViewNodeKindV1::Card(CardNodeV1 {
                    title: None,
                    children,
                }),
                events: vec![],
            },
        });

        let err = spec.validate().unwrap_err().to_string();
        assert!(err.contains("exceeds maximum node count"));
    }

    #[test]
    fn view_spec_validation_rejects_malformed_event_names() {
        for bad_name in ["", "foo..bar", "foo.bar!", ".leading", "trailing."] {
            let spec = ViewSpec::V1(ViewSpecV1 {
                root: ViewNodeV1 {
                    id: ViewNodeId::from("root"),
                    kind: ViewNodeKindV1::Button(ButtonNodeV1 { label: "Go".into() }),
                    events: vec![ViewEventBindingV1 {
                        event: ViewEventKindV1::Click,
                        action: ViewActionV1::Emit(EmitViewActionV1 {
                            name: bad_name.into(),
                            payload: serde_json::json!({ "ok": true }),
                        }),
                    }],
                },
            });

            let err = spec.validate().unwrap_err().to_string();
            assert!(
                err.contains("event name"),
                "unexpected error for '{bad_name}': {err}"
            );
        }
    }
}
