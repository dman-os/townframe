//! Differential test for C2, the item model of `CodeView`
//! (`oracle/s12c_code_view.mjs`, golden `oracle/golden/s12c_code_view.json.gz`):
//! the published class, constructed over a bare document and never set up, ran
//! handwritten and seeded random scenarios of item operations, option changes,
//! selection moves and layout recomputes, recording each step's result and the
//! whole observable state afterwards. Each scenario is replayed against
//! `CodeView` over `FakeDom`. The two things the replay composes as the
//! published run does: `render` is observed (the port's `render_calls`;
//! upstream's is replaced on the instance by a recorder), and the `recompute`
//! step is the layout half of `computeRenderRangeAndEmit` (`recompute_layout`
//! over the dirty index and the pending reset, then both cleared). The option
//! callbacks record their arguments and the item context they receive (the
//! trailing argument; its `instance` is omitted, parity/divergences.psv).
//! Test-only, original to this crate.

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::CodeView;
use super::options::CodeViewDiffItemContext;
use super::options::CodeViewHunkSeparators;
use super::options::CodeViewOptions;
use super::options::ItemCallback;
use super::options::ItemCallback2;
use super::options::ItemSelectionCallback;
use super::options::SelectedLinesChangeCallback;
use super::state::CodeViewLineSelection;
use crate::components::virtualized_file_diff::advanced::AdvancedVirtualizer;
use crate::components::virtualized_file_diff::instance::VirtualizedFileDiff;
use crate::components::virtualized_file_diff::layout_oracle_tests::MetricsSpec;
use crate::components::virtualized_file_diff::layout_oracle_tests::Spec;
use crate::components::virtualized_file_diff::layout_oracle_tests::options_from_spec;
use crate::components::virtualized_file_diff::layout_oracle_tests::panic_message;
use crate::components::virtualizer::InstanceRef;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::LineEventBaseProps;
use crate::managers::interaction_manager::LineEventKind;
use crate::managers::interaction_manager::LineEventProps;
use crate::managers::interaction_manager::PointerEventData;
use crate::managers::interaction_manager::PointerType;
use crate::managers::interaction_manager::SelectionCallback;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::CodeViewLayout;
use crate::types::layout::VirtualFileMetrics;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewItem;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectedLineRange;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

type View = CodeView<FakeDom, u8, u8>;
type Instance = VirtualizedFileDiff<FakeDom, u8, u8>;

#[derive(Deserialize)]
struct Golden {
    diffs: HashMap<String, FileDiffMetadata>,
    metrics: HashMap<String, MetricsSpec>,
    layouts: HashMap<String, CodeViewLayout>,
    lists: HashMap<String, Vec<DiffLineAnnotation<u8>>>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    results: Vec<Value>,
    trace: Vec<Value>,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    options: OptionsSpec,
    steps: Vec<Step>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct OptionsSpec {
    #[serde(default)]
    diff: Spec,
    hunk_separators: Option<String>,
    sticky_headers: Option<bool>,
    controlled_selection: Option<bool>,
    item_metrics: Option<String>,
    layout: Option<String>,
    pointer_events_on_scroll: Option<bool>,
    #[serde(default)]
    callbacks: Vec<String>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ItemSpec {
    id: String,
    diff: String,
    version: Option<usize>,
    collapsed: Option<bool>,
    annotations: Option<String>,
}

#[derive(Deserialize)]
struct SelectionSpec {
    id: String,
    range: SelectedLineRange,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Step {
    SetItems {
        items: Vec<ItemSpec>,
    },
    AddItems {
        items: Vec<ItemSpec>,
    },
    AddItem {
        item: ItemSpec,
    },
    UpdateItem {
        item: ItemSpec,
    },
    UpdateItemId {
        from: String,
        to: String,
    },
    RemoveItem {
        id: String,
    },
    SetOptions {
        options: OptionsSpec,
    },
    #[serde(rename_all = "camelCase")]
    InstanceChanged {
        id: String,
        layout_dirty: bool,
    },
    Recompute,
    SetSelectedLines {
        selection: Option<SelectionSpec>,
        notify: Option<bool>,
    },
    ClearSelectedLines,
    #[serde(rename_all = "camelCase")]
    Fire {
        id: String,
        key: String,
        range: Option<SelectedLineRange>,
        line_number: Option<i64>,
        diff: Option<String>,
    },
    FirstVisible {
        top: f64,
    },
    LastVisible {
        bottom: f64,
    },
    TopForItem {
        id: String,
    },
    GetItem {
        id: String,
    },
}

/// The option callbacks a scenario names, one `Rc` per name for the whole run
/// (a callback is compared by identity), each recording what it was called with.
struct Callbacks {
    calls: Rc<RefCell<Vec<Value>>>,
    selection: HashMap<&'static str, ItemSelectionCallback<u8>>,
    on_selected_lines_change: SelectedLinesChangeCallback,
    on_line_click: ItemCallback<u8, LineEventProps<NodeKey>>,
    on_line_enter: ItemCallback<u8, LineEventProps<NodeKey>>,
    on_gutter_utility_click: ItemCallback<u8, SelectedLineRange>,
    render_custom_header: ItemCallback<u8, FileDiffMetadata, Option<u8>>,
    render_annotation: ItemCallback2<u8, DiffLineAnnotation<u8>, FileDiffMetadata, Option<u8>>,
    header: PtrEq<dyn Fn() -> Option<u8>>,
    footer: PtrEq<dyn Fn() -> Option<u8>>,
}

fn range_json(range: Option<&SelectedLineRange>) -> Value {
    range.map_or(Value::Null, |range| {
        serde_json::to_value(range).expect("a range serializes")
    })
}

fn selection_json(selection: Option<&CodeViewLineSelection>) -> Value {
    selection.map_or(
        Value::Null,
        |selection| json!({ "id": selection.id, "range": range_json(Some(&selection.range)) }),
    )
}

/// What an option callback sees of the item: the trailing `context` argument,
/// as the script's `encodeContext` records it.
fn context_json(context: &CodeViewDiffItemContext<u8>) -> Value {
    json!({
        "type": "diff",
        "id": context.item.id,
        "itemVersion": context.item.version,
        "itemCollapsed": context.item.collapsed,
        "index": context.index,
        "top": context.top,
        "height": context.height,
        "version": context.version,
        "renderedOptionsRevision": context.rendered_options_revision,
        "mounted": context.element.is_some(),
    })
}

impl Callbacks {
    fn new() -> Self {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let selection = [
            "onLineSelected",
            "onLineSelectionStart",
            "onLineSelectionChange",
            "onLineSelectionEnd",
        ]
        .into_iter()
        .map(|name| {
            let calls = Rc::clone(&calls);
            let callback: ItemSelectionCallback<u8> = PtrEq(Rc::new(move |range, context| {
                calls
                    .borrow_mut()
                    .push(json!([name, range_json(range), context_json(context)]));
            }));
            (name, callback)
        })
        .collect();
        let changes = Rc::clone(&calls);
        let on_selected_lines_change: SelectedLinesChangeCallback = Rc::new(move |selection| {
            changes
                .borrow_mut()
                .push(json!(["onSelectedLinesChange", selection_json(selection)]));
        });
        let line_callback = |name: &'static str| -> ItemCallback<u8, LineEventProps<NodeKey>> {
            let calls = Rc::clone(&calls);
            PtrEq(Rc::new(move |props, context| {
                calls.borrow_mut().push(json!([
                    name,
                    props.base.line_number,
                    context_json(context)
                ]));
            }))
        };
        let on_line_click = line_callback("onLineClick");
        let on_line_enter = line_callback("onLineEnter");
        let gutter_calls = Rc::clone(&calls);
        let on_gutter_utility_click: ItemCallback<u8, SelectedLineRange> =
            PtrEq(Rc::new(move |range, context| {
                gutter_calls.borrow_mut().push(json!([
                    "onGutterUtilityClick",
                    range_json(Some(range)),
                    context_json(context)
                ]));
            }));
        let header_calls = Rc::clone(&calls);
        let render_custom_header: ItemCallback<u8, FileDiffMetadata, Option<u8>> =
            PtrEq(Rc::new(move |file_diff, context| {
                header_calls.borrow_mut().push(json!([
                    "renderCustomHeader",
                    file_diff.name,
                    context_json(context)
                ]));
                None
            }));
        let annotation_calls = Rc::clone(&calls);
        let render_annotation: ItemCallback2<
            u8,
            DiffLineAnnotation<u8>,
            FileDiffMetadata,
            Option<u8>,
        > = PtrEq(Rc::new(move |annotation, file_diff, context| {
            annotation_calls.borrow_mut().push(json!([
                "renderAnnotation",
                annotation.line_number,
                file_diff.name,
                context_json(context)
            ]));
            None
        }));
        Self {
            calls,
            selection,
            on_selected_lines_change,
            on_line_click,
            on_line_enter,
            on_gutter_utility_click,
            render_custom_header,
            render_annotation,
            header: PtrEq(Rc::new(|| None)),
            footer: PtrEq(Rc::new(|| None)),
        }
    }
}

struct World {
    view: Rc<View>,
    golden: Rc<Golden>,
    callbacks: Callbacks,
    seen_instances: RefCell<Vec<Rc<Instance>>>,
    /// The diffs the scenario's items name, one `Rc` per name.
    diffs: HashMap<String, Rc<FileDiffMetadata>>,
}

fn build_options(
    spec: &OptionsSpec,
    golden: &Golden,
    callbacks: &Callbacks,
) -> CodeViewOptions<u8, u8> {
    let mut options = CodeViewOptions {
        diff: options_from_spec(&spec.diff, &mut HashMap::new()),
        ..CodeViewOptions::default()
    };
    options.hunk_separators = spec.hunk_separators.as_deref().map(|name| match name {
        "simple" => CodeViewHunkSeparators::Simple,
        "metadata" => CodeViewHunkSeparators::Metadata,
        "line-info" => CodeViewHunkSeparators::LineInfo,
        "line-info-basic" => CodeViewHunkSeparators::LineInfoBasic,
        other => panic!("unknown separators {other}"),
    });
    options.sticky_headers = spec.sticky_headers;
    options.diff.interaction.controlled_selection = spec.controlled_selection;
    let selection = &callbacks.selection;
    options.item_metrics = spec
        .item_metrics
        .as_ref()
        .map(|name| PartialVirtualFileMetrics::from(golden.metrics[name]));
    options.layout = spec.layout.as_ref().map(|name| golden.layouts[name]);
    options.pointer_events_on_scroll = spec.pointer_events_on_scroll;
    for name in &spec.callbacks {
        match name.as_str() {
            "onSelectedLinesChange" => {
                options.on_selected_lines_change =
                    Some(Rc::clone(&callbacks.on_selected_lines_change));
            }
            "onLineSelected" => {
                options.callbacks.on_line_selected = Some(selection["onLineSelected"].clone());
            }
            "onLineSelectionStart" => {
                options.callbacks.on_line_selection_start =
                    Some(selection["onLineSelectionStart"].clone());
            }
            "onLineSelectionChange" => {
                options.callbacks.on_line_selection_change =
                    Some(selection["onLineSelectionChange"].clone());
            }
            "onLineSelectionEnd" => {
                options.callbacks.on_line_selection_end =
                    Some(selection["onLineSelectionEnd"].clone());
            }
            "onLineClick" => {
                options.callbacks.on_line_click = Some(callbacks.on_line_click.clone())
            }
            "onLineEnter" => {
                options.callbacks.on_line_enter = Some(callbacks.on_line_enter.clone())
            }
            "onGutterUtilityClick" => {
                options.callbacks.on_gutter_utility_click =
                    Some(callbacks.on_gutter_utility_click.clone());
            }
            "renderCustomHeader" => {
                options.callbacks.render_custom_header =
                    Some(callbacks.render_custom_header.clone());
            }
            "renderAnnotation" => {
                options.callbacks.render_annotation = Some(callbacks.render_annotation.clone());
            }
            "header" => options.render_code_view_header = Some(callbacks.header.clone()),
            "footer" => options.render_code_view_footer = Some(callbacks.footer.clone()),
            other => panic!("unknown callback {other}"),
        }
    }
    options
}

impl World {
    fn item(&self, spec: &ItemSpec) -> CodeViewItem<u8> {
        CodeViewItem::Diff(CodeViewDiffItem {
            id: spec.id.clone(),
            file_diff: Rc::clone(&self.diffs[&spec.diff]),
            annotations: spec
                .annotations
                .as_ref()
                .map(|name| Rc::new(self.golden.lists[name].clone())),
            version: spec.version,
            collapsed: spec.collapsed,
            edit: None,
        })
    }

    fn items(&self, specs: &[ItemSpec]) -> Vec<CodeViewItem<u8>> {
        specs.iter().map(|spec| self.item(spec)).collect()
    }

    fn instance_of(&self, id: &str) -> Rc<Instance> {
        let item = self
            .view
            .item_by_id(id)
            .expect("the scenario names an item");
        let item = item.borrow();
        Rc::clone(item.instance())
    }

    fn apply(&self, step: &Step) -> Value {
        let view = &self.view;
        match step {
            Step::SetItems { items } => {
                view.set_items(&self.items(items));
                Value::Null
            }
            Step::AddItems { items } => {
                view.add_items(&self.items(items));
                Value::Null
            }
            Step::AddItem { item } => {
                view.add_item(&self.item(item));
                Value::Null
            }
            Step::UpdateItem { item } => json!(view.update_item(&self.item(item))),
            Step::UpdateItemId { from, to } => json!(view.update_item_id(from, to)),
            Step::RemoveItem { id } => json!(view.remove_item(id)),
            Step::SetOptions { options } => {
                view.set_options(Some(build_options(options, &self.golden, &self.callbacks)));
                Value::Null
            }
            Step::InstanceChanged { id, layout_dirty } => {
                let instance: InstanceRef = self.instance_of(id);
                view.instance_changed(&instance, *layout_dirty);
                Value::Null
            }
            Step::Recompute => {
                let pending = {
                    let mut state = view.state.borrow_mut();
                    let reset = state.pending_layout_reset;
                    state.layout_dirty_index.take().map(|index| (index, reset))
                };
                if let Some((index, reset)) = pending {
                    view.recompute_layout(index, reset);
                    view.state.borrow_mut().pending_layout_reset = None;
                }
                Value::Null
            }
            Step::SetSelectedLines { selection, notify } => {
                view.set_selected_lines(
                    selection.as_ref().map(|selection| CodeViewLineSelection {
                        id: selection.id.clone(),
                        range: selection.range.clone(),
                    }),
                    SelectionWriteOptions {
                        notify: *notify,
                        ..SelectionWriteOptions::default()
                    },
                );
                Value::Null
            }
            Step::ClearSelectedLines => {
                view.clear_selected_lines(SelectionWriteOptions::default());
                Value::Null
            }
            Step::Fire {
                id,
                key,
                range,
                line_number,
                diff,
            } => {
                let instance = self.instance_of(id);
                let options = instance.file_diff().options();
                let interaction = &options.interaction;
                // Every callback is called with the options borrow released, as
                // the instance does.
                let selection = |callback: &Option<SelectionCallback>| {
                    callback
                        .clone()
                        .expect("the instance holds the selection callbacks")
                };
                match key.as_str() {
                    "onLineSelected" => {
                        let callback = selection(&interaction.on_line_selected);
                        drop(options);
                        callback(range.as_ref());
                    }
                    "onLineSelectionStart" => {
                        let callback = selection(&interaction.on_line_selection_start);
                        drop(options);
                        callback(range.as_ref());
                    }
                    "onLineSelectionChange" => {
                        let callback = selection(&interaction.on_line_selection_change);
                        drop(options);
                        callback(range.as_ref());
                    }
                    "onLineSelectionEnd" => {
                        let callback = selection(&interaction.on_line_selection_end);
                        drop(options);
                        callback(range.as_ref());
                    }
                    "onLineClick" | "onLineEnter" => {
                        let callback = if key == "onLineClick" {
                            interaction.on_line_click.clone()
                        } else {
                            interaction.on_line_enter.clone()
                        }
                        .expect("the instance holds the line callback");
                        drop(options);
                        let element = view.sticky_container;
                        callback(&LineEventProps {
                            base: LineEventBaseProps {
                                kind: LineEventKind::DiffLine,
                                line_element: element,
                                line_number: line_number.expect("a line number"),
                                number_column: false,
                                number_element: element,
                                annotation_side: None,
                                line_type: None,
                            },
                            event: PointerEventData {
                                pointer_id: 0,
                                pointer_type: PointerType::Mouse,
                                button: 0,
                                shift_key: false,
                                client_x: 0.0,
                                client_y: 0.0,
                                target: None,
                            },
                        });
                    }
                    "onGutterUtilityClick" => {
                        let callback = interaction
                            .on_gutter_utility_click
                            .clone()
                            .expect("the instance holds the gutter click callback");
                        drop(options);
                        callback(range.clone().expect("a range"));
                    }
                    "renderCustomHeader" => {
                        let callback = options
                            .render_custom_header
                            .clone()
                            .expect("the instance holds the header callback");
                        drop(options);
                        callback(&self.golden.diffs[diff.as_ref().expect("a diff")]);
                    }
                    "renderAnnotation" => {
                        let callback = options
                            .render_annotation
                            .clone()
                            .expect("the instance holds the annotation callback");
                        drop(options);
                        callback(
                            &DiffLineAnnotation {
                                side: AnnotationSide::Additions,
                                line_number: usize::try_from(line_number.expect("a line number"))
                                    .expect("a non-negative line number"),
                                metadata: Some(1),
                            },
                            &self.golden.diffs[diff.as_ref().expect("a diff")],
                        );
                    }
                    other => panic!("unknown callback {other}"),
                }
                Value::Null
            }
            Step::FirstVisible { top } => json!(view.find_first_visible_index(*top)),
            Step::LastVisible { bottom } => json!(view.find_last_visible_index(*bottom)),
            Step::TopForItem { id } => json!(view.get_top_for_item(id)),
            Step::GetItem { id } => match view.get_item(id) {
                Some(CodeViewItem::Diff(item)) => json!({
                    "id": item.id,
                    "version": item.version,
                    "collapsed": item.collapsed,
                }),
                Some(CodeViewItem::File(_)) => unreachable!("file items cannot exist"),
                None => Value::Null,
            },
        }
    }

    fn instance_number(&self, instance: &Rc<Instance>) -> usize {
        let mut seen = self.seen_instances.borrow_mut();
        if let Some(number) = seen.iter().position(|other| Rc::ptr_eq(other, instance)) {
            return number;
        }
        seen.push(Rc::clone(instance));
        seen.len() - 1
    }

    fn snapshot(&self) -> Value {
        let (items, mut ids, mut instances) = {
            let state = self.view.state.borrow();
            (
                state.items.clone(),
                state.id_to_item.keys().cloned().collect::<Vec<_>>(),
                state
                    .instance_to_item
                    .values()
                    .map(|item| item.borrow().id().to_owned())
                    .collect::<Vec<_>>(),
            )
        };
        ids.sort();
        instances.sort();
        let items: Vec<Value> = items
            .iter()
            .map(|record| {
                let record = record.borrow();
                let instance = record.instance();
                let layout = instance.layout();
                let super::state::ItemKind::Diff(diff) = &record.kind else {
                    unreachable!("file items cannot exist");
                };
                json!({
                    "id": record.id(),
                    "type": "diff",
                    "index": record.index,
                    "top": record.top,
                    "height": record.height,
                    "version": record.version,
                    "renderedOptionsRevision": record.rendered_options_revision,
                    "instance": self.instance_number(instance),
                    "collapsed": diff.item.collapsed,
                    "instanceHeight": layout.height,
                    "instanceTop": layout.top,
                    "layoutDirty": layout.layout_dirty,
                    "estimatedSplitHeight": layout.cache.estimated_split_height,
                    "estimatedUnifiedHeight": layout.cache.estimated_unified_height,
                    "totalLines": layout.cache.total_lines,
                    "fileAnnotationHeight": layout.cache.file_annotation_height,
                })
            })
            .collect();
        let state = self.view.state.borrow();
        json!({
            "items": items,
            "ids": ids,
            "instances": instances,
            "scrollHeight": state.scroll_height,
            "scrollDirty": state.scroll_dirty,
            "layoutDirtyIndex": state.layout_dirty_index,
            "pendingLayoutReset": state.pending_layout_reset.map(|reset| {
                let mut value = json!({
                    "resetFileLayoutCache": reset.reset_file_layout_cache,
                    "resetDiffLayoutCache": reset.reset_diff_layout_cache,
                    "includeEstimatedDiffHeights": reset.include_estimated_diff_heights,
                });
                if let Some(metrics) = reset.metrics {
                    value["metrics"] = metrics_json(&metrics);
                }
                value
            }),
            "renderOptionsRevision": state.render_options_revision,
            "selection": selection_json(state.selected_lines.as_ref()),
            "renders": self.view.render_calls.borrow_mut().drain(..).collect::<Vec<_>>(),
            "calls": self.callbacks.calls.borrow_mut().drain(..).collect::<Vec<_>>(),
        })
    }
}

/// A `VirtualFileMetrics` the way `JSON.stringify` writes it: unset optionals
/// are absent.
fn metrics_json(metrics: &VirtualFileMetrics) -> Value {
    let mut value = json!({
        "hunkLineCount": metrics.hunk_line_count,
        "lineHeight": metrics.line_height,
        "diffHeaderHeight": metrics.diff_header_height,
        "spacing": metrics.spacing,
    });
    for (key, optional) in [
        ("hunkSeparatorHeight", metrics.hunk_separator_height),
        ("paddingTop", metrics.padding_top),
        ("paddingBottom", metrics.padding_bottom),
    ] {
        if let Some(optional) = optional {
            value[key] = json!(optional);
        }
    }
    value
}

#[test]
fn code_view_item_model_matches_the_published_class() {
    let golden: Rc<Golden> = Rc::new(load_golden("s12c_code_view.json"));
    assert!(golden.cases.len() > 150, "{} scenarios", golden.cases.len());
    let mut steps = 0;
    for case in &golden.cases {
        let scenario = &case.scenario;
        let dom = Rc::new(FakeDom::default());
        let callbacks = Callbacks::new();
        let view = CodeView::new(
            Rc::clone(&dom),
            ResizeObserverHub::new(Rc::clone(&dom)),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            Rc::new(PlainTextHighlighter),
            build_options(&scenario.options, &golden, &callbacks),
        );
        let diffs = golden
            .diffs
            .iter()
            .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
            .collect();
        let world = World {
            view,
            golden: Rc::clone(&golden),
            callbacks,
            seen_instances: RefCell::default(),
            diffs,
        };
        let context = |step: usize| format!("{} step {step}", scenario.name);
        assert_json_eq(&context(0), &case.trace[0], &world.snapshot());
        for (index, step) in scenario.steps.iter().enumerate() {
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| world.apply(step)));
            let result = match result {
                Ok(value) => json!({ "value": value }),
                Err(payload) => json!({ "throws": panic_message(&*payload) }),
            };
            assert_json_eq(
                &format!("{} result", context(index + 1)),
                &case.results[index],
                &result,
            );
            assert_json_eq(
                &context(index + 1),
                &case.trace[index + 1],
                &world.snapshot(),
            );
            steps += 1;
        }
    }
    assert!(steps > 3_000, "{steps} steps");
}
