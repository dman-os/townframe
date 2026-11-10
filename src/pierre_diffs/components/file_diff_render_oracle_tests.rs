//! Replay of `oracle/s12b_render.mjs` (see its header): the published
//! `FileDiff` over jsdom against the port's `FileDiff` over `FakeDom`, on the
//! same scenarios. After every step the return value, the trace (manager host
//! calls and option callbacks), the owner render requests, a few instance
//! fields and the tree of every named container are compared. Test-only,
//! original to this crate.

use std::cell::OnceCell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::components::file_diff::FileDiff;
use crate::components::file_diff::FileDiffHunkSeparators;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::file_diff::LineAnnotations;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::hast;
use crate::highlighter_types::PlainTextHighlighter;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::LineHoverHighlight;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::RenderRange;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::options::PostRenderPhase;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;

type Instance = FileDiff<FakeDom, hast::Element, i64>;
type Options = FileDiffOptions<NodeKey, hast::Element, i64>;

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct OptionsSpec {
    theme: Option<ThemeOption>,
    theme_type: Option<ThemeTypes>,
    overflow: Option<Overflow>,
    diff_style: Option<DiffStyle>,
    collapsed: Option<bool>,
    disable_file_header: Option<bool>,
    disable_virtualization_buffers: Option<bool>,
    disable_line_numbers: Option<bool>,
    disable_background: Option<bool>,
    diff_indicators: Option<DiffIndicators>,
    expand_unchanged: Option<bool>,
    #[serde(rename = "unsafeCSS")]
    unsafe_css: Option<String>,
    line_hover_highlight: Option<LineHoverHighlight>,
    enable_gutter_utility: Option<bool>,
    hunk_separators: Option<String>,
    #[serde(default)]
    render_annotation: bool,
    #[serde(default)]
    render_header_prefix: bool,
    #[serde(default)]
    render_header_filename_suffix: bool,
    #[serde(default)]
    render_header_metadata: bool,
    #[serde(default)]
    render_custom_header: bool,
    #[serde(default)]
    render_gutter_utility: bool,
    #[serde(default)]
    on_post_render: bool,
    #[serde(default)]
    load_diff_files: bool,
}

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct Step {
    op: String,
    instance: Option<usize>,
    options: Option<OptionsSpec>,
    spec: Option<OptionsSpec>,
    theme_type: Option<ThemeTypes>,
    diff: Option<String>,
    files: Option<String>,
    annotations: Option<String>,
    range: Option<RangeSpec>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    defer: bool,
    #[serde(default)]
    no_emit: bool,
    container: Option<String>,
    wrapper: Option<String>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
struct RangeSpec {
    starting_line: f64,
    total_lines: Option<f64>,
    buffer_before: f64,
    buffer_after: f64,
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    options: Option<OptionsSpec>,
    #[serde(default)]
    managed: bool,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct FilePair {
    old: Option<FileContents>,
    new: Option<FileContents>,
}

#[derive(Deserialize)]
struct Templates {
    files: HashMap<String, FilePair>,
    diffs: HashMap<String, FileDiffMetadata>,
    annotations: HashMap<String, Vec<DiffLineAnnotation<i64>>>,
    partials: HashMap<String, FilePair>,
}

#[derive(Deserialize)]
struct Case {
    scenario: Scenario,
    snapshots: Vec<Value>,
}

#[derive(Deserialize)]
struct Golden {
    templates: Templates,
    cases: Vec<Case>,
}

/// The pool a scenario names, with the identity of its diffs and annotations.
struct Pool {
    diffs: RefCell<HashMap<String, Rc<FileDiffMetadata>>>,
    annotations: HashMap<String, LineAnnotations<i64>>,
}

impl Pool {
    fn diff(&self, name: &str) -> Rc<FileDiffMetadata> {
        Rc::clone(&self.diffs.borrow()[name])
    }

    fn diff_name(&self, diff: &FileDiffMetadata) -> Option<String> {
        self.diffs
            .borrow()
            .iter()
            .find(|(_, candidate)| std::ptr::eq(&***candidate, diff))
            .map(|(name, _)| name.clone())
    }

    fn annotations_name(
        &self,
        list: &LineAnnotations<i64>,
        empty: &LineAnnotations<i64>,
    ) -> String {
        if Rc::ptr_eq(list, empty) {
            return "EMPTY".to_owned();
        }
        self.annotations
            .iter()
            .find(|(_, candidate)| Rc::ptr_eq(candidate, list))
            .map_or("?".to_owned(), |(name, _)| name.clone())
    }
}

fn element(tag: &str, attributes: &[(&str, String)], text: Option<&str>) -> hast::Element {
    let mut properties = hast::Properties::new();
    for (name, value) in attributes {
        properties.set(name, value.as_str());
    }
    hast::Element::new(
        tag,
        properties,
        text.map(|text| hast::Text::new(text).into())
            .into_iter()
            .collect(),
    )
}

type SeparatorCallback = PtrEq<dyn Fn(&crate::types::lines::HunkData) -> Option<hast::Element>>;
type AnnotationCallback =
    PtrEq<dyn Fn(&DiffLineAnnotation<i64>, &FileDiffMetadata) -> Option<hast::Element>>;
type HeaderCallback = PtrEq<dyn Fn(&FileDiffMetadata) -> Option<hast::Element>>;
type HoveredLine = Option<crate::managers::interaction_manager::GetHoveredLineResult>;
type GutterUtilityCallback = PtrEq<dyn Fn(&dyn Fn() -> HoveredLine) -> Option<hast::Element>>;
type PostRenderCallback = PtrEq<dyn Fn(&NodeKey, PostRenderPhase)>;

/// The option callbacks, one function per field for the whole scenario (the
/// JavaScript side caches them by field, so `setOptions` keeps their identity).
#[derive(Default)]
struct Callbacks {
    separators: OnceCell<SeparatorCallback>,
    annotation: OnceCell<AnnotationCallback>,
    header: RefCell<HashMap<&'static str, HeaderCallback>>,
    gutter_utility: OnceCell<GutterUtilityCallback>,
    post_render: OnceCell<PostRenderCallback>,
}

struct World {
    dom: Rc<FakeDom>,
    pool: Rc<Pool>,
    callbacks: Callbacks,
}

fn side_name(side: AnnotationSide) -> &'static str {
    match side {
        AnnotationSide::Additions => "additions",
        AnnotationSide::Deletions => "deletions",
    }
}

impl World {
    fn build_options(&self, spec: &OptionsSpec) -> Options {
        let mut options = Options::default();
        options.base.base.theme = spec.theme.clone();
        options.base.base.theme_type = spec.theme_type;
        options.base.base.overflow = spec.overflow;
        options.base.base.collapsed = spec.collapsed;
        options.base.base.disable_file_header = spec.disable_file_header;
        options.base.base.disable_virtualization_buffers = spec.disable_virtualization_buffers;
        options.base.base.disable_line_numbers = spec.disable_line_numbers;
        options.base.base.unsafe_css = spec.unsafe_css.clone();
        options.base.diff_style = spec.diff_style;
        options.base.disable_background = spec.disable_background;
        options.base.diff_indicators = spec.diff_indicators;
        options.base.expand_unchanged = spec.expand_unchanged;
        options.interaction.line_hover_highlight = spec.line_hover_highlight;
        options.interaction.enable_gutter_utility = spec.enable_gutter_utility;
        options.has_diff_file_loader = spec.load_diff_files;
        options.hunk_separators = spec.hunk_separators.as_deref().map(|name| match name {
            "simple" => FileDiffHunkSeparators::Simple,
            "metadata" => FileDiffHunkSeparators::Metadata,
            "line-info" => FileDiffHunkSeparators::LineInfo,
            "line-info-basic" => FileDiffHunkSeparators::LineInfoBasic,
            "fn" => FileDiffHunkSeparators::Custom(
                self.callbacks
                    .separators
                    .get_or_init(|| {
                        let dom = Rc::clone(&self.dom);
                        PtrEq(Rc::new(move |hunk: &crate::types::lines::HunkData| {
                            dom.emit(json!([
                                "cb",
                                "hunkSeparators",
                                hunk.slot_name,
                                hunk.hunk_index
                            ]));
                            Some(element(
                                "span",
                                &[("data-separator-content", hunk.slot_name.clone())],
                                None,
                            ))
                        }))
                    })
                    .clone(),
            ),
            other => panic!("unknown separators {other}"),
        });
        if spec.render_annotation {
            options.render_annotation = Some(
                self.callbacks
                    .annotation
                    .get_or_init(|| {
                        let dom = Rc::clone(&self.dom);
                        let pool = Rc::clone(&self.pool);
                        PtrEq(Rc::new(
                            move |annotation: &DiffLineAnnotation<i64>,
                                  file_diff: &FileDiffMetadata| {
                                dom.emit(json!([
                                    "cb",
                                    "renderAnnotation",
                                    side_name(annotation.side),
                                    annotation.line_number,
                                    pool.diff_name(file_diff).unwrap_or_else(|| "?".to_owned()),
                                ]));
                                if annotation.line_number.is_multiple_of(5) {
                                    return None;
                                }
                                Some(element(
                                    "span",
                                    &[(
                                        "data-annotation",
                                        format!(
                                            "{}-{}-{}",
                                            side_name(annotation.side),
                                            annotation.line_number,
                                            annotation.metadata.expect("metadata")
                                        ),
                                    )],
                                    Some("note"),
                                ))
                            },
                        ))
                    })
                    .clone(),
            );
        }
        self.add_header_callbacks(spec, &mut options);
        if spec.render_gutter_utility {
            options.render_gutter_utility = Some(
                self.callbacks
                    .gutter_utility
                    .get_or_init(|| {
                        let dom = Rc::clone(&self.dom);
                        PtrEq(Rc::new(move |_: &dyn Fn() -> _| {
                            dom.emit(json!(["cb", "renderGutterUtility"]));
                            Some(element(
                                "div",
                                &[("data-utility", String::new())],
                                Some("u"),
                            ))
                        }))
                    })
                    .clone(),
            );
        }
        if spec.on_post_render {
            options.on_post_render = Some(
                self.callbacks
                    .post_render
                    .get_or_init(|| {
                        let dom = Rc::clone(&self.dom);
                        PtrEq(Rc::new(move |node: &NodeKey, phase: PostRenderPhase| {
                            dom.emit(json!([
                                "cb",
                                "onPostRender",
                                match phase {
                                    PostRenderPhase::Mount => "mount",
                                    PostRenderPhase::Update => "update",
                                    PostRenderPhase::Unmount => "unmount",
                                },
                                dom.name_of(*node),
                            ]));
                        }))
                    })
                    .clone(),
            );
        }
        options
    }

    fn add_header_callbacks(&self, spec: &OptionsSpec, options: &mut Options) {
        for (enabled, field, tag) in [
            (spec.render_header_prefix, "renderHeaderPrefix", "prefix"),
            (
                spec.render_header_filename_suffix,
                "renderHeaderFilenameSuffix",
                "suffix",
            ),
            (
                spec.render_header_metadata,
                "renderHeaderMetadata",
                "metadata",
            ),
            (spec.render_custom_header, "renderCustomHeader", "custom"),
        ] {
            if !enabled {
                continue;
            }
            let callback = self
                .callbacks
                .header
                .borrow_mut()
                .entry(field)
                .or_insert_with(|| {
                    let dom = Rc::clone(&self.dom);
                    PtrEq(Rc::new(move |file_diff: &FileDiffMetadata| {
                        dom.emit(json!(["cb", field, file_diff.name]));
                        if tag == "suffix" && file_diff.change_type == ChangeTypes::New {
                            return None;
                        }
                        Some(element(
                            "span",
                            &[(&format!("data-{tag}"), String::new())],
                            Some(&file_diff.name),
                        ))
                    }))
                })
                .clone();
            match field {
                "renderHeaderPrefix" => options.render_header_prefix = Some(callback),
                "renderHeaderFilenameSuffix" => {
                    options.render_header_filename_suffix = Some(callback);
                }
                "renderHeaderMetadata" => options.render_header_metadata = Some(callback),
                _ => options.render_custom_header = Some(callback),
            }
        }
    }
}

// ---- snapshots ------------------------------------------------------------------

fn fnv32(text: &str) -> String {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in text.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{hash:08x}")
}

/// `name:value;` declarations, trimmed; `None` when there are none.
fn canonical_style(style: &str) -> Option<String> {
    let declarations: String = style
        .split(';')
        .map(str::trim)
        .filter(|declaration| !declaration.is_empty())
        .map(|declaration| {
            let (name, value) = declaration.split_once(':').expect("a declaration");
            format!("{}:{};", name.trim(), value.trim())
        })
        .collect();
    (!declarations.is_empty()).then_some(declarations)
}

fn signature(dom: &FakeDom, node: NodeKey) -> String {
    let tag = dom.doc.tag_name(node);
    if tag.is_empty() {
        return dom.doc.text_content(node);
    }
    let tag = tag.to_lowercase();
    let attributes = dom
        .doc
        .attributes(node)
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let children: String = dom
        .doc
        .child_nodes(node)
        .into_iter()
        .map(|child| signature(dom, child))
        .collect();
    format!("<{tag} {attributes}>{children}</{tag}>")
}

fn snapshot_tree(dom: &FakeDom, node: NodeKey) -> Value {
    let tag = dom.doc.tag_name(node);
    if tag.is_empty() {
        return json!(dom.doc.text_content(node));
    }
    if tag == "SVG" && dom.doc.get_attribute(node, "data-icon-sprite").is_some() {
        return json!(["SVG", format!("sprite:{}", fnv32(&signature(dom, node)))]);
    }
    let mut attributes: Vec<(String, String)> = dom
        .doc
        .attributes(node)
        .into_iter()
        .filter_map(|(name, value)| {
            if name == "style" {
                if dom.doc.get_attribute(node, "slot").as_deref() == Some("gutter-utility-slot") {
                    return None;
                }
                canonical_style(&value).map(|style| (name, style))
            } else {
                Some((name, value))
            }
        })
        .collect();
    attributes.sort();
    let children: Vec<Value> = dom
        .doc
        .child_nodes(node)
        .into_iter()
        .map(|child| snapshot_tree(dom, child))
        .collect();
    let shadow = dom.doc.shadow_root(node).map(|root| {
        dom.doc
            .child_nodes(root)
            .into_iter()
            .map(|child| snapshot_tree(dom, child))
            .collect::<Vec<_>>()
    });
    json!([tag, attributes, children, shadow])
}

fn state_snapshot(instance: &Instance, pool: &Pool) -> Value {
    let state = instance.state();
    let empty = Rc::clone(&state.empty_line_annotations);
    let name_of =
        |diff: &Rc<FileDiffMetadata>| pool.diff_name(diff).unwrap_or_else(|| "parsed".to_owned());
    json!({
        "inputDiff": state.file_diff.as_ref().map(name_of),
        "renderedDiff": state.rendered_data.as_ref().map(|data| name_of(&data.file_diff)),
        "renderedAnnotations": state
            .rendered_data
            .as_ref()
            .map(|data| pool.annotations_name(&data.line_annotations, &empty)),
        "pendingDiff": state.pending_data.as_ref().map(|data| name_of(&data.file_diff)),
        "pendingAnnotations": state
            .pending_data
            .as_ref()
            .map(|data| pool.annotations_name(&data.line_annotations, &empty)),
        "lastRowCount": state.last_row_count,
        "pendingFiles": state
            .pending_files
            .as_ref()
            .map(|diff| pool.diff_name(diff).unwrap_or_else(|| "?".to_owned())),
        "hasFiles": [state.deletion_file.is_some(), state.addition_file.is_some()],
    })
}

/// The entries of the trace the JavaScript side records.
fn recorded(entry: &Value) -> bool {
    match entry[0].as_str() {
        Some(
            "observer.create"
            | "observe"
            | "unobserve"
            | "observer.disconnect"
            | "cb"
            | "ownerRender",
        ) => true,
        Some("listener.add" | "listener.remove") => entry[2] == "scroll",
        _ => false,
    }
}

fn range_of(spec: Option<RangeSpec>) -> Option<RenderRange> {
    spec.map(|spec| RenderRange {
        starting_line: spec.starting_line,
        total_lines: spec.total_lines.unwrap_or(f64::INFINITY),
        buffer_before: spec.buffer_before,
        buffer_after: spec.buffer_after,
    })
}

fn container_trees(
    dom: &FakeDom,
    containers: &HashMap<String, NodeKey>,
) -> serde_json::Map<String, Value> {
    let mut names: Vec<(&String, &NodeKey)> = containers.iter().collect();
    names.sort_by(|a, b| a.0.cmp(b.0));
    names
        .into_iter()
        .map(|(name, node)| (name.clone(), snapshot_tree(dom, *node)))
        .collect()
}

fn run_case(templates: &Templates, case: &Case) {
    let scenario = &case.scenario;
    let dom = Rc::new(FakeDom::default());
    dom.shared.path_names.set(true);
    dom.recording().set(true);
    let pool = Rc::new(Pool {
        diffs: RefCell::new(
            templates
                .diffs
                .iter()
                .map(|(name, diff)| (name.clone(), Rc::new(diff.clone())))
                .collect(),
        ),
        annotations: templates
            .annotations
            .iter()
            .map(|(name, list)| (name.clone(), Rc::new(list.clone())))
            .collect(),
    });
    let world = World {
        dom: Rc::clone(&dom),
        pool: Rc::clone(&pool),
        callbacks: Callbacks::default(),
    };
    let hub = ResizeObserverHub::new(Rc::clone(&dom));
    let containers: RefCell<HashMap<String, NodeKey>> = RefCell::default();
    let container = |name: &str| -> NodeKey {
        *containers
            .borrow_mut()
            .entry(name.to_owned())
            .or_insert_with(|| {
                let node = dom.doc.create_element(if name == "W" {
                    "div"
                } else {
                    "diffs-container"
                });
                dom.shared.labels.borrow_mut().insert(node, name.to_owned());
                node
            })
    };
    let make_instance = |spec: &OptionsSpec| -> Rc<Instance> {
        let owner = Rc::clone(&dom);
        Instance::new(
            Rc::clone(&dom),
            Rc::clone(&hub),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            Rc::new(PlainTextHighlighter),
            world.build_options(spec),
            scenario
                .managed
                .then(|| Rc::new(move || owner.emit(json!(["ownerRender"]))) as Rc<dyn Fn()>),
        )
    };
    let mut instances: Vec<Rc<Instance>> =
        vec![make_instance(&scenario.options.clone().unwrap_or_default())];
    let input = |step: &Step| -> FileDiffRenderProps<i64> {
        let mut props = FileDiffRenderProps::default();
        if let Some(name) = &step.diff {
            props.file_diff = Some(pool.diff(name));
        }
        if let Some(name) = &step.files {
            let source = templates
                .files
                .get(name)
                .or_else(|| templates.partials.get(name))
                .expect("a named file pair");
            props.old_file = Some(source.old.clone());
            props.new_file = Some(source.new.clone());
        }
        if let Some(name) = &step.annotations {
            props.line_annotations = Some(Rc::clone(&pool.annotations[name]));
        }
        props.render_range = range_of(step.range);
        props.force_render = step.force;
        props.defer_managers = step.defer;
        props.prevent_emit = step.no_emit;
        props.file_container = step.container.as_deref().map(&container);
        props.container_wrapper = step.wrapper.as_deref().map(&container);
        props
    };

    assert_eq!(
        scenario.steps.len(),
        case.snapshots.len(),
        "scenario `{}`",
        scenario.name
    );
    let mut trace_start = 0;
    for (index, (step, expected)) in scenario.steps.iter().zip(&case.snapshots).enumerate() {
        let which = step.instance.unwrap_or(0);
        while instances.len() <= which {
            instances.push(make_instance(&step.options.clone().unwrap_or_default()));
        }
        let instance = Rc::clone(&instances[which]);
        let mut ret = Value::Null;
        match step.op.as_str() {
            "render" => ret = json!(instance.render(input(step))),
            "setOptions" => {
                instance.set_options(Some(
                    world.build_options(step.spec.as_ref().expect("a spec")),
                ));
            }
            "setThemeType" => instance.set_theme_type(step.theme_type.expect("a theme type")),
            "setLineAnnotations" => {
                instance.set_line_annotations(Rc::clone(
                    &pool.annotations[step.annotations.as_ref().expect("annotations")],
                ));
            }
            "rerender" => instance.rerender(),
            "flushManagers" => instance.flush_managers(),
            "hydrate" => instance.hydrate(input(step)),
            "filesLoaded" => {
                let pending = instance.pending_files().expect("a load was requested");
                let name = pool.diff_name(&pending).expect("a named diff");
                let source = &templates.partials[step.files.as_ref().expect("files")];
                instance.files_loaded(
                    &pending,
                    &FileDiffLoadedFiles::Changed {
                        old_file: source.old.clone().expect("old"),
                        new_file: source.new.clone().expect("new"),
                    },
                );
                // The merge hydrates a copy (see `loading.rs`): the host adopts it, so
                // the name now stands for the hydrated diff.
                let hydrated = instance.get_input_diff().expect("an input");
                pool.diffs.borrow_mut().insert(name, hydrated);
            }
            other => panic!("unknown step {other}"),
        }

        let trace: Vec<Value> = dom.trace()[trace_start..]
            .iter()
            .filter(|entry| recorded(entry))
            .cloned()
            .collect();
        trace_start = dom.trace().len();
        let trees = container_trees(&dom, &containers.borrow());
        let actual = json!({
            "ret": ret,
            "trace": trace,
            "state": state_snapshot(&instance, &pool),
            "trees": trees,
        });
        assert_json_eq(
            &format!("scenario `{}` step {index} ({})", scenario.name, step.op),
            expected,
            &actual,
        );
    }
}

#[test]
fn file_diff_render_matches_the_published_class() {
    let golden: Golden = load_golden("s12b_render.json");
    assert!(golden.cases.len() > 240);
    for case in &golden.cases {
        run_case(&golden.templates, case);
    }
}
