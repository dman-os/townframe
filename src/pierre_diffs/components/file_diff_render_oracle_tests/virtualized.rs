//! Replay of `oracle/s12b_virtualized.mjs` (S12b-3): the published
//! `VirtualizedFileDiff` connected to the published `Virtualizer` over jsdom,
//! against the port over `FakeDom`'s rule geometry. A child of the render
//! oracle's tests: it reuses their scenario types, option builder, pool and
//! tree snapshots. Test-only, original to this crate.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::Value;
use serde_json::json;

use super::*;
use crate::components::virtualized_file_diff::instance::VirtualizedFileDiff;
use crate::components::virtualizer::IntersectionEntry;
use crate::components::virtualizer::ResizeEntry;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::SubscribedInstance;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;

type VInstance = VirtualizedFileDiff<FakeDom, hast::Element, i64>;

/// The entries of the trace the JavaScript side records.
fn recorded_here(entry: &Value) -> bool {
    match entry[0].as_str() {
        Some(
            "observers.create"
            | "resize.observe"
            | "resize.disconnect"
            | "intersection.observe"
            | "intersection.unobserve"
            | "intersection.disconnect"
            | "window.addEventListener"
            | "window.removeEventListener"
            | "window.scrollTo"
            | "raf.request"
            | "raf.cancel"
            | "log",
        ) => true,
        _ => recorded(entry),
    }
}

fn names_of(pool: &Pool, diff: Option<&Rc<FileDiffMetadata>>) -> Value {
    json!(diff.map(|diff| pool.diff_name(diff).unwrap_or_else(|| "parsed".to_owned())))
}

fn instance_state(instance: &VInstance, virtualizer: &Virtualizer<FakeDom>, pool: &Pool) -> Value {
    let file_diff = instance.file_diff();
    let (layout_state, estimates) = {
        let layout = instance.layout();
        let cache = &layout.cache;
        let mut deltas: Vec<(i64, f64)> = cache
            .height_deltas
            .iter()
            .map(|(key, delta)| (*key, *delta))
            .collect();
        deltas.sort_by_key(|(key, _)| *key);
        (
            json!({
                "top": layout.top,
                "height": layout.height,
                "layoutDirty": layout.layout_dirty,
                "measuredTotal": cache.measured_height_delta_total,
                "fileAnnotationHeight": cache.file_annotation_height,
                "deltas": deltas,
                "totalLines": cache.total_lines,
                "checkpoints": cache.checkpoints.len(),
            }),
            json!([cache.estimated_split_height, cache.estimated_unified_height]),
        )
    };
    let state = file_diff.state();
    let empty = Rc::clone(&state.empty_line_annotations);
    let pending_render = instance.pending_render_diffs().map(|(working, diff)| {
        json!([names_of(pool, Some(&working)), names_of(pool, Some(&diff))])
    });
    let (observers, visible, window_top, window_bottom) = virtualizer.observed_state();
    let mut expanded: Vec<(usize, usize, usize)> = file_diff
        .hunks_renderer()
        .get_expanded_hunks_map()
        .iter()
        .map(|(index, region)| (*index, region.from_start, region.from_end))
        .collect();
    expanded.sort_unstable();
    let mut result = layout_state;
    let fields = result.as_object_mut().expect("an object");
    fields.insert("isVisible".to_owned(), json!(instance.is_visible()));
    fields.insert("isSetup".to_owned(), json!(instance.is_setup()));
    fields.insert("estimates".to_owned(), estimates);
    fields.insert(
        "renderRange".to_owned(),
        json!(state.render_range.map(|range| json!([
            range.starting_line,
            range.total_lines.is_finite().then_some(range.total_lines),
            range.buffer_before,
            range.buffer_after
        ]))),
    );
    fields.insert(
        "forceRenderOverride".to_owned(),
        json!(state.force_render_override),
    );
    fields.insert(
        "inputDiff".to_owned(),
        names_of(pool, state.file_diff.as_ref()),
    );
    fields.insert(
        "renderedDiff".to_owned(),
        names_of(
            pool,
            state.rendered_data.as_ref().map(|data| &data.file_diff),
        ),
    );
    fields.insert(
        "renderedAnnotations".to_owned(),
        json!(
            state
                .rendered_data
                .as_ref()
                .map(|data| pool.annotations_name(&data.line_annotations, &empty))
        ),
    );
    fields.insert(
        "pendingData".to_owned(),
        json!(state.pending_data.as_ref().map(|data| json!([
            names_of(pool, Some(&data.file_diff)),
            pool.annotations_name(&data.line_annotations, &empty)
        ]))),
    );
    fields.insert("pendingRender".to_owned(), json!(pending_render));
    fields.insert("enabled".to_owned(), json!(state.enabled));
    fields.insert("mounted".to_owned(), json!(state.mounted));
    fields.insert("expanded".to_owned(), json!(expanded));
    fields.insert(
        "virtualizer".to_owned(),
        json!({
            "observers": observers,
            "visible": visible,
            "window": [window_top, window_bottom],
        }),
    );
    result
}

/// One scenario's world: the fake host, the virtualizer and the instances.
struct Replay<'a> {
    templates: &'a Templates,
    dom: Rc<FakeDom>,
    pool: Rc<Pool>,
    world: World,
    hub: Rc<ResizeObserverHub<FakeDom>>,
    /// One queue for everything, as upstream's module-level one is.
    render_queue: UniversalRenderingManager<FakeDom>,
    virtualizer: Rc<Virtualizer<FakeDom>>,
    containers: RefCell<HashMap<String, NodeKey>>,
    managed: bool,
    /// A `setup` ran: the observer callbacks exist (the JavaScript side captures
    /// them when the observers are created).
    ever_setup: bool,
}

impl<'a> Replay<'a> {
    fn new(templates: &'a Templates, scenario: &Scenario) -> Self {
        let dom = Rc::new(FakeDom::default());
        dom.shared.path_names.set(true);
        dom.recording().set(true);
        {
            let geometry = scenario.geometry;
            let mut world = dom.world.borrow_mut();
            world.rules = true;
            world.viewport_height = geometry.viewport_height.unwrap_or(600.0);
            world.scroll_height = geometry.scroll_height.unwrap_or(5_000.0);
            world.container_top = geometry.container_top.unwrap_or(0.0);
            world.container_height = geometry.container_height.unwrap_or(0.0);
        }
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
        let render_queue = UniversalRenderingManager::new(Rc::clone(&dom));
        Self {
            templates,
            world: World {
                dom: Rc::clone(&dom),
                pool: Rc::clone(&pool),
                callbacks: Callbacks::default(),
            },
            hub: ResizeObserverHub::new(Rc::clone(&dom)),
            virtualizer: Virtualizer::new(
                Rc::clone(&dom),
                render_queue.clone(),
                VirtualizerConfig::default(),
            ),
            render_queue,
            dom,
            pool,
            containers: RefCell::default(),
            managed: scenario.managed,
            ever_setup: false,
        }
    }

    fn container(&self, name: &str) -> NodeKey {
        *self
            .containers
            .borrow_mut()
            .entry(name.to_owned())
            .or_insert_with(|| {
                let node = self.dom.doc.create_element(if name == "W" {
                    "div"
                } else {
                    "diffs-container"
                });
                self.dom
                    .shared
                    .labels
                    .borrow_mut()
                    .insert(node, name.to_owned());
                node
            })
    }

    fn make_instance(&self, spec: &OptionsSpec) -> Rc<VInstance> {
        let owner = Rc::clone(&self.dom);
        VirtualizedFileDiff::new(
            Rc::clone(&self.dom),
            Rc::clone(&self.hub),
            self.render_queue.clone(),
            Rc::new(PlainTextHighlighter),
            self.world.build_options(spec),
            Rc::clone(&self.virtualizer),
            Default::default(),
            self.managed
                .then(|| Rc::new(move || owner.emit(json!(["ownerRender"]))) as Rc<dyn Fn()>),
        )
    }

    fn input(&self, step: &Step) -> FileDiffRenderProps<i64> {
        let mut props = FileDiffRenderProps::default();
        if let Some(name) = &step.diff {
            props.file_diff = Some(self.pool.diff(name));
        }
        if let Some(name) = &step.files {
            let source = self.templates.files.get(name).expect("a named file pair");
            props.old_file = Some(source.old.clone());
            props.new_file = Some(source.new.clone());
        }
        if let Some(name) = &step.annotations {
            props.line_annotations = Some(Rc::clone(&self.pool.annotations[name]));
        }
        props.render_range = range_of(step.range);
        props.force_render = step.force;
        props.defer_managers = step.defer;
        props.prevent_emit = step.no_emit;
        props.file_container = step.container.as_deref().map(|name| self.container(name));
        props
    }

    /// The scripted world's own steps: scroll, resize, geometry, frames and the
    /// observers' deliveries.
    fn drive_world(&self, step: &Step) {
        let virtualizer = &self.virtualizer;
        match step.op.as_str() {
            "scroll" => {
                {
                    let mut world = self.dom.world.borrow_mut();
                    let limit = (world.scroll_height - world.viewport_height).max(0.0);
                    world.scroll_top = step.top.expect("a top").clamp(0.0, limit);
                }
                if virtualizer.get_root().is_some() {
                    virtualizer.handle_window_scroll();
                }
            }
            "resize" => {
                self.dom.world.borrow_mut().viewport_height = step.height.expect("a height");
                if virtualizer.get_root().is_some() {
                    virtualizer.handle_window_resize();
                }
            }
            "geometry" => {
                let mut world = self.dom.world.borrow_mut();
                if let Some(value) = step.container_top {
                    world.container_top = value;
                }
                if let Some(value) = step.container_height {
                    world.container_height = value;
                }
                if let Some(value) = step.scroll_height {
                    world.scroll_height = value;
                }
            }
            "frames" => {
                self.dom.frames.flush_frames(step.limit.unwrap_or(16), 0.0);
            }
            "intersect" if self.ever_setup => {
                virtualizer.handle_intersection_change(&[IntersectionEntry {
                    target: self.container(step.container.as_deref().unwrap_or("A")),
                    is_intersecting: step.visible,
                }]);
            }
            "docResize" if self.ever_setup => {
                virtualizer.handle_container_resize(&[ResizeEntry {
                    target: self.container("A"),
                    block_size: step.block_size.expect("a block size"),
                }]);
            }
            "intersect" | "docResize" => {}
            other => panic!("unknown step {other}"),
        }
    }

    /// `filesLoaded`: the host resolves the load the instance asked for.
    fn files_loaded(&self, instance: &VInstance, step: &Step) {
        let loading = instance.file_diff().loading();
        let pending = loading.pending_files().expect("a load was requested");
        let name = self.pool.diff_name(&pending).expect("a named diff");
        let source = &self.templates.partials[step.files.as_ref().expect("files")];
        loading.files_loaded(
            &pending,
            &FileDiffLoadedFiles::Changed {
                old_file: source.old.clone().expect("old"),
                new_file: source.new.clone().expect("new"),
            },
        );
        // The merge hydrates a copy: the host adopts it, so the name now stands
        // for the hydrated diff.
        let hydrated = instance.file_diff().get_input_diff().expect("an input");
        self.pool.diffs.borrow_mut().insert(name, hydrated);
    }

    /// Performs one step and returns its value.
    fn perform(&mut self, step: &Step, instance: &Rc<VInstance>) -> Value {
        let mut ret = Value::Null;
        match step.op.as_str() {
            "setup" => {
                self.ever_setup = true;
                self.virtualizer.setup(Root::Document, None);
            }
            "virtualizerCleanUp" => self.virtualizer.clean_up(),
            "render" => ret = json!(instance.render(self.input(step))),
            "onRender" => ret = json!(SubscribedInstance::on_render(&**instance, step.dirty)),
            "setOptions" => instance.set_options(Some(
                self.world
                    .build_options(step.spec.as_ref().expect("a spec")),
            )),
            "setThemeType" => instance.set_theme_type(step.theme_type.expect("a theme type")),
            "setLineAnnotations" => instance.set_line_annotations(Rc::clone(
                &self.pool.annotations[step.annotations.as_ref().expect("annotations")],
            )),
            "rerender" => instance.rerender(),
            "expandHunk" => instance.expand_hunk(
                step.hunk_index.expect("a hunk index"),
                direction_of(step.direction.as_deref().expect("a direction")),
                step.count,
            ),
            "isLineRenderable" => {
                ret = json!(instance.is_line_renderable(step.line.expect("a line")));
            }
            "revealLine" => ret = json!(instance.reveal_line(step.line.expect("a line"))),
            "reconcile" => ret = json!(instance.reconcile_heights()),
            "filesLoaded" => self.files_loaded(instance, step),
            "cleanUp" => instance.clean_up(step.recycle),
            _ => self.drive_world(step),
        }
        ret
    }
}

fn run_virtual_case(templates: &Templates, case: &Case) {
    let scenario = &case.scenario;
    let mut replay = Replay::new(templates, scenario);
    let mut instances: Vec<Rc<VInstance>> =
        vec![replay.make_instance(&scenario.options.clone().unwrap_or_default())];
    assert_eq!(
        scenario.steps.len(),
        case.snapshots.len(),
        "scenario `{}`",
        scenario.name
    );
    let mut trace_start = 0;
    let mut previous_trees = json!({});
    for (index, (step, expected)) in scenario.steps.iter().zip(&case.snapshots).enumerate() {
        let which = step.instance.unwrap_or(0);
        while instances.len() <= which {
            instances.push(replay.make_instance(&step.options.clone().unwrap_or_default()));
        }
        let instance = Rc::clone(&instances[which]);
        let ret = replay.perform(step, &instance);

        let dom = &replay.dom;
        let trace = ordered_trace(
            dom.trace()[trace_start..]
                .iter()
                .filter(|entry| recorded_here(entry))
                .cloned()
                .collect(),
        );
        trace_start = dom.trace().len();
        let trees = Value::Object(container_trees(dom, &replay.containers.borrow()));
        let unchanged = expected["trees"] == "same";
        let actual = json!({
            "ret": ret,
            "trace": trace,
            "state": instance_state(&instance, &replay.virtualizer, &replay.pool),
            "trees": if unchanged { json!("same") } else { trees.clone() },
        });
        if unchanged {
            assert_json_eq(
                &format!(
                    "scenario `{}` step {index} ({}) left the DOM alone",
                    scenario.name, step.op
                ),
                &previous_trees,
                &trees,
            );
        }
        previous_trees = trees;
        if expected["trace"] != actual["trace"] {
            eprintln!(
                "trace expected {}\n      actual {}",
                expected["trace"], actual["trace"]
            );
        }
        assert_json_eq(
            &format!("scenario `{}` step {index} ({})", scenario.name, step.op),
            expected,
            &actual,
        );
    }
}

#[test]
fn virtualized_file_diff_integration_matches_the_published_class() {
    let golden: Golden = load_golden("s12b_virtualized.json");
    assert!(golden.cases.len() > 130);
    for case in &golden.cases {
        run_virtual_case(&golden.templates, case);
    }
}
