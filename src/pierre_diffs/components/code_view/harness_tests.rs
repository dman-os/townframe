//! The harness the upstream `CodeView` tests that run the render pass share
//! (`test/domHarness.ts`), over `FakeDom`. Test-only, original to this crate.
//!
//! * `installDom()` is `FakeDom::default()` (the world has no layout, and a
//!   `ResizeObserver` that delivers nothing on its own: tests call
//!   `handle_resize` as upstream's do).
//! * `createRoot({ height })` is `create_root`: a root whose `scrollTop` is a
//!   plain property and whose `scrollTo` does not clamp; `createClampingRoot`
//!   of `CodeView.scrollAnchoring.test.ts` is `create_clamping_root`.
//! * `await wait(0)` lets the `setTimeout(0)` a queued render frame is run
//!   `wait`: it runs the frames pending now (`wait_a_frame`). A render a frame
//!   queues while it runs waits for the next one, as it does upstream.
//! * `makeFile`/`makeFileItem` build file items, which are DEFER with
//!   `VirtualizedFile` (closure notes, Q1): `file_like_item` is a diff item
//!   over `lineCount` lines whose first line changed, which the viewer renders
//!   in full (`expandUnchanged`), so its height is a file's plus a hunk
//!   separator and the deleted line's row in the unified style (the cases that
//!   assert numbers say what changed, divergences.psv).
//! * `getRenderedItems()` (the React slot coordinator's snapshot) is
//!   `rendered_items`: the items holding a shell, in order.

use std::rc::Rc;

use super::CodeView;
use super::options::CodeViewOptions;
use super::state::ItemRef;
use crate::components::virtualized_file_diff::layout_upstream_tests::parse_files;
use crate::components::virtualized_file_diff::layout_upstream_tests::plain_file;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverEntry;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewItemScrollTarget;
use crate::types::view::CodeViewLineScrollTarget;
use crate::types::view::CodeViewPositionScrollTarget;
use crate::types::view::CodeViewRangeScrollTarget;
use crate::types::view::CodeViewScrollBehavior;
use crate::types::view::CodeViewScrollTarget;
use crate::types::view::ScrollAlign;
use crate::types::view::SelectedLineRange;
use crate::types::view::SelectionSide;

pub(super) type View = CodeView<FakeDom, u8, u8>;

/// The viewer, the world and the root a case runs in.
pub(super) struct Page {
    pub dom: Rc<FakeDom>,
    pub view: Rc<View>,
    pub root: NodeKey,
}

/// `options` with `expandUnchanged` on unless the case set it, so
/// `file_like_item`s render every line. Options a case hands to `set_options`
/// go through it too: `setOptions` replaces the whole option set.
pub(super) fn with_defaults(mut options: CodeViewOptions<u8, u8>) -> CodeViewOptions<u8, u8> {
    options.diff.base.expand_unchanged.get_or_insert(true);
    options
}

/// A viewer over `dom` (see [`with_defaults`]).
pub(super) fn viewer(dom: &Rc<FakeDom>, options: CodeViewOptions<u8, u8>) -> Rc<View> {
    CodeView::new(
        Rc::clone(dom),
        ResizeObserverHub::new(Rc::clone(dom)),
        UniversalRenderingManager::new(Rc::clone(dom)),
        Rc::new(PlainTextHighlighter),
        with_defaults(options),
    )
}

/// `createRoot({ height })`: a root nothing clamps.
pub(super) fn create_root(dom: &FakeDom, height: f64) -> NodeKey {
    let root = dom.doc.create_element("div");
    dom.heights.borrow_mut().insert(root, height);
    let mut world = dom.world.borrow_mut();
    world.viewport_height = height;
    world.scroll_height = f64::INFINITY;
    root
}

/// `createClampingRoot()`: a root whose `scrollTo` clamps to the container's
/// declared extent, as a browser does.
pub(super) fn create_clamping_root(dom: &FakeDom, height: f64) -> NodeKey {
    let root = create_root(dom, height);
    dom.world.borrow_mut().clamp_root = Some(root);
    root
}

/// A viewer set up on a root of `height`.
pub(super) fn page(options: CodeViewOptions<u8, u8>, height: f64) -> Page {
    page_on(Rc::new(FakeDom::default()), options, height)
}

/// [`page`] over a world the case has prepared (`installDom({ navigator })`).
pub(super) fn page_on(dom: Rc<FakeDom>, options: CodeViewOptions<u8, u8>, height: f64) -> Page {
    let view = viewer(&dom, options);
    let root = create_root(&dom, height);
    view.setup(root);
    Page { dom, view, root }
}

/// `viewer.setup(createClampingRoot())`
pub(super) fn clamping_page(
    dom: Rc<FakeDom>,
    options: CodeViewOptions<u8, u8>,
    height: f64,
) -> Page {
    let view = viewer(&dom, options);
    let root = create_clamping_root(&dom, height);
    view.setup(root);
    Page { dom, view, root }
}

impl Page {
    /// `await wait(0)`
    pub(super) fn wait_a_frame(&self) {
        self.dom.frames.run_round(self.dom.clock.get());
    }

    /// `await renderItems(viewer, items)`
    pub(super) fn render_items(&self, items: &[CodeViewItem<u8>]) {
        self.view.set_items(items);
        self.view.render(true);
        self.wait_a_frame();
    }

    /// `root.scrollTop`
    pub(super) fn scroll_top(&self) -> f64 {
        self.dom.world.borrow().scroll_top
    }

    /// `root.scrollTop = top`
    pub(super) fn set_scroll_top(&self, top: f64) {
        self.dom.world.borrow_mut().scroll_top = top;
    }

    /// `dispatchScroll(root)`
    pub(super) fn dispatch_scroll(&self) {
        self.view.handle_scroll();
    }

    /// `root.scrollTop = top; dispatchScroll(root); viewer.render(true)`
    pub(super) fn scroll_and_render(&self, top: f64) {
        self.set_scroll_top(top);
        self.dispatch_scroll();
        self.view.render(true);
    }

    /// `viewer.cleanUp(); await wait(0)`
    pub(super) fn finish(&self) {
        self.view.clean_up();
        self.wait_a_frame();
    }

    /// `getRenderedItems()`
    pub(super) fn rendered_items(&self) -> Vec<ItemRef<FakeDom, u8, u8>> {
        rendered_items(&self.view)
    }

    pub(super) fn rendered_ids(&self) -> Vec<String> {
        self.rendered_items()
            .iter()
            .map(|item| item.borrow().id().to_owned())
            .collect()
    }

    /// The shell of the first rendered item with `id`.
    pub(super) fn element_of(&self, id: &str) -> Option<NodeKey> {
        self.rendered_items()
            .iter()
            .find(|item| item.borrow().id() == id)
            .and_then(|item| item.borrow().element)
    }

    /// The `ResizeObserver` entry for `target` at `block_size`.
    pub(super) fn resize(&self, target: NodeKey, block_size: f64) {
        self.view.handle_resize(&[ResizeObserverEntry {
            target,
            border_box_block_size: block_size,
            content_box_inline_size: 1000.0,
        }]);
    }
}

/// `getRenderedItems()`: the items that hold a shell, in order.
pub(super) fn rendered_items(view: &View) -> Vec<ItemRef<FakeDom, u8, u8>> {
    view.state
        .borrow()
        .items
        .iter()
        .filter(|item| item.borrow().element.is_some())
        .cloned()
        .collect()
}

/// `getHeaderElement()`
pub(super) fn header_element(view: &View) -> Option<NodeKey> {
    view.state.borrow().header.element
}

/// `getFooterElement()`
pub(super) fn footer_element(view: &View) -> Option<NodeKey> {
    view.state.borrow().footer.element
}

/// A diff item over `old` and `new` lines (`context` lines around a change).
pub(super) fn diff_item(
    id: &str,
    old: &[String],
    new: &[String],
    context: Option<usize>,
) -> CodeViewItem<u8> {
    let parsed = parse_files_with_context(old, new, context);
    CodeViewItem::Diff(CodeViewDiffItem {
        id: id.to_owned(),
        file_diff: Rc::new(parsed),
        annotations: None,
        version: None,
        collapsed: None,
        edit: None,
    })
}

fn parse_files_with_context(
    old: &[String],
    new: &[String],
    context: Option<usize>,
) -> crate::types::patch::FileDiffMetadata {
    use crate::diff::PatchOptions;
    use crate::types::patch::DiffFileInput;
    use crate::types::patch::ParseDiffOptions;
    use crate::utils::parse_diff_from_file::parse_diff_from_file;

    let file = |contents: &[String]| plain_file("src/example.ts", &contents.join("\n"));
    parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file(old),
            new_file: file(new),
        },
        &ParseDiffOptions {
            create_patch: PatchOptions {
                context,
                ..PatchOptions::default()
            },
            ..ParseDiffOptions::default()
        },
        false,
    )
    .expect("the files diff")
    .value
}

/// `count` lines `{prefix}{n}`, `n` from 1.
pub(super) fn lines(count: usize, prefix: &str) -> Vec<String> {
    (1..=count)
        .map(|index| format!("{prefix}{index}"))
        .collect()
}

/// `makeFileItem(id, lineCount)` / `makeFileItem(id, label, lineCount)`: see
/// the header.
pub(super) fn file_like_item(id: &str, label: &str, line_count: usize) -> CodeViewItem<u8> {
    let old: Vec<String> = (1..=line_count)
        .map(|index| format!("{label} line {index}"))
        .collect();
    let mut new = old.clone();
    if let Some(first) = new.first_mut() {
        first.push_str(" changed");
    }
    diff_item(id, &old, &new, None)
}

/// `parseDiffFromFile(oldFile, newFile)` of two small files, as
/// `makeDiffItem` of the upstream cases builds.
pub(super) fn small_diff_item(id: &str, name: &str) -> CodeViewItem<u8> {
    CodeViewItem::Diff(CodeViewDiffItem {
        id: id.to_owned(),
        file_diff: parse_files(
            plain_file(name, "one\ntwo\nthree\n"),
            plain_file(name, "one\ntwo changed\nthree\n"),
        ),
        annotations: None,
        version: None,
        collapsed: None,
        edit: None,
    })
}

/// `{ type: 'position', position, behavior: 'instant' }`
pub(super) fn position_target(position: f64) -> CodeViewScrollTarget {
    CodeViewScrollTarget::Position(CodeViewPositionScrollTarget {
        position,
        behavior: Some(CodeViewScrollBehavior::Instant),
    })
}

/// `{ type: 'item', id, align, behavior: 'instant' }`
pub(super) fn item_target(id: &str, align: ScrollAlign) -> CodeViewScrollTarget {
    CodeViewScrollTarget::Item(CodeViewItemScrollTarget {
        id: id.to_owned(),
        align: Some(align),
        offset: None,
        behavior: Some(CodeViewScrollBehavior::Instant),
    })
}

/// `{ type: 'line', id, lineNumber, side, align, behavior: 'instant' }`
pub(super) fn line_target(
    id: &str,
    line_number: usize,
    side: Option<SelectionSide>,
    align: ScrollAlign,
) -> CodeViewScrollTarget {
    CodeViewScrollTarget::Line(CodeViewLineScrollTarget {
        id: id.to_owned(),
        line_number,
        side,
        align: Some(align),
        offset: None,
        behavior: Some(CodeViewScrollBehavior::Instant),
    })
}

/// `{ type: 'range', id, range, align, behavior: 'instant' }`
pub(super) fn range_target(
    id: &str,
    start: usize,
    end: usize,
    side: Option<SelectionSide>,
    align: ScrollAlign,
) -> CodeViewScrollTarget {
    CodeViewScrollTarget::Range(CodeViewRangeScrollTarget {
        id: id.to_owned(),
        range: SelectedLineRange {
            start,
            side,
            end,
            end_side: None,
        },
        align: Some(align),
        offset: None,
        behavior: Some(CodeViewScrollBehavior::Instant),
    })
}

/// `shadowRoot.querySelectorAll(selector).length` for the elements of `tag`
/// (a descendant of the shadow root) that carry `attribute`, when given.
pub(super) fn shadow_count(dom: &FakeDom, element: NodeKey, tag: &str, attribute: Option<&str>) -> usize {
    let shadow = dom
        .doc
        .shadow_root(element)
        .expect("the shell has a shadow root");
    let mut count = 0;
    let mut stack = dom.doc.child_nodes(shadow);
    while let Some(node) = stack.pop() {
        if dom.doc.tag_name(node).eq_ignore_ascii_case(tag)
            && attribute.is_none_or(|attribute| dom.doc.has_attribute(node, attribute))
        {
            count += 1;
        }
        stack.extend(dom.doc.child_nodes(node));
    }
    count
}


/// `item` as a controlled update would carry it: a `collapsed` state and a
/// `version`.
pub(super) fn versioned(
    item: &CodeViewItem<u8>,
    collapsed: Option<bool>,
    version: Option<usize>,
) -> CodeViewItem<u8> {
    let CodeViewItem::Diff(mut diff) = item.clone() else {
        unreachable!("the fixtures are diff items");
    };
    diff.collapsed = collapsed;
    diff.version = version;
    CodeViewItem::Diff(diff)
}
