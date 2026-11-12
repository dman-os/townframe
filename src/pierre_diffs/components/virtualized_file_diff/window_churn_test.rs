//! Churn (no upstream counterpart): scrolling a long diff through its window
//! thousands of times, the way the virtualizer's frames drive it (compute the
//! render range from the window, render it, reconcile the measured heights),
//! (S12b-3: through the real `render`, connected to a `Virtualizer`) must keep the layout cache (checkpoints, height deltas) and the `Document`
//! at the size of one window. Test-only, original to this crate.

use std::rc::Rc;

use super::instance::VirtualizedFileDiff;
use super::layout::VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL;
use super::layout_upstream_tests::parse_files;
use super::layout_upstream_tests::plain_file;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::virtualizer::IntersectionEntry;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::options::Overflow;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

#[test]
fn scrolling_a_long_diff_through_its_window_keeps_the_layout_cache_and_the_document_bounded() {
    let rows = 7_000;
    let numbered = |edits: &[usize]| {
        (0..rows)
            .map(|index| {
                if edits.contains(&index) {
                    format!("edited {index}\n")
                } else {
                    format!("line {index}\n")
                }
            })
            .collect::<String>()
    };
    let file_diff = parse_files(
        plain_file("long.txt", &numbered(&[])),
        plain_file("long.txt", &numbered(&[1_200, 3_100, 6_500])),
    );
    let dom = Rc::new(FakeDom::default());
    {
        let mut world = dom.world.borrow_mut();
        world.rules = true;
        world.viewport_height = 900.0;
    }
    let mut options = FileDiffOptions::<_, u8, u8>::default();
    // Wrapping makes every reconcile measure every rendered row.
    options.base.base.overflow = Some(Overflow::Wrap);
    options.base.expand_unchanged = Some(true);
    let virtualizer = Virtualizer::new(
        Rc::clone(&dom),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        VirtualizerConfig::default(),
    );
    virtualizer.setup(Root::Document, None);
    let instance = VirtualizedFileDiff::new(
        Rc::clone(&dom),
        ResizeObserverHub::new(Rc::clone(&dom)),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        Rc::new(PlainTextHighlighter),
        options,
        Rc::clone(&virtualizer),
        PartialVirtualFileMetrics::default(),
        None,
    );
    let container = dom.doc.create_element("diffs-container");
    // A render connects the instance and sets the layout up; the intersection
    // observer then reports it visible, as a browser would.
    assert!(instance.render(FileDiffRenderProps {
        file_diff: Some(Rc::clone(&file_diff)),
        file_container: Some(container),
        ..FileDiffRenderProps::default()
    }));
    virtualizer.handle_intersection_change(&[IntersectionEntry {
        target: container,
        is_intersecting: true,
    }]);
    let height = instance.get_virtualized_height();
    dom.world.borrow_mut().scroll_height = height;

    let warm_up = 100;
    let mut warm_capacity = 0;
    for round in 0..4_000usize {
        let top = ((round * 7_919) % (height as usize - 900)) as f64;
        dom.world.borrow_mut().scroll_top = top;
        virtualizer.handle_window_scroll();
        assert!(
            dom.frames.flush_frames(16, round as f64) > 0,
            "round {round}: a scroll queues a frame"
        );
        {
            let layout = instance.layout();
            assert!(
                layout.cache.checkpoints.len()
                    <= rows / VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL + 1,
                "round {round}: {} checkpoints",
                layout.cache.checkpoints.len()
            );
            assert!(
                layout.cache.height_deltas.len() <= file_diff.unified_line_count + rows,
                "round {round}: {} height deltas",
                layout.cache.height_deltas.len()
            );
        }
        if round == warm_up {
            warm_capacity = dom.doc.capacity();
        }
    }
    assert!(
        dom.doc.capacity() <= warm_capacity + warm_capacity / 10,
        "the document grew from {warm_capacity} to {} slots",
        dom.doc.capacity()
    );

    // Cleaning up frees what the instance made, including the nodes a render
    // retired, and leaves nothing for the virtualizer to drive.
    instance.clean_up(false);
    assert!(instance.file_diff().state().retired.is_empty());
    // `disconnect` queues a pass; it has no instance left to render.
    dom.frames.flush_frames(16, 4_001.0);
    assert!(dom.frames.frame_ids().is_empty());
    assert!(dom.doc.capacity() <= warm_capacity + warm_capacity / 10);
}
