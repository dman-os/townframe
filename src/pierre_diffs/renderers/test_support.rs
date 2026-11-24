//! Shared construction for the renderer tests: a renderer over the
//! plain-text stub highlighter. Test-only, original to this crate.

use crate::constants::DEFAULT_RENDER_RANGE;
use crate::hast::ElementContent;
use crate::highlighter_types::PlainTextHighlighter;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererOptions;
use crate::renderers::diff_hunks_renderer::HunksRenderResult;
use crate::types::layout::RenderRange;
use crate::types::options::BaseDiffOptions;
use crate::types::options::DiffStyle;
use crate::types::patch::FileDiffMetadata;

/// `new DiffHunksRenderer(options)` with `configure` applied to the default
/// options (split style, line-info separators, default theme).
pub fn renderer_with<LAnnotation>(
    configure: impl FnOnce(&mut BaseDiffOptions),
) -> DiffHunksRenderer<LAnnotation> {
    let mut options = DiffHunksRendererOptions::default();
    configure(&mut options.base);
    DiffHunksRenderer::new(options)
}

pub fn split_renderer<LAnnotation>() -> DiffHunksRenderer<LAnnotation> {
    renderer_with(|options| options.diff_style = Some(DiffStyle::Split))
}

pub fn unified_renderer<LAnnotation>() -> DiffHunksRenderer<LAnnotation> {
    renderer_with(|options| options.diff_style = Some(DiffStyle::Unified))
}

/// `await renderer.asyncRender(diff)`.
pub fn render<LAnnotation>(
    renderer: &DiffHunksRenderer<LAnnotation>,
    diff: &FileDiffMetadata,
) -> HunksRenderResult {
    renderer.render_diff(&PlainTextHighlighter, diff, &DEFAULT_RENDER_RANGE)
}

/// `await renderer.asyncRender(diff, range)`.
pub fn render_in<LAnnotation>(
    renderer: &DiffHunksRenderer<LAnnotation>,
    diff: &FileDiffMetadata,
    range: &RenderRange,
) -> HunksRenderResult {
    renderer.render_diff(&PlainTextHighlighter, diff, range)
}

/// A finite window: `{ startingLine, totalLines, bufferBefore: 0, bufferAfter: 0 }`.
pub fn window(starting_line: usize, total_lines: usize) -> RenderRange {
    RenderRange {
        starting_line: starting_line as f64,
        total_lines: total_lines as f64,
        buffer_before: 0.0,
        buffer_after: 0.0,
    }
}

pub fn texts(nodes: &[ElementContent]) -> String {
    nodes.iter().map(ElementContent::text_content).collect()
}
