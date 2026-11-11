//! What the host knows about layout, and how it answers the `VirtualizerHost` reads from it.
//!
//! Upstream reads geometry from a live DOM, synchronously. Standard Dioxus delivers geometry as
//! events (`onresize`, `onscroll`) after layout, so this cache holds the last delivered values and
//! every synchronous read is answered from it. Two reads need more than a lookup:
//!
//! * A node's top. No event reports the top of an arbitrary row, and the port's scroll fix
//!   compares a line's top before and after it edits the document, which no cache of past events
//!   can show (the edit has not been laid out yet). So tops are computed from the document and the
//!   cached heights: the sum of the heights of everything stacked above the node, which is what
//!   the browser's layout does for the diff's vertical flow. The container sits at the top of the
//!   scroller's content, and only the shadow root's children and a `[data-content]` column stack
//!   (the `pre`'s `code` columns and a column's gutter and content sit side by side, so they add
//!   nothing). Buffers and placeholders take their height from the `style` the port set on them,
//!   which is the edit the scroll fix has to see before layout does.
//! * A height nothing has measured yet (a row the port rendered a moment ago). It reads as the
//!   layout's own line estimate, so reconciling heights against an unmeasured row sees no change,
//!   instead of seeing a row that shrank to nothing.

use std::collections::HashMap;

use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;

/// What `onscroll` reports for a node.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct ScrollSample {
    pub(crate) top: f64,
    pub(crate) left: f64,
    pub(crate) scroll_height: f64,
    pub(crate) client_height: f64,
}

pub(crate) struct Geometry {
    scroll: HashMap<NodeKey, ScrollSample>,
    /// `onresize` border-box block size.
    sizes: HashMap<NodeKey, f64>,
    /// The layout's estimate for one line; what an unmeasured line is taken to be.
    line_height: f64,
}

/// Whether the browser's layout stacks `parent`'s children one above the other.
fn stacks_children(document: &Document, parent: NodeKey) -> bool {
    document
        .tag_name(parent)
        .eq_ignore_ascii_case("#shadow-root")
        || document.has_attribute(parent, "data-content")
}

/// A line of code or its gutter row: what the layout estimates at `line_height`.
fn is_line(document: &Document, node: NodeKey) -> bool {
    document.has_attribute(node, "data-line") || document.has_attribute(node, "data-line-index")
}

/// The `height` the port set on a buffer or placeholder (`style="height:12px;..."`).
fn style_height(document: &Document, node: NodeKey) -> Option<f64> {
    let style = document.get_attribute(node, "style")?;
    style
        .split(';')
        .filter_map(|declaration| declaration.split_once(':'))
        .find(|(name, _)| name.trim() == "height")
        .and_then(|(_, value)| value.trim().strip_suffix("px")?.parse().ok())
}

impl Geometry {
    pub(crate) fn new(line_height: f64) -> Self {
        Self {
            scroll: HashMap::new(),
            sizes: HashMap::new(),
            line_height,
        }
    }

    pub(crate) fn scroll(&self, node: NodeKey) -> ScrollSample {
        self.scroll.get(&node).copied().unwrap_or_default()
    }

    pub(crate) fn record_scroll(&mut self, node: NodeKey, sample: ScrollSample) {
        self.scroll.insert(node, sample);
    }

    /// A scroll the host just issued; the `onscroll` it causes will confirm it.
    pub(crate) fn set_scroll_top(&mut self, node: NodeKey, top: f64) {
        self.scroll.entry(node).or_default().top = top;
    }

    /// The previous size, so a handler can tell a change from a repeat.
    pub(crate) fn record_size(&mut self, node: NodeKey, block_size: f64) -> Option<f64> {
        self.sizes.insert(node, block_size)
    }

    pub(crate) fn measured_size(&self, node: NodeKey) -> Option<f64> {
        self.sizes.get(&node).copied()
    }

    /// Forgets nodes the document no longer has.
    pub(crate) fn prune(&mut self, document: &Document) {
        self.scroll.retain(|node, _| document.exists(*node));
        self.sizes.retain(|node, _| document.exists(*node));
    }

    /// `getBoundingClientRect().height`: measured, else the height the port styled the node to,
    /// else the estimate for a line, else nothing.
    pub(crate) fn height(&self, document: &Document, node: NodeKey) -> f64 {
        if let Some(size) = self.measured_size(node) {
            return size;
        }
        if let Some(size) = style_height(document, node) {
            return size;
        }
        if is_line(document, node) {
            return self.line_height;
        }
        0.0
    }

    /// The node's distance from the top of the scroller's content; see the module docs. The
    /// container is the first thing in the content, so it is at 0.
    pub(crate) fn top_in_content(
        &self,
        document: &Document,
        container: NodeKey,
        node: NodeKey,
    ) -> f64 {
        let mut offset = 0.0;
        let mut current = node;
        while current != container {
            let Some(parent) = document.parent(current) else {
                break;
            };
            if stacks_children(document, parent) {
                offset += document
                    .child_nodes(parent)
                    .into_iter()
                    .take_while(|sibling| *sibling != current)
                    .filter(|sibling| !document.tag_name(sibling).is_empty())
                    .map(|sibling| self.height(document, sibling))
                    .sum::<f64>();
            }
            current = parent;
        }
        offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// container (shadow root: header, before-buffer, pre > code > content > two lines)
    fn diff() -> (Document, NodeKey, NodeKey, [NodeKey; 2], NodeKey) {
        let document = Document::new();
        let container = document.create_element("diffs-container");
        let shadow = document.attach_shadow(container);
        let header = document.create_element("div");
        document.set_attribute(header, "data-diffs-header", "default");
        let buffer = document.create_element("div");
        document.set_style_property(buffer, "height", "100px");
        let pre = document.create_element("pre");
        let code = document.create_element("code");
        let gutter = document.create_element("div");
        let content = document.create_element("div");
        document.set_attribute(content, "data-content", "");
        let lines = [0, 1].map(|index| {
            let line = document.create_element("div");
            document.set_attribute(line, "data-line", &(index + 1).to_string());
            document.set_attribute(line, "data-line-index", &index.to_string());
            line
        });
        for child in [header, buffer, pre] {
            document.append_child(shadow, child);
        }
        document.append_child(pre, code);
        document.append_child(code, gutter);
        document.append_child(code, content);
        for line in lines {
            document.append_child(content, line);
        }
        (document, container, header, lines, buffer)
    }

    #[test]
    fn a_top_is_the_sum_of_what_stacks_above_it() {
        let (document, container, header, lines, _) = diff();
        let mut geometry = Geometry::new(20.0);
        geometry.record_size(header, 40.0);
        geometry.record_size(lines[0], 25.0);
        assert_eq!(
            geometry.top_in_content(&document, container, container),
            0.0
        );
        // header 40 + buffer 100 (its style height); the gutter beside the content adds nothing.
        assert_eq!(
            geometry.top_in_content(&document, container, lines[0]),
            140.0
        );
        // + the first line, measured at 25.
        assert_eq!(
            geometry.top_in_content(&document, container, lines[1]),
            165.0
        );
    }

    #[test]
    fn an_unmeasured_line_reads_as_the_estimate_and_a_style_edit_moves_tops_at_once() {
        let (document, container, _, lines, buffer) = diff();
        let geometry = Geometry::new(20.0);
        assert_eq!(geometry.height(&document, lines[1]), 20.0);
        assert_eq!(
            geometry.top_in_content(&document, container, lines[1]),
            120.0
        );
        // The port resizes the buffer in the document; the next read sees it, no layout needed.
        document.set_style_property(buffer, "height", "300px");
        assert_eq!(
            geometry.top_in_content(&document, container, lines[1]),
            320.0
        );
    }
}
