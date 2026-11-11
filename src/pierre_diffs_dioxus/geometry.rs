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
//! * A row under a client coordinate (`element_at`). Nothing reports where a row is in the
//!   client's coordinates, only how tall it is. A `HitFrame` anchors the stacked heights: when a
//!   drag starts the host asks the engine for the client rects of the `pre` (for its left edge
//!   and width) and of one content column (for the top its first row starts at, which includes
//!   the code block's own padding no stylesheet read could see), and every later lookup moves the
//!   anchor by how far the scroller and the stack above the column have moved since.
//!
//! Heights are border boxes (`onresize`), and a row's CSS margin is outside one. The stylesheet
//! gives a hunk separator a block margin above and below it (`--diffs-gap-block`, 8px by default,
//! zero on the first hunk's top and the last one's bottom), which no event reports and no
//! stylesheet read could see, so rows stacked by heights alone sit higher than they are by that
//! gap for every separator above them and a drag would resolve rows below the pointer. The frame
//! therefore also carries the margin above each separator and above the row after one, read
//! from their client rects at drag start (`margin_anchors`, `Geometry::margins`): rows that are
//! stacked without margins do not need any.

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

/// What the engine measured when a drag started; see the module docs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HitFrame {
    /// The `pre`'s client left edge and width.
    pub(crate) pre_left: f64,
    pub(crate) pre_width: f64,
    /// The content column whose top was measured, and that top in client coordinates.
    pub(crate) content: NodeKey,
    pub(crate) content_client_top: f64,
    /// The column's top in the scroller's content, and the scroll position, at the measurement.
    pub(crate) content_offset: f64,
    pub(crate) scroll_top: f64,
    /// The CSS margin above a row of a content column, for the rows that have one
    /// (`Geometry::margins`); a row not listed has none.
    pub(crate) margins: HashMap<NodeKey, f64>,
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

/// The rows whose margin the frame measures: the hunk separators of a content column and the row
/// after each (whose top shows the margin below the separator). Nothing else in a column has a
/// margin.
pub(crate) fn is_margin_anchor(document: &Document, node: NodeKey) -> bool {
    let in_content = document
        .parent(node)
        .is_some_and(|parent| document.has_attribute(parent, "data-content"));
    in_content
        && (document.has_attribute(node, "data-separator")
            || document
                .previous_element_sibling(node)
                .is_some_and(|previous| document.has_attribute(previous, "data-separator")))
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

    /// `elementFromPoint` over the rows of the `pre`: the child of a content column (a line, an
    /// annotation, a buffer) whose client box holds `(x, y)`, or `None` when the point is outside
    /// the `pre` or between rows. A split diff's two code columns share the width equally (the
    /// stylesheet's `1fr 1fr`); a unified one has the whole width. The gutter is not hit-tested
    /// apart from its row: the row's content element answers for it, which the manager
    /// normalizes back to the row anyway.
    pub(crate) fn element_at(
        &self,
        document: &Document,
        container: NodeKey,
        pre: NodeKey,
        frame: &HitFrame,
        scroll_top: f64,
        (x, y): (f64, f64),
    ) -> Option<NodeKey> {
        let local_x = x - frame.pre_left;
        if !(0.0..frame.pre_width).contains(&local_x) {
            return None;
        }
        let is_split = document.get_attribute(pre, "data-diff-type").as_deref() == Some("split");
        let column_width = frame.pre_width / if is_split { 2.0 } else { 1.0 };
        // The column whose span holds `local_x`, in document order.
        let content = document
            .child_nodes(pre)
            .into_iter()
            .filter(|child| document.has_attribute(child, "data-code"))
            .zip((1..).map(|index| f64::from(index) * column_width))
            .find(|(_, right_edge)| local_x < *right_edge)
            .and_then(|(column, _)| {
                document
                    .child_nodes(column)
                    .into_iter()
                    .find(|child| document.has_attribute(child, "data-content"))
            })?;
        let moved = (self.top_in_content(document, container, frame.content)
            - frame.content_offset)
            - (scroll_top - frame.scroll_top);
        let mut top = frame.content_client_top + moved;
        for row in document
            .child_nodes(content)
            .into_iter()
            .filter(|row| !document.tag_name(row).is_empty())
        {
            top += frame.margins.get(&row).copied().unwrap_or(0.0);
            let bottom = top + self.height(document, row);
            if (top..bottom).contains(&y) {
                return Some(row);
            }
            top = bottom;
        }
        None
    }

    /// The margin above each anchor row of each content column of `pre`, given the client tops
    /// the platform reported for the anchors (`is_margin_anchor`) and the client top of the
    /// columns' first row. Each column is stacked by heights, and at an anchor the margin is how
    /// far its reported top is below where the stack has reached.
    pub(crate) fn margins(
        &self,
        document: &Document,
        pre: NodeKey,
        content_client_top: f64,
        anchor_tops: &HashMap<NodeKey, f64>,
    ) -> HashMap<NodeKey, f64> {
        let mut margins = HashMap::new();
        for column in document.child_nodes(pre).into_iter().filter_map(|code| {
            document
                .child_nodes(code)
                .into_iter()
                .find(|child| document.has_attribute(child, "data-content"))
        }) {
            let mut top = content_client_top;
            for row in document
                .child_nodes(column)
                .into_iter()
                .filter(|row| !document.tag_name(row).is_empty())
            {
                if let Some(anchor_top) = anchor_tops.get(&row) {
                    margins.insert(row, anchor_top - top);
                    top = *anchor_top;
                }
                top += self.height(document, row);
            }
        }
        margins
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

#[cfg(test)]
mod hit_tests {
    use super::*;

    /// container (shadow root: pre[split] > two code columns, each gutter + content of 3 lines)
    struct Split {
        document: Document,
        container: NodeKey,
        pre: NodeKey,
        content: [NodeKey; 2],
        rows: [[NodeKey; 3]; 2],
    }

    fn split() -> Split {
        let document = Document::new();
        let container = document.create_element("diffs-container");
        let shadow = document.attach_shadow(container);
        let pre = document.create_element("pre");
        document.set_attribute(pre, "data-diff-type", "split");
        document.append_child(shadow, pre);
        let columns = ["data-deletions", "data-additions"].map(|side| {
            let code = document.create_element("code");
            document.set_attribute(code, "data-code", "");
            document.set_attribute(code, side, "");
            document.append_child(pre, code);
            let gutter = document.create_element("div");
            document.set_attribute(gutter, "data-gutter", "");
            let content = document.create_element("div");
            document.set_attribute(content, "data-content", "");
            document.append_child(code, gutter);
            document.append_child(code, content);
            let rows = [0, 1, 2].map(|index| {
                let row = document.create_element("div");
                document.set_attribute(row, "data-line", &(index + 1).to_string());
                document.append_child(content, row);
                row
            });
            (content, rows)
        });
        Split {
            document,
            container,
            pre,
            content: [columns[0].0, columns[1].0],
            rows: [columns[0].1, columns[1].1],
        }
    }

    fn frame(split: &Split) -> HitFrame {
        // The pre is 800 wide at client x 100, and its first row starts at client y 50.
        HitFrame {
            pre_left: 100.0,
            pre_width: 800.0,
            content: split.content[0],
            content_client_top: 50.0,
            content_offset: 0.0,
            scroll_top: 0.0,
            margins: HashMap::new(),
        }
    }

    #[test]
    fn a_point_is_answered_with_the_row_of_the_column_it_is_in() {
        let split = split();
        let geometry = Geometry::new(20.0);
        let frame = frame(&split);
        let at = |x, y, scroll| {
            geometry.element_at(
                &split.document,
                split.container,
                split.pre,
                &frame,
                scroll,
                (x, y),
            )
        };
        // Left half is the deletions column, right half the additions; rows are 20 high.
        assert_eq!(at(150.0, 55.0, 0.0), Some(split.rows[0][0]));
        assert_eq!(at(150.0, 95.0, 0.0), Some(split.rows[0][2]));
        assert_eq!(at(650.0, 75.0, 0.0), Some(split.rows[1][1]));
        // Above the first row, below the last, left or right of the pre: nothing.
        assert_eq!(at(150.0, 49.0, 0.0), None);
        assert_eq!(at(150.0, 110.0, 0.0), None);
        assert_eq!(at(99.0, 60.0, 0.0), None);
        assert_eq!(at(900.0, 60.0, 0.0), None);
        // The scroller moved 40 down since the measurement: the rows moved up with it.
        assert_eq!(at(150.0, 35.0, 40.0), Some(split.rows[0][1]));
    }

    #[test]
    fn measured_row_heights_move_the_rows_below_them() {
        let split = split();
        let mut geometry = Geometry::new(20.0);
        geometry.record_size(split.rows[0][0], 45.0);
        let frame = frame(&split);
        let at = |y| {
            geometry.element_at(
                &split.document,
                split.container,
                split.pre,
                &frame,
                0.0,
                (150.0, y),
            )
        };
        // A wrapped first row takes 45, so the second row starts at 95.
        assert_eq!(at(90.0), Some(split.rows[0][0]));
        assert_eq!(at(100.0), Some(split.rows[0][1]));
    }

    #[test]
    fn a_unified_diff_gives_its_one_column_the_whole_width() {
        let split = split();
        split.document.remove_attribute(split.pre, "data-diff-type");
        let geometry = Geometry::new(20.0);
        let frame = frame(&split);
        let hit = geometry.element_at(
            &split.document,
            split.container,
            split.pre,
            &frame,
            0.0,
            (850.0, 55.0),
        );
        // x 850 is in the second half of a split, but a unified pre has only the first column,
        // which spans it.
        assert_eq!(hit, Some(split.rows[0][0]));
    }
}
