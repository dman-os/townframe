// Part of the S11b port of InteractionManager.ts (see ../interaction_manager.rs
// for the header): the row-range arithmetic and the per-column attribute writes of `renderSelection`.
// A view over the manager: it derefs to `InteractionManager`, so the methods
// read the manager's state as they do upstream, and exists to keep each type's
// method count inside the guardrails budget.

use std::ops::Deref;

use super::*;

pub(super) struct SelectionRows<'manager, Host: InteractionHost>(
    pub(super) &'manager InteractionManager<Host>,
);

impl<'manager, Host: InteractionHost> Deref for SelectionRows<'manager, Host> {
    type Target = InteractionManager<Host>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<Host: InteractionHost> SelectionRows<'_, Host> {
    pub(super) fn get_indexes_from_selection(
        &self,
        selected_range: &SelectedLineRange,
        split: bool,
    ) -> Option<(i64, i64)> {
        self.pre.borrow().as_ref()?;
        let start_indexes =
            Targets(self.0).get_line_index(selected_range.start, selected_range.side)?;
        let final_indexes = Targets(self.0).get_line_index(
            selected_range.end,
            selected_range.end_side.or(selected_range.side),
        )?;
        Some(if split {
            (start_indexes.1, final_indexes.1)
        } else {
            (start_indexes.0, final_indexes.0)
        })
    }

    // Resolve a desired line range before touching the last successful DOM
    // state, so a transient line-index mismatch leaves that rendering intact and
    // dirty.
    pub(super) fn resolve_line_render_range(
        &self,
        state: &LineRenderState,
        split: bool,
    ) -> Option<(i64, i64)> {
        let range = state.range.as_ref()?;
        let row_range = self.get_indexes_from_selection(range, split);
        assert!(
            row_range.is_some(),
            "InteractionManager.renderSelection: No valid selected-lines rowRange"
        );
        row_range
    }

    // Validate every code column that will receive changed attributes before
    // clearing either channel's previous rendering.
    pub(super) fn get_line_render_columns(
        &self,
        selected_lines: &LineRenderState,
        selected_row_range: Option<(i64, i64)>,
    ) -> Vec<LineRenderColumn<Host::Element>> {
        let Some(pre) = self.pre.borrow().clone() else {
            return Vec::new();
        };
        if selected_row_range.is_none() {
            return Vec::new();
        }

        let code_elements = self.host.children(&pre);
        assert!(
            code_elements.len() <= 2,
            "InteractionManager.renderSelection: Somehow there are more than 2 code elements..."
        );

        let mut columns = Vec::new();
        for code in code_elements {
            let side = if Targets(self.0).has_attribute(&code, "data-deletions") {
                Some(SelectionSide::Deletions)
            } else if Targets(self.0).has_attribute(&code, "data-additions") {
                Some(SelectionSide::Additions)
            } else {
                None
            };
            let render_selected_lines = selected_lines.highlight_side.is_none()
                || side.is_none()
                || selected_lines.highlight_side == side;
            if !render_selected_lines {
                continue;
            }

            let code_children = self.host.children(&code);
            let (Some(gutter), Some(content)) = (code_children.first(), code_children.get(1))
            else {
                panic!("InteractionManager.renderSelection: missing gutter or content element");
            };
            assert!(
                self.host.children(content).len() == self.host.children(gutter).len(),
                "InteractionManager.renderSelection: gutter and content children dont match, something is wrong"
            );
            columns.push(LineRenderColumn {
                content: content.clone(),
                gutter: gutter.clone(),
            });
        }
        columns
    }

    pub(super) fn render_selected_column(
        &self,
        column: &LineRenderColumn<Host::Element>,
        selected_first: i64,
        selected_last: i64,
        selected_lines: &LineRenderState,
        split: bool,
    ) {
        let selected_is_single = selected_first == selected_last;
        let content_children = self.host.children(&column.content);
        let gutter_children = self.host.children(&column.gutter);
        for (content_element, gutter_element) in content_children.iter().zip(&gutter_children) {
            let line_index = Targets(self.0).parse_line_index(content_element, split);
            if line_index.unwrap_or(0) > selected_last {
                break;
            }
            let Some(line_index) = line_index else {
                continue;
            };
            if line_index < selected_first || line_index > selected_last {
                continue;
            }
            let mut attribute_value = if selected_is_single {
                "single"
            } else if line_index == selected_first {
                "first"
            } else if line_index == selected_last {
                "last"
            } else {
                ""
            };
            self.host
                .set_attribute(gutter_element, "data-selected-line", attribute_value);
            // A number-only highlight marks just the gutter number, leaving the
            // line background and any annotation rows untouched.
            if selected_lines.line_number_only {
                continue;
            }
            self.host
                .set_attribute(content_element, "data-selected-line", attribute_value);
            let gutter_next = self.host.next_sibling_element(gutter_element);
            let content_next = self.host.next_sibling_element(content_element);
            if let (Some(gutter_next), Some(content_next)) = (gutter_next, content_next)
                && Targets(self.0).has_attribute(&content_next, "data-line-annotation")
            {
                if selected_is_single {
                    attribute_value = "last";
                    self.host
                        .set_attribute(content_element, "data-selected-line", "first");
                } else if line_index == selected_first {
                    attribute_value = "";
                } else if line_index == selected_last {
                    self.host
                        .set_attribute(content_element, "data-selected-line", "");
                }
                self.host
                    .set_attribute(&content_next, "data-selected-line", attribute_value);
                self.host
                    .set_attribute(&gutter_next, "data-selected-line", attribute_value);
            }
        }
    }
}
