// Part of the S11b port of InteractionManager.ts (see ../interaction_manager.rs
// for the header): path and target resolution: the event path as a model walk, `closest` over it, annotation slots, hit-testing, `resolvePointerTarget` and the selection points derived from it.
// A view over the manager: it derefs to `InteractionManager`, so the methods
// read the manager's state as they do upstream, and exists to keep each type's
// method count inside the guardrails budget.

use std::ops::Deref;

use super::*;

pub(super) struct Targets<'manager, Host: InteractionHost>(
    pub(super) &'manager InteractionManager<Host>,
);

impl<'manager, Host: InteractionHost> Deref for Targets<'manager, Host> {
    type Target = InteractionManager<Host>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<Host: InteractionHost> Targets<'_, Host> {
    pub(super) fn get_element_path(&self, element: &NodeKey) -> Vec<NodeKey> {
        let mut path = Vec::new();
        let mut current = Some(*element);
        while let Some(node) = current {
            current = self.host.document().parent(node);
            path.push(node);
        }
        path
    }

    /// `event.composedPath()`: the target and its ancestors in the model.
    pub(super) fn composed_path(&self, event: &PointerEventData<NodeKey>) -> Vec<NodeKey> {
        event
            .target
            .as_ref()
            .map(|target| self.get_element_path(target))
            .unwrap_or_default()
    }

    pub(super) fn is_gutter_utility_path(&self, path: &[NodeKey]) -> bool {
        is_gutter_utility_path(path, |element, name| {
            self.host.document().get_attribute(element, name)
        })
    }

    pub(super) fn has_attribute(&self, element: &NodeKey, name: &str) -> bool {
        self.host.document().get_attribute(element, name).is_some()
    }

    // `element.closest(...)` over the model: the element itself, then ancestors.
    pub(super) fn closest(
        &self,
        element: &NodeKey,
        matches: impl Fn(&NodeKey) -> bool,
    ) -> Option<NodeKey> {
        let mut current = Some(*element);
        while let Some(node) = current {
            if matches(&node) {
                return Some(node);
            }
            current = self.host.document().parent(node);
        }
        None
    }

    // Pointer hits often land on text/token descendants. This normalizes those
    // hits back to the row elements InteractionManager knows how to resolve.
    pub(super) fn closest_selectable_row(&self, element: &NodeKey) -> Option<NodeKey> {
        if let Some(row) = self.closest(element, |node| {
            self.has_attribute(node, "data-line") || self.has_attribute(node, "data-column-number")
        }) {
            return Some(row);
        }

        let annotation_row = self.closest(element, |node| {
            self.has_attribute(node, "data-line-annotation")
                || self
                    .host
                    .document()
                    .get_attribute(node, "data-gutter-buffer")
                    .as_deref()
                    == Some("annotation")
        })?;

        let previous_row = self
            .host
            .document()
            .previous_element_sibling(annotation_row)?;
        (self.has_attribute(&previous_row, "data-line")
            || self.has_attribute(&previous_row, "data-column-number"))
        .then_some(previous_row)
    }

    // Resolve the shadow annotation row an event target projects into, from
    // either slotted light-DOM content or the slot element itself.
    pub(super) fn annotation_row_from_slot(&self, element: &NodeKey) -> Option<NodeKey> {
        let slotted = self.closest(element, |node| {
            self.host
                .document()
                .get_attribute(node, "slot")
                .is_some_and(|slot| slot.starts_with("annotation-"))
        });
        let slot = match slotted.and_then(|slotted| self.host.document().assigned_slot(slotted)) {
            Some(slot) => Some(slot),
            None if self.host.document().tag_name(element) == "SLOT" => Some(*element),
            None => None,
        };
        let slot = slot?;
        self.closest(&slot, |node| {
            self.has_attribute(node, "data-line-annotation")
        })
    }

    pub(super) fn get_annotation_slot_name(&self, element: &NodeKey) -> Option<String> {
        let slotted_element = self.closest(element, |node| {
            self.host
                .document()
                .get_attribute(node, "slot")
                .is_some_and(|slot| slot.starts_with("annotation-"))
        });
        if let Some(slotted_element) = slotted_element {
            return self.host.document().get_attribute(slotted_element, "slot");
        }

        let slot_name = self.host.document().get_attribute(element, "name")?;
        slot_name.starts_with("annotation-").then_some(slot_name)
    }

    pub(super) fn path_from_annotation_slot(&self, element: &NodeKey) -> Option<Vec<NodeKey>> {
        // Don't trust the slot name for the line number: during an edit session
        // an annotation keeps its original name while edits move it to other
        // lines, so a name like "annotation-20" can point at a line the
        // annotation left long ago. The code row directly above the annotation
        // row always reflects where it sits now, so read the line from there and
        // parse the name only as a fallback.
        let annotated_line = self
            .annotation_row_from_slot(element)
            .and_then(|row| self.host.document().previous_element_sibling(row));
        if let Some(annotated_line) = &annotated_line
            && (self.has_attribute(annotated_line, "data-line")
                || self.has_attribute(annotated_line, "data-column-number"))
        {
            return Some(self.get_element_path(annotated_line));
        }
        let point = selection_point_from_annotation_slot_name(
            self.get_annotation_slot_name(element).as_deref(),
        )?;
        let target = self.target_for_selection_point(&point)?;
        Some(self.get_element_path(&target.line_element))
    }

    pub(super) fn path_from_element(
        &self,
        element: &NodeKey,
        exclude_utility: bool,
    ) -> Option<Vec<NodeKey>> {
        let path = self.get_element_path(element);
        if exclude_utility && self.is_gutter_utility_path(&path) {
            return None;
        }
        // Selection is row-based, not child-element-based. Lateral movement over
        // tokens, annotations, or line-number text should still update to the
        // nearest content or gutter row; anything else is ignored by returning
        // undefined.
        if let Some(row) = self.closest_selectable_row(element) {
            return Some(self.get_element_path(&row));
        }
        self.path_from_annotation_slot(element)
    }

    pub(super) fn path_from_event_path(
        &self,
        path: &[NodeKey],
        exclude_utility: bool,
    ) -> Option<Vec<NodeKey>> {
        if exclude_utility && self.is_gutter_utility_path(path) {
            return None;
        }
        // Every path entry is an element in the model, so the first one is it.
        let element = path.first()?;
        self.path_from_element(element, exclude_utility)
    }

    pub(super) fn path_from_coordinates(
        &self,
        event: &PointerEventData<NodeKey>,
        exclude_utility: bool,
    ) -> CoordinatePath<NodeKey> {
        match self.hit_test(event) {
            HitTest::Unavailable => CoordinatePath::Unavailable,
            HitTest::NoElement => CoordinatePath::NoElement,
            // A coordinate hit can be a token span, line-number span, utility overlay,
            // or separator. Normalize valid descendants back to their row; invalid
            // hits intentionally return null so the last selected row stays sticky.
            HitTest::Element(element) => self
                .path_from_element(&element, exclude_utility)
                .map_or(CoordinatePath::NoElement, CoordinatePath::Path),
        }
    }

    pub(super) fn hit_test(&self, event: &PointerEventData<NodeKey>) -> HitTest<NodeKey> {
        // Return Unavailable when coordinate hit-testing cannot run at all, which
        // allows callers to fall back to the event path for tests and older DOMs.
        // Return NoElement when hit-testing ran but found no element, so active drags
        // keep their previous endpoint instead of using a stale captured target.
        if !event.client_x.is_finite() || !event.client_y.is_finite() {
            return HitTest::Unavailable;
        }
        self.host.element_from_point(event.client_x, event.client_y)
    }

    pub(super) fn resolve_selection_path(
        &self,
        event: &PointerEventData<NodeKey>,
        options: SelectionHitOptions,
    ) -> Option<Vec<NodeKey>> {
        let exclude_utility = options.exclude_utility != Some(false);
        match options.source {
            SelectionHitSource::EventPath => {
                self.path_from_event_path(&self.composed_path(event), exclude_utility)
            }
            SelectionHitSource::CoordinatesFirst => {
                // Touch pointers can keep dispatching move/up events to the element
                // where the drag began. Prefer the row currently under the finger,
                // then fall back only when coordinate hit-testing is unavailable.
                match self.path_from_coordinates(event, exclude_utility) {
                    CoordinatePath::Unavailable => {
                        self.path_from_event_path(&self.composed_path(event), exclude_utility)
                    }
                    CoordinatePath::NoElement => None,
                    CoordinatePath::Path(path) => Some(path),
                }
            }
        }
    }

    pub(super) fn resolve_selection_point(
        &self,
        event: &PointerEventData<NodeKey>,
        options: SelectionHitOptions,
    ) -> Option<SelectionPoint> {
        let path = self.resolve_selection_path(event, options)?;
        self.selection_point_from_path(&path)
    }

    pub(super) fn resolve_selection_info(
        &self,
        event: &PointerEventData<NodeKey>,
        options: SelectionHitOptions,
        require_number_column: bool,
    ) -> Option<SelectionInfo> {
        let path = self.resolve_selection_path(event, options)?;
        self.selection_info_from_path(&path, require_number_column)
    }

    pub(super) fn selection_point_from_path(&self, path: &[NodeKey]) -> Option<SelectionPoint> {
        let Some(ResolvedPointerTarget::Line(target)) = self.resolve_pointer_target(path) else {
            return None;
        };
        Some(SelectionPoint {
            line_number: usize::try_from(target.line_number).ok()?,
            side: if self.mode == InteractionManagerMode::Diff {
                target.side
            } else {
                None
            },
        })
    }

    pub(super) fn selection_info_from_path(
        &self,
        path: &[NodeKey],
        require_number_column: bool,
    ) -> Option<SelectionInfo> {
        let Some(ResolvedPointerTarget::Line(target)) = self.resolve_pointer_target(path) else {
            return None;
        };
        if require_number_column && !target.number_column {
            return None;
        }
        let line_index = target.split_line_index?;
        Some(SelectionInfo {
            line_index,
            line_number: usize::try_from(target.line_number).ok()?,
            event_side: if self.mode == InteractionManagerMode::Diff {
                target.side
            } else {
                None
            },
        })
    }

    pub(super) fn resolve_pointer_target(
        &self,
        path: &[NodeKey],
    ) -> Option<ResolvedPointerTarget<NodeKey>> {
        let host = self.host.document();
        let mut number_column = false;
        let mut line_type: Option<LineTypes> = None;
        let mut code_element: Option<NodeKey> = None;
        let mut line_element: Option<NodeKey> = None;
        let mut line_index_value: Option<String> = None;
        let mut number_element: Option<NodeKey> = None;
        let mut token_element: Option<NodeKey> = None;
        let mut expand_info: Option<(Option<i64>, ExpansionDirections, bool)> = None;
        let mut line_number: Option<i64> = None;

        for element in path {
            // Token interactions are dropped, but a `data-char` node is skipped
            // for every other check once its start parses (the `continue`).
            if token_element.is_none()
                && let Some(start_attr) = host.get_attribute(element, "data-char")
            {
                token_element = Some(*element);
                if parse_int(&start_attr).is_some() {
                    continue;
                }
            }

            let column_number = if number_element.is_none() {
                host.get_attribute(element, "data-column-number")
            } else {
                None
            };
            if let Some(column_number) = column_number {
                number_element = Some(*element);
                line_number = parse_int(&column_number);
                number_column = true;
                line_type = self.get_line_type_from_element(element);
                line_index_value = host.get_attribute(element, "data-line-index");
                continue;
            }

            let line_attr = if line_element.is_none() {
                host.get_attribute(element, "data-line")
            } else {
                None
            };
            if let Some(line_attr) = line_attr {
                line_element = Some(*element);
                line_number = parse_int(&line_attr);
                line_type = self.get_line_type_from_element(element);
                line_index_value = host.get_attribute(element, "data-line-index");
                continue;
            }

            if expand_info.is_none()
                && (self.has_attribute(element, "data-expand-button")
                    || self.has_attribute(element, "data-unmodified-lines"))
            {
                let direction = if self.has_attribute(element, "data-expand-up") {
                    ExpansionDirections::Up
                } else if self.has_attribute(element, "data-expand-down") {
                    ExpansionDirections::Down
                } else {
                    ExpansionDirections::Both
                };
                expand_info = Some((
                    None,
                    direction,
                    self.has_attribute(element, "data-expand-all-button"),
                ));
                continue;
            }

            if let Some(info) = &mut expand_info
                && let Some(expand_index_value) = host.get_attribute(element, "data-expand-index")
            {
                if let Some(expand_index) = parse_int(&expand_index_value) {
                    info.0 = Some(expand_index);
                }
                continue;
            }

            if code_element.is_none() && self.has_attribute(element, "data-code") {
                code_element = Some(*element);
                break;
            }
        }

        if let Some((Some(hunk_index), direction, all)) = expand_info {
            return Some(ResolvedPointerTarget::Expando {
                hunk_index,
                direction,
                all,
            });
        }

        if line_element.is_none()
            && let (Some(code), Some(value)) = (&code_element, &line_index_value)
        {
            line_element = host
                .query_by_attribute(code, "data-line-index", Some(value))
                .into_iter()
                .find(|element| self.has_attribute(element, "data-line"));
        }
        if number_element.is_none()
            && let (Some(code), Some(value)) = (&code_element, &line_index_value)
        {
            number_element = host
                .query_by_attribute(code, "data-line-index", Some(value))
                .into_iter()
                .find(|element| self.has_attribute(element, "data-column-number"));
        }

        let (
            Some(code_element),
            Some(line_element),
            Some(number_element),
            Some(line_type),
            Some(line_number),
        ) = (
            code_element,
            line_element,
            number_element,
            line_type,
            line_number,
        )
        else {
            return None;
        };

        let split_line_index = self.parse_line_index(&line_element, self.is_split_diff());
        let side = if self.mode == InteractionManagerMode::File {
            None
        } else {
            Some(self.get_annotation_side(line_type, &code_element))
        };
        Some(ResolvedPointerTarget::Line(TargetFields {
            line_type,
            line_element,
            line_number,
            number_column,
            number_element,
            side,
            split_line_index,
        }))
    }

    pub(super) fn get_annotation_side(
        &self,
        line_type: LineTypes,
        code_element: &NodeKey,
    ) -> AnnotationSide {
        match line_type {
            LineTypes::ChangeDeletion => AnnotationSide::Deletions,
            LineTypes::ChangeAddition => AnnotationSide::Additions,
            _ => {
                if self.has_attribute(code_element, "data-deletions") {
                    AnnotationSide::Deletions
                } else {
                    AnnotationSide::Additions
                }
            }
        }
    }

    pub(super) fn get_line_type_from_element(&self, element: &NodeKey) -> Option<LineTypes> {
        match self
            .host
            .document()
            .get_attribute(element, "data-line-type")?
            .as_str()
        {
            "change-deletion" => Some(LineTypes::ChangeDeletion),
            "change-addition" => Some(LineTypes::ChangeAddition),
            "context" => Some(LineTypes::Context),
            "context-expanded" => Some(LineTypes::ContextExpanded),
            _ => None,
        }
    }

    pub(super) fn is_split_diff(&self) -> bool {
        self.pre
            .borrow()
            .as_ref()
            .and_then(|pre| self.host.document().get_attribute(pre, "data-diff-type"))
            .as_deref()
            == Some("split")
    }

    pub(super) fn parse_line_index(&self, element: &NodeKey, split: bool) -> Option<i64> {
        let attribute = self
            .host
            .document()
            .get_attribute(element, "data-line-index")
            .unwrap_or_default();
        let line_indexes: Vec<i64> = attribute.split(',').filter_map(parse_int).collect();

        if split && line_indexes.len() == 2 {
            return Some(line_indexes[1]);
        }
        if !split {
            return line_indexes.first().copied();
        }
        None
    }
    pub(super) fn get_line_index(
        &self,
        line_number: usize,
        side: Option<SelectionSide>,
    ) -> Option<(i64, i64)> {
        match &self.options().base.get_line_index {
            Some(get_line_index) => get_line_index(line_number, side),
            None => {
                let index = i64::try_from(line_number).expect("line numbers fit in i64") - 1;
                Some((index, index))
            }
        }
    }

    pub(super) fn target_for_selection_point(
        &self,
        point: &SelectionPoint,
    ) -> Option<TargetFields<NodeKey>> {
        let pre = (*self.pre.borrow())?;
        let indexes = self.get_line_index(point.line_number, point.side)?;
        let line_index = if self.mode == InteractionManagerMode::Diff {
            format!("{},{}", indexes.0, indexes.1)
        } else {
            format!("{}", indexes.0)
        };
        // Rebuild the real gutter-row target from the selection point so the
        // utility can anchor to the visible row even when the point came from
        // controlled state rather than a pointer event.
        let line_number = point.line_number.to_string();
        let candidates =
            self.host
                .document()
                .query_by_attribute(pre, "data-column-number", Some(&line_number));
        for element in candidates {
            if self
                .host
                .document()
                .get_attribute(element, "data-line-index")
                .as_deref()
                != Some(&line_index)
            {
                continue;
            }
            let Some(ResolvedPointerTarget::Line(target)) =
                self.resolve_pointer_target(&self.get_element_path(&element))
            else {
                continue;
            };
            if self.mode == InteractionManagerMode::Diff
                && point.side.is_some()
                && target.side != point.side
            {
                continue;
            }
            return Some(target);
        }
        None
    }

    pub(super) fn to_event_base_props(
        &self,
        target: &TargetFields<NodeKey>,
    ) -> LineEventBaseProps<NodeKey> {
        if self.mode == InteractionManagerMode::File {
            return LineEventBaseProps {
                kind: LineEventKind::Line,
                line_element: target.line_element,
                line_number: target.line_number,
                number_column: target.number_column,
                number_element: target.number_element,
                annotation_side: None,
                line_type: None,
            };
        }

        LineEventBaseProps {
            kind: LineEventKind::DiffLine,
            annotation_side: target.side,
            line_type: Some(target.line_type),
            line_element: target.line_element,
            number_element: target.number_element,
            line_number: target.line_number,
            number_column: target.number_column,
        }
    }
}
