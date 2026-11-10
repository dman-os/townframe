// Part of the S11b port of InteractionManager.ts (see ../interaction_manager.rs
// for the header): the gutter utility node and where it is placed (selection first, then hover).
// A view over the manager: it derefs to `InteractionManager`, so the methods
// read the manager's state as they do upstream, and exists to keep each type's
// method count inside the guardrails budget.

use std::ops::Deref;

use super::*;

pub(super) struct GutterUtility<'manager, Host: InteractionHost>(
    pub(super) &'manager InteractionManager<Host>,
);

impl<'manager, Host: InteractionHost> Deref for GutterUtility<'manager, Host> {
    type Target = InteractionManager<Host>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<Host: InteractionHost> GutterUtility<'_, Host> {
    pub(super) fn ensure_gutter_utility_node(&self, use_custom_gutter_utility: bool) {
        if self.gutter_utility_container.borrow().is_none() {
            let container = self.host.create_element("div");
            self.host
                .set_attribute(&container, "data-gutter-utility-slot", "");
            *self.gutter_utility_container.borrow_mut() = Some(container);
        }
        let container = self
            .gutter_utility_container
            .borrow()
            .clone()
            .expect("the container was just ensured");
        if use_custom_gutter_utility {
            if let Some(button) = self.gutter_utility_button.borrow_mut().take() {
                self.host.remove_node(&button);
            }
            if self.gutter_utility_slot.borrow().is_none() {
                let slot = self.host.create_element("slot");
                self.host
                    .set_attribute(&slot, "name", "gutter-utility-slot");
                *self.gutter_utility_slot.borrow_mut() = Some(slot);
            }
            let slot = self
                .gutter_utility_slot
                .borrow()
                .clone()
                .expect("the slot was just ensured");
            if self.host.parent(&slot).as_ref() != Some(&container) {
                self.host.replace_children(&container, &[slot]);
            }
        } else {
            if let Some(slot) = self.gutter_utility_slot.borrow_mut().take() {
                self.host.remove_node(&slot);
            }
            if self.gutter_utility_button.borrow().is_none() {
                let button = self
                    .host
                    .create_element_from_hast(&create_gutter_utility_element());
                *self.gutter_utility_button.borrow_mut() = Some(button);
            }
            let button = self
                .gutter_utility_button
                .borrow()
                .clone()
                .expect("the button was just ensured");
            if self.host.parent(&button).as_ref() != Some(&container) {
                self.host.replace_children(&container, &[button]);
            }
        }
    }

    pub(super) fn reveal_utility_from_gutter_path(&self, path: &[Host::Element]) {
        if self.place_utility_from_selection() {
            return;
        }
        if let Some(ResolvedPointerTarget::Line(fields)) =
            Targets(self.0).resolve_pointer_target(path)
            && fields.number_column
        {
            self.show_utility_on_line(Targets(self.0).to_event_base_props(&fields));
        }
    }

    // The gutter utility follows the active selection first because a selected
    // range is more stable than transient hover/touch state. Without a
    // selection, desktop hover and mobile tap reveal can still place it on a
    // single row.
    pub(super) fn place_utility(&self) {
        if self.place_utility_from_selection() {
            return;
        }
        let hovered_line = self.hovered_line.borrow().clone();
        if let Some(hovered_line) = hovered_line {
            self.show_utility_on_line(hovered_line);
            return;
        }
        self.hide_utility();
    }

    pub(super) fn place_utility_from_selection(&self) -> bool {
        let Some(endpoints) = self.current_selection_ends() else {
            return false;
        };
        match Targets(self.0).target_for_selection_point(&endpoints.bottom) {
            None => self.hide_utility(),
            Some(target) => self.show_utility_on_line(Targets(self.0).to_event_base_props(&target)),
        }
        true
    }

    pub(super) fn show_utility_on_line(&self, line: LineEventBaseProps<Host::Element>) {
        let Some(container) = self.gutter_utility_container.borrow().clone() else {
            return;
        };
        let number_element = line.number_element.clone();
        *self.gutter_utility_line.borrow_mut() = Some(line);
        self.host.append_child(&number_element, &container);
    }

    pub(super) fn hide_utility(&self) {
        if let Some(container) = self.gutter_utility_container.borrow().as_ref() {
            self.host.remove_node(container);
        }
        *self.gutter_utility_line.borrow_mut() = None;
    }

    // Selection ranges preserve drag direction, so compare rendered row indexes to
    // find the visually top-most and bottom-most endpoints.
    pub(super) fn current_selection_ends(&self) -> Option<SelectionEnds> {
        let range = self.get_current_selection_range()?;
        self.selection_ends(&range)
    }

    pub(super) fn selection_ends(&self, range: &SelectedLineRange) -> Option<SelectionEnds> {
        let start = SelectionPoint {
            line_number: range.start,
            side: range.side,
        };
        let end = SelectionPoint {
            line_number: range.end,
            side: range.end_side.or(range.side),
        };
        let start_index = self.selection_point_row_index(&start)?;
        let end_index = self.selection_point_row_index(&end)?;
        Some(if start_index > end_index {
            SelectionEnds {
                top: end,
                bottom: start,
            }
        } else {
            SelectionEnds {
                top: start,
                bottom: end,
            }
        })
    }

    pub(super) fn selection_point_row_index(&self, point: &SelectionPoint) -> Option<i64> {
        let indexes = Targets(self.0).get_line_index(point.line_number, point.side)?;
        Some(if Targets(self.0).is_split_diff() {
            indexes.1
        } else {
            indexes.0
        })
    }
}
