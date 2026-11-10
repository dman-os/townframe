// Part of the S12a-2b port of FileDiff.ts (see ../file_diff.rs for the header):
// the shell the render builds in the container's shadow root and the slotted
// light-DOM nodes around it: `getOrCreateFileContainer`,
// `adoptReusableShellElements`, `ensureSpriteSVG`, `getOrCreatePreNode`,
// `syncCodeNodesFromPre`, `removeRenderedCode`, `clearAuxiliaryNodes`,
// `applyHeaderToDOM` and the header slots, `renderSeparators`,
// `renderAnnotations`, `renderGutterUtility`, `applyPreNodeAttributes` and
// `applyBuffers`. A view over the instance (see `file_diff_view!`).
// Changes (besides the ones in the main header):
// * Every node a method takes out of the tree is detached with `remove` (or
//   `replace_children`) and handed to `retire`; `retire_children` does that for
//   the children a `replace_children` drops (upstream lets the garbage
//   collector have them).
// * `hastToHtml(headerAST)` is the hast element itself: the header cache holds
//   elements, which compare structurally where upstream compares the strings
//   (`tempDiv.innerHTML = html; tempDiv.firstElementChild` is
//   `create_from_hast`).
// * `fileContainer.shadowRoot ?? fileContainer.attachShadow(...)` is
//   `shadow_root` or `attach_shadow`; `element.slot = x` and `dataset.y = z`
//   are `slot` and `data-y` attributes; `style.x = y` is `set_style_property`.
// * `element.innerText = `${content}`` for header text is the text node a
//   `String` content makes (`RenderedContent::to_node`).
// * `this.editor?.__captureFocusForDOMReplacement()` is dropped with the editor.

use std::rc::Rc;

use super::*;
use crate::constants::CUSTOM_HEADER_SLOT_ID;
use crate::constants::DIFFS_TAG_NAME;
use crate::constants::HEADER_FILENAME_SUFFIX_SLOT_ID;
use crate::constants::HEADER_METADATA_SLOT_ID;
use crate::constants::HEADER_PREFIX_SLOT_ID;
use crate::constants::THEME_CSS_ATTRIBUTE;
use crate::constants::UNSAFE_CSS_ATTRIBUTE;
use crate::sprite::sprite_sheet_element;
use crate::types::options::PreType;
use crate::utils::dom::create_annotation_wrapper_node::create_annotation_wrapper_node;
use crate::utils::dom::create_gutter_utility_content_node::create_gutter_utility_content_node;
use crate::utils::dom::set_wrapper_node_props::set_pre_node_properties;
use crate::utils::predicates::are_diff_line_annotations_equal::are_diff_line_annotations_equal;
use crate::utils::predicates::are_hunk_data_equal::are_hunk_data_equal;
use crate::utils::predicates::are_pre_properties_equal::are_pre_properties_equal;

file_diff_view!(Nodes);

impl<Host, Rendered, LAnnotation> Nodes<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `node.remove()` for a node the instance is done with: detached now,
    /// discarded after the managers' next setup (see the ownership rule).
    pub(super) fn drop_node(&self, node: NodeKey) {
        self.document().remove(node);
        self.retire(node);
    }

    pub(super) fn drop_optional(&self, node: Option<NodeKey>) {
        if let Some(node) = node {
            self.drop_node(node);
        }
    }

    /// `parent.replaceChildren(...children)`, retiring the old children that
    /// are not among the new ones.
    pub(super) fn replace_children_retiring(&self, parent: NodeKey, children: &[NodeKey]) {
        let old = self.document().child_nodes(parent);
        self.document().replace_children(parent, children);
        for node in old {
            if !children.contains(&node) {
                self.retire(node);
            }
        }
    }

    pub(super) fn get_or_create_file_container(
        &self,
        file_container: Option<NodeKey>,
        parent_node: Option<NodeKey>,
    ) -> NodeKey {
        let previous_container = self.state.borrow().file_container;
        let next_container = file_container
            .or(previous_container)
            .unwrap_or_else(|| self.document().create_element(DIFFS_TAG_NAME));
        let container_changed = previous_container != Some(next_container);
        if container_changed {
            Render(self.0).emit_post_render(true);
        }
        {
            let mut state = self.state.borrow_mut();
            state.file_container = Some(next_container);
            if previous_container.is_some() && container_changed {
                state.header_cache.last_rendered_header = None;
                state.header_element = None;
            }
        }
        if let Some(parent_node) = parent_node
            && self.document().parent(next_container) != Some(parent_node)
        {
            self.document().append_child(parent_node, next_container);
        }
        if container_changed {
            self.adopt_reusable_shell_elements(next_container);
        }
        self.ensure_sprite_svg(next_container);
        next_container
    }

    // NOTE(amadeus): Technically this method is not safe for use outside of
    // the CodeView component, however I don't think in practice it really
    // should matter, but maybe there's some system we need in place to prevent
    // this from running outside of that environment?
    //
    // It's making very specific assumptions that all the elements will have the
    // correct content based on CodeView global options
    fn adopt_reusable_shell_elements(&self, file_container: NodeKey) {
        let Some(shadow_root) = self.document().shadow_root(file_container) else {
            return;
        };
        let unsafe_css = self.options.borrow().base.base.unsafe_css.clone();
        for element in self.document().children(shadow_root) {
            let tag = self.document().tag_name(element);
            let has = |name: &str| self.document().get_attribute(element, name).is_some();
            let mut state = self.state.borrow_mut();
            if tag == "SVG" {
                state.sprite_svg.get_or_insert(element);
            } else if tag == "STYLE" && has(THEME_CSS_ATTRIBUTE) {
                state.theme_css_style.get_or_insert(element);
                state.has_adopted_theme_css = true;
            } else if tag == "STYLE" && has(UNSAFE_CSS_ATTRIBUTE) {
                state.unsafe_css_style.get_or_insert(element);
                if state.applied_unsafe_css.is_none() {
                    state.applied_unsafe_css = unsafe_css.clone();
                }
            }
        }
    }

    fn ensure_sprite_svg(&self, file_container: NodeKey) -> NodeKey {
        let shadow_root = self.shadow_root_of(file_container);
        let existing = self.state.borrow().sprite_svg;
        let sprite_svg = existing.unwrap_or_else(|| {
            let sprite_svg = self.document().create_from_hast(&sprite_sheet_element());
            self.state.borrow_mut().sprite_svg = Some(sprite_svg);
            sprite_svg
        });
        if self.document().parent(sprite_svg) != Some(shadow_root) {
            self.document().append_child(shadow_root, sprite_svg);
        }
        shadow_root
    }

    /// `container.shadowRoot ?? container.attachShadow({ mode: 'open' })`
    pub(super) fn shadow_root_of(&self, container: NodeKey) -> NodeKey {
        self.document()
            .shadow_root(container)
            .unwrap_or_else(|| self.document().attach_shadow(container))
    }

    pub(super) fn get_or_create_pre_node(&self, container: NodeKey) -> NodeKey {
        let shadow_root = self.shadow_root_of(container);
        let pre = self.state.borrow().pre;
        let pre = match pre {
            // If we haven't created a pre element yet, lets go ahead and do that
            None => {
                let pre = self.document().create_element("pre");
                {
                    let mut state = self.state.borrow_mut();
                    state.pre = Some(pre);
                    state.applied_pre_attributes = None;
                    state.code_unified = None;
                    state.code_deletions = None;
                    state.code_additions = None;
                }
                self.document().append_child(shadow_root, pre);
                pre
            }
            // If we have a new parent container for the pre element, lets go
            // ahead and move it into the new container
            Some(pre) => {
                if self.document().parent(pre) != Some(shadow_root) {
                    self.document().append_child(shadow_root, pre);
                    self.state.borrow_mut().applied_pre_attributes = None;
                }
                pre
            }
        };
        let place_holder = self.state.borrow_mut().place_holder.take();
        self.drop_optional(place_holder);
        pre
    }

    pub(super) fn sync_code_nodes_from_pre(&self, pre: NodeKey) {
        let (mut unified, mut deletions, mut additions) = (None, None, None);
        for child in self.document().children(pre) {
            let has = |name: &str| self.document().get_attribute(child, name).is_some();
            if has("data-unified") {
                unified = Some(child);
            } else if has("data-deletions") {
                deletions = Some(child);
            } else if has("data-additions") {
                additions = Some(child);
            }
        }
        let mut state = self.state.borrow_mut();
        state.code_unified = unified;
        state.code_deletions = deletions;
        state.code_additions = additions;
    }

    pub(super) fn remove_rendered_code(&self) {
        self.resize_manager.clean_up();
        self.scroll_sync_manager.clean_up();
        self.interaction_manager.clean_up();

        let nodes = {
            let mut state = self.state.borrow_mut();
            let nodes = [
                state.buffer_before.take(),
                state.buffer_after.take(),
                state.code_unified.take(),
                state.code_deletions.take(),
                state.code_additions.take(),
                state.pre.take(),
            ];
            state.applied_pre_attributes = None;
            state.last_row_count = None;
            nodes
        };
        for node in nodes {
            self.drop_optional(node);
        }
    }

    pub(super) fn clear_auxiliary_nodes(&self) {
        let (separators, annotations, gutter_utility_content) = {
            let mut state = self.state.borrow_mut();
            (
                std::mem::take(&mut state.separator_cache),
                std::mem::take(&mut state.annotation_cache),
                state.gutter_utility_content.take(),
            )
        };
        for (element, _) in separators.into_values() {
            self.drop_node(element);
        }
        for cache in annotations.into_values() {
            self.drop_node(cache.element);
        }
        self.drop_optional(gutter_utility_content);
    }

    pub(super) fn clear_header_slots(&self) {
        let slots = {
            let mut state = self.state.borrow_mut();
            [
                state.header_prefix.take(),
                state.header_filename_suffix.take(),
                state.header_metadata.take(),
                state.header_custom.take(),
            ]
        };
        for slot in slots {
            self.drop_optional(slot);
        }
    }

    pub(super) fn apply_header_to_dom(
        &self,
        header_ast: &hast::Element,
        container: NodeKey,
        file_diff: &Rc<FileDiffMetadata>,
    ) {
        let place_holder = self.state.borrow_mut().place_holder.take();
        self.drop_optional(place_holder);
        // Session metadata changes in place, so an HTML cache created from the
        // input diff cannot describe the current edit-session header.
        // (No edit session here: the diff is never the session's.)
        let (reusable_header, last_rendered_header) = {
            let state = self.state.borrow();
            let cache = &state.header_cache;
            let reusable = if are_diff_targets_equal(cache.file_diff.as_deref(), Some(&**file_diff))
            {
                cache.header.clone()
            } else {
                None
            };
            (reusable, cache.last_rendered_header.clone())
        };
        let header = reusable_header.unwrap_or_else(|| header_ast.clone());
        {
            let mut state = self.state.borrow_mut();
            state.header_cache.header = Some(header.clone());
            state.header_cache.file_diff = Some(Rc::clone(file_diff));
        }
        if last_rendered_header.as_ref() != Some(&header) {
            let new_header = self.document().create_from_hast(&header);
            let shadow_root = self.document().shadow_root(container);
            let previous_header = self.state.borrow().header_element;
            if let Some(shadow_root) = shadow_root {
                if let Some(previous_header) = previous_header {
                    // `shadowRoot.replaceChild(newHeader, headerElement)`
                    self.document()
                        .insert_before(shadow_root, new_header, Some(previous_header));
                    self.drop_node(previous_header);
                } else {
                    // `shadowRoot.prepend(newHeader)`
                    let first = self.document().child_nodes(shadow_root).first().copied();
                    self.document()
                        .insert_before(shadow_root, new_header, first);
                }
            }
            let mut state = self.state.borrow_mut();
            state.header_element = Some(new_header);
            state.header_cache.last_rendered_header = Some(header);
        }

        if self.request_owner_render.is_some() {
            return;
        }

        let options = self.options.borrow().clone();
        if let Some(render_custom_header) = &options.render_custom_header {
            let content = render_custom_header(file_diff);
            let current = self.state.borrow().header_custom;
            let custom =
                self.upsert_header_slot_element(container, current, CUSTOM_HEADER_SLOT_ID, content);
            let others = {
                let mut state = self.state.borrow_mut();
                state.header_custom = custom;
                [
                    state.header_prefix.take(),
                    state.header_filename_suffix.take(),
                    state.header_metadata.take(),
                ]
            };
            for other in others {
                self.drop_optional(other);
            }
            return;
        }

        let prefix = options
            .render_header_prefix
            .as_ref()
            .and_then(|render| render(file_diff));
        let suffix = options
            .render_header_filename_suffix
            .as_ref()
            .and_then(|render| render(file_diff));
        let content = options
            .render_header_metadata
            .as_ref()
            .and_then(|render| render(file_diff));
        let current = self.state.borrow().header_prefix;
        let header_prefix =
            self.upsert_header_slot_element(container, current, HEADER_PREFIX_SLOT_ID, prefix);
        self.state.borrow_mut().header_prefix = header_prefix;
        let current = self.state.borrow().header_filename_suffix;
        let header_filename_suffix = self.upsert_header_slot_element(
            container,
            current,
            HEADER_FILENAME_SUFFIX_SLOT_ID,
            suffix,
        );
        self.state.borrow_mut().header_filename_suffix = header_filename_suffix;
        let current = self.state.borrow().header_metadata;
        let header_metadata =
            self.upsert_header_slot_element(container, current, HEADER_METADATA_SLOT_ID, content);
        let custom = {
            let mut state = self.state.borrow_mut();
            state.header_metadata = header_metadata;
            state.header_custom.take()
        };
        self.drop_optional(custom);
    }

    // Header slot callbacks are presence-based render hooks, not reactive views.
    fn upsert_header_slot_element(
        &self,
        container: NodeKey,
        current: Option<NodeKey>,
        slot: &str,
        content: Option<Rendered>,
    ) -> Option<NodeKey> {
        let Some(content) = content else {
            self.drop_optional(current);
            return None;
        };
        let element = current.unwrap_or_else(|| {
            // `createHeaderSlotElement`
            let element = self.document().create_element("div");
            self.document().set_attribute(element, "slot", slot);
            element
        });
        if current.is_none() {
            self.document().append_child(container, element);
        }
        // `replaceHeaderSlotContent`
        let content = content.to_node(self.document());
        self.replace_children_retiring(element, &[content]);
        Some(element)
    }

    pub(super) fn render_separators(&self, hunk_data: &[HunkData]) {
        let (file_container, custom_separator) = {
            let options = self.options.borrow();
            let custom = match &options.hunk_separators {
                Some(FileDiffHunkSeparators::Custom(render)) => Some(render.clone()),
                _ => None,
            };
            (self.state.borrow().file_container, custom)
        };
        let (Some(file_container), Some(render_separator), false) = (
            file_container,
            custom_separator,
            self.request_owner_render.is_some(),
        ) else {
            let separators = std::mem::take(&mut self.state.borrow_mut().separator_cache);
            for (element, _) in separators.into_values() {
                self.drop_node(element);
            }
            return;
        };
        let mut stale_separators = self
            .state
            .borrow()
            .separator_cache
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        for hunk in hunk_data {
            let id = hunk.slot_name.clone();
            let cached = self
                .state
                .borrow()
                .separator_cache
                .get(&id)
                .map(|(element, data)| (*element, data.clone()));
            let up_to_date = cached
                .as_ref()
                .is_some_and(|(_, data)| are_hunk_data_equal(hunk, data));
            if !up_to_date {
                if let Some((element, _)) = cached {
                    self.drop_node(element);
                }
                let element = self.document().create_element("div");
                self.document()
                    .set_style_property(element, "display", "contents");
                self.document()
                    .set_attribute(element, "slot", &hunk.slot_name);
                if let Some(child) = render_separator(hunk) {
                    let child = child.to_node(self.document());
                    self.document().append_child(element, child);
                }
                self.document().append_child(file_container, element);
                self.state
                    .borrow_mut()
                    .separator_cache
                    .insert(id.clone(), (element, hunk.clone()));
            }
            stale_separators.remove(&id);
        }
        for id in stale_separators {
            let removed = self.state.borrow_mut().separator_cache.remove(&id);
            if let Some((element, _)) = removed {
                self.drop_node(element);
            }
        }
    }

    pub(super) fn render_annotations(&self) {
        let file_container = self.state.borrow().file_container;
        let Some(file_container) = file_container.filter(|_| !self.request_owner_render.is_some())
        else {
            let annotations = std::mem::take(&mut self.state.borrow_mut().annotation_cache);
            for cache in annotations.into_values() {
                self.drop_node(cache.element);
            }
            return;
        };
        let mut stale_annotations = self
            .state
            .borrow()
            .annotation_cache
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let render_annotation = self.options.borrow().render_annotation.clone();
        let rendered_data = self.state.borrow().rendered_data.clone();
        if let (Some(render_annotation), Some(rendered_data)) = (render_annotation, rendered_data) {
            let FileDiffRenderData {
                file_diff,
                line_annotations,
            } = rendered_data;
            for (index, annotation) in line_annotations.iter().enumerate() {
                let name =
                    FileDiff::<Host, Rendered, LAnnotation>::get_annotation_slot_name(annotation);
                let id = format!("{index}-{name}");
                let cached = self.state.borrow().annotation_cache.get(&id).map(|cache| {
                    (
                        cache.element,
                        Rc::ptr_eq(&cache.file_diff, &file_diff)
                            && are_diff_line_annotations_equal(annotation, &cache.annotation),
                    )
                });
                if !cached.is_some_and(|(_, up_to_date)| up_to_date) {
                    if let Some((element, _)) = cached {
                        self.drop_node(element);
                        self.state.borrow_mut().annotation_cache.remove(&id);
                    }
                    let content = render_annotation(annotation, &file_diff);
                    // If we can't render anything, then we should not render anything
                    // and clear the annotation cache if necessary.
                    let Some(content) = content else {
                        continue;
                    };
                    let element = self
                        .document()
                        .create_from_hast(&create_annotation_wrapper_node(&name));
                    let content = content.to_node(self.document());
                    self.document().append_child(element, content);
                    self.document().append_child(file_container, element);
                    self.state.borrow_mut().annotation_cache.insert(
                        id.clone(),
                        AnnotationElementCache {
                            element,
                            annotation: annotation.clone(),
                            file_diff: Rc::clone(&file_diff),
                        },
                    );
                }
                stale_annotations.remove(&id);
            }
        }
        for id in stale_annotations {
            let removed = self.state.borrow_mut().annotation_cache.remove(&id);
            if let Some(cache) = removed {
                self.drop_node(cache.element);
            }
        }
    }

    pub(super) fn render_gutter_utility(&self) {
        let (file_container, render_gutter_utility) = {
            (
                self.state.borrow().file_container,
                self.options.borrow().render_gutter_utility.clone(),
            )
        };
        let (Some(file_container), Some(render_gutter_utility)) =
            (file_container, render_gutter_utility)
        else {
            let content = self.state.borrow_mut().gutter_utility_content.take();
            self.drop_optional(content);
            return;
        };
        let interaction_manager = Rc::clone(&self.interaction_manager);
        let element = render_gutter_utility(&move || interaction_manager.get_hovered_line());
        let has_content = self.state.borrow().gutter_utility_content.is_some();
        let Some(element) = element else {
            let content = self.state.borrow_mut().gutter_utility_content.take();
            self.drop_optional(content);
            return;
        };
        if has_content {
            return;
        }
        let gutter_utility_content = self
            .document()
            .create_from_hast(&create_gutter_utility_content_node());
        let element = element.to_node(self.document());
        self.document()
            .append_child(gutter_utility_content, element);
        self.document()
            .append_child(file_container, gutter_utility_content);
        self.state.borrow_mut().gutter_utility_content = Some(gutter_utility_content);
    }

    pub(super) fn apply_buffers(&self, pre: NodeKey, render_range: Option<&RenderRange>) {
        let disabled = self
            .options
            .borrow()
            .base
            .base
            .disable_virtualization_buffers
            .unwrap_or(false);
        let Some(render_range) = render_range.filter(|_| !disabled) else {
            let (before, after) = {
                let mut state = self.state.borrow_mut();
                (state.buffer_before.take(), state.buffer_after.take())
            };
            self.drop_optional(before);
            self.drop_optional(after);
            return;
        };
        // NOTE(amadeus): A very hacky pass at buffers outside the pre elements...
        // i may need to improve this...
        // `pre.before(...)` and `pre.after(...)` do nothing for a pre that was
        // removed from its parent.
        let pre_parent = self.document().parent(pre);
        if render_range.buffer_before > 0.0 {
            let existing = self.state.borrow().buffer_before;
            let buffer = existing.unwrap_or_else(|| {
                let buffer = self.document().create_element("div");
                self.document()
                    .set_attribute(buffer, "data-virtualizer-buffer", "before");
                // `pre.before(bufferBefore)`
                if let Some(pre_parent) = pre_parent {
                    self.document().insert_before(pre_parent, buffer, Some(pre));
                }
                self.state.borrow_mut().buffer_before = Some(buffer);
                buffer
            });
            self.document().set_style_property(
                buffer,
                "height",
                &format!("{}px", render_range.buffer_before),
            );
            self.document()
                .set_style_property(buffer, "contain", "strict");
        } else {
            let before = self.state.borrow_mut().buffer_before.take();
            self.drop_optional(before);
        }

        if render_range.buffer_after > 0.0 {
            let existing = self.state.borrow().buffer_after;
            let buffer = existing.unwrap_or_else(|| {
                let buffer = self.document().create_element("div");
                self.document()
                    .set_attribute(buffer, "data-virtualizer-buffer", "after");
                // `pre.after(bufferAfter)`
                if let Some(pre_parent) = pre_parent {
                    let siblings = self.document().child_nodes(pre_parent);
                    let position = siblings
                        .iter()
                        .position(|node| *node == pre)
                        .expect("pre is a child");
                    self.document().insert_before(
                        pre_parent,
                        buffer,
                        siblings.get(position + 1).copied(),
                    );
                }
                self.state.borrow_mut().buffer_after = Some(buffer);
                buffer
            });
            self.document().set_style_property(
                buffer,
                "height",
                &format!("{}px", render_range.buffer_after),
            );
            self.document()
                .set_style_property(buffer, "contain", "strict");
        } else {
            let after = self.state.borrow_mut().buffer_after.take();
            self.drop_optional(after);
        }
    }

    pub(super) fn apply_pre_node_attributes(&self, pre: NodeKey, result: &HunksRenderResult) {
        let options = self.options.borrow();
        let split = match options.base.diff_style.unwrap_or(DiffStyle::Split) {
            DiffStyle::Unified => false,
            DiffStyle::Split => {
                result.additions_content_ast.is_some() && result.deletions_content_ast.is_some()
            }
        };
        let pre_properties = PrePropertiesConfig {
            pre_type: PreType::Diff,
            diff_indicators: options.base.diff_indicators.unwrap_or(DiffIndicators::Bars),
            disable_background: options.base.disable_background.unwrap_or(false),
            disable_line_numbers: options.base.base.disable_line_numbers.unwrap_or(false),
            overflow: options.base.base.overflow.unwrap_or(Overflow::Scroll),
            split,
            total_lines: result.total_lines,
            custom_properties: None,
        };
        drop(options);
        if are_pre_properties_equal(
            Some(&pre_properties),
            self.state.borrow().applied_pre_attributes.as_ref(),
        ) {
            return;
        }
        self.set_pre_properties(pre, &pre_properties);
        self.state.borrow_mut().applied_pre_attributes = Some(pre_properties);
    }

    /// `setPreNodeProperties(pre, config)` on a document node: the util edits
    /// the node's property bag as an element's.
    fn set_pre_properties(&self, pre: NodeKey, config: &PrePropertiesConfig) {
        self.document().edit_properties(pre, |properties| {
            let mut element = hast::Element::new("pre", std::mem::take(properties), Vec::new());
            set_pre_node_properties(&mut element, config);
            *properties = element.properties;
        });
    }
}
