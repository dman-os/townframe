// Part of the S12a-2b port of FileDiff.ts (see ../file_diff.rs for the header):
// the theme CSS and the unsafe CSS the render keeps in the shadow root:
// `applyCachedThemeState`, `hasThemeChanged`, `applyThemeState`,
// `hydrateMeasuredScrollbar`, `injectUnsafeCSS`. A view over the instance (see
// `file_diff_view!`).
// Changes (besides the ones in the main header):
// * `upsertHostThemeStyle` (a util over a `ShadowRoot`) is done here over the
//   document: an all-blank CSS removes the node, `textContent = css` replaces
//   the children with one text node, and the node is appended to the shadow root
//   if it is not in it. The `style` node is created through
//   `create_host_theme_style_node`.
// * `getMeasuredScrollbarGutter(shadowRoot)` asks the host
//   (`ScrollbarGutterHost`) to measure.
// * `wrapUnsafeCSS`/`createUnsafeCSSStyleNode` build the node as upstream does;
//   `pre?.toggleAttribute('data-custom-styles', Boolean(unsafeCSS))` is
//   `set_attribute`/`remove_attribute`.
// * Test builds can replace `applyThemeState` with a recorder (the oracle seam
//   `FileDiffState::theme_state_recorder`), as the lifecycle oracle replaces it
//   on the published class.

use super::*;
use crate::utils::dom::create_unsafe_css_style_node::create_unsafe_css_style_node;
use crate::utils::dom::css_wrappers::patch_scrollbar_gutter_size;
use crate::utils::dom::css_wrappers::wrap_theme_css;
use crate::utils::dom::css_wrappers::wrap_unsafe_css;
use crate::utils::dom::host_theme::create_host_theme_style_node;
use crate::utils::dom::scrollbar_gutter::get_measured_scrollbar_gutter;

file_diff_view!(pub(crate) Theme);

impl<Host, Rendered, LAnnotation> Theme<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub(super) fn apply_cached_theme_state(&self, theme_type: ThemeTypes) -> bool {
        let (file_container, applied_theme_css) = {
            let state = self.state.borrow();
            (state.file_container, state.applied_theme_css.clone())
        };
        let theme_is_name = matches!(
            self.options.borrow().base.base.theme,
            Some(ThemeOption::Name(_))
        );
        let (false, Some(file_container), Some(applied_theme_css)) =
            (theme_is_name, file_container, applied_theme_css)
        else {
            return false;
        };
        let effective_theme_type = match applied_theme_css.base_theme_type {
            Some(BaseThemeType::Light) => ThemeTypes::Light,
            Some(BaseThemeType::Dark) => ThemeTypes::Dark,
            None => theme_type,
        };
        if applied_theme_css.theme_type == effective_theme_type {
            return false;
        }
        self.apply_theme_state(
            file_container,
            &applied_theme_css.theme_styles,
            theme_type,
            applied_theme_css.base_theme_type,
        );
        true
    }

    pub(crate) fn has_theme_changed(&self) -> bool {
        let theme = self.get_theme();
        self.state
            .borrow()
            .applied_theme_css
            .as_ref()
            .is_some_and(|applied| !are_themes_equal(Some(&applied.theme), Some(&theme)))
    }

    pub(super) fn apply_theme_state(
        &self,
        container: NodeKey,
        theme_styles: &str,
        theme_type: ThemeTypes,
        base_theme_type: Option<BaseThemeType>,
    ) {
        #[cfg(test)]
        {
            let recorder = self.state.borrow().theme_state_recorder.clone();
            if let Some(recorder) = recorder {
                recorder(container, theme_styles, theme_type, base_theme_type);
                return;
            }
        }
        let shadow_root = Nodes(self.0).shadow_root_of(container);
        let effective_theme_type = match base_theme_type {
            Some(BaseThemeType::Light) => ThemeTypes::Light,
            Some(BaseThemeType::Dark) => ThemeTypes::Dark,
            None => theme_type,
        };
        let theme = self.get_theme();
        let scrollbar_gutter = get_measured_scrollbar_gutter(&*self.host);
        let theme_css_style = self.state.borrow().theme_css_style;
        let style_in_shadow_root =
            theme_css_style.is_some_and(|style| self.document().parent(style) == Some(shadow_root));
        {
            let mut state = self.state.borrow_mut();
            if style_in_shadow_root
                && let Some(applied) = state.applied_theme_css.as_mut()
                && applied.theme_styles == theme_styles
                && applied.theme_type == effective_theme_type
                && applied.scrollbar_gutter == scrollbar_gutter
            {
                applied.theme = theme;
                return;
            }
        }
        let applied = AppliedThemeStyleCache {
            theme,
            theme_styles: theme_styles.to_owned(),
            theme_type: effective_theme_type,
            base_theme_type,
            scrollbar_gutter,
        };
        if self.state.borrow().has_adopted_theme_css && style_in_shadow_root {
            let mut state = self.state.borrow_mut();
            state.has_adopted_theme_css = false;
            state.applied_theme_css = Some(applied);
            return;
        }
        let theme_css = wrap_theme_css(theme_styles, effective_theme_type, scrollbar_gutter);
        let style = self.upsert_host_theme_style(shadow_root, theme_css_style, &theme_css);
        let mut state = self.state.borrow_mut();
        state.theme_css_style = style;
        state.applied_theme_css = style.map(|_| applied);
    }

    /// `upsertHostThemeStyle` over the document.
    fn upsert_host_theme_style(
        &self,
        shadow_root: NodeKey,
        current_node: Option<NodeKey>,
        theme_css: &str,
    ) -> Option<NodeKey> {
        if crate::js_text::trim(theme_css).is_empty() {
            Nodes(self.0).drop_optional(current_node);
            return None;
        }
        let node = current_node.unwrap_or_else(|| {
            self.document()
                .create_from_hast(&create_host_theme_style_node())
        });
        self.set_text_content(node, theme_css);
        if self.document().parent(node) != Some(shadow_root) {
            self.document().append_child(shadow_root, node);
        }
        Some(node)
    }

    /// `node.textContent = text`: one text node replaces the children.
    pub(super) fn set_text_content(&self, node: NodeKey, text: &str) {
        let text = self.document().create_text(text);
        Nodes(self.0).replace_children_retiring(node, &[text]);
    }

    pub(super) fn hydrate_measured_scrollbar(&self) {
        let (file_container, theme_css_style) = {
            let state = self.state.borrow();
            (state.file_container, state.theme_css_style)
        };
        let (Some(file_container), Some(theme_css_style)) = (file_container, theme_css_style)
        else {
            return;
        };
        if self.document().shadow_root(file_container).is_none() {
            return;
        }
        let patched = patch_scrollbar_gutter_size(
            &self.document().text_content(theme_css_style),
            get_measured_scrollbar_gutter(&*self.host),
        );
        self.set_text_content(theme_css_style, &patched);
    }

    pub(super) fn inject_unsafe_css(&self) {
        let (unsafe_css, pre, file_container) = {
            let state = self.state.borrow();
            (
                self.options.borrow().base.base.unsafe_css.clone(),
                state.pre,
                state.file_container,
            )
        };
        if let Some(pre) = pre {
            if unsafe_css.as_deref().is_some_and(|css| !css.is_empty()) {
                self.document().set_attribute(pre, "data-custom-styles", "");
            } else {
                self.document().remove_attribute(pre, "data-custom-styles");
            }
        }
        let Some(shadow_root) =
            file_container.and_then(|container| self.document().shadow_root(container))
        else {
            return;
        };

        let Some(unsafe_css) = unsafe_css.filter(|css| !css.is_empty()) else {
            let style = {
                let mut state = self.state.borrow_mut();
                state.applied_unsafe_css = None;
                state.unsafe_css_style.take()
            };
            Nodes(self.0).drop_optional(style);
            return;
        };

        let (existing, applied) = {
            let state = self.state.borrow();
            (state.unsafe_css_style, state.applied_unsafe_css.clone())
        };
        if existing.is_some_and(|style| self.document().parent(style) == Some(shadow_root))
            && applied.as_deref() == Some(unsafe_css.as_str())
        {
            return;
        }

        // Create or update the style element
        let style = existing.unwrap_or_else(|| {
            let style = self
                .document()
                .create_from_hast(&create_unsafe_css_style_node());
            self.state.borrow_mut().unsafe_css_style = Some(style);
            style
        });
        if self.document().parent(style) != Some(shadow_root) {
            self.document().append_child(shadow_root, style);
        }
        // Wrap in @layer unsafe to match SSR behavior
        self.set_text_content(style, &wrap_unsafe_css(&unsafe_css));
        self.state.borrow_mut().applied_unsafe_css = Some(unsafe_css);
    }
}
