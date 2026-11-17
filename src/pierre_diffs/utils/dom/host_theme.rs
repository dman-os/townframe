// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/hostTheme.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `upsertHostThemeStyle` has no `ShadowRoot`: the caller owns the style node as an `Option<Element>` and a `Some` result means "attached to the host". `currentNode?.remove()` is returning `None`; `textContent = css` replaces the children with one text node; the `currentNode.parentNode !== shadowRoot` append is the caller rendering the returned node.

use crate::constants::THEME_CSS_ATTRIBUTE;
use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::Properties;
use crate::hast::Text;
use crate::js_text;

// Keep the host theme style stable so renderers can update the host-scoped theme
// CSS without rebuilding the rest of the shadow DOM.
pub fn upsert_host_theme_style(current_node: Option<Element>, theme_css: &str) -> Option<Element> {
    if js_text::trim(theme_css).is_empty() {
        return None;
    }

    let mut current_node = current_node.unwrap_or_else(create_host_theme_style_node);
    current_node.children = vec![ElementContent::Text(Text::new(theme_css))];
    Some(current_node)
}

pub fn create_host_theme_style_node() -> Element {
    let mut properties = Properties::new();
    properties.set(THEME_CSS_ATTRIBUTE, "");
    Element::new("style", properties, Vec::new())
}
