// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createGutterUtilityContentNode.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: a created `HTMLElement` is a hast `Element`; the `style.*` assignments are declarations of its `style` property.

use crate::hast::Element;
use crate::hast::Properties;

pub fn create_gutter_utility_content_node() -> Element {
    let mut properties = Properties::new();
    properties.set("slot", "gutter-utility-slot");
    properties.set_style_property("position", "absolute");
    properties.set_style_property("top", "0");
    properties.set_style_property("bottom", "0");
    properties.set_style_property("text-align", "center");
    properties.set_style_property("white-space", "normal");
    properties.set_style_property("touch-action", "none");
    Element::new("div", properties, Vec::new())
}
