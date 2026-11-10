// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createUnsafeCSSStyleNode.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: a created `HTMLStyleElement` is a hast `Element`.

use crate::constants::UNSAFE_CSS_ATTRIBUTE;
use crate::hast::Element;
use crate::hast::Properties;

pub fn create_unsafe_css_style_node() -> Element {
    let mut properties = Properties::new();
    properties.set(UNSAFE_CSS_ATTRIBUTE, "");
    Element::new("style", properties, Vec::new())
}
