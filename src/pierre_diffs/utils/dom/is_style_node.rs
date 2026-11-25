// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/isStyleNode.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `instanceof HTMLStyleElement` branch has no Rust counterpart; the tag-name fallback (`tagName ?? nodeName`, lower-cased) is the whole test.

use crate::hast::Element;

pub fn is_style_node(element: &Element) -> bool {
    element.tag_name.to_lowercase() == "style"
}
