// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createAnnotationWrapperNode.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: a created `HTMLElement` is a hast `Element`; the `style.whiteSpace` assignment is the `white-space:normal;` declaration of its `style` property and `dataset.annotationSlot = ''` the `data-annotation-slot` attribute.

use crate::hast::Element;
use crate::hast::Properties;

pub fn create_annotation_wrapper_node(slot: &str) -> Element {
    let mut properties = Properties::new();
    properties.set("data-annotation-slot", "");
    properties.set("slot", slot);
    properties.set_style_property("white-space", "normal");
    Element::new("div", properties, Vec::new())
}
