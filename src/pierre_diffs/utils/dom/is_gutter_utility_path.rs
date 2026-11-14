// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/isGutterUtilityPath.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the event path is the target and its ancestors in the rendered model (`composedPath` becomes a model walk); `instanceof HTMLElement` filtering is the host handing over elements only; attribute reads are an `Fn(&Element, &str) -> Option<String>`.

// Recognize both the built-in utility button and custom content assigned to the
// gutter utility slot so other pointer handlers leave the gesture untouched.
pub fn is_gutter_utility_path<Element>(
    path: &[Element],
    get_attribute: impl Fn(&Element, &str) -> Option<String>,
) -> bool {
    path.iter().any(|element| {
        get_attribute(element, "data-utility-button").is_some()
            || get_attribute(element, "data-gutter-utility-slot").is_some()
            || get_attribute(element, "slot").as_deref() == Some("gutter-utility-slot")
            || get_attribute(element, "name").as_deref() == Some("gutter-utility-slot")
    })
}
