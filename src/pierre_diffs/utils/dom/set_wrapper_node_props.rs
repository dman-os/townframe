// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/setWrapperNodeProps.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `pre` is a hast `Element` edited in place (upstream returns the same node); attribute writes are `Properties::set`/`remove` and `style.setProperty` a declaration of `style`; `${value}` of a custom property is the number's JavaScript text (integers print without a fraction).

use crate::hast::Element;
use crate::types::options::CustomPreProperty;
use crate::types::options::DiffIndicators;
use crate::types::options::Overflow;
use crate::types::options::PrePropertiesConfig;
use crate::types::options::PreType;

pub fn set_pre_node_properties(pre: &mut Element, config: &PrePropertiesConfig) {
    let PrePropertiesConfig {
        diff_indicators,
        disable_background,
        disable_line_numbers,
        overflow,
        pre_type,
        split,
        total_lines,
        custom_properties,
    } = config;
    let properties = &mut pre.properties;
    // NOTE: We always apply custom properties first so the important properties
    // cannot be overridden.
    if let Some(custom_properties) = custom_properties {
        // `for (const key in customProperties)` is insertion order; a map has
        // none, so keys apply in sorted order (as `create_pre_wrapper_properties`).
        let mut keys: Vec<&String> = custom_properties.keys().collect();
        keys.sort();
        for key in keys {
            match &custom_properties[key] {
                None => {}
                Some(CustomPreProperty::Text(text)) => properties.set(key, text.as_str()),
                // Rust prints `4.0` as `4` and `1.5` as `1.5`, as JavaScript does
                // (up to the exponent forms past 1e21, which no CSS value uses).
                Some(CustomPreProperty::Number(number)) => properties.set(key, number.to_string()),
            }
        }
    }
    if *pre_type == PreType::Diff {
        properties.set("data-diff", "");
        properties.remove("data-file");
    } else {
        properties.set("data-file", "");
        properties.remove("data-diff");
    }
    match diff_indicators {
        DiffIndicators::Bars => properties.set("data-indicators", "bars"),
        DiffIndicators::Classic => properties.set("data-indicators", "classic"),
        DiffIndicators::None => properties.remove("data-indicators"),
    }
    if *disable_line_numbers {
        properties.set("data-disable-line-numbers", "");
    } else {
        properties.remove("data-disable-line-numbers");
    }
    if *disable_background {
        properties.remove("data-background");
    } else {
        properties.set("data-background", "");
    }
    if *pre_type == PreType::Diff {
        properties.set("data-diff-type", if *split { "split" } else { "single" });
    } else {
        properties.remove("data-diff-type");
    }
    properties.set(
        "data-overflow",
        match overflow {
            Overflow::Scroll => "scroll",
            Overflow::Wrap => "wrap",
        },
    );
    // Set CSS custom property for line number column width
    properties.set_style_property(
        "--diffs-min-number-column-width-default",
        &format!("{}ch", total_lines.to_string().len()),
    );
}
