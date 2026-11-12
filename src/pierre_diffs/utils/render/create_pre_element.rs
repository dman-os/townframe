// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createPreElement.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `PrePropertiesConfig` is the `types::options` struct; `CustomPreProperties` values are `PropValue`s through `CustomPreProperty`.

use crate::hast::Element;
use crate::hast::PropValue;
use crate::hast::Properties;
use crate::types::options::CustomPreProperty;
use crate::types::options::DiffIndicators;
use crate::types::options::PrePropertiesConfig;
use crate::types::options::PreType;
use crate::utils::render::hast_utils::create_hast_element;

pub fn create_pre_element(options: &PrePropertiesConfig) -> Element {
    create_hast_element("pre", Vec::new(), create_pre_wrapper_properties(options))
}

pub fn create_pre_wrapper_properties(options: &PrePropertiesConfig) -> Properties {
    let PrePropertiesConfig {
        diff_indicators,
        disable_background,
        disable_line_numbers,
        overflow,
        pre_type,
        split,
        total_lines,
        custom_properties,
    } = options;
    // NOTE: We always apply custom properties first so the important
    // properties cannot be overridden
    let mut properties = Properties::new();
    if let Some(custom_properties) = custom_properties {
        let mut keys: Vec<&String> = custom_properties.keys().collect();
        keys.sort();
        for key in keys {
            let value = match &custom_properties[key] {
                None => PropValue::Undefined,
                Some(CustomPreProperty::Text(text)) => PropValue::Str(text.clone()),
                Some(CustomPreProperty::Number(number)) if number.fract() == 0.0 => {
                    PropValue::Int(*number as i64)
                }
                Some(CustomPreProperty::Number(number)) => PropValue::Float(*number),
            };
            properties.set(key, value);
        }
    }
    let is_diff = *pre_type == PreType::Diff;
    let flag = |present: bool| {
        if present {
            PropValue::Str(String::new())
        } else {
            PropValue::Undefined
        }
    };
    properties.set("data-diff", flag(is_diff));
    properties.set("data-file", flag(!is_diff));
    properties.set(
        "data-diff-type",
        if is_diff {
            PropValue::from(if *split { "split" } else { "single" })
        } else {
            PropValue::Undefined
        },
    );
    properties.set(
        "data-overflow",
        match overflow {
            crate::types::options::Overflow::Scroll => "scroll",
            crate::types::options::Overflow::Wrap => "wrap",
        },
    );
    properties.set("data-disable-line-numbers", flag(*disable_line_numbers));
    properties.set("data-background", flag(!*disable_background));
    properties.set(
        "data-indicators",
        match diff_indicators {
            DiffIndicators::Bars => PropValue::from("bars"),
            DiffIndicators::Classic => PropValue::from("classic"),
            DiffIndicators::None => PropValue::Undefined,
        },
    );
    // The pre is intentionally not focusable. It is not the scroll container
    // (`[data-code]` inside it is) and has no keyboard behavior of its own, so
    // a tabindex would only add a tab stop per diff and steal focus from a host
    // wrapper that wants to own keyboard navigation of the selection.
    properties.set(
        "style",
        format!(
            "--diffs-min-number-column-width-default:{}ch;",
            total_lines.to_string().len()
        ),
    );
    properties
}
